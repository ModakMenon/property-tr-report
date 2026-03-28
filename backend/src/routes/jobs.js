import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import Busboy from 'busboy';
import { S3Client, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { uploadStreamToS3, uploadToS3, getJsonFromS3, putJsonToS3, listS3Objects, getSignedDownloadUrl, getFromS3, streamToBuffer, getSignedUploadUrl } from '../services/s3Service.js';
import { queueManager, addSSEClient, removeSSEClient, getJobStatus, setJobStatus, updateJobStatus } from '../services/queueService.js';
import { getDocumentAnalysis } from '../services/claudeService.js';
import { analyzePdf } from '../services/pdfChunkService.js';

const router = express.Router();

// S3 client for multipart operations
const s3Client = new S3Client({ region: process.env.AWS_REGION });
const BUCKET = process.env.S3_BUCKET_NAME;

// Create a new job
router.post('/create', async (req, res) => {
  try {
    const jobId = `${new Date().toISOString().split('T')[0].replace(/-/g, '')}_${Date.now()}_${uuidv4().slice(0, 8)}`;
    
    const jobData = {
      id: jobId,
      status: 'created',
      createdAt: new Date().toISOString(),
      createdBy: req.body.userEmail || 'unknown',
      totalDocuments: 0,
      processedCount: 0,
      failedCount: 0
    };

    await putJsonToS3(`jobs/${jobId}/metadata.json`, jobData);
    setJobStatus(jobId, jobData);

    res.json({ success: true, jobId, job: jobData });
  } catch (error) {
    console.error('Error creating job:', error);
    res.status(500).json({ error: 'Failed to create job' });
  }
});

// ─────────────────────────────────────────────────────────────
// MULTIPART UPLOAD — Step 1
// Initiates multipart upload on S3, returns one presigned PUT
// URL per part so the browser uploads chunks directly to S3.
// partCount is calculated by the frontend: ceil(fileSize / 10MB)
// ─────────────────────────────────────────────────────────────
router.post('/:jobId/presign-upload', async (req, res) => {
  const { jobId } = req.params;
  const { fileName, contentType, partCount } = req.body;

  try {
    const s3Key = `jobs/${jobId}/uploads/raw/documents.zip`;

    // Tell S3 we're starting a multipart upload
    const multipart = await s3Client.send(new CreateMultipartUploadCommand({
      Bucket: BUCKET,
      Key: s3Key,
      ContentType: contentType || 'application/zip',
    }));

    const uploadId = multipart.UploadId;

    // Generate a presigned PUT URL for each part (expires in 1 hour)
    const partUrls = await Promise.all(
      Array.from({ length: partCount }, (_, i) =>
        getSignedUrl(
          s3Client,
          new UploadPartCommand({
            Bucket: BUCKET,
            Key: s3Key,
            UploadId: uploadId,
            PartNumber: i + 1,
          }),
          { expiresIn: 3600 }
        )
      )
    );

    console.log(`[Presign] Multipart initiated for job: ${jobId}, parts: ${partCount}, uploadId: ${uploadId}`);
    res.json({ success: true, uploadId, s3Key, partUrls });

  } catch (error) {
    console.error('[Presign] Error initiating multipart upload:', error);
    res.status(500).json({ error: 'Failed to initiate upload' });
  }
});

// ─────────────────────────────────────────────────────────────
// MULTIPART UPLOAD — Step 2
// Called after all parts are uploaded. Tells S3 to assemble
// the parts into a single file.
// Body: { s3Key, uploadId, parts: [{ PartNumber, ETag }] }
// ─────────────────────────────────────────────────────────────
router.post('/:jobId/complete-multipart', async (req, res) => {
  const { jobId } = req.params;
  const { s3Key, uploadId, parts } = req.body;

  try {
    await s3Client.send(new CompleteMultipartUploadCommand({
      Bucket: BUCKET,
      Key: s3Key,
      UploadId: uploadId,
      MultipartUpload: { Parts: parts },
    }));

    console.log(`[Multipart] Completed for job: ${jobId}, key: ${s3Key}`);
    res.json({ success: true });

  } catch (error) {
    console.error('[Multipart] Complete failed:', error);
    res.status(500).json({ error: 'Failed to complete upload' });
  }
});

// ─────────────────────────────────────────────────────────────
// MULTIPART UPLOAD — Abort (cleanup on error or cancel)
// Tells S3 to discard all uploaded parts so you don't get
// charged for incomplete multipart storage.
// ─────────────────────────────────────────────────────────────
router.post('/:jobId/abort-multipart', async (req, res) => {
  const { jobId } = req.params;
  const { s3Key, uploadId } = req.body;

  try {
    await s3Client.send(new AbortMultipartUploadCommand({
      Bucket: BUCKET,
      Key: s3Key,
      UploadId: uploadId,
    }));
    console.log(`[Multipart] Aborted for job: ${jobId}`);
    res.json({ success: true });

  } catch (error) {
    console.error('[Multipart] Abort failed:', error);
    res.status(500).json({ error: 'Failed to abort upload' });
  }
});

// ─────────────────────────────────────────────────────────────
// CONFIRM UPLOAD — Step 3
// Called after complete-multipart succeeds. Updates job status
// to 'uploaded' so the worker knows it can start processing.
// ─────────────────────────────────────────────────────────────
router.post('/:jobId/confirm-upload', async (req, res) => {
  const { jobId } = req.params;
  const { s3Key, fileName, fileSize } = req.body;

  try {
    updateJobStatus(jobId, {
      status: 'uploaded',
      uploadedAt: new Date().toISOString(),
      fileName: fileName,
      fileSize: fileSize
    });

    console.log(`[Confirm] Upload confirmed for job: ${jobId}, file: ${fileName}, size: ${(fileSize / 1024 / 1024).toFixed(2)}MB`);

    res.json({ success: true, message: 'File uploaded successfully', fileSize, fileName });
  } catch (error) {
    console.error('[Confirm] Error confirming upload:', error);
    res.status(500).json({ error: 'Failed to confirm upload' });
  }
});

// Upload documents (ZIP file) - Direct stream to S3 (kept as fallback)
router.post('/:jobId/upload', async (req, res) => {
  const { jobId } = req.params;
  
  console.log(`[Upload] Starting upload for job: ${jobId}`);
  console.log(`[Upload] Content-Length: ${req.headers['content-length']} bytes`);
  
  try {
    const busboy = Busboy({ 
      headers: req.headers,
      limits: { fileSize: 4 * 1024 * 1024 * 1024 } // 4GB
    });
    
    let uploadPromise = null;
    let fileName = '';
    let fileSize = 0;

    busboy.on('file', (fieldname, fileStream, info) => {
      fileName = info.filename;
      console.log(`[Upload] Receiving file: ${fileName}`);
      
      const s3Key = `jobs/${jobId}/uploads/raw/documents.zip`;
      
      fileStream.on('data', (chunk) => {
        fileSize += chunk.length;
        if (fileSize % (10 * 1024 * 1024) === 0) {
          console.log(`[Upload] Received: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);
        }
      });

      uploadPromise = uploadStreamToS3(s3Key, fileStream, 'application/zip');
    });

    busboy.on('finish', async () => {
      if (!uploadPromise) {
        return res.status(400).json({ error: 'No file received' });
      }

      try {
        await uploadPromise;
        console.log(`[Upload] S3 upload complete for ${fileName}`);

        updateJobStatus(jobId, {
          status: 'uploaded',
          uploadedAt: new Date().toISOString(),
          fileName,
          fileSize
        });

        res.json({ success: true, message: 'File uploaded successfully', fileSize, fileName });
      } catch (s3Error) {
        console.error(`[Upload] S3 upload failed:`, s3Error);
        res.status(500).json({ error: 'S3 upload failed: ' + s3Error.message });
      }
    });

    busboy.on('error', (err) => {
      console.error(`[Upload] Busboy error:`, err);
      res.status(500).json({ error: 'Upload parsing failed: ' + err.message });
    });

    req.pipe(busboy);
    
  } catch (error) {
    console.error('[Upload] Error:', error);
    res.status(500).json({ error: 'Failed to upload file: ' + error.message });
  }
});

// Upload single PDF (for large single-file processing)
router.post('/:jobId/upload-pdf', async (req, res) => {
  const { jobId } = req.params;
  
  console.log(`[Upload-PDF] Starting single PDF upload for job: ${jobId}`);
  
  try {
    const busboy = Busboy({ 
      headers: req.headers,
      limits: { fileSize: 500 * 1024 * 1024 } // 500MB max for single PDF
    });
    
    let fileName = '';
    let fileSize = 0;
    let fileBuffer = [];

    busboy.on('file', (fieldname, fileStream, info) => {
      fileName = info.filename;
      console.log(`[Upload-PDF] Receiving file: ${fileName}`);
      
      fileStream.on('data', (chunk) => {
        fileBuffer.push(chunk);
        fileSize += chunk.length;
        if (fileSize % (10 * 1024 * 1024) === 0) {
          console.log(`[Upload-PDF] Received: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);
        }
      });
    });

    busboy.on('finish', async () => {
      if (fileBuffer.length === 0) {
        return res.status(400).json({ error: 'No file received' });
      }

      try {
        const buffer = Buffer.concat(fileBuffer);
        
        console.log(`[Upload-PDF] Analyzing PDF...`);
        const analysis = await analyzePdf(buffer);
        console.log(`[Upload-PDF] Analysis:`, JSON.stringify(analysis, null, 2));
        
        const s3Key = `jobs/${jobId}/uploads/extracted/${fileName}`;
        await uploadToS3(s3Key, buffer, 'application/pdf');
        console.log(`[Upload-PDF] S3 upload complete: ${s3Key}`);

        const queueData = {
          totalDocuments: 1,
          processedCount: 0,
          documents: [{
            name: fileName,
            key: s3Key,
            type: '.pdf',
            size: fileSize,
            status: 'pending',
            analysis
          }],
          results: [],
          failedDocuments: [],
          status: 'ready'
        };
        
        await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);

        updateJobStatus(jobId, {
          status: 'extracted',
          uploadedAt: new Date().toISOString(),
          fileName,
          fileSize,
          totalDocuments: 1,
          processedCount: 0,
          pdfAnalysis: analysis
        });

        res.json({
          success: true,
          message: 'PDF uploaded successfully',
          fileSize,
          fileSizeMB: (fileSize / 1024 / 1024).toFixed(2),
          fileName,
          analysis
        });
      } catch (error) {
        console.error(`[Upload-PDF] Processing failed:`, error);
        res.status(500).json({ error: 'PDF processing failed: ' + error.message });
      }
    });

    busboy.on('error', (err) => {
      console.error(`[Upload-PDF] Busboy error:`, err);
      res.status(500).json({ error: 'Upload parsing failed: ' + err.message });
    });

    req.pipe(busboy);
    
  } catch (error) {
    console.error('[Upload-PDF] Error:', error);
    res.status(500).json({ error: 'Failed to upload PDF: ' + error.message });
  }
});

// Analyze uploaded PDF (preview processing strategy)
router.get('/:jobId/analyze-pdf', async (req, res) => {
  const { jobId } = req.params;
  
  try {
    const queueData = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
    
    if (!queueData.documents || queueData.documents.length === 0) {
      return res.status(404).json({ error: 'No documents found in job' });
    }

    const pdfDoc = queueData.documents.find(d => d.type === '.pdf');
    if (!pdfDoc) {
      return res.status(404).json({ error: 'No PDF document found in job' });
    }

    if (pdfDoc.analysis) {
      return res.json({ success: true, documentName: pdfDoc.name, ...pdfDoc.analysis });
    }

    const pdfStream = await getFromS3(pdfDoc.key);
    const pdfBuffer = await streamToBuffer(pdfStream);
    const analysis = await analyzePdf(pdfBuffer);

    res.json({ success: true, documentName: pdfDoc.name, ...analysis });
  } catch (error) {
    console.error('PDF analysis error:', error);
    res.status(500).json({ error: 'Failed to analyze PDF: ' + error.message });
  }
});

// Start extraction process
router.post('/:jobId/extract', async (req, res) => {
  try {
    const { jobId } = req.params;

    await queueManager.add('extract', { jobId, type: 'extract' }, { jobId: `${jobId}-extract` });
    updateJobStatus(jobId, { status: 'extracting' });

    res.json({ success: true, message: 'Extraction started' });
  } catch (error) {
    console.error('Extraction error:', error);
    res.status(500).json({ error: 'Failed to start extraction' });
  }
});

// Start analysis process
router.post('/:jobId/analyze', async (req, res) => {
  try {
    const { jobId } = req.params;

    await queueManager.add('analyze', { jobId, type: 'analyze' }, { jobId: `${jobId}-analyze` });
    updateJobStatus(jobId, { status: 'analyzing' });

    res.json({ success: true, message: 'Analysis started' });
  } catch (error) {
    console.error('Analysis error:', error);
    res.status(500).json({ error: 'Failed to start analysis' });
  }
});

// Resume a failed/interrupted job
router.post('/:jobId/resume', async (req, res) => {
  try {
    const { jobId } = req.params;
    
    console.log(`[Resume] Attempting to resume job: ${jobId}`);

    let queueData;
    try {
      queueData = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
    } catch (err) {
      return res.status(404).json({ 
        error: 'Job queue data not found. Cannot resume.',
        details: 'The job may not have started processing or queue.json is missing.'
      });
    }

    const pendingDocs = queueData.documents.filter(d => d.status === 'pending');
    const completedDocs = queueData.documents.filter(d => d.status === 'completed');
    const failedDocs = queueData.documents.filter(d => d.status === 'failed');

    console.log(`[Resume] Job ${jobId} state: ${completedDocs.length} completed, ${failedDocs.length} failed, ${pendingDocs.length} pending`);

    if (pendingDocs.length === 0) {
      if (queueData.status === 'completed') {
        return res.json({
          success: true,
          message: 'Job already completed',
          status: 'completed',
          stats: { total: queueData.totalDocuments, completed: completedDocs.length, failed: failedDocs.length, pending: 0 }
        });
      }

      await queueManager.add('generate-report', { jobId, type: 'generate-report' }, { jobId: `${jobId}-report-resume` });
      updateJobStatus(jobId, { status: 'generating-report' });

      return res.json({
        success: true,
        message: 'All documents already processed. Generating report...',
        status: 'generating-report',
        stats: { total: queueData.totalDocuments, completed: completedDocs.length, failed: failedDocs.length, pending: 0 }
      });
    }

    try {
      const metadata = await getJsonFromS3(`jobs/${jobId}/metadata.json`);
      metadata.status = 'processing';
      metadata.resumedAt = new Date().toISOString();
      metadata.resumeCount = (metadata.resumeCount || 0) + 1;
      await putJsonToS3(`jobs/${jobId}/metadata.json`, metadata);
    } catch (err) {
      console.error('[Resume] Failed to update metadata:', err.message);
    }

    await queueManager.add('analyze', { jobId, type: 'analyze', isResume: true }, { jobId: `${jobId}-analyze-resume-${Date.now()}` });
    updateJobStatus(jobId, { status: 'processing' });

    res.json({
      success: true,
      message: `Resuming job. ${pendingDocs.length} documents remaining.`,
      status: 'processing',
      stats: { total: queueData.totalDocuments, completed: completedDocs.length, failed: failedDocs.length, pending: pendingDocs.length }
    });
  } catch (error) {
    console.error('Resume error:', error);
    res.status(500).json({ error: 'Failed to resume job', details: error.message });
  }
});

// Generate report
router.post('/:jobId/generate-report', async (req, res) => {
  try {
    const { jobId } = req.params;

    await queueManager.add('generate-report', { jobId, type: 'generate-report' }, { jobId: `${jobId}-report` });

    res.json({ success: true, message: 'Report generation started' });
  } catch (error) {
    console.error('Report generation error:', error);
    res.status(500).json({ error: 'Failed to start report generation' });
  }
});

// Get job status
router.get('/:jobId/status', async (req, res) => {
  try {
    const { jobId } = req.params;
    
    let status = getJobStatus(jobId);
    
    if (!status) {
      try {
        status = await getJsonFromS3(`jobs/${jobId}/metadata.json`);
        setJobStatus(jobId, status);
      } catch {
        return res.status(404).json({ error: 'Job not found' });
      }
    }

    try {
      const queueData = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
      status.queueStatus = queueData.status;
      status.totalDocuments = queueData.totalDocuments;
      status.processedCount = queueData.processedCount;
      status.failedCount = queueData.failedDocuments?.length || 0;
    } catch {
      // Queue not yet created
    }

    res.json(status);
  } catch (error) {
    console.error('Status error:', error);
    res.status(500).json({ error: 'Failed to get job status' });
  }
});

// Get job logs
router.get('/:jobId/logs', async (req, res) => {
  try {
    const { jobId } = req.params;
    
    try {
      const logsData = await getJsonFromS3(`jobs/${jobId}/processing/logs.json`);
      res.json({ success: true, logs: logsData.logs || [], lastUpdated: logsData.lastUpdated });
    } catch (err) {
      res.json({ success: true, logs: [], lastUpdated: null });
    }
  } catch (error) {
    console.error('Logs error:', error);
    res.status(500).json({ error: 'Failed to fetch logs' });
  }
});

// SSE endpoint for live updates
router.get('/:jobId/events', (req, res) => {
  const { jobId } = req.params;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();

  res.write(`event: connected\ndata: ${JSON.stringify({ jobId })}\n\n`);

  addSSEClient(jobId, res);

  const heartbeat = setInterval(() => {
    res.write(`event: heartbeat\ndata: ${JSON.stringify({ time: Date.now() })}\n\n`);
  }, 30000);

  req.on('close', () => {
    clearInterval(heartbeat);
    removeSSEClient(jobId, res);
  });
});

// Download report
router.get('/:jobId/download', async (req, res) => {
  try {
    const { jobId } = req.params;
    const reportKey = `jobs/${jobId}/output/Legal_Audit_Report.xlsx`;
    const downloadUrl = await getSignedDownloadUrl(reportKey, 3600);
    res.json({ success: true, downloadUrl, expiresIn: 3600 });
  } catch (error) {
    console.error('Download error:', error);
    res.status(500).json({ error: 'Failed to generate download link' });
  }
});

// List all jobs
router.get('/', async (req, res) => {
  try {
    const objects = await listS3Objects('jobs/');
    
    const jobIds = new Set();
    objects.forEach(obj => {
      const match = obj.Key.match(/^jobs\/([^\/]+)\//);
      if (match) jobIds.add(match[1]);
    });

    const jobs = [];
    for (const jobId of jobIds) {
      try {
        const metadata = await getJsonFromS3(`jobs/${jobId}/metadata.json`);
        jobs.push(metadata);
      } catch {
        // Skip jobs without metadata
      }
    }

    jobs.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    res.json({ jobs });
  } catch (error) {
    console.error('List jobs error:', error);
    res.status(500).json({ error: 'Failed to list jobs' });
  }
});

export default router;
