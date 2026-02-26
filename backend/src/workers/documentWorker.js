import { Worker } from 'bullmq';
import unzipper from 'unzipper';
import pdf from 'pdf-parse';
import { getWorkerConnection, isUsingInMemoryQueue, broadcastToJob, updateJobStatus, getJobStatus, registerJobHandler, documentQueue } from '../services/queueService.js';
import { getFromS3, uploadToS3, getJsonFromS3, putJsonToS3, streamToBuffer } from '../services/s3Service.js';
import { analyzeDocument, getDocumentAnalysis } from '../services/claudeService.js';
import { generateAuditReport } from '../services/excelService.js';
import { analyzePdf, PDF_CONFIG } from '../services/pdfChunkService.js';

let worker = null;

// Large file size threshold (15MB)
const LARGE_FILE_THRESHOLD = PDF_CONFIG.MAX_DIRECT_PDF_SIZE;

// In-memory log cache to batch writes
const logCache = new Map();
const LOG_FLUSH_INTERVAL = 5000; // Flush logs every 5 seconds
const LOG_FLUSH_COUNT = 20; // Or every 20 logs

/**
 * Save a log entry to S3 (batched for performance)
 */
async function saveJobLog(jobId, time, message, type = 'info') {
  if (!logCache.has(jobId)) {
    logCache.set(jobId, []);
  }
  
  logCache.get(jobId).push({ time, message, type, timestamp: Date.now() });
  
  // Flush if we have enough logs
  if (logCache.get(jobId).length >= LOG_FLUSH_COUNT) {
    await flushJobLogs(jobId);
  }
}

/**
 * Flush logs to S3
 */
async function flushJobLogs(jobId) {
  const logs = logCache.get(jobId);
  if (!logs || logs.length === 0) return;
  
  try {
    // Load existing logs
    let existingLogs = [];
    try {
      const existing = await getJsonFromS3(`jobs/${jobId}/processing/logs.json`);
      existingLogs = existing.logs || [];
    } catch (e) {
      // No existing logs file
    }
    
    // Append new logs
    const allLogs = [...existingLogs, ...logs];
    
    // Keep only last 1000 logs to prevent file from growing too large
    const trimmedLogs = allLogs.slice(-1000);
    
    await putJsonToS3(`jobs/${jobId}/processing/logs.json`, {
      logs: trimmedLogs,
      lastUpdated: new Date().toISOString()
    });
    
    // Clear cache
    logCache.set(jobId, []);
  } catch (err) {
    console.error(`[Logs] Failed to flush logs for ${jobId}:`, err.message);
  }
}

/**
 * Ensure all logs are flushed for a job
 */
async function finalizeJobLogs(jobId) {
  await flushJobLogs(jobId);
}

// Job processor function
async function processJob(job) {
  const { jobId, type } = job.data;
  
  console.log(`[Worker] Processing job: ${job.id}, type: ${type}, jobId: ${jobId}`);
  
  try {
    switch (type) {
      case 'extract':
        await processExtraction(jobId, job);
        break;
      case 'analyze':
        await processAnalysis(jobId, job);
        break;
      case 'generate-report':
        await processReportGeneration(jobId, job);
        break;
      default:
        throw new Error(`Unknown job type: ${type}`);
    }
  } catch (error) {
    console.error(`[Worker] Job ${job.id} failed:`, error);
    broadcastToJob(jobId, 'error', { message: error.message });
    throw error;
  }
}

export async function initializeWorker() {
  // Register handler for in-memory queue fallback
  registerJobHandler('document-processing', processJob);

  // Check if using in-memory queue
  if (isUsingInMemoryQueue()) {
    console.log('✓ Using in-memory job processing');
    return null;
  }

  // Get the worker connection
  const workerConnection = getWorkerConnection();
  
  if (!workerConnection) {
    console.log('✓ Using in-memory job processing (no worker connection)');
    return null;
  }

  try {
    // Wait for worker connection to be ready
    await new Promise((resolve, reject) => {
      if (workerConnection.status === 'ready') {
        resolve();
        return;
      }
      const timeout = setTimeout(() => reject(new Error('Worker connection timeout')), 15000);
      workerConnection.once('ready', () => {
        clearTimeout(timeout);
        resolve();
      });
      workerConnection.once('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });

    console.log('✓ Redis worker connection ready');

    worker = new Worker('document-processing', processJob, {
      connection: workerConnection,
      concurrency: 1,
      // Very long lock duration for large file processing
      lockDuration: 1800000, // 30 minutes
      stalledInterval: 600000, // Check for stalled jobs every 10 minutes
      maxStalledCount: 5, // Allow 5 stall checks before failing
      lockRenewTime: 300000, // Renew lock every 5 minutes
    });

    worker.on('completed', (job) => {
      console.log(`[Worker] Job ${job.id} completed successfully`);
    });

    worker.on('failed', (job, err) => {
      console.error(`[Worker] Job ${job?.id} failed:`, err.message);
    });

    worker.on('stalled', (jobId) => {
      console.warn(`[Worker] Job ${jobId} stalled - will retry`);
    });

    worker.on('error', (err) => {
      console.error(`[Worker] Worker error:`, err.message);
    });

    console.log('✓ BullMQ worker initialized');
    console.log('  Lock duration: 30 minutes');
    console.log('  Stall check interval: 10 minutes');
    console.log('  Max stall count: 5');
    
    return worker;
  } catch (err) {
    console.warn('⚠️  BullMQ worker failed to initialize:', err.message);
    console.warn('⚠️  Using in-memory processing');
    return null;
  }
}

async function processExtraction(jobId, job) {
  const timestamp = () => new Date().toISOString().split('T')[1].split('.')[0];
  
  console.log(`[Extract] ========================================`);
  console.log(`[Extract] Starting TRUE STREAMING extraction for job: ${jobId}`);
  console.log(`[Extract] Memory-efficient: processes one file at a time`);
  console.log(`[Extract] ========================================`);
  
  // Save initial log
  await saveJobLog(jobId, timestamp(), 'Starting ZIP extraction (streaming mode)...');
  
  broadcastToJob(jobId, 'log', { 
    time: timestamp(), 
    message: 'Starting ZIP extraction (streaming mode)...' 
  });

  const supportedExtensions = ['.pdf', '.png', '.jpg', '.jpeg', '.docx'];
  const documents = [];
  let extractedCount = 0;
  let skippedCount = 0;
  let totalEntriesSeen = 0;

  try {
    // Get the ZIP stream from S3
    console.log(`[Extract] Opening S3 stream for ZIP...`);
    const zipKey = `jobs/${jobId}/uploads/raw/documents.zip`;
    const zipStream = await getFromS3(zipKey);
    
    await saveJobLog(jobId, timestamp(), 'Connected to ZIP file, starting streaming extraction...');
    broadcastToJob(jobId, 'log', { 
      time: timestamp(), 
      message: 'Connected to ZIP file, starting streaming extraction...' 
    });

    if (job.updateProgress) {
      await job.updateProgress(5);
    }

    // Use unzipper.Parse() with async iteration for true streaming
    // Process one file at a time - read, upload, release memory
    const zip = zipStream.pipe(unzipper.Parse({ forceStream: true }));
    
    for await (const entry of zip) {
      totalEntriesSeen++;
      const filePath = entry.path;
      const fileName = filePath.split('/').pop().split('\\').pop();
      const type = entry.type;
      
      // Skip directories
      if (type === 'Directory') {
        entry.autodrain();
        skippedCount++;
        continue;
      }
      
      // Skip hidden/system files
      if (!fileName || fileName.startsWith('.') || fileName.startsWith('__') || 
          fileName === 'Thumbs.db' || fileName === '.DS_Store') {
        entry.autodrain();
        skippedCount++;
        continue;
      }
      
      const ext = fileName.toLowerCase().substring(fileName.lastIndexOf('.'));
      
      // Skip unsupported extensions
      if (!supportedExtensions.includes(ext)) {
        entry.autodrain();
        skippedCount++;
        continue;
      }
      
      try {
        // Read this single file into memory
        const chunks = [];
        for await (const chunk of entry) {
          chunks.push(chunk);
        }
        const content = Buffer.concat(chunks);
        const fileSizeMB = (content.length / 1024 / 1024).toFixed(2);
        
        // Upload immediately to S3
        const extractedKey = `jobs/${jobId}/uploads/extracted/${fileName}`;
        await uploadToS3(extractedKey, content);
        
        // Add to documents list (metadata only, not content)
        documents.push({
          name: fileName,
          key: extractedKey,
          type: ext,
          size: content.length,
          status: 'pending'
        });
        
        extractedCount++;
        
        // Log progress periodically
        if (extractedCount % 50 === 0 || extractedCount <= 5) {
          const logMsg = `Extracted ${extractedCount} documents (${fileName}, ${fileSizeMB}MB)...`;
          console.log(`[Extract] ${logMsg}`);
          await saveJobLog(jobId, timestamp(), logMsg);
          broadcastToJob(jobId, 'log', { time: timestamp(), message: logMsg });
          
          if (job.updateProgress) {
            await job.updateProgress(Math.min(5 + Math.floor(extractedCount / 10), 55));
          }
        }
        
        // Force garbage collection hint by nullifying
        chunks.length = 0;
        
      } catch (fileErr) {
        console.error(`[Extract] Failed to process ${fileName}:`, fileErr.message);
        skippedCount++;
      }
    }

    console.log(`[Extract] ========================================`);
    console.log(`[Extract] Extraction complete!`);
    console.log(`[Extract] Total entries seen: ${totalEntriesSeen}`);
    console.log(`[Extract] Extracted: ${extractedCount}`);
    console.log(`[Extract] Skipped: ${skippedCount}`);
    console.log(`[Extract] ========================================`);

    const completeMsg = `✓ Extraction complete. ${documents.length} documents found.`;
    await saveJobLog(jobId, timestamp(), completeMsg);
    broadcastToJob(jobId, 'log', { time: timestamp(), message: completeMsg });

    // Save queue
    const queueData = {
      totalDocuments: documents.length,
      processedCount: 0,
      documents,
      results: [],
      failedDocuments: [],
      status: 'ready'
    };
    
    await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);
    
    if (job.updateProgress) {
      await job.updateProgress(60);
    }
    
    updateJobStatus(jobId, {
      status: 'extracted',
      totalDocuments: documents.length,
      processedCount: 0
    });

    broadcastToJob(jobId, 'extraction-complete', { 
      totalDocuments: documents.length 
    });

    // Auto-start analysis immediately after extraction
    broadcastToJob(jobId, 'log', { 
      time: timestamp(), 
      message: 'Auto-starting document analysis...' 
    });
    
    await processAnalysis(jobId, job);
    
  } catch (error) {
    console.error(`[Extract] FATAL ERROR:`, error);
    broadcastToJob(jobId, 'log', { 
      time: timestamp(), 
      message: `❌ Extraction failed: ${error.message}` 
    });
    throw error;
  }
}

async function processAnalysis(jobId, job) {
  const timestamp = () => new Date().toISOString().split('T')[1].split('.')[0];
  
  // Load queue
  let queueData = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
  const pendingDocs = queueData.documents.filter(d => d.status === 'pending');
  
  if (pendingDocs.length === 0) {
    const msg = 'No pending documents to process.';
    await saveJobLog(jobId, timestamp(), msg);
    broadcastToJob(jobId, 'log', { time: timestamp(), message: msg });
    return;
  }

  const startMsg = `Starting analysis of ${pendingDocs.length} documents...`;
  await saveJobLog(jobId, timestamp(), startMsg);
  broadcastToJob(jobId, 'log', { time: timestamp(), message: startMsg });

  updateJobStatus(jobId, { status: 'processing' });

  for (let i = 0; i < pendingDocs.length; i++) {
    const doc = pendingDocs[i];
    const docIndex = queueData.documents.findIndex(d => d.key === doc.key);
    
    broadcastToJob(jobId, 'processing', {
      current: i + 1,
      total: pendingDocs.length,
      documentName: doc.name,
      status: 'analyzing'
    });

    const processingMsg = `[${i + 1}/${pendingDocs.length}] Processing: ${doc.name}`;
    await saveJobLog(jobId, timestamp(), processingMsg);
    broadcastToJob(jobId, 'log', { time: timestamp(), message: processingMsg });

    try {
      // Get document content
      const docStream = await getFromS3(doc.key);
      const docBuffer = await streamToBuffer(docStream);
      
      let documentContent;
      let mediaType = 'text';
      let pdfBuffer = null; // Keep original PDF buffer for large file processing

      if (doc.type === '.pdf') {
        const fileSize = docBuffer.length;
        const fileSizeMB = (fileSize / 1024 / 1024).toFixed(2);
        
        // Check if this is a large PDF
        if (fileSize > LARGE_FILE_THRESHOLD) {
          const largeMsg = `[${i + 1}/${pendingDocs.length}] 📄 Large PDF detected (${fileSizeMB}MB) - analyzing...`;
          await saveJobLog(jobId, timestamp(), largeMsg);
          broadcastToJob(jobId, 'log', { time: timestamp(), message: largeMsg });
          
          // Analyze PDF to determine strategy
          const analysis = await analyzePdf(docBuffer);
          
          const strategyMsg = `[${i + 1}/${pendingDocs.length}] Strategy: ${analysis.strategy} (${analysis.pageCount} pages, ${analysis.estimatedChunks} chunks)`;
          await saveJobLog(jobId, timestamp(), strategyMsg);
          broadcastToJob(jobId, 'log', { time: timestamp(), message: strategyMsg });
          
          // For large PDFs, we'll pass the buffer and let Claude service handle chunking
          pdfBuffer = docBuffer;
          documentContent = docBuffer.toString('base64');
          mediaType = 'pdf';
          
        } else {
          // Small PDF - use existing logic
          try {
            const pdfData = await pdf(docBuffer);
            if (pdfData.text && pdfData.text.trim().length > 100) {
              // Text-based PDF - use extracted text
              documentContent = pdfData.text;
              mediaType = 'text';
            } else {
              // Scanned PDF - send as PDF document to Claude
              documentContent = docBuffer.toString('base64');
              mediaType = 'pdf';
            }
          } catch (pdfErr) {
            // PDF parsing failed, send as PDF document
            documentContent = docBuffer.toString('base64');
            mediaType = 'pdf';
          }
        }
      } else if (doc.type === '.png') {
        documentContent = docBuffer.toString('base64');
        mediaType = 'image/png';
      } else if (doc.type === '.jpg' || doc.type === '.jpeg') {
        documentContent = docBuffer.toString('base64');
        mediaType = 'image/jpeg';
      } else if (doc.type === '.gif') {
        documentContent = docBuffer.toString('base64');
        mediaType = 'image/gif';
      } else if (doc.type === '.webp') {
        documentContent = docBuffer.toString('base64');
        mediaType = 'image/webp';
      } else {
        // For DOCX and others, try text extraction
        documentContent = docBuffer.toString('utf-8');
        mediaType = 'text';
      }

      // Progress callback for large PDF chunking
      const onProgress = (progress) => {
        broadcastToJob(jobId, 'log', { 
          time: timestamp(), 
          message: `[${i + 1}/${pendingDocs.length}] ${progress.message}` 
        });
        
        if (progress.phase === 'processing' && progress.total > 1) {
          broadcastToJob(jobId, 'chunk-progress', {
            document: doc.name,
            current: progress.current,
            total: progress.total,
            phase: progress.phase
          });
        }
      };

      // Analyze with Claude (with chunking support for large PDFs)
      const result = await analyzeDocument(documentContent, doc.name, mediaType, {
        onProgress: pdfBuffer ? onProgress : null,
        pdfBuffer: pdfBuffer
      });
      
      // Debug logging
      console.log(`[Analysis] Result for ${doc.name}:`, {
        success: result.success,
        hasData: !!result.data,
        confidenceScore: result.data?.confidence_score,
        riskRating: result.data?.risk_rating,
        error: result.error
      });
      
      // Check if we got a valid result with confidence score
      const hasValidConfidence = result.data?.confidence_score !== undefined && result.data?.confidence_score !== null;
      const confidenceScore = hasValidConfidence ? result.data.confidence_score : 0;
      
      if (result.success && confidenceScore > 0) {
        result.data.document_name = doc.name;
        result.data.processed_at = new Date().toISOString();
        
        // Add chunking metadata if applicable
        if (result.chunksProcessed) {
          result.data._processing_strategy = result.strategy;
          result.data._chunks_processed = result.chunksProcessed;
        }
        
        // Add token usage to result
        if (result.tokenDetails) {
          result.data._tokens_input = result.tokenDetails.input;
          result.data._tokens_output = result.tokenDetails.output;
        }
        
        queueData.results.push(result.data);
        queueData.documents[docIndex].status = 'completed';
        
        // Track cumulative token usage
        queueData.totalTokensInput = (queueData.totalTokensInput || 0) + (result.tokenDetails?.input || 0);
        queueData.totalTokensOutput = (queueData.totalTokensOutput || 0) + (result.tokenDetails?.output || 0);
        
        const riskEmoji = result.data.risk_rating === 'High' ? '🔴' : 
                          result.data.risk_rating === 'Medium' ? '🟡' : '🟢';
        
        // Build completion message with token info
        const tokensIn = result.tokenDetails?.input || 0;
        const tokensOut = result.tokenDetails?.output || 0;
        const tokenInfo = tokensIn > 0 ? ` | Tokens: ${tokensIn.toLocaleString()}↓ ${tokensOut.toLocaleString()}↑` : '';
        
        let completionMsg = `[${i + 1}/${pendingDocs.length}] ✓ Complete - Risk: ${result.data.risk_rating} ${riskEmoji}${tokenInfo}`;
        if (result.chunksProcessed && result.chunksProcessed > 1) {
          completionMsg += ` (${result.chunksProcessed} chunks)`;
        }
        
        await saveJobLog(jobId, timestamp(), completionMsg, 'success');
        broadcastToJob(jobId, 'log', { time: timestamp(), message: completionMsg });
        
        // Broadcast token usage update
        broadcastToJob(jobId, 'tokens', {
          document: doc.name,
          input: tokensIn,
          output: tokensOut,
          totalInput: queueData.totalTokensInput,
          totalOutput: queueData.totalTokensOutput
        });
        
      } else {
        // Log why it's being marked for manual review
        const reason = !result.success 
          ? (result.error || 'API call failed')
          : (confidenceScore === 0 ? 'Zero confidence score' : 'Missing confidence score');
        
        console.log(`[Analysis] Manual review for ${doc.name}: ${reason}`);
        
        queueData.documents[docIndex].status = 'failed';
        queueData.failedDocuments.push({
          name: doc.name,
          reason: reason
        });
        
        // Still add to results for manual review (if we have data)
        if (result.data) {
          result.data.document_name = doc.name;
          result.data.processed_at = new Date().toISOString();
          queueData.results.push(result.data);
        }
        
        const warnMsg = `[${i + 1}/${pendingDocs.length}] ⚠️ Manual review: ${doc.name} - ${reason}`;
        await saveJobLog(jobId, timestamp(), warnMsg, 'warning');
        broadcastToJob(jobId, 'log', { time: timestamp(), message: warnMsg });
      }

      queueData.processedCount = i + 1;
      
      // Update job progress to prevent stalling (60-95% for analysis)
      const analysisProgress = 60 + Math.round((i + 1) / pendingDocs.length * 35);
      if (job.updateProgress) {
        await job.updateProgress(analysisProgress);
      }
      
      // Update progress
      broadcastToJob(jobId, 'progress', {
        current: i + 1,
        total: pendingDocs.length,
        percentage: Math.round((i + 1) / pendingDocs.length * 100)
      });

      // Save progress periodically (every 5 documents or after large file)
      if ((i + 1) % 5 === 0 || i === pendingDocs.length - 1 || pdfBuffer) {
        await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);
        await flushJobLogs(jobId); // Also flush logs
        console.log(`[Analysis] Progress saved: ${i + 1}/${pendingDocs.length} documents. Total tokens: ${(queueData.totalTokensInput || 0).toLocaleString()}↓ ${(queueData.totalTokensOutput || 0).toLocaleString()}↑`);
      }

      // Adaptive rate limiting based on document type and size
      // Large PDFs already have delays built into chunk processing
      // For regular documents, add delay to avoid rate limits
      let waitTime = 1000; // Base 1 second between documents
      if (pdfBuffer) {
        waitTime = 2000; // 2 seconds after large file processing
      } else if (doc.size > 5 * 1024 * 1024) {
        waitTime = 1500; // 1.5 seconds for medium files
      }
      
      await new Promise(resolve => setTimeout(resolve, waitTime));
      
    } catch (docError) {
      console.error(`Error processing ${doc.name}:`, docError);
      
      // Check if it's a rate limit error that exhausted retries
      const isRateLimit = docError.status === 429 || docError.message?.includes('Rate limit');
      
      queueData.documents[docIndex].status = 'failed';
      queueData.failedDocuments.push({
        name: doc.name,
        reason: isRateLimit ? 'Rate limit exceeded after retries' : docError.message
      });
      
      const errMsg = `[${i + 1}/${pendingDocs.length}] ❌ Error: ${doc.name} - ${docError.message}`;
      await saveJobLog(jobId, timestamp(), errMsg, 'error');
      broadcastToJob(jobId, 'log', { time: timestamp(), message: errMsg });
      
      // If rate limited, add extra delay before next document
      if (isRateLimit) {
        const extraDelay = 30000; // 30 seconds
        console.log(`[Analysis] Rate limit hit, waiting ${extraDelay / 1000}s before next document...`);
        await saveJobLog(jobId, timestamp(), `Rate limit hit, waiting 30s before continuing...`, 'warning');
        broadcastToJob(jobId, 'log', { time: timestamp(), message: `⏳ Rate limit hit, waiting 30s before continuing...` });
        await new Promise(resolve => setTimeout(resolve, extraDelay));
      }
    }
  }

  // Final save and flush logs
  queueData.status = 'analysis-complete';
  await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);
  
  // Calculate total tokens
  const totalIn = queueData.totalTokensInput || 0;
  const totalOut = queueData.totalTokensOutput || 0;
  const totalTokens = totalIn + totalOut;
  
  const completeMsg = `✓ Analysis complete. ${queueData.results.length} processed, ${queueData.failedDocuments.length} flagged for review.`;
  const tokenSummary = `📊 Total tokens used: ${totalTokens.toLocaleString()} (${totalIn.toLocaleString()}↓ input, ${totalOut.toLocaleString()}↑ output)`;
  
  await saveJobLog(jobId, timestamp(), completeMsg, 'success');
  await saveJobLog(jobId, timestamp(), tokenSummary, 'info');
  await finalizeJobLogs(jobId);
  
  updateJobStatus(jobId, {
    status: 'analysis-complete',
    processedCount: queueData.processedCount,
    failedCount: queueData.failedDocuments.length,
    totalTokensInput: totalIn,
    totalTokensOutput: totalOut
  });

  broadcastToJob(jobId, 'log', { 
    time: timestamp(), 
    message: completeMsg 
  });
  
  broadcastToJob(jobId, 'log', { 
    time: timestamp(), 
    message: tokenSummary 
  });

  broadcastToJob(jobId, 'analysis-complete', {
    processed: queueData.results.length,
    failed: queueData.failedDocuments.length,
    totalTokensInput: totalIn,
    totalTokensOutput: totalOut
  });

  // Auto-start report generation
  const autoStartMsg = 'Auto-starting report generation...';
  await saveJobLog(jobId, timestamp(), autoStartMsg);
  broadcastToJob(jobId, 'log', { time: timestamp(), message: autoStartMsg });
  
  await processReportGeneration(jobId, job);
}

async function processReportGeneration(jobId, job) {
  const timestamp = () => new Date().toISOString().split('T')[1].split('.')[0];
  
  const genMsg = 'Generating Excel report...';
  await saveJobLog(jobId, timestamp(), genMsg);
  broadcastToJob(jobId, 'log', { time: timestamp(), message: genMsg });

  updateJobStatus(jobId, { status: 'generating-report' });

  // Load results
  const queueData = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
  
  // Generate report
  const reportKey = await generateAuditReport(
    jobId, 
    queueData.results, 
    queueData.failedDocuments
  );

  const completedAt = new Date().toISOString();
  
  queueData.status = 'completed';
  queueData.reportKey = reportKey;
  queueData.completedAt = completedAt;
  await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);

  // Also update the job metadata with completedAt
  try {
    const metadata = await getJsonFromS3(`jobs/${jobId}/metadata.json`);
    metadata.status = 'completed';
    metadata.completedAt = completedAt;
    metadata.reportKey = reportKey;
    await putJsonToS3(`jobs/${jobId}/metadata.json`, metadata);
  } catch (err) {
    console.error('[Report] Failed to update metadata:', err.message);
  }

  updateJobStatus(jobId, {
    status: 'completed',
    reportKey,
    completedAt
  });

  const successMsg = '✓ Report generated successfully!';
  await saveJobLog(jobId, timestamp(), successMsg, 'success');
  
  const allCompleteMsg = '✓ All processing complete!';
  await saveJobLog(jobId, timestamp(), allCompleteMsg, 'success');
  await finalizeJobLogs(jobId);
  
  broadcastToJob(jobId, 'log', { time: timestamp(), message: successMsg });
  broadcastToJob(jobId, 'log', { time: timestamp(), message: allCompleteMsg });

  broadcastToJob(jobId, 'complete', {
    reportKey,
    completedAt,
    stats: {
      total: queueData.results.length,
      high: queueData.results.filter(r => r.risk_rating === 'High').length,
      medium: queueData.results.filter(r => r.risk_rating === 'Medium').length,
      low: queueData.results.filter(r => r.risk_rating === 'Low').length,
      manualReview: queueData.failedDocuments.length
    }
  });
}

export { worker };
// Start the worker process
console.log('[Worker] Starting worker process...');
initializeWorker()
  .then(() => {
    console.log('[Worker] Worker started successfully - waiting for jobs');
  })
  .catch((err) => {
    console.error('[Worker] Failed to start:', err);
    process.exit(1);
  });