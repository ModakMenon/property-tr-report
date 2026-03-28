import { Worker } from 'bullmq';
import unzipper from 'unzipper';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
import { getWorkerConnection, isUsingInMemoryQueue, broadcastToJob, updateJobStatus, getJobStatus, registerJobHandler, documentQueue } from '../services/queueService.js';
import { getFromS3, uploadToS3, getJsonFromS3, putJsonToS3, streamToBuffer, listS3Objects } from '../services/s3Service.js';
import { generateAuditReport } from '../services/excelService.js';

const lambdaClient = new LambdaClient({ region: process.env.AWS_REGION || 'ap-south-1' });
const LAMBDA_FUNCTION_NAME = process.env.LAMBDA_FUNCTION_NAME || 'legal-audit-document-processor';
const LAMBDA_BATCH_SIZE = 10;   // ← reduced from 50 to 10 to avoid Bedrock rate limits
const BATCH_DELAY_MS   = 8000;  // ← 8 second pause between batches (gives Bedrock time to recover)
const POLL_INTERVAL    = 5000;

let worker = null;

/* ------------------------------------------------ */
/* LOG HELPERS                                       */
/* ------------------------------------------------ */

const logCache = new Map();

async function saveJobLog(jobId, time, message, type = 'info') {
  if (!logCache.has(jobId)) logCache.set(jobId, []);
  logCache.get(jobId).push({ time, message, type, timestamp: Date.now() });
  if (logCache.get(jobId).length >= 20) await flushJobLogs(jobId);
}

async function flushJobLogs(jobId) {
  const logs = logCache.get(jobId);
  if (!logs || logs.length === 0) return;
  try {
    let existingLogs = [];
    try {
      const existing = await getJsonFromS3(`jobs/${jobId}/processing/logs.json`);
      existingLogs = existing.logs || [];
    } catch { }
    const allLogs = [...existingLogs, ...logs].slice(-1000);
    await putJsonToS3(`jobs/${jobId}/processing/logs.json`, { logs: allLogs, lastUpdated: new Date().toISOString() });
    logCache.set(jobId, []);
  } catch (err) {
    console.error(`[Logs] Failed to flush:`, err.message);
  }
}

async function finalizeJobLogs(jobId) {
  await flushJobLogs(jobId);
}

/* ------------------------------------------------ */
/* JOB PROCESSOR                                     */
/* ------------------------------------------------ */

async function processJob(job) {
  const { jobId, type } = job.data;
  console.log(`[Worker] Processing job: ${job.id}, type: ${type}, jobId: ${jobId}`);
  try {
    switch (type) {
      case 'extract': await processExtraction(jobId, job); break;
      case 'analyze': await processAnalysis(jobId, job); break;
      case 'generate-report': await processReportGeneration(jobId, job); break;
      default: throw new Error(`Unknown job type: ${type}`);
    }
  } catch (error) {
    console.error(`[Worker] Job ${job.id} failed:`, error);
    broadcastToJob(jobId, 'error', { message: error.message });
    throw error;
  }
}

/* ------------------------------------------------ */
/* EXTRACTION (unchanged)                            */
/* ------------------------------------------------ */

async function processExtraction(jobId, job) {
  const timestamp = () => new Date().toISOString().split('T')[1].split('.')[0];

  await saveJobLog(jobId, timestamp(), 'Starting ZIP extraction (streaming mode)...');
  broadcastToJob(jobId, 'log', { time: timestamp(), message: 'Starting ZIP extraction (streaming mode)...' });

  const supportedExtensions = ['.pdf', '.png', '.jpg', '.jpeg', '.docx'];
  const documents = [];
  let extractedCount = 0;
  let skippedCount = 0;

  try {
    const zipKey = `jobs/${jobId}/uploads/raw/documents.zip`;
    const zipStream = await getFromS3(zipKey);

    await saveJobLog(jobId, timestamp(), 'Connected to ZIP file, starting streaming extraction...');
    broadcastToJob(jobId, 'log', { time: timestamp(), message: 'Connected to ZIP file, starting streaming extraction...' });

    if (job.updateProgress) await job.updateProgress(5);

    const zip = zipStream.pipe(unzipper.Parse({ forceStream: true }));

    for await (const entry of zip) {
      const filePath = entry.path;
      const fileName = filePath.split('/').pop().split('\\').pop();
      const type = entry.type;

      if (type === 'Directory') { entry.autodrain(); skippedCount++; continue; }
      if (!fileName || fileName.startsWith('.') || fileName.startsWith('__') || fileName === 'Thumbs.db') { entry.autodrain(); skippedCount++; continue; }

      const ext = fileName.toLowerCase().substring(fileName.lastIndexOf('.'));
      if (!supportedExtensions.includes(ext)) { entry.autodrain(); skippedCount++; continue; }

      try {
        const chunks = [];
        for await (const chunk of entry) chunks.push(chunk);
        const content = Buffer.concat(chunks);
        const fileSizeMB = (content.length / 1024 / 1024).toFixed(2);

        const extractedKey = `jobs/${jobId}/uploads/extracted/${fileName}`;
        await uploadToS3(extractedKey, content);

        documents.push({ name: fileName, key: extractedKey, type: ext, size: content.length, status: 'pending' });
        extractedCount++;

        if (extractedCount % 50 === 0 || extractedCount <= 5) {
          const logMsg = `Extracted ${extractedCount} documents (${fileName}, ${fileSizeMB}MB)...`;
          await saveJobLog(jobId, timestamp(), logMsg);
          broadcastToJob(jobId, 'log', { time: timestamp(), message: logMsg });
          if (job.updateProgress) await job.updateProgress(Math.min(5 + Math.floor(extractedCount / 10), 55));
        }

        chunks.length = 0;
      } catch (fileErr) {
        console.error(`[Extract] Failed to process ${fileName}:`, fileErr.message);
        skippedCount++;
      }
    }

    const completeMsg = `✓ Extraction complete. ${documents.length} documents found.`;
    await saveJobLog(jobId, timestamp(), completeMsg);
    broadcastToJob(jobId, 'log', { time: timestamp(), message: completeMsg });

    const queueData = {
      totalDocuments: documents.length,
      processedCount: 0,
      documents,
      results: [],
      failedDocuments: [],
      status: 'ready'
    };

    await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);
    if (job.updateProgress) await job.updateProgress(60);

    updateJobStatus(jobId, { status: 'extracted', totalDocuments: documents.length, processedCount: 0 });
    broadcastToJob(jobId, 'extraction-complete', { totalDocuments: documents.length });

    broadcastToJob(jobId, 'log', { time: timestamp(), message: 'Auto-starting document analysis...' });
    await processAnalysis(jobId, job);

  } catch (error) {
    console.error(`[Extract] FATAL ERROR:`, error);
    broadcastToJob(jobId, 'log', { time: timestamp(), message: `❌ Extraction failed: ${error.message}` });
    throw error;
  }
}

/* ------------------------------------------------ */
/* ANALYSIS - LAMBDA WITH RATE LIMIT PROTECTION     */
/* ------------------------------------------------ */

async function invokeLambdaWithRetry(jobId, document, documentIndex, totalDocuments, maxRetries = 3) {
  const payload = JSON.stringify({ jobId, document, documentIndex, totalDocuments });

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const command = new InvokeCommand({
        FunctionName: LAMBDA_FUNCTION_NAME,
        InvocationType: 'RequestResponse',
        Payload: Buffer.from(payload)
      });

      const response = await lambdaClient.send(command);
      const responsePayload = JSON.parse(Buffer.from(response.Payload).toString());

      // Check if Bedrock throttled inside Lambda
      if (response.FunctionError) {
        const errMsg = JSON.stringify(responsePayload);
        const isThrottle = errMsg.includes('ThrottlingException') ||
                           errMsg.includes('Rate exceeded') ||
                           errMsg.includes('Too Many Requests');

        if (isThrottle && attempt < maxRetries) {
          const waitMs = 15000 * attempt; // 15s, 30s
          console.warn(`[Worker] Bedrock throttle on ${document.name}, waiting ${waitMs}ms before retry ${attempt + 1}/${maxRetries}`);
          await new Promise(r => setTimeout(r, waitMs));
          continue;
        }

        console.error(`[Worker] Lambda error for ${document.name}:`, errMsg);
      } else {
        console.log(`[Worker] Lambda completed: ${document.name} (${documentIndex + 1}/${totalDocuments})`);
      }

      return responsePayload;

    } catch (error) {
      if (attempt < maxRetries) {
        const waitMs = 10000 * attempt;
        console.warn(`[Worker] Lambda invoke failed for ${document.name}, retry ${attempt + 1}/${maxRetries} in ${waitMs}ms:`, error.message);
        await new Promise(r => setTimeout(r, waitMs));
      } else {
        console.error(`[Worker] Lambda failed after ${maxRetries} attempts for ${document.name}:`, error.message);
        throw error;
      }
    }
  }
}

async function waitForResults(jobId, totalDocuments) {
  const timestamp = () => new Date().toISOString().split('T')[1].split('.')[0];
  let lastCompleted = 0;

  console.log(`[Worker] Waiting for ${totalDocuments} Lambda results...`);

  while (true) {
    let completedCount = 0;
    try {
      const objects = await listS3Objects(`jobs/${jobId}/processing/results/`);
      completedCount = objects.length;
    } catch {
      completedCount = 0;
    }

    if (completedCount > lastCompleted) {
      const message = `[${completedCount}/${totalDocuments}] Documents processed by Lambda...`;
      await saveJobLog(jobId, timestamp(), message);
      broadcastToJob(jobId, 'log', { time: timestamp(), message });
      broadcastToJob(jobId, 'progress', {
        current: completedCount,
        total: totalDocuments,
        percentage: Math.round((completedCount / totalDocuments) * 100)
      });
      lastCompleted = completedCount;
    }

    if (completedCount >= totalDocuments) {
      console.log(`[Worker] All ${totalDocuments} Lambda results received!`);
      break;
    }

    await new Promise(r => setTimeout(r, POLL_INTERVAL));
  }
}

async function collectResults(jobId, documents) {
  const results = [];
  const failedDocuments = [];
  let totalInput = 0;
  let totalOutput = 0;

  for (let i = 0; i < documents.length; i++) {
    try {
      const resultData = await getJsonFromS3(`jobs/${jobId}/processing/results/${i}.json`);

      if (resultData.status === 'completed' && resultData.data) {
        results.push(resultData.data);
        totalInput += resultData.tokenDetails?.input || 0;
        totalOutput += resultData.tokenDetails?.output || 0;
      } else {
        failedDocuments.push({
          name: resultData.documentName || documents[i].name,
          reason: resultData.reason || 'Processing failed'
        });
        if (resultData.data) results.push(resultData.data);
      }
    } catch {
      failedDocuments.push({ name: documents[i].name, reason: 'Result file missing' });
    }
  }

  return { results, failedDocuments, totalInput, totalOutput };
}

async function processAnalysis(jobId, job) {
  const timestamp = () => new Date().toISOString().split('T')[1].split('.')[0];

  let queueData = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
  const pendingDocs = queueData.documents.filter(d => d.status === 'pending');

  if (pendingDocs.length === 0) {
    broadcastToJob(jobId, 'log', { time: timestamp(), message: 'No pending documents to process.' });
    return;
  }

  const totalBatches = Math.ceil(pendingDocs.length / LAMBDA_BATCH_SIZE);
  const startMsg = `Starting parallel Lambda analysis of ${pendingDocs.length} documents in ${totalBatches} batches of ${LAMBDA_BATCH_SIZE}...`;
  await saveJobLog(jobId, timestamp(), startMsg);
  broadcastToJob(jobId, 'log', { time: timestamp(), message: startMsg });
  updateJobStatus(jobId, { status: 'processing' });

  // ── Fire Lambdas in small batches with a delay between each ──
  for (let batchStart = 0; batchStart < pendingDocs.length; batchStart += LAMBDA_BATCH_SIZE) {
    const batch = pendingDocs.slice(batchStart, batchStart + LAMBDA_BATCH_SIZE);
    const batchEnd = Math.min(batchStart + LAMBDA_BATCH_SIZE, pendingDocs.length);
    const batchNum = Math.floor(batchStart / LAMBDA_BATCH_SIZE) + 1;

    const batchMsg = `Invoking Lambda batch ${batchNum}/${totalBatches} — docs ${batchStart + 1}–${batchEnd} of ${pendingDocs.length}`;
    await saveJobLog(jobId, timestamp(), batchMsg);
    broadcastToJob(jobId, 'log', { time: timestamp(), message: batchMsg });

    // Invoke this batch in parallel — each with built-in retry on throttle
    const invokePromises = batch.map((doc, idx) =>
      invokeLambdaWithRetry(jobId, doc, batchStart + idx, pendingDocs.length)
    );

    // Wait for ALL invocations in this batch before moving to the next
    const batchResults = await Promise.allSettled(invokePromises);

    const batchFailed = batchResults.filter(r => r.status === 'rejected').length;
    const invokedMsg = batchFailed > 0
      ? `⚠️ Batch ${batchNum} done — ${batch.length - batchFailed} succeeded, ${batchFailed} failed`
      : `✓ Batch ${batchNum}/${totalBatches} complete (${batch.length} docs)`;

    await saveJobLog(jobId, timestamp(), invokedMsg, batchFailed > 0 ? 'warning' : 'success');
    broadcastToJob(jobId, 'log', { time: timestamp(), message: invokedMsg });

    // ── RATE LIMIT GUARD: pause between batches so Bedrock doesn't throttle ──
    if (batchEnd < pendingDocs.length) {
      const delayMsg = `Pausing ${BATCH_DELAY_MS / 1000}s before next batch to avoid Bedrock rate limits...`;
      await saveJobLog(jobId, timestamp(), delayMsg);
      broadcastToJob(jobId, 'log', { time: timestamp(), message: delayMsg });
      await new Promise(r => setTimeout(r, BATCH_DELAY_MS));
    }
  }

  const waitMsg = `All Lambda batches fired! Waiting for all ${pendingDocs.length} results...`;
  await saveJobLog(jobId, timestamp(), waitMsg);
  broadcastToJob(jobId, 'log', { time: timestamp(), message: waitMsg });

  await waitForResults(jobId, pendingDocs.length);

  const { results, failedDocuments, totalInput, totalOutput } = await collectResults(jobId, pendingDocs);

  queueData.results = results;
  queueData.failedDocuments = failedDocuments;
  queueData.processedCount = pendingDocs.length;
  queueData.totalTokensInput = totalInput;
  queueData.totalTokensOutput = totalOutput;
  queueData.status = 'analysis-complete';

  queueData.documents = queueData.documents.map(doc => ({
    ...doc,
    status: failedDocuments.find(f => f.name === doc.name) ? 'failed' : 'completed'
  }));

  await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);

  const totalTokens = totalInput + totalOutput;
  const completeMsg = `✓ Analysis complete. ${results.length} processed, ${failedDocuments.length} flagged for review.`;
  const tokenMsg = `📊 Total tokens used: ${totalTokens.toLocaleString()} (${totalInput.toLocaleString()}↓ ${totalOutput.toLocaleString()}↑)`;

  await saveJobLog(jobId, timestamp(), completeMsg, 'success');
  await saveJobLog(jobId, timestamp(), tokenMsg, 'info');
  await finalizeJobLogs(jobId);

  updateJobStatus(jobId, {
    status: 'analysis-complete',
    processedCount: queueData.processedCount,
    failedCount: failedDocuments.length,
    totalTokensInput: totalInput,
    totalTokensOutput: totalOutput
  });

  broadcastToJob(jobId, 'log', { time: timestamp(), message: completeMsg });
  broadcastToJob(jobId, 'log', { time: timestamp(), message: tokenMsg });
  broadcastToJob(jobId, 'analysis-complete', {
    processed: results.length,
    failed: failedDocuments.length,
    totalTokensInput: totalInput,
    totalTokensOutput: totalOutput
  });

  broadcastToJob(jobId, 'log', { time: timestamp(), message: 'Auto-starting report generation...' });
  await processReportGeneration(jobId, job);
}

/* ------------------------------------------------ */
/* REPORT GENERATION (unchanged)                     */
/* ------------------------------------------------ */

async function processReportGeneration(jobId, job) {
  const timestamp = () => new Date().toISOString().split('T')[1].split('.')[0];

  broadcastToJob(jobId, 'log', { time: timestamp(), message: 'Generating Excel report...' });
  updateJobStatus(jobId, { status: 'generating-report' });

  const queueData = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);

  const reportKey = await generateAuditReport(jobId, queueData.results, queueData.failedDocuments);

  const completedAt = new Date().toISOString();

  queueData.status = 'completed';
  queueData.reportKey = reportKey;
  queueData.completedAt = completedAt;
  await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);

  try {
    const metadata = await getJsonFromS3(`jobs/${jobId}/metadata.json`);
    metadata.status = 'completed';
    metadata.completedAt = completedAt;
    metadata.reportKey = reportKey;
    await putJsonToS3(`jobs/${jobId}/metadata.json`, metadata);
  } catch (err) {
    console.error('[Report] Failed to update metadata:', err.message);
  }

  updateJobStatus(jobId, { status: 'completed', reportKey, completedAt });

  await saveJobLog(jobId, timestamp(), '✓ Report generated successfully!', 'success');
  await saveJobLog(jobId, timestamp(), '✓ All processing complete!', 'success');
  await finalizeJobLogs(jobId);

  broadcastToJob(jobId, 'log', { time: timestamp(), message: '✓ Report generated successfully!' });
  broadcastToJob(jobId, 'log', { time: timestamp(), message: '✓ All processing complete!' });
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

/* ------------------------------------------------ */
/* WORKER INITIALIZATION                             */
/* ------------------------------------------------ */

export async function initializeWorker() {
  registerJobHandler('document-processing', processJob);

  if (isUsingInMemoryQueue()) {
    console.log('✓ Using in-memory job processing');
    return null;
  }

  const workerConnection = getWorkerConnection();

  if (!workerConnection) {
    console.log('✓ Using in-memory job processing (no worker connection)');
    return null;
  }

  try {
    await new Promise((resolve, reject) => {
      if (workerConnection.status === 'ready') { resolve(); return; }
      const timeout = setTimeout(() => reject(new Error('Worker connection timeout')), 15000);
      workerConnection.once('ready', () => { clearTimeout(timeout); resolve(); });
      workerConnection.once('error', (err) => { clearTimeout(timeout); reject(err); });
    });

    console.log('✓ Redis worker connection ready');

    worker = new Worker('document-processing', processJob, {
      connection: workerConnection,
      concurrency: 1,
      lockDuration: 1800000,
      stalledInterval: 600000,
      maxStalledCount: 5,
      lockRenewTime: 300000,
    });

    worker.on('completed', (job) => console.log(`[Worker] Job ${job.id} completed successfully`));
    worker.on('failed', (job, err) => console.error(`[Worker] Job ${job?.id} failed:`, err.message));
    worker.on('stalled', (jobId) => console.warn(`[Worker] Job ${jobId} stalled - will retry`));
    worker.on('error', (err) => console.error(`[Worker] Worker error:`, err.message));

    console.log(`✓ BullMQ worker initialized — batch size: ${LAMBDA_BATCH_SIZE}, batch delay: ${BATCH_DELAY_MS}ms`);
    return worker;

  } catch (err) {
    console.warn('⚠️  BullMQ worker failed to initialize:', err.message);
    return null;
  }
}

export { worker };

console.log('[Worker] Starting worker process...');
initializeWorker()
  .then(() => console.log('[Worker] Worker started successfully - waiting for jobs'))
  .catch((err) => { console.error('[Worker] Failed to start:', err); process.exit(1); });

process.on('SIGTERM', () => { console.log('[Worker] Received SIGTERM, shutting down gracefully...'); process.exit(0); });
process.on('SIGINT', () => { console.log('[Worker] Received SIGINT, shutting down gracefully...'); process.exit(0); });
setInterval(() => {}, 1000 * 60 * 60);