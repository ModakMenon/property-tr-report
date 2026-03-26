const API_URL = import.meta.env.VITE_API_URL || '';

async function fetchApi(endpoint, options = {}) {
  const token = localStorage.getItem('token');
  const headers = { ...options.headers };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  if (!(options.body instanceof FormData)) headers['Content-Type'] = 'application/json';
  const response = await fetch(`${API_URL}${endpoint}`, { ...options, headers });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Request failed' }));
    throw new Error(error.error || 'Request failed');
  }
  return response.json();
}

export const authApi = {
  login: (email, name) => fetchApi('/api/auth/login', { method: 'POST', body: JSON.stringify({ email, name }) }),
  verify: () => fetchApi('/api/auth/verify'),
  logout: () => { localStorage.removeItem('token'); localStorage.removeItem('user'); return Promise.resolve(); }
};

export const mastersApi = {
  getPrompt: () => fetchApi('/api/masters/prompt'),
  updatePrompt: (promptData) => fetchApi('/api/masters/prompt', { method: 'PUT', body: JSON.stringify(promptData) })
};

export const jobsApi = {
  create: (userEmail) => fetchApi('/api/jobs/create', { method: 'POST', body: JSON.stringify({ userEmail }) }),

  uploadZip: async (jobId, file, onProgress) => {
    const PART_SIZE = 10 * 1024 * 1024; // 10MB per part
    const partCount = Math.ceil(file.size / PART_SIZE);

    // Step 1 — tell backend to initiate multipart upload on S3
    // Backend returns: uploadId, s3Key, partUrls (one presigned PUT URL per part)
    const { uploadId, s3Key, partUrls } = await fetchApi(`/api/jobs/${jobId}/presign-upload`, {
      method: 'POST',
      body: JSON.stringify({
        fileName: file.name,
        contentType: file.type || 'application/zip',
        partCount,
      })
    });

    // Step 2 — upload each 10MB chunk directly to S3 using its presigned URL
    // If a part fails, it retries up to 3 times before giving up
    const parts = [];
    let uploadedBytes = 0;

    for (let i = 0; i < partCount; i++) {
      const start = i * PART_SIZE;
      const end = Math.min(start + PART_SIZE, file.size);
      const chunk = file.slice(start, end);

      let attempts = 0;
      let etag;

      while (attempts < 3) {
        try {
          const res = await fetch(partUrls[i], {
            method: 'PUT',
            body: chunk,
            headers: { 'Content-Type': file.type || 'application/zip' },
          });
          if (!res.ok) throw new Error(`Part ${i + 1} failed with status ${res.status}`);
          etag = res.headers.get('ETag');
          break; // success — move to next part
        } catch (err) {
          attempts++;
          if (attempts === 3) {
            // 3 strikes — abort cleanly so S3 doesn't keep incomplete parts
            await fetchApi(`/api/jobs/${jobId}/abort-multipart`, {
              method: 'POST',
              body: JSON.stringify({ s3Key, uploadId }),
            }).catch(() => {}); // best-effort abort
            throw new Error(`Upload failed after 3 attempts on part ${i + 1}: ${err.message}`);
          }
          // wait before retry: 2s after 1st fail, 4s after 2nd
          await new Promise(r => setTimeout(r, 2000 * attempts));
        }
      }

      parts.push({ PartNumber: i + 1, ETag: etag });
      uploadedBytes += (end - start);
      if (onProgress) onProgress(Math.round((uploadedBytes / file.size) * 100));
    }

    // Step 3 — tell S3 to assemble all parts into one file
    await fetchApi(`/api/jobs/${jobId}/complete-multipart`, {
      method: 'POST',
      body: JSON.stringify({ s3Key, uploadId, parts }),
    });

    // Step 4 — tell backend upload is confirmed so it can enqueue the job
    return fetchApi(`/api/jobs/${jobId}/confirm-upload`, {
      method: 'POST',
      body: JSON.stringify({ s3Key, fileName: file.name, fileSize: file.size }),
    });
  },

  uploadPdf: async (jobId, file, onProgress) => {
    const formData = new FormData();
    formData.append('file', file);
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.upload.addEventListener('progress', (event) => {
        if (event.lengthComputable && onProgress) onProgress(Math.round((event.loaded / event.total) * 100));
      });
      xhr.addEventListener('load', () => {
        if (xhr.status >= 200 && xhr.status < 300) resolve(JSON.parse(xhr.responseText));
        else { try { const error = JSON.parse(xhr.responseText); reject(new Error(error.error || 'Upload failed')); } catch { reject(new Error('Upload failed')); } }
      });
      xhr.addEventListener('error', () => reject(new Error('Upload failed')));
      xhr.open('POST', `${API_URL}/api/jobs/${jobId}/upload-pdf`);
      const token = localStorage.getItem('token');
      if (token) xhr.setRequestHeader('Authorization', `Bearer ${token}`);
      xhr.send(formData);
    });
  },

  analyzePdf: (jobId) => fetchApi(`/api/jobs/${jobId}/analyze-pdf`),
  uploadDocuments: async (jobId, files) => { const formData = new FormData(); files.forEach(file => formData.append('files', file)); return fetchApi(`/api/jobs/${jobId}/upload-documents`, { method: 'POST', body: formData }); },
  startExtraction: (jobId) => fetchApi(`/api/jobs/${jobId}/extract`, { method: 'POST' }),
  startAnalysis: (jobId) => fetchApi(`/api/jobs/${jobId}/analyze`, { method: 'POST' }),
  generateReport: (jobId) => fetchApi(`/api/jobs/${jobId}/generate-report`, { method: 'POST' }),
  getStatus: (jobId) => fetchApi(`/api/jobs/${jobId}/status`),
  getDownloadUrl: (jobId) => fetchApi(`/api/jobs/${jobId}/download`),
  getLogs: (jobId) => fetchApi(`/api/jobs/${jobId}/logs`),
  resumeJob: (jobId) => fetchApi(`/api/jobs/${jobId}/resume`, { method: 'POST' }),
  list: () => fetchApi('/api/jobs'),

  subscribeToEvents: (jobId, handlers) => {
    const eventSource = new EventSource(`${API_URL}/api/jobs/${jobId}/events`);
    eventSource.addEventListener('connected', (e) => handlers.onConnected?.(JSON.parse(e.data)));
    eventSource.addEventListener('log', (e) => handlers.onLog?.(JSON.parse(e.data)));
    eventSource.addEventListener('progress', (e) => handlers.onProgress?.(JSON.parse(e.data)));
    eventSource.addEventListener('processing', (e) => handlers.onProcessing?.(JSON.parse(e.data)));
    eventSource.addEventListener('chunk-progress', (e) => handlers.onChunkProgress?.(JSON.parse(e.data)));
    eventSource.addEventListener('tokens', (e) => handlers.onTokens?.(JSON.parse(e.data)));
    eventSource.addEventListener('extraction-complete', (e) => handlers.onExtractionComplete?.(JSON.parse(e.data)));
    eventSource.addEventListener('analysis-complete', (e) => handlers.onAnalysisComplete?.(JSON.parse(e.data)));
    eventSource.addEventListener('complete', (e) => handlers.onComplete?.(JSON.parse(e.data)));
    eventSource.addEventListener('error', (e) => handlers.onError?.(e));
    eventSource.onerror = () => handlers.onError?.({ message: 'Connection lost' });
    return () => eventSource.close();
  }
};