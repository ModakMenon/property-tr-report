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
    const { uploadUrl, s3Key } = await fetchApi(`/api/jobs/${jobId}/presign-upload`, {
      method: 'POST',
      body: JSON.stringify({ fileName: file.name, contentType: file.type || 'application/zip' })
    });

    const MAX_RETRIES = 3;
    let lastError;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        await new Promise((resolve, reject) => {
          const xhr = new XMLHttpRequest();
          xhr.upload.addEventListener('progress', (e) => {
            if (e.lengthComputable && onProgress) onProgress(Math.round((e.loaded / e.total) * 100));
          });
          xhr.addEventListener('load', () => {
            if (xhr.status >= 200 && xhr.status < 300) resolve();
            else reject(new Error(`S3 upload failed: ${xhr.status}`));
          });
          xhr.addEventListener('error',   () => reject(new Error('Network error during upload')));
          xhr.addEventListener('timeout', () => reject(new Error('Upload timed out')));
          xhr.open('PUT', uploadUrl);
          xhr.setRequestHeader('Content-Type', file.type || 'application/zip');
          xhr.timeout = 60 * 60 * 1000;
          xhr.send(file);
        });
        break;
      } catch (err) {
        lastError = err;
        console.warn(`[Upload] Attempt ${attempt}/${MAX_RETRIES} failed: ${err.message}`);
        if (attempt < MAX_RETRIES) {
          if (onProgress) onProgress(0);
          await new Promise(r => setTimeout(r, 5000 * attempt));
        } else {
          throw new Error(`Upload failed after ${MAX_RETRIES} attempts: ${lastError.message}`);
        }
      }
    }

    return fetchApi(`/api/jobs/${jobId}/confirm-upload`, {
      method: 'POST',
      body: JSON.stringify({ s3Key, fileName: file.name, fileSize: file.size })
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

  analyzePdf:      (jobId)       => fetchApi(`/api/jobs/${jobId}/analyze-pdf`),
  uploadDocuments: async (jobId, files) => { const formData = new FormData(); files.forEach(f => formData.append('files', f)); return fetchApi(`/api/jobs/${jobId}/upload-documents`, { method: 'POST', body: formData }); },
  startExtraction: (jobId)       => fetchApi(`/api/jobs/${jobId}/extract`,        { method: 'POST' }),
  startAnalysis:   (jobId)       => fetchApi(`/api/jobs/${jobId}/analyze`,         { method: 'POST' }),
  generateReport:  (jobId)       => fetchApi(`/api/jobs/${jobId}/generate-report`, { method: 'POST' }),
  getStatus:       (jobId)       => fetchApi(`/api/jobs/${jobId}/status`),
  getDownloadUrl:  (jobId)       => fetchApi(`/api/jobs/${jobId}/download`),
  getLogs:         (jobId)       => fetchApi(`/api/jobs/${jobId}/logs`),
  resumeJob:       (jobId)       => fetchApi(`/api/jobs/${jobId}/resume`,          { method: 'POST' }),
  list:            ()            => fetchApi('/api/jobs'),

  subscribeToEvents: (jobId, handlers) => {
    const eventSource = new EventSource(`${API_URL}/api/jobs/${jobId}/events`);
    eventSource.addEventListener('connected',           (e) => handlers.onConnected?.(JSON.parse(e.data)));
    eventSource.addEventListener('log',                 (e) => handlers.onLog?.(JSON.parse(e.data)));
    eventSource.addEventListener('progress',            (e) => handlers.onProgress?.(JSON.parse(e.data)));
    eventSource.addEventListener('processing',          (e) => handlers.onProcessing?.(JSON.parse(e.data)));
    eventSource.addEventListener('chunk-progress',      (e) => handlers.onChunkProgress?.(JSON.parse(e.data)));
    eventSource.addEventListener('tokens',              (e) => handlers.onTokens?.(JSON.parse(e.data)));
    eventSource.addEventListener('extraction-complete', (e) => handlers.onExtractionComplete?.(JSON.parse(e.data)));
    eventSource.addEventListener('analysis-complete',   (e) => handlers.onAnalysisComplete?.(JSON.parse(e.data)));
    eventSource.addEventListener('complete',            (e) => handlers.onComplete?.(JSON.parse(e.data)));
    eventSource.addEventListener('error',               (e) => handlers.onError?.(e));
    eventSource.onerror = () => handlers.onError?.({ message: 'Connection lost' });
    return () => eventSource.close();
  }
};
