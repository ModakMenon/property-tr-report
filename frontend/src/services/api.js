const API_URL = import.meta.env.VITE_API_URL || '';

async function fetchApi(endpoint, options = {}) {
  const token = localStorage.getItem('token');
  
  const headers = {
    ...options.headers
  };
  
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }
  
  if (!(options.body instanceof FormData)) {
    headers['Content-Type'] = 'application/json';
  }

  const response = await fetch(`${API_URL}${endpoint}`, {
    ...options,
    headers
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Request failed' }));
    throw new Error(error.error || 'Request failed');
  }

  return response.json();
}

export const authApi = {
  login: (email, name) => fetchApi('/api/auth/login', {
    method: 'POST',
    body: JSON.stringify({ email, name })
  }),
  
  verify: () => fetchApi('/api/auth/verify'),
  
  logout: () => {
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    return Promise.resolve();
  }
};

export const mastersApi = {
  getPrompt: () => fetchApi('/api/masters/prompt'),
  
  updatePrompt: (promptData) => fetchApi('/api/masters/prompt', {
    method: 'PUT',
    body: JSON.stringify(promptData)
  })
};

export const jobsApi = {
  create: (userEmail) => fetchApi('/api/jobs/create', {
    method: 'POST',
    body: JSON.stringify({ userEmail })
  }),

  // Upload ZIP directly to S3 via presigned URL (bypasses CloudFront/ALB - no size limit)
  uploadZip: async (jobId, file, onProgress) => {
    // Step 1 - Get presigned URL from backend
    const { uploadUrl, s3Key } = await fetchApi(`/api/jobs/${jobId}/presign-upload`, {
      method: 'POST',
      body: JSON.stringify({
        fileName: file.name,
        contentType: file.type || 'application/zip'
      })
    });

    // Step 2 - Upload directly to S3 (no CloudFront, no size limit)
    await new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();

      xhr.upload.addEventListener('progress', (event) => {
        if (event.lengthComputable && onProgress) {
          onProgress(Math.round((event.loaded / event.total) * 100));
        }
      });

      xhr.addEventListener('load', () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve();
        } else {
          reject(new Error(`S3 upload failed with status ${xhr.status}`));
        }
      });

      xhr.addEventListener('error', () => reject(new Error('Upload failed')));

      // PUT directly to S3 presigned URL
      xhr.open('PUT', uploadUrl);
      xhr.setRequestHeader('Content-Type', file.type || 'application/zip');
      xhr.send(file);
    });

    // Step 3 - Notify backend that upload is complete
    return fetchApi(`/api/jobs/${jobId}/confirm-upload`, {
      method: 'POST',
      body: JSON.stringify({
        s3Key,
        fileName: file.name,
        fileSize: file.size
      })
    });
  },
  
  // Upload single PDF (including large files)
  uploadPdf: async (jobId, file, onProgress) => {
    const formData = new FormData();
    formData.append('file', file);
    
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      
      xhr.upload.addEventListener('progress', (event) => {
        if (event.lengthComputable && onProgress) {
          onProgress(Math.round((event.loaded / event.total) * 100));
        }
      });
      
      xhr.addEventListener('load', () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve(JSON.parse(xhr.responseText));
        } else {
          try {
            const error = JSON.parse(xhr.responseText);
            reject(new Error(error.error || 'Upload failed'));
          } catch {
            reject(new Error('Upload failed'));
          }
        }
      });
      
      xhr.addEventListener('error', () => reject(new Error('Upload failed')));
      
      xhr.open('POST', `${API_URL}/api/jobs/${jobId}/upload-pdf`);
      const token = localStorage.getItem('token');
      if (token) {
        xhr.setRequestHeader('Authorization', `Bearer ${token}`);
      }
      xhr.send(formData);
    });
  },
  
  analyzePdf: (jobId) => fetchApi(`/api/jobs/${jobId}/analyze-pdf`),
  
  uploadDocuments: async (jobId, files) => {
    const formData = new FormData();
    files.forEach(file => formData.append('files', file));
    return fetchApi(`/api/jobs/${jobId}/upload-documents`, {
      method: 'POST',
      body: formData
    });
  },
  
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
    
    eventSource.addEventListener('connected', (e) => {
      handlers.onConnected?.(JSON.parse(e.data));
    });
    
    eventSource.addEventListener('log', (e) => {
      handlers.onLog?.(JSON.parse(e.data));
    });
    
    eventSource.addEventListener('progress', (e) => {
      handlers.onProgress?.(JSON.parse(e.data));
    });
    
    eventSource.addEventListener('processing', (e) => {
      handlers.onProcessing?.(JSON.parse(e.data));
    });
    
    eventSource.addEventListener('chunk-progress', (e) => {
      handlers.onChunkProgress?.(JSON.parse(e.data));
    });
    
    eventSource.addEventListener('tokens', (e) => {
      handlers.onTokens?.(JSON.parse(e.data));
    });
    
    eventSource.addEventListener('extraction-complete', (e) => {
      handlers.onExtractionComplete?.(JSON.parse(e.data));
    });
    
    eventSource.addEventListener('analysis-complete', (e) => {
      handlers.onAnalysisComplete?.(JSON.parse(e.data));
    });
    
    eventSource.addEventListener('complete', (e) => {
      handlers.onComplete?.(JSON.parse(e.data));
    });
    
    eventSource.addEventListener('error', (e) => {
      handlers.onError?.(e);
    });
    
    eventSource.onerror = () => {
      handlers.onError?.({ message: 'Connection lost' });
    };
    
    return () => eventSource.close();
  }
};