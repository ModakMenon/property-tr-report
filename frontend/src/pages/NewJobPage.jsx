import React, { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { jobsApi } from '../services/api';
import { 
  Upload, 
  FileArchive, 
  Play, 
  CheckCircle2, 
  Loader2,
  AlertCircle,
  FileText,
  X,
  File,
  Layers,
  Clock,
  Zap,
  Activity
} from 'lucide-react';

export default function NewJobPage() {
  const { user } = useAuth();
  const navigate = useNavigate();
  const fileInputRef = useRef(null);
  
  const [step, setStep] = useState(1); // 1: Create, 2: Upload, 3: Process
  const [jobId, setJobId] = useState(null);
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [logs, setLogs] = useState([]);
  const [processingStatus, setProcessingStatus] = useState(null);
  const [stats, setStats] = useState({ total: 0, processed: 0, failed: 0 });
  const [uploadType, setUploadType] = useState('zip'); // 'zip' or 'pdf'
  const [pdfAnalysis, setPdfAnalysis] = useState(null); // Analysis for large PDFs
  const [chunkProgress, setChunkProgress] = useState(null); // Track chunk processing
  const [tokenUsage, setTokenUsage] = useState({ input: 0, output: 0 }); // Track token usage

  const addLog = (message, type = 'info') => {
    const time = new Date().toISOString().split('T')[1].split('.')[0];
    setLogs(prev => [...prev, { time, message, type }]);
  };

  // Create job
  const handleCreateJob = async () => {
    setLoading(true);
    setError('');
    try {
      const response = await jobsApi.create(user?.email);
      setJobId(response.jobId);
      addLog(`Job created: ${response.jobId}`, 'success');
      setStep(2);
    } catch (err) {
      setError(err.message);
      addLog(`Failed to create job: ${err.message}`, 'error');
    } finally {
      setLoading(false);
    }
  };

  // Handle file selection
  const handleFileSelect = (e) => {
    const file = e.target.files[0];
    if (file) {
      const isZip = file.name.endsWith('.zip');
      const isPdf = file.name.endsWith('.pdf');
      
      if (!isZip && !isPdf) {
        setError('Please select a ZIP file or PDF document');
        return;
      }
      
      setSelectedFile(file);
      setUploadType(isZip ? 'zip' : 'pdf');
      setPdfAnalysis(null);
      setError('');
      addLog(`Selected: ${file.name} (${(file.size / 1024 / 1024).toFixed(2)} MB)`);
      
      // For large PDFs (>20MB), show a notification
      if (isPdf && file.size > 20 * 1024 * 1024) {
        addLog(`Large PDF detected. Will use chunked processing strategy.`, 'warning');
      }
    }
  };

  // Upload file (ZIP or PDF)
  const handleUpload = async () => {
    if (!selectedFile) return;
    
    setLoading(true);
    setError('');
    addLog(`Uploading ${selectedFile.name}...`);
    
    try {
      if (uploadType === 'zip') {
        await jobsApi.uploadZip(jobId, selectedFile, (progress) => {
          setUploadProgress(progress);
        });
        addLog('Upload complete!', 'success');
        setStep(3);
        await handleStartExtraction();
      } else {
        // Single PDF upload
        const response = await jobsApi.uploadPdf(jobId, selectedFile, (progress) => {
          setUploadProgress(progress);
        });
        addLog('Upload complete!', 'success');
        
        // Show PDF analysis if available
        if (response.analysis) {
          setPdfAnalysis(response.analysis);
          addLog(`PDF Analysis: ${response.analysis.pageCount} pages, ${response.analysis.fileSizeMB}MB`, 'info');
          addLog(`Processing strategy: ${response.analysis.strategy}`, 'info');
          if (response.analysis.estimatedChunks > 1) {
            addLog(`Will process in ${response.analysis.estimatedChunks} chunks`, 'warning');
          }
        }
        
        setStep(3);
        await handleStartAnalysis();
      }
    } catch (err) {
      setError(err.message);
      addLog(`Upload failed: ${err.message}`, 'error');
    } finally {
      setLoading(false);
    }
  };

  // Start extraction (for ZIP files)
  const handleStartExtraction = async () => {
    addLog('Starting extraction...');
    setProcessingStatus('extracting');
    
    // Connect to SSE
    const unsubscribe = jobsApi.subscribeToEvents(jobId, {
      onLog: (data) => {
        addLog(data.message, data.message.includes('✓') ? 'success' : 
               data.message.includes('❌') ? 'error' : 
               data.message.includes('⚠️') ? 'warning' : 'info');
      },
      onProgress: (data) => {
        setStats(prev => ({ ...prev, processed: data.current, total: data.total }));
      },
      onChunkProgress: (data) => {
        setChunkProgress(data);
      },
      onTokens: (data) => {
        setTokenUsage({ input: data.totalInput || 0, output: data.totalOutput || 0 });
      },
      onExtractionComplete: (data) => {
        setStats(prev => ({ ...prev, total: data.totalDocuments }));
        setProcessingStatus('analyzing');
      },
      onAnalysisComplete: (data) => {
        setStats(prev => ({ ...prev, processed: data.processed, failed: data.failed }));
        if (data.totalTokensInput || data.totalTokensOutput) {
          setTokenUsage({ input: data.totalTokensInput || 0, output: data.totalTokensOutput || 0 });
        }
        setProcessingStatus('generating');
        setChunkProgress(null);
      },
      onComplete: (data) => {
        setProcessingStatus('completed');
        addLog('✓ All processing complete!', 'success');
      },
      onError: (data) => {
        addLog(`Error: ${data?.message || 'Unknown error'}`, 'error');
      }
    });

    try {
      await jobsApi.startExtraction(jobId);
    } catch (err) {
      addLog(`Error: ${err.message}`, 'error');
    }

    return unsubscribe;
  };

  // Start analysis directly (for single PDF uploads)
  const handleStartAnalysis = async () => {
    addLog('Starting analysis...');
    setProcessingStatus('analyzing');
    setStats(prev => ({ ...prev, total: 1 }));
    
    // Connect to SSE
    const unsubscribe = jobsApi.subscribeToEvents(jobId, {
      onLog: (data) => {
        addLog(data.message, data.message.includes('✓') ? 'success' : 
               data.message.includes('❌') ? 'error' : 
               data.message.includes('⚠️') ? 'warning' : 'info');
      },
      onProgress: (data) => {
        setStats(prev => ({ ...prev, processed: data.current, total: data.total }));
      },
      onChunkProgress: (data) => {
        setChunkProgress(data);
      },
      onTokens: (data) => {
        setTokenUsage({ input: data.totalInput || 0, output: data.totalOutput || 0 });
      },
      onAnalysisComplete: (data) => {
        setStats(prev => ({ ...prev, processed: data.processed, failed: data.failed }));
        if (data.totalTokensInput || data.totalTokensOutput) {
          setTokenUsage({ input: data.totalTokensInput || 0, output: data.totalTokensOutput || 0 });
        }
        setProcessingStatus('generating');
        setChunkProgress(null);
      },
      onComplete: (data) => {
        setProcessingStatus('completed');
        addLog('✓ All processing complete!', 'success');
      },
      onError: (data) => {
        addLog(`Error: ${data?.message || 'Unknown error'}`, 'error');
      }
    });

    try {
      await jobsApi.startAnalysis(jobId);
    } catch (err) {
      addLog(`Error: ${err.message}`, 'error');
    }

    return unsubscribe;
  };

  // View results
  const handleViewResults = () => {
    navigate(`/job/${jobId}`);
  };

  return (
    <div className="max-w-4xl mx-auto space-y-8 animate-fade-in">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-display font-bold text-gray-900">New Audit Job</h1>
        <p className="text-gray-500 mt-1">Upload documents and start the legal audit process</p>
      </div>

      {/* Progress steps */}
      <div className="flex items-center justify-between">
        {[
          { num: 1, label: 'Create Job' },
          { num: 2, label: 'Upload Documents' },
          { num: 3, label: 'Process & Analyze' }
        ].map((s, i) => (
          <React.Fragment key={s.num}>
            <div className="flex items-center gap-3">
              <div className={`
                w-10 h-10 rounded-full flex items-center justify-center font-semibold
                ${step > s.num ? 'bg-green-100 text-green-600' : 
                  step === s.num ? 'bg-primary-600 text-white' : 'bg-gray-100 text-gray-400'}
              `}>
                {step > s.num ? <CheckCircle2 className="w-5 h-5" /> : s.num}
              </div>
              <span className={`font-medium ${step >= s.num ? 'text-gray-900' : 'text-gray-400'}`}>
                {s.label}
              </span>
            </div>
            {i < 2 && (
              <div className={`flex-1 h-0.5 mx-4 ${step > s.num ? 'bg-green-200' : 'bg-gray-200'}`} />
            )}
          </React.Fragment>
        ))}
      </div>

      {/* Error display */}
      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg flex items-center gap-2">
          <AlertCircle className="w-5 h-5 flex-shrink-0" />
          {error}
        </div>
      )}

      {/* Step 1: Create Job */}
      {step === 1 && (
        <div className="card p-8 text-center">
          <div className="w-16 h-16 bg-primary-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
            <FileText className="w-8 h-8 text-primary-600" />
          </div>
          <h2 className="text-xl font-semibold text-gray-900 mb-2">Create New Audit Job</h2>
          <p className="text-gray-500 mb-6 max-w-md mx-auto">
            Start by creating a new audit job. You'll then be able to upload your documents for processing.
          </p>
          <button
            onClick={handleCreateJob}
            disabled={loading}
            className="btn-primary inline-flex items-center gap-2"
          >
            {loading ? (
              <Loader2 className="w-5 h-5 animate-spin" />
            ) : (
              <Play className="w-5 h-5" />
            )}
            Create Job
          </button>
        </div>
      )}

      {/* Step 2: Upload */}
      {step === 2 && (
        <div className="card p-8">
          <div className="text-center mb-6">
            <h2 className="text-xl font-semibold text-gray-900 mb-2">Upload Documents</h2>
            <p className="text-gray-500">
              Upload a ZIP file containing multiple documents, or a single PDF (including large files up to 500MB).
            </p>
          </div>

          <input
            ref={fileInputRef}
            type="file"
            accept=".zip,.pdf"
            onChange={handleFileSelect}
            className="hidden"
          />

          {!selectedFile ? (
            <div
              onClick={() => fileInputRef.current?.click()}
              className="border-2 border-dashed border-gray-200 rounded-xl p-12 text-center cursor-pointer hover:border-primary-400 hover:bg-primary-50/50 transition-all"
            >
              <div className="flex justify-center gap-4 mb-4">
                <FileArchive className="w-10 h-10 text-gray-400" />
                <File className="w-10 h-10 text-gray-400" />
              </div>
              <p className="font-medium text-gray-900 mb-1">Click to select file</p>
              <p className="text-sm text-gray-500">or drag and drop your file here</p>
              <div className="flex justify-center gap-4 mt-4 text-xs text-gray-400">
                <span className="flex items-center gap-1">
                  <FileArchive className="w-4 h-4" /> ZIP (bulk)
                </span>
                <span className="flex items-center gap-1">
                  <File className="w-4 h-4" /> PDF (single)
                </span>
              </div>
              <p className="text-xs text-gray-400 mt-2">Supports files up to 500MB • Large PDFs auto-chunked</p>
            </div>
          ) : (
            <div className="space-y-4">
              <div className="bg-gray-50 rounded-lg p-4 flex items-center gap-4">
                <div className={`w-12 h-12 rounded-lg flex items-center justify-center ${
                  uploadType === 'zip' ? 'bg-primary-100' : 'bg-amber-100'
                }`}>
                  {uploadType === 'zip' ? (
                    <FileArchive className="w-6 h-6 text-primary-600" />
                  ) : (
                    <File className="w-6 h-6 text-amber-600" />
                  )}
                </div>
                <div className="flex-1">
                  <p className="font-medium text-gray-900">{selectedFile.name}</p>
                  <div className="flex items-center gap-3 text-sm text-gray-500">
                    <span>{(selectedFile.size / 1024 / 1024).toFixed(2)} MB</span>
                    <span>•</span>
                    <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                      uploadType === 'zip' 
                        ? 'bg-primary-100 text-primary-700' 
                        : 'bg-amber-100 text-amber-700'
                    }`}>
                      {uploadType === 'zip' ? 'Bulk Upload' : 'Single PDF'}
                    </span>
                    {uploadType === 'pdf' && selectedFile.size > 20 * 1024 * 1024 && (
                      <>
                        <span>•</span>
                        <span className="flex items-center gap-1 text-amber-600">
                          <Layers className="w-3 h-3" />
                          Large file - will chunk
                        </span>
                      </>
                    )}
                  </div>
                </div>
                <button
                  onClick={() => {
                    setSelectedFile(null);
                    setPdfAnalysis(null);
                  }}
                  className="p-2 text-gray-400 hover:text-gray-600"
                >
                  <X className="w-5 h-5" />
                </button>
              </div>

              {uploadProgress > 0 && uploadProgress < 100 && (
                <div className="space-y-2">
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-600">Uploading...</span>
                    <span className="font-medium text-primary-600">{uploadProgress}%</span>
                  </div>
                  <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                    <div 
                      className="h-full bg-primary-600 transition-all duration-300"
                      style={{ width: `${uploadProgress}%` }}
                    />
                  </div>
                </div>
              )}

              <button
                onClick={handleUpload}
                disabled={loading}
                className="w-full btn-primary inline-flex items-center justify-center gap-2"
              >
                {loading ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <Upload className="w-5 h-5" />
                )}
                Upload & Start Processing
              </button>
            </div>
          )}
        </div>
      )}

      {/* Step 3: Processing */}
      {step === 3 && (
        <div className="space-y-6">
          {/* PDF Analysis Preview (for large PDFs) */}
          {pdfAnalysis && pdfAnalysis.estimatedChunks > 1 && (
            <div className="card p-4 bg-amber-50 border-amber-200">
              <div className="flex items-start gap-3">
                <Layers className="w-5 h-5 text-amber-600 mt-0.5" />
                <div className="flex-1">
                  <p className="font-medium text-amber-800">Large PDF Processing</p>
                  <p className="text-sm text-amber-700 mt-1">{pdfAnalysis.reason}</p>
                  <div className="flex gap-4 mt-2 text-sm text-amber-600">
                    <span className="flex items-center gap-1">
                      <FileText className="w-4 h-4" />
                      {pdfAnalysis.pageCount} pages
                    </span>
                    <span className="flex items-center gap-1">
                      <Layers className="w-4 h-4" />
                      {pdfAnalysis.estimatedChunks} chunks
                    </span>
                    <span className="flex items-center gap-1">
                      <Zap className="w-4 h-4" />
                      {pdfAnalysis.strategy}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Progress stats */}
          <div className="grid grid-cols-4 gap-4">
            <div className="card p-4 text-center">
              <p className="text-3xl font-bold text-gray-900">{stats.total}</p>
              <p className="text-sm text-gray-500">Total Documents</p>
            </div>
            <div className="card p-4 text-center">
              <p className="text-3xl font-bold text-green-600">{stats.processed}</p>
              <p className="text-sm text-gray-500">Processed</p>
            </div>
            <div className="card p-4 text-center">
              <p className="text-3xl font-bold text-amber-600">{stats.failed}</p>
              <p className="text-sm text-gray-500">Manual Review</p>
            </div>
            <div className="card p-4 text-center">
              <div className="flex items-center justify-center gap-1">
                <Activity className="w-5 h-5 text-purple-500" />
                <p className="text-xl font-bold text-purple-600">
                  {((tokenUsage.input + tokenUsage.output) / 1000).toFixed(1)}K
                </p>
              </div>
              <p className="text-xs text-gray-500">
                {tokenUsage.input > 0 && `${(tokenUsage.input / 1000).toFixed(1)}K↓ ${(tokenUsage.output / 1000).toFixed(1)}K↑`}
                {tokenUsage.input === 0 && 'Tokens'}
              </p>
            </div>
          </div>

          {/* Chunk progress (for large PDFs) */}
          {chunkProgress && (
            <div className="card p-4 bg-blue-50 border-blue-200">
              <div className="flex items-center justify-between text-sm mb-2">
                <span className="text-blue-700 font-medium flex items-center gap-2">
                  <Layers className="w-4 h-4" />
                  Processing chunk {chunkProgress.current}/{chunkProgress.total}
                </span>
                <span className="text-blue-600">
                  {Math.round((chunkProgress.current / chunkProgress.total) * 100)}%
                </span>
              </div>
              <div className="h-2 bg-blue-200 rounded-full overflow-hidden">
                <div 
                  className="h-full bg-blue-500 transition-all duration-300"
                  style={{ width: `${(chunkProgress.current / chunkProgress.total) * 100}%` }}
                />
              </div>
              <p className="text-xs text-blue-600 mt-2">
                Document: {chunkProgress.document}
              </p>
            </div>
          )}

          {/* Progress bar */}
          {stats.total > 0 && (
            <div className="card p-4">
              <div className="flex justify-between text-sm mb-2">
                <span className="text-gray-600">
                  {processingStatus === 'extracting' && 'Extracting documents...'}
                  {processingStatus === 'analyzing' && 'Analyzing documents...'}
                  {processingStatus === 'generating' && 'Generating report...'}
                  {processingStatus === 'completed' && 'Complete!'}
                </span>
                <span className="font-medium">
                  {Math.round((stats.processed / stats.total) * 100)}%
                </span>
              </div>
              <div className="h-3 bg-gray-200 rounded-full overflow-hidden">
                <div 
                  className={`h-full transition-all duration-500 ${
                    processingStatus === 'completed' ? 'bg-green-500' : 'bg-primary-600'
                  }`}
                  style={{ width: `${(stats.processed / stats.total) * 100}%` }}
                />
              </div>
            </div>
          )}

          {/* Action button */}
          {processingStatus === 'completed' && (
            <button
              onClick={handleViewResults}
              className="w-full btn-primary inline-flex items-center justify-center gap-2"
            >
              <CheckCircle2 className="w-5 h-5" />
              View Results & Download Report
            </button>
          )}
        </div>
      )}

      {/* Live logs */}
      {logs.length > 0 && (
        <div className="card overflow-hidden">
          <div className="px-4 py-3 bg-gray-900 border-b border-gray-700 flex items-center justify-between">
            <span className="text-sm font-medium text-gray-300">Processing Log</span>
            <span className="text-xs text-gray-500">{logs.length} entries</span>
          </div>
          <div className="terminal" style={{ maxHeight: '300px' }}>
            {logs.map((log, i) => (
              <div key={i} className="terminal-line flex gap-3">
                <span className="terminal-time">[{log.time}]</span>
                <span className={
                  log.type === 'success' ? 'terminal-success' :
                  log.type === 'error' ? 'terminal-error' :
                  log.type === 'warning' ? 'terminal-warning' : 'text-gray-300'
                }>
                  {log.message}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Job ID display */}
      {jobId && (
        <div className="text-center text-sm text-gray-500">
          Job ID: <span className="font-mono text-gray-700">{jobId}</span>
        </div>
      )}
    </div>
  );
}
