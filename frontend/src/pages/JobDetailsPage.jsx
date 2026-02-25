import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { jobsApi } from '../services/api';
import { 
  Download, 
  ArrowLeft, 
  FileText, 
  AlertTriangle,
  CheckCircle2,
  Clock,
  Loader2,
  RefreshCw,
  ExternalLink,
  ScrollText,
  PlayCircle
} from 'lucide-react';

export default function JobDetailsPage() {
  const { jobId } = useParams();
  const [job, setJob] = useState(null);
  const [loading, setLoading] = useState(true);
  const [downloadLoading, setDownloadLoading] = useState(false);
  const [resumeLoading, setResumeLoading] = useState(false);
  const [error, setError] = useState('');
  const [logs, setLogs] = useState([]);
  const [logsLoading, setLogsLoading] = useState(false);

  useEffect(() => {
    loadJobDetails();
    loadLogs();
    
    // Poll for updates if job is processing
    const interval = setInterval(() => {
      if (job && !['completed', 'failed'].includes(job.status)) {
        loadJobDetails();
        loadLogs();
      }
    }, 5000);

    return () => clearInterval(interval);
  }, [jobId]);

  const loadJobDetails = async () => {
    try {
      const data = await jobsApi.getStatus(jobId);
      setJob(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const loadLogs = async () => {
    try {
      setLogsLoading(true);
      const data = await jobsApi.getLogs(jobId);
      if (data.logs && data.logs.length > 0) {
        setLogs(data.logs);
      }
    } catch (err) {
      console.error('Failed to load logs:', err);
    } finally {
      setLogsLoading(false);
    }
  };

  const handleDownload = async () => {
    setDownloadLoading(true);
    try {
      const { downloadUrl } = await jobsApi.getDownloadUrl(jobId);
      window.open(downloadUrl, '_blank');
    } catch (err) {
      setError('Failed to get download link');
    } finally {
      setDownloadLoading(false);
    }
  };

  const handleResume = async () => {
    setResumeLoading(true);
    setError('');
    try {
      const result = await jobsApi.resumeJob(jobId);
      // Reload job details
      await loadJobDetails();
      await loadLogs();
      
      // Show success message
      if (result.message) {
        alert(result.message);
      }
    } catch (err) {
      setError(`Failed to resume job: ${err.message}`);
    } finally {
      setResumeLoading(false);
    }
  };

  // Check if job can be resumed
  const canResume = job && 
    !['completed', 'processing', 'analyzing', 'extracting', 'generating-report'].includes(job.status) &&
    job.processedCount > 0 && 
    job.processedCount < job.totalDocuments;

  // Determine effective status (show interrupted if can resume)
  const effectiveStatus = canResume ? 'interrupted' : job?.status;

  const getStatusInfo = (status) => {
    const statusMap = {
      created: { label: 'Created', color: 'gray', icon: Clock },
      uploaded: { label: 'Uploaded', color: 'blue', icon: FileText },
      extracting: { label: 'Extracting', color: 'amber', icon: Loader2 },
      extracted: { label: 'Extracted', color: 'amber', icon: FileText },
      processing: { label: 'Processing', color: 'purple', icon: Loader2 },
      analyzing: { label: 'Analyzing', color: 'purple', icon: Loader2 },
      'analysis-complete': { label: 'Analyzed', color: 'green', icon: CheckCircle2 },
      'generating-report': { label: 'Generating Report', color: 'blue', icon: Loader2 },
      completed: { label: 'Completed', color: 'green', icon: CheckCircle2 },
      failed: { label: 'Failed', color: 'red', icon: AlertTriangle },
      interrupted: { label: 'Interrupted', color: 'amber', icon: AlertTriangle }
    };
    return statusMap[status] || statusMap.created;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
      </div>
    );
  }

  if (error && !job) {
    return (
      <div className="text-center py-12">
        <AlertTriangle className="w-12 h-12 text-red-500 mx-auto mb-4" />
        <h2 className="text-xl font-semibold text-gray-900 mb-2">Error Loading Job</h2>
        <p className="text-gray-500 mb-4">{error}</p>
        <Link to="/" className="btn-secondary inline-flex items-center gap-2">
          <ArrowLeft className="w-4 h-4" />
          Back to Dashboard
        </Link>
      </div>
    );
  }

  const statusInfo = getStatusInfo(effectiveStatus);
  const StatusIcon = statusInfo.icon;
  const isProcessing = ['extracting', 'processing', 'analyzing', 'generating-report'].includes(job?.status);

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <Link to="/" className="text-sm text-primary-600 hover:text-primary-700 inline-flex items-center gap-1 mb-2">
            <ArrowLeft className="w-4 h-4" />
            Back to Dashboard
          </Link>
          <h1 className="text-2xl font-display font-bold text-gray-900">Job Details</h1>
          <p className="text-gray-500 font-mono text-sm mt-1">{jobId}</p>
        </div>
        
        <div className="flex items-center gap-3">
          <button
            onClick={loadJobDetails}
            className="btn-secondary inline-flex items-center gap-2"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
          
          {job?.status === 'completed' && (
            <button
              onClick={handleDownload}
              disabled={downloadLoading}
              className="btn-primary inline-flex items-center gap-2"
            >
              {downloadLoading ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Download className="w-4 h-4" />
              )}
              Download Report
            </button>
          )}
        </div>
      </div>

      {/* Status card */}
      <div className="card p-6">
        <div className="flex items-center gap-4 mb-6">
          <div className={`
            w-14 h-14 rounded-xl flex items-center justify-center
            ${statusInfo.color === 'green' ? 'bg-green-100' :
              statusInfo.color === 'amber' ? 'bg-amber-100' :
              statusInfo.color === 'red' ? 'bg-red-100' :
              statusInfo.color === 'purple' ? 'bg-purple-100' :
              statusInfo.color === 'blue' ? 'bg-blue-100' : 'bg-gray-100'}
          `}>
            <StatusIcon className={`
              w-7 h-7
              ${statusInfo.color === 'green' ? 'text-green-600' :
                statusInfo.color === 'amber' ? 'text-amber-600' :
                statusInfo.color === 'red' ? 'text-red-600' :
                statusInfo.color === 'purple' ? 'text-purple-600' :
                statusInfo.color === 'blue' ? 'text-blue-600' : 'text-gray-600'}
              ${isProcessing ? 'animate-spin' : ''}
            `} />
          </div>
          <div>
            <h2 className="text-xl font-semibold text-gray-900">{statusInfo.label}</h2>
            <p className="text-gray-500">
              {isProcessing ? 'Processing your documents...' : 
               job?.status === 'completed' ? 'Your audit report is ready' :
               job?.status === 'failed' ? 'An error occurred during processing' :
               'Waiting for next step'}
            </p>
          </div>
        </div>

        {/* Progress bar */}
        {job?.totalDocuments > 0 && (
          <div className="mb-6">
            <div className="flex justify-between text-sm mb-2">
              <span className="text-gray-600">Progress</span>
              <span className="font-medium text-gray-900">
                {job?.processedCount || 0} / {job?.totalDocuments} documents
              </span>
            </div>
            <div className="h-3 bg-gray-200 rounded-full overflow-hidden">
              <div 
                className={`h-full transition-all duration-500 ${
                  job?.status === 'completed' ? 'bg-green-500' : 
                  job?.status === 'failed' ? 'bg-red-500' : 'bg-primary-600'
                }`}
                style={{ width: `${((job?.processedCount || 0) / job?.totalDocuments) * 100}%` }}
              />
            </div>
          </div>
        )}

        {/* Stats grid */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <div className="bg-gray-50 rounded-lg p-4">
            <p className="text-2xl font-bold text-gray-900">{job?.totalDocuments || '-'}</p>
            <p className="text-sm text-gray-500">Total Documents</p>
          </div>
          <div className="bg-gray-50 rounded-lg p-4">
            <p className="text-2xl font-bold text-green-600">{job?.processedCount || 0}</p>
            <p className="text-sm text-gray-500">Processed</p>
          </div>
          <div className="bg-gray-50 rounded-lg p-4">
            <p className="text-2xl font-bold text-amber-600">{job?.failedCount || 0}</p>
            <p className="text-sm text-gray-500">Manual Review</p>
          </div>
          <div className="bg-gray-50 rounded-lg p-4">
            <p className="text-2xl font-bold text-primary-600">
              {job?.totalDocuments > 0 
                ? Math.round(((job?.processedCount || 0) / job?.totalDocuments) * 100)
                : 0}%
            </p>
            <p className="text-sm text-gray-500">Completion</p>
          </div>
        </div>
      </div>

      {/* Job info */}
      <div className="card">
        <div className="px-6 py-4 border-b border-gray-100">
          <h3 className="font-semibold text-gray-900">Job Information</h3>
        </div>
        <div className="p-6">
          <dl className="grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-4">
            <div>
              <dt className="text-sm text-gray-500">Job ID</dt>
              <dd className="font-mono text-gray-900">{job?.id}</dd>
            </div>
            <div>
              <dt className="text-sm text-gray-500">Created At</dt>
              <dd className="text-gray-900">
                {job?.createdAt && new Date(job.createdAt).toLocaleString('en-IN')}
              </dd>
            </div>
            <div>
              <dt className="text-sm text-gray-500">Created By</dt>
              <dd className="text-gray-900">{job?.createdBy || '-'}</dd>
            </div>
            <div>
              <dt className="text-sm text-gray-500">Status</dt>
              <dd>
                <span className={`
                  inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium
                  ${statusInfo.color === 'green' ? 'bg-green-100 text-green-700' :
                    statusInfo.color === 'amber' ? 'bg-amber-100 text-amber-700' :
                    statusInfo.color === 'red' ? 'bg-red-100 text-red-700' :
                    statusInfo.color === 'purple' ? 'bg-purple-100 text-purple-700' :
                    statusInfo.color === 'blue' ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-700'}
                `}>
                  {statusInfo.label}
                </span>
              </dd>
            </div>
            {job?.fileName && (
              <div>
                <dt className="text-sm text-gray-500">Uploaded File</dt>
                <dd className="text-gray-900">{job.fileName}</dd>
              </div>
            )}
            {job?.fileSize && (
              <div>
                <dt className="text-sm text-gray-500">File Size</dt>
                <dd className="text-gray-900">
                  {(job.fileSize / 1024 / 1024).toFixed(2)} MB
                </dd>
              </div>
            )}
          </dl>
        </div>
      </div>

      {/* Download section for completed jobs */}
      {/* Resume section for interrupted jobs */}
      {canResume && (
        <div className="card p-6 bg-amber-50 border-amber-200">
          <div className="flex items-start gap-4">
            <div className="w-12 h-12 bg-amber-100 rounded-xl flex items-center justify-center flex-shrink-0">
              <AlertTriangle className="w-6 h-6 text-amber-600" />
            </div>
            <div className="flex-1">
              <h3 className="font-semibold text-amber-900 mb-1">Job Interrupted</h3>
              <p className="text-amber-700 text-sm mb-4">
                This job was interrupted before completion. 
                {job?.processedCount && job?.totalDocuments && (
                  <span className="font-medium"> {job.processedCount} of {job.totalDocuments} documents were processed. </span>
                )}
                You can resume processing from where it left off.
              </p>
              <button
                onClick={handleResume}
                disabled={resumeLoading}
                className="inline-flex items-center gap-2 bg-amber-600 hover:bg-amber-700 text-white font-medium py-2.5 px-5 rounded-lg transition-all"
              >
                {resumeLoading ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <PlayCircle className="w-4 h-4" />
                )}
                Resume Processing
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Download section for completed jobs */}
      {job?.status === 'completed' && (
        <div className="card p-6 bg-green-50 border-green-200">
          <div className="flex items-start gap-4">
            <div className="w-12 h-12 bg-green-100 rounded-xl flex items-center justify-center flex-shrink-0">
              <CheckCircle2 className="w-6 h-6 text-green-600" />
            </div>
            <div className="flex-1">
              <h3 className="font-semibold text-green-900 mb-1">Report Ready</h3>
              <p className="text-green-700 text-sm mb-4">
                Your legal audit report has been generated and is ready for download. 
                The Excel file contains two tabs: detailed audit results and a summary with risk analysis.
              </p>
              <button
                onClick={handleDownload}
                disabled={downloadLoading}
                className="inline-flex items-center gap-2 bg-green-600 hover:bg-green-700 text-white font-medium py-2.5 px-5 rounded-lg transition-all"
              >
                {downloadLoading ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Download className="w-4 h-4" />
                )}
                Download Excel Report
                <ExternalLink className="w-4 h-4 ml-1" />
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Processing Logs */}
      {logs.length > 0 && (
        <div className="card overflow-hidden">
          <div className="px-4 py-3 bg-gray-900 border-b border-gray-700 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <ScrollText className="w-4 h-4 text-gray-400" />
              <span className="text-sm font-medium text-gray-300">Processing Log</span>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-xs text-gray-500">{logs.length} entries</span>
              <button 
                onClick={loadLogs}
                disabled={logsLoading}
                className="text-gray-400 hover:text-gray-300"
              >
                <RefreshCw className={`w-4 h-4 ${logsLoading ? 'animate-spin' : ''}`} />
              </button>
            </div>
          </div>
          <div className="bg-gray-950 p-4 font-mono text-sm max-h-96 overflow-y-auto">
            {logs.map((log, i) => (
              <div key={i} className="flex gap-3 py-0.5">
                <span className="text-gray-500 flex-shrink-0">[{log.time}]</span>
                <span className={
                  log.type === 'success' || log.message.includes('✓') ? 'text-green-400' :
                  log.type === 'error' || log.message.includes('❌') ? 'text-red-400' :
                  log.type === 'warning' || log.message.includes('⚠️') ? 'text-amber-400' : 'text-gray-300'
                }>
                  {log.message}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
