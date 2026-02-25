import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { jobsApi } from '../services/api';
import { 
  Plus, 
  FileText, 
  Clock, 
  CheckCircle2, 
  AlertCircle, 
  Loader2,
  TrendingUp,
  FileWarning,
  ChevronRight
} from 'lucide-react';

export default function DashboardPage() {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    loadJobs();
  }, []);

  const loadJobs = async () => {
    try {
      const response = await jobsApi.list();
      setJobs(response.jobs || []);
    } catch (err) {
      setError('Failed to load jobs');
    } finally {
      setLoading(false);
    }
  };

  const getStatusBadge = (status) => {
    const badges = {
      created: { color: 'bg-gray-100 text-gray-700', icon: Clock, label: 'Created' },
      uploaded: { color: 'bg-blue-100 text-blue-700', icon: FileText, label: 'Uploaded' },
      extracting: { color: 'bg-amber-100 text-amber-700', icon: Loader2, label: 'Extracting' },
      extracted: { color: 'bg-amber-100 text-amber-700', icon: FileText, label: 'Extracted' },
      processing: { color: 'bg-purple-100 text-purple-700', icon: Loader2, label: 'Processing' },
      analyzing: { color: 'bg-purple-100 text-purple-700', icon: Loader2, label: 'Analyzing' },
      'analysis-complete': { color: 'bg-green-100 text-green-700', icon: CheckCircle2, label: 'Analyzed' },
      'generating-report': { color: 'bg-blue-100 text-blue-700', icon: Loader2, label: 'Generating' },
      completed: { color: 'bg-green-100 text-green-700', icon: CheckCircle2, label: 'Completed' },
      failed: { color: 'bg-red-100 text-red-700', icon: AlertCircle, label: 'Failed' }
    };
    
    const badge = badges[status] || badges.created;
    const Icon = badge.icon;
    
    return (
      <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${badge.color}`}>
        <Icon className={`w-3.5 h-3.5 ${status.includes('ing') ? 'animate-spin' : ''}`} />
        {badge.label}
      </span>
    );
  };

  // Calculate stats
  const stats = {
    total: jobs.length,
    completed: jobs.filter(j => j.status === 'completed').length,
    processing: jobs.filter(j => ['processing', 'analyzing', 'extracting'].includes(j.status)).length,
    documentsProcessed: jobs.reduce((sum, j) => sum + (j.processedCount || 0), 0)
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-display font-bold text-gray-900">Dashboard</h1>
          <p className="text-gray-500 mt-1">Overview of your legal audit jobs</p>
        </div>
        <Link to="/new-job" className="btn-primary inline-flex items-center gap-2">
          <Plus className="w-5 h-5" />
          New Audit Job
        </Link>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="card p-5">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-primary-100 rounded-lg flex items-center justify-center">
              <FileText className="w-5 h-5 text-primary-600" />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{stats.total}</p>
              <p className="text-sm text-gray-500">Total Jobs</p>
            </div>
          </div>
        </div>
        
        <div className="card p-5">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-green-100 rounded-lg flex items-center justify-center">
              <CheckCircle2 className="w-5 h-5 text-green-600" />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{stats.completed}</p>
              <p className="text-sm text-gray-500">Completed</p>
            </div>
          </div>
        </div>
        
        <div className="card p-5">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-amber-100 rounded-lg flex items-center justify-center">
              <Loader2 className="w-5 h-5 text-amber-600" />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{stats.processing}</p>
              <p className="text-sm text-gray-500">In Progress</p>
            </div>
          </div>
        </div>
        
        <div className="card p-5">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-purple-100 rounded-lg flex items-center justify-center">
              <TrendingUp className="w-5 h-5 text-purple-600" />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{stats.documentsProcessed}</p>
              <p className="text-sm text-gray-500">Docs Analyzed</p>
            </div>
          </div>
        </div>
      </div>

      {/* Jobs list */}
      <div className="card">
        <div className="px-6 py-4 border-b border-gray-100">
          <h2 className="font-semibold text-gray-900">Recent Jobs</h2>
        </div>
        
        {error && (
          <div className="p-4 bg-red-50 text-red-600 text-sm">
            {error}
          </div>
        )}
        
        {jobs.length === 0 ? (
          <div className="p-12 text-center">
            <FileWarning className="w-12 h-12 text-gray-300 mx-auto mb-4" />
            <h3 className="font-medium text-gray-900 mb-1">No jobs yet</h3>
            <p className="text-gray-500 text-sm mb-4">Create your first legal audit job to get started</p>
            <Link to="/new-job" className="btn-primary inline-flex items-center gap-2">
              <Plus className="w-4 h-4" />
              Create Job
            </Link>
          </div>
        ) : (
          <div className="divide-y divide-gray-100">
            {jobs.map((job) => (
              <Link
                key={job.id}
                to={`/job/${job.id}`}
                className="flex items-center justify-between px-6 py-4 hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center gap-4">
                  <div className="w-10 h-10 bg-gray-100 rounded-lg flex items-center justify-center">
                    <FileText className="w-5 h-5 text-gray-500" />
                  </div>
                  <div>
                    <p className="font-medium text-gray-900">{job.id}</p>
                    <div className="flex items-center gap-2 text-sm text-gray-500">
                      <span>
                        Created: {new Date(job.createdAt).toLocaleDateString('en-IN', {
                          day: 'numeric',
                          month: 'short',
                          hour: '2-digit',
                          minute: '2-digit'
                        })}
                      </span>
                      {job.completedAt && (
                        <>
                          <span>•</span>
                          <span className="text-green-600">
                            Completed: {new Date(job.completedAt).toLocaleDateString('en-IN', {
                              day: 'numeric',
                              month: 'short',
                              hour: '2-digit',
                              minute: '2-digit'
                            })}
                          </span>
                        </>
                      )}
                    </div>
                  </div>
                </div>
                
                <div className="flex items-center gap-4">
                  <div className="text-right hidden sm:block">
                    <p className="text-sm font-medium text-gray-900">
                      {job.processedCount || 0} / {job.totalDocuments || '-'} docs
                    </p>
                    <p className="text-xs text-gray-500">
                      {job.failedCount ? `${job.failedCount} failed` : 'No failures'}
                    </p>
                  </div>
                  {getStatusBadge(job.status)}
                  <ChevronRight className="w-5 h-5 text-gray-400" />
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
