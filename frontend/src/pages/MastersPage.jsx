import React, { useState, useEffect } from 'react';
import { mastersApi } from '../services/api';
import { useAuth } from '../context/AuthContext';
import { 
  Save, 
  RefreshCw, 
  AlertCircle, 
  CheckCircle2,
  Loader2,
  Settings,
  FileText,
  List,
  AlertTriangle,
  Shield
} from 'lucide-react';

export default function MastersPage() {
  const { user } = useAuth();
  const [prompt, setPrompt] = useState(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [activeTab, setActiveTab] = useState('system');

  useEffect(() => {
    loadPrompt();
  }, []);

  const loadPrompt = async () => {
    setLoading(true);
    try {
      const data = await mastersApi.getPrompt();
      setPrompt(data);
    } catch (err) {
      setError('Failed to load prompt configuration');
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    setError('');
    setSuccess('');
    
    try {
      const response = await mastersApi.updatePrompt({
        ...prompt,
        updatedBy: user?.email
      });
      setPrompt(response.prompt);
      setSuccess('Configuration saved successfully!');
      setTimeout(() => setSuccess(''), 3000);
    } catch (err) {
      setError(err.message || 'Failed to save configuration');
    } finally {
      setSaving(false);
    }
  };

  const updateRiskClassification = (level, index, value) => {
    setPrompt(prev => ({
      ...prev,
      riskClassification: {
        ...prev.riskClassification,
        [level]: prev.riskClassification[level].map((item, i) => 
          i === index ? value : item
        )
      }
    }));
  };

  const addRiskItem = (level) => {
    setPrompt(prev => ({
      ...prev,
      riskClassification: {
        ...prev.riskClassification,
        [level]: [...prev.riskClassification[level], '']
      }
    }));
  };

  const removeRiskItem = (level, index) => {
    setPrompt(prev => ({
      ...prev,
      riskClassification: {
        ...prev.riskClassification,
        [level]: prev.riskClassification[level].filter((_, i) => i !== index)
      }
    }));
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
      </div>
    );
  }

  const tabs = [
    { id: 'system', label: 'System Role', icon: Settings },
    { id: 'scope', label: 'Review Scope', icon: List },
    { id: 'risk', label: 'Risk Classification', icon: AlertTriangle },
    { id: 'output', label: 'Output Schema', icon: FileText }
  ];

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-display font-bold text-gray-900">Masters Configuration</h1>
          <p className="text-gray-500 mt-1">Configure the legal audit prompt and risk classification rules</p>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={loadPrompt}
            disabled={loading}
            className="btn-secondary inline-flex items-center gap-2"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            Reload
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className="btn-primary inline-flex items-center gap-2"
          >
            {saving ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Save className="w-4 h-4" />
            )}
            Save Changes
          </button>
        </div>
      </div>

      {/* Alerts */}
      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg flex items-center gap-2">
          <AlertCircle className="w-5 h-5 flex-shrink-0" />
          {error}
        </div>
      )}
      
      {success && (
        <div className="bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded-lg flex items-center gap-2">
          <CheckCircle2 className="w-5 h-5 flex-shrink-0" />
          {success}
        </div>
      )}

      {/* Version info */}
      <div className="card p-4">
        <div className="flex flex-wrap items-center gap-6 text-sm">
          <div>
            <span className="text-gray-500">Version:</span>
            <span className="ml-2 font-mono text-gray-900">{prompt?.version}</span>
          </div>
          <div>
            <span className="text-gray-500">Last Updated:</span>
            <span className="ml-2 text-gray-900">
              {prompt?.updatedAt && new Date(prompt.updatedAt).toLocaleString('en-IN')}
            </span>
          </div>
          <div>
            <span className="text-gray-500">Updated By:</span>
            <span className="ml-2 text-gray-900">{prompt?.updatedBy || '-'}</span>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="card overflow-hidden">
        <div className="border-b border-gray-200">
          <nav className="flex -mb-px overflow-x-auto">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`
                    flex items-center gap-2 px-6 py-4 text-sm font-medium border-b-2 transition-colors whitespace-nowrap
                    ${activeTab === tab.id 
                      ? 'border-primary-600 text-primary-600' 
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'}
                  `}
                >
                  <Icon className="w-4 h-4" />
                  {tab.label}
                </button>
              );
            })}
          </nav>
        </div>

        <div className="p-6">
          {/* System Role Tab */}
          {activeTab === 'system' && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  System Role Prompt
                </label>
                <p className="text-sm text-gray-500 mb-3">
                  This defines Claude's persona and primary directive for analyzing legal documents.
                </p>
                <textarea
                  value={prompt?.systemRole || ''}
                  onChange={(e) => setPrompt(prev => ({ ...prev, systemRole: e.target.value }))}
                  rows={10}
                  className="input font-mono text-sm"
                  placeholder="Enter the system role..."
                />
              </div>
            </div>
          )}

          {/* Review Scope Tab */}
          {activeTab === 'scope' && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Scope of Review
                </label>
                <p className="text-sm text-gray-500 mb-3">
                  Define what areas the AI should examine in each document.
                </p>
                {prompt?.scope?.map((item, index) => (
                  <div key={index} className="flex gap-2 mb-2">
                    <input
                      type="text"
                      value={item}
                      onChange={(e) => {
                        const newScope = [...prompt.scope];
                        newScope[index] = e.target.value;
                        setPrompt(prev => ({ ...prev, scope: newScope }));
                      }}
                      className="input flex-1"
                    />
                    <button
                      onClick={() => {
                        setPrompt(prev => ({
                          ...prev,
                          scope: prev.scope.filter((_, i) => i !== index)
                        }));
                      }}
                      className="px-3 py-2 text-red-600 hover:bg-red-50 rounded-lg"
                    >
                      Remove
                    </button>
                  </div>
                ))}
                <button
                  onClick={() => setPrompt(prev => ({ ...prev, scope: [...prev.scope, ''] }))}
                  className="btn-secondary text-sm mt-2"
                >
                  + Add Scope Item
                </button>
              </div>
            </div>
          )}

          {/* Risk Classification Tab */}
          {activeTab === 'risk' && (
            <div className="space-y-8">
              {/* High Risk */}
              <div>
                <div className="flex items-center gap-2 mb-3">
                  <div className="w-3 h-3 rounded-full bg-red-500"></div>
                  <h3 className="font-medium text-gray-900">High Risk Criteria</h3>
                  <span className="badge-high">Critical</span>
                </div>
                <p className="text-sm text-gray-500 mb-3">
                  Documents matching any of these criteria will be marked as High Risk.
                </p>
                {prompt?.riskClassification?.high?.map((item, index) => (
                  <div key={index} className="flex gap-2 mb-2">
                    <input
                      type="text"
                      value={item}
                      onChange={(e) => updateRiskClassification('high', index, e.target.value)}
                      className="input flex-1 border-red-200 focus:border-red-400 focus:ring-red-400"
                    />
                    <button
                      onClick={() => removeRiskItem('high', index)}
                      className="px-3 py-2 text-red-600 hover:bg-red-50 rounded-lg"
                    >
                      ×
                    </button>
                  </div>
                ))}
                <button
                  onClick={() => addRiskItem('high')}
                  className="text-sm text-red-600 hover:text-red-700 font-medium mt-2"
                >
                  + Add High Risk Criterion
                </button>
              </div>

              {/* Medium Risk */}
              <div>
                <div className="flex items-center gap-2 mb-3">
                  <div className="w-3 h-3 rounded-full bg-amber-500"></div>
                  <h3 className="font-medium text-gray-900">Medium Risk Criteria</h3>
                  <span className="badge-medium">Attention Needed</span>
                </div>
                <p className="text-sm text-gray-500 mb-3">
                  Documents matching these criteria will be marked as Medium Risk.
                </p>
                {prompt?.riskClassification?.medium?.map((item, index) => (
                  <div key={index} className="flex gap-2 mb-2">
                    <input
                      type="text"
                      value={item}
                      onChange={(e) => updateRiskClassification('medium', index, e.target.value)}
                      className="input flex-1 border-amber-200 focus:border-amber-400 focus:ring-amber-400"
                    />
                    <button
                      onClick={() => removeRiskItem('medium', index)}
                      className="px-3 py-2 text-amber-600 hover:bg-amber-50 rounded-lg"
                    >
                      ×
                    </button>
                  </div>
                ))}
                <button
                  onClick={() => addRiskItem('medium')}
                  className="text-sm text-amber-600 hover:text-amber-700 font-medium mt-2"
                >
                  + Add Medium Risk Criterion
                </button>
              </div>

              {/* Low Risk */}
              <div>
                <div className="flex items-center gap-2 mb-3">
                  <div className="w-3 h-3 rounded-full bg-green-500"></div>
                  <h3 className="font-medium text-gray-900">Low Risk Criteria</h3>
                  <span className="badge-low">Clean</span>
                </div>
                <p className="text-sm text-gray-500 mb-3">
                  Documents matching these criteria will be marked as Low Risk.
                </p>
                {prompt?.riskClassification?.low?.map((item, index) => (
                  <div key={index} className="flex gap-2 mb-2">
                    <input
                      type="text"
                      value={item}
                      onChange={(e) => updateRiskClassification('low', index, e.target.value)}
                      className="input flex-1 border-green-200 focus:border-green-400 focus:ring-green-400"
                    />
                    <button
                      onClick={() => removeRiskItem('low', index)}
                      className="px-3 py-2 text-green-600 hover:bg-green-50 rounded-lg"
                    >
                      ×
                    </button>
                  </div>
                ))}
                <button
                  onClick={() => addRiskItem('low')}
                  className="text-sm text-green-600 hover:text-green-700 font-medium mt-2"
                >
                  + Add Low Risk Criterion
                </button>
              </div>
            </div>
          )}

          {/* Output Schema Tab */}
          {activeTab === 'output' && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Output Schema Fields
                </label>
                <p className="text-sm text-gray-500 mb-3">
                  These are the columns that will appear in the generated Excel report.
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                  {prompt?.outputSchema?.map((field, index) => (
                    <div key={index} className="flex items-center gap-2 bg-gray-50 rounded-lg p-2">
                      <span className="text-xs text-gray-400 w-6">{index + 1}.</span>
                      <span className="text-sm text-gray-700 flex-1">{field}</span>
                    </div>
                  ))}
                </div>
                <p className="text-xs text-gray-400 mt-4">
                  Note: Output schema is currently read-only. Contact support to modify.
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
