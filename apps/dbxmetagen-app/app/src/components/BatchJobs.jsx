import React, { useState, useEffect } from 'react'
import { safeFetchObj, ErrorBanner } from '../App'

export default function BatchJobs() {
  const [jobs, setJobs] = useState([])
  const [tableNames, setTableNames] = useState('')
  const [mode, setMode] = useState('comment')
  const [applyDdl, setApplyDdl] = useState(false)
  const [catalogName, setCatalogName] = useState('')
  const [schemaName, setSchemaName] = useState('')
  const [runStatus, setRunStatus] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [ontologyBundle, setOntologyBundle] = useState('healthcare')
  const [bundles, setBundles] = useState([])

  useEffect(() => {
    setError(null)
    fetch('/api/jobs').then(r => {
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
      return r.json()
    }).then(setJobs)
      .catch(e => setError(`Failed to load jobs: ${e.message}`))
  }, [])

  useEffect(() => {
    safeFetchObj('/api/config').then(({ data: cfg }) => {
      if (cfg) {
        setCatalogName(cfg.catalog_name || '')
        setSchemaName(cfg.schema_name || '')
      }
    })
    fetch('/api/ontology/bundles').then(r => r.ok ? r.json() : []).then(setBundles).catch(() => {})
  }, [])

  const findJob = (suffix) => jobs.find(j => j.name?.endsWith(suffix))

  const runJob = async (jobNameSuffix, params = {}) => {
    setLoading(true)
    setError(null)
    try {
      const match = findJob(jobNameSuffix)
      const body = match
        ? { job_id: match.job_id, ...params }
        : { job_name: jobNameSuffix, ...params }
      const res = await fetch('/api/jobs/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await res.json()
      if (!res.ok) { setError(data.detail || 'Failed to start job'); setLoading(false); return }
      setRunStatus({ ...data, job_name: match?.name || jobNameSuffix, state: 'PENDING' })
    } catch (e) { setError(e.message) }
    setLoading(false)
  }

  const checkStatus = async () => {
    if (!runStatus?.run_id) return
    try {
      const res = await fetch(`/api/jobs/${runStatus.run_id}/status`)
      if (!res.ok) { setError(`Status check failed: ${res.status}`); return }
      const data = await res.json()
      setRunStatus(prev => ({ ...prev, ...data }))
    } catch (e) { setError(`Status check error: ${e.message}`) }
  }

  return (
    <div className="space-y-6">
      <ErrorBanner error={error} />

      {/* Metadata generation */}
      <section className="bg-white rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-4">Run Metadata Generation</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Table Names</label>
            <textarea value={tableNames} onChange={e => setTableNames(e.target.value)}
              placeholder="catalog.schema.table1, catalog.schema.*"
              className="w-full border rounded-md p-2 text-sm h-24" />
          </div>
          <div className="space-y-3">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Mode</label>
              <select value={mode} onChange={e => setMode(e.target.value)} className="w-full border rounded-md p-2 text-sm">
                <option value="comment">Comment</option>
                <option value="pi">PI Classification</option>
                <option value="domain">Domain Classification</option>
              </select>
            </div>
            <label className="flex items-center gap-2 text-sm">
              <input type="checkbox" checked={applyDdl} onChange={e => setApplyDdl(e.target.checked)} />
              Apply DDL to tables
            </label>
          </div>
        </div>
        <div className="flex gap-3 mt-4">
          <button onClick={() => runJob('_metadata_job', { table_names: tableNames, mode, apply_ddl: applyDdl })}
            disabled={loading || !tableNames.trim()}
            className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm hover:bg-blue-700 disabled:opacity-50">
            {loading ? 'Starting...' : 'Run Single Mode'}
          </button>
          <button onClick={() => runJob('_parallel_modes_job', { table_names: tableNames })}
            disabled={loading || !tableNames.trim()}
            className="px-4 py-2 bg-indigo-600 text-white rounded-md text-sm hover:bg-indigo-700 disabled:opacity-50">
            Run All 3 Modes (Parallel)
          </button>
        </div>
      </section>

      {/* Analytics pipeline */}
      <section className="bg-white rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-4">Analytics Pipeline</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Catalog Name</label>
            <input value={catalogName} onChange={e => setCatalogName(e.target.value)}
              placeholder="e.g. eswanson" className="w-full border rounded-md p-2 text-sm" />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Schema Name</label>
            <input value={schemaName} onChange={e => setSchemaName(e.target.value)}
              placeholder="e.g. metadata_results" className="w-full border rounded-md p-2 text-sm" />
          </div>
        </div>
        <div className="flex gap-3 items-end">
          <button onClick={() => runJob('_full_analytics_pipeline', { catalog_name: catalogName, schema_name: schemaName, ontology_bundle: ontologyBundle })}
            disabled={loading}
            className="px-4 py-2 bg-emerald-600 text-white rounded-md text-sm hover:bg-emerald-700 disabled:opacity-50">
            Full Analytics Pipeline
          </button>
          <button onClick={() => runJob('sync_graph_lakebase', { catalog_name: catalogName, schema_name: schemaName })}
            disabled={loading}
            className="px-4 py-2 bg-violet-600 text-white rounded-md text-sm hover:bg-violet-700 disabled:opacity-50">
            Sync Graph to Lakebase
          </button>
        </div>
      </section>

      {/* Ontology Prediction */}
      <section className="bg-white rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-4">Ontology Prediction</h2>
        <p className="text-sm text-gray-500 mb-4">
          Discover entity types from table and column metadata, validate against ontology config, and add entity relationships to the knowledge graph.
        </p>
        <div className="mb-4">
          <label className="block text-sm font-medium text-gray-700 mb-1">Ontology Bundle</label>
          <select value={ontologyBundle} onChange={e => setOntologyBundle(e.target.value)}
            className="w-full max-w-xs border rounded-md p-2 text-sm">
            {bundles.length > 0 ? bundles.map(b => (
              <option key={b.key} value={b.key}>{b.name} ({b.entity_count} entities, {b.domain_count} domains)</option>
            )) : (
              <>
                <option value="healthcare">Healthcare</option>
                <option value="general">General</option>
              </>
            )}
          </select>
          {bundles.find(b => b.key === ontologyBundle)?.description && (
            <p className="text-xs text-gray-400 mt-1">{bundles.find(b => b.key === ontologyBundle).description}</p>
          )}
        </div>
        <button onClick={() => runJob('_ontology_prediction', { catalog_name: catalogName, schema_name: schemaName, ontology_bundle: ontologyBundle })}
          disabled={loading}
          className="px-4 py-2 bg-teal-600 text-white rounded-md text-sm hover:bg-teal-700 disabled:opacity-50">
          Run Ontology Prediction
        </button>
      </section>

      {/* FK Prediction */}
      <section className="bg-white rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-4">Foreign Key Prediction</h2>
        <p className="text-sm text-gray-500 mb-4">
          Predict FK relationships between columns using embedding similarity, rule-based scoring, and AI judgment.
          Uses the catalog/schema from the Analytics Pipeline section above.
        </p>
        <div className="flex gap-3">
          <button onClick={() => runJob('fk_prediction', { catalog_name: catalogName, schema_name: schemaName })}
            disabled={loading}
            className="px-4 py-2 bg-amber-600 text-white rounded-md text-sm hover:bg-amber-700 disabled:opacity-50">
            Predict Foreign Keys
          </button>
          <button onClick={() => runJob('fk_prediction', { catalog_name: catalogName, schema_name: schemaName, apply_ddl: 'true' })}
            disabled={loading}
            className="px-4 py-2 bg-red-600 text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50">
            Predict + Apply Foreign Keys
          </button>
        </div>
      </section>

      {/* Semantic Layer */}
      <section className="bg-white rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-4">Semantic Layer</h2>
        <p className="text-sm text-gray-500 mb-4">
          Generate metric views from business questions and catalog metadata, then optionally create a Genie space.
          Add questions in the Semantic Layer tab first.
        </p>
        <div className="flex gap-3">
          <button onClick={() => runJob('_semantic_layer', { catalog_name: catalogName, schema_name: schemaName })}
            disabled={loading}
            className="px-4 py-2 bg-indigo-600 text-white rounded-md text-sm hover:bg-indigo-700 disabled:opacity-50">
            Generate Semantic Layer
          </button>
        </div>
      </section>

      {runStatus && (
        <section className="bg-white rounded-lg border p-6">
          <div className="flex items-center justify-between mb-2">
            <h3 className="font-medium">Run Status</h3>
            <button onClick={checkStatus} className="text-sm text-blue-600 hover:underline">Refresh</button>
          </div>
          <div className="text-sm space-y-1">
            <p><span className="text-gray-500">Job:</span> {runStatus.job_name}</p>
            <p><span className="text-gray-500">Run ID:</span> {runStatus.run_id}</p>
            <p><span className="text-gray-500">State:</span>{' '}
              <span className={runStatus.state === 'TERMINATED' ? 'text-green-600 font-medium' : 'text-yellow-600 font-medium'}>
                {runStatus.state}
              </span>
              {runStatus.result && ` - ${runStatus.result}`}
            </p>
          </div>
        </section>
      )}

      <section className="bg-white rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-3">Available Jobs</h2>
        {jobs.length === 0 ? (
          <p className="text-sm text-gray-500">No jobs found. Run 'databricks bundle deploy' to create jobs, then restart the app.</p>
        ) : (
          <div className="divide-y">
            {jobs.map(j => (
              <div key={j.job_id} className="py-2 flex justify-between items-center text-sm">
                <span>{j.name}</span>
                <span className="text-gray-400">#{j.job_id}</span>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  )
}
