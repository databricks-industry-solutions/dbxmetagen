import React, { useState, useEffect } from 'react'
import { ErrorBanner } from '../App'

const DOC_TYPES = [
  { value: '', label: 'All Types' },
  { value: 'table', label: 'Table' },
  { value: 'column', label: 'Column' },
  { value: 'entity', label: 'Entity' },
  { value: 'metric_view', label: 'Metric View' },
  { value: 'fk_relationship', label: 'FK Relationship' },
]

const DOC_TYPE_COLORS = {
  table: 'bg-blue-100 text-blue-700',
  column: 'bg-emerald-100 text-emerald-700',
  entity: 'bg-violet-100 text-violet-700',
  metric_view: 'bg-amber-100 text-amber-700',
  fk_relationship: 'bg-rose-100 text-rose-700',
}

function DocTypeBadge({ type }) {
  const color = DOC_TYPE_COLORS[type] || 'bg-dbx-oat text-slate-600 dark:bg-dbx-navy-500 dark:text-slate-300'
  return <span className={`badge ${color}`}>{type}</span>
}

function classifyState(state) {
  if (!state) return 'loading'
  const s = String(state).toUpperCase()
  if (s === 'NOT_FOUND') return 'not_found'
  if (s === 'ERROR') return 'error'
  if (s === 'ONLINE' || s === 'PROVISIONING') return 'ok'
  if (s.includes('NOT_FOUND') || s.includes('NOT FOUND')) return 'not_found'
  if (s.includes('ERROR')) return 'error'
  return 'ok'
}

export default function VectorSearch() {
  const [status, setStatus] = useState(null)
  const [error, setError] = useState(null)
  const [query, setQuery] = useState('')
  const [docType, setDocType] = useState('')
  const [numResults, setNumResults] = useState(5)
  const [queryType, setQueryType] = useState('ANN')
  const [results, setResults] = useState(null)
  const [searching, setSearching] = useState(false)
  const [syncing, setSyncing] = useState(false)

  const loadStatus = () => {
    fetch('/api/vector/status').then(r => r.ok ? r.json() : null).then(setStatus).catch(() => {})
  }

  useEffect(() => { loadStatus() }, [])

  const doSearch = async () => {
    if (!query.trim()) return
    setSearching(true)
    setError(null)
    try {
      const res = await fetch('/api/vector/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: query.trim(), doc_type: docType || null, num_results: numResults, query_type: queryType }),
      })
      const data = await res.json()
      if (!res.ok) { setError(data.detail || 'Search failed'); setResults(null) }
      else setResults(data)
    } catch (e) { setError(e.message) }
    setSearching(false)
  }

  const doSync = async () => {
    setSyncing(true)
    setError(null)
    try {
      const res = await fetch('/api/vector/sync', { method: 'POST' })
      if (!res.ok) { const d = await res.json(); setError(d.detail || 'Sync failed') }
      else loadStatus()
    } catch (e) { setError(e.message) }
    setSyncing(false)
  }

  const handleKey = (e) => {
    if (e.key === 'Enter') { e.preventDefault(); doSearch() }
  }

  const epClass = status ? classifyState(status.endpoint_state) : 'loading'
  const idxClass = status ? classifyState(status.index_status) : 'loading'
  const endpointMissing = epClass === 'not_found'
  const endpointError = epClass === 'error'
  const indexMissing = idxClass === 'not_found' || idxClass === 'error'

  return (
    <div className="space-y-5 max-w-full overflow-hidden">
      <ErrorBanner error={error} />

      {/* Index Status */}
      <div className="card p-5">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100">Index Status</h2>
          <div className="flex gap-2">
            <button onClick={doSync} disabled={syncing || endpointMissing || endpointError}
              className="btn-secondary btn-sm">{syncing ? 'Syncing...' : 'Sync Now'}</button>
            <button onClick={loadStatus} className="btn-ghost btn-sm">Refresh</button>
          </div>
        </div>

        {endpointMissing ? (
          <div className="bg-amber-50 border border-amber-200 rounded-lg p-4 text-sm">
            <p className="font-medium text-amber-800 mb-1">Vector Search endpoint not yet created</p>
            <p className="text-amber-700">
              The endpoint <code className="bg-amber-100 px-1 rounded">{status.endpoint_name}</code> does not exist yet.
              Run the <strong>Build Vector Index</strong> job from the Batch Jobs tab, or redeploy with <code className="bg-amber-100 px-1 rounded">./deploy.sh</code> which will create it automatically.
            </p>
          </div>
        ) : endpointError ? (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-sm">
            <p className="font-medium text-red-800 mb-1">Error checking Vector Search endpoint</p>
            <p className="text-red-700 mb-2">
              Could not query endpoint <code className="bg-red-100 px-1 rounded">{status.endpoint_name}</code>.
              This is usually a permission or SDK issue, not a missing endpoint.
            </p>
            {status.endpoint_error && (
              <details className="text-xs text-red-600">
                <summary className="cursor-pointer hover:underline">Show error details</summary>
                <pre className="mt-1 bg-red-100 p-2 rounded whitespace-pre-wrap break-all">{status.endpoint_error}</pre>
              </details>
            )}
          </div>
        ) : status ? (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div className="min-w-0">
              <span className="text-xs text-gray-500 block">Endpoint</span>
              <span className="font-medium">{status.endpoint_name}</span>
              <span className={`ml-2 px-1.5 py-0.5 rounded text-xs ${status.endpoint_state === 'ONLINE' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'}`}>
                {status.endpoint_state}
              </span>
            </div>
            <div className="min-w-0">
              <span className="text-xs text-gray-500 block">Index</span>
              {indexMissing ? (
                <span className="text-xs text-amber-600">Not yet created -- run Build Vector Index</span>
              ) : (
                <span className="font-mono text-xs break-all">{status.index_name}</span>
              )}
            </div>
            <div className="min-w-0">
              <span className="text-xs text-gray-500 block">Total Documents</span>
              <span className="text-2xl font-bold text-dbx-navy">{status.total_documents}</span>
            </div>
            <div className="min-w-0">
              <span className="text-xs text-gray-500 block">By Type</span>
              <div className="flex flex-wrap gap-1 mt-1">
                {Object.entries(status.doc_counts || {}).map(([k, v]) => (
                  <span key={k} className="text-xs"><DocTypeBadge type={k} /> {v}</span>
                ))}
              </div>
            </div>
          </div>
        ) : (
          <p className="text-sm text-gray-500">Loading status...</p>
        )}
      </div>

      {/* Search Form */}
      <div className="card p-5">
        <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100 mb-3">Similarity Search</h2>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3 mb-3">
          <div className="md:col-span-2">
            <input value={query} onChange={e => setQuery(e.target.value)} onKeyDown={handleKey}
              placeholder="Search metadata (e.g. 'patient encounter tables')"
              className="input-base" />
          </div>
          <div>
            <select value={docType} onChange={e => setDocType(e.target.value)}
              className="select-base">
              {DOC_TYPES.map(dt => <option key={dt.value} value={dt.value}>{dt.label}</option>)}
            </select>
          </div>
          <div className="flex gap-2 items-center">
            <label className="text-xs text-gray-500 whitespace-nowrap">Results: {numResults}</label>
            <input type="range" min={1} max={20} value={numResults} onChange={e => setNumResults(Number(e.target.value))}
              className="flex-1" />
          </div>
        </div>
        <div className="flex items-center gap-3">
          <button onClick={doSearch} disabled={searching || !query.trim() || endpointMissing || endpointError}
            className="btn-secondary btn-md">{searching ? 'Searching...' : 'Search'}</button>
          <label className="flex items-center gap-2 text-sm text-gray-600">
            <input type="checkbox" checked={queryType === 'HYBRID'}
              onChange={e => setQueryType(e.target.checked ? 'HYBRID' : 'ANN')} />
            Hybrid search (vector + keyword)
          </label>
        </div>
      </div>

      {/* Results */}
      {results && (
        <div className="card p-5 overflow-hidden">
          <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100 mb-3">
            Results <span className="text-sm font-normal text-gray-500">({results.count} matches, {results.query_type})</span>
          </h2>
          {results.matches && results.matches.length > 0 ? (
            <div className="space-y-3">
              {results.matches.map((m, i) => (
                <div key={i} className="card-interactive p-4 overflow-hidden">
                  <div className="flex items-center gap-2 mb-1.5 min-w-0">
                    <DocTypeBadge type={m.doc_type} />
                    <span className="font-mono text-xs text-gray-600 truncate min-w-0 flex-1">{m.doc_id}</span>
                    {m.score != null && (
                      <span className="text-xs font-medium text-gray-500 flex-shrink-0">score: {Number(m.score).toFixed(3)}</span>
                    )}
                  </div>
                  <p className="text-sm text-gray-700 whitespace-pre-wrap line-clamp-4 max-w-full break-words">
                    {typeof m.content === 'string' ? m.content.slice(0, 500) : JSON.stringify(m.content)}
                  </p>
                  <div className="flex flex-wrap gap-3 mt-1.5 text-xs text-gray-400">
                    {m.table_name && <span>table: {m.table_name}</span>}
                    {m.domain && <span>domain: {m.domain}</span>}
                    {m.entity_type && <span>entity: {m.entity_type}</span>}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-sm text-gray-500">No matches found.</p>
          )}
        </div>
      )}
    </div>
  )
}
