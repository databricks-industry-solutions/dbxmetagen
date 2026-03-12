import React, { useState, useEffect, useRef } from 'react'
import { safeFetch, safeFetchObj, ErrorBanner } from '../App'

const STAGES = {
  starting: 'Starting...',
  gathering_context: 'Gathering metadata context...',
  initializing: 'Initializing agent...',
  agent_running: 'Agent generating configuration...',
  generating: 'Generating SQL & instructions...',
  parsing: 'Parsing output...',
  done: 'Complete',
  error: 'Failed',
}

export default function GenieBuilder() {
  const [tables, setTables] = useState([])
  const [selectedTables, setSelectedTables] = useState([])
  const [metricViews, setMetricViews] = useState([])
  const [selectedMVs, setSelectedMVs] = useState(new Set())
  const [showMVs, setShowMVs] = useState(false)
  const [questions, setQuestions] = useState('')
  const [suggestingQs, setSuggestingQs] = useState(false)
  const [taskId, setTaskId] = useState(null)
  const [taskStatus, setTaskStatus] = useState(null)
  const [result, setResult] = useState(null)
  const [editedJson, setEditedJson] = useState('')
  const [title, setTitle] = useState('')
  const [createdSpace, setCreatedSpace] = useState(null)
  const [trackedSpaces, setTrackedSpaces] = useState([])
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const pollRef = useRef(null)

  useEffect(() => {
    (async () => {
      const { data } = await safeFetch('/api/coverage/tables')
      if (data.length) {
        setTables(data.map(r => ({
          id: `${r.table_catalog}.${r.table_schema}.${r.table_name}`,
          label: r.table_name,
          schema: r.table_schema,
          tableType: r.table_type,
        })))
      }
    })()
    fetch('/api/semantic/metric-views?status=all')
      .then(r => r.ok ? r.json() : []).then(setMetricViews).catch(() => {})
    loadTrackedSpaces()
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [])

  const loadTrackedSpaces = () => {
    fetch('/api/genie/spaces').then(r => r.ok ? r.json() : []).then(setTrackedSpaces).catch(() => {})
  }

  useEffect(() => {
    if (!metricViews.length) return
    const sel = new Set()
    metricViews.forEach(mv => {
      if (selectedTables.includes(mv.source_table)) sel.add(mv.metric_view_name)
    })
    setSelectedMVs(sel)
  }, [selectedTables, metricViews])

  const toggleTable = (id) => {
    setSelectedTables(prev =>
      prev.includes(id) ? prev.filter(t => t !== id) : [...prev, id]
    )
  }

  const toggleMV = (name) => {
    setSelectedMVs(prev => {
      const next = new Set(prev)
      next.has(name) ? next.delete(name) : next.add(name)
      return next
    })
  }

  const suggestQuestions = async () => {
    if (!selectedTables.length) return setError('Select at least one table first')
    setSuggestingQs(true); setError(null)
    try {
      const res = await fetch('/api/genie/generate-questions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_identifiers: selectedTables, metric_view_names: [...selectedMVs] }),
      })
      if (!res.ok) { const b = await res.json().catch(() => ({})); setError(b.detail || `Error ${res.status}`); return }
      const { questions: qs } = await res.json()
      const existing = questions.trim()
      setQuestions(existing ? existing + '\n' + qs.join('\n') : qs.join('\n'))
    } catch (e) { setError(e.message) }
    finally { setSuggestingQs(false) }
  }

  const startGeneration = async () => {
    if (!selectedTables.length) return setError('Select at least one table')
    setError(null); setResult(null); setCreatedSpace(null); setLoading(true)
    const qs = questions.split('\n').map(q => q.trim()).filter(Boolean)
    const res = await fetch('/api/genie/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ table_identifiers: selectedTables, questions: qs, metric_view_names: [...selectedMVs] }),
    })
    if (!res.ok) {
      const body = await res.json().catch(() => ({}))
      setError(body.detail || `Error ${res.status}`); setLoading(false); return
    }
    const { task_id } = await res.json()
    setTaskId(task_id); setTaskStatus({ status: 'running', stage: 'starting' })

    let missCount = 0
    pollRef.current = setInterval(async () => {
      const { data } = await safeFetchObj(`/api/genie/tasks/${task_id}`)
      if (!data) {
        missCount++
        if (missCount >= 3) {
          clearInterval(pollRef.current)
          setError('Task lost (app may have restarted). Please try again.')
          setLoading(false)
        }
        return
      }
      missCount = 0
      setTaskStatus(data)
      if (data.status === 'done') {
        clearInterval(pollRef.current)
        setResult(data.result)
        setEditedJson(JSON.stringify(data.result, null, 2))
        setLoading(false)
      } else if (data.status === 'error') {
        clearInterval(pollRef.current)
        setError(data.error || 'Agent failed')
        setLoading(false)
      }
    }, 3000)
  }

  const createSpace = async () => {
    if (!title.trim()) return setError('Enter a title for the Genie space')
    setError(null)
    let parsed
    try { parsed = JSON.parse(editedJson) } catch { return setError('Invalid JSON') }
    const res = await fetch('/api/genie/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: title.trim(),
        description: parsed.description || undefined,
        serialized_space: parsed,
      }),
    })
    if (!res.ok) {
      const body = await res.json().catch(() => ({}))
      return setError(body.detail || `Error ${res.status}`)
    }
    const created = await res.json()
    setCreatedSpace(created)
    if (created.space_id) {
      fetch(`/api/genie/spaces/track?space_id=${encodeURIComponent(created.space_id)}&title=${encodeURIComponent(title)}${selectedTables.map(t => `&tables=${encodeURIComponent(t)}`).join('')}&config_json=${encodeURIComponent(editedJson || '')}`, { method: 'POST' }).catch(() => {})
      loadTrackedSpaces()
    }
  }

  const schemas = [...new Set(tables.map(t => t.schema))]
  const [openSchemas, setOpenSchemas] = useState(new Set())

  const toggleSchema = (schema) => {
    setOpenSchemas(prev => {
      const next = new Set(prev)
      next.has(schema) ? next.delete(schema) : next.add(schema)
      return next
    })
  }

  const toggleAllInSchema = (schema) => {
    const ids = tables.filter(t => t.schema === schema).map(t => t.id)
    const allSelected = ids.every(id => selectedTables.includes(id))
    setSelectedTables(prev =>
      allSelected ? prev.filter(id => !ids.includes(id)) : [...new Set([...prev, ...ids])]
    )
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-lg font-semibold text-slate-800 dark:text-slate-200">Genie Space Builder</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
          Select tables and optionally provide business questions. An agent will generate
          a comprehensive Genie space configuration with instructions, SQL, filters, and measures.
        </p>
      </div>

      <ErrorBanner error={error} />

      {/* Table selection */}
      <div className="bg-dbx-oat-light dark:bg-[#152E35] rounded-lg border border-slate-200 dark:border-slate-700 p-4">
        <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-3">Select Tables</h3>
        {schemas.map(schema => {
          const schemaTables = tables.filter(t => t.schema === schema)
          const schemaIds = schemaTables.map(t => t.id)
          const selectedCount = schemaIds.filter(id => selectedTables.includes(id)).length
          const allSelected = selectedCount === schemaIds.length
          const isOpen = openSchemas.has(schema)

          return (
            <div key={schema} className="mb-2 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
              <button
                onClick={() => toggleSchema(schema)}
                className="w-full flex items-center justify-between px-3 py-2 bg-dbx-oat dark:bg-slate-700/50 hover:bg-dbx-oat dark:hover:bg-slate-700 transition-colors"
              >
                <div className="flex items-center gap-2">
                  <span className={`text-xs transition-transform ${isOpen ? 'rotate-90' : ''}`}>&#9654;</span>
                  <span className="text-xs font-medium text-slate-600 dark:text-slate-300">{schema}</span>
                  <span className="text-[10px] text-slate-400">({selectedCount}/{schemaIds.length})</span>
                </div>
                <span
                  onClick={e => { e.stopPropagation(); toggleAllInSchema(schema) }}
                  title="Toggle selection of all tables in this schema"
                  className="text-[10px] px-2 py-0.5 rounded border border-slate-300 dark:border-slate-600 text-slate-500 dark:text-slate-400 hover:bg-dbx-oat-dark dark:hover:bg-slate-600 cursor-pointer"
                >
                  {allSelected ? 'Deselect all' : 'Select all'}
                </span>
              </button>
              {isOpen && (
                <div className="flex flex-wrap gap-2 p-3">
                  {schemaTables.map(t => (
                    <button
                      key={t.id}
                      onClick={() => toggleTable(t.id)}
                      className={`px-3 py-1.5 text-xs rounded-md border transition-colors ${selectedTables.includes(t.id)
                        ? 'bg-orange-100 dark:bg-orange-900 border-orange-400 text-red-700 dark:text-orange-300'
                        : 'bg-dbx-oat-light dark:bg-slate-700 border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-300 hover:bg-dbx-oat dark:hover:bg-slate-600'
                        }`}
                    >
                      {t.label}
                      {t.tableType && t.tableType !== 'MANAGED' && (
                        <span className="ml-1 text-[10px] text-purple-500 dark:text-purple-400">
                          {({ VIEW: 'V', EXTERNAL: 'EXT', STREAMING_TABLE: 'ST', MATERIALIZED_VIEW: 'MV', FOREIGN: 'FT' })[t.tableType] || t.tableType}
                        </span>
                      )}
                    </button>
                  ))}
                </div>
              )}
            </div>
          )
        })}
        {!tables.length && <p className="text-sm text-slate-400">Loading tables...</p>}
      </div>

      {/* Metric Views */}
      {metricViews.length > 0 && (
        <div className="bg-dbx-oat-light dark:bg-[#152E35] rounded-lg border border-slate-200 dark:border-slate-700 p-4">
          <button onClick={() => setShowMVs(!showMVs)}
            className="w-full flex items-center justify-between text-sm font-medium text-slate-700 dark:text-slate-300">
            <span>Metric Views ({selectedMVs.size}/{metricViews.length} selected)</span>
            <span className={`text-xs transition-transform ${showMVs ? 'rotate-90' : ''}`}>&#9654;</span>
          </button>
          {showMVs && (
            <div className="flex flex-wrap gap-2 mt-3">
              {metricViews.map(mv => (
                <button key={mv.metric_view_name} onClick={() => toggleMV(mv.metric_view_name)}
                  className={`px-3 py-1.5 text-xs rounded-md border transition-colors ${selectedMVs.has(mv.metric_view_name)
                    ? 'bg-violet-100 dark:bg-violet-900 border-violet-400 text-violet-700 dark:text-violet-300'
                    : 'bg-dbx-oat-light dark:bg-slate-700 border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-300 hover:bg-dbx-oat dark:hover:bg-slate-600'
                  }`}>
                  {mv.metric_view_name}
                  <span className={`ml-1 text-[10px] ${mv.status === 'applied' ? 'text-green-500' : 'text-amber-500'}`}>
                    {mv.status === 'applied' ? 'LIVE' : mv.status?.toUpperCase()}
                  </span>
                </button>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Questions */}
      <div className="bg-dbx-oat-light dark:bg-[#152E35] rounded-lg border border-slate-200 dark:border-slate-700 p-4">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300">Business Questions (optional)</h3>
          <button onClick={suggestQuestions} disabled={suggestingQs || !selectedTables.length}
            className="text-xs px-3 py-1 bg-dbx-navy text-white rounded hover:bg-slate-700 disabled:opacity-50 transition-colors flex items-center gap-1.5">
            {suggestingQs && <span className="w-3 h-3 border-2 border-white border-t-transparent rounded-full animate-spin" />}
            {suggestingQs ? 'Generating...' : 'Suggest Questions'}
          </button>
        </div>
        <textarea
          value={questions}
          onChange={e => setQuestions(e.target.value)}
          rows={3}
          placeholder="One question per line..."
          className="w-full border border-slate-200 dark:border-slate-600 rounded-md px-3 py-2 text-sm bg-dbx-oat-light dark:bg-slate-700 text-slate-800 dark:text-slate-200 placeholder-slate-400"
        />
      </div>

      {/* Generate button */}
      <button
        onClick={startGeneration}
        disabled={loading || !selectedTables.length}
        className="px-5 py-2.5 bg-dbx-lava text-white text-sm font-medium rounded-lg hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      >
        {loading ? 'Generating...' : 'Generate Genie Config'}
      </button>

      {/* Progress */}
      {taskStatus && taskStatus.status === 'running' && (
        <div className="bg-orange-50 dark:bg-orange-900/30 border border-orange-200 dark:border-red-700 rounded-lg px-4 py-3">
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 border-2 border-orange-400 border-t-transparent rounded-full animate-spin" />
            <span className="text-sm font-medium text-red-700 dark:text-orange-300">
              {STAGES[taskStatus.stage] || taskStatus.stage}
            </span>
          </div>
        </div>
      )}

      {/* Result JSON */}
      {result && (
        <div className="space-y-4">
          <div className="bg-dbx-oat-light dark:bg-[#152E35] rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">Generated Configuration</h3>
            <textarea
              value={editedJson}
              onChange={e => setEditedJson(e.target.value)}
              rows={20}
              className="w-full font-mono text-xs border border-slate-200 dark:border-slate-600 rounded-md p-3 bg-dbx-oat dark:bg-slate-900 text-slate-800 dark:text-slate-200"
            />
          </div>

          <div className="flex items-end gap-3">
            <div className="flex-1">
              <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Genie Space Title</label>
              <input
                type="text"
                value={title}
                onChange={e => setTitle(e.target.value)}
                placeholder="My Analytics Space"
                title="Name for the Genie space (used as display name in Databricks)"
                className="w-full border border-slate-200 dark:border-slate-600 rounded-md px-3 py-2 text-sm bg-dbx-oat-light dark:bg-slate-700 text-slate-800 dark:text-slate-200"
              />
            </div>
            <button
              onClick={createSpace}
              disabled={!title.trim()}
              title="Create a Databricks Genie space from the selected tables with AI-generated instructions and example queries"
              className="px-5 py-2.5 bg-emerald-600 text-white text-sm font-medium rounded-lg hover:bg-emerald-700 disabled:opacity-50 transition-colors"
            >
              Create Genie Space
            </button>
          </div>

          {createdSpace && (
            <div className="bg-emerald-50 dark:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-700 rounded-lg px-4 py-3 text-sm text-emerald-700 dark:text-emerald-300">
              Genie space created! ID: <span className="font-mono">{createdSpace.space_id}</span>
            </div>
          )}
        </div>
      )}

      {/* Tracked Spaces */}
      {trackedSpaces.length > 0 && (
        <div className="bg-dbx-oat-light dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-6 shadow-sm mt-6">
          <h2 className="text-lg font-semibold text-slate-800 dark:text-slate-200 mb-4">My Genie Spaces</h2>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead><tr>
                {['Title', 'Space ID', 'Version', 'Tables', 'Last Updated', ''].map(h =>
                  <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat dark:bg-slate-700 font-semibold text-slate-600 dark:text-slate-300 border-b border-slate-200 dark:border-slate-600 text-xs uppercase tracking-wider">{h}</th>)}
              </tr></thead>
              <tbody>
                {trackedSpaces.map((s, i) => (
                  <tr key={i} className="border-b border-slate-100 dark:border-slate-700 hover:bg-orange-50/30 dark:hover:bg-slate-700/50 transition-colors">
                    <td className="px-3 py-2 font-medium text-slate-700 dark:text-slate-200">{s.title}</td>
                    <td className="px-3 py-2 font-mono text-xs text-slate-500">{s.space_id}</td>
                    <td className="px-3 py-2 text-slate-600 dark:text-slate-400">{s.version || 1}</td>
                    <td className="px-3 py-2 text-slate-600 dark:text-slate-400 text-xs">{Array.isArray(s.tables) ? s.tables.length : 0}</td>
                    <td className="px-3 py-2 text-xs text-slate-500">{s.updated_at ? new Date(s.updated_at).toLocaleString() : ''}</td>
                    <td className="px-3 py-2">
                      <button onClick={async () => {
                        if (!confirm(`Delete Genie space "${s.title}"?`)) return
                        await fetch(`/api/genie/spaces/${s.space_id}`, { method: 'DELETE' })
                        loadTrackedSpaces()
                      }} className="text-xs text-red-600 hover:text-red-800 hover:underline">Delete</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
