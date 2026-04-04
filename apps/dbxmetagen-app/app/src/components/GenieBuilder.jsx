import React, { useState, useEffect, useRef } from 'react'
import { safeFetchObj, ErrorBanner } from '../App'
import { cachedFetch, TTL } from '../apiCache'
import { PageHeader, EmptyState, SkeletonTable } from './ui'
import GenieUpdater from './GenieUpdater'

const STAGES = {
  starting: 'Starting...',
  gathering_context: 'Gathering metadata context...',
  initializing: 'Initializing agent...',
  agent_running: 'Agent generating configuration...',
  generating: 'Generating configuration...',
  validating_sql: 'Validating SQL expressions...',
  tool_round: 'Validating SQL...',
  parsing: 'Finalizing output...',
  recovering: 'Recovering partial result...',
  done: 'Complete',
  error: 'Failed',
}

function formatElapsed(seconds) {
  if (!seconds && seconds !== 0) return ''
  const m = Math.floor(seconds / 60)
  const s = seconds % 60
  return m > 0 ? `${m}m ${s}s` : `${s}s`
}

export default function GenieBuilder() {
  const [editingSpaceId, setEditingSpaceId] = useState(null)
  const [loadByIdValue, setLoadByIdValue] = useState('')
  const [tables, setTables] = useState([])
  const [selectedTables, setSelectedTables] = useState([])
  const [metricViews, setMetricViews] = useState([])
  const [selectedMVs, setSelectedMVs] = useState(new Set())
  const [businessContext, setBusinessContext] = useState('')
  const [questions, setQuestions] = useState('')
  const [suggestingQs, setSuggestingQs] = useState(false)
  const [taskId, setTaskId] = useState(null)
  const [taskStatus, setTaskStatus] = useState(null)
  const [result, setResult] = useState(null)
  const [editedJson, setEditedJson] = useState('')
  const [title, setTitle] = useState('')
  const [createdSpace, setCreatedSpace] = useState(null)
  const [trackedSpaces, setTrackedSpaces] = useState([])
  const [kpis, setKpis] = useState([])
  const [selectedKpis, setSelectedKpis] = useState(new Set())
  const [showKpis, setShowKpis] = useState(false)
  const [workspaceHost, setWorkspaceHost] = useState('')
  const [error, setError] = useState(null)
  const [createError, setCreateError] = useState(null)
  const [fetchErrors, setFetchErrors] = useState({})
  const [loading, setLoading] = useState(false)
  const [creating, setCreating] = useState(false)
  const [refineFeedback, setRefineFeedback] = useState('')
  const [showRefine, setShowRefine] = useState(false)
  const [tablesLoading, setTablesLoading] = useState(true)
  const [catalogs, setCatalogs] = useState([])
  const [selectedCatalog, setSelectedCatalog] = useState('')
  const [tableFilter, setTableFilter] = useState('')
  const pollRef = useRef(null)
  const [elapsed, setElapsed] = useState(0)
  const timerRef = useRef(null)
  const prevTablesRef = useRef(selectedTables)

  useEffect(() => {
    if (loading) {
      setElapsed(0)
      timerRef.current = setInterval(() => setElapsed(e => e + 1), 1000)
    } else {
      if (timerRef.current) clearInterval(timerRef.current)
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [loading])

  useEffect(() => {
    fetch('/api/catalogs').then(r => r.ok ? r.json() : []).then(setCatalogs).catch(() => {})
    fetch('/api/semantic/metric-views?status=applied')
      .then(r => r.ok ? r.json() : []).then(setMetricViews)
      .catch(e => setFetchErrors(prev => ({ ...prev, metricViews: e.message })))
    fetch('/api/kpis').then(r => r.ok ? r.json() : []).then(setKpis)
      .catch(e => setFetchErrors(prev => ({ ...prev, kpis: e.message })))
    fetch('/api/config').then(r => r.ok ? r.json() : {}).then(c => {
      setWorkspaceHost(c.workspace_host || '')
      if (c.catalog_name) setSelectedCatalog(c.catalog_name)
    }).catch(() => {})
    loadTrackedSpaces()
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [])

  const loadTrackedSpaces = () => {
    fetch('/api/genie/spaces').then(r => r.ok ? r.json() : []).then(setTrackedSpaces).catch(() => {})
  }

  const catalogChangeRef = useRef(false)
  useEffect(() => {
    if (!selectedCatalog) return
    setTablesLoading(true)
    if (catalogChangeRef.current) {
      setSelectedTables([])
      setTableFilter('')
    }
    catalogChangeRef.current = true
    ;(async () => {
      const { data } = await cachedFetch(`/api/coverage/tables?catalog=${encodeURIComponent(selectedCatalog)}`, {}, TTL.CONFIG)
      setTables(data.length ? data.map(r => ({
        id: `${r.table_catalog}.${r.table_schema}.${r.table_name}`,
        label: r.table_name,
        schema: r.table_schema,
        tableType: r.table_type,
      })) : [])
      setTablesLoading(false)
    })()
  }, [selectedCatalog])

  useEffect(() => {
    if (!metricViews.length) return
    const prev = new Set(prevTablesRef.current)
    const curr = new Set(selectedTables)
    const added = selectedTables.filter(t => !prev.has(t))
    const removed = prevTablesRef.current.filter(t => !curr.has(t))
    prevTablesRef.current = selectedTables
    if (!added.length && !removed.length) return
    setSelectedMVs(prevSel => {
      const next = new Set(prevSel)
      metricViews.forEach(mv => {
        if (added.some(t => t === mv.source_table)) next.add(mv.metric_view_name)
        if (removed.some(t => t === mv.source_table)) next.delete(mv.metric_view_name)
      })
      return next
    })
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
        body: JSON.stringify({ table_identifiers: selectedTables, metric_view_names: [...selectedMVs], business_context: businessContext || undefined }),
      })
      if (!res.ok) { const b = await res.json().catch(() => ({})); setError(b.detail || `Error ${res.status}`); return }
      const { questions: qs } = await res.json()
      const existing = questions.trim()
      setQuestions(existing ? existing + '\n' + qs.join('\n') : qs.join('\n'))
    } catch (e) { setError(e.message) }
    finally { setSuggestingQs(false) }
  }

  const startGeneration = async ({ feedback, priorResult } = {}) => {
    if (!selectedTables.length) return setError('Select at least one table')
    setError(null); setCreatedSpace(null); setCreateError(null); setLoading(true)
    if (!feedback) { setResult(null) }
    const qs = questions.split('\n').map(q => q.trim()).filter(Boolean)
    const body = {
      table_identifiers: selectedTables, questions: qs,
      metric_view_names: [...selectedMVs],
      kpi_names: kpis.filter(k => selectedKpis.has(k.kpi_id)).map(k => k.name),
      business_context: businessContext || undefined,
    }
    if (feedback && priorResult) {
      body.refinement_feedback = feedback
      body.prior_result = priorResult
    }
    const res = await fetch('/api/genie/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!res.ok) {
      const b = await res.json().catch(() => ({}))
      setError(b.detail || `Error ${res.status}`); setLoading(false); return
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
        setError(null)
        setRefineFeedback('')
      } else if (data.status === 'error') {
        clearInterval(pollRef.current)
        setError(null)
        setLoading(false)
      }
    }, 3000)
  }

  const createSpace = async () => {
    if (!title.trim()) return setCreateError('Enter a title for the Genie space')
    setCreateError(null); setCreating(true)
    let parsed
    try { parsed = JSON.parse(editedJson) } catch { setCreating(false); return setCreateError('Invalid JSON') }
    try {
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
        setCreateError(body.detail || `Error ${res.status}`)
        setCreating(false)
        return
      }
      const created = await res.json()
      setCreatedSpace(created)
      if (created.space_id) loadTrackedSpaces()
    } catch (e) {
      setCreateError(e.message || 'Network error creating Genie space')
    }
    setCreating(false)
  }

  const filterLower = tableFilter.toLowerCase()
  const filteredTables = filterLower
    ? tables.filter(t => t.label.toLowerCase().includes(filterLower) || t.schema.toLowerCase().includes(filterLower))
    : tables
  const filteredMVs = metricViews.filter(mv => {
    const parts = mv.source_table?.split('.') || []
    if (parts.length < 3 || parts[0] !== selectedCatalog) return false
    if (filterLower) return mv.metric_view_name.toLowerCase().includes(filterLower) || parts[1].toLowerCase().includes(filterLower)
    return true
  })
  const mvsBySchema = {}
  filteredMVs.forEach(mv => {
    const schema = mv.source_table.split('.')[1]
    ;(mvsBySchema[schema] ||= []).push(mv)
  })
  const schemas = [...new Set([...filteredTables.map(t => t.schema), ...Object.keys(mvsBySchema)])]
  const [openSchemas, setOpenSchemas] = useState(new Set())

  const toggleSchema = (schema) => {
    setOpenSchemas(prev => {
      const next = new Set(prev)
      next.has(schema) ? next.delete(schema) : next.add(schema)
      return next
    })
  }

  const toggleAllInSchema = (schema) => {
    const ids = filteredTables.filter(t => t.schema === schema).map(t => t.id)
    const schemaMvNames = (mvsBySchema[schema] || []).map(mv => mv.metric_view_name)
    const allTablesSelected = ids.every(id => selectedTables.includes(id))
    const allMvsSelected = schemaMvNames.every(n => selectedMVs.has(n))
    const allSelected = allTablesSelected && allMvsSelected && (ids.length + schemaMvNames.length > 0)
    setSelectedTables(prev =>
      allSelected ? prev.filter(id => !ids.includes(id)) : [...new Set([...prev, ...ids])]
    )
    setSelectedMVs(prev => {
      const next = new Set(prev)
      schemaMvNames.forEach(n => allSelected ? next.delete(n) : next.add(n))
      return next
    })
  }

  if (editingSpaceId) {
    return <GenieUpdater spaceId={editingSpaceId} onBack={() => { setEditingSpaceId(null); loadTrackedSpaces() }} />
  }

  return (
    <div className="space-y-6">
      <PageHeader title="Genie Space Builder" subtitle="Select tables and optionally provide business questions to generate a Genie space configuration" />

      <ErrorBanner error={error} />

      {/* Table selection */}
      <div className="card p-5">
        <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-3">Select Tables</h3>
        <div className="flex gap-3 mb-3">
          <select
            value={selectedCatalog}
            onChange={e => setSelectedCatalog(e.target.value)}
            className="border border-slate-200 dark:border-slate-600 rounded-md px-3 py-1.5 text-sm bg-dbx-oat-light dark:bg-slate-700 text-slate-800 dark:text-slate-200"
          >
            {!catalogs.length && selectedCatalog && <option value={selectedCatalog}>{selectedCatalog}</option>}
            {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
          <input
            type="text"
            value={tableFilter}
            onChange={e => setTableFilter(e.target.value)}
            placeholder="Filter tables..."
            className="flex-1 border border-slate-200 dark:border-slate-600 rounded-md px-3 py-1.5 text-sm bg-dbx-oat-light dark:bg-slate-700 text-slate-800 dark:text-slate-200 placeholder-slate-400"
          />
        </div>
        {schemas.map(schema => {
          const schemaTables = filteredTables.filter(t => t.schema === schema)
          const schemaMVs = mvsBySchema[schema] || []
          const schemaIds = schemaTables.map(t => t.id)
          const totalItems = schemaIds.length + schemaMVs.length
          const selectedCount = schemaIds.filter(id => selectedTables.includes(id)).length + schemaMVs.filter(mv => selectedMVs.has(mv.metric_view_name)).length
          const allSelected = selectedCount === totalItems && totalItems > 0
          const isOpen = openSchemas.has(schema)

          return (
            <div key={schema} className="mb-2 card overflow-hidden">
              <button
                onClick={() => toggleSchema(schema)}
                className="w-full flex items-center justify-between px-3 py-2 bg-dbx-oat dark:bg-slate-700/50 hover:bg-dbx-oat dark:hover:bg-slate-700 transition-colors"
              >
                <div className="flex items-center gap-2">
                  <span className={`text-xs transition-transform ${isOpen ? 'rotate-90' : ''}`}>&#9654;</span>
                  <span className="text-xs font-medium text-slate-600 dark:text-slate-300">{schema}</span>
                  <span className="text-[10px] text-slate-400">({selectedCount}/{totalItems})</span>
                </div>
                <span
                  onClick={e => { e.stopPropagation(); toggleAllInSchema(schema) }}
                  title="Toggle selection of all items in this schema"
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
                  {schemaMVs.map(mv => (
                    <button
                      key={mv.metric_view_name}
                      onClick={() => toggleMV(mv.metric_view_name)}
                      title={mv.status === 'applied' ? `Applied metric view on ${mv.source_table}` : `Not yet applied to UC. Will contribute SQL snippets only, not a Genie data source. Apply it in the Semantic Layer tab first.`}
                      style={mv.status !== 'applied' ? { borderStyle: 'dashed' } : undefined}
                      className={`px-3 py-1.5 text-xs rounded-md border transition-colors ${selectedMVs.has(mv.metric_view_name)
                        ? 'bg-violet-100 dark:bg-violet-900 border-violet-400 text-violet-700 dark:text-violet-300'
                        : 'bg-dbx-oat-light dark:bg-slate-700 border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-300 hover:bg-dbx-oat dark:hover:bg-slate-600'
                        }`}
                    >
                      {mv.metric_view_name}
                      {mv.status !== 'applied' && <span className="text-[9px] text-amber-500 ml-1">(snippet only)</span>}
                      <span className="ml-1 text-[10px] text-violet-500 dark:text-violet-400">MV</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          )
        })}
        {!tables.length && tablesLoading && <SkeletonTable rows={3} cols={3} />}
        {!tables.length && !tablesLoading && <p className="text-sm text-slate-400">No tables found in this catalog.</p>}
        {tables.length > 0 && !filteredTables.length && <p className="text-sm text-slate-400">No tables match "{tableFilter}".</p>}
      </div>

      {fetchErrors.metricViews && (
        <p className="text-xs text-amber-600 dark:text-amber-400 px-1">Could not load metric views. <button onClick={() => { setFetchErrors(p => ({ ...p, metricViews: null })); fetch('/api/semantic/metric-views?status=applied').then(r => r.ok ? r.json() : []).then(setMetricViews).catch(e => setFetchErrors(p => ({ ...p, metricViews: e.message }))) }} className="underline">Retry</button></p>
      )}
      {fetchErrors.kpis && (
        <p className="text-xs text-amber-600 dark:text-amber-400 px-1">Could not load KPIs. <button onClick={() => { setFetchErrors(p => ({ ...p, kpis: null })); fetch('/api/kpis').then(r => r.ok ? r.json() : []).then(setKpis).catch(e => setFetchErrors(p => ({ ...p, kpis: e.message }))) }} className="underline">Retry</button></p>
      )}


      {/* KPIs -- filtered to those whose target_tables overlap with selectedTables */}
      {(() => {
        const selectedShort = new Set(selectedTables.map(t => t.split('.').pop()))
        const relevantKpis = kpis.filter(k => {
          const targets = Array.isArray(k.target_tables) ? k.target_tables : []
          return targets.some(t => selectedTables.includes(t) || selectedShort.has(t.split('.').pop()))
        })
        return relevantKpis.length > 0 && (
        <div className="card p-5">
          <button onClick={() => setShowKpis(!showKpis)}
            className="w-full flex items-center justify-between text-sm font-medium text-slate-700 dark:text-slate-300">
            <span>KPIs ({selectedKpis.size}/{relevantKpis.length} selected)</span>
            <span className={`text-xs transition-transform ${showKpis ? 'rotate-90' : ''}`}>&#9654;</span>
          </button>
          {showKpis && (
            <div className="mt-3 space-y-1.5 max-h-48 overflow-y-auto">
              {relevantKpis.map(k => (
                <label key={k.kpi_id} className="flex items-start gap-2 text-sm cursor-pointer">
                  <input type="checkbox" checked={selectedKpis.has(k.kpi_id)}
                    onChange={() => {
                      const s = new Set(selectedKpis)
                      s.has(k.kpi_id) ? s.delete(k.kpi_id) : s.add(k.kpi_id)
                      setSelectedKpis(s)
                    }}
                    className="mt-0.5 rounded" />
                  <div className="min-w-0">
                    <span className="font-medium dark:text-gray-200">{k.name}</span>
                    {k.domain && <span className="ml-1.5 text-xs px-1 py-0.5 rounded bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300">{k.domain}</span>}
                    {k.description && <p className="text-xs text-gray-500 dark:text-gray-400 truncate">{k.description}</p>}
                  </div>
                </label>
              ))}
            </div>
          )}
        </div>
        )
      })()}

      {/* Business Context */}
      <div className="card p-5">
        <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Business Context (optional)</h3>
        <p className="text-xs text-gray-500 dark:text-gray-400 mb-2">Describe your business, priorities, and key terminology. This steers question suggestions, measure generation, and sample questions toward what matters to your users.</p>
        <textarea
          value={businessContext}
          onChange={e => setBusinessContext(e.target.value)}
          rows={2}
          placeholder="e.g. We are a B2B SaaS company focused on ARR growth, net revenue retention, and pipeline velocity. Our CFO reviews quarterly cohort metrics."
          className="w-full border border-slate-200 dark:border-slate-600 rounded-md px-3 py-2 text-sm bg-dbx-oat-light dark:bg-slate-700 text-slate-800 dark:text-slate-200 placeholder-slate-400"
        />
      </div>

      {/* Questions */}
      <div className="card p-5">
        <div className="flex items-center justify-between mb-2">
          <div>
            <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300">Business Questions (optional)</h3>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Questions are generated based on the selected tables, their metric views, and discovered business entities.</p>
          </div>
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

      {/* Table count warning (tables + metric views both count toward complexity) */}
      {(selectedTables.length + selectedMVs.size) > 10 && (
        <div className="bg-amber-50 dark:bg-amber-900/30 border border-amber-200 dark:border-amber-700 rounded-lg px-4 py-2.5 text-sm text-amber-700 dark:text-amber-300">
          You have {selectedTables.length} table{selectedTables.length !== 1 ? 's' : ''}{selectedMVs.size > 0 ? ` + ${selectedMVs.size} metric view${selectedMVs.size !== 1 ? 's' : ''}` : ''} selected. Generation may be slow or fail with many sources. Start with 3-5 tables to test, then expand.
        </div>
      )}

      {/* Generate button */}
      <button
        onClick={() => startGeneration()}
        disabled={loading || !selectedTables.length}
        className="px-5 py-2.5 bg-dbx-lava text-white text-sm font-medium rounded-lg hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      >
        {loading ? 'Generating...' : 'Generate Genie Config'}
      </button>

      {/* Progress */}
      {taskStatus && taskStatus.status === 'running' && (
        <div className="bg-orange-50 dark:bg-orange-900/30 border border-orange-200 dark:border-red-700 rounded-lg px-4 py-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 border-2 border-orange-400 border-t-transparent rounded-full animate-spin" />
              <span className="text-sm font-medium text-red-700 dark:text-orange-300">
                {taskStatus.message || STAGES[taskStatus.stage] || taskStatus.stage}
              </span>
              {taskStatus.round > 0 && (
                <span className="text-xs px-1.5 py-0.5 rounded bg-orange-200 dark:bg-orange-800 text-orange-700 dark:text-orange-200">
                  Round {taskStatus.round}
                </span>
              )}
              {taskStatus.validated != null && taskStatus.total != null && (
                <span className="text-xs text-slate-500 dark:text-slate-400">
                  ({taskStatus.validated}/{taskStatus.total} validated)
                </span>
              )}
            </div>
            <span className="text-xs text-slate-500 dark:text-slate-400 tabular-nums">
              {formatElapsed(taskStatus.elapsed_seconds ?? elapsed)}
            </span>
          </div>
        </div>
      )}

      {/* Error detail (shown instead of generic banner when we have diagnostics) */}
      {taskStatus && taskStatus.status === 'error' && !loading && (
        <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg px-4 py-3 space-y-1.5">
          <p className="text-sm font-medium text-red-700 dark:text-red-300">
            {taskStatus.error || 'Generation failed'}
          </p>
          {taskStatus.elapsed_seconds ? (
            <p className="text-xs text-red-600/70 dark:text-red-400/70">
              Failed after {formatElapsed(taskStatus.elapsed_seconds ?? elapsed)}.
            </p>
          ) : null}
          {(selectedTables.length + selectedMVs.size) > 5 && (
            <p className="text-xs text-red-600/70 dark:text-red-400/70 mt-1">
              Tip: Try reducing the number of selected tables or metric views and re-running.
            </p>
          )}
        </div>
      )}

      {/* Result JSON */}
      {result && (
        <div className="space-y-4">
          {taskStatus && taskStatus.status === 'done' && taskStatus.elapsed_seconds != null && (
            <div className="bg-emerald-50 dark:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-700 rounded-lg px-4 py-2 text-xs text-emerald-700 dark:text-emerald-300">
              Generated in {formatElapsed(taskStatus.elapsed_seconds)}
              {(taskStatus.warnings?.filter(w => w.includes('metric view')) ?? []).length > 0 && (
                <div className="mt-1 text-xs text-amber-600 dark:text-amber-400 font-medium">
                  {(taskStatus.warnings?.filter(w => w.includes('metric view')) ?? []).map((w, i) => <p key={i}>{w}</p>)}
                </div>
              )}
              {taskStatus.warnings?.length > 0 && (
                <details className="inline-block ml-2">
                  <summary className="cursor-pointer text-amber-600 dark:text-amber-400">{taskStatus.warnings.length} quality warning(s)</summary>
                  <ul className="mt-1 ml-4 list-disc text-amber-600 dark:text-amber-400 space-y-0.5">
                    {taskStatus.warnings.map((w, i) => <li key={i}>{w}</li>)}
                  </ul>
                </details>
              )}
            </div>
          )}
          {(() => {
            let parsed = null; try { parsed = JSON.parse(editedJson) } catch {}
            if (!parsed) return null
            const ds = parsed.data_sources || {}
            const tblCount = (ds.tables?.length || 0)
            const mvCount = (ds.metric_views?.length || 0)
            const tblDisplay = tblCount > 0 ? tblCount : (selectedTables.length > 0 ? `${selectedTables.length} (from selection)` : 0)
            const mvDisplay = mvCount > 0 ? mvCount : (selectedMVs.size > 0 ? `${selectedMVs.size} (from selection)` : 0)
            const instrText = parsed.instructions?.text
            const exampleSqlCount = parsed.instructions?.example_sql?.length || 0
            const joinCount = parsed.instructions?.join_specs?.length || 0
            const snippetFilters = parsed.instructions?.sql_snippets?.filters?.length || 0
            const snippetMeasures = parsed.instructions?.sql_snippets?.measures?.length || 0
            const snippetExprs = parsed.instructions?.sql_snippets?.expressions?.length || 0
            const sampleQCount = parsed.sample_questions?.length || 0
            return (
              <div className="card p-5 space-y-2">
                <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300">Config Preview</h3>
                <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-xs">
                  <div><span className="text-slate-500">Tables</span><p className="font-medium">{tblDisplay}{mvDisplay > 0 || (typeof mvDisplay === 'string') ? ` + ${mvDisplay} MVs` : ''}</p></div>
                  <div><span className="text-slate-500">Example SQL</span><p className="font-medium">{exampleSqlCount}</p></div>
                  <div><span className="text-slate-500">Joins</span><p className="font-medium">{joinCount}</p></div>
                  <div><span className="text-slate-500">Snippets</span><p className="font-medium">{snippetFilters + snippetMeasures + snippetExprs} ({snippetFilters}F / {snippetMeasures}M / {snippetExprs}E)</p></div>
                  <div><span className="text-slate-500">Sample Qs</span><p className="font-medium">{sampleQCount}</p></div>
                </div>
                {parsed.description && <p className="text-xs text-slate-500 mt-1 line-clamp-2">{parsed.description}</p>}
                {instrText && <p className="text-xs text-slate-400 mt-1">Instructions: {instrText.length.toLocaleString()} chars</p>}
                <details className="mt-3 pt-3 border-t border-slate-200 dark:border-slate-600 text-xs text-slate-600 dark:text-slate-400">
                  <summary className="cursor-pointer font-medium text-slate-700 dark:text-slate-300 select-none">How joins and metric views work</summary>
                  <ul className="mt-2 ml-4 list-disc space-y-1.5">
                    <li><span className="font-medium text-slate-700 dark:text-slate-300">Joins</span> counts explicit <code className="text-[11px] bg-slate-100 dark:bg-slate-800 px-1 rounded">join_specs</code> between selected tables (and selected join targets). A low or zero count is normal when the space is metric-view heavy or when underlying fact tables are not in your <em>table</em> selection.</li>
                    <li><span className="font-medium text-slate-700 dark:text-slate-300">Snippets</span> (filters / measures / expressions) are built from metric views that are <em>not yet applied</em> to Unity Catalog; applied MVs appear as metric view data sources instead.</li>
                    <li>Join logic defined <em>inside</em> an MV definition is not the same as a high Joins count here; Genie still uses instructions and example SQL to query MVs.</li>
                  </ul>
                </details>
              </div>
            )
          })()}
          {/* Regenerate / Refine */}
          <div className="card p-4 space-y-3">
            <div className="flex items-center gap-2">
              <button
                onClick={() => startGeneration()}
                disabled={loading}
                className="px-4 py-2 text-xs font-medium rounded-md border border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-300 hover:bg-dbx-oat dark:hover:bg-slate-700 disabled:opacity-50 transition-colors"
              >
                Regenerate
              </button>
              <button
                onClick={() => setShowRefine(!showRefine)}
                disabled={loading}
                className="px-4 py-2 text-xs font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                {showRefine ? 'Hide Feedback' : 'Refine with Feedback'}
              </button>
            </div>
            {showRefine && (
              <div className="space-y-2">
                <textarea
                  value={refineFeedback}
                  onChange={e => setRefineFeedback(e.target.value)}
                  rows={2}
                  placeholder="e.g. Add more time-based filters, improve the description, add window function examples..."
                  className="w-full border border-slate-200 dark:border-slate-600 rounded-md px-3 py-2 text-sm bg-dbx-oat-light dark:bg-slate-700 text-slate-800 dark:text-slate-200 placeholder-slate-400"
                />
                <button
                  onClick={() => {
                    if (!refineFeedback.trim()) return setError('Enter feedback first')
                    let prior; try { prior = JSON.parse(editedJson) } catch { prior = result }
                    startGeneration({ feedback: refineFeedback.trim(), priorResult: prior })
                    setShowRefine(false)
                  }}
                  disabled={loading || !refineFeedback.trim()}
                  className="px-4 py-2 text-xs font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 transition-colors"
                >
                  Apply Feedback & Regenerate
                </button>
              </div>
            )}
          </div>

          <details className="card overflow-hidden group">
            <summary className="px-5 py-3 text-sm font-medium text-slate-700 dark:text-slate-300 cursor-pointer select-none flex items-center gap-1.5">
              <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>
              Edit Raw JSON
            </summary>
            <div className="px-5 pb-5">
              <textarea
                value={editedJson}
                onChange={e => setEditedJson(e.target.value)}
                rows={20}
                className="w-full font-mono text-xs border border-slate-200 dark:border-slate-600 rounded-md p-3 bg-dbx-oat dark:bg-slate-900 text-slate-800 dark:text-slate-200"
              />
            </div>
          </details>

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
              disabled={!title.trim() || creating}
              title="Create a Databricks Genie space from the selected tables with AI-generated instructions and example queries"
              className="px-5 py-2.5 bg-emerald-600 text-white text-sm font-medium rounded-lg hover:bg-emerald-700 disabled:opacity-50 transition-colors flex items-center gap-1.5"
            >
              {creating && <span className="w-3 h-3 border-2 border-white border-t-transparent rounded-full animate-spin" />}
              {creating ? 'Creating...' : 'Create Genie Space'}
            </button>
          </div>

          {createError && (
            <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg px-4 py-3 text-sm space-y-1">
              <p className="font-medium text-red-700 dark:text-red-300">
                {createError.includes('Data source validation failed') ? 'Some data sources could not be found' : 'Failed to create Genie space'}
              </p>
              {createError.includes('Data source validation failed') ? (
                <div className="text-xs text-red-600 dark:text-red-400 space-y-1">
                  {createError.replace('Data source validation failed: ', '').split('; ').map((item, i) => {
                    const match = item.match(/data_sources identifier '([^']+)'/)
                    return (
                      <p key={i}>
                        <span className="font-mono bg-red-100 dark:bg-red-900/50 px-1 rounded">{match ? match[1] : item.split(':')[0]}</span>
                        {' '}-- table or view not found. It may need to be re-applied as a metric view, or removed from the config.
                      </p>
                    )
                  })}
                </div>
              ) : (
                <p className="text-xs text-red-600 dark:text-red-400">{createError}</p>
              )}
            </div>
          )}

          {createdSpace && (
            <div className="bg-emerald-50 dark:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-700 rounded-lg px-4 py-3 text-sm text-emerald-700 dark:text-emerald-300">
              Genie space {createdSpace.updated ? 'updated' : 'created'}! ID: <span className="font-mono">{createdSpace.space_id}</span>
              {(createdSpace.table_count != null || createdSpace.join_count != null) && (
                <span className="ml-2 text-xs opacity-80">
                  ({createdSpace.table_count ?? '?'} tables{createdSpace.mv_count ? `, ${createdSpace.mv_count} MVs` : ''}, {createdSpace.join_count ?? '?'} explicit join_specs deployed)
                </span>
              )}
              {workspaceHost && createdSpace.space_id && (
                <> &mdash; <a href={`${workspaceHost}/genie/rooms/${createdSpace.space_id}`} target="_blank" rel="noopener noreferrer"
                  className="underline font-medium">Open in Databricks</a></>
              )}
              {createdSpace.warnings?.length > 0 && (
                <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">{createdSpace.warnings.length} warning(s): {createdSpace.warnings.join('; ')}</p>
              )}
            </div>
          )}
        </div>
      )}

      {/* Tracked Spaces */}
      {trackedSpaces.length > 0 && (
        <div className="card p-6 mt-6">
          <h2 className="heading-section mb-4">My Genie Spaces</h2>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead><tr>
                {['Title', 'Space ID', 'Version', 'Tables', 'Last Updated', ''].map(h =>
                  <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat dark:bg-slate-700 font-semibold text-slate-600 dark:text-slate-300 border-b border-slate-200 dark:border-slate-600 text-xs uppercase tracking-wider">{h}</th>)}
              </tr></thead>
              <tbody>
                {trackedSpaces.map((s, i) => (
                  <tr key={i} className="border-b border-dbx-oat-dark/20 dark:border-dbx-navy-400/10 hover:bg-dbx-teal-light/20 dark:hover:bg-dbx-navy-500/30 transition-colors">
                    <td className="px-3 py-2 font-medium text-slate-700 dark:text-slate-200">
                      {workspaceHost ? (
                        <a href={`${workspaceHost}/genie/rooms/${s.space_id}`} target="_blank" rel="noopener noreferrer"
                          className="text-blue-600 dark:text-blue-400 hover:underline">{s.title}</a>
                      ) : s.title}
                    </td>
                    <td className="px-3 py-2 font-mono text-xs text-slate-500">{s.space_id}</td>
                    <td className="px-3 py-2 text-slate-600 dark:text-slate-400">{s.version || 1}</td>
                    <td className="px-3 py-2 text-slate-600 dark:text-slate-400 text-xs">{Array.isArray(s.tables) ? s.tables.length : 0}</td>
                    <td className="px-3 py-2 text-xs text-slate-500">{s.updated_at ? new Date(s.updated_at).toLocaleString() : ''}</td>
                    <td className="px-3 py-2 flex gap-2">
                      {workspaceHost && (
                        <a href={`${workspaceHost}/genie/rooms/${s.space_id}`} target="_blank" rel="noopener noreferrer"
                          className="text-xs text-emerald-600 hover:text-emerald-800 hover:underline">Open</a>
                      )}
                      <button onClick={() => setEditingSpaceId(s.space_id)}
                        className="text-xs text-blue-600 hover:text-blue-800 hover:underline">Edit</button>
                      <button onClick={async () => {
                        if (!confirm(`Delete Genie space "${s.title}"?`)) return
                        try {
                          const res = await fetch(`/api/genie/spaces/${s.space_id}`, { method: 'DELETE' })
                          if (!res.ok) { const d = await res.json().catch(() => ({})); setError(d.detail || 'Delete failed'); return }
                          loadTrackedSpaces()
                        } catch (e) { setError(e.message || 'Delete failed') }
                      }} className="text-xs text-red-600 hover:text-red-800 hover:underline">Delete</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Load by Space ID */}
      <div className="card p-5">
        <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">Edit Existing Space by ID</h3>
        <div className="flex gap-2">
          <input value={loadByIdValue} onChange={e => setLoadByIdValue(e.target.value)}
            placeholder="Paste a Genie Space ID..."
            className="flex-1 border border-slate-200 dark:border-slate-600 rounded-md px-3 py-2 text-sm bg-dbx-oat-light dark:bg-slate-700 text-slate-800 dark:text-slate-200" />
          <button onClick={() => { if (loadByIdValue.trim()) setEditingSpaceId(loadByIdValue.trim()) }}
            disabled={!loadByIdValue.trim()}
            className="px-4 py-2 text-sm font-medium rounded-md bg-dbx-navy text-white hover:bg-slate-700 disabled:opacity-50 transition-colors">
            Load
          </button>
        </div>
      </div>
    </div>
  )
}
