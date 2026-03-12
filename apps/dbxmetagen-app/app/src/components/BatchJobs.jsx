import React, { useState, useEffect, useRef, useCallback } from 'react'
import { safeFetchObj, ErrorBanner } from '../App'

const TERMINAL_STATES = new Set(['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR'])

function stateBadge(state, result) {
  if (!state) return null
  let color = 'bg-dbx-oat text-gray-700'
  if (state === 'RUNNING' || state === 'PENDING') color = 'bg-blue-100 text-blue-700'
  if (state === 'TERMINATED' && result === 'SUCCESS') color = 'bg-green-100 text-green-700'
  if (state === 'TERMINATED' && result === 'FAILED') color = 'bg-red-100 text-red-700'
  if (state === 'TERMINATED' && result === 'TIMEDOUT') color = 'bg-orange-100 text-red-700'
  if (state === 'TERMINATED' && result === 'CANCELLED') color = 'bg-gray-200 text-gray-600'
  if (state === 'SKIPPED') color = 'bg-gray-200 text-gray-500'
  if (state === 'INTERNAL_ERROR') color = 'bg-red-200 text-red-800'
  const label = result ? `${state} / ${result}` : state
  return <span className={`px-2 py-0.5 rounded text-xs font-medium ${color}`}>{label}</span>
}

function TaskProgress({ tasks }) {
  if (!tasks || tasks.length === 0) return null
  const done = tasks.filter(t => TERMINAL_STATES.has(t.state)).length
  const failed = tasks.filter(t => t.result === 'FAILED').length
  return (
    <div className="mt-2">
      <div className="flex items-center gap-2 mb-1">
        <span className="text-xs text-gray-500">Tasks: {done}/{tasks.length}</span>
        {failed > 0 && <span className="text-xs text-red-600">{failed} failed</span>}
      </div>
      <div className="flex gap-0.5">
        {tasks.map(t => {
          let bg = 'bg-gray-200'
          if (t.state === 'RUNNING' || t.state === 'PENDING') bg = 'bg-blue-400 animate-pulse'
          if (t.result === 'SUCCESS') bg = 'bg-green-500'
          if (t.result === 'FAILED') bg = 'bg-red-500'
          if (t.result === 'EXCLUDED') bg = 'bg-gray-300'
          return <div key={t.task_key} className={`h-2 flex-1 rounded-sm ${bg}`} title={`${t.task_key}: ${t.state}${t.result ? ' / ' + t.result : ''}`} />
        })}
      </div>
    </div>
  )
}

function RunEntry({ run }) {
  const [expanded, setExpanded] = useState(false)
  const hasTasks = run.tasks && run.tasks.length > 0
  return (
    <div className="py-3 border-b last:border-b-0">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <span className="text-sm font-medium truncate">{run.job_name || 'Unknown Job'}</span>
          {stateBadge(run.state, run.result)}
          {!TERMINAL_STATES.has(run.state) && (
            <span className="inline-block w-2 h-2 bg-blue-500 rounded-full animate-pulse" title="Polling..." />
          )}
        </div>
        <div className="flex items-center gap-3 shrink-0">
          {run.run_page_url && (
            <a href={run.run_page_url} target="_blank" rel="noopener noreferrer"
              className="text-xs text-blue-600 hover:underline">View in Databricks</a>
          )}
          {hasTasks && (
            <button onClick={() => setExpanded(e => !e)} className="text-xs text-gray-500 hover:text-gray-700">
              {expanded ? 'Hide tasks' : `${run.tasks.length} tasks`}
            </button>
          )}
          <span className="text-xs text-gray-400">#{run.run_id}</span>
        </div>
      </div>
      {run.state_message && run.result === 'FAILED' && (
        <div className="mt-1 text-xs text-red-600 bg-red-50 rounded px-2 py-1 break-words">
          {run.state_message}
        </div>
      )}
      {hasTasks && !expanded && <TaskProgress tasks={run.tasks} />}
      {hasTasks && expanded && (
        <div className="mt-2 space-y-1">
          {run.tasks.map(t => (
            <div key={t.task_key} className="flex items-center justify-between text-xs px-2 py-1 bg-dbx-oat rounded">
              <span className="font-mono">{t.task_key}</span>
              {stateBadge(t.state, t.result)}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function HealthWarnings({ health }) {
  if (!health || health.errors.length === 0) return null
  return (
    <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-3 text-sm space-y-1">
      <span className="font-medium text-amber-800">Diagnostics:</span>
      {health.errors.map((e, i) => (
        <p key={i} className="text-amber-700">{e}</p>
      ))}
    </div>
  )
}

function Step({ num, color, title, prereq, children }) {
  return (
    <section className="bg-dbx-oat-light rounded-lg border overflow-hidden">
      <div className={`flex items-center gap-3 px-6 py-3 border-b ${color}`}>
        <span className="flex items-center justify-center w-7 h-7 rounded-full bg-dbx-oat-light/90 text-sm font-bold">{num}</span>
        <h2 className="text-white font-semibold">{title}</h2>
        {prereq && <span className="ml-auto text-xs text-white/70">{prereq}</span>}
      </div>
      <div className="p-6">{children}</div>
    </section>
  )
}

export default function BatchJobs() {
  const [jobs, setJobs] = useState([])
  const [tableNames, setTableNames] = useState('')
  const [mode, setMode] = useState('comment')
  const [applyDdl, setApplyDdl] = useState(false)
  const [catalogName, setCatalogName] = useState('')
  const [schemaName, setSchemaName] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [ontologyBundle, setOntologyBundle] = useState('healthcare')
  const [entityTagKey, setEntityTagKey] = useState('entity_type')
  const [bundles, setBundles] = useState([])
  const [domainConfig, setDomainConfig] = useState('')
  const [domainConfigs, setDomainConfigs] = useState([])
  const [runHistory, setRunHistory] = useState([])
  const [historyPage, setHistoryPage] = useState(0)
  const [health, setHealth] = useState(null)
  const pollRef = useRef(null)

  const [pickerOpen, setPickerOpen] = useState(false)
  const [pickerCatalog, setPickerCatalog] = useState('')
  const [pickerSchema, setPickerSchema] = useState('')
  const [pickerCatalogs, setPickerCatalogs] = useState([])
  const [pickerSchemas, setPickerSchemas] = useState([])
  const [pickerTables, setPickerTables] = useState([])
  const [pickerSelected, setPickerSelected] = useState([])
  const [pickerFilter, setPickerFilter] = useState('')

  // Load jobs, config, bundles, health check on mount
  useEffect(() => {
    setError(null)
    fetch('/api/jobs').then(r => {
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
      return r.json()
    }).then(setJobs)
      .catch(e => setError(`Failed to load jobs: ${e.message}`))

    safeFetchObj('/api/config').then(({ data: cfg, error: cfgErr }) => {
      if (cfgErr) setError(prev => prev ? `${prev} | Config: ${cfgErr}` : `Config load failed: ${cfgErr}`)
      if (cfg) {
        setCatalogName(cfg.catalog_name || '')
        setSchemaName(cfg.schema_name || '')
      }
    })
    fetch('/api/ontology/bundles').then(r => r.ok ? r.json() : []).then(setBundles).catch(() => { })
    fetch('/api/domain-configs').then(r => r.ok ? r.json() : []).then(setDomainConfigs).catch(() => { })
    fetch('/api/jobs/health').then(r => r.ok ? r.json() : null).then(setHealth).catch(() => { })

    // Load recent run history
    fetch('/api/jobs/runs').then(r => r.ok ? r.json() : []).then(runs => {
      setRunHistory(runs.map(r => ({ ...r, _polling: false })))
    }).catch(() => { })
  }, [])

  // Table picker cascades
  useEffect(() => {
    fetch('/api/catalogs').then(r => r.json()).then(list => {
      setPickerCatalogs(list)
      if (catalogName && list.includes(catalogName)) setPickerCatalog(catalogName)
    }).catch(() => {})
  }, [])
  useEffect(() => {
    setPickerSchemas([]); setPickerTables([]); setPickerSelected([])
    if (!pickerCatalog) return
    fetch(`/api/schemas?catalog=${pickerCatalog}`).then(r => r.json()).then(setPickerSchemas).catch(() => setPickerSchemas([]))
  }, [pickerCatalog])
  useEffect(() => {
    setPickerTables([]); setPickerSelected([])
    if (!pickerCatalog || !pickerSchema) return
    fetch(`/api/tables?catalog=${pickerCatalog}&schema=${pickerSchema}`)
      .then(r => r.json()).then(setPickerTables).catch(() => setPickerTables([]))
  }, [pickerCatalog, pickerSchema])

  const addSelectedTables = () => {
    if (pickerSelected.length === 0) return
    const fqNames = pickerSelected.map(t => `${pickerCatalog}.${pickerSchema}.${t}`)
    setTableNames(prev => {
      const trimmed = prev.trim()
      return trimmed ? `${trimmed}, ${fqNames.join(', ')}` : fqNames.join(', ')
    })
    setPickerSelected([])
  }

  const togglePickerTable = (t) => {
    setPickerSelected(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t])
  }

  // Auto-poll active runs
  const pollActiveRuns = useCallback(async () => {
    const active = runHistory.filter(r => !TERMINAL_STATES.has(r.state))
    if (active.length === 0) return
    const updates = await Promise.all(active.map(async (r) => {
      try {
        const res = await fetch(`/api/jobs/${r.run_id}/status`)
        if (!res.ok) return null
        return await res.json()
      } catch { return null }
    }))
    setRunHistory(prev => prev.map(r => {
      const upd = updates.find(u => u && u.run_id === r.run_id)
      return upd ? { ...r, ...upd } : r
    }))
  }, [runHistory])

  useEffect(() => {
    const hasActive = runHistory.some(r => !TERMINAL_STATES.has(r.state))
    if (hasActive) {
      pollRef.current = setInterval(pollActiveRuns, 5000)
      return () => clearInterval(pollRef.current)
    }
    if (pollRef.current) clearInterval(pollRef.current)
  }, [runHistory, pollActiveRuns])

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
      const newRun = {
        ...data,
        job_name: match?.name || jobNameSuffix,
        state: 'PENDING',
        result: null,
        tasks: [],
        run_page_url: null,
        state_message: null,
      }
      setRunHistory(prev => [newRun, ...prev])
    } catch (e) { setError(e.message) }
    setLoading(false)
  }

  const activeRuns = runHistory.filter(r => !TERMINAL_STATES.has(r.state))
  const completedRuns = runHistory.filter(r => TERMINAL_STATES.has(r.state))

  return (
    <div className="space-y-5">
      <ErrorBanner error={error} />
      <HealthWarnings health={health} />

      {/* Shared config */}
      <div className="bg-dbx-oat-light rounded-lg border p-4">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Catalog</label>
            <input value={catalogName} onChange={e => setCatalogName(e.target.value)}
              placeholder="e.g. eswanson" className="w-full border rounded-md p-2 text-sm" />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Schema</label>
            <input value={schemaName} onChange={e => setSchemaName(e.target.value)}
              placeholder="e.g. metadata_results" className="w-full border rounded-md p-2 text-sm" />
          </div>
        </div>

        <details className="mt-3">
          <summary className="text-xs font-medium text-gray-500 cursor-pointer hover:text-gray-700 select-none">
            Advanced Options
          </summary>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-3">
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Ontology Bundle</label>
              <select value={ontologyBundle} onChange={e => setOntologyBundle(e.target.value)}
                className="w-full border rounded-md p-2 text-sm">
                {bundles.length > 0 ? bundles.map(b => (
                  <option key={b.key} value={b.key}>{b.name} ({b.entity_count} entities)</option>
                )) : (
                  <option value="" disabled>No bundles found</option>
                )}
              </select>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Domain Config</label>
              <select value={domainConfig} onChange={e => setDomainConfig(e.target.value)}
                className="w-full border rounded-md p-2 text-sm">
                <option value="">(Use bundle domains)</option>
                {domainConfigs.map(d => (
                  <option key={d.key} value={d.key}>
                    {d.name} ({d.domain_count} domains)
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Ontology UC Tag Key</label>
              <input value={entityTagKey} onChange={e => setEntityTagKey(e.target.value)}
                placeholder="entity_type" title="Unity Catalog tag key used for entity type classifications (e.g. entity_type)"
                className="w-full border rounded-md p-2 text-sm" />
            </div>
          </div>
        </details>
      </div>

      {/* Step 1 -- Generate */}
      <Step num={1} color="bg-dbx-navy" title="Generate Metadata" prereq="Entry point">
        <p className="text-sm text-gray-500 mb-4">
          <strong>Single Mode</strong> runs one pass (comment, PI, or domain).
          <strong> All 3 Modes</strong> runs comments first, then PI + domain in parallel.
          Results go to <code className="text-xs bg-dbx-oat px-1 rounded">metadata_generation_log</code> for review.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Table Names</label>
            <textarea value={tableNames} onChange={e => setTableNames(e.target.value)}
              placeholder="catalog.schema.table1, catalog.schema.*"
              className="w-full border rounded-md p-2 text-sm h-20" />
            <button onClick={() => setPickerOpen(o => !o)}
              className="mt-1 text-xs text-blue-600 hover:text-blue-800 flex items-center gap-1">
              <span>{pickerOpen ? '\u25BC' : '\u25B6'}</span> Browse Tables
            </button>
            {pickerOpen && (
              <div className="mt-2 border rounded-lg p-3 bg-dbx-oat space-y-2">
                <div className="grid grid-cols-3 gap-2">
                  <select value={pickerCatalog} onChange={e => setPickerCatalog(e.target.value)}
                    className="border rounded p-1.5 text-sm">
                    <option value="">Catalog...</option>
                    {pickerCatalogs.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                  <select value={pickerSchema} onChange={e => setPickerSchema(e.target.value)}
                    className="border rounded p-1.5 text-sm" disabled={!pickerCatalog}>
                    <option value="">Schema...</option>
                    {pickerSchemas.map(s => <option key={s} value={s}>{s}</option>)}
                  </select>
                  <input value={pickerFilter} onChange={e => setPickerFilter(e.target.value)}
                    placeholder="Filter..." className="border rounded p-1.5 text-sm" />
                </div>
                {pickerTables.length > 0 && (
                  <>
                    <div className="max-h-40 overflow-y-auto border rounded bg-dbx-oat-light p-2 grid grid-cols-2 gap-x-4 gap-y-0.5">
                      {pickerTables
                        .filter(t => !pickerFilter || t.toLowerCase().includes(pickerFilter.toLowerCase()))
                        .map(t => (
                          <label key={t} className="flex items-center gap-1.5 text-xs cursor-pointer hover:bg-dbx-oat px-1 py-0.5 rounded">
                            <input type="checkbox" checked={pickerSelected.includes(t)}
                              onChange={() => togglePickerTable(t)} className="rounded" />
                            {t}
                          </label>
                        ))}
                    </div>
                    <div className="flex items-center justify-between">
                      <div className="flex gap-2">
                        <button onClick={() => {
                          const visible = pickerTables.filter(t => !pickerFilter || t.toLowerCase().includes(pickerFilter.toLowerCase()))
                          setPickerSelected(visible)
                        }} className="text-xs text-blue-600 hover:underline">Select All</button>
                        <button onClick={() => setPickerSelected([])}
                          className="text-xs text-gray-500 hover:underline">Clear</button>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-gray-500">{pickerSelected.length} selected</span>
                        <button onClick={addSelectedTables} disabled={pickerSelected.length === 0}
                          className="px-3 py-1 text-xs bg-slate-700 text-white rounded hover:bg-slate-800 disabled:opacity-40">
                          Add Selected
                        </button>
                      </div>
                    </div>
                  </>
                )}
                {pickerCatalog && pickerSchema && pickerTables.length === 0 && (
                  <p className="text-xs text-gray-400 italic">No tables found</p>
                )}
              </div>
            )}
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
              Apply DDL directly
            </label>
          </div>
        </div>
        <div className="flex gap-3 mt-4">
          <button onClick={() => runJob('_metadata_job', { table_names: tableNames, mode, apply_ddl: applyDdl, ...(domainConfig ? { domain_config: domainConfig } : {}) })}
            disabled={loading || !tableNames.trim()}
            title="Run a single metadata generation pass (comment, PI, or domain) for the specified tables"
            className="px-4 py-2 bg-slate-700 text-white rounded-md text-sm hover:bg-slate-800 disabled:opacity-50">
            {loading ? 'Starting...' : 'Run Single Mode'}
          </button>
          <button onClick={() => runJob('_parallel_modes_job', { table_names: tableNames, apply_ddl: applyDdl, ...(domainConfig ? { domain_config: domainConfig } : {}) })}
            disabled={loading || !tableNames.trim()}
            title="Run all three modes (comment, PI, domain) in parallel for faster coverage"
            className="px-4 py-2 bg-dbx-lava text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50">
            All 3 Modes (Parallel)
          </button>
        </div>
      </Step>

      {/* Step 2 -- Analyze */}
      <Step num={2} color="bg-dbx-navy/90" title="Analyze" prereq="Requires: metadata generation log">
        <p className="text-sm text-gray-500 mb-4">
          The <strong>Full Pipeline</strong> runs everything end-to-end: knowledge bases, graph, embeddings, profiling, ontology, clustering, FK prediction, and final analysis.
          Or run <strong>individual steps</strong> to re-run specific stages.
        </p>
        <div className="flex gap-3 mb-4">
          <button onClick={() => runJob('_full_analytics_pipeline', { catalog_name: catalogName, schema_name: schemaName, ontology_bundle: ontologyBundle, ...(domainConfig ? { domain_config: domainConfig } : {}) })}
            disabled={loading}
            title="Run the complete end-to-end pipeline: KB, graph, embeddings, profiling, ontology, clustering, FK prediction, analysis, and vector index"
            className="px-4 py-2 bg-dbx-lava text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50">
            Full Analytics Pipeline
          </button>
        </div>
        <details className="border rounded-lg">
          <summary className="px-4 py-2 text-sm font-medium text-gray-600 cursor-pointer hover:bg-dbx-oat">
            Individual steps
          </summary>
          <div className="px-4 py-3 space-y-3 border-t bg-dbx-oat/50">
            <div>
              <h4 className="text-sm font-medium text-gray-700 mb-1">Ontology Prediction</h4>
              <p className="text-xs text-gray-500 mb-2">
                Isolates entity discovery + validation. Re-classify with a different bundle without re-running embeddings or profiling.
              </p>
              <button onClick={() => runJob('_ontology_prediction', { catalog_name: catalogName, schema_name: schemaName, ontology_bundle: ontologyBundle })}
                disabled={loading}
                title="Classify tables into ontology entities using the selected bundle"
                className="px-3 py-1.5 bg-slate-700 text-white rounded text-sm hover:bg-slate-800 disabled:opacity-50">
                Run Ontology
              </button>
            </div>
            <div>
              <h4 className="text-sm font-medium text-gray-700 mb-1">Foreign Key Prediction</h4>
              <p className="text-xs text-gray-500 mb-2">
                Requires embeddings + similarity edges. Uses column similarity, rule scoring, and LLM judgment.
              </p>
              <div className="flex gap-2">
                <button onClick={() => runJob('fk_prediction', { catalog_name: catalogName, schema_name: schemaName })}
                  disabled={loading}
                  title="Discover FK relationships using column similarity, rule scoring, and LLM judgment"
                  className="px-3 py-1.5 bg-slate-700 text-white rounded text-sm hover:bg-slate-800 disabled:opacity-50">
                  Predict FKs
                </button>
                <button onClick={() => runJob('fk_prediction', { catalog_name: catalogName, schema_name: schemaName, apply_ddl: 'true' })}
                  disabled={loading}
                  title="Predict FKs and immediately apply them as ALTER TABLE constraints"
                  className="px-3 py-1.5 bg-dbx-lava text-white rounded text-sm hover:bg-red-700 disabled:opacity-50">
                  Predict + Apply
                </button>
              </div>
            </div>
            <div>
              <h4 className="text-sm font-medium text-gray-700 mb-1">Profiling</h4>
              <p className="text-xs text-gray-500 mb-2">
                Statistical profiling, data quality scores, and graph quality update.
              </p>
              <button onClick={() => runJob('_profiling_job', { catalog_name: catalogName, schema_name: schemaName })}
                disabled={loading}
                title="Run statistical profiling, data quality scores, and graph quality updates"
                className="px-3 py-1.5 bg-slate-700 text-white rounded text-sm hover:bg-slate-800 disabled:opacity-50">
                Run Profiling
              </button>
            </div>
            <div>
              <h4 className="text-sm font-medium text-gray-700 mb-1">Knowledge Base + Graph</h4>
              <p className="text-xs text-gray-500 mb-2">
                Build table/column knowledge bases and the knowledge graph with GraphFrames.
              </p>
              <button onClick={() => runJob('_knowledge_base_job', { catalog_name: catalogName, schema_name: schemaName })}
                disabled={loading}
                title="Build table/column knowledge bases and the knowledge graph with GraphFrames"
                className="px-3 py-1.5 bg-slate-700 text-white rounded text-sm hover:bg-slate-800 disabled:opacity-50">
                Build KB + Graph
              </button>
            </div>
          </div>
        </details>
      </Step>

      {/* Step 3 -- Enrich */}
      <Step num={3} color="bg-dbx-navy/80" title="Enrich" prereq="Requires: knowledge base + analytics">
        <p className="text-sm text-gray-500 mb-4">
          Generate metric views from business questions (add them in the <strong>Semantic Layer</strong> tab first),
          then apply as Unity Catalog metric views.
        </p>
        <button onClick={() => runJob('_semantic_layer', { catalog_name: catalogName, schema_name: schemaName })}
          disabled={loading}
          title="Generate metric views and Genie spaces from business questions defined in the Semantic Layer tab"
          className="px-4 py-2 bg-dbx-lava text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50">
          Generate Semantic Layer
        </button>
      </Step>

      {/* Step 4 -- Index */}
      <Step num={4} color="bg-dbx-navy/70" title="Index" prereq="Requires: completed analytics pipeline">
        <p className="text-sm text-gray-500 mb-4">
          Build a Vector Search index over enriched metadata for similarity search, hybrid search,
          and agent-driven retrieval. Consolidates table, column, entity, and metric-view docs.
        </p>
        <button onClick={() => runJob('_build_vector_index', { catalog_name: catalogName, schema_name: schemaName })}
          disabled={loading}
          title="Build metadata_documents table and Delta Sync vector index with managed embeddings for retrieval"
          className="px-4 py-2 bg-dbx-lava text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50">
          Build Vector Index
        </button>
      </Step>

      {/* Step 5 -- Sync */}
      <Step num={5} color="bg-dbx-navy/60" title="Sync & Integrate" prereq="After graph or DDL changes">
        <p className="text-sm text-gray-500 mb-4">
          Push results to downstream systems. <strong>Lakebase</strong> syncs the graph for the GraphRAG agent.
          <strong> DDL Sync</strong> re-applies reviewed metadata edits to Unity Catalog.
        </p>
        <div className="flex gap-3">
          <button onClick={() => runJob('sync_graph_lakebase', { catalog_name: catalogName, schema_name: schemaName })}
            disabled={loading}
            title="Push the knowledge graph to Lakebase PostgreSQL for the GraphRAG agent"
            className="px-4 py-2 bg-slate-700 text-white rounded-md text-sm hover:bg-slate-800 disabled:opacity-50">
            Sync Graph to Lakebase
          </button>
          <button onClick={() => runJob('_sync_ddl_job')}
            disabled={loading}
            title="Re-apply reviewed metadata edits (comments, tags) as ALTER statements to Unity Catalog"
            className="px-4 py-2 bg-slate-700 text-white rounded-md text-sm hover:bg-slate-800 disabled:opacity-50">
            Sync Reviewed DDL
          </button>
        </div>
      </Step>

      {/* Active Runs */}
      {activeRuns.length > 0 && (
        <section className="bg-dbx-oat-light rounded-lg border p-6">
          <div className="flex items-center gap-2 mb-3">
            <h2 className="text-lg font-semibold">Active Runs</h2>
            <span className="inline-block w-2 h-2 bg-blue-500 rounded-full animate-pulse" />
            <span className="text-xs text-gray-400">Auto-refreshing every 5s</span>
          </div>
          {activeRuns.map(r => <RunEntry key={r.run_id} run={r} />)}
        </section>
      )}

      {/* Run History */}
      <section className="bg-dbx-oat-light rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-3">Run History</h2>
        {completedRuns.length === 0 ? (
          <p className="text-sm text-gray-500">No completed runs yet.</p>
        ) : (() => {
          const PAGE_SIZE = 10
          const MAX_RUNS = 50
          const capped = completedRuns.slice(0, MAX_RUNS)
          const totalPages = Math.min(Math.ceil(capped.length / PAGE_SIZE), 5)
          const page = Math.min(historyPage, totalPages - 1)
          const pageRuns = capped.slice(page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE)
          return <>
            {pageRuns.map(r => <RunEntry key={r.run_id} run={r} />)}
            {totalPages > 1 && (
              <div className="flex items-center justify-between mt-3 pt-3 border-t">
                <button onClick={() => setHistoryPage(p => Math.max(0, p - 1))} disabled={page === 0}
                  className="px-3 py-1 text-sm rounded border hover:bg-dbx-oat disabled:opacity-40 disabled:cursor-not-allowed">
                  Previous
                </button>
                <span className="text-sm text-gray-500">Page {page + 1} of {totalPages}</span>
                <button onClick={() => setHistoryPage(p => Math.min(totalPages - 1, p + 1))} disabled={page >= totalPages - 1}
                  className="px-3 py-1 text-sm rounded border hover:bg-dbx-oat disabled:opacity-40 disabled:cursor-not-allowed">
                  Next
                </button>
              </div>
            )}
          </>
        })()}
      </section>

      {/* Available Jobs */}
      <details className="bg-dbx-oat-light rounded-lg border">
        <summary className="px-6 py-3 text-sm font-semibold cursor-pointer hover:bg-dbx-oat">
          Available Jobs ({jobs.length})
        </summary>
        <div className="px-6 pb-4 divide-y">
          {jobs.map(j => (
            <div key={j.job_id} className="py-2 flex justify-between items-center text-sm">
              <span>{j.name}</span>
              <span className="text-gray-400">#{j.job_id}</span>
            </div>
          ))}
        </div>
      </details>
    </div>
  )
}
