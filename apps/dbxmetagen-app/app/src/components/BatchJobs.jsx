import React, { useState, useEffect, useRef, useCallback, Component } from 'react'
import { ErrorBanner } from '../App'
import { cachedFetchObj, TTL } from '../apiCache'
import { PageHeader, EmptyState, Skeleton } from './ui'
import { useCatalogSchemaTables } from '../hooks/useCatalogSchemaTables'

class TabErrorBoundary extends Component {
  state = { error: null }
  static getDerivedStateFromError(error) { return { error } }
  render() {
    if (this.state.error) {
      return (
        <div className="card p-6 border-l-4 border-l-red-400">
          <h3 className="text-sm font-semibold text-red-700 dark:text-red-400 mb-1">This section encountered an error</h3>
          <p className="text-xs text-red-600 dark:text-red-300 break-words mb-1">{String(this.state.error)}</p>
          <p className="text-xs text-slate-500 dark:text-slate-400">Try again, or refresh the page if the problem persists.</p>
          <button onClick={() => this.setState({ error: null })} className="btn-ghost btn-sm mt-3 text-red-600">Retry</button>
        </div>
      )
    }
    return this.props.children
  }
}

const TERMINAL_STATES = new Set(['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR'])

const TABS = [
  { id: 'core', label: 'Generate Core Metadata', sub: 'Descriptions \u00b7 Sensitivity \u00b7 Domain', color: 'bg-dbx-lava' },
  { id: 'advanced', label: 'Generate Advanced Metadata', sub: 'Ontology \u00b7 Foreign Keys \u00b7 Knowledge Graph', color: 'bg-dbx-amber' },
  { id: 'assets', label: 'Semantic Layer Assets', sub: 'Metric Views', color: 'bg-dbx-teal' },
]

const STATUS_LABELS = {
  'RUNNING': 'Running', 'PENDING': 'Queued', 'SKIPPED': 'Skipped', 'INTERNAL_ERROR': 'Internal Error',
  'SUCCESS': 'Succeeded', 'FAILED': 'Failed', 'TIMEDOUT': 'Timed Out', 'CANCELLED': 'Cancelled',
}

function stateBadge(state, result) {
  if (!state) return null
  let color = 'bg-dbx-oat text-slate-600 dark:bg-dbx-navy-500 dark:text-slate-300'
  if (state === 'RUNNING' || state === 'PENDING') color = 'bg-blue-50 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300'
  if (state === 'TERMINATED' && result === 'SUCCESS') color = 'bg-emerald-50 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300'
  if (state === 'TERMINATED' && result === 'FAILED') color = 'bg-red-50 text-red-700 dark:bg-red-900/40 dark:text-red-300'
  if (state === 'TERMINATED' && result === 'TIMEDOUT') color = 'bg-amber-50 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300'
  if (state === 'TERMINATED' && result === 'CANCELLED') color = 'bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400'
  if (state === 'SKIPPED') color = 'bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400'
  if (state === 'INTERNAL_ERROR') color = 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300'
  const label = result ? (STATUS_LABELS[result] || result) : (STATUS_LABELS[state] || state)
  return <span className={`badge ${color}`}>{label}</span>
}

function runAccentColor(run) {
  if (!TERMINAL_STATES.has(run.state)) return 'border-l-blue-400'
  if (run.result === 'SUCCESS') return 'border-l-emerald-400'
  if (run.result === 'FAILED') return 'border-l-red-400'
  return 'border-l-slate-300 dark:border-l-slate-600'
}

function TaskProgress({ tasks }) {
  if (!tasks || tasks.length === 0) return null
  const done = tasks.filter(t => TERMINAL_STATES.has(t.state)).length
  const failed = tasks.filter(t => t.result === 'FAILED').length
  return (
    <div className="mt-2.5">
      <div className="flex items-center gap-2 mb-1.5">
        <span className="text-xs text-slate-500 dark:text-slate-400">Tasks: {done}/{tasks.length}</span>
        {failed > 0 && <span className="text-xs text-red-600 dark:text-red-400 font-medium">{failed} failed</span>}
      </div>
      <div className="flex gap-1">
        {tasks.map(t => {
          let bg = 'bg-slate-200 dark:bg-dbx-navy-500'
          if (t.state === 'RUNNING' || t.state === 'PENDING') bg = 'bg-blue-400 animate-pulse'
          if (t.result === 'SUCCESS') bg = 'bg-emerald-500'
          if (t.result === 'FAILED') bg = 'bg-red-500'
          if (t.result === 'EXCLUDED') bg = 'bg-slate-300 dark:bg-dbx-navy-400'
          return <div key={t.task_key} className={`h-2.5 flex-1 rounded-full ${bg}`} title={`${t.task_key}: ${t.state}${t.result ? ' / ' + t.result : ''}`} />
        })}
      </div>
    </div>
  )
}

function RunEntry({ run }) {
  const [expanded, setExpanded] = useState(false)
  const hasTasks = run.tasks && run.tasks.length > 0
  return (
    <div className={`py-3 px-4 border-l-4 ${runAccentColor(run)} border-b border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 last:border-b-0`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2.5 min-w-0">
          <span className="text-sm font-medium truncate text-slate-800 dark:text-slate-200">{run.job_name || 'Unnamed Job'}</span>
          {stateBadge(run.state, run.result)}
          {!TERMINAL_STATES.has(run.state) && (
            <span className="inline-block w-2 h-2 bg-blue-500 rounded-full animate-pulse" title="Checking status..." />
          )}
        </div>
        <div className="flex items-center gap-3 shrink-0">
          {run.run_page_url && (
            <a href={run.run_page_url} target="_blank" rel="noopener noreferrer"
              className="text-xs text-dbx-teal hover:text-dbx-teal/80 font-medium">View in Databricks</a>
          )}
          {hasTasks && (
            <button onClick={() => setExpanded(prev => !prev)} className="text-xs text-slate-500 hover:text-slate-700 dark:hover:text-slate-300 font-medium" aria-label={expanded ? 'Hide tasks' : 'Show tasks'}>
              {expanded ? 'Hide tasks' : `${run.tasks.length} tasks`}
            </button>
          )}
          <span className="text-xs text-slate-400 font-mono">#{run.run_id}</span>
        </div>
      </div>
      {run.state_message && run.result === 'FAILED' && (
        <div className="mt-2 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-lg px-3 py-2 break-words">
          {run.state_message}
        </div>
      )}
      {hasTasks && !expanded && <TaskProgress tasks={run.tasks} />}
      {hasTasks && expanded && (
        <div className="mt-2.5 space-y-1 animate-slide-up">
          {run.tasks.map(t => (
            <div key={t.task_key} className="flex items-center justify-between text-xs px-3 py-1.5 bg-dbx-oat-light dark:bg-dbx-navy-500/50 rounded-lg">
              <span className="font-mono text-slate-600 dark:text-slate-300">{t.task_key}</span>
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
    <div className="card border-l-4 border-l-amber-400 px-4 py-3 text-sm space-y-1">
      <span className="font-semibold text-amber-700 dark:text-amber-400">Diagnostics:</span>
      {health.errors.map((e, i) => (
        <p key={i} className="text-amber-600 dark:text-amber-300">{e}</p>
      ))}
    </div>
  )
}

export default function BatchJobs({ onNavigate }) {
  const [jobs, setJobs] = useState([])
  const [tableNames, setTableNames] = useState('')
  const [mode, setMode] = useState('comment')
  const [applyDdl, setApplyDdl] = useState(false)
  const [catalogName, setCatalogName] = useState('')
  const [schemaName, setSchemaName] = useState('')
  const [runningAction, setRunningAction] = useState(null)
  const [error, setError] = useState(null)
  const [ontologyBundle, setOntologyBundle] = useState('')
  const [entityTagKey, setEntityTagKey] = useState('entity_type')
  const [bundles, setBundles] = useState([])
  const [bundlesLoadError, setBundlesLoadError] = useState(null)
  const [domainConfig, setDomainConfig] = useState('')
  const [domainConfigs, setDomainConfigs] = useState([])
  const [runHistory, setRunHistory] = useState([])
  const [historyPage, setHistoryPage] = useState(0)
  const [health, setHealth] = useState(null)
  const [activeTab, setActiveTab] = useState('core')
  const [similarityThreshold, setSimilarityThreshold] = useState(0.8)
  const [incremental, setIncremental] = useState(true)
  const [clusterMinK, setClusterMinK] = useState(2)
  const [clusterMaxK, setClusterMaxK] = useState(15)
  const [lakebaseCatalog, setLakebaseCatalog] = useState('')
  const [lakebaseError, setLakebaseError] = useState(null)
  const [lakebaseConfigured, setLakebaseConfigured] = useState(false)
  const [importStatus, setImportStatus] = useState(null)
  const [availableModels, setAvailableModels] = useState(['databricks-claude-sonnet-4-6', 'databricks-gpt-oss-120b'])
  const pollRef = useRef(null)

  const [settings, setSettings] = useState({
    model: 'databricks-claude-sonnet-4-6',
    sample_size: 5,
    columns_per_call: 20,
    use_kb_comments: false,
    include_lineage: false,
    build_kb_after: true,
    use_serverless: true,
  })
  const setSetting = (key, value) => setSettings(prev => {
    const next = { ...prev, [key]: value }
    if (key === 'use_serverless' && value) next.build_kb_after = true
    return next
  })

  const getJobSuffix = () => {
    if (settings.use_serverless) return '_metadata_serverless_job'
    if (settings.build_kb_after) return '_metadata_kb_build_job'
    return '_metadata_job'
  }

  const [pickerOpen, setPickerOpen] = useState(false)
  const picker = useCatalogSchemaTables()
  const { catalogs: pickerCatalogs, schemas: pickerSchemas, filtered: filteredPickerTables, catalog: pickerCatalog, schema: pickerSchema, filter: pickerFilter, setCatalog: setPickerCatalog, setSchema: setPickerSchema, setFilter: setPickerFilter } = picker
  const pickerTables = picker.tables
  const [pickerSelected, setPickerSelected] = useState([])

  const buildExtraParams = () => ({
    model: settings.model,
    sample_size: String(settings.sample_size),
    columns_per_call: String(settings.columns_per_call),
  })

  const loadBundles = useCallback(() => {
    setBundlesLoadError(null)
    fetch('/api/ontology/bundles')
      .then(r => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
        return r.json()
      })
      .then(data => {
        setBundles(Array.isArray(data) ? data : [])
        setBundlesLoadError(null)
      })
      .catch(e => {
        setBundles([])
        setBundlesLoadError(e.message || 'Failed to load bundles')
      })
  }, [])

  useEffect(() => {
    setError(null)
    fetch('/api/jobs').then(r => {
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
      return r.json()
    }).then(setJobs)
      .catch(e => setError(`Failed to load jobs: ${e.message}`))

    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data: cfg, error: cfgErr }) => {
      if (cfgErr) setError(prev => prev ? `${prev} | Config: ${cfgErr}` : `Config load failed: ${cfgErr}`)
      if (cfg) {
        setCatalogName(cfg.catalog_name || '')
        setSchemaName(cfg.schema_name || '')
        if (cfg.catalog_name) setPickerCatalog(cfg.catalog_name)
        setSettings(prev => ({
          ...prev,
          model: cfg.model ?? prev.model,
          sample_size: cfg.sample_size ?? prev.sample_size,
          use_kb_comments: cfg.use_kb_comments ?? prev.use_kb_comments,
          include_lineage: cfg.include_lineage ?? prev.include_lineage,
        }))
        setApplyDdl(cfg.apply_ddl ?? false)
        if (Array.isArray(cfg.available_models) && cfg.available_models.length) setAvailableModels(cfg.available_models)
        setLakebaseConfigured(!!cfg.lakebase_configured)
      }
    })
    loadBundles()
    fetch('/api/domain-configs').then(r => r.ok ? r.json() : []).then(setDomainConfigs)
      .catch(() => setError(prev => prev ? `${prev} | Domain configs could not be loaded` : 'Domain configs could not be loaded'))
    fetch('/api/jobs/health').then(r => r.ok ? r.json() : null).then(setHealth).catch(() => {})
    fetch('/api/jobs/runs').then(r => r.ok ? r.json() : []).then(runs => {
      setRunHistory(runs.map(r => ({ ...r, _polling: false })))
    }).catch(() => setError(prev => prev ? `${prev} | Run history could not be loaded` : 'Run history could not be loaded'))
  }, [loadBundles])

  useEffect(() => { setPickerSelected([]) }, [pickerCatalog, pickerSchema])

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

  const runHistoryRef = useRef(runHistory)
  useEffect(() => { runHistoryRef.current = runHistory }, [runHistory])

  const pollActiveRuns = useCallback(async () => {
    const active = runHistoryRef.current.filter(r => !TERMINAL_STATES.has(r.state))
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
  }, [])

  useEffect(() => {
    const hasActive = runHistory.some(r => !TERMINAL_STATES.has(r.state))
    if (hasActive && !pollRef.current) {
      pollRef.current = setInterval(pollActiveRuns, 5000)
    } else if (!hasActive && pollRef.current) {
      clearInterval(pollRef.current)
      pollRef.current = null
    }
    return () => { if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null } }
  }, [runHistory, pollActiveRuns])

  const findJob = (suffix) => jobs.find(j => j.name?.endsWith(suffix))

  const hasDomainSource = !!(ontologyBundle || domainConfig)
  const needsDomain = mode === 'domain' || mode === 'all'

  const [runError, setRunError] = useState(null)

  const runJob = async (jobNameSuffix, params = {}, actionKey = 'default') => {
    setRunningAction(actionKey)
    setRunError(null)
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
      const data = await res.json().catch(() => ({}))
      if (!res.ok) { setRunError(data.detail || `Failed to start job (${res.status})`); setRunningAction(null); return }
      const newRun = {
        ...data,
        job_name: match?.name || jobNameSuffix,
        state: 'PENDING', result: null, tasks: [],
        run_page_url: null, state_message: null,
      }
      setRunHistory(prev => [newRun, ...prev])
    } catch (e) { setRunError(e.message) }
    setRunningAction(null)
  }

  const syncLakebase = async () => {
    setLakebaseError(null)
    try {
      await runJob('sync_graph_lakebase', {
        catalog_name: catalogName,
        schema_name: schemaName,
        extra_params: {
          ...(lakebaseCatalog ? { lakebase_catalog: lakebaseCatalog } : {}),
        },
      }, 'lakebase')
    } catch (e) {
      setLakebaseError(e.message || 'Lakebase sync failed')
    }
  }

  const activeRuns = runHistory.filter(r => !TERMINAL_STATES.has(r.state))
  const completedRuns = runHistory.filter(r => TERMINAL_STATES.has(r.state))

  return (
    <div className="space-y-5">
      <PageHeader title="Generate Metadata" subtitle="Generate descriptions, sensitivity labels, domains, and advanced analytics from your Unity Catalog tables" badge={catalogName && schemaName ? `${catalogName}.${schemaName}` : undefined} />
      <ErrorBanner error={error} />
      <ErrorBanner error={runError} />
      <HealthWarnings health={health} />

      {/* Shared config */}
      <div className="card p-5">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="section-title mb-1.5 block">Output Catalog</label>
            <span className="input-base block bg-gray-50 text-gray-700 cursor-default">{catalogName || '(not configured)'}</span>
          </div>
          <div>
            <label className="section-title mb-1.5 block">Output Schema</label>
            <span className="input-base block bg-gray-50 text-gray-700 cursor-default">{schemaName || '(not configured)'}</span>
          </div>
          <p className="text-xs text-gray-400 col-span-2">Set before deployment in the project settings.</p>
        </div>

        <details className="mt-4 group" open>
          <summary className="section-title cursor-pointer select-none flex items-center gap-1.5 py-2 border-t border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 mt-3 pt-3">
            <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
            Ontology &amp; Business Domain
          </summary>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-3 animate-slide-up">
            <div>
              <label className="section-title mb-1.5 flex items-center gap-2">
                Industry Ontology
                {(() => {
                  const sel = bundles.find(b => b.key === ontologyBundle)
                  const isFormal = sel?.bundle_type === 'formal_ontology'
                  const hasTiers = sel?.has_tier_indexes
                  const isV2 = sel?.format_version === '2.0'
                  return (<>
                    {isFormal && (
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-violet-100 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300 font-medium" title="Entities auto-extracted from published OWL/Turtle ontology">
                        Formal OWL
                      </span>
                    )}
                    {hasTiers && !isFormal && (
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300 font-medium" title="Three-pass prediction enabled via formal ontology tier indexes">
                        Formally Grounded
                      </span>
                    )}
                    {isV2 && (
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300 font-medium" title="OWL v2 format with entity URIs and source ontology alignment">
                        OWL v2
                      </span>
                    )}
                  </>)
                })()}
              </label>
              <select value={ontologyBundle} onChange={e => setOntologyBundle(e.target.value)} className="select-base">
                <option value="">(None — use domain list only)</option>
                {bundles.length > 0 && (() => {
                  const formal = bundles.filter(b => b.bundle_type === 'formal_ontology')
                  const curated = bundles.filter(b => b.bundle_type !== 'formal_ontology')
                  const counts = (b) => {
                    const parts = [`${b.entity_count} entities`]
                    if (b.edge_count) parts.push(`${b.edge_count} edges`)
                    return parts.join(', ')
                  }
                  const suffix = (b) => (b.standards_alignment && b.standards_alignment !== b.name) ? ` -- ${b.standards_alignment}` : ''
                  return (<>
                    {formal.length > 0 && <optgroup label="Formal Ontologies (from OWL or Turtle files)">
                      {formal.map(b => (
                        <option key={b.key} value={b.key}>
                          {b.name} ({counts(b)}){suffix(b)}
                        </option>
                      ))}
                    </optgroup>}
                    {curated.length > 0 && <optgroup label="Curated Industry Bundles">
                      {curated.map(b => (
                        <option key={b.key} value={b.key}>
                          {b.has_tier_indexes ? '\u2713 ' : ''}{b.name} ({counts(b)}){suffix(b)}
                        </option>
                      ))}
                    </optgroup>}
                  </>)
                })()}
              </select>
              {bundlesLoadError && (
                <p className="text-xs text-amber-700 dark:text-amber-300 mt-1.5">
                  Could not load ontology bundles: {bundlesLoadError}.{' '}
                  <button type="button" onClick={loadBundles} className="underline font-medium">Retry</button>
                </p>
              )}
              {(() => {
                const sel = bundles.find(b => b.key === ontologyBundle)
                if (!sel?.description) return null
                return (
                  <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">
                    {sel.description}
                    {sel.source_url && <> &mdash; <a href={sel.source_url} target="_blank" rel="noopener noreferrer" className="underline hover:text-blue-500">view source</a></>}
                  </p>
                )
              })()}
              {(() => {
                const sel = bundles.find(b => b.key === ontologyBundle)
                if (!sel?.tier_indexes_stale) return null
                return (
                  <details className="mt-2 rounded-lg border border-amber-200/60 dark:border-amber-700/30 bg-amber-50/50 dark:bg-amber-900/10 px-3 py-2 text-xs text-amber-800 dark:text-amber-300">
                    <summary className="cursor-pointer font-medium">Index rebuild available</summary>
                    <p className="mt-1 text-amber-700 dark:text-amber-300/80">
                      The bundle definition file has changed since the keyword search indexes were last generated.
                      This does not affect correctness -- ontology matching still works. Rebuilding indexes can improve
                      keyword search quality for entity classification.
                    </p>
                    <button
                      className="mt-1.5 px-2.5 py-1 text-xs font-medium rounded bg-amber-200 dark:bg-amber-800 text-amber-900 dark:text-amber-100 hover:bg-amber-300 dark:hover:bg-amber-700 disabled:opacity-50"
                      disabled={!!runningAction}
                      onClick={async () => {
                        try {
                          const resp = await fetch(`/api/ontology/bundles/${ontologyBundle}/rebuild-indexes`, { method: 'POST' })
                          const data = await resp.json()
                          if (resp.ok) {
                            loadBundles()
                          } else {
                            setError(`Index rebuild failed: ${data.error || 'Unknown error'}`)
                          }
                        } catch (err) { setError(`Index rebuild error: ${err.message}`) }
                      }}
                    >Rebuild indexes</button>
                  </details>
                )
              })()}
              <label className="inline-flex items-center gap-2 mt-2 text-xs text-slate-600 dark:text-slate-400 cursor-pointer hover:text-blue-600">
                <input type="file" accept=".ttl,.owl,.rdf" className="hidden" onChange={async (e) => {
                  const f = e.target.files?.[0]
                  if (!f) return
                  const name = f.name.replace(/\.(ttl|owl|rdf)$/, '').replace(/[^a-zA-Z0-9_-]/g, '_')
                  const fd = new FormData()
                  fd.append('file', f)
                  fd.append('bundle_name', name)
                  try {
                    const resp = await fetch('/api/ontology/import', { method: 'POST', body: fd })
                    const data = await resp.json()
                    if (resp.ok) {
                      loadBundles()
                      setOntologyBundle(name)
                      setImportStatus(`Imported "${name}": ${data.entity_count} entities, ${data.edge_count} edges`)
                      setTimeout(() => setImportStatus(null), 6000)
                    } else {
                      setError(`Ontology import failed: ${data.error || 'Unknown error'}`)
                    }
                  } catch (err) { setError(`Ontology import error: ${err.message}`) }
                  e.target.value = ''
                }} />
                Import ontology file (.ttl, .owl, or .rdf)
              </label>
              {importStatus && <p className="text-xs text-green-600 dark:text-green-400 mt-1">{importStatus}</p>}
            </div>
            <div>
              <label className="section-title mb-1.5 block">Business Domain List</label>
              <select value={domainConfig} onChange={e => setDomainConfig(e.target.value)} className="select-base">
                <option value="">{ontologyBundle ? '(Use domains from selected ontology)' : '(No domain list selected)'}</option>
                {domainConfigs.map(d => (
                  <option key={d.key} value={d.key}>{d.name} ({d.domain_count} domains)</option>
                ))}
              </select>
              {ontologyBundle && <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Replaces the default domain list from the selected ontology.</p>}
            </div>
          </div>
        </details>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 bg-dbx-oat dark:bg-dbx-navy-500/50 rounded-xl p-1">
        {TABS.map(tab => (
          <button key={tab.id} onClick={() => setActiveTab(tab.id)}
            className={`flex-1 py-2.5 px-4 rounded-lg text-sm font-medium transition-all ${
              activeTab === tab.id
                ? 'bg-white dark:bg-dbx-navy text-slate-800 dark:text-slate-100 shadow-sm'
                : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'
            }`}>
            {tab.label}
            {tab.sub && <span className="block text-[10px] font-normal opacity-60 mt-0.5">{tab.sub}</span>}
          </button>
        ))}
      </div>

      <TabErrorBoundary key={activeTab}>
      {/* Tab 1: Generate Core Metadata */}
      {activeTab === 'core' && (
        <section className="card border-l-4 border-l-dbx-lava overflow-hidden">
          <div className="p-6 space-y-4">
            <details className="group">
              <summary className="text-sm font-medium text-slate-600 dark:text-slate-300 cursor-pointer select-none flex items-center gap-1.5">
                <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
                What is core metadata?
              </summary>
              <div className="mt-2 text-sm text-slate-500 dark:text-slate-400 bg-dbx-oat-light dark:bg-dbx-navy-500/30 rounded-lg p-4 animate-slide-up">
                Core metadata generation uses LLMs to analyze your tables and produce three types of metadata: <strong className="text-slate-700 dark:text-slate-200">table and column descriptions</strong> (human-readable comments), <strong className="text-slate-700 dark:text-slate-200">sensitive data classification</strong> (PII/PHI/PCI labels), and <strong className="text-slate-700 dark:text-slate-200">business domain classification</strong>. Results are written to a review log and can be inspected in the Review tab before being applied to your tables.
              </div>
            </details>

            <p className="text-sm text-slate-500 dark:text-slate-400">
              <strong className="text-slate-700 dark:text-slate-200">Run Selected Mode</strong> runs one generation type at a time.{' '}
              <strong className="text-slate-700 dark:text-slate-200">All Three</strong> runs descriptions, sensitivity, and domain in a single optimized pass — reads each table once, then uses the generated descriptions as context for classification.
            </p>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium text-slate-700 dark:text-slate-200 mb-1.5 block">Tables to Process</label>
                <textarea value={tableNames} onChange={e => setTableNames(e.target.value)}
                  placeholder="catalog.schema.table1, catalog.schema.*"
                  className="textarea-base h-20" />
                <p className="text-[10px] text-slate-400 dark:text-slate-500 mt-1">Comma-separated. Use <code className="bg-dbx-oat dark:bg-dbx-navy-500 px-1 rounded">*</code> for all tables in a schema.</p>
                <button onClick={() => setPickerOpen(o => !o)}
                  className="btn-ghost btn-sm mt-1.5 !px-0 text-dbx-teal">
                  <svg className={`w-3 h-3 transition-transform ${pickerOpen ? 'rotate-90' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                  Browse Unity Catalog
                </button>
                {pickerOpen && (
                  <div className="mt-2 card p-3 space-y-2 animate-slide-up">
                    <div className="grid grid-cols-3 gap-2">
                      <select value={pickerCatalog} onChange={e => setPickerCatalog(e.target.value)} className="select-base !text-xs !py-1.5">
                        <option value="">Select catalog</option>
                        {pickerCatalogs.map(c => <option key={c} value={c}>{c}</option>)}
                      </select>
                      <select value={pickerSchema} onChange={e => setPickerSchema(e.target.value)} className="select-base !text-xs !py-1.5" disabled={!pickerCatalog}>
                        <option value="">Select schema</option>
                        {pickerSchemas.map(s => <option key={s} value={s}>{s}</option>)}
                      </select>
                      <input value={pickerFilter} onChange={e => setPickerFilter(e.target.value)}
                        placeholder="Search tables..." className="input-base !text-xs !py-1.5" aria-label="Search tables" />
                    </div>
                    {pickerTables.length > 0 && (
                      <>
                        <div className="max-h-40 overflow-y-auto scrollbar-thin border border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 rounded-lg bg-dbx-oat-light dark:bg-dbx-navy/50 p-2 grid grid-cols-2 gap-x-4 gap-y-1">
                          {pickerTables
                            .filter(t => !pickerFilter || t.toLowerCase().includes(pickerFilter.toLowerCase()))
                            .map(t => (
                              <label key={t} className="flex items-center gap-2 text-xs cursor-pointer hover:bg-white dark:hover:bg-dbx-navy-500 px-2 py-1 rounded-lg transition-colors">
                                <input type="checkbox" checked={pickerSelected.includes(t)}
                                  onChange={() => togglePickerTable(t)} />
                                <span className="text-slate-600 dark:text-slate-300">{t}</span>
                              </label>
                            ))}
                        </div>
                        <div className="flex items-center justify-between">
                          <div className="flex gap-3">
                            <button onClick={() => {
                              const visible = pickerTables.filter(t => !pickerFilter || t.toLowerCase().includes(pickerFilter.toLowerCase()))
                              setPickerSelected(visible)
                            }} className="text-xs text-dbx-teal hover:text-dbx-teal/80 font-medium">Select All</button>
                            <button onClick={() => setPickerSelected([])}
                              className="text-xs text-slate-500 hover:text-slate-700 dark:hover:text-slate-300 font-medium">Clear</button>
                          </div>
                          <div className="flex items-center gap-2.5">
                            <span className="text-xs text-slate-500">{pickerSelected.length} selected</span>
                            <button onClick={addSelectedTables} disabled={pickerSelected.length === 0}
                              className="btn-secondary btn-sm">Add Selected</button>
                          </div>
                        </div>
                      </>
                    )}
                    {pickerCatalog && pickerSchema && pickerTables.length === 0 && (
                      <p className="text-xs text-slate-400 italic py-2">No tables found in this schema</p>
                    )}
                  </div>
                )}
              </div>
              <div className="space-y-3">
                <div>
                  <label className="text-sm font-medium text-slate-700 dark:text-slate-200 mb-1.5 block">Generation Mode</label>
                  <select value={mode} onChange={e => setMode(e.target.value)} className="select-base">
                    <option value="comment">Table &amp; Column Descriptions</option>
                    <option value="pi">Sensitive Data (PII / PHI / PCI)</option>
                    <option value="domain" disabled={!hasDomainSource}>Business Domain{!hasDomainSource ? ' (select ontology or domain list first)' : ''}</option>
                    <option value="all">All Three (recommended)</option>
                  </select>
                  {needsDomain && !hasDomainSource && <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">Domain classification requires an ontology bundle or domain list. Select one above, or switch to Descriptions or Sensitivity mode.</p>}
                </div>
                <label className="flex items-center gap-2.5 text-sm text-slate-600 dark:text-slate-300 cursor-pointer"
                  title="Applies SQL comments directly to your tables. Disable this to review results first in the Review tab.">
                  <input type="checkbox" checked={applyDdl} onChange={e => setApplyDdl(e.target.checked)} />
                  Apply to tables immediately
                </label>
              </div>
            </div>

            <details className="group mt-3">
              <summary className="text-xs font-medium text-slate-500 dark:text-slate-400 cursor-pointer select-none flex items-center gap-1.5 py-2 border-t border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 pt-3">
                <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
                Model &amp; Processing Options
              </summary>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3 animate-slide-up">
                <div>
                  <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Model</label>
                  <select value={settings.model} onChange={e => setSetting('model', e.target.value)}
                    className="input-base !text-xs">
                    {availableModels.map(m => <option key={m} value={m}>{m}</option>)}
                  </select>
                </div>
                <div>
                  <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Number of rows sampled per table. Higher values improve quality but increase cost. 0 uses the default.">Rows Sampled per Table</label>
                  <input type="number" min="0" max="100" value={settings.sample_size}
                    onChange={e => setSetting('sample_size', parseInt(e.target.value) || 0)} className="input-base !text-xs" />
                </div>
                <div>
                  <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Number of columns sent to the LLM per prompt chunk. Tables with more columns than this value are split into multiple LLM calls. Lower values reduce prompt size; higher values reduce the number of calls.">Columns per LLM Call</label>
                  <input type="number" min="1" max="100" value={settings.columns_per_call}
                    onChange={e => setSetting('columns_per_call', Math.max(1, parseInt(e.target.value) || 20))} className="input-base !text-xs" />
                </div>
                <div className="flex flex-col gap-2 pt-1">
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Include column-level lineage information from Unity Catalog in the metadata generation prompt">
                    <input type="checkbox" checked={settings.include_lineage} onChange={e => setSetting('include_lineage', e.target.checked)} />
                    Include lineage
                  </label>
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Use existing knowledge base comments as additional context when generating metadata">
                    <input type="checkbox" checked={settings.use_kb_comments} onChange={e => setSetting('use_kb_comments', e.target.checked)} />
                    Use knowledge base descriptions
                  </label>
                </div>
                <div className="flex flex-col gap-2 pt-1">
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Build table + column knowledge base after metadata generation so the Review tab is populated">
                    <input type="checkbox" checked={settings.build_kb_after}
                      disabled={settings.use_serverless}
                      onChange={e => setSetting('build_kb_after', e.target.checked)} />
                    Build knowledge base after
                  </label>
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Run on serverless Databricks compute (faster startup, no cluster to configure)">
                    <input type="checkbox" checked={settings.use_serverless} onChange={e => setSetting('use_serverless', e.target.checked)} />
                    Use serverless
                  </label>
                </div>
              </div>
            </details>

            <p className="text-xs text-slate-500 dark:text-slate-400 mt-2 font-medium">
              Ontology: {ontologyBundle ? bundles.find(b => b.key === ontologyBundle)?.name || ontologyBundle : <em>None</em>}
              {' | '}Domains: {domainConfig ? domainConfigs.find(d => d.key === domainConfig)?.name || domainConfig : (ontologyBundle ? 'from selected ontology' : <em>none</em>)}
            </p>
            <div className="flex flex-wrap gap-3 mt-2">
              <button onClick={() => runJob(getJobSuffix(), { table_names: tableNames, mode, apply_ddl: applyDdl, ontology_bundle: ontologyBundle, use_kb_comments: settings.use_kb_comments, include_lineage: settings.include_lineage, ...(domainConfig ? { domain_config: domainConfig } : {}), extra_params: buildExtraParams() }, 'single')}
                disabled={!!runningAction || !tableNames.trim() || mode === 'all' || (needsDomain && !hasDomainSource)} title={mode === 'all' ? 'Use the "All Three" button when mode is set to All Three' : (needsDomain && !hasDomainSource) ? 'Select an ontology bundle or domain list to run domain classification' : 'Run only the mode selected above (one generation pass)'}
                className="btn-secondary btn-md">{runningAction === 'single' ? 'Starting...' : `Run Selected Mode${settings.build_kb_after ? ' + KB' : ''}${settings.use_serverless ? ' (Serverless)' : ''}`}</button>
              <button onClick={() => runJob(getJobSuffix(), { table_names: tableNames, mode: 'all', apply_ddl: applyDdl, ontology_bundle: ontologyBundle, use_kb_comments: settings.use_kb_comments, include_lineage: settings.include_lineage, ...(domainConfig ? { domain_config: domainConfig } : {}), extra_params: buildExtraParams() }, 'all3')}
                disabled={!!runningAction || !tableNames.trim() || !hasDomainSource || mode !== 'all'} title={mode !== 'all' ? 'Switch Generation Mode to "All Three" to use this button' : !hasDomainSource ? 'Select an ontology bundle or domain list to run All Three (includes domain classification)' : 'Runs descriptions, then sensitivity and domain using those descriptions — one table scan, best quality'}
                className="btn-primary btn-md">{runningAction === 'all3' ? 'Starting...' : `All Three${settings.build_kb_after ? ' + KB' : ''}${settings.use_serverless ? ' (Serverless)' : ''}`}</button>
              <button onClick={() => runJob('_kb_enriched_modes_job', { table_names: tableNames, apply_ddl: applyDdl, ontology_bundle: ontologyBundle, use_kb_comments: settings.use_kb_comments, include_lineage: settings.include_lineage, ...(domainConfig ? { domain_config: domainConfig } : {}), extra_params: buildExtraParams() }, 'kb_enriched')}
                disabled={!!runningAction || !tableNames.trim() || !hasDomainSource || mode !== 'all'} title={mode !== 'all' ? 'Switch Generation Mode to "All Three" to use this button' : !hasDomainSource ? 'Select an ontology bundle or domain list to run Two-Pass (includes domain classification)' : 'Generates descriptions first, builds the knowledge base, then runs sensitivity and domain classification enriched with those descriptions. Uses classic compute.'}
                className="btn-md bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 transition-all">{runningAction === 'kb_enriched' ? 'Starting...' : 'Two-Pass (KB-Enriched)'}</button>
            </div>
            <p className="text-xs text-slate-400">
              {settings.build_kb_after && <><strong className="text-slate-500">+ KB</strong>: Builds the knowledge base after generation so the Review tab is populated. </>}
              <strong className="text-slate-500">Two-Pass (KB-Enriched)</strong>: Generates descriptions first, builds the knowledge base, then runs sensitivity and domain classification enriched with those descriptions. Uses classic compute.
            </p>
          </div>
        </section>
      )}

      {/* Tab 2: Generate Advanced Metadata */}
      {activeTab === 'advanced' && (
        <section className="card border-l-4 border-l-dbx-amber overflow-hidden">
          <div className="p-6 space-y-5">
            <details className="group">
              <summary className="text-sm font-medium text-slate-600 dark:text-slate-300 cursor-pointer select-none flex items-center gap-1.5">
                <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
                What is advanced metadata?
              </summary>
              <div className="mt-2 text-sm text-slate-500 dark:text-slate-400 bg-dbx-oat-light dark:bg-dbx-navy-500/30 rounded-lg p-4 animate-slide-up">
                The advanced pipeline builds on your core metadata to produce the full semantic layer: <strong className="text-slate-700 dark:text-slate-200">knowledge bases</strong>, a <strong className="text-slate-700 dark:text-slate-200">knowledge graph</strong>, <strong className="text-slate-700 dark:text-slate-200">embeddings</strong> and similarity edges, <strong className="text-slate-700 dark:text-slate-200">ontology</strong> entity discovery, <strong className="text-slate-700 dark:text-slate-200">profiling</strong> and data quality scores, <strong className="text-slate-700 dark:text-slate-200">foreign key prediction</strong>, <strong className="text-slate-700 dark:text-slate-200">table clustering</strong>, and a <strong className="text-slate-700 dark:text-slate-200">vector search index</strong>. This runs as a single Databricks job with 15 orchestrated tasks. <em className="text-slate-400">Requires core metadata to be generated first.</em>
              </div>
            </details>

            <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
              <div>
                <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Minimum embedding similarity score (0–1) for creating edges between columns. Higher values mean fewer, stronger connections.">Similarity Threshold</label>
                <input type="number" step="0.05" min="0" max="1" value={similarityThreshold}
                  onChange={e => setSimilarityThreshold(parseFloat(e.target.value) || 0.8)}
                  title="Minimum embedding similarity for edge creation (0–1)"
                  className="input-base !text-xs" />
              </div>
              <div>
                <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Minimum number of table groups for clustering analysis">Cluster Min Groups</label>
                <input type="number" min="1" max="50" value={clusterMinK}
                  onChange={e => setClusterMinK(parseInt(e.target.value) || 2)}
                  className="input-base !text-xs" />
              </div>
              <div>
                <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Maximum number of table groups for clustering analysis">Cluster Max Groups</label>
                <input type="number" min="2" max="100" value={clusterMaxK}
                  onChange={e => setClusterMaxK(parseInt(e.target.value) || 15)}
                  className="input-base !text-xs" />
              </div>
              <div>
                <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Unity Catalog tag key where entity type classifications are stored">Entity Type Tag Key</label>
                <input value={entityTagKey} onChange={e => setEntityTagKey(e.target.value)}
                  placeholder="entity_type" title="Unity Catalog tag key for entity type classifications"
                  className="input-base !text-xs" />
              </div>
              <div className="flex items-end pb-1">
                <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="checkbox" checked={incremental} onChange={e => setIncremental(e.target.checked)} />
                  Incremental mode (skip unchanged tables)
                </label>
              </div>
            </div>

            <div>
              <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Table Filter <span className="text-slate-400 dark:text-slate-500">(comma-separated; leave blank to include all tables)</span></label>
              <textarea value={tableNames} onChange={e => setTableNames(e.target.value)}
                placeholder="catalog.schema.table1, catalog.schema.table2"
                className="textarea-base h-16 !text-xs" />
            </div>

            {!ontologyBundle && <p className="text-xs text-amber-600 dark:text-amber-400">An ontology bundle must be selected in the Generate Metadata section to run the full analytics pipeline.</p>}
            <button onClick={() => runJob('_full_analytics_pipeline', {
              catalog_name: catalogName, schema_name: schemaName,
              ontology_bundle: ontologyBundle,
              use_kb_comments: settings.use_kb_comments,
              include_lineage: settings.include_lineage,
              ...(tableNames.trim() ? { table_names: tableNames } : {}),
              ...(domainConfig ? { domain_config: domainConfig } : {}),
              extra_params: {
                model: settings.model,
                sample_size: String(settings.sample_size),
                similarity_threshold: String(similarityThreshold),
                incremental: String(incremental),
                cluster_min_k: String(clusterMinK),
                cluster_max_k: String(clusterMaxK),
                ...(entityTagKey !== 'entity_type' ? { entity_tag_key: entityTagKey } : {}),
              },
            }, 'pipeline')} disabled={!!runningAction || !catalogName.trim() || !schemaName.trim() || !ontologyBundle} title={!ontologyBundle ? 'Select an ontology bundle in the Generate Metadata tab to run the full analytics pipeline' : ''} className="btn-primary btn-md">
              {runningAction === 'pipeline' ? 'Starting...' : (tableNames.trim() ? `Run Pipeline (${tableNames.split(',').filter(t => t.trim()).length} tables)` : 'Run Full Pipeline')}
            </button>

            {/* Lakebase sync card */}
            <div className="mt-2 card p-4 border border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 bg-dbx-oat-light/50 dark:bg-dbx-navy/30 space-y-3">
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">Sync Knowledge Graph to Lakebase</h3>
                <span className="badge bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300 text-[10px]">Beta</span>
                {lakebaseConfigured
                  ? <span className="badge bg-emerald-50 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300 text-[10px]">Lakebase Configured</span>
                  : <span className="badge bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400 text-[10px]" title="Lakebase is not configured. The graph will use Delta tables instead.">Using Delta Tables</span>
                }
                <span className="relative group/tip">
                  <svg className="w-4 h-4 text-slate-400 cursor-help" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-72 p-2 text-xs text-slate-200 bg-slate-800 rounded-lg shadow-lg opacity-0 group-hover/tip:opacity-100 pointer-events-none transition-opacity z-10">
                    Syncs the knowledge graph (nodes, edges, entities, relationships) to Lakebase (PostgreSQL) for low-latency graph queries by the exploration agents. Requires a completed analytics pipeline and a configured Lakebase catalog.
                  </span>
                </span>
              </div>
              <div className="flex items-end gap-3">
                <div className="flex-1">
                  <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Target Lakebase Catalog (optional)</label>
                  <input value={lakebaseCatalog} onChange={e => setLakebaseCatalog(e.target.value)}
                    placeholder="e.g. lakebase_catalog" className="input-base !text-xs" />
                </div>
                <button onClick={syncLakebase} disabled={!!runningAction || !catalogName.trim() || !schemaName.trim()}
                  className="btn-secondary btn-md whitespace-nowrap">
                  {runningAction === 'lakebase' ? 'Syncing...' : 'Sync to Lakebase'}
                </button>
              </div>
              {lakebaseError && (
                <div className="text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-lg px-3 py-2">
                  {lakebaseError}
                </div>
              )}
            </div>
          </div>
        </section>
      )}

      {/* Tab 3: Create Semantic Layer Assets */}
      {activeTab === 'assets' && (
        <section className="card border-l-4 border-l-dbx-teal overflow-hidden">
          <div className="p-6 space-y-4">
            <p className="text-sm text-slate-600 dark:text-slate-300">
              Create <strong className="text-slate-700 dark:text-slate-200">metric views</strong>, manage question profiles, and generate reusable KPI definitions from the <strong className="text-slate-700 dark:text-slate-200">Define Metrics</strong> page.
            </p>
            <button onClick={() => onNavigate?.('semantic')} className="btn-primary btn-md">
              Go to Define Metrics
            </button>
          </div>
        </section>
      )}
      </TabErrorBoundary>

      {/* Active Runs */}
      {activeRuns.length > 0 && (
        <section className="card p-5">
          <div className="flex items-center gap-2.5 mb-4">
            <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100">Active Runs</h2>
            <span className="inline-block w-2 h-2 bg-blue-500 rounded-full animate-pulse" />
            <span className="text-xs text-slate-400">Auto-refreshing every 5s</span>
          </div>
          {activeRuns.map(r => <RunEntry key={r.run_id} run={r} />)}
        </section>
      )}

      {/* Run History */}
      <section className="card p-5">
        <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100 mb-4">Run History</h2>
        {completedRuns.length === 0 ? (
          <EmptyState title="No completed runs yet" description="Run a metadata job above to see completed runs here." />
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
              <div className="flex items-center justify-between mt-4 pt-4 border-t border-dbx-oat-dark/30 dark:border-dbx-navy-400/20">
                <button onClick={() => setHistoryPage(p => Math.max(0, p - 1))} disabled={page === 0}
                  className="btn-ghost btn-sm disabled:opacity-30">Previous</button>
                <span className="text-sm text-slate-500 dark:text-slate-400">Page {page + 1} of {totalPages}</span>
                <button onClick={() => setHistoryPage(p => Math.min(totalPages - 1, p + 1))} disabled={page >= totalPages - 1}
                  className="btn-ghost btn-sm disabled:opacity-30">Next</button>
              </div>
            )}
            {runHistory.length >= 50 && (
              <p className="text-xs text-slate-400 mt-2 text-center">Showing 50 most recent runs</p>
            )}
          </>
        })()}
      </section>

      {/* Available Jobs */}
      <details className="card overflow-hidden group">
        <summary className="px-5 py-3 text-sm font-semibold text-slate-700 dark:text-slate-200 cursor-pointer hover:bg-dbx-oat-light dark:hover:bg-dbx-navy-500/50 flex items-center gap-1.5 transition-colors">
          <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
          Available Jobs ({jobs.length})
        </summary>
        <div className="px-5 pb-4 divide-y divide-dbx-oat-dark/30 dark:divide-dbx-navy-400/20">
          {jobs.length === 0 ? (
            <p className="text-xs text-slate-400 dark:text-slate-500 py-4">{error?.includes('Failed to load jobs') ? 'Could not load jobs — check your connection and try refreshing.' : 'No jobs found. Deploy the Databricks Asset Bundle to your workspace first.'}</p>
          ) : jobs.map(j => (
            <div key={j.job_id} className="py-2.5 flex justify-between items-center text-sm">
              <span className="text-slate-700 dark:text-slate-300">{j.name}</span>
              <span className="text-slate-400 font-mono text-xs">#{j.job_id}</span>
            </div>
          ))}
        </div>
      </details>
    </div>
  )
}
