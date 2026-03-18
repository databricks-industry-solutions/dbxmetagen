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
          <h3 className="text-sm font-semibold text-red-700 dark:text-red-400 mb-1">Something went wrong</h3>
          <p className="text-xs text-red-600 dark:text-red-300 break-words">{String(this.state.error)}</p>
          <button onClick={() => this.setState({ error: null })} className="btn-ghost btn-sm mt-3 text-red-600">Retry</button>
        </div>
      )
    }
    return this.props.children
  }
}

const TERMINAL_STATES = new Set(['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR'])

const TABS = [
  { id: 'core', label: 'Generate Core Metadata', color: 'bg-dbx-lava' },
  { id: 'advanced', label: 'Generate Advanced Metadata', color: 'bg-dbx-amber' },
  { id: 'assets', label: 'Create Semantic Layer Assets', color: 'bg-dbx-teal' },
]

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
  const label = result ? `${state} / ${result}` : state
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
          <span className="text-sm font-medium truncate text-slate-800 dark:text-slate-200">{run.job_name || 'Unknown Job'}</span>
          {stateBadge(run.state, run.result)}
          {!TERMINAL_STATES.has(run.state) && (
            <span className="inline-block w-2 h-2 bg-blue-500 rounded-full animate-pulse" title="Polling..." />
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
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [ontologyBundle, setOntologyBundle] = useState('general')
  const [entityTagKey, setEntityTagKey] = useState('entity_type')
  const [bundles, setBundles] = useState([])
  const [domainConfig, setDomainConfig] = useState('')
  const [domainConfigs, setDomainConfigs] = useState([])
  const [runHistory, setRunHistory] = useState([])
  const [historyPage, setHistoryPage] = useState(0)
  const [health, setHealth] = useState(null)
  const [activeTab, setActiveTab] = useState('core')
  const [similarityThreshold, setSimilarityThreshold] = useState(0.8)
  const [incremental, setIncremental] = useState(false)
  const [clusterMinK, setClusterMinK] = useState(2)
  const [clusterMaxK, setClusterMaxK] = useState(15)
  const [lakebaseCatalog, setLakebaseCatalog] = useState('')
  const [lakebaseError, setLakebaseError] = useState(null)
  const [lakebaseLoading, setLakebaseLoading] = useState(false)
  const pollRef = useRef(null)

  const [settings, setSettings] = useState({
    model: 'databricks-claude-sonnet-4-6',
    sample_size: 5,
    use_kb_comments: false,
    include_lineage: false,
    build_kb_after: true,
    use_serverless: false,
  })
  const setSetting = (key, value) => setSettings(prev => {
    const next = { ...prev, [key]: value }
    if (key === 'use_serverless' && value) next.build_kb_after = true
    return next
  })

  const getJobSuffix = (isParallel) => {
    if (settings.use_serverless) {
      return isParallel ? '_parallel_serverless_job' : '_metadata_serverless_job'
    }
    if (settings.build_kb_after) {
      return isParallel ? '_parallel_kb_build_job' : '_metadata_kb_build_job'
    }
    return isParallel ? '_parallel_modes_job' : '_metadata_job'
  }

  const [pickerOpen, setPickerOpen] = useState(false)
  const picker = useCatalogSchemaTables()
  const { catalogs: pickerCatalogs, schemas: pickerSchemas, filtered: filteredPickerTables, catalog: pickerCatalog, schema: pickerSchema, filter: pickerFilter, setCatalog: setPickerCatalog, setSchema: setPickerSchema, setFilter: setPickerFilter } = picker
  const pickerTables = picker.tables
  const [pickerSelected, setPickerSelected] = useState([])

  const buildExtraParams = () => {
    const p = {
      model: settings.model,
      sample_size: String(settings.sample_size),
      include_lineage: String(settings.include_lineage),
    }
    if (settings.use_kb_comments) p.use_kb_comments = 'true'
    return p
  }

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
      }
    })
    fetch('/api/ontology/bundles').then(r => r.ok ? r.json() : []).then(setBundles).catch(() => {})
    fetch('/api/domain-configs').then(r => r.ok ? r.json() : []).then(setDomainConfigs).catch(() => {})
    fetch('/api/jobs/health').then(r => r.ok ? r.json() : null).then(setHealth).catch(() => {})
    fetch('/api/jobs/runs').then(r => r.ok ? r.json() : []).then(runs => {
      setRunHistory(runs.map(r => ({ ...r, _polling: false })))
    }).catch(() => {})
  }, [])

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
        state: 'PENDING', result: null, tasks: [],
        run_page_url: null, state_message: null,
      }
      setRunHistory(prev => [newRun, ...prev])
    } catch (e) { setError(e.message) }
    setLoading(false)
  }

  const syncLakebase = async () => {
    setLakebaseError(null)
    setLakebaseLoading(true)
    try {
      await runJob('sync_graph_lakebase', {
        catalog_name: catalogName,
        schema_name: schemaName,
        extra_params: {
          ...(lakebaseCatalog ? { lakebase_catalog: lakebaseCatalog } : {}),
        },
      })
    } catch (e) {
      setLakebaseError(e.message || 'Lakebase sync failed')
    }
    setLakebaseLoading(false)
  }

  const activeRuns = runHistory.filter(r => !TERMINAL_STATES.has(r.state))
  const completedRuns = runHistory.filter(r => TERMINAL_STATES.has(r.state))

  return (
    <div className="space-y-5">
      <PageHeader title="Generate Semantic Layer" subtitle="Build the full semantic metadata layer from your data catalog" badge={catalogName && schemaName ? `${catalogName}.${schemaName}` : undefined} />
      <ErrorBanner error={error} />
      <HealthWarnings health={health} />

      {/* Shared config */}
      <div className="card p-5">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="section-title mb-1.5 block">Catalog</label>
            <input value={catalogName} onChange={e => setCatalogName(e.target.value)}
              placeholder="e.g. my_catalog" className="input-base" />
          </div>
          <div>
            <label className="section-title mb-1.5 block">Schema</label>
            <input value={schemaName} onChange={e => setSchemaName(e.target.value)}
              placeholder="e.g. metadata_results" className="input-base" />
          </div>
        </div>

        <details className="mt-4 group">
          <summary className="section-title cursor-pointer select-none flex items-center gap-1.5 py-2 border-t border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 mt-3 pt-3">
            <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
            Advanced Options
          </summary>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-3 animate-slide-up">
            <div>
              <label className="section-title mb-1.5 block">Ontology Bundle</label>
              <select value={ontologyBundle} onChange={e => setOntologyBundle(e.target.value)} className="select-base">
                {bundles.length > 0 ? bundles.map(b => (
                  <option key={b.key} value={b.key}>{b.name} ({b.entity_count} entities)</option>
                )) : (
                  <option value="" disabled>No bundles found</option>
                )}
              </select>
            </div>
            <div>
              <label className="section-title mb-1.5 block">Domain Config</label>
              <select value={domainConfig} onChange={e => setDomainConfig(e.target.value)} className="select-base">
                <option value="">(Use bundle domains)</option>
                {domainConfigs.map(d => (
                  <option key={d.key} value={d.key}>{d.name} ({d.domain_count} domains)</option>
                ))}
              </select>
            </div>
            <div>
              <label className="section-title mb-1.5 block">Ontology UC Tag Key</label>
              <input value={entityTagKey} onChange={e => setEntityTagKey(e.target.value)}
                placeholder="entity_type" title="Unity Catalog tag key used for entity type classifications"
                className="input-base" />
            </div>
          </div>

          <div className="mt-4 pt-3 border-t border-dbx-oat-dark/30 dark:border-dbx-navy-400/20">
            <span className="section-title">Processing Settings</span>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3">
              <div>
                <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Model</label>
                <select value={settings.model} onChange={e => setSetting('model', e.target.value)} className="select-base !text-xs">
                  <option value="databricks-claude-sonnet-4-6">Claude Sonnet 4.6</option>
                </select>
              </div>
              <div>
                <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Sample Size</label>
                <input type="number" min="0" max="100" value={settings.sample_size}
                  onChange={e => setSetting('sample_size', parseInt(e.target.value) || 0)} className="input-base !text-xs" />
              </div>
              <div className="flex flex-col gap-2 pt-1">
                <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="checkbox" checked={settings.include_lineage} onChange={e => setSetting('include_lineage', e.target.checked)} />
                  Include lineage
                </label>
                <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="checkbox" checked={settings.use_kb_comments} onChange={e => setSetting('use_kb_comments', e.target.checked)} />
                  Use KB comments
                </label>
              </div>
              <div className="flex flex-col gap-2 pt-1">
                <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Build table + column knowledge base after metadata generation so the Review tab is populated">
                  <input type="checkbox" checked={settings.build_kb_after}
                    disabled={settings.use_serverless}
                    onChange={e => setSetting('build_kb_after', e.target.checked)} />
                  Build KB after
                </label>
                <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Run on serverless Databricks compute (faster startup, no cluster to configure)">
                  <input type="checkbox" checked={settings.use_serverless} onChange={e => setSetting('use_serverless', e.target.checked)} />
                  Use serverless
                </label>
              </div>
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
                Core metadata generation uses LLMs to analyze your tables and produce three types of metadata: <strong className="text-slate-700 dark:text-slate-200">column comments</strong> (human-readable descriptions), <strong className="text-slate-700 dark:text-slate-200">PI classification</strong> (PII/PHI/PCI sensitivity labels), and <strong className="text-slate-700 dark:text-slate-200">domain classification</strong> (business domain assignments). These are written to <code className="bg-dbx-oat dark:bg-dbx-navy-500 px-1.5 py-0.5 rounded text-xs font-mono">metadata_generation_log</code> and can be reviewed before applying as DDL.
              </div>
            </details>

            <p className="text-sm text-slate-500 dark:text-slate-400">
              <strong className="text-slate-700 dark:text-slate-200">Single Mode</strong> runs one pass (comment, PI, or domain).
              <strong className="text-slate-700 dark:text-slate-200"> All 3 Modes</strong> runs comments first, then PI + domain in parallel.
            </p>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium text-slate-700 dark:text-slate-200 mb-1.5 block">Table Names</label>
                <textarea value={tableNames} onChange={e => setTableNames(e.target.value)}
                  placeholder="catalog.schema.table1, catalog.schema.*"
                  className="textarea-base h-20" />
                <button onClick={() => setPickerOpen(o => !o)}
                  className="btn-ghost btn-sm mt-1.5 !px-0 text-dbx-teal">
                  <svg className={`w-3 h-3 transition-transform ${pickerOpen ? 'rotate-90' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                  Browse Tables
                </button>
                {pickerOpen && (
                  <div className="mt-2 card p-3 space-y-2 animate-slide-up">
                    <div className="grid grid-cols-3 gap-2">
                      <select value={pickerCatalog} onChange={e => setPickerCatalog(e.target.value)} className="select-base !text-xs !py-1.5">
                        <option value="">Catalog...</option>
                        {pickerCatalogs.map(c => <option key={c} value={c}>{c}</option>)}
                      </select>
                      <select value={pickerSchema} onChange={e => setPickerSchema(e.target.value)} className="select-base !text-xs !py-1.5" disabled={!pickerCatalog}>
                        <option value="">Schema...</option>
                        {pickerSchemas.map(s => <option key={s} value={s}>{s}</option>)}
                      </select>
                      <input value={pickerFilter} onChange={e => setPickerFilter(e.target.value)}
                        placeholder="Filter..." className="input-base !text-xs !py-1.5" aria-label="Filter tables" />
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
                      <p className="text-xs text-slate-400 italic py-2">No tables found</p>
                    )}
                  </div>
                )}
              </div>
              <div className="space-y-3">
                <div>
                  <label className="text-sm font-medium text-slate-700 dark:text-slate-200 mb-1.5 block">Mode</label>
                  <select value={mode} onChange={e => setMode(e.target.value)} className="select-base">
                    <option value="comment">Comment</option>
                    <option value="pi">PI Classification</option>
                    <option value="domain">Domain Classification</option>
                  </select>
                </div>
                <label className="flex items-center gap-2.5 text-sm text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="checkbox" checked={applyDdl} onChange={e => setApplyDdl(e.target.checked)} />
                  Apply DDL directly
                </label>
              </div>
            </div>
            <div className="flex flex-wrap gap-3 mt-2">
              <button onClick={() => runJob(getJobSuffix(false), { table_names: tableNames, mode, apply_ddl: applyDdl, ontology_bundle: ontologyBundle, ...(domainConfig ? { domain_config: domainConfig } : {}), extra_params: buildExtraParams() })}
                disabled={loading || !tableNames.trim()} title="Run a single metadata generation pass"
                className="btn-secondary btn-md">{loading ? 'Starting...' : `Run Single Mode${settings.build_kb_after ? ' + KB' : ''}${settings.use_serverless ? ' (Serverless)' : ''}`}</button>
              <button onClick={() => runJob(getJobSuffix(true), { table_names: tableNames, apply_ddl: applyDdl, ontology_bundle: ontologyBundle, ...(domainConfig ? { domain_config: domainConfig } : {}), extra_params: buildExtraParams() })}
                disabled={loading || !tableNames.trim()} title="Run all three modes in parallel"
                className="btn-primary btn-md">{`All 3 Modes${settings.build_kb_after ? ' + KB' : ''}${settings.use_serverless ? ' (Serverless)' : ''}`}</button>
              <button onClick={() => runJob('_kb_enriched_modes_job', { table_names: tableNames, apply_ddl: applyDdl, ontology_bundle: ontologyBundle, ...(domainConfig ? { domain_config: domainConfig } : {}), extra_params: buildExtraParams() })}
                disabled={loading || !tableNames.trim()} title="Comments -> KB build -> PI + Domain with KB enrichment"
                className="btn-md bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 transition-all">KB-Enriched Modes</button>
            </div>
            <p className="text-xs text-slate-400">
              {settings.build_kb_after && <><strong className="text-slate-500">+ KB</strong>: Builds table + column knowledge base after generation so the Review tab is populated. </>}
              <strong className="text-slate-500">KB-Enriched Modes</strong>: Generates comments, builds the knowledge base, then runs PI + domain classification enriched with KB-generated descriptions.
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
                The advanced metadata pipeline builds on core metadata to produce the full semantic layer foundation: <strong className="text-slate-700 dark:text-slate-200">knowledge bases</strong> (table, column, schema), <strong className="text-slate-700 dark:text-slate-200">knowledge graph</strong> with GraphFrames, <strong className="text-slate-700 dark:text-slate-200">embeddings</strong> and similarity edges, <strong className="text-slate-700 dark:text-slate-200">ontology</strong> (entity discovery, classification, validation), <strong className="text-slate-700 dark:text-slate-200">profiling</strong> (column stats, data quality scores), <strong className="text-slate-700 dark:text-slate-200">FK prediction</strong> (foreign key relationships), <strong className="text-slate-700 dark:text-slate-200">clustering</strong>, and a <strong className="text-slate-700 dark:text-slate-200">vector search index</strong>. This runs as a single Databricks job with 15 orchestrated tasks.
              </div>
            </details>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div>
                <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Similarity Threshold</label>
                <input type="number" step="0.05" min="0" max="1" value={similarityThreshold}
                  onChange={e => setSimilarityThreshold(parseFloat(e.target.value) || 0.8)}
                  title="Controls edge creation threshold for embedding similarity"
                  className="input-base !text-xs" />
              </div>
              <div>
                <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Cluster Min K</label>
                <input type="number" min="1" max="50" value={clusterMinK}
                  onChange={e => setClusterMinK(parseInt(e.target.value) || 2)}
                  className="input-base !text-xs" />
              </div>
              <div>
                <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Cluster Max K</label>
                <input type="number" min="2" max="100" value={clusterMaxK}
                  onChange={e => setClusterMaxK(parseInt(e.target.value) || 15)}
                  className="input-base !text-xs" />
              </div>
              <div className="flex items-end pb-1">
                <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="checkbox" checked={incremental} onChange={e => setIncremental(e.target.checked)} />
                  Incremental mode (skip unchanged tables)
                </label>
              </div>
            </div>

            <button onClick={() => runJob('_full_analytics_pipeline', {
              catalog_name: catalogName, schema_name: schemaName,
              ontology_bundle: ontologyBundle,
              ...(domainConfig ? { domain_config: domainConfig } : {}),
              extra_params: {
                similarity_threshold: String(similarityThreshold),
                incremental: String(incremental),
                cluster_min_k: String(clusterMinK),
                cluster_max_k: String(clusterMaxK),
                ...(entityTagKey !== 'entity_type' ? { entity_tag_key: entityTagKey } : {}),
              },
            })} disabled={loading || !catalogName.trim() || !schemaName.trim()} className="btn-primary btn-md">
              {loading ? 'Starting...' : 'Run Full Pipeline'}
            </button>

            {/* Lakebase sync card */}
            <div className="mt-2 card p-4 border border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 bg-dbx-oat-light/50 dark:bg-dbx-navy/30 space-y-3">
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">Sync Knowledge Graph to Lakebase</h3>
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
                  <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Lakebase Catalog (optional)</label>
                  <input value={lakebaseCatalog} onChange={e => setLakebaseCatalog(e.target.value)}
                    placeholder="e.g. lakebase_catalog" className="input-base !text-xs" />
                </div>
                <button onClick={syncLakebase} disabled={loading || lakebaseLoading || !catalogName.trim() || !schemaName.trim()}
                  className="btn-secondary btn-md whitespace-nowrap">
                  {lakebaseLoading ? 'Syncing...' : 'Sync to Lakebase'}
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
          <EmptyState title="No completed runs yet" description="Run a pipeline above to see results here" />
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
          {jobs.map(j => (
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
