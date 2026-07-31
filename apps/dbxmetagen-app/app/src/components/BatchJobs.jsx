import React, { useState, useEffect, useRef, useCallback, Component } from 'react'
import { ErrorBanner } from '../App'
import { cachedFetchObj, TTL } from '../apiCache'
import { PageHeader, EmptyState, Skeleton, InfoTip } from './ui'
import { useSharedJobRunner, TERMINAL_STATES } from '../hooks/useJobRunner'
import TableScopePicker, { scopeToTableNames } from './TableScopePicker'

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

export default function BatchJobs({ onNavigate, pipelineStats }) {
  const { jobs, runHistory, runningAction, runError, runJob, jobsError } = useSharedJobRunner()
  const [tableNames, setTableNames] = useState('')
  const [applyDdl, setApplyDdl] = useState(false)
  const [federationMode, setFederationMode] = useState(false)
  const [catalogName, setCatalogName] = useState('')
  const [schemaName, setSchemaName] = useState('')
  const [error, setError] = useState(null)
  const [ontologyBundle, setOntologyBundle] = useState(() => {
    try { return localStorage.getItem('dbxmetagen_ontologyBundle') || '' } catch { return '' }
  })
  const [bundles, setBundles] = useState([])
  const [bundlesLoading, setBundlesLoading] = useState(false)
  const [bundlesLoadError, setBundlesLoadError] = useState(null)
  const [domainConfig, setDomainConfig] = useState('')
  const [domainConfigs, setDomainConfigs] = useState([])
  const [historyPage, setHistoryPage] = useState(0)
  const [health, setHealth] = useState(null)
  // Core-metadata run scope: 'all3' runs descriptions + sensitivity + domain;
  // 'comment' / 'pi' / 'domain' run a single mode. Serverless is unified into
  // settings.use_serverless. (The advanced analytics pipeline now lives in the
  // Semantic Layer's foundation gate, not here.)
  const [genMenuOpen, setGenMenuOpen] = useState(false)
  const [skipKbEnrich, setSkipKbEnrich] = useState(false)
  const genMenuRef = useRef(null)
  const [importStatus, setImportStatus] = useState(null)
  const [availableModels, setAvailableModels] = useState(['databricks-claude-sonnet-4-6', 'databricks-gpt-oss-120b'])

  const [settings, setSettings] = useState({
    model: 'databricks-claude-sonnet-4-6',
    sample_size: 5,
    columns_per_call: 20,
    comment_style: 'standard',
    use_kb_comments: false,
    use_customer_context: false,
    include_lineage: true,
    build_kb_after: true,
    use_serverless: true,
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

  // Single entry point for core-metadata generation. `scope` is 'all3' (runs
  // descriptions + sensitivity + domain together) or a single mode
  // ('comment' | 'pi' | 'domain'). By default the all-3 path is KB-enriched
  // (comments -> KB build -> PI/domain with KB context); "skip KB enrich"
  // flips it to the faster non-enriched parallel job. Serverless is read from
  // the unified settings.use_serverless.
  const runGenerate = (scope) => {
    setGenMenuOpen(false)
    const common = {
      table_names: tableNames,
      apply_ddl: applyDdl,
      federation_mode: federationMode,
      ontology_bundle: ontologyBundle,
      use_customer_context: settings.use_customer_context,
      include_lineage: settings.include_lineage,
      ...(domainConfig ? { domain_config: domainConfig } : {}),
      extra_params: buildExtraParams(),
    }
    if (scope === 'all3') {
      if (skipKbEnrich) {
        // All 3 modes in parallel, no KB enrichment between passes.
        runJob(getJobSuffix(true), { ...common, use_kb_comments: settings.use_kb_comments }, 'all3')
      } else {
        // Comments -> KB build -> PI + domain enriched with KB descriptions.
        runJob(settings.use_serverless ? '_kb_enriched_serverless_job' : '_kb_enriched_modes_job', common, 'kb_enriched')
      }
      return
    }
    // Single mode (comment, PI, or domain).
    runJob(getJobSuffix(false), { ...common, mode: scope, use_kb_comments: settings.use_kb_comments }, 'single')
  }

  // Table scope drives `tableNames` (the string the generate handlers read).
  // Core metadata defaults to "Selected" — running descriptions on the ENTIRE
  // workspace is expensive and rarely intended, so require an explicit choice
  // (a pattern like catalog.schema.* still covers a whole schema). A separate
  // advanced "paste patterns" field preserves `*` wildcard support.
  const [coreScope, setCoreScope] = useState({ mode: 'selected', tables: [] })
  const [patternText, setPatternText] = useState('')
  const [showPatterns, setShowPatterns] = useState(false)
  useEffect(() => {
    const fromScope = scopeToTableNames(coreScope)
    const pat = patternText.trim()
    // Patterns (if any) union with the selected tables; else scope alone.
    const combined = [fromScope, pat].filter(Boolean).join(', ')
    setTableNames(combined)
  }, [coreScope, patternText])

  const buildExtraParams = () => ({
    model: settings.model,
    sample_size: String(settings.sample_size),
    columns_per_call: String(settings.columns_per_call),
    comment_style: settings.comment_style,
  })

  useEffect(() => {
    if (!genMenuOpen) return
    const handler = e => { if (genMenuRef.current && !genMenuRef.current.contains(e.target)) setGenMenuOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [genMenuOpen])

  const loadBundles = useCallback(() => {
    setBundlesLoadError(null)
    setBundlesLoading(true)
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
      .finally(() => setBundlesLoading(false))
  }, [])

  useEffect(() => {
    setError(null)
    // Job list + run history are loaded/polled by useJobRunner.
    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data: cfg, error: cfgErr }) => {
      if (cfgErr) setError(prev => prev ? `${prev} | Config: ${cfgErr}` : `Config load failed: ${cfgErr}`)
      if (cfg) {
        setCatalogName(cfg.catalog_name || '')
        setSchemaName(cfg.schema_name || '')
        setSettings(prev => ({
          ...prev,
          model: cfg.model ?? prev.model,
          sample_size: cfg.sample_size ?? prev.sample_size,
          use_kb_comments: cfg.use_kb_comments ?? prev.use_kb_comments,
          use_customer_context: cfg.use_customer_context ?? prev.use_customer_context,
          include_lineage: cfg.include_lineage ?? prev.include_lineage,
        }))
        setApplyDdl(cfg.apply_ddl ?? false)
        setFederationMode(cfg.federation_mode ?? false)
        if (Array.isArray(cfg.available_models) && cfg.available_models.length) setAvailableModels(cfg.available_models)
      }
    })
    loadBundles()
    fetch('/api/domain-configs').then(r => r.ok ? r.json() : []).then(setDomainConfigs)
      .catch(() => setError(prev => prev ? `${prev} | Domain configs could not be loaded` : 'Domain configs could not be loaded'))
    fetch('/api/jobs/health').then(r => r.ok ? r.json() : null).then(setHealth).catch(() => {})
  }, [loadBundles])

  const hasDomainSource = !!(ontologyBundle || domainConfig)

  const activeRuns = runHistory.filter(r => !TERMINAL_STATES.has(r.state))
  const completedRuns = runHistory.filter(r => TERMINAL_STATES.has(r.state))

  return (
    <div className="space-y-5">
      <PageHeader title="Generate Metadata" subtitle="Generate descriptions, sensitivity labels, domains, and advanced analytics from your Unity Catalog tables" badge={catalogName && schemaName ? `${catalogName}.${schemaName}` : undefined} />
      <ErrorBanner error={error} />
      <ErrorBanner error={jobsError} />
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

        <details className="mt-4 group">
          <summary className="section-title cursor-pointer select-none flex items-center gap-1.5 py-2 border-t border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 mt-3 pt-3">
            <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
            Industry &amp; Domain
            <span className="text-xs font-normal text-slate-400 dark:text-slate-500 ml-1">
              — optional for descriptions &amp; sensitivity; required for domain classification and the advanced pipeline
            </span>
          </summary>
          <div className="mt-3 animate-slide-up">
            <div>
              <label className="section-title mb-1.5 flex items-center gap-2">
                Industry Ontology
                <span className="relative group/tip cursor-help">
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-3.5 w-3.5 text-slate-400 group-hover/tip:text-slate-600 dark:group-hover/tip:text-slate-300 transition-colors" viewBox="0 0 20 20" fill="currentColor">
                    <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd"/>
                  </svg>
                  <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-72 p-2 text-xs text-slate-200 bg-slate-800 rounded-lg shadow-lg opacity-0 group-hover/tip:opacity-100 pointer-events-none transition-opacity z-10">
                    Each bundle defines its own entity IDs. Entities from different bundles will not unify — use the same bundle across all data for consistent entity linkage.
                  </span>
                </span>
                {bundlesLoading && (
                  <span className="text-[10px] text-dbx-oat-medium dark:text-dbx-navy-300 italic animate-pulse">Loading...</span>
                )}
                {!bundlesLoading && bundles.length === 0 && !bundlesLoadError && (
                  <span className="text-[10px] text-dbx-oat-medium dark:text-dbx-navy-300 italic">May take a few seconds to load</span>
                )}
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
              <select value={ontologyBundle} onChange={e => {
                const key = e.target.value
                setOntologyBundle(key)
                try { localStorage.setItem('dbxmetagen_ontologyBundle', key) } catch {}
                // The domain-taxonomy override only applies to custom-imported
                // ontologies; clear any lingering selection when switching to a
                // curated/formal bundle so we never send a stale invisible override.
                if (!bundles.find(b => b.key === key)?.custom) setDomainConfig('')
              }} className="select-base">
                <option value="">(None — use domain list only)</option>
                {bundles.length > 0 && (() => {
                  const custom = bundles.filter(b => b.custom)
                  const formal = bundles.filter(b => !b.custom && b.bundle_type === 'formal_ontology')
                  const curated = bundles.filter(b => !b.custom && b.bundle_type !== 'formal_ontology')
                  const counts = (b) => {
                    const parts = [`${b.entity_count} entities`]
                    if (b.edge_count) parts.push(`${b.edge_count} edges`)
                    return parts.join(', ')
                  }
                  const suffix = (b) => (b.standards_alignment && b.standards_alignment !== b.name) ? ` -- ${b.standards_alignment}` : ''
                  return (<>
                    {custom.length > 0 && <optgroup label="Custom (built in Ontology Builder)">
                      {custom.map(b => (
                        <option key={b.key} value={b.key}>
                          {b.name} ({counts(b)})
                        </option>
                      ))}
                    </optgroup>}
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
              <button type="button" onClick={() => onNavigate?.('ontologyBuilder')}
                className="text-xs text-dbx-teal hover:underline mt-1 inline-block">
                Or build a custom ontology &rarr;
              </button>
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
              <details className="mt-2 rounded-lg border border-slate-200/60 dark:border-slate-700/40 bg-slate-50/50 dark:bg-slate-800/30 px-3 py-2 text-xs text-slate-600 dark:text-slate-300">
                <summary className="cursor-pointer font-medium">How to choose an ontology bundle</summary>
                <p className="mt-1.5 leading-relaxed">
                  An ontology bundle defines the entity types, properties, and relationships that dbxmetagen looks for
                  when classifying your tables and columns. Choosing the right bundle improves entity discovery accuracy
                  and produces more meaningful knowledge graph edges.
                </p>
                <ul className="mt-1.5 ml-3 list-disc space-y-1 leading-relaxed">
                  <li><strong>General</strong> &mdash; Good starting point for mixed or unknown data. Covers common patterns (Person, Organization, Location, Event, Product, etc.) across industries.</li>
                  <li><strong>Healthcare / FHIR R4 / OMOP CDM</strong> &mdash; Use for clinical, EHR, or health data. FHIR R4 and OMOP CDM are formal ontologies with standards-aligned entity URIs; Healthcare is a lighter curated bundle.</li>
                  <li><strong>Financial Services</strong> &mdash; Banking, insurance, and capital-markets data (accounts, transactions, instruments, risk, compliance).</li>
                  <li><strong>Retail &amp; CPG</strong> &mdash; Retail, supply chain, and consumer goods data (customers, orders, products, inventory, promotions).</li>
                  <li><strong>Schema.org</strong> &mdash; Broad formal ontology from schema.org. Best for web-originated or loosely structured data.</li>
                </ul>
                <p className="mt-1.5 leading-relaxed">
                  <strong>Formal ontologies</strong> (labeled &ldquo;Formal OWL&rdquo;) are auto-extracted from published OWL/Turtle files
                  and carry entity URIs for standards alignment. <strong>Curated bundles</strong> are hand-authored with industry-specific
                  keywords and tend to classify more aggressively. Bundles marked with a checkmark have pre-built keyword
                  indexes for faster classification.
                </p>
                <p className="mt-1 leading-relaxed">
                  If you are unsure, start with <strong>General</strong>. You can re-run the ontology step later with a
                  different bundle without regenerating core metadata.
                </p>
              </details>
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
                      setImportStatus(`Imported "${name}": ${data.entity_count} entities, ${data.edge_count} edges. Select a Domain Taxonomy below to enable domain classification.`)
                      setTimeout(() => setImportStatus(null), 10000)
                    } else {
                      setError(`Ontology import failed: ${data.error || 'Unknown error'}`)
                    }
                  } catch (err) { setError(`Ontology import error: ${err.message}`) }
                  e.target.value = ''
                }} />
                Import ontology file (.ttl, .owl, or .rdf)
              </label>
              <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">Imported ontologies provide entity types and relationships but no domain definitions — a domain taxonomy fallback appears below when one is selected.</p>
              {importStatus && <p className="text-xs text-green-600 dark:text-green-400 mt-1">{importStatus}</p>}

              {/* Domains derive from the selected bundle automatically. The
                  explicit Domain Taxonomy control only surfaces for custom-imported
                  ontologies, which carry entity types but no domain definitions. */}
              {ontologyBundle && !bundles.find(b => b.key === ontologyBundle)?.custom && (
                <p className="text-xs text-slate-500 dark:text-slate-400 mt-2 flex items-center gap-1.5">
                  Domains come from the selected industry bundle.
                  <InfoTip text="Each ontology bundle carries its own business-domain list, used for domain classification. No separate taxonomy selection is needed. Custom-imported ontologies (.ttl/.owl/.rdf) are the exception — they define entity types but no domains, so they show a taxonomy picker here." />
                </p>
              )}
              {ontologyBundle && bundles.find(b => b.key === ontologyBundle)?.custom && (
                <div className="mt-3 rounded-lg border border-amber-200/60 dark:border-amber-700/30 bg-amber-50/50 dark:bg-amber-900/10 px-3 py-2.5">
                  <label className="section-title mb-1.5 flex items-center gap-2">
                    Domain Taxonomy
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300 font-medium">Required for custom ontology</span>
                  </label>
                  <select value={domainConfig} onChange={e => setDomainConfig(e.target.value)} className="select-base">
                    <option value="">(Select a domain taxonomy for classification)</option>
                    {domainConfigs.map(d => (
                      <option key={d.key} value={d.key}>{d.name} ({d.domain_count} domains)</option>
                    ))}
                  </select>
                  <p className="text-xs text-amber-700 dark:text-amber-300/90 mt-1">This imported ontology has no domains — pick a taxonomy that matches your data's industry to enable domain classification.</p>
                </div>
              )}
            </div>
          </div>
        </details>
      </div>

      {/* Step 1: Generate Core Metadata — always visible, the primary action */}
      <div className="flex items-center gap-2.5 pt-1">
        <span className="flex items-center justify-center w-6 h-6 rounded-full bg-dbx-lava text-white text-xs font-bold shrink-0">1</span>
        <div>
          <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100">Generate core metadata</h2>
          <p className="text-xs text-slate-500 dark:text-slate-400">Descriptions · Sensitivity · Domain</p>
        </div>
      </div>

      <TabErrorBoundary key="core">
      {(
        <section className="card border-l-4 border-l-dbx-lava">
          <div className="p-6 space-y-4">
            <p className="text-sm text-slate-500 dark:text-slate-400 flex items-center gap-1.5">
              Generate <strong className="text-slate-700 dark:text-slate-200">descriptions, sensitivity, and domain</strong> for the chosen tables.
              <InfoTip text="Core metadata = table/column descriptions (comments), PII/PHI/PCI classification, and business-domain classification. Results land in Review & Apply before anything is written. Use the dropdown beside Generate to run just one type for targeted re-runs." />
            </p>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium text-slate-700 dark:text-slate-200 mb-1.5 block">Tables to process</label>
                <TableScopePicker value={coreScope} onChange={setCoreScope} allowAll={false} seedTables={[]} />
                <button onClick={() => setShowPatterns(o => !o)} className="btn-ghost btn-sm mt-1.5 !px-0 text-dbx-teal">
                  <svg className={`w-3 h-3 transition-transform ${showPatterns ? 'rotate-90' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                  Paste patterns / wildcards
                </button>
                {showPatterns && (
                  <div className="mt-1.5">
                    <textarea value={patternText} onChange={e => setPatternText(e.target.value)}
                      placeholder="catalog.schema.*, catalog.schema.table1"
                      title="Comma-separated fully-qualified table names; use * for all tables in a schema. Combined with any tables selected above."
                      className="textarea-base h-16 !text-xs" />
                  </div>
                )}
              </div>
              <div className="space-y-3">
                <p className="text-sm font-medium text-slate-700 dark:text-slate-200">Options</p>
                <label className="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="checkbox" checked={applyDdl} disabled={federationMode} onChange={e => setApplyDdl(e.target.checked)} />
                  Apply to tables immediately
                  <InfoTip text="Applies SQL comments directly to your tables. When on, this writes SQL COMMENT ON statements to your Unity Catalog tables and columns — existing comments will be overwritten. Disable to review results first in the Review tab." />
                </label>
                <label className="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="checkbox" checked={federationMode} onChange={e => {
                    setFederationMode(e.target.checked)
                    if (e.target.checked) setApplyDdl(false)
                  }} />
                  Federation mode (external catalogs)
                  <InfoTip text="Enable for external/federated catalogs (Redshift, Snowflake, etc.). DDL apply is disabled and DESCRIBE EXTENDED is skipped for federated tables." />
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
                <div>
                  <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Controls comment verbosity. Concise: 1-2 sentence columns. Standard: 3-5 sentences (default). Detailed: 4-8+ sentences with analyst-oriented depth.">Comment Style</label>
                  <select value={settings.comment_style} onChange={e => setSetting('comment_style', e.target.value)}
                    className="input-base !text-xs">
                    <option value="concise">Concise</option>
                    <option value="standard">Standard</option>
                    <option value="detailed">Detailed</option>
                  </select>
                </div>
                <div className="flex flex-col gap-2 pt-1">
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Include column-level lineage information from Unity Catalog in the metadata generation prompt">
                    <input type="checkbox" checked={settings.include_lineage} onChange={e => setSetting('include_lineage', e.target.checked)} />
                    Include lineage
                  </label>
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Include previously generated descriptions from the knowledge base as context when running sensitivity or domain classification. Only useful after an initial descriptions run has completed.">
                    <input type="checkbox" checked={settings.use_kb_comments} onChange={e => setSetting('use_kb_comments', e.target.checked)} />
                    Enrich with prior descriptions
                  </label>
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Enrich prompts with customer-provided context from the Customer Context tab. Context is matched hierarchically by catalog, schema, pattern, or table scope.">
                    <input type="checkbox" checked={settings.use_customer_context} onChange={e => setSetting('use_customer_context', e.target.checked)} />
                    Use customer context
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
            {/* Primary generate: main click runs all 3 modes (KB-enriched by
                default); the caret dropdown picks all-3 or a single mode. */}
            <div className="flex flex-wrap items-center gap-3 mt-2">
              <div className="relative inline-flex" ref={genMenuRef}>
                <button onClick={() => runGenerate('all3')}
                  disabled={!!runningAction || !tableNames.trim()}
                  title="Generate all three metadata types (descriptions, sensitivity, and domain). By default, descriptions are generated first and used to enrich sensitivity and domain classification."
                  className="btn-primary btn-md rounded-r-none">
                  {(runningAction === 'kb_enriched' || runningAction === 'all3') ? 'Starting...' : 'Generate Metadata'}
                </button>
                <button onClick={() => setGenMenuOpen(o => !o)}
                  disabled={!!runningAction || !tableNames.trim()}
                  aria-label="Choose what to generate"
                  className="btn-primary btn-md rounded-l-none border-l border-white/25 px-2">
                  <svg className={`w-4 h-4 transition-transform ${genMenuOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {genMenuOpen && (
                  <div className="absolute top-full left-0 mt-1 w-64 z-20 card p-1.5 shadow-elevated animate-slide-up">
                    <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400 px-2 py-1">Run</p>
                    <button onClick={() => runGenerate('all3')} disabled={!!runningAction || !tableNames.trim()}
                      className="w-full text-left px-2 py-1.5 rounded-md text-sm text-slate-700 dark:text-slate-200 hover:bg-dbx-oat/60 dark:hover:bg-dbx-navy-500 disabled:opacity-50">
                      All three <span className="text-xs text-slate-400">· descriptions · sensitivity · domain</span>
                    </button>
                    <div className="border-t border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 my-1" />
                    <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400 px-2 py-1">Just one</p>
                    <button onClick={() => runGenerate('comment')} disabled={!!runningAction || !tableNames.trim()}
                      className="w-full text-left px-2 py-1.5 rounded-md text-sm text-slate-700 dark:text-slate-200 hover:bg-dbx-oat/60 dark:hover:bg-dbx-navy-500 disabled:opacity-50">
                      Descriptions
                    </button>
                    <button onClick={() => runGenerate('pi')} disabled={!!runningAction || !tableNames.trim()}
                      className="w-full text-left px-2 py-1.5 rounded-md text-sm text-slate-700 dark:text-slate-200 hover:bg-dbx-oat/60 dark:hover:bg-dbx-navy-500 disabled:opacity-50">
                      Sensitivity (PII / PHI / PCI)
                    </button>
                    <button onClick={() => runGenerate('domain')} disabled={!!runningAction || !tableNames.trim() || !hasDomainSource}
                      title={!hasDomainSource ? 'Select an ontology bundle or domain list first' : ''}
                      className="w-full text-left px-2 py-1.5 rounded-md text-sm text-slate-700 dark:text-slate-200 hover:bg-dbx-oat/60 dark:hover:bg-dbx-navy-500 disabled:opacity-50">
                      Business Domain{!hasDomainSource ? ' (needs ontology/domain)' : ''}
                    </button>
                  </div>
                )}
              </div>
              <span className="text-xs text-slate-400 dark:text-slate-500">
                Runs on {settings.use_serverless ? 'serverless' : 'classic'} compute · {skipKbEnrich ? 'no KB enrichment' : 'KB-enriched'}
              </span>
            </div>

            <details className="group mt-1">
              <summary className="text-xs text-slate-500 dark:text-slate-400 cursor-pointer select-none inline-flex items-center gap-1.5">
                <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
                Other run options
              </summary>
              <div className="mt-2 text-xs text-slate-500 dark:text-slate-400 space-y-2 animate-slide-up pl-1">
                <label className="flex items-start gap-2 cursor-pointer" title="Run all three modes in parallel without building the knowledge base between passes. Faster, but sensitivity/domain prompts won't see the generated descriptions unless DDL was already applied. Best when tables already have good comments.">
                  <input type="checkbox" checked={skipKbEnrich} onChange={e => setSkipKbEnrich(e.target.checked)} className="mt-0.5" />
                  <span>Skip knowledge-base enrichment <span className="text-slate-400">(faster; sensitivity/domain won't see fresh descriptions)</span></span>
                </label>
                <p className="text-slate-400 flex items-center gap-1.5">
                  <span><strong className="text-slate-500">Default (KB-enriched):</strong> descriptions first, then sensitivity + domain enriched with them.</span>
                  <InfoTip text="Descriptions are generated first, then a knowledge-base build runs, then sensitivity + domain classification are enriched with those descriptions — so they benefit from the new comments even before DDL is applied." />
                </p>
              </div>
            </details>

            <div className="border-t border-slate-200/80 dark:border-dbx-navy-400/20 pt-4 mt-2">
              <button onClick={() => onNavigate?.('context')} className="btn-ghost btn-sm text-slate-600 dark:text-slate-300">
                <svg className="w-4 h-4 mr-1.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
                </svg>
                Customer Context
                <span className="text-xs text-slate-400 dark:text-slate-500 ml-1.5">Add domain knowledge to enrich LLM prompts</span>
              </button>
            </div>
          </div>
        </section>
      )}
      </TabErrorBoundary>

      {/* Next steps: the advanced pipeline lives in the Semantic Layer and the
          post-generation maintenance actions live in Sync & Ops. Simple links
          to where each action actually runs, instead of full pointer cards. */}
      {onNavigate && (
        <div className="pt-1">
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-400 dark:text-slate-500 mb-2">Next steps</p>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            <button onClick={() => onNavigate('semantic')} className="btn-secondary btn-sm">Generate Semantic Layer</button>
            <button onClick={() => onNavigate('syncops')} className="btn-secondary btn-sm">Post-review Sync</button>
            <button onClick={() => onNavigate('syncops')} className="btn-secondary btn-sm">Lakebase</button>
            <button onClick={() => onNavigate('syncops')} className="btn-secondary btn-sm">Create MCPs</button>
          </div>
        </div>
      )}

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

      {jobs.length === 0 && error?.includes('Failed to load jobs') && (
        <p className="text-xs text-slate-400 dark:text-slate-500 py-2">Could not load jobs — check your connection and try refreshing.</p>
      )}
    </div>
  )
}
