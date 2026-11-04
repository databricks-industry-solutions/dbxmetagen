import React, { useState, useEffect, useRef, useCallback, Component } from 'react'
import { ErrorBanner } from '../App'
import { cachedFetchObj, TTL } from '../apiCache'
import { PageHeader, EmptyState, Skeleton, InfoTip } from './ui'
import { useSharedJobRunner, TERMINAL_STATES, classifyRun } from '../hooks/useJobRunner'
import { useViewMode } from '../hooks/useViewMode'
import { useCertifiedApplyPolicy } from '../hooks/useCertifiedApplyPolicy'
import RunEntry from './RunEntry'
import TableScopePicker, { scopeToTableNames } from './TableScopePicker'
import { useLanguage } from '../contexts/LanguageContext'
import { LS_ONTOLOGY_BUNDLE } from '../constants/storageKeys'

class TabErrorBoundary extends Component {
  state = { error: null }
  static getDerivedStateFromError(error) { return { error } }
  render() {
    if (this.state.error) {
      // Class component can't use hooks; parent passes `t` as a prop.
      const t = this.props.t
      return (
        <div className="card p-6 border-l-4 border-l-red-400">
          <h3 className="text-sm font-semibold text-red-700 dark:text-red-400 mb-1">{t('jobs.sectionError')}</h3>
          <p className="text-xs text-red-600 dark:text-red-300 break-words mb-1">{String(this.state.error)}</p>
          <p className="text-xs text-slate-500 dark:text-slate-400">{t('jobs.sectionErrorHint')}</p>
          <button onClick={() => this.setState({ error: null })} className="btn-ghost btn-sm mt-3 text-red-600">{t('jobs.retryBtn')}</button>
        </div>
      )
    }
    return this.props.children
  }
}

function HealthWarnings({ health }) {
  const { t } = useLanguage()
  if (!health || health.errors.length === 0) return null
  return (
    <div className="card border-l-4 border-l-amber-400 px-4 py-3 text-sm space-y-1">
      <span className="font-semibold text-amber-700 dark:text-amber-400">{t('jobs.diagnostics')}</span>
      {health.errors.map((e, i) => (
        <p key={i} className="text-amber-600 dark:text-amber-300">{e}</p>
      ))}
    </div>
  )
}

export default function BatchJobs({ onNavigate, pipelineStats }) {
  const { t } = useLanguage()
  const { jobs, runHistory, runningAction, runError, runJob, jobsError, cancelRun } = useSharedJobRunner()
  const { simplified } = useViewMode()
  const { policy: certifiedPolicy } = useCertifiedApplyPolicy()
  const [skipCertified, setSkipCertified] = useState(false)
  const [tableNames, setTableNames] = useState('')
  // null = unknown (not yet loaded / transient error); false = never generated
  // metadata here; true = has. Drives the large-first-run nudge in runGenerate.
  const [hasRun, setHasRun] = useState(null)
  const [applyDdl, setApplyDdl] = useState(false)
  const [federationMode, setFederationMode] = useState(false)
  const [catalogName, setCatalogName] = useState('')
  const [schemaName, setSchemaName] = useState('')
  const [error, setError] = useState(null)
  const [ontologyBundle, setOntologyBundle] = useState(() => {
    try { return localStorage.getItem(LS_ONTOLOGY_BUNDLE) || '' } catch { return '' }
  })
  const [bundles, setBundles] = useState([])
  const [bundlesLoading, setBundlesLoading] = useState(false)
  const [bundlesLoadError, setBundlesLoadError] = useState(null)
  const [domainConfig, setDomainConfig] = useState('')
  const [domainConfigs, setDomainConfigs] = useState([])
  // Generate a domain taxonomy YAML from an uploaded document (saved to the volume, then
  // selectable here). Does not touch the built-in configurations/ taxonomies.
  const [showDomainGen, setShowDomainGen] = useState(false)
  const [domainGenFile, setDomainGenFile] = useState(null)
  const [domainGenName, setDomainGenName] = useState('')
  const [domainGenDesc, setDomainGenDesc] = useState('')
  const [domainGenBusy, setDomainGenBusy] = useState(false)
  const [domainGenResult, setDomainGenResult] = useState(null)
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
    columns_per_call: 10,
    comment_style: 'standard',
    use_kb_comments: false,
    use_customer_context: false,
    include_lineage: true,
    include_existing_table_comment: true,  // matches variables.yml global default
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

    // --- Pre-run guardrails (cheap click-through confirms; cost/perf safety) ---
    const raw = tableNames.trim()
    // Wildcards can silently fan out to a whole schema — confirm intent.
    if (raw.includes('*') && !window.confirm(
      'Wildcard patterns (e.g. catalog.schema.*) can match a large number of tables, '
      + 'which can be slow and costly. Continue?'
    )) return
    // First run + a large explicit selection: nudge toward a small pilot. Count
    // only explicit (non-wildcard) entries; wildcards are covered by the check
    // above. Fires only when we KNOW nothing has been generated here (hasRun===false).
    const explicitCount = raw.split(',').map(s => s.trim()).filter(t => t && !t.includes('*')).length
    if (hasRun === false && explicitCount >= 100 && !window.confirm(
      `You're about to run on ${explicitCount} tables, and dbxmetagen hasn't generated `
      + 'metadata here yet. Consider starting with ~10 tables to check cost, performance, '
      + `and quality, then scaling up. Continue with ${explicitCount}?`
    )) return
    // Certified-asset warn: apply is on, policy=warn, and the user has NOT opted into
    // skipping -> the comments/tags on certified assets in scope get overwritten. Warn
    // before the run (#247). The assets and their certified status are never changed.
    if (applyDdl && certifiedPolicy === 'warn' && !skipCertified && !window.confirm(
      'Apply is on and the certified-asset policy is "warn": the comments/tags on any '
      + 'UC-certified tables/columns in scope WILL be overwritten (the assets themselves and '
      + 'their certified status are not changed). Check "Skip applying to certified assets" to '
      + 'leave their comments/tags untouched. Continue?'
    )) return

    // skip_certified_assets is only meaningful when applying; block always skips (the job
    // enforces it regardless), warn skips only when the user opted in.
    const skipCert = certifiedPolicy === 'block' ? true : (applyDdl && skipCertified)
    const common = {
      table_names: tableNames,
      apply_ddl: applyDdl,
      federation_mode: federationMode,
      ontology_bundle: ontologyBundle,
      use_customer_context: settings.use_customer_context,
      include_lineage: settings.include_lineage,
      include_existing_table_comment: settings.include_existing_table_comment,
      ...(skipCert ? { skip_certified_assets: true } : {}),
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
      if (cfgErr) setError(prev => prev ? `${prev} | ${t('jobs.configErr', { err: cfgErr })}` : t('jobs.configLoadFailed', { err: cfgErr }))
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
          include_existing_table_comment: cfg.include_existing_table_comment ?? prev.include_existing_table_comment,
        }))
        setApplyDdl(cfg.apply_ddl ?? false)
        setFederationMode(cfg.federation_mode ?? false)
        if (Array.isArray(cfg.available_models) && cfg.available_models.length) setAvailableModels(cfg.available_models)
      }
    })
    loadBundles()
    fetch('/api/domain-configs').then(r => r.ok ? r.json() : []).then(setDomainConfigs)
      .catch(() => setError(prev => prev ? `${prev} | ${t('jobs.domainConfigsError')}` : t('jobs.domainConfigsError')))
    fetch('/api/jobs/health').then(r => r.ok ? r.json() : null).then(setHealth).catch(() => {})
    // Has metadata ever been generated here? Non-ok / network error leaves hasRun
    // null (unknown) so the first-run nudge stays silent unless we KNOW it's empty.
    fetch('/api/metadata/has-run').then(r => r.ok ? r.json() : null)
      .then(d => { if (d) setHasRun(!!d.has_run) }).catch(() => {})
  }, [loadBundles])

  const hasDomainSource = !!(ontologyBundle || domainConfig)

  const generateDomainConfig = async () => {
    if (!domainGenFile || domainGenBusy) return
    setDomainGenBusy(true); setDomainGenResult(null)
    try {
      const fd = new FormData()
      fd.append('file', domainGenFile)
      if (domainGenName.trim()) fd.append('name', domainGenName.trim())
      if (domainGenDesc.trim()) fd.append('description', domainGenDesc.trim())
      const res = await fetch('/api/domain-configs/generate', { method: 'POST', body: fd })
      const j = await res.json().catch(() => ({}))
      if (!res.ok) { setDomainGenResult({ error: j.detail || `Request failed (${res.status})` }); return }
      setDomainGenResult(j)
      const list = await fetch('/api/domain-configs').then(r => r.ok ? r.json() : [])
      setDomainConfigs(list)
      // Auto-select the new taxonomy (domain-only selection: taxonomy, no ontology).
      setDomainConfig(j.key); setOntologyBundle('')
      try { localStorage.setItem('dbxmetagen_ontologyBundle', '') } catch {}
      setDomainGenFile(null); setDomainGenName('')
    } catch (e) {
      setDomainGenResult({ error: e.message || 'Network error' })
    } finally { setDomainGenBusy(false) }
  }

  // This page shows only core-metadata-generation runs; the advanced analytics
  // pipeline's runs live in the Semantic Layer's advanced section.
  const coreRuns = runHistory.filter(r => classifyRun(r) === 'core')
  const activeRuns = coreRuns.filter(r => !TERMINAL_STATES.has(r.state))
  const completedRuns = coreRuns.filter(r => TERMINAL_STATES.has(r.state))

  return (
    <div className="space-y-5">
      <PageHeader title={t('jobs.pageTitle')} subtitle={t('jobs.pageSubtitle')} badge={catalogName && schemaName ? `${catalogName}.${schemaName}` : undefined} />
      <ErrorBanner error={error} />
      <ErrorBanner error={jobsError} />
      <ErrorBanner error={runError} />
      <HealthWarnings health={health} />

      {/* Shared config */}
      <div className="card p-5">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="section-title mb-1.5 block">{t('jobs.outputCatalog')}</label>
            <span className="input-base block bg-gray-50 text-gray-700 cursor-default">{catalogName || t('jobs.notConfigured')}</span>
          </div>
          <div>
            <label className="section-title mb-1.5 block">{t('jobs.outputSchema')}</label>
            <span className="input-base block bg-gray-50 text-gray-700 cursor-default">{schemaName || t('jobs.notConfigured')}</span>
          </div>
          <p className="text-xs text-gray-400 col-span-2">{t('jobs.catalogHint')}</p>
        </div>

        {!simplified && <details className="mt-4 group">
          <summary className="section-title cursor-pointer select-none flex items-center gap-1.5 py-2 border-t border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 mt-3 pt-3">
            <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
            {t('jobs.industryDomain')}
            <span className="text-xs font-normal text-slate-400 dark:text-slate-500 ml-1">
              {t('jobs.industryDomain.hint')}
            </span>
          </summary>
          <div className="mt-3 animate-slide-up">
            <div>
              <label className="section-title mb-1.5 flex items-center gap-2">
                {t('jobs.industryOntology')}
                <span className="relative group/tip cursor-help">
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-3.5 w-3.5 text-slate-400 group-hover/tip:text-slate-600 dark:group-hover/tip:text-slate-300 transition-colors" viewBox="0 0 20 20" fill="currentColor">
                    <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd"/>
                  </svg>
                  <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-72 p-2 text-xs text-slate-200 bg-slate-800 rounded-lg shadow-lg opacity-0 group-hover/tip:opacity-100 pointer-events-none transition-opacity z-10">
                    {t('jobs.bundleTip')}
                  </span>
                </span>
                {bundlesLoading && (
                  <span className="text-[10px] text-dbx-oat-medium dark:text-dbx-navy-300 italic animate-pulse">{t('jobs.loading')}</span>
                )}
                {!bundlesLoading && bundles.length === 0 && !bundlesLoadError && (
                  <span className="text-[10px] text-dbx-oat-medium dark:text-dbx-navy-300 italic">{t('jobs.mayTakeSeconds')}</span>
                )}
                {(() => {
                  const sel = bundles.find(b => b.key === ontologyBundle)
                  const isFormal = sel?.bundle_type === 'formal_ontology'
                  const hasTiers = sel?.has_tier_indexes
                  const isV2 = sel?.format_version === '2.0'
                  return (<>
                    {isFormal && (
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-violet-100 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300 font-medium" title={t('jobs.badgeFormalOwlTip')}>
                        {t('jobs.badgeFormalOwl')}
                      </span>
                    )}
                    {hasTiers && !isFormal && (
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300 font-medium" title={t('jobs.badgeFormallyGroundedTip')}>
                        {t('jobs.badgeFormallyGrounded')}
                      </span>
                    )}
                    {isV2 && (
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300 font-medium" title={t('jobs.badgeOwlV2Tip')}>
                        {t('jobs.badgeOwlV2')}
                      </span>
                    )}
                  </>)
                })()}
              </label>
              <select value={ontologyBundle || (domainConfig ? `domain:${domainConfig}` : '')} onChange={e => {
                const raw = e.target.value
                if (raw.startsWith('domain:')) {
                  // Domain-only selection: a standalone domain taxonomy with no ontology.
                  // For customers who want domain classification but not a full ontology.
                  const dk = raw.slice('domain:'.length)
                  setDomainConfig(dk)
                  setOntologyBundle('')
                  try { localStorage.setItem(LS_ONTOLOGY_BUNDLE, '') } catch {}
                  return
                }
                setOntologyBundle(raw)
                try { localStorage.setItem(LS_ONTOLOGY_BUNDLE, raw) } catch {}
                // The domain-taxonomy override only applies to custom-imported
                // ontologies; clear any lingering selection when switching to a
                // curated/formal bundle so we never send a stale invisible override.
                if (!bundles.find(b => b.key === raw)?.custom) setDomainConfig('')
              }} className="select-base">
                <option value="">{t('jobs.bundleNone')}</option>
                {bundles.length > 0 && (() => {
                  const custom = bundles.filter(b => b.custom)
                  const formal = bundles.filter(b => !b.custom && b.bundle_type === 'formal_ontology')
                  const curated = bundles.filter(b => !b.custom && b.bundle_type !== 'formal_ontology')
                  const counts = (b) => {
                    const parts = [t('jobs.entitiesCount', { n: b.entity_count })]
                    if (b.edge_count) parts.push(t('jobs.edgesCount', { n: b.edge_count }))
                    return parts.join(', ')
                  }
                  const suffix = (b) => (b.standards_alignment && b.standards_alignment !== b.name) ? ` -- ${b.standards_alignment}` : ''
                  return (<>
                    {custom.length > 0 && <optgroup label={t('jobs.optgroup.custom')}>
                      {custom.map(b => (
                        <option key={b.key} value={b.key}>
                          {b.name} ({counts(b)})
                        </option>
                      ))}
                    </optgroup>}
                    {formal.length > 0 && <optgroup label={t('jobs.optgroup.formal')}>
                      {formal.map(b => (
                        <option key={b.key} value={b.key}>
                          {b.name} ({counts(b)}){suffix(b)}
                        </option>
                      ))}
                    </optgroup>}
                    {curated.length > 0 && <optgroup label={t('jobs.optgroup.curated')}>
                      {curated.map(b => (
                        <option key={b.key} value={b.key}>
                          {b.has_tier_indexes ? '\u2713 ' : ''}{b.name} ({counts(b)}){suffix(b)}
                        </option>
                      ))}
                    </optgroup>}
                    {domainConfigs.length > 0 && <optgroup label={t('jobs.optgroup.domainTax')}>
                      {domainConfigs.map(d => (
                        <option key={`domain:${d.key}`} value={`domain:${d.key}`}>
                          {d.name} ({d.domain_count} domains)
                        </option>
                      ))}
                    </optgroup>}
                  </>)
                })()}
              </select>
              <button type="button" onClick={() => onNavigate?.('ontologyBuilder')}
                className="text-xs text-dbx-teal hover:underline mt-1 inline-block">
                {t('jobs.buildCustomOntology')}
              </button>
              <div className="mt-0.5">
                <button type="button" onClick={() => setShowDomainGen(s => !s)}
                  className="text-xs text-dbx-teal hover:underline inline-block">
                  Or generate a domain taxonomy from a document &rarr;
                </button>
              </div>
              {showDomainGen && (
                <div className="mt-2 rounded-lg border border-slate-200 dark:border-dbx-navy-400/40 p-3 space-y-2">
                  <p className="text-xs text-slate-500 dark:text-slate-400">
                    Upload a document listing your business <strong>domains and subdomains</strong> with short
                    descriptions (.txt/.md/.pdf/.docx). It&rsquo;s turned into a domain-taxonomy YAML, saved, and
                    selected here for domain classification &mdash; the built-in taxonomies are untouched.
                  </p>
                  <input type="file" accept=".txt,.md,.pdf,.docx"
                    onChange={e => { setDomainGenFile(e.target.files[0] || null); setDomainGenResult(null) }}
                    className="block w-full text-xs text-slate-500 file:mr-3 file:py-1.5 file:px-3 file:rounded file:border-0 file:text-xs file:font-medium file:bg-dbx-teal/10 file:text-dbx-teal hover:file:bg-dbx-teal/20" />
                  <div className="grid grid-cols-2 gap-2">
                    <input value={domainGenName} onChange={e => setDomainGenName(e.target.value)} placeholder="Name (e.g. Insurance)"
                      className="w-full px-2 py-1.5 rounded border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-xs" />
                    <input value={domainGenDesc} onChange={e => setDomainGenDesc(e.target.value)} placeholder="Optional: what it covers"
                      className="w-full px-2 py-1.5 rounded border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-xs" />
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-slate-400">{domainGenFile ? domainGenFile.name : 'No file selected'}</span>
                    <button type="button" onClick={generateDomainConfig} disabled={!domainGenFile || domainGenBusy}
                      className="btn-primary btn-sm disabled:opacity-50">{domainGenBusy ? 'Generating… (can take a minute)' : 'Generate taxonomy'}</button>
                  </div>
                  {domainGenResult?.error && <p className="text-xs text-red-500">{String(domainGenResult.error)}</p>}
                  {domainGenResult && !domainGenResult.error && (
                    <p className="text-xs text-green-600 dark:text-green-400">
                      Created &ldquo;{domainGenResult.name}&rdquo; &mdash; {domainGenResult.domain_count} domains, {domainGenResult.subdomain_count} subdomains. Selected for this run.
                    </p>
                  )}
                </div>
              )}
              {bundlesLoadError && (
                <p className="text-xs text-amber-700 dark:text-amber-300 mt-1.5">
                  {t('jobs.bundlesLoadError', { err: bundlesLoadError })}{' '}
                  <button type="button" onClick={loadBundles} className="underline font-medium">{t('jobs.retry')}</button>
                </p>
              )}
              {(() => {
                const sel = bundles.find(b => b.key === ontologyBundle)
                if (!sel?.description) return null
                return (
                  <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">
                    {sel.description}
                    {sel.source_url && <> &mdash; <a href={sel.source_url} target="_blank" rel="noopener noreferrer" className="underline hover:text-blue-500">{t('jobs.viewSource')}</a></>}
                  </p>
                )
              })()}
              <details className="mt-2 rounded-lg border border-slate-200/60 dark:border-slate-700/40 bg-slate-50/50 dark:bg-slate-800/30 px-3 py-2 text-xs text-slate-600 dark:text-slate-300">
                <summary className="cursor-pointer font-medium">{t('jobs.howToChoose')}</summary>
                <p className="mt-1.5 leading-relaxed">{t('jobs.howToChooseIntro')}</p>
                <ul className="mt-1.5 ml-3 list-disc space-y-1 leading-relaxed">
                  <li>{t('jobs.bundleGeneral')}</li>
                  <li>{t('jobs.bundleHealthcare')}</li>
                  <li>{t('jobs.bundleFinancial')}</li>
                  <li>{t('jobs.bundleRetail')}</li>
                  <li>{t('jobs.bundleSchemaOrg')}</li>
                  <li>{t('jobs.bundleDublinCore')}</li>
                </ul>
                <p className="mt-1.5 leading-relaxed">{t('jobs.howToChooseFormal')}</p>
                <p className="mt-1 leading-relaxed">{t('jobs.howToChooseUnsure')}</p>
              </details>
              {(() => {
                const sel = bundles.find(b => b.key === ontologyBundle)
                if (!sel?.tier_indexes_stale) return null
                return (
                  <details className="mt-2 rounded-lg border border-amber-200/60 dark:border-amber-700/30 bg-amber-50/50 dark:bg-amber-900/10 px-3 py-2 text-xs text-amber-800 dark:text-amber-300">
                    <summary className="cursor-pointer font-medium">{t('jobs.indexRebuild')}</summary>
                    <p className="mt-1 text-amber-700 dark:text-amber-300/80">{t('jobs.indexRebuildBody')}</p>
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
                            setError(t('jobs.indexRebuildFailed', { err: data.error || t('jobs.unknownError') }))
                          }
                        } catch (err) { setError(t('jobs.indexRebuildError', { err: err.message })) }
                      }}
                    >{t('jobs.rebuildIndexes')}</button>
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
                      setImportStatus(t('jobs.importSuccess', { name, entities: data.entity_count, edges: data.edge_count }))
                      setTimeout(() => setImportStatus(null), 10000)
                    } else {
                      setError(t('jobs.importFailed', { err: data.error || t('jobs.unknownError') }))
                    }
                  } catch (err) { setError(t('jobs.importError', { err: err.message })) }
                  e.target.value = ''
                }} />
                {t('jobs.importOntology')}
              </label>
              <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">{t('jobs.importHint')}</p>
              {importStatus && <p className="text-xs text-green-600 dark:text-green-400 mt-1">{importStatus}</p>}

              {/* Domains derive from the selected bundle automatically. The
                  explicit Domain Taxonomy control only surfaces for custom-imported
                  ontologies, which carry entity types but no domain definitions. */}
              {ontologyBundle && !bundles.find(b => b.key === ontologyBundle)?.custom && (
                <p className="text-xs text-slate-500 dark:text-slate-400 mt-2 flex items-center gap-1.5">
                  {t('jobs.domainsFromBundle')}
                  <InfoTip text={t('jobs.customOntologyTaxTip')} />
                </p>
              )}
              {ontologyBundle && bundles.find(b => b.key === ontologyBundle)?.custom && (
                <div className="mt-3 rounded-lg border border-amber-200/60 dark:border-amber-700/30 bg-amber-50/50 dark:bg-amber-900/10 px-3 py-2.5">
                  <label className="section-title mb-1.5 flex items-center gap-2">
                    {t('jobs.domainTaxonomy')}
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300 font-medium">{t('jobs.requiredForCustom')}</span>
                  </label>
                  <select value={domainConfig} onChange={e => setDomainConfig(e.target.value)} className="select-base">
                    <option value="">{t('jobs.selectDomainTax')}</option>
                    {domainConfigs.map(d => (
                      <option key={d.key} value={d.key}>{d.name} ({d.domain_count} {t('jobs.domainsCount')})</option>
                    ))}
                  </select>
                  <p className="text-xs text-amber-700 dark:text-amber-300/90 mt-1">{t('jobs.noDomainsWarn')}</p>
                </div>
              )}
            </div>
          </div>
        </details>}
      </div>

      {/* Step 1: Generate Core Metadata — always visible, the primary action */}
      <div className="flex items-center gap-2.5 pt-1">
        <span className="flex items-center justify-center w-6 h-6 rounded-full bg-dbx-lava text-white text-xs font-bold shrink-0">1</span>
        <div>
          <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100">{t('jobs.coreTitle')}</h2>
          <p className="text-xs text-slate-500 dark:text-slate-400">{t('jobs.coreSubtitle')}</p>
        </div>
      </div>

      <TabErrorBoundary key="core" t={t}>
      {(
        <section className="card border-l-4 border-l-dbx-lava">
          <div className="p-6 space-y-4">
            <p className="text-sm text-slate-500 dark:text-slate-400 flex items-center gap-1.5">
              {t('jobs.coreDesc')}
              <InfoTip text={t('jobs.coreDescTip')} />
            </p>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Tip: start with a handful of tables to evaluate cost, performance, and quality, then scale up iteratively.
            </p>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium text-slate-700 dark:text-slate-200 mb-1.5 block">{t('jobs.tablesToProcess')}</label>
                <TableScopePicker value={coreScope} onChange={setCoreScope} allowAll={false} seedTables={[]} />
                <button onClick={() => setShowPatterns(o => !o)} className="btn-ghost btn-sm mt-1.5 !px-0 text-dbx-teal">
                  <svg className={`w-3 h-3 transition-transform ${showPatterns ? 'rotate-90' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                  {t('jobs.pastePatterns')}
                </button>
                {showPatterns && (
                  <div className="mt-1.5">
                    <textarea value={patternText} onChange={e => setPatternText(e.target.value)}
                      placeholder={t('jobs.placeholder.tables')}
                      title={t('jobs.patternsTip')}
                      className="textarea-base h-16 !text-xs" />
                  </div>
                )}
              </div>
              <div className="space-y-3">
                <p className="text-sm font-medium text-slate-700 dark:text-slate-200">{t('jobs.options')}</p>
                <label className="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="checkbox" checked={applyDdl} disabled={federationMode} onChange={e => setApplyDdl(e.target.checked)} />
                  {t('jobs.applyImmediately')}
                  <InfoTip text={t('jobs.applyImmediatelyTip')} />
                </label>
                {applyDdl && certifiedPolicy === 'warn' && (
                  <label className="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300 cursor-pointer ml-6">
                    <input type="checkbox" checked={skipCertified} onChange={e => setSkipCertified(e.target.checked)} />
                    Skip applying to certified assets
                    <InfoTip text="Under the 'warn' policy, applying overwrites the existing comments/tags on UC-certified assets by default (you'll be warned before the run). Check this to leave them untouched: certified tables/columns are skipped while everything else still applies. The asset and its certified status are never changed." />
                  </label>
                )}
                {applyDdl && certifiedPolicy === 'block' && (
                  <p className="text-xs text-emerald-700 dark:text-emerald-300 ml-6">
                    <span className="px-1 py-0 rounded bg-emerald-100 dark:bg-emerald-900/40 font-medium">policy: block</span> Certified assets are protected — their comments/tags are skipped on apply.
                  </p>
                )}
                <label className="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="checkbox" checked={federationMode} onChange={e => {
                    setFederationMode(e.target.checked)
                    if (e.target.checked) setApplyDdl(false)
                  }} />
                  {t('jobs.federationMode')}
                  <InfoTip text={t('jobs.federationModeTip')} />
                </label>
              </div>
            </div>

            {!simplified && <details className="group mt-3">
              <summary className="text-xs font-medium text-slate-500 dark:text-slate-400 cursor-pointer select-none flex items-center gap-1.5 py-2 border-t border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 pt-3">
                <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
                {t('jobs.modelProcessingOptions')}
              </summary>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3 animate-slide-up">
                <div>
                  <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">{t('jobs.model')}</label>
                  <select value={settings.model} onChange={e => setSetting('model', e.target.value)}
                    className="input-base !text-xs">
                    {availableModels.map(m => <option key={m} value={m}>{m}</option>)}
                  </select>
                </div>
                <div>
                  <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title={t('jobs.rowsSampledTip')}>{t('jobs.rowsSampled')}</label>
                  <input type="number" min="0" max="100" value={settings.sample_size}
                    onChange={e => setSetting('sample_size', parseInt(e.target.value) || 0)} className="input-base !text-xs" />
                </div>
                <div>
                  <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title={t('jobs.columnsPerCallTip')}>{t('jobs.columnsPerCall')}</label>
                  <input type="number" min="1" max="100" value={settings.columns_per_call}
                    onChange={e => setSetting('columns_per_call', Math.max(1, parseInt(e.target.value) || 10))} className="input-base !text-xs" />
                </div>
                <div>
                  <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title={t('jobs.commentStyleTip')}>{t('jobs.commentStyle')}</label>
                  <select value={settings.comment_style} onChange={e => setSetting('comment_style', e.target.value)}
                    className="input-base !text-xs">
                    <option value="concise">{t('jobs.commentStyle.concise')}</option>
                    <option value="standard">{t('jobs.commentStyle.standard')}</option>
                    <option value="detailed">{t('jobs.commentStyle.detailed')}</option>
                  </select>
                </div>
                <div className="flex flex-col gap-2 pt-1">
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title={t('jobs.lineageTip')}>
                    <input type="checkbox" checked={settings.include_lineage} onChange={e => setSetting('include_lineage', e.target.checked)} />
                    {t('jobs.opt.lineage')}
                  </label>
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title={t('jobs.enrichPriorTip')}>
                    <input type="checkbox" checked={settings.use_kb_comments} onChange={e => setSetting('use_kb_comments', e.target.checked)} />
                    {t('jobs.enrichPrior')}
                  </label>
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title={t('jobs.customerContextOptTip')}>
                    <input type="checkbox" checked={settings.use_customer_context} onChange={e => setSetting('use_customer_context', e.target.checked)} />
                    {t('jobs.opt.customerContext')}
                  </label>
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Include the table's existing Unity Catalog comment in the prompt. On by default: helps carry forward prior author intent, but a strong existing comment can make the model lean on it over new context/acronyms. Turn off for fresh comments driven only by data + context.">
                    <input type="checkbox" checked={settings.include_existing_table_comment} onChange={e => setSetting('include_existing_table_comment', e.target.checked)} />
                    Include existing table comment
                  </label>
                </div>
                <div className="flex flex-col gap-2 pt-1">
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title={t('jobs.buildKbAfterTip')}>
                    <input type="checkbox" checked={settings.build_kb_after}
                      disabled={settings.use_serverless}
                      onChange={e => setSetting('build_kb_after', e.target.checked)} />
                    {t('jobs.buildKbAfter')}
                  </label>
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title={t('jobs.useServerlessTip')}>
                    <input type="checkbox" checked={settings.use_serverless} onChange={e => setSetting('use_serverless', e.target.checked)} />
                    {t('jobs.useServerless')}
                  </label>
                </div>
              </div>
            </details>}

            {/* Customer context is the one enrichment worth exposing in Simplified view:
                acronyms always apply automatically, but context is opt-in (default off),
                so without this toggle a simplified-view user could never enable the
                context they uploaded. The full toggle still lives in the advanced block. */}
            {simplified && (
              <div className="flex flex-col gap-2 mt-3">
                <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Enrich prompts with customer-provided context from the Customer Context tab. Context is matched hierarchically by catalog, schema, pattern, or table scope.">
                  <input type="checkbox" checked={settings.use_customer_context} onChange={e => setSetting('use_customer_context', e.target.checked)} />
                  Use customer context
                </label>
                <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer" title="Include the table's existing Unity Catalog comment in the prompt. On by default: helps carry forward prior author intent, but a strong existing comment can make the model lean on it over new context/acronyms. Turn off for fresh comments driven only by data + context.">
                  <input type="checkbox" checked={settings.include_existing_table_comment} onChange={e => setSetting('include_existing_table_comment', e.target.checked)} />
                  Include existing table comment
                </label>
              </div>
            )}

            {!simplified && <p className="text-xs text-slate-500 dark:text-slate-400 mt-2 font-medium">
              {t('jobs.ontologyLabel')}: {ontologyBundle ? bundles.find(b => b.key === ontologyBundle)?.name || ontologyBundle : <em>{t('jobs.none')}</em>}
              {' | '}{t('jobs.domainsLabel')}: {domainConfig ? domainConfigs.find(d => d.key === domainConfig)?.name || domainConfig : (ontologyBundle ? t('jobs.fromSelectedOntology') : <em>{t('jobs.none')}</em>)}
            </p>}
            {/* Primary generate: main click runs all 3 modes (KB-enriched by
                default); the caret dropdown picks all-3 or a single mode. */}
            <div className="flex flex-wrap items-center gap-3 mt-2">
              <div className="relative inline-flex" ref={genMenuRef}>
                <button onClick={() => runGenerate('all3')}
                  disabled={!!runningAction || !tableNames.trim()}
                  title={t('jobs.generateAll3Tip')}
                  className={`btn-primary btn-md ${simplified ? '' : 'rounded-r-none'}`}>
                  {(runningAction === 'kb_enriched' || runningAction === 'all3') ? t('jobs.starting') : t('jobs.generateBtn')}
                </button>
                {!simplified && <button onClick={() => setGenMenuOpen(o => !o)}
                  disabled={!!runningAction || !tableNames.trim()}
                  aria-label={t('jobs.chooseGenerate')}
                  className="btn-primary btn-md rounded-l-none border-l border-white/25 px-2">
                  <svg className={`w-4 h-4 transition-transform ${genMenuOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>}
                {!simplified && genMenuOpen && (
                  <div className="absolute top-full left-0 mt-1 w-64 z-20 card p-1.5 shadow-elevated animate-slide-up">
                    <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400 px-2 py-1">{t('jobs.run')}</p>
                    <button onClick={() => runGenerate('all3')} disabled={!!runningAction || !tableNames.trim()}
                      className="w-full text-left px-2 py-1.5 rounded-md text-sm text-slate-700 dark:text-slate-200 hover:bg-dbx-oat/60 dark:hover:bg-dbx-navy-500 disabled:opacity-50">
                      {t('jobs.allThree')} <span className="text-xs text-slate-400">· {t('jobs.allThree.hint')}</span>
                    </button>
                    <div className="border-t border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 my-1" />
                    <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400 px-2 py-1">{t('jobs.justOne')}</p>
                    <button onClick={() => runGenerate('comment')} disabled={!!runningAction || !tableNames.trim()}
                      className="w-full text-left px-2 py-1.5 rounded-md text-sm text-slate-700 dark:text-slate-200 hover:bg-dbx-oat/60 dark:hover:bg-dbx-navy-500 disabled:opacity-50">
                      {t('jobs.mode.comment')}
                    </button>
                    <button onClick={() => runGenerate('pi')} disabled={!!runningAction || !tableNames.trim()}
                      className="w-full text-left px-2 py-1.5 rounded-md text-sm text-slate-700 dark:text-slate-200 hover:bg-dbx-oat/60 dark:hover:bg-dbx-navy-500 disabled:opacity-50">
                      {t('jobs.mode.pi')}
                    </button>
                    <button onClick={() => runGenerate('domain')} disabled={!!runningAction || !tableNames.trim() || !hasDomainSource}
                      title={!hasDomainSource ? t('jobs.domainNeedsSource') : ''}
                      className="w-full text-left px-2 py-1.5 rounded-md text-sm text-slate-700 dark:text-slate-200 hover:bg-dbx-oat/60 dark:hover:bg-dbx-navy-500 disabled:opacity-50">
                      {t('jobs.mode.domain')}{!hasDomainSource ? t('jobs.mode.domainNeeds') : ''}
                    </button>
                  </div>
                )}
              </div>
              <span className="text-xs text-slate-400 dark:text-slate-500">
                {t('jobs.runsOn', {
                  compute: settings.use_serverless ? t('jobs.compute.serverless') : t('jobs.compute.classic'),
                  kb: skipKbEnrich ? t('jobs.kb.none') : t('jobs.kb.enriched'),
                })}
              </span>
            </div>

            {!simplified && <details className="group mt-1">
              <summary className="text-xs text-slate-500 dark:text-slate-400 cursor-pointer select-none inline-flex items-center gap-1.5">
                <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
                {t('jobs.otherRunOptions')}
              </summary>
              <div className="mt-2 text-xs text-slate-500 dark:text-slate-400 space-y-2 animate-slide-up pl-1">
                <label className="flex items-start gap-2 cursor-pointer" title={t('jobs.skipKbEnrich.hint')}>
                  <input type="checkbox" checked={skipKbEnrich} onChange={e => setSkipKbEnrich(e.target.checked)} className="mt-0.5" />
                  <span>{t('jobs.skipKbEnrich')} <span className="text-slate-400">{t('jobs.skipKbEnrich.hint')}</span></span>
                </label>
                <p className="text-slate-400 flex items-center gap-1.5">
                  <span>{t('jobs.defaultKbEnriched')}</span>
                  <InfoTip text={t('jobs.defaultKbEnrichedTip')} />
                </p>
              </div>
            </details>}

            <div className="border-t border-slate-200/80 dark:border-dbx-navy-400/20 pt-4 mt-2">
              <button onClick={() => onNavigate?.('context')} className="btn-ghost btn-sm text-slate-600 dark:text-slate-300">
                <svg className="w-4 h-4 mr-1.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
                </svg>
                {t('jobs.customerContext')}
                <span className="text-xs text-slate-400 dark:text-slate-500 ml-1.5">{t('jobs.customerContext.hint')}</span>
              </button>
            </div>
          </div>
        </section>
      )}
      </TabErrorBoundary>

      {/* Next steps: the advanced pipeline lives in the Semantic Layer and the
          post-generation maintenance actions live in Sync & Ops. Simple links
          to where each action actually runs, instead of full pointer cards.
          All route to advanced-only tabs, so the block is hidden in simplified view. */}
      {onNavigate && !simplified && (
        <div className="pt-1">
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-400 dark:text-slate-500 mb-2">{t('jobs.nextSteps')}</p>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            <button onClick={() => onNavigate('semantic')} className="btn-secondary btn-sm">{t('jobs.next.semantic')}</button>
            <button onClick={() => onNavigate('syncops')} className="btn-secondary btn-sm">{t('jobs.next.sync')}</button>
            <button onClick={() => onNavigate('syncops')} className="btn-secondary btn-sm">{t('jobs.next.lakebase')}</button>
            <button onClick={() => onNavigate('syncops')} className="btn-secondary btn-sm">{t('jobs.next.mcp')}</button>
          </div>
        </div>
      )}

      {/* Active Runs */}
      {activeRuns.length > 0 && (
        <section className="card p-5">
          <div className="flex items-center gap-2.5 mb-4">
            <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100">{t('jobs.activeRuns')}</h2>
            <span className="inline-block w-2 h-2 bg-blue-500 rounded-full animate-pulse" />
            <span className="text-xs text-slate-400">{t('jobs.autoRefresh')}</span>
          </div>
          {activeRuns.map(r => <RunEntry key={r.run_id} run={r} onCancel={cancelRun} />)}
        </section>
      )}

      {/* Run History */}
      <section className="card p-5">
        <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100 mb-4">{t('jobs.runHistory')}</h2>
        {completedRuns.length === 0 ? (
          <EmptyState title={t('jobs.noRuns')} description={t('jobs.noRuns.desc')} />
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
                  className="btn-ghost btn-sm disabled:opacity-30">{t('jobs.previous')}</button>
                <span className="text-sm text-slate-500 dark:text-slate-400">{t('jobs.pageOf', { page: page + 1, total: totalPages })}</span>
                <button onClick={() => setHistoryPage(p => Math.min(totalPages - 1, p + 1))} disabled={page >= totalPages - 1}
                  className="btn-ghost btn-sm disabled:opacity-30">{t('jobs.next')}</button>
              </div>
            )}
            {runHistory.length >= 50 && (
              <p className="text-xs text-slate-400 mt-2 text-center">{t('jobs.showingRecent')}</p>
            )}
          </>
        })()}
      </section>

      {jobs.length === 0 && error?.includes('Failed to load jobs') && (
        <p className="text-xs text-slate-400 dark:text-slate-500 py-2">{t('jobs.loadJobsError')}</p>
      )}
    </div>
  )
}
