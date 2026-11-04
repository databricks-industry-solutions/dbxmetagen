import React, { useState, useEffect, useRef } from 'react'
import { InfoTip } from './ui'
import { cachedFetchObj, TTL } from '../apiCache'
import { TERMINAL_STATES, classifyRun } from '../hooks/useJobRunner'
import RunEntry from './RunEntry'
import TableScopePicker, { scopeToTableNames } from './TableScopePicker'
import { useLanguage } from '../contexts/LanguageContext'
import { LS_ONTOLOGY_BUNDLE as BUNDLE_LS_KEY } from '../constants/storageKeys'

/**
 * Advanced analytics pipeline controls (ontology, FK prediction, knowledge graph,
 * embeddings, vector index). Extracted from BatchJobs so it can run inside the
 * Semantic Layer's foundation gate — the pipeline is the prerequisite for metric
 * views. Backend is unchanged: this starts the `_full_analytics_pipeline[_serverless]`
 * job via the shared job runner and sends the exact same param payload as before.
 *
 * Props:
 *   catalogName, schemaName   - run scope (required to enable the run button)
 *   tableNames                - optional comma-separated table filter (blank = all in KB)
 *   runJob(suffix, params, actionKey) - from useJobRunner
 *   runningAction             - from useJobRunner (this panel uses actionKey 'pipeline')
 *   runHistory                - from useJobRunner; used to detect run completion
 *   pipelineStats             - foundation stats (for the "core metadata present" hint)
 *   useServerless, onServerlessChange - lifted serverless toggle (shared with core gen)
 *   onCompleted()             - fired once when the started pipeline run reaches a
 *                               terminal SUCCESS state (used to refresh the gate)
 *   variant                   - 'gate' (compact, default) | 'full'
 *   onNavigate(tabId)         - deep-link (e.g. to the ontology builder)
 */
export default function AdvancedPipelinePanel({
  catalogName,
  schemaName,
  tableNames = '',
  runJob,
  runningAction,
  runError,
  runHistory = [],
  cancelRun,
  pipelineStats,
  useServerless = true,
  onServerlessChange,
  onCompleted,
  variant = 'gate',
  onNavigate,
}) {
  // Aliased to `tr` -- line ~56 uses `t` as a `.map(t => ...)` param.
  const { t: tr } = useLanguage()
  // Ontology bundle — carried over from the choice made in Generate Metadata
  // (localStorage), plus an inline selector so the user never has to leave.
  const [ontologyBundle, setOntologyBundle] = useState(() => {
    try { return localStorage.getItem(BUNDLE_LS_KEY) || '' } catch { return '' }
  })
  const [bundles, setBundles] = useState([])
  const [bundlesLoading, setBundlesLoading] = useState(false)

  // Table scope: explicit All / Selected. Seed from the tables carried over from
  // the Semantic Layer selection (if any); the user can change it here for a
  // targeted (e.g. incremental) re-run without touching the metric-view set.
  const seedTables = (tableNames || '').split(',').map(t => t.trim()).filter(Boolean)
  const [scope, setScope] = useState(() =>
    seedTables.length ? { mode: 'selected', tables: seedTables } : { mode: 'selected', tables: [] })
  // Track the incoming Semantic Layer selection until the user edits scope here.
  // Once they touch it (scopeDirty), stop syncing so their choice is preserved.
  const [scopeDirty, setScopeDirty] = useState(false)
  const seedKey = seedTables.join(',')
  useEffect(() => {
    if (scopeDirty) return
    setScope(seedTables.length ? { mode: 'selected', tables: seedTables } : { mode: 'selected', tables: [] })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seedKey, scopeDirty])
  const changeScope = (next) => { setScopeDirty(true); setScope(next) }

  // Pipeline knobs (defaults identical to the previous BatchJobs form, so a bare
  // "Run" click sends the same payload as before).
  const [similarityThreshold, setSimilarityThreshold] = useState(0.8)
  const [clusterMinK, setClusterMinK] = useState(2)
  const [clusterMaxK, setClusterMaxK] = useState(15)
  const [entityTagKey, setEntityTagKey] = useState('entity_type')
  const [incremental, setIncremental] = useState(true)
  const [sweepStale, setSweepStale] = useState(false)
  const [applyDdl, setApplyDdl] = useState(false)
  const [federationMode, setFederationMode] = useState(false)

  // Workspace config drives the model, sample size, and metadata-enrichment
  // flags sent to the pipeline — the same source the old Generate Metadata form
  // used, so relocating the controls doesn't change what the job runs with.
  const [config, setConfig] = useState({
    model: 'databricks-claude-sonnet-4-6',
    sample_size: 5,
    use_kb_comments: false,
    use_customer_context: false,
    include_lineage: true,
  })

  useEffect(() => {
    setBundlesLoading(true)
    fetch('/api/ontology/bundles')
      .then(r => r.ok ? r.json() : [])
      .then(data => setBundles(Array.isArray(data) ? data : []))
      .catch(() => setBundles([]))
      .finally(() => setBundlesLoading(false))
  }, [])

  useEffect(() => {
    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data: cfg }) => {
      if (!cfg) return
      setConfig(prev => ({
        model: cfg.model ?? prev.model,
        sample_size: cfg.sample_size ?? prev.sample_size,
        use_kb_comments: cfg.use_kb_comments ?? prev.use_kb_comments,
        use_customer_context: cfg.use_customer_context ?? prev.use_customer_context,
        include_lineage: cfg.include_lineage ?? prev.include_lineage,
      }))
      // Seed apply_ddl / federation_mode from config too (the old form did this).
      if (typeof cfg.apply_ddl === 'boolean') setApplyDdl(cfg.apply_ddl)
      if (typeof cfg.federation_mode === 'boolean') setFederationMode(cfg.federation_mode)
    })
  }, [])

  const chooseBundle = (b) => {
    setOntologyBundle(b)
    try { if (b) localStorage.setItem(BUNDLE_LS_KEY, b) } catch { /* ignore */ }
  }

  // Track ONLY the run this panel launched (by run_id), so a pre-existing or
  // externally-started pipeline run never makes this panel look busy or fire
  // completion. Fire onCompleted on terminal SUCCESS; surface terminal
  // non-SUCCESS as an error the user can see.
  const startedRunIdRef = useRef(null)
  const firedRef = useRef(false)
  const [ourRunState, setOurRunState] = useState(null)   // { state, result } of our run
  const [runFailure, setRunFailure] = useState(null)

  useEffect(() => {
    const rid = startedRunIdRef.current
    if (!rid) return
    const run = runHistory.find(r => r.run_id === rid)
    if (!run) return
    setOurRunState({ state: run.state, result: run.result })
    if (firedRef.current || !TERMINAL_STATES.has(run.state)) return
    firedRef.current = true
    if (run.result === 'SUCCESS') {
      onCompleted?.()
    } else {
      setRunFailure(run.state_message || tr('adp.pipelineRunFailure', { status: run.result || run.state }))
    }
  }, [runHistory, onCompleted])

  // "Running" reflects our launch (optimistic runningAction) or our tracked run
  // still being non-terminal — never an unrelated pipeline run.
  const isRunning = runningAction === 'pipeline' ||
    (ourRunState != null && !TERMINAL_STATES.has(ourRunState.state))

  // Scope is valid unless the user chose "Selected" but picked nothing.
  const scopeReady = !!(catalogName && catalogName.trim() && schemaName && schemaName.trim())
    && !(scope.mode === 'selected' && scope.tables.length === 0)

  const launch = async () => {
    firedRef.current = false
    setRunFailure(null)
    setOurRunState(null)
    const tableNamesParam = scopeToTableNames(scope)
    const params = {
      catalog_name: catalogName,
      schema_name: schemaName,
      ontology_bundle: ontologyBundle,
      apply_ddl: applyDdl,
      federation_mode: federationMode,
      sweep_stale_docs: sweepStale,
      sweep_stale_edges: sweepStale,
      sweep_stale_entities: sweepStale,
      // Metadata-enrichment flags — sourced from workspace config (same as the
      // old Generate Metadata form) so relocating the controls is behavior-neutral.
      use_kb_comments: config.use_kb_comments,
      use_customer_context: config.use_customer_context,
      include_lineage: config.include_lineage,
      ...(tableNamesParam ? { table_names: tableNamesParam } : {}),
      extra_params: {
        model: config.model,
        sample_size: String(config.sample_size),
        similarity_threshold: String(similarityThreshold),
        incremental: String(incremental),
        cluster_min_k: String(clusterMinK),
        cluster_max_k: String(clusterMaxK),
        ...(entityTagKey !== 'entity_type' ? { entity_tag_key: entityTagKey } : {}),
      },
    }
    const suffix = useServerless ? '_full_analytics_pipeline_serverless' : '_full_analytics_pipeline'
    const newRun = await runJob(suffix, params, 'pipeline')
    if (newRun?.run_id) {
      startedRunIdRef.current = newRun.run_id
      setOurRunState({ state: newRun.state || 'PENDING', result: null })
    }
  }

  const tableCount = scope.mode === 'selected' ? scope.tables.length : 0

  return (
    <div className="space-y-3">
      {/* Core-metadata presence hint */}
      {pipelineStats && (pipelineStats.profiled || 0) > 0 ? (
        <p className="text-xs text-slate-500 dark:text-slate-400">
          {tr('adp.coreMetadataPresent', {
            profiled: pipelineStats.profiled,
            ofTotal: pipelineStats.total_tables ? tr('adp.ofTotal', { total: pipelineStats.total_tables }) : '',
          })}
        </p>
      ) : (
        <p className="text-xs text-amber-600 dark:text-amber-400">
          {tr('adp.coreMetadataMissing')}
        </p>
      )}

      {/* Ontology bundle selector (required) */}
      <div>
        <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">{tr('adp.ontologyBundleLabel')} <span className="text-slate-400">{tr('adp.requiredHint')}</span></label>
        <select value={ontologyBundle} onChange={e => chooseBundle(e.target.value)} className="input-base !text-xs" disabled={bundlesLoading}>
          <option value="">{bundlesLoading ? tr('adp.loadingBundles') : tr('adp.selectBundlePlaceholder')}</option>
          {bundles.map(b => {
            // The value MUST be the bundle KEY (file stem, e.g. "fhir_r4") --
            // that's what resolve_bundle_path() expects. b.name is the friendly
            // display label (e.g. "FHIR R4"); sending it as the value makes the
            // pipeline look for "FHIR R4.yaml" and fail.
            const key = typeof b === 'string' ? b : (b.key || b.name || b.id || '')
            const label = typeof b === 'string' ? b : (b.name || b.key || b.id || '')
            return <option key={key} value={key}>{label}</option>
          })}
        </select>
        {!ontologyBundle && (
          <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">
            {tr('adp.selectBundleToRun')}{onNavigate && <> {tr('adp.needCustomBundle')} <button onClick={() => onNavigate('ontologyBuilder')} className="font-semibold text-dbx-lava hover:underline">{tr('adp.buildOntologyLink')}</button></>}
          </p>
        )}
      </div>

      {/* Table scope — explicit All / Selected (no "blank = all" guessing) */}
      <div>
        <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">{tr('adp.tablesToProcessLabel')}</label>
        {/* Panel owns seeding (effect above); picker seed disabled to avoid double-seed. */}
        <TableScopePicker value={scope} onChange={changeScope} kbOnly seedTables={[]}
          allWarning="On a large knowledge base, running the full pipeline on all tables can be slow and costly. Start with a small selection to evaluate cost, performance, and quality, then scale up." />
      </div>

      {/* Incremental — the headline reason to re-run, kept visible */}
      <div className="flex items-center gap-1.5">
        <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
          <input type="checkbox" checked={incremental} onChange={e => setIncremental(e.target.checked)} />
          {tr('adp.incrementalLabel')}
        </label>
        <InfoTip text={tr('adp.incrementalTip')} />
      </div>

      {/* Advanced options — collapsed by default; defaults match the prior form */}
      <details className="group">
        <summary className="text-xs font-medium text-slate-600 dark:text-slate-300 cursor-pointer select-none flex items-center gap-1.5">
          <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
          {tr('adp.advancedOptions')}
        </summary>
        <div className="mt-2 grid grid-cols-2 md:grid-cols-4 gap-3">
          <div>
            <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 flex items-center gap-1">{tr('adp.similarityThresholdLabel')} <InfoTip text={tr('adp.similarityThresholdTip')} /></label>
            <input type="number" step="0.05" min="0" max="1" value={similarityThreshold}
              onChange={e => setSimilarityThreshold(parseFloat(e.target.value) || 0.8)} className="input-base !text-xs" />
          </div>
          <div>
            <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 flex items-center gap-1">{tr('adp.clusterMinLabel')} <InfoTip text={tr('adp.clusterMinTip')} /></label>
            <input type="number" min="1" max="50" value={clusterMinK}
              onChange={e => setClusterMinK(parseInt(e.target.value) || 2)} className="input-base !text-xs" />
          </div>
          <div>
            <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 flex items-center gap-1">{tr('adp.clusterMaxLabel')} <InfoTip text={tr('adp.clusterMaxTip')} /></label>
            <input type="number" min="2" max="100" value={clusterMaxK}
              onChange={e => setClusterMaxK(parseInt(e.target.value) || 15)} className="input-base !text-xs" />
          </div>
          <div>
            <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 flex items-center gap-1">{tr('adp.entityTagKeyLabel')} <InfoTip text={tr('adp.entityTagKeyTip')} /></label>
            <input value={entityTagKey} onChange={e => setEntityTagKey(e.target.value)} placeholder="entity_type" className="input-base !text-xs" />
          </div>
          <div className="pb-1 flex items-center gap-1.5">
            <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
              <input type="checkbox" checked={applyDdl} disabled={federationMode} onChange={e => setApplyDdl(e.target.checked)} />
              {tr('adp.applyDdlLabel')}
            </label>
            <InfoTip text={tr('adp.applyDdlTip')} />
            {applyDdl && !federationMode && <span className="text-[10px] text-amber-600 dark:text-amber-400 font-medium">{tr('adp.writesToTables')}</span>}
          </div>
          <div className="pb-1 flex items-center gap-1.5">
            <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
              <input type="checkbox" checked={federationMode} onChange={e => { setFederationMode(e.target.checked); if (e.target.checked) setApplyDdl(false) }} />
              {tr('adp.federationModeLabel')}
            </label>
            <InfoTip text={tr('adp.federationModeTip')} />
          </div>
          <div className="pb-1 flex items-center gap-1.5">
            <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
              <input type="checkbox" checked={sweepStale} onChange={e => setSweepStale(e.target.checked)} />
              {tr('adp.sweepStaleLabel')}
            </label>
            <InfoTip text={tr('adp.sweepStaleTip')} />
          </div>
        </div>
      </details>

      {/* Run controls */}
      <div className="flex items-center gap-3 flex-wrap">
        <button onClick={launch} disabled={!!runningAction || !scopeReady || !ontologyBundle}
          title={!ontologyBundle ? tr('adp.runTitleNoBundle') : (!scopeReady ? tr('adp.runTitleNoScope') : '')}
          className="btn-primary btn-md">
          {isRunning ? tr('adp.running') : (tableCount > 0 ? tr('adp.runPipelineWithCount', { count: tableCount, tableWord: tableCount === 1 ? tr('adp.tableSingular') : tr('adp.tablePlural') }) : tr('adp.runPipeline'))}
        </button>
        <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
          <input type="checkbox" checked={useServerless} onChange={e => onServerlessChange?.(e.target.checked)} />
          {tr('adp.serverlessLabel')}
        </label>
        <InfoTip text={tr('adp.serverlessTip')} />
      </div>
      {(runError || runFailure) && (
        <div className="text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-md px-3 py-2">
          {runError || runFailure}
        </div>
      )}
      <p className="text-[11px] text-slate-400 dark:text-slate-500 italic">
        {tr('adp.footerCaption')}
      </p>

      {/* Advanced pipeline runs — collapsible so it doesn't crowd the gate.
          Open by default while a run is active. (#229.2 cancel lives here.) */}
      {(() => {
        const advRuns = runHistory.filter(r => classifyRun(r) === 'advanced')
        if (advRuns.length === 0) return null
        const anyActive = advRuns.some(r => !TERMINAL_STATES.has(r.state))
        return (
          <details open={anyActive} className="rounded-lg border border-slate-200 dark:border-dbx-navy-400/30">
            <summary className="cursor-pointer select-none px-3 py-2 text-xs font-semibold text-slate-600 dark:text-slate-300 flex items-center gap-2">
              Pipeline runs ({advRuns.length})
              {anyActive && <span className="inline-block w-2 h-2 bg-blue-500 rounded-full animate-pulse" title="A run is active" />}
            </summary>
            <div className="border-t border-slate-200 dark:border-dbx-navy-400/30 max-h-80 overflow-y-auto">
              {advRuns.slice(0, 20).map(r => <RunEntry key={r.run_id} run={r} onCancel={cancelRun} />)}
            </div>
          </details>
        )
      })()}
    </div>
  )
}
