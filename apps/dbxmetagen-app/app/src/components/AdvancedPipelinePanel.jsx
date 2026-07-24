import React, { useState, useEffect, useRef } from 'react'
import { InfoTip } from './ui'
import { cachedFetchObj, TTL } from '../apiCache'
import { TERMINAL_STATES } from '../hooks/useJobRunner'

const BUNDLE_LS_KEY = 'dbxmetagen_ontologyBundle'

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
  pipelineStats,
  useServerless = true,
  onServerlessChange,
  onCompleted,
  variant = 'gate',
  onNavigate,
}) {
  // Ontology bundle — carried over from the choice made in Generate Metadata
  // (localStorage), plus an inline selector so the user never has to leave.
  const [ontologyBundle, setOntologyBundle] = useState(() => {
    try { return localStorage.getItem(BUNDLE_LS_KEY) || '' } catch { return '' }
  })
  const [bundles, setBundles] = useState([])
  const [bundlesLoading, setBundlesLoading] = useState(false)

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
      setRunFailure(run.state_message || `Pipeline run ${run.result || run.state}`)
    }
  }, [runHistory, onCompleted])

  // "Running" reflects our launch (optimistic runningAction) or our tracked run
  // still being non-terminal — never an unrelated pipeline run.
  const isRunning = runningAction === 'pipeline' ||
    (ourRunState != null && !TERMINAL_STATES.has(ourRunState.state))

  const scopeReady = !!(catalogName && catalogName.trim() && schemaName && schemaName.trim())

  const launch = async () => {
    firedRef.current = false
    setRunFailure(null)
    setOurRunState(null)
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
      ...(tableNames && tableNames.trim() ? { table_names: tableNames } : {}),
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

  const tableCount = tableNames && tableNames.trim()
    ? tableNames.split(',').filter(t => t.trim()).length : 0

  return (
    <div className="space-y-3">
      {/* Core-metadata presence hint */}
      {pipelineStats && (pipelineStats.profiled || 0) > 0 ? (
        <p className="text-xs text-slate-500 dark:text-slate-400">
          <strong>{pipelineStats.profiled}</strong>{pipelineStats.total_tables ? ` of ${pipelineStats.total_tables}` : ''} tables have core metadata — the analytics pipeline will build on these results.
        </p>
      ) : (
        <p className="text-xs text-amber-600 dark:text-amber-400">
          Generate core metadata first — the analytics pipeline reads the knowledge-base tables it produces.
        </p>
      )}

      {/* Ontology bundle selector (required) */}
      <div>
        <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Ontology bundle <span className="text-slate-400">(required)</span></label>
        <select value={ontologyBundle} onChange={e => chooseBundle(e.target.value)} className="input-base !text-xs" disabled={bundlesLoading}>
          <option value="">{bundlesLoading ? 'Loading bundles…' : 'Select an ontology bundle…'}</option>
          {bundles.map(b => {
            const name = typeof b === 'string' ? b : (b.name || b.id || '')
            return <option key={name} value={name}>{name}</option>
          })}
        </select>
        {!ontologyBundle && (
          <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">
            Select an ontology bundle to run the pipeline.{onNavigate && <> Need a custom one? <button onClick={() => onNavigate('ontologyBuilder')} className="font-semibold text-dbx-lava hover:underline">Build an ontology &rarr;</button></>}
          </p>
        )}
      </div>

      {/* Table-scope summary */}
      <p className="text-xs text-slate-500 dark:text-slate-400">
        Scope: {catalogName || '—'}.{schemaName || '—'} · {tableCount > 0 ? `${tableCount} selected table${tableCount === 1 ? '' : 's'}` : 'all knowledge-base tables'}
      </p>

      {/* Advanced options — collapsed by default; defaults match the prior form */}
      <details className="group">
        <summary className="text-xs font-medium text-slate-600 dark:text-slate-300 cursor-pointer select-none flex items-center gap-1.5">
          <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
          Advanced options
        </summary>
        <div className="mt-2 grid grid-cols-2 md:grid-cols-4 gap-3">
          <div>
            <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Minimum embedding similarity (0–1) for edges between columns.">Similarity threshold</label>
            <input type="number" step="0.05" min="0" max="1" value={similarityThreshold}
              onChange={e => setSimilarityThreshold(parseFloat(e.target.value) || 0.8)} className="input-base !text-xs" />
          </div>
          <div>
            <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Minimum number of table groups for clustering.">Cluster min groups</label>
            <input type="number" min="1" max="50" value={clusterMinK}
              onChange={e => setClusterMinK(parseInt(e.target.value) || 2)} className="input-base !text-xs" />
          </div>
          <div>
            <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Maximum number of table groups for clustering.">Cluster max groups</label>
            <input type="number" min="2" max="100" value={clusterMaxK}
              onChange={e => setClusterMaxK(parseInt(e.target.value) || 15)} className="input-base !text-xs" />
          </div>
          <div>
            <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block" title="Unity Catalog tag key for entity-type classifications.">Entity type tag key</label>
            <input value={entityTagKey} onChange={e => setEntityTagKey(e.target.value)} placeholder="entity_type" className="input-base !text-xs" />
          </div>
          <div className="pb-1 flex items-center gap-1.5">
            <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
              <input type="checkbox" checked={incremental} onChange={e => setIncremental(e.target.checked)} />
              Incremental mode
            </label>
            <InfoTip text="When checked, each analytics task early-exits if its inputs haven't changed since the last run (fast, cheap). Uncheck for a full run — needed after code/ontology changes, and whenever you want the sweep to take effect." />
          </div>
          <div className="pb-1 flex items-center gap-1.5">
            <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
              <input type="checkbox" checked={applyDdl} disabled={federationMode} onChange={e => setApplyDdl(e.target.checked)} />
              Apply DDL
            </label>
            <InfoTip text="Apply ontology tags and FK constraints directly to Unity Catalog tables. Disable to review first." />
            {applyDdl && !federationMode && <span className="text-[10px] text-amber-600 dark:text-amber-400 font-medium">writes to tables</span>}
          </div>
          <div className="pb-1 flex items-center gap-1.5">
            <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
              <input type="checkbox" checked={federationMode} onChange={e => { setFederationMode(e.target.checked); if (e.target.checked) setApplyDdl(false) }} />
              Federation mode
            </label>
            <InfoTip text="Enable for external/federated catalogs. Disables DDL apply and skips DESCRIBE EXTENDED." />
          </div>
          <div className="pb-1 flex items-center gap-1.5">
            <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
              <input type="checkbox" checked={sweepStale} onChange={e => setSweepStale(e.target.checked)} />
              Sweep stale artifacts
            </label>
            <InfoTip text={'Clean rebuild of derived data for tables in scope: deletes ontology entities, relationships, graph nodes/edges, and vector-index docs this run no longer reproduces, then regenerates them. Only takes effect on a full run, so leave Incremental unchecked when using it.'} />
          </div>
        </div>
      </details>

      {/* Run controls */}
      <div className="flex items-center gap-3 flex-wrap">
        <button onClick={launch} disabled={!!runningAction || !scopeReady || !ontologyBundle}
          title={!ontologyBundle ? 'Select an ontology bundle to run the pipeline' : (!scopeReady ? 'A catalog and schema are required' : '')}
          className="btn-primary btn-md">
          {isRunning ? 'Running…' : (tableCount > 0 ? `Run pipeline (${tableCount} table${tableCount === 1 ? '' : 's'})` : 'Run analytics pipeline')}
        </button>
        <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
          <input type="checkbox" checked={useServerless} onChange={e => onServerlessChange?.(e.target.checked)} />
          Serverless compute
        </label>
        <InfoTip text="Run the pipeline on serverless compute instead of classic ML clusters. Faster cold-start, no cluster management." />
      </div>
      {(runError || runFailure) && (
        <div className="text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-md px-3 py-2">
          {runError || runFailure}
        </div>
      )}
      <p className="text-[11px] text-slate-400 dark:text-slate-500 italic">
        Runs the full analytics pipeline (knowledge bases, knowledge graph, embeddings, ontology, profiling, FK prediction, clustering, vector index). No row-level data is sent to LLMs — only metadata.
      </p>
    </div>
  )
}
