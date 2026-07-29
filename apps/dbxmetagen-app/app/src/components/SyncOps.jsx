import React, { useState, useEffect } from 'react'
import { ErrorBanner } from '../App'
import { cachedFetchObj, TTL } from '../apiCache'
import { PageHeader, InfoTip } from './ui'
import { useSharedJobRunner } from '../hooks/useJobRunner'
import TableScopePicker, { scopeToTableNames } from './TableScopePicker'

/**
 * Sync & Ops surface: post-generation maintenance actions that used to clutter
 * the Generate Metadata screen. Each launches an existing job via the shared
 * job runner; nothing here is part of the "generate core metadata" flow.
 *   - Post-Review Sync: rebuild the knowledge graph / vector index after edits.
 *   - Lakebase sync: push the graph to Lakebase (Postgres) for the agents.
 *   - MCP setup: expose the metadata as MCP tools.
 */
export default function SyncOps({ onNavigate }) {
  const { runningAction, runError, runJob, waitForRun } = useSharedJobRunner()
  const [catalogName, setCatalogName] = useState('')
  const [schemaName, setSchemaName] = useState('')
  const [lakebaseConfigured, setLakebaseConfigured] = useState(false)
  const [error, setError] = useState(null)

  const [kgScope, setKgScope] = useState({ mode: 'all', tables: [] })
  const [kgiError, setKgiError] = useState(null)
  const [lakebaseCatalog, setLakebaseCatalog] = useState('')
  const [lakebaseError, setLakebaseError] = useState(null)
  const [mcpDropExisting, setMcpDropExisting] = useState(false)
  const [mcpError, setMcpError] = useState(null)

  useEffect(() => {
    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data: cfg, error: cfgErr }) => {
      if (cfgErr) setError(`Config load failed: ${cfgErr}`)
      if (cfg) {
        setCatalogName(cfg.catalog_name || '')
        setSchemaName(cfg.schema_name || '')
        setLakebaseConfigured(!!cfg.lakebase_configured)
      }
    })
  }, [])

  const ready = !!(catalogName.trim() && schemaName.trim())

  // Refresh both the knowledge graph and the vector index. The vector index is
  // built from metadata_documents derived from the graph, so the KG rebuild must
  // COMPLETE before the index rebuild starts — otherwise VI re-indexes the stale
  // pre-refresh graph. runJob only triggers a run (returns immediately, doesn't
  // throw), so wait for the KG run to reach a terminal state before launching VI.
  const refreshKgIndex = async () => {
    setKgiError(null)
    const kgRun = await runJob('build_knowledge_graph', {
      catalog_name: catalogName, schema_name: schemaName,
      ...(scopeToTableNames(kgScope) ? { table_names: scopeToTableNames(kgScope) } : {}),
      sweep_stale_edges: 'true', incremental: 'false',
    }, 'refresh_kgi')
    if (!kgRun) { setKgiError('Could not start the knowledge-graph rebuild.'); return }
    const kgResult = await waitForRun(kgRun.run_id)
    if (kgResult !== 'SUCCESS') {
      setKgiError(`Knowledge-graph rebuild ${kgResult === 'TIMEOUT' ? 'is still running' : `did not succeed (${kgResult})`} — vector index was not refreshed.`)
      return
    }
    const viRun = await runJob('build_vector_index', {
      catalog_name: catalogName, schema_name: schemaName,
      sweep_stale_docs: 'true', incremental: 'false',
    }, 'refresh_kgi')
    if (!viRun) setKgiError('Knowledge graph refreshed, but the vector-index rebuild failed to start.')
  }

  const syncLakebase = async () => {
    setLakebaseError(null)
    // runJob returns null (and sets the shared runError banner) on failure rather
    // than throwing, so check the return value instead of relying on a catch.
    const run = await runJob('sync_graph_lakebase', {
      catalog_name: catalogName, schema_name: schemaName,
      extra_params: { ...(lakebaseCatalog ? { lakebase_catalog: lakebaseCatalog } : {}) },
    }, 'lakebase')
    if (!run) setLakebaseError('Lakebase sync failed to start.')
  }

  const setupMcpServers = async () => {
    setMcpError(null)
    const run = await runJob('setup_mcp_servers', {
      catalog_name: catalogName, schema_name: schemaName,
      extra_params: { drop_existing: String(mcpDropExisting) },
    }, 'mcp_setup')
    if (!run) setMcpError('MCP setup failed to start.')
  }

  const cardCls = 'card p-5 space-y-3'

  return (
    <div className="space-y-5">
      <PageHeader title="Sync & Ops" subtitle="Post-generation maintenance: refresh the graph and index, sync to Lakebase, and set up MCP access"
        badge={catalogName && schemaName ? `${catalogName}.${schemaName}` : undefined} />
      <ErrorBanner error={error} />
      <ErrorBanner error={runError} />

      {!ready && (
        <p className="text-xs text-amber-600 dark:text-amber-400">
          Output catalog/schema not configured yet — set them before deployment. These actions run against <strong>{catalogName || '—'}.{schemaName || '—'}</strong>.
        </p>
      )}

      {/* Post-Review Sync — three actions after reviewing FKs / editing metadata */}
      <section className={cardCls}>
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">Post-Review Sync</h3>
          <InfoTip text="After reviewing foreign keys or editing metadata in Review & Apply, propagate those changes and expose the metadata — without re-running the full pipeline." />
        </div>
        <div>
          <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Tables (knowledge-graph rebuild scope)</label>
          <TableScopePicker value={kgScope} onChange={setKgScope} kbOnly />
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          {/* Generate MCPs */}
          <div>
            <button onClick={setupMcpServers} disabled={!!runningAction || !ready} className="btn-secondary btn-md w-full">
              {runningAction === 'mcp_setup' ? 'Starting…' : 'Generate MCPs'}
            </button>
            <label className="flex items-center gap-1.5 text-[11px] text-slate-500 dark:text-slate-400 cursor-pointer select-none mt-1.5">
              <input type="checkbox" checked={mcpDropExisting} onChange={e => setMcpDropExisting(e.target.checked)} className="rounded" />
              Recreate existing functions
            </label>
            <p className="text-[11px] text-slate-400 mt-1 flex items-center gap-1">
              Expose metadata as MCP tools
              <InfoTip text="Creates UC functions and validates the Vector Search index so the knowledge base, graph, FKs, and ontology entities are exposed as MCP tools for Cursor, Claude Code, AI Playground, and custom agents." />
            </p>
            {mcpError && <div className="text-[11px] text-red-600 dark:text-red-400 mt-1">{mcpError}</div>}
          </div>
          {/* Refresh KG/Index */}
          <div>
            <button onClick={refreshKgIndex} disabled={!!runningAction || !ready} className="btn-secondary btn-md w-full">
              {runningAction === 'refresh_kgi' ? 'Refreshing…' : 'Refresh KG/Index'}
            </button>
            <p className="text-[11px] text-slate-400 mt-1 flex items-center gap-1">
              Rebuild knowledge graph + vector index
              <InfoTip text="Rebuilds graph nodes/edges from the knowledge base (applying approved/rejected FKs and sweeping orphaned edges), then regenerates and re-indexes metadata documents in Vector Search — which powers the agent's semantic search." />
            </p>
            {kgiError && <div className="text-[11px] text-red-600 dark:text-red-400 mt-1">{kgiError}</div>}
          </div>
          {/* Lakebase Sync (Beta) */}
          <div>
            <button onClick={syncLakebase} disabled={!!runningAction || !ready} className="btn-secondary btn-md w-full">
              {runningAction === 'lakebase' ? 'Syncing…' : 'Lakebase Sync'}
            </button>
            <div className="flex items-center gap-1.5 mt-1.5">
              <span className="badge bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300 text-[10px]">Beta</span>
              {lakebaseConfigured
                ? <span className="badge bg-emerald-50 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300 text-[10px]">Configured</span>
                : <span className="badge bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400 text-[10px]" title="Not configured — the graph uses Delta tables instead.">Using Delta Tables</span>}
              <InfoTip text="Syncs the graph (nodes, edges, entities, relationships) to Lakebase (Postgres) for low-latency queries by the exploration agents. Requires a completed analytics pipeline and a configured Lakebase catalog." />
            </div>
            <input value={lakebaseCatalog} onChange={e => setLakebaseCatalog(e.target.value)} placeholder="Target catalog (optional)" className="input-base !text-xs mt-1.5" />
            {lakebaseError && <div className="text-[11px] text-red-600 dark:text-red-400 mt-1">{lakebaseError}</div>}
          </div>
        </div>
      </section>
    </div>
  )
}
