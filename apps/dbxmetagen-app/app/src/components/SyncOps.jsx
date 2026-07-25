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
  const { runningAction, runError, runJob } = useSharedJobRunner()
  const [catalogName, setCatalogName] = useState('')
  const [schemaName, setSchemaName] = useState('')
  const [lakebaseConfigured, setLakebaseConfigured] = useState(false)
  const [error, setError] = useState(null)

  const [kgScope, setKgScope] = useState({ mode: 'all', tables: [] })
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

  const syncKnowledgeGraph = () => runJob('build_knowledge_graph', {
    catalog_name: catalogName, schema_name: schemaName,
    ...(scopeToTableNames(kgScope) ? { table_names: scopeToTableNames(kgScope) } : {}),
    sweep_stale_edges: 'true', incremental: 'false',
  }, 'sync_kg')

  const syncVectorIndex = () => runJob('build_vector_index', {
    catalog_name: catalogName, schema_name: schemaName,
    sweep_stale_docs: 'true', incremental: 'false',
  }, 'sync_vi')

  const syncLakebase = async () => {
    setLakebaseError(null)
    try {
      await runJob('sync_graph_lakebase', {
        catalog_name: catalogName, schema_name: schemaName,
        extra_params: { ...(lakebaseCatalog ? { lakebase_catalog: lakebaseCatalog } : {}) },
      }, 'lakebase')
    } catch (e) { setLakebaseError(e.message || 'Lakebase sync failed') }
  }

  const setupMcpServers = async () => {
    setMcpError(null)
    try {
      await runJob('setup_mcp_servers', {
        catalog_name: catalogName, schema_name: schemaName,
        extra_params: { drop_existing: String(mcpDropExisting) },
      }, 'mcp_setup')
    } catch (e) { setMcpError(e.message || 'MCP setup failed') }
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

      {/* Post-Review Sync */}
      <section className={cardCls}>
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">Post-Review Sync</h3>
          <InfoTip text="After reviewing foreign keys or editing metadata in Review & Apply, propagate those changes to the knowledge graph and vector index without re-running the full pipeline." />
        </div>
        <div>
          <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Tables (knowledge-graph rebuild)</label>
          <TableScopePicker value={kgScope} onChange={setKgScope} kbOnly />
        </div>
        <div className="flex flex-wrap gap-3">
          <div className="flex-1 min-w-[200px]">
            <button onClick={syncKnowledgeGraph} disabled={!!runningAction || !ready} className="btn-secondary btn-md w-full">
              {runningAction === 'sync_kg' ? 'Starting…' : 'Sync Knowledge Graph'}
            </button>
            <p className="text-[11px] text-slate-400 mt-1">Rebuilds graph nodes/edges from the knowledge base, applying approved/rejected FKs and sweeping orphaned edges.</p>
          </div>
          <div className="flex-1 min-w-[200px]">
            <button onClick={syncVectorIndex} disabled={!!runningAction || !ready} className="btn-secondary btn-md w-full">
              {runningAction === 'sync_vi' ? 'Starting…' : 'Sync Vector Index'}
            </button>
            <p className="text-[11px] text-slate-400 mt-1">Regenerates metadata documents and re-indexes them in Vector Search — powers the agent's semantic search.</p>
          </div>
        </div>
      </section>

      {/* Lakebase sync */}
      <section className={cardCls}>
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">Sync Knowledge Graph to Lakebase</h3>
          <span className="badge bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300 text-[10px]">Beta</span>
          {lakebaseConfigured
            ? <span className="badge bg-emerald-50 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300 text-[10px]">Configured</span>
            : <span className="badge bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400 text-[10px]" title="Not configured — the graph uses Delta tables instead.">Using Delta Tables</span>}
          <InfoTip text="Syncs the graph (nodes, edges, entities, relationships) to Lakebase (Postgres) for low-latency queries by the exploration agents. Requires a completed analytics pipeline and a configured Lakebase catalog." />
        </div>
        <div className="flex items-end gap-3">
          <div className="flex-1">
            <label className="text-xs text-slate-500 dark:text-slate-400 mb-1 block">Target Lakebase catalog (optional)</label>
            <input value={lakebaseCatalog} onChange={e => setLakebaseCatalog(e.target.value)} placeholder="e.g. lakebase_catalog" className="input-base !text-xs" />
          </div>
          <button onClick={syncLakebase} disabled={!!runningAction || !ready} className="btn-secondary btn-md whitespace-nowrap">
            {runningAction === 'lakebase' ? 'Syncing…' : 'Sync to Lakebase'}
          </button>
        </div>
        {lakebaseError && <div className="text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-lg px-3 py-2">{lakebaseError}</div>}
      </section>

      {/* MCP setup */}
      <section className={cardCls}>
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">Setup MCP Servers</h3>
          <span className="badge bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300 text-[10px]">Beta</span>
          <InfoTip text="Creates UC functions and validates the Vector Search index so the knowledge base, graph, FKs, and ontology entities are exposed as MCP tools for Cursor, Claude Code, AI Playground, and custom agents." />
        </div>
        <div className="flex items-end gap-3">
          <label className="flex items-center gap-1.5 text-xs text-slate-600 dark:text-slate-400 cursor-pointer select-none">
            <input type="checkbox" checked={mcpDropExisting} onChange={e => setMcpDropExisting(e.target.checked)} className="rounded" />
            Recreate existing functions
          </label>
          <button onClick={setupMcpServers} disabled={!!runningAction || !ready} className="btn-secondary btn-md whitespace-nowrap">
            {runningAction === 'mcp_setup' ? 'Starting…' : 'Setup MCP Servers'}
          </button>
        </div>
        {mcpError && <div className="text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-lg px-3 py-2">{mcpError}</div>}
      </section>
    </div>
  )
}
