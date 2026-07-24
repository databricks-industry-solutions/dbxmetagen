import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import {
  ReactFlow, Background, Controls, MiniMap,
  useNodesState, useEdgesState, addEdge, Handle, Position,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import dagre from 'dagre'
import { InfoTip } from './ui'

/**
 * Visual ERD designer for the semantic layer's Model step.
 *
 * Loads a recommended star-schema ERD (fact/dimension roles + FK joins) from
 * /api/semantic-layer/erd-recommendation -- built from the metadata dbxmetagen
 * already produced -- lays it out with dagre, and lets the user adjust it:
 * drag tables, flip a table's role (fact <-> dimension), and add/remove joins by
 * dragging between node handles. Node designations + layout persist to the
 * project (PATCH .../erd); join edits persist through the FK endpoints so they
 * feed the analytics pipeline and seed metric-view generation.
 *
 * Props:
 *   tables        - FQ table names in scope (comma-join or array)
 *   projectId     - project to load/save the ERD for
 *   profileId     - optional, scopes KPI coverage in the recommendation
 *   businessContext - optional, passed to the LLM "explain" enrichment
 *   onSaved()     - called after a successful save (e.g. refresh coverage)
 */

const ROLE_STYLES = {
  fact:      { bg: '#FF3621', label: 'Fact' },
  source:    { bg: '#b45309', label: 'Source' },
  dimension: { bg: '#2563eb', label: 'Dimension' },
  bridge:    { bg: '#7c3aed', label: 'Bridge' },
}

function _short(t) { return (t || '').split('.').pop() }

// --- Custom table node --------------------------------------------------------
function TableNode({ data }) {
  const style = ROLE_STYLES[data.role] || ROLE_STYLES.source
  return (
    <div className="rounded-md border shadow-sm bg-white dark:bg-dbx-navy-500 dark:border-slate-600 min-w-[150px] max-w-[220px]">
      <Handle type="target" position={Position.Left} className="!bg-slate-400" />
      <div className="px-2 py-1 rounded-t-md text-white text-xs font-semibold flex items-center justify-between gap-1"
        style={{ background: style.bg }}>
        <span className="truncate" title={data.table}>{_short(data.table)}</span>
        <span className="text-[9px] uppercase tracking-wide opacity-90">{style.label}</span>
      </div>
      <div className="px-2 py-1 text-[10px] text-slate-500 dark:text-slate-400 leading-snug">
        {data.grain && <div>key: <span className="font-mono">{data.grain}</span></div>}
        {data.measurable_columns?.length > 0 && (
          <div className="truncate" title={data.measurable_columns.join(', ')}>
            {data.measurable_columns.length} measure{data.measurable_columns.length === 1 ? '' : 's'}
          </div>
        )}
        {data.reasons?.length > 0 && (
          <div className="truncate italic opacity-80" title={data.reasons.join('; ')}>{data.reasons[0]}</div>
        )}
      </div>
      <Handle type="source" position={Position.Right} className="!bg-slate-400" />
    </div>
  )
}

const nodeTypes = { table: TableNode }

// --- dagre auto-layout --------------------------------------------------------
function layout(nodes, edges) {
  const g = new dagre.graphlib.Graph()
  g.setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: 'LR', nodesep: 40, ranksep: 90 })
  nodes.forEach(n => g.setNode(n.id, { width: 200, height: 70 }))
  edges.forEach(e => g.setEdge(e.source, e.target))
  dagre.layout(g)
  return nodes.map(n => {
    const p = g.node(n.id)
    return { ...n, position: { x: p.x - 100, y: p.y - 35 } }
  })
}

export default function ErdDesigner({ tables, projectId, profileId, businessContext, onSaved }) {
  const tableList = useMemo(
    () => (Array.isArray(tables) ? tables : String(tables || '').split(',').map(t => t.trim()).filter(Boolean)),
    [tables]
  )

  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)
  const [sufficiency, setSufficiency] = useState(null)
  const [schemaType, setSchemaType] = useState('SIMPLE')
  const [selectedNodeId, setSelectedNodeId] = useState(null)
  const [explain, setExplain] = useState(null)
  const [explaining, setExplaining] = useState(false)
  // Role per table is the mutable state we persist (separate from React Flow's
  // node objects so a re-layout never loses designations).
  const roleRef = useRef({})

  const buildGraph = useCallback((rec) => {
    const roles = {}
    const flowNodes = (rec.nodes || []).map(n => {
      roles[n.table] = n.role
      return {
        id: n.table,
        type: 'table',
        position: { x: 0, y: 0 },
        data: { ...n },
      }
    })
    roleRef.current = roles
    const flowEdges = (rec.edges || []).map((e, i) => ({
      id: `${e.src}::${e.dst}::${i}`,
      source: e.src,
      target: e.dst,
      label: e.on,
      animated: e.source === 'predicted',
      style: { stroke: e.source === 'confirmed' ? '#16a34a' : '#94a3b8',
               strokeDasharray: e.source === 'predicted' ? '4 3' : undefined },
      data: { on: e.on, confidence: e.confidence, source: e.source },
    }))
    setNodes(layout(flowNodes, flowEdges))
    setEdges(flowEdges)
    setSufficiency(rec.sufficiency || null)
    setSchemaType(rec.schema_type || 'SIMPLE')
  }, [setNodes, setEdges])

  const load = useCallback(() => {
    if (!tableList.length) return
    setLoading(true)
    setError(null)
    const params = new URLSearchParams({ tables: tableList.join(',') })
    if (projectId) params.set('project_id', projectId)
    if (profileId) params.set('profile_id', profileId)
    fetch(`/api/semantic-layer/erd-recommendation?${params}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then(rec => buildGraph(rec))
      .catch(e => setError(`Could not load ERD recommendation: ${e.message}`))
      .finally(() => setLoading(false))
  }, [tableList, projectId, profileId, buildGraph])

  useEffect(() => { load() }, [load])

  // Draw a new join edge between two tables.
  const onConnect = useCallback((conn) => {
    setEdges(eds => addEdge({
      ...conn,
      id: `${conn.source}::${conn.target}::manual`,
      label: `${_short(conn.source)}.? = ${_short(conn.target)}.?`,
      style: { stroke: '#16a34a' },
      data: { on: '', confidence: 1.0, source: 'confirmed' },
    }, eds))
  }, [setEdges])

  const flipRole = useCallback((nodeId) => {
    setNodes(nds => nds.map(n => {
      if (n.id !== nodeId) return n
      const next = n.data.role === 'fact' ? 'dimension' : 'fact'
      roleRef.current[n.id] = next
      return { ...n, data: { ...n.data, role: next } }
    }))
  }, [setNodes])

  const removeEdge = useCallback((edgeId) => {
    setEdges(eds => eds.filter(e => e.id !== edgeId))
  }, [setEdges])

  const save = useCallback(async () => {
    if (!projectId) { setError('Select a project to save the model.'); return }
    setSaving(true)
    setError(null)
    try {
      // 1. Persist node roles + layout to the project.
      const erdPayload = {
        schema_type: schemaType,
        nodes: nodes.map(n => ({
          table: n.id,
          role: roleRef.current[n.id] || n.data.role,
          position: n.position,
          grain: n.data.grain || null,
        })),
      }
      const r = await fetch(`/api/semantic-layer/projects/${projectId}/erd`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ erd_json: erdPayload }),
      })
      if (!r.ok) throw new Error(`save ERD failed (HTTP ${r.status})`)

      // 2. Persist user-confirmed joins through the FK endpoint (parses "a.x = b.y").
      const manualJoins = edges.filter(e => e.data?.source === 'confirmed' && e.data?.on)
      for (const e of manualJoins) {
        const m = /(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/.exec(e.data.on || '')
        if (!m) continue
        await fetch('/api/analytics/fk-add', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            src_table: e.source, src_column: m[2],
            dst_table: e.target, dst_column: m[4],
            reasoning: 'Confirmed in ERD designer',
          }),
        }).catch(() => {})
      }
      onSaved?.()
    } catch (e) {
      setError(e.message)
    } finally {
      setSaving(false)
    }
  }, [projectId, schemaType, nodes, edges, onSaved])

  const runExplain = useCallback(() => {
    setExplaining(true)
    fetch('/api/semantic-layer/erd-recommendation/explain', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        erd: { nodes: nodes.map(n => n.data), edges: edges.map(e => e.data),
               sufficiency },
        business_context: businessContext || undefined,
      }),
    })
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then(setExplain)
      .catch(e => setError(`Explain failed: ${e.message}`))
      .finally(() => setExplaining(false))
  }, [nodes, edges, sufficiency, businessContext])

  const selectedNode = nodes.find(n => n.id === selectedNodeId)

  if (!tableList.length) {
    return (
      <p className="text-xs text-amber-600 dark:text-amber-400">
        Select tables in Setup to see a recommended data model.
      </p>
    )
  }

  return (
    <div className="space-y-3">
      {/* Sufficiency banner */}
      {sufficiency && (
        <div className="px-3 py-2 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-xs flex flex-wrap items-center gap-x-3 gap-y-1">
          <span className="font-semibold text-slate-700 dark:text-slate-200">{schemaType} schema</span>
          <span className="text-slate-600 dark:text-slate-300">
            Recommended <strong>{sufficiency.metric_views_recommended}</strong> metric view{sufficiency.metric_views_recommended === 1 ? '' : 's'}
            {sufficiency.metric_views_current > 0 && <> ({sufficiency.metric_views_current} exist)</>}
          </span>
          {sufficiency.reasons?.length > 0 && (
            <InfoTip text={sufficiency.reasons.join(' · ')} />
          )}
          {(sufficiency.missing_kpis?.length > 0) && (
            <span className="text-amber-600 dark:text-amber-400">
              {sufficiency.missing_kpis.length} KPI{sufficiency.missing_kpis.length === 1 ? '' : 's'} uncovered
            </span>
          )}
        </div>
      )}

      {error && (
        <div className="text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-md px-3 py-2">{error}</div>
      )}

      {/* Canvas */}
      <div className="h-[440px] rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-dbx-navy-700 relative">
        {loading && (
          <div className="absolute inset-0 z-10 flex items-center justify-center text-xs text-slate-500 bg-white/60 dark:bg-black/40">
            Building recommended model…
          </div>
        )}
        <ReactFlow
          nodes={nodes} edges={edges}
          onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onNodeClick={(_, n) => setSelectedNodeId(n.id)}
          onEdgeClick={(_, e) => { if (window.confirm(`Remove join ${e.label}?`)) removeEdge(e.id) }}
          nodeTypes={nodeTypes}
          fitView proOptions={{ hideAttribution: true }}
        >
          <Background />
          <Controls />
          <MiniMap pannable zoomable className="!bg-white dark:!bg-dbx-navy-600" />
        </ReactFlow>
      </div>

      {/* Node inspector + actions */}
      <div className="flex flex-wrap items-center gap-3">
        {selectedNode ? (
          <div className="flex items-center gap-2 text-xs">
            <span className="text-slate-500 dark:text-slate-400">{_short(selectedNode.id)}:</span>
            <span className="font-medium" style={{ color: (ROLE_STYLES[roleRef.current[selectedNode.id] || selectedNode.data.role] || {}).bg }}>
              {(ROLE_STYLES[roleRef.current[selectedNode.id] || selectedNode.data.role] || {}).label}
            </span>
            <button onClick={() => flipRole(selectedNode.id)} className="px-2 py-0.5 rounded border border-slate-300 dark:border-slate-600 hover:bg-slate-100 dark:hover:bg-dbx-navy-500">
              Make {(roleRef.current[selectedNode.id] || selectedNode.data.role) === 'fact' ? 'dimension' : 'fact'}
            </button>
          </div>
        ) : (
          <span className="text-[11px] text-slate-400">Click a table to change its role · drag between handles to add a join · click a join to remove it</span>
        )}
        <div className="flex-1" />
        <button onClick={runExplain} disabled={explaining || !nodes.length}
          className="px-3 py-1.5 text-xs rounded border border-slate-300 dark:border-slate-600 hover:bg-slate-100 dark:hover:bg-dbx-navy-500 disabled:opacity-50">
          {explaining ? 'Explaining…' : 'Explain (AI)'}
        </button>
        <button onClick={save} disabled={saving || !projectId} title={!projectId ? 'Select a project to save' : ''}
          className="btn-primary btn-md">
          {saving ? 'Saving…' : 'Save model'}
        </button>
      </div>

      {explain && (
        <div className="px-3 py-2 rounded-md bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700/50 text-xs text-amber-800 dark:text-amber-300 leading-relaxed space-y-1">
          <div>{explain.explanation}</div>
          {explain.suggested_view_themes?.length > 0 && (
            <div><span className="font-semibold">Suggested views:</span> {explain.suggested_view_themes.join(' · ')}</div>
          )}
        </div>
      )}
    </div>
  )
}
