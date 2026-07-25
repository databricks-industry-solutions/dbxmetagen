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
 * already produced -- lays it out with dagre, and lets the user review + adjust:
 * understand the model (Explain), flip roles, set join columns, then Save. The
 * ERD arrives pre-populated; the job is review, not authoring from blank.
 *
 * Joins render as clean lines (condition shown on hover only). Clicking a join
 * opens a column editor with ranked FK suggestions + full column dropdowns.
 * Node roles + layout persist to the project (PATCH .../erd); confirmed joins
 * persist through the FK endpoints so they seed metric-view generation.
 *
 * Props: tables, projectId, profileId, businessContext, onSaved()
 */

const ROLE_STYLES = {
  fact:      { bg: '#FF3621', label: 'Fact' },
  source:    { bg: '#b45309', label: 'Source' },
  dimension: { bg: '#2563eb', label: 'Dimension' },
  bridge:    { bg: '#7c3aed', label: 'Bridge' },
}

const SCHEMA_NOTES = {
  STAR: 'Star: a central fact joins dimension lookups. One broad view per fact is the default.',
  SNOWFLAKE: 'Snowflake: dimensions chain into further dimensions. Views nest joins along the chain.',
  DATA_MART: 'Data mart: tables are pre-joined/aggregated — usually one view each, few or no joins.',
  SIMPLE: 'Simple: few or no foreign keys found — expect single-table views, no fabricated joins.',
}

function _short(t) { return (t || '').split('.').pop() }

// Parse "src.col = alias.col" -> {src_column, dst_column}
function _parseOn(on) {
  const m = /(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/.exec(on || '')
  return m ? { src_column: m[2], dst_column: m[4] } : { src_column: '', dst_column: '' }
}

// --- Custom table node --------------------------------------------------------
function TableNode({ data }) {
  const style = ROLE_STYLES[data.role] || ROLE_STYLES.source
  return (
    <div className={`rounded-md border shadow-sm bg-white dark:bg-dbx-navy-500 min-w-[150px] max-w-[220px] ${data._selected ? 'ring-2 ring-dbx-lava border-dbx-lava' : 'dark:border-slate-600'}`}>
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

function _edgeStyle(source) {
  return {
    stroke: source === 'confirmed' ? '#16a34a' : '#94a3b8',
    strokeWidth: 1.5,
    strokeDasharray: source === 'predicted' ? '4 3' : undefined,
  }
}

// Simple client cache for the (costly) AI explanation, keyed on project + model hash.
const _explainCache = new Map()
function _erdHash(nodes, edges) {
  const n = nodes.map(x => `${x.id}:${x.data.role}`).sort().join(',')
  const e = edges.map(x => `${x.source}>${x.target}:${x.data?.on || ''}`).sort().join(',')
  return `${n}|${e}`
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
  const [selectedEdgeId, setSelectedEdgeId] = useState(null)
  const [hoverEdge, setHoverEdge] = useState(null)   // {id, label, x, y}
  const [explain, setExplain] = useState(null)
  const [explaining, setExplaining] = useState(false)
  const [colsByTable, setColsByTable] = useState({})  // {table: [{column_name,...}]}
  const [fkCands, setFkCands] = useState({})           // {"src||dst": [{src_column,dst_column,confidence}]}
  const roleRef = useRef({})

  const buildGraph = useCallback((rec) => {
    const roles = {}
    const flowNodes = (rec.nodes || []).map(n => {
      roles[n.table] = n.role
      return { id: n.table, type: 'table', position: { x: 0, y: 0 }, data: { ...n } }
    })
    roleRef.current = roles
    const flowEdges = (rec.edges || []).map((e, i) => {
      const cols = _parseOn(e.on)
      return {
        id: `${e.src}::${e.dst}::${i}`,
        source: e.src, target: e.dst,
        style: _edgeStyle(e.source),
        data: { on: e.on, confidence: e.confidence, source: e.source, ...cols },
      }
    })
    setNodes(layout(flowNodes, flowEdges))
    setEdges(flowEdges)
    setSufficiency(rec.sufficiency || null)
    setSchemaType(rec.schema_type || 'SIMPLE')
  }, [setNodes, setEdges])

  const load = useCallback(() => {
    if (!tableList.length) return
    setLoading(true); setError(null); setExplain(null)
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

  // Lazily fetch a table's columns (for the join editor dropdowns).
  const ensureColumns = useCallback((table) => {
    if (!table || colsByTable[table]) return
    fetch(`/api/genie/table-columns?table_identifier=${encodeURIComponent(table)}`)
      .then(r => r.ok ? r.json() : { columns: [] })
      .then(d => setColsByTable(prev => ({ ...prev, [table]: d.columns || [] })))
      .catch(() => setColsByTable(prev => ({ ...prev, [table]: [] })))
  }, [colsByTable])

  // Lazily fetch ranked FK candidates for a table pair.
  const ensureCandidates = useCallback((src, dst) => {
    const key = `${src}||${dst}`
    if (fkCands[key]) return
    const p = new URLSearchParams({ src_table: src, dst_table: dst, limit: '8' })
    fetch(`/api/analytics/fk-candidates?${p}`)
      .then(r => r.ok ? r.json() : { candidates: [] })
      .then(d => setFkCands(prev => ({ ...prev, [key]: d.candidates || [] })))
      .catch(() => setFkCands(prev => ({ ...prev, [key]: [] })))
  }, [fkCands])

  const onConnect = useCallback((conn) => {
    const id = `${conn.source}::${conn.target}::manual::${Math.round(nodes.length + edges.length)}`
    setEdges(eds => addEdge({
      ...conn, id,
      style: _edgeStyle('confirmed'),
      data: { on: '', confidence: 1.0, source: 'confirmed', src_column: '', dst_column: '' },
    }, eds))
    setSelectedEdgeId(id)   // open the join editor immediately
    ensureColumns(conn.source); ensureColumns(conn.target)
    ensureCandidates(conn.source, conn.target)
  }, [setEdges, nodes.length, edges.length, ensureColumns, ensureCandidates])

  const setRole = useCallback((nodeId, role) => {
    roleRef.current[nodeId] = role
    setNodes(nds => nds.map(n => n.id === nodeId ? { ...n, data: { ...n.data, role } } : n))
  }, [setNodes])

  const removeEdge = useCallback((edgeId) => {
    setEdges(eds => eds.filter(e => e.id !== edgeId))
    setSelectedEdgeId(null)
  }, [setEdges])

  // Set a join's columns (from a suggestion click or a dropdown change).
  const setJoinColumns = useCallback((edgeId, srcCol, dstCol) => {
    setEdges(eds => eds.map(e => {
      if (e.id !== edgeId) return e
      const on = (srcCol && dstCol) ? `source.${srcCol} = ${_short(e.target)}.${dstCol}` : ''
      return { ...e, data: { ...e.data, on, src_column: srcCol, dst_column: dstCol, source: 'confirmed' },
               style: _edgeStyle('confirmed') }
    }))
  }, [setEdges])

  // Reflect selection ring into node data.
  useEffect(() => {
    setNodes(nds => nds.map(n => n.data._selected === (n.id === selectedNodeId)
      ? n : { ...n, data: { ...n.data, _selected: n.id === selectedNodeId } }))
  }, [selectedNodeId, setNodes])

  const save = useCallback(async () => {
    if (!projectId) { setError('Select a project to save the model.'); return }
    setSaving(true); setError(null)
    try {
      const erdPayload = {
        schema_type: schemaType,
        nodes: nodes.map(n => ({
          table: n.id, role: roleRef.current[n.id] || n.data.role,
          position: n.position, grain: n.data.grain || null,
        })),
      }
      const r = await fetch(`/api/semantic-layer/projects/${projectId}/erd`, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ erd_json: erdPayload }),
      })
      if (!r.ok) throw new Error(`save ERD failed (HTTP ${r.status})`)

      const confirmedJoins = edges.filter(e => e.data?.source === 'confirmed' && e.data?.src_column && e.data?.dst_column)
      for (const e of confirmedJoins) {
        await fetch('/api/analytics/fk-add', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            src_table: e.source, src_column: e.data.src_column,
            dst_table: e.target, dst_column: e.data.dst_column,
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

  const runExplain = useCallback((force = false) => {
    const key = `${projectId || 'noproj'}::${_erdHash(nodes, edges)}`
    if (!force && _explainCache.has(key)) { setExplain(_explainCache.get(key)); return }
    setExplaining(true)
    fetch('/api/semantic-layer/erd-recommendation/explain', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        erd: { nodes: nodes.map(n => n.data), edges: edges.map(e => e.data), sufficiency },
        business_context: businessContext || undefined,
      }),
    })
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then(res => { _explainCache.set(key, res); setExplain(res) })
      .catch(e => setError(`Explain failed: ${e.message}`))
      .finally(() => setExplaining(false))
  }, [projectId, nodes, edges, sufficiency, businessContext])

  const selectedNode = nodes.find(n => n.id === selectedNodeId)
  const selectedEdge = edges.find(e => e.id === selectedEdgeId)

  if (!tableList.length) {
    return (
      <p className="text-xs text-amber-600 dark:text-amber-400">
        Select tables in Setup to see a recommended data model.
      </p>
    )
  }

  const roleOf = (n) => roleRef.current[n.id] || n.data.role

  return (
    <div className="space-y-3">
      {/* Guided stepper */}
      <div className="flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400 flex-wrap">
        <span className="font-semibold text-slate-600 dark:text-slate-300">Steps:</span>
        <span><strong className="text-dbx-lava">1.</strong> Understand (Explain)</span><span className="text-slate-300">→</span>
        <span><strong className="text-dbx-lava">2.</strong> Review &amp; adjust facts + joins</span><span className="text-slate-300">→</span>
        <span><strong className="text-dbx-lava">3.</strong> Save — seeds metric-view generation</span>
        <span className="text-slate-400 italic ml-1">This model is pre-filled from your metadata; you just confirm and tweak it.</span>
      </div>

      {/* Schema type + sufficiency + Explain (step 1) */}
      {sufficiency && (
        <div className="px-3 py-2 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-xs space-y-1.5">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <span className="font-semibold text-slate-700 dark:text-slate-200">{schemaType} schema</span>
            <span className="text-slate-600 dark:text-slate-300">
              Recommended <strong>{sufficiency.metric_views_recommended}</strong> metric view{sufficiency.metric_views_recommended === 1 ? '' : 's'}
              {sufficiency.metric_views_current > 0 && <> ({sufficiency.metric_views_current} exist)</>}
            </span>
            {sufficiency.reasons?.length > 0 && <InfoTip text={sufficiency.reasons.join(' · ')} />}
            {(sufficiency.missing_kpis?.length > 0) && (
              <span className="text-amber-600 dark:text-amber-400">{sufficiency.missing_kpis.length} KPI{sufficiency.missing_kpis.length === 1 ? '' : 's'} uncovered</span>
            )}
            <div className="flex-1" />
            <button onClick={() => runExplain(false)} disabled={explaining || !nodes.length}
              className="px-2.5 py-1 text-xs rounded border border-slate-300 dark:border-slate-600 hover:bg-white dark:hover:bg-dbx-navy-500 disabled:opacity-50">
              {explaining ? 'Explaining…' : (explain ? 'Re-explain' : 'Explain this model (AI)')}
            </button>
          </div>
          <p className="text-[11px] text-slate-400 dark:text-slate-500">{SCHEMA_NOTES[schemaType]}</p>
        </div>
      )}

      {explain && (
        <div className="px-3 py-2 rounded-md bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700/50 text-xs text-amber-800 dark:text-amber-300 leading-relaxed space-y-1">
          <div>{explain.explanation}</div>
          {explain.suggested_view_themes?.length > 0 && (
            <div><span className="font-semibold">Suggested views:</span> {explain.suggested_view_themes.join(' · ')}</div>
          )}
        </div>
      )}

      {error && (
        <div className="text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-md px-3 py-2">{error}</div>
      )}

      {/* Canvas */}
      <div className="h-[460px] rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-dbx-navy-700 relative">
        {loading && (
          <div className="absolute inset-0 z-10 flex items-center justify-center text-xs text-slate-500 bg-white/60 dark:bg-black/40">
            Building recommended model…
          </div>
        )}
        <ReactFlow
          nodes={nodes} edges={edges}
          onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onNodeClick={(_, n) => { setSelectedNodeId(n.id); setSelectedEdgeId(null) }}
          onEdgeClick={(_, e) => {
            setSelectedEdgeId(e.id); setSelectedNodeId(null)
            ensureColumns(e.source); ensureColumns(e.target); ensureCandidates(e.source, e.target)
          }}
          onEdgeMouseEnter={(evt, e) => setHoverEdge({ id: e.id, label: e.data?.on ? `${e.data.src_column || '?'} = ${e.data.dst_column || '?'}` : 'click to set join columns', x: evt.clientX, y: evt.clientY })}
          onEdgeMouseLeave={() => setHoverEdge(null)}
          onPaneClick={() => { setSelectedNodeId(null); setSelectedEdgeId(null) }}
          nodeTypes={nodeTypes}
          fitView proOptions={{ hideAttribution: true }}
        >
          <Background />
          <Controls />
          <MiniMap pannable zoomable className="!bg-white dark:!bg-dbx-navy-600" />
        </ReactFlow>

        {/* Hover-only join label */}
        {hoverEdge && (
          <div className="pointer-events-none fixed z-50 px-2 py-1 rounded bg-slate-800 text-white text-[10px] font-mono shadow-lg"
            style={{ left: hoverEdge.x + 8, top: hoverEdge.y + 8 }}>
            {hoverEdge.label}
          </div>
        )}
      </div>

      {/* Bottom panel: node inspector OR join editor OR hint */}
      {selectedNode ? (
        <div className="px-3 py-2 rounded-md border border-slate-200 dark:border-slate-700 bg-white dark:bg-dbx-navy-600 text-xs space-y-1.5">
          <div className="flex items-center gap-2">
            <span className="font-semibold text-slate-700 dark:text-slate-200">{_short(selectedNode.id)}</span>
            <span className="font-medium" style={{ color: (ROLE_STYLES[roleOf(selectedNode)] || {}).bg }}>
              {(ROLE_STYLES[roleOf(selectedNode)] || {}).label}
            </span>
            <div className="flex-1" />
            <span className="text-slate-400">Set role:</span>
            {['fact', 'dimension', 'source'].map(r => (
              <button key={r} onClick={() => setRole(selectedNode.id, r)}
                className={`px-2 py-0.5 rounded border ${roleOf(selectedNode) === r ? 'text-white border-transparent' : 'border-slate-300 dark:border-slate-600 hover:bg-slate-100 dark:hover:bg-dbx-navy-500'}`}
                style={roleOf(selectedNode) === r ? { background: ROLE_STYLES[r].bg } : {}}>
                {ROLE_STYLES[r].label}
              </button>
            ))}
          </div>
          {selectedNode.data.reasons?.length > 0 && (
            <p className="text-[11px] text-slate-500 dark:text-slate-400">Why: {selectedNode.data.reasons.join(' · ')}</p>
          )}
          {selectedNode.data.grain && (
            <p className="text-[11px] text-slate-500 dark:text-slate-400">Grain key: <span className="font-mono">{selectedNode.data.grain}</span></p>
          )}
        </div>
      ) : selectedEdge ? (
        <JoinEditor
          edge={selectedEdge}
          srcCols={colsByTable[selectedEdge.source] || null}
          dstCols={colsByTable[selectedEdge.target] || null}
          candidates={fkCands[`${selectedEdge.source}||${selectedEdge.target}`] || null}
          onPick={(sc, dc) => setJoinColumns(selectedEdge.id, sc, dc)}
          onRemove={() => removeEdge(selectedEdge.id)}
          onClose={() => setSelectedEdgeId(null)}
        />
      ) : (
        <p className="text-[11px] text-slate-400">
          Click a <strong>table</strong> to set its role · drag between the dots on two tables to add a join · click a <strong>join line</strong> to choose its columns.
        </p>
      )}

      {/* Commit */}
      <div className="flex items-center gap-3">
        <div className="flex-1" />
        <button onClick={save} disabled={saving || !projectId} title={!projectId ? 'Select a project to save' : ''}
          className="btn-primary btn-md">
          {saving ? 'Saving…' : 'Save model'}
        </button>
      </div>
    </div>
  )
}

// --- Join column editor -------------------------------------------------------
function JoinEditor({ edge, srcCols, dstCols, candidates, onPick, onRemove, onClose }) {
  const src = edge.source, dst = edge.target
  const cur = { src_column: edge.data?.src_column || '', dst_column: edge.data?.dst_column || '' }
  const colName = c => (typeof c === 'string' ? c : c.column_name)

  return (
    <div className="px-3 py-2 rounded-md border border-slate-200 dark:border-slate-700 bg-white dark:bg-dbx-navy-600 text-xs space-y-2">
      <div className="flex items-center gap-2">
        <span className="font-semibold text-slate-700 dark:text-slate-200">Join: {_short(src)} → {_short(dst)}</span>
        <div className="flex-1" />
        <button onClick={onRemove} className="text-red-600 dark:text-red-400 hover:underline">Remove join</button>
        <button onClick={onClose} className="text-slate-400 hover:text-slate-600">✕</button>
      </div>

      {/* Suggestions */}
      {candidates === null ? (
        <p className="text-[11px] text-slate-400">Loading suggestions…</p>
      ) : candidates.length > 0 ? (
        <div className="flex flex-wrap gap-1.5 items-center">
          <span className="text-[11px] text-slate-400">Suggested:</span>
          {candidates.map((c, i) => {
            const active = c.src_column === cur.src_column && c.dst_column === cur.dst_column
            return (
              <button key={i} onClick={() => onPick(c.src_column, c.dst_column)}
                className={`px-2 py-0.5 rounded border font-mono ${active ? 'bg-dbx-lava text-white border-transparent' : 'border-slate-300 dark:border-slate-600 hover:bg-slate-100 dark:hover:bg-dbx-navy-500'}`}
                title={`confidence ${(c.confidence * 100).toFixed(0)}%`}>
                {c.src_column} = {c.dst_column}
                <span className="ml-1 opacity-70">{(c.confidence * 100).toFixed(0)}%</span>
              </button>
            )
          })}
        </div>
      ) : (
        <p className="text-[11px] text-slate-400">No FK suggestions for this pair — pick columns below.</p>
      )}

      {/* Full dropdowns */}
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-slate-500 dark:text-slate-400">source</span>
        <select value={cur.src_column} onChange={e => onPick(e.target.value, cur.dst_column)}
          className="input-base !text-xs !py-1 max-w-[180px]">
          <option value="">{srcCols === null ? 'loading…' : 'select column…'}</option>
          {(srcCols || []).map(c => <option key={colName(c)} value={colName(c)}>{colName(c)}</option>)}
        </select>
        <span className="text-slate-400 font-mono">=</span>
        <select value={cur.dst_column} onChange={e => onPick(cur.src_column, e.target.value)}
          className="input-base !text-xs !py-1 max-w-[180px]">
          <option value="">{dstCols === null ? 'loading…' : 'select column…'}</option>
          {(dstCols || []).map(c => <option key={colName(c)} value={colName(c)}>{colName(c)}</option>)}
        </select>
      </div>
      {(!cur.src_column || !cur.dst_column) && (
        <p className="text-[11px] text-amber-600 dark:text-amber-400">Pick both columns to confirm this join (unconfirmed joins are not saved).</p>
      )}
    </div>
  )
}
