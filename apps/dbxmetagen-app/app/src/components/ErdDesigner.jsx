import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import {
  ReactFlow, Background, Controls, MiniMap,
  useNodesState, useEdgesState, addEdge, Handle, Position,
  BaseEdge, EdgeLabelRenderer, getBezierPath,
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

// A true foreign-key constraint needs a (near-)unique parent key and (near-)full
// referential integrity. Below these floors we only let the user save a join key,
// not assert an FK — the constraint would likely fail at apply otherwise.
const FK_PK_UNIQUENESS_FLOOR = 0.99
const FK_RI_SCORE_FLOOR = 0.99

function _short(t) { return (t || '').split('.').pop() }

// Rebuild a join edge's `on` clause (and composite metadata) from its primary
// column pair plus any additional composite pairs. Single pair -> simple `on`,
// is_composite=false. Multiple complete pairs -> AND-joined `on` mirrored into
// `join_condition`, is_composite=true. Incomplete extra pairs are ignored.
function _withJoinCondition(data, targetTable) {
  const alias = _short(targetTable)
  const primary = (data.src_column && data.dst_column)
    ? [{ src: data.src_column, dst: data.dst_column }] : []
  const extras = (data.extra_pairs || []).filter(p => p.src && p.dst)
  const pairs = [...primary, ...extras]
  const on = pairs.map(p => `source.${p.src} = ${alias}.${p.dst}`).join(' AND ')
  const is_composite = pairs.length > 1
  return {
    ...data,
    on,
    is_composite,
    join_condition: is_composite ? on : null,
  }
}

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

// Custom edge that separates PARALLEL joins between the same table pair. Two
// fact tables (or a fact and dim) can be joined on more than one column pair;
// the default bezier draws every such edge on the identical path, so they hide
// each other. We fan them out by offsetting the control point by the edge's
// index among its siblings (parallelIndex / parallelCount, injected in
// buildGraph), and always render the join columns as a small label so each
// edge is individually visible and clickable.
function ParallelEdge({ id, sourceX, sourceY, targetX, targetY,
                        sourcePosition, targetPosition, style, markerEnd, data }) {
  const count = data?.parallelCount || 1
  const idx = data?.parallelIndex || 0
  // Symmetric offset: for count=1 -> 0; count=2 -> [-1,+1]*step; etc.
  const step = 26
  const offset = count > 1 ? (idx - (count - 1) / 2) * step : 0
  const [path, labelX, labelY] = getBezierPath({
    sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition,
    // Bow the curve outward proportional to the offset so parallels separate.
    curvature: 0.25 + Math.abs(offset) / 200,
  })
  const label = data?.on
    ? `${data.src_column || '?'} = ${data.dst_column || '?'}`
    : null
  return (
    <>
      <BaseEdge id={id} path={path} style={style} markerEnd={markerEnd} />
      {label && (
        <EdgeLabelRenderer>
          <div
            className="nodrag nopan absolute px-1 py-0.5 rounded bg-white/90 dark:bg-dbx-navy-600
                       text-[9px] font-mono text-slate-600 dark:text-slate-300 border
                       border-slate-200 dark:border-slate-600 pointer-events-none"
            style={{
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY + offset}px)`,
            }}
          >
            {label}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  )
}

const edgeTypes = { parallel: ParallelEdge }

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
  const manualEdgeSeq = useRef(0)   // monotonic counter for manual-edge ids

  const buildGraph = useCallback((rec) => {
    const roles = {}
    const flowNodes = (rec.nodes || []).map(n => {
      roles[n.table] = n.role
      return { id: n.table, type: 'table', position: { x: 0, y: 0 }, data: { ...n } }
    })
    roleRef.current = roles
    // Group edges by unordered table pair so PARALLEL joins (multiple column
    // pairs between the same two tables) can be fanned out and all shown.
    const recEdges = rec.edges || []
    const pairCounts = {}
    recEdges.forEach(e => {
      const pk = [e.src, e.dst].map(s => (s || '').toLowerCase()).sort().join('::')
      pairCounts[pk] = (pairCounts[pk] || 0) + 1
    })
    const pairSeen = {}
    const flowEdges = recEdges.map((e, i) => {
      const cols = _parseOn(e.on)
      const pk = [e.src, e.dst].map(s => (s || '').toLowerCase()).sort().join('::')
      const parallelIndex = pairSeen[pk] || 0
      pairSeen[pk] = parallelIndex + 1
      return {
        id: `${e.src}::${e.dst}::${i}`,
        source: e.src, target: e.dst,
        type: 'parallel',
        style: _edgeStyle(e.source),
        data: {
          on: e.on, confidence: e.confidence, source: e.source, ...cols,
          parallelIndex, parallelCount: pairCounts[pk] || 1,
        },
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
    // Monotonic id so add/remove/re-add between the same pair never collides
    // (a length-based suffix can repeat after a removal).
    const id = `${conn.source}::${conn.target}::manual::${manualEdgeSeq.current++}`
    setEdges(eds => addEdge({
      ...conn, id,
      type: 'parallel',
      style: _edgeStyle('confirmed'),
      data: { on: '', confidence: 1.0, source: 'confirmed', src_column: '', dst_column: '' },
    }, eds))
    setSelectedEdgeId(id)   // open the join editor immediately
    ensureColumns(conn.source); ensureColumns(conn.target)
    ensureCandidates(conn.source, conn.target)
  }, [setEdges, ensureColumns, ensureCandidates])

  const setRole = useCallback((nodeId, role) => {
    roleRef.current[nodeId] = role
    setNodes(nds => nds.map(n => n.id === nodeId ? { ...n, data: { ...n.data, role } } : n))
  }, [setNodes])

  const removeEdge = useCallback((edgeId) => {
    setEdges(eds => eds.filter(e => e.id !== edgeId))
    setSelectedEdgeId(null)
  }, [setEdges])

  // Set a join's columns (from a suggestion click or a dropdown change).
  // Rebuilds the `on` clause from the primary pair plus any additional composite
  // pairs, and records is_composite + join_condition when there is more than one.
  const setJoinColumns = useCallback((edgeId, srcCol, dstCol) => {
    setEdges(eds => eds.map(e => {
      if (e.id !== edgeId) return e
      return { ...e, data: _withJoinCondition({ ...e.data, src_column: srcCol, dst_column: dstCol, source: 'confirmed' }, e.target),
               style: _edgeStyle('confirmed') }
    }))
  }, [setEdges])

  // Composite keys: manage the additional column pairs beyond the primary one.
  const addJoinPair = useCallback((edgeId) => {
    setEdges(eds => eds.map(e => e.id === edgeId
      ? { ...e, data: _withJoinCondition({ ...e.data, extra_pairs: [...(e.data?.extra_pairs || []), { src: '', dst: '' }] }, e.target) }
      : e))
  }, [setEdges])
  const setJoinPair = useCallback((edgeId, idx, src, dst) => {
    setEdges(eds => eds.map(e => {
      if (e.id !== edgeId) return e
      const pairs = [...(e.data?.extra_pairs || [])]
      pairs[idx] = { src, dst }
      return { ...e, data: _withJoinCondition({ ...e.data, extra_pairs: pairs, source: 'confirmed' }, e.target), style: _edgeStyle('confirmed') }
    }))
  }, [setEdges])
  const removeJoinPair = useCallback((edgeId, idx) => {
    setEdges(eds => eds.map(e => {
      if (e.id !== edgeId) return e
      const pairs = (e.data?.extra_pairs || []).filter((_, i) => i !== idx)
      return { ...e, data: _withJoinCondition({ ...e.data, extra_pairs: pairs }, e.target) }
    }))
  }, [setEdges])

  // Mark a join as a plain join key (default) vs a true foreign key. Only
  // 'foreign_key' joins become an ALTER TABLE ADD CONSTRAINT downstream; both
  // kinds feed metric-view / Genie joins identically. Setting the kind on a
  // still-predicted edge also promotes it to 'confirmed' (and restyles it) so
  // save() persists it -- otherwise the assertion would be silently dropped by
  // the confirmedJoins filter.
  const setJoinKind = useCallback((edgeId, kind) => {
    setEdges(eds => eds.map(e => e.id === edgeId
      ? { ...e, data: { ...e.data, kind, source: 'confirmed' }, style: _edgeStyle('confirmed') }
      : e))
  }, [setEdges])

  // Flip a join's direction: swap which table is the FK (child) side vs the
  // referenced (parent) side. Swaps endpoints AND every column pair (primary +
  // composite extras), then recomputes on/is_composite/join_condition via the
  // shared helper so a flipped composite key stays consistent (not stale).
  const flipJoinDirection = useCallback((edgeId) => {
    setEdges(eds => eds.map(e => {
      if (e.id !== edgeId) return e
      const swappedExtras = (e.data?.extra_pairs || []).map(p => ({ src: p.dst, dst: p.src }))
      const newTarget = e.source  // old source becomes the new join target (alias base)
      const data = _withJoinCondition({
        ...e.data,
        src_column: e.data?.dst_column || '',
        dst_column: e.data?.src_column || '',
        extra_pairs: swappedExtras,
      }, newTarget)
      return { ...e, source: e.target, target: e.source, data }
    }))
  }, [setEdges])

  // Reflect selection ring into node data.
  useEffect(() => {
    setNodes(nds => nds.map(n => n.data._selected === (n.id === selectedNodeId)
      ? n : { ...n, data: { ...n.data, _selected: n.id === selectedNodeId } }))
  }, [selectedNodeId, setNodes])

  // Keep parallel-edge fan-out metadata correct as edges are added/removed.
  // Recompute parallelIndex/parallelCount per unordered table pair; only write
  // back when a value actually changed so we don't loop. Depends on the pair
  // signature (not the whole array) to avoid churn on unrelated edge edits.
  const edgePairSig = edges
    .map(e => [e.source, e.target].map(s => (s || '').toLowerCase()).sort().join('::'))
    .sort().join('|')
  useEffect(() => {
    const counts = {}
    edges.forEach(e => {
      const pk = [e.source, e.target].map(s => (s || '').toLowerCase()).sort().join('::')
      counts[pk] = (counts[pk] || 0) + 1
    })
    const seen = {}
    let changed = false
    const next = edges.map(e => {
      const pk = [e.source, e.target].map(s => (s || '').toLowerCase()).sort().join('::')
      const idx = seen[pk] || 0
      seen[pk] = idx + 1
      const cnt = counts[pk] || 1
      if (e.data?.parallelIndex !== idx || e.data?.parallelCount !== cnt || e.type !== 'parallel') {
        changed = true
        return { ...e, type: 'parallel', data: { ...e.data, parallelIndex: idx, parallelCount: cnt } }
      }
      return e
    })
    if (changed) setEdges(next)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [edgePairSig])

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
        // Persist the current edge set so deletions stick. On reload the backend
        // treats a saved edge set as authoritative (a removed edge is simply
        // absent here) instead of re-deriving every recommended join, which is
        // what made deleted edges reappear. Same shape the recommender emits.
        edges: edges.map(e => ({
          src: e.source, dst: e.target,
          on: e.data?.on || '',
          confidence: e.data?.confidence ?? null,
          source: e.data?.source || 'recommended',
        })),
      }
      const r = await fetch(`/api/semantic-layer/projects/${projectId}/erd`, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ erd_json: erdPayload }),
      })
      if (!r.ok) throw new Error(`save ERD failed (HTTP ${r.status})`)

      const confirmedJoins = edges.filter(e => e.data?.source === 'confirmed' && e.data?.src_column && e.data?.dst_column)
      let joinFailures = 0
      for (const e of confirmedJoins) {
        // Default to a join key; only send foreign_key when the user explicitly
        // asserted a true FK. join_key confirmations never become DDL constraints.
        const kind = e.data?.kind === 'foreign_key' ? 'foreign_key' : 'join_key'
        try {
          const jr = await fetch('/api/analytics/fk-add', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              src_table: e.source, src_column: e.data.src_column,
              dst_table: e.target, dst_column: e.data.dst_column,
              kind,
              // Composite (multi-column) joins send the full ON condition; the
              // backend records is_composite and generation consumes it verbatim.
              ...(e.data?.is_composite && e.data?.join_condition
                ? { join_condition: e.data.join_condition } : {}),
              reasoning: kind === 'foreign_key'
                ? 'Confirmed as foreign key in ERD designer'
                : 'Confirmed as join key in ERD designer',
            }),
          })
          if (!jr.ok) joinFailures++
        } catch { joinFailures++ }
      }
      // The ERD itself saved (PATCH above); surface a partial-save warning rather
      // than silently reporting full success when some relationships did not persist.
      if (joinFailures > 0) {
        setError(`Model saved, but ${joinFailures} of ${confirmedJoins.length} join${confirmedJoins.length === 1 ? '' : 's'} could not be persisted — try Save again.`)
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
          edgeTypes={edgeTypes}
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
          onKind={(k) => setJoinKind(selectedEdge.id, k)}
          onFlip={() => flipJoinDirection(selectedEdge.id)}
          onAddPair={() => addJoinPair(selectedEdge.id)}
          onSetPair={(idx, s, d) => setJoinPair(selectedEdge.id, idx, s, d)}
          onRemovePair={(idx) => removeJoinPair(selectedEdge.id, idx)}
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

// A single evidence chip: label + value, dimmed when the signal is absent.
function _pct(v) { return v == null ? '—' : `${Math.round(v * 100)}%` }
function EvidenceChip({ label, value, title, tone }) {
  const toneCls = tone === 'good' ? 'text-emerald-600 dark:text-emerald-400'
    : tone === 'warn' ? 'text-amber-600 dark:text-amber-400'
    : tone === 'bad' ? 'text-red-600 dark:text-red-400'
    : 'text-slate-500 dark:text-slate-400'
  return (
    <span className="inline-flex items-baseline gap-1 px-1.5 py-0.5 rounded bg-slate-100 dark:bg-dbx-navy-500" title={title}>
      <span className="text-[10px] uppercase tracking-wide text-slate-400">{label}</span>
      <span className={`font-mono ${toneCls}`}>{value}</span>
    </span>
  )
}

// --- Join column editor -------------------------------------------------------
function JoinEditor({ edge, srcCols, dstCols, candidates, onPick, onKind, onFlip, onAddPair, onSetPair, onRemovePair, onRemove, onClose }) {
  const src = edge.source, dst = edge.target
  const cur = { src_column: edge.data?.src_column || '', dst_column: edge.data?.dst_column || '' }
  const extraPairs = edge.data?.extra_pairs || []
  const colName = c => (typeof c === 'string' ? c : c.column_name)
  const isFk = edge.data?.kind === 'foreign_key'
  const bothCols = !!(cur.src_column && cur.dst_column)

  // Evidence for the currently-chosen columns (matched against the candidate set
  // the backend returned for this pair). Lets the reviewer see WHY, and gates the
  // "true foreign key" assertion on real referential-integrity signals.
  const chosen = (candidates || []).find(c => c.src_column === cur.src_column && c.dst_column === cur.dst_column)
  const ri = chosen?.ri_score, pk = chosen?.pk_uniqueness, jr = chosen?.join_rate
  const fkEligible = pk != null && ri != null && pk >= FK_PK_UNIQUENESS_FLOOR && ri >= FK_RI_SCORE_FLOOR
  const tone = v => v == null ? undefined : v >= 0.95 ? 'good' : v >= 0.7 ? 'warn' : 'bad'

  return (
    <div className="px-3 py-2 rounded-md border border-slate-200 dark:border-slate-700 bg-white dark:bg-dbx-navy-600 text-xs space-y-2">
      <div className="flex items-center gap-2">
        <span className="font-semibold text-slate-700 dark:text-slate-200">Join: {_short(src)} → {_short(dst)}</span>
        <button onClick={onFlip} title="Swap which table is the foreign-key (child) side vs the referenced (parent) side"
          className="text-slate-400 hover:text-dbx-lava" aria-label="Flip join direction">⇄ flip</button>
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
            const evid = [
              c.ri_score != null && `RI ${_pct(c.ri_score)}`,
              c.join_rate != null && `join ${_pct(c.join_rate)}`,
              c.pk_uniqueness != null && `PK-uniq ${_pct(c.pk_uniqueness)}`,
            ].filter(Boolean).join(' · ')
            return (
              <button key={i} onClick={() => onPick(c.src_column, c.dst_column)}
                className={`px-2 py-0.5 rounded border font-mono ${active ? 'bg-dbx-lava text-white border-transparent' : 'border-slate-300 dark:border-slate-600 hover:bg-slate-100 dark:hover:bg-dbx-navy-500'}`}
                title={[`confidence ${(c.confidence * 100).toFixed(0)}%`, evid, c.reasoning].filter(Boolean).join('\n')}>
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
      {!bothCols && (
        <p className="text-[11px] text-amber-600 dark:text-amber-400">Pick both columns to confirm this join (unconfirmed joins are not saved).</p>
      )}

      {/* Composite (multi-column) key: additional column pairs ANDed onto the
          join. Use when a single column isn't unique enough to join on (e.g.
          (order_id, line_no) or (date, store_id)). */}
      {bothCols && (
        <div className="space-y-1">
          {extraPairs.map((p, i) => (
            <div key={i} className="flex items-center gap-2 flex-wrap">
              <span className="text-slate-400 font-mono">AND</span>
              <select value={p.src} onChange={e => onSetPair(i, e.target.value, p.dst)}
                className="input-base !text-xs !py-1 max-w-[160px]">
                <option value="">column…</option>
                {(srcCols || []).map(c => <option key={colName(c)} value={colName(c)}>{colName(c)}</option>)}
              </select>
              <span className="text-slate-400 font-mono">=</span>
              <select value={p.dst} onChange={e => onSetPair(i, p.src, e.target.value)}
                className="input-base !text-xs !py-1 max-w-[160px]">
                <option value="">column…</option>
                {(dstCols || []).map(c => <option key={colName(c)} value={colName(c)}>{colName(c)}</option>)}
              </select>
              <button onClick={() => onRemovePair(i)} className="text-red-400 hover:text-red-600" aria-label="Remove column pair">✕</button>
            </div>
          ))}
          <button onClick={onAddPair} className="text-[11px] text-dbx-lava hover:underline">
            + Add column pair (composite key)
          </button>
          {edge.data?.is_composite && (
            <p className="text-[10px] text-slate-400 font-mono break-all">on: {edge.data.on}</p>
          )}
        </div>
      )}

      {/* Evidence for the chosen columns: the signals the predictor already
          measured, so the reviewer verifies rather than trusts one number. */}
      {bothCols && chosen && (
        <div className="flex flex-wrap gap-1.5 items-center">
          <span className="text-[10px] uppercase tracking-wide text-slate-400">Evidence:</span>
          <EvidenceChip label="RI" value={_pct(ri)} tone={tone(ri)}
            title="Referential integrity: fraction of child rows whose key exists in the parent (join-and-count probe). Low = orphan rows." />
          <EvidenceChip label="join" value={_pct(jr)} tone={tone(jr)}
            title={`Actual join hit rate on sampled rows${chosen.join_matched != null ? ` (${chosen.join_matched} matched)` : ''}.`} />
          <EvidenceChip label="PK-uniq" value={_pct(pk)} tone={tone(pk)}
            title="Parent-side key uniqueness. A true FK needs a (near-)unique parent key." />
          {chosen.col_similarity != null && (
            <EvidenceChip label="sim" value={_pct(chosen.col_similarity)}
              title="Column-name/embedding similarity between the two columns." />
          )}
          {chosen.stored_reversed && (
            <span className="text-[10px] text-slate-400" title="This pair is stored parent→child in the predictions table; RI / PK-uniqueness describe that stored orientation.">(stored reversed)</span>
          )}
        </div>
      )}
      {bothCols && chosen?.reasoning && (
        <p className="text-[11px] text-slate-500 dark:text-slate-400 italic">{chosen.reasoning}</p>
      )}

      {/* Relationship kind. Confirming a join saves it as a join key (used for
          metric-view / Genie joins). Only allow the "true foreign key" assertion
          when the evidence supports it (unique parent key + full RI) — an FK
          constraint on a non-unique / orphaned parent would fail at apply. */}
      {bothCols && (
        <label className={`flex items-start gap-2 text-[11px] ${fkEligible ? 'text-slate-600 dark:text-slate-300 cursor-pointer' : 'text-slate-400 cursor-not-allowed'}`}>
          <input type="checkbox" className="mt-0.5" checked={isFk} disabled={!fkEligible}
            onChange={e => onKind?.(e.target.checked ? 'foreign_key' : 'join_key')} />
          <span>
            This join is a <strong>true foreign key</strong> (referential constraint)
            <span className="block text-slate-400">
              {!fkEligible
                ? (chosen
                    ? `Evidence too weak to assert an FK (needs PK-uniqueness ≥ ${Math.round(FK_PK_UNIQUENESS_FLOOR * 100)}% and RI ≥ ${Math.round(FK_RI_SCORE_FLOOR * 100)}%). Saved as a join key.`
                    : 'Run FK prediction to get referential-integrity evidence before asserting an FK. Saved as a join key.')
                : isFk
                  ? 'Eligible for an ALTER TABLE ADD CONSTRAINT. Requires a unique parent key + no orphan rows.'
                  : 'Saved as a join key: used for metric-view & Genie joins, but never emitted as a DDL constraint.'}
            </span>
          </span>
        </label>
      )}
    </div>
  )
}
