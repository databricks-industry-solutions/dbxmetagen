import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { safeFetch, safeFetchObj, ErrorBanner } from '../App'

const CLUSTER_PALETTE = [
  '#6366f1', '#ec4899', '#10b981', '#f59e0b', '#3b82f6',
  '#ef4444', '#8b5cf6', '#14b8a6', '#f97316', '#06b6d4',
]
const shortName = id => (id || '').split('.').pop()
const tableColumn = id => { const parts = (id || '').split('.'); return parts.length >= 2 ? `${parts[parts.length - 2]}.${parts[parts.length - 1]}` : id }

function FKMapViz() {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [strength, setStrength] = useState(0.3)
  const graphRef = useRef()

  const load = useCallback(() => {
    safeFetchObj('/api/viz/fk-map').then(r => {
      if (r.error) setError(r.error)
      else setData(r.data)
    })
  }, [])

  useEffect(() => { load() }, [load])

  const graphData = useMemo(() => {
    if (!data) return { nodes: [], links: [] }
    const clusterMap = {}
    ;(data.clusters || []).forEach(c => { clusterMap[c.id] = c.cluster })
    const nodes = (data.tables || []).map(t => ({
      id: t.id, label: shortName(t.id), domain: t.domain,
      cluster: clusterMap[t.id] ?? -1,
    }))
    const nodeIds = new Set(nodes.map(n => n.id))
    const links = (data.fk_edges || [])
      .filter(e => nodeIds.has(e.src_table) && nodeIds.has(e.dst_table))
      .map(e => ({
        source: e.src_table, target: e.dst_table,
        label: `${tableColumn(e.src_column)} -> ${tableColumn(e.dst_column)}`,
        confidence: Math.min(1, Math.max(0, Number(e.final_confidence) || 0)),
      }))
    return { nodes, links }
  }, [data])

  const clusterCenters = useMemo(() => {
    const centers = {}
    const clusters = [...new Set(graphData.nodes.map(n => n.cluster).filter(c => c >= 0))]
    const r = 120
    clusters.forEach((c, i) => {
      const angle = (2 * Math.PI * i) / clusters.length
      centers[c] = { x: r * Math.cos(angle), y: r * Math.sin(angle) }
    })
    return centers
  }, [graphData])

  const onTick = useCallback(() => {
    graphData.nodes.forEach(n => {
      const target = clusterCenters[n.cluster]
      if (!target || !n.x) return
      n.vx += (target.x - n.x) * strength * 0.02
      n.vy += (target.y - n.y) * strength * 0.02
    })
  }, [graphData, clusterCenters, strength])

  const nodeColor = useCallback(n => {
    if (n.cluster < 0) return '#94a3b8'
    return CLUSTER_PALETTE[n.cluster % CLUSTER_PALETTE.length]
  }, [])

  const paintClusterCircles = useCallback((ctx) => {
    if (graphData.nodes.length === 0) return
    const groups = {}
    graphData.nodes.forEach(n => {
      if (n.cluster < 0 || !n.x) return
      groups[n.cluster] = groups[n.cluster] || []
      groups[n.cluster].push(n)
    })
    Object.entries(groups).forEach(([c, members]) => {
      if (members.length < 2) return
      let cx = 0, cy = 0
      members.forEach(m => { cx += m.x; cy += m.y })
      cx /= members.length; cy /= members.length
      let maxR = 0
      members.forEach(m => {
        const d = Math.sqrt((m.x - cx) ** 2 + (m.y - cy) ** 2)
        if (d > maxR) maxR = d
      })
      const color = CLUSTER_PALETTE[Number(c) % CLUSTER_PALETTE.length]
      ctx.beginPath()
      ctx.arc(cx, cy, maxR + 20, 0, 2 * Math.PI)
      ctx.fillStyle = color + '15'
      ctx.strokeStyle = color + '40'
      ctx.lineWidth = 1.5
      ctx.fill()
      ctx.stroke()
      ctx.font = '8px sans-serif'
      ctx.fillStyle = color
      ctx.textAlign = 'center'
      ctx.fillText(`Cluster ${c}`, cx, cy - maxR - 8)
    })
  }, [graphData])

  if (!data) return <p className="text-sm text-slate-400">Loading FK Map...</p>

  return (
    <div className="space-y-3">
      <ErrorBanner error={error} />
      <div className="flex items-center gap-3 text-xs text-slate-500">
        <label>Cluster grouping</label>
        <input type="range" min="0" max="1" step="0.05" value={strength}
          onChange={e => setStrength(Number(e.target.value))} className="w-32" title="Adjust how strongly tables are grouped by cluster (0=loose, 1=tight)" />
        <span>{strength.toFixed(2)}</span>
        <button type="button" onClick={load} className="text-dbx-lava hover:underline" title="Reload FK map data">Refresh</button>
      </div>
      {graphData.nodes.length === 0
        ? <p className="text-sm text-slate-400">No table nodes or FK predictions available.</p>
        : <div className="bg-dbx-oat-light border border-slate-200 rounded-xl shadow-sm overflow-hidden" style={{ height: 500 }}>
          <ForceGraph2D
            ref={graphRef} graphData={graphData} width={900} height={500}
            nodeColor={nodeColor}
            nodeLabel={n => `${n.id}\nDomain: ${n.domain || '?'}\nCluster: ${n.cluster}`}
            nodeCanvasObject={(node, ctx, globalScale) => {
              const r = 5
              ctx.beginPath(); ctx.arc(node.x, node.y, r, 0, 2 * Math.PI)
              ctx.fillStyle = nodeColor(node); ctx.fill()
              if (globalScale > 0.8) {
                ctx.font = `${Math.max(8, 10 / globalScale)}px sans-serif`
                ctx.fillStyle = '#334155'; ctx.textAlign = 'center'
                ctx.fillText(node.label, node.x, node.y + r + 10 / globalScale)
              }
            }}
            nodeCanvasObjectMode={() => 'replace'}
            linkLabel={l => `${l.label} (${(l.confidence * 100).toFixed(0)}%)`}
            linkWidth={l => 1 + Math.min(1, l.confidence) * 3}
            linkColor={l => `rgba(99,102,241,${0.3 + Math.min(1, l.confidence) * 0.7})`}
            linkDirectionalArrowLength={4} linkDirectionalArrowRelPos={1}
            d3AlphaDecay={0.05} d3VelocityDecay={0.3}
            warmupTicks={50} cooldownTicks={200}
            minZoom={0.5} maxZoom={8}
            enableZoomInteraction={true}
            onEngineTick={onTick}
            onRenderFramePost={(ctx) => paintClusterCircles(ctx)}
          />
        </div>
      }
      <div className="flex flex-wrap gap-3 text-xs text-slate-500">
        {[...new Set(graphData.nodes.map(n => n.cluster))].filter(c => c >= 0).sort((a, b) => a - b).map(c => (
          <span key={c} className="flex items-center gap-1">
            <span className="inline-block w-3 h-3 rounded-full" style={{ background: CLUSTER_PALETTE[c % CLUSTER_PALETTE.length] }} />
            Cluster {c}
          </span>
        ))}
      </div>
    </div>
  )
}

function FKPredictionsTable({ onRefresh }) {
  const [predictions, setPredictions] = useState([])
  const [error, setError] = useState(null)

  const load = useCallback(() => {
    safeFetch('/api/analytics/fk-predictions').then(r => {
      setPredictions(r.data || [])
      if (r.error) setError(r.error)
    })
  }, [])

  useEffect(() => { load() }, [load])
  useEffect(() => { if (onRefresh) onRefresh(load) }, [onRefresh, load])

  return (
    <div className="bg-dbx-oat-light rounded-xl border border-slate-200 p-6 shadow-sm">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-slate-800">Predicted Foreign Keys</h2>
        <button type="button" onClick={load} className="text-sm text-dbx-lava hover:underline" title="Reload FK predictions">Refresh</button>
      </div>
      <ErrorBanner error={error} />
      {predictions.length === 0
        ? <p className="text-sm text-slate-400">No FK predictions available. Run the FK prediction job first.</p>
        : <div className="overflow-x-auto max-h-96">
          <table className="min-w-full text-sm">
            <thead><tr>
              {['Source Column', 'Target Column', 'Col Sim', 'Rule', 'AI Conf', 'Final', 'Reasoning'].map(h =>
                <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider" title={
                  h === 'Source Column' ? 'Source table.column (referencing table)' :
                  h === 'Target Column' ? 'Target table.column (referenced table)' :
                  h === 'Col Sim' ? 'Column embedding similarity score (0-1)' :
                  h === 'Rule' ? 'Rule-based heuristic score' :
                  h === 'AI Conf' ? 'AI model confidence score' :
                  h === 'Final' ? 'Combined final confidence score' :
                  'AI explanation for the prediction'
                }>{h}</th>)}
            </tr></thead>
            <tbody>
              {predictions.map((p, i) => (
                <tr key={i} className="border-b border-slate-100 hover:bg-orange-50/30 transition-colors">
                  <td className="px-3 py-2 text-slate-700 font-mono text-xs" title={p.src_column}>{tableColumn(p.src_column)}</td>
                  <td className="px-3 py-2 text-slate-700 font-mono text-xs" title={p.dst_column}>{tableColumn(p.dst_column)}</td>
                  <td className="px-3 py-2">{Number(p.col_similarity).toFixed(2)}</td>
                  <td className="px-3 py-2 font-medium text-red-700">{Number(p.rule_score).toFixed(2)}</td>
                  <td className="px-3 py-2 font-medium text-emerald-700">{Number(p.ai_confidence).toFixed(2)}</td>
                  <td className="px-3 py-2 font-bold text-slate-800">{Number(p.final_confidence).toFixed(2)}</td>
                  <td className="px-3 py-2 text-slate-500 text-xs max-w-xs truncate" title={p.ai_reasoning}>{p.ai_reasoning}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      }
    </div>
  )
}

function FKDDLTable({ onRefresh }) {
  const [ddlRows, setDdlRows] = useState([])
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(new Set())
  const [applyResult, setApplyResult] = useState(null)
  const [applying, setApplying] = useState(false)

  const load = useCallback(() => {
    safeFetch('/api/analytics/fk-ddl').then(r => {
      setDdlRows(Array.isArray(r.data) ? r.data : [])
      if (r.error) setError(r.error)
      setSelected(new Set())
    })
  }, [])

  useEffect(() => { load() }, [load])
  useEffect(() => { if (onRefresh) onRefresh(load) }, [onRefresh, load])

  const toggle = (i) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(i)) next.delete(i); else next.add(i)
      return next
    })
  }

  const applySelected = async () => {
    if (selected.size === 0) return
    setApplying(true)
    setApplyResult(null)
    const statements = [...selected].map(i => ddlRows[i]?.ddl_statement).filter(Boolean)
    try {
      const r = await fetch('/api/analytics/fk-apply', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ statements }) })
      const j = await r.json().catch(() => ({}))
      setApplyResult(r.ok ? j : { error: j.detail || j })
      if (r.ok) setSelected(new Set())
    } catch (e) { setApplyResult({ error: e.message }) }
    setApplying(false)
  }

  return (
    <div className="bg-dbx-oat-light rounded-xl border border-slate-200 p-6 shadow-sm">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-slate-800">FK DDL Statements (Apply to catalog)</h2>
        <div className="flex items-center gap-2">
          <button type="button" onClick={load} className="text-sm text-dbx-lava hover:underline" title="Reload FK DDL statements">Refresh</button>
          {selected.size > 0 && (
            <button type="button" onClick={applySelected} disabled={applying}
              className="px-3 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 disabled:opacity-50"
              title="Execute ALTER TABLE statements to add selected foreign keys to the catalog">
              Apply selected ({selected.size})
            </button>
          )}
        </div>
      </div>
      {applyResult && (
        <div className={`mb-4 text-sm ${applyResult.error ? 'text-red-600' : 'text-green-600'}`}>
          {applyResult.error ? JSON.stringify(applyResult.error) : `Applied: ${(applyResult.results || []).filter(r => r.ok).length} succeeded, ${(applyResult.results || []).filter(r => !r.ok).length} failed.`}
        </div>
      )}
      <ErrorBanner error={error} />
      {ddlRows.length === 0
        ? <p className="text-sm text-slate-400">No FK DDL statements. Run the FK prediction job first to populate this table.</p>
        : <div className="overflow-x-auto max-h-96">
          <table className="min-w-full text-sm">
            <thead><tr>
              <th className="w-10 px-2 py-2.5 bg-dbx-oat border-b border-slate-200" title="Select rows to apply"></th>
              {['Source Column', 'Target Column', 'Confidence', 'DDL'].map(h =>
                <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider" title={
                  h === 'Source Column' ? 'Source table.column (referencing table)' :
                  h === 'Target Column' ? 'Target table.column (referenced table)' :
                  h === 'Confidence' ? 'Prediction confidence score (0-1)' :
                  'ALTER TABLE DDL statement to add the foreign key'
                }>{h}</th>)}
            </tr></thead>
            <tbody>
              {ddlRows.map((row, i) => (
                <tr key={i} className={`border-b border-slate-100 hover:bg-orange-50/30 ${selected.has(i) ? 'bg-orange-50/50' : ''}`}>
                  <td className="px-2 py-2">
                    <input type="checkbox" checked={selected.has(i)} onChange={() => toggle(i)} className="rounded border-slate-300" title="Select to apply this FK DDL" />
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-slate-700">{row.src_column}</td>
                  <td className="px-3 py-2 font-mono text-xs text-slate-700">{row.dst_column}</td>
                  <td className="px-3 py-2">{Number(row.confidence).toFixed(2)}</td>
                  <td className="px-3 py-2 font-mono text-xs text-slate-500 max-w-md truncate" title={row.ddl_statement}>{row.ddl_statement}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      }
    </div>
  )
}

export default function ForeignKeyGeneration() {
  const [ddlCount, setDdlCount] = useState(null)
  const [applyingAll, setApplyingAll] = useState(false)
  const [applyAllResult, setApplyAllResult] = useState(null)
  const refreshDdlRef = useRef(null)

  useEffect(() => { loadDdlCount() }, [])

  const loadDdlCount = () => {
    safeFetch('/api/analytics/fk-ddl').then(r => {
      setDdlCount(Array.isArray(r.data) ? r.data.length : 0)
    })
  }

  const applyAllHighConfidence = async () => {
    setApplyingAll(true)
    setApplyAllResult(null)
    try {
      const ddlRes = await safeFetch('/api/analytics/fk-ddl')
      const ddlRows = Array.isArray(ddlRes.data) ? ddlRes.data : []
      const highConf = ddlRows.filter(r => Number(r.confidence) >= 0.8)
      if (highConf.length === 0) {
        setApplyAllResult({ error: 'No DDL statements with confidence >= 0.8' })
        setApplyingAll(false)
        return
      }
      const statements = highConf.map(r => r.ddl_statement).filter(Boolean)
      const res = await fetch('/api/analytics/fk-apply', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ statements }),
      })
      const j = await res.json().catch(() => ({}))
      setApplyAllResult(res.ok ? j : { error: j.detail || JSON.stringify(j) })
      if (refreshDdlRef.current) refreshDdlRef.current()
    } catch (e) {
      setApplyAllResult({ error: e.message })
    }
    setApplyingAll(false)
  }

  return (
    <div className="space-y-6">
      <section className="bg-dbx-oat-light rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-2">DDL Quick Actions</h2>
        <p className="text-xs text-slate-500 mb-3">
          {ddlCount != null ? `${ddlCount} DDL statements available` : 'Loading DDL count...'}.
          Apply all high-confidence FKs or refresh the DDL list.
        </p>
        <div className="flex gap-2 mb-3">
          <button onClick={() => { loadDdlCount(); if (refreshDdlRef.current) refreshDdlRef.current() }}
            className="px-3 py-2 bg-slate-600 text-white rounded-md text-sm hover:bg-slate-700"
            title="Refresh DDL statements from predictions">
            Refresh DDL
          </button>
          <button onClick={applyAllHighConfidence} disabled={applyingAll || ddlCount === 0}
            className="px-3 py-2 bg-dbx-lava text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50"
            title="Apply all FK DDL statements with confidence >= 0.8">
            {applyingAll ? 'Applying...' : 'Apply All (conf >= 0.8)'}
          </button>
        </div>
        {applyAllResult && (
          <div className={`text-xs ${applyAllResult.error ? 'text-red-600' : 'text-green-600'}`}>
            {applyAllResult.error
              ? String(applyAllResult.error)
              : `Applied: ${(applyAllResult.results || []).filter(r => r.ok).length} succeeded, ${(applyAllResult.results || []).filter(r => !r.ok).length} failed.`}
          </div>
        )}
      </section>

      <section>
        <h2 className="text-lg font-semibold text-slate-800 mb-3">FK Map</h2>
        <FKMapViz />
      </section>

      <section>
        <FKPredictionsTable />
      </section>

      <section>
        <FKDDLTable onRefresh={(fn) => { refreshDdlRef.current = fn }} />
      </section>
    </div>
  )
}
