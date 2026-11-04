import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { safeFetch, safeFetchObj, ErrorBanner } from '../App'
import { useLanguage } from '../contexts/LanguageContext'
import { PageHeader, EmptyState } from './ui'

const CLUSTER_PALETTE = [
  '#6366f1', '#ec4899', '#10b981', '#f59e0b', '#3b82f6',
  '#ef4444', '#8b5cf6', '#14b8a6', '#f97316', '#06b6d4',
]
const shortName = id => (id || '').split('.').pop()

export function FKApplyResult({ result, onSwitchToTags }) {
  const { t } = useLanguage()
  const [showDetails, setShowDetails] = useState(false)
  if (!result) return null
  if (result.error) return <div className="text-sm text-red-600">{String(result.error)}</div>
  const ok = (result.results || []).filter(r => r.ok)
  const failed = (result.results || []).filter(r => !r.ok)
  const hasPermError = failed.some(r => (r.error || '').includes('PERMISSION_DENIED'))
  return (
    <div className="text-sm space-y-1">
      <div className={failed.length ? 'text-amber-700' : 'text-green-600'}>
        {t('fk.appliedResult', { ok: ok.length, failed: failed.length })}
        {failed.length > 0 && (
          <button onClick={() => setShowDetails(d => !d)} className="ml-2 text-xs text-blue-600 hover:underline">
            {showDetails ? t('fk.hideErrors') : t('fk.showErrors')}
          </button>
        )}
      </div>
      {hasPermError && (
        <div className="border border-amber-200 rounded-lg bg-amber-50 p-3 text-xs text-amber-800">
          <strong>{t('fk.permissionIssue')}</strong> {t('fk.permManageHint')}
          {onSwitchToTags
            ? <> Try <button onClick={onSwitchToTags} className="text-blue-600 hover:underline font-medium">{t('fk.applyAsTagsBtn')}</button> instead &mdash; it only requires APPLY_TAG permission and stores the same relationship metadata.</>
            : <> Switch to "Apply as Tags" mode, which only requires APPLY_TAG permission.</>
          }
        </div>
      )}
      {showDetails && failed.length > 0 && (
        <div className="border border-red-200 rounded-lg bg-red-50 p-2 space-y-1.5 max-h-60 overflow-y-auto">
          {failed.map((r, i) => (
            <div key={i} className="text-xs">
              <p className="font-mono text-slate-700 truncate" title={r.statement || r.sql}>{r.statement || r.sql || '(unknown)'}</p>
              <p className="text-red-600 ml-2">{(r.error || '').replace(/\s*\[Hint:.*?\]/, '')}</p>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
const tableColumn = id => { const parts = (id || '').split('.'); return parts.length >= 2 ? `${parts[parts.length - 2]}.${parts[parts.length - 1]}` : id }

export function FKMapViz() {
  const { t: tr } = useLanguage()
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

  if (!data) return <p className="text-sm text-slate-400">{tr('fk.loadingFkMap')}</p>

  return (
    <div className="space-y-3">
      <ErrorBanner error={error} />
      <div className="flex items-center gap-3 text-xs text-slate-500">
        <label>{tr('fk.clusterGrouping')}</label>
        <input type="range" min="0" max="1" step="0.05" value={strength}
          onChange={e => setStrength(Number(e.target.value))} className="w-32" title={tr('fk.clusterTip')} />
        <span>{strength.toFixed(2)}</span>
        <button type="button" onClick={load} className="text-dbx-lava hover:underline" title={tr('fk.reloadFkMap')}>{tr('fk.refresh')}</button>
      </div>
      {graphData.nodes.length === 0
        ? <p className="text-sm text-slate-400">{tr('fk.noNodes')}</p>
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

export function FKPredictionsTable({ onRefresh }) {
  const { t } = useLanguage()
  const [predictions, setPredictions] = useState([])
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(new Set())
  const [deleting, setDeleting] = useState(false)

  const load = useCallback(() => {
    safeFetch('/api/analytics/fk-predictions').then(r => {
      setPredictions(Array.isArray(r.data) ? r.data : [])
      if (r.error) setError(r.error)
      setSelected(new Set())
    })
  }, [])

  useEffect(() => { load() }, [load])
  useEffect(() => { if (onRefresh) onRefresh(load) }, [onRefresh, load])

  const toggle = (i) => setSelected(prev => {
    const next = new Set(prev); next.has(i) ? next.delete(i) : next.add(i); return next
  })

  const deletePreds = async (rows) => {
    setDeleting(true)
    try {
      const body = rows.map(r => ({
        src_table: r.src_table || parseFQColumn(r.src_column).table,
        src_column: r.src_column,
        dst_table: r.dst_table || parseFQColumn(r.dst_column).table,
        dst_column: r.dst_column,
      }))
      const res = await fetch('/api/analytics/fk-delete', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ predictions: body }) })
      const j = await res.json().catch(() => ({}))
      if (!res.ok) { setError(j.detail || `Server error (${res.status})`); return }
      if (j.deleted > 0) {
        const removed = new Set(rows.map(r => `${r.src_column}::${r.dst_column}`))
        setPredictions(prev => prev.filter(p => !removed.has(`${p.src_column}::${p.dst_column}`)))
        setSelected(new Set())
      }
    } catch (err) { setError(err.message) }
    setDeleting(false)
  }

  return (
    <div className="bg-dbx-oat-light rounded-xl border border-slate-200 p-6 shadow-sm">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-slate-800">{t('fk.predictedFks')}</h2>
        <div className="flex items-center gap-2">
          <button type="button" onClick={load} className="text-sm text-dbx-lava hover:underline" title={t('fk.reloadFkPreds')}>{t('fk.refresh')}</button>
          {selected.size > 0 && (
            <button type="button" onClick={() => deletePreds([...selected].map(i => predictions[i]).filter(Boolean))}
              disabled={deleting}
              className="px-3 py-1.5 bg-red-600 text-white rounded-lg text-sm font-medium hover:bg-red-700 disabled:opacity-50">
              {deleting ? t('fk.removing') : t('fk.removeN', { n: selected.size })}
            </button>
          )}
        </div>
      </div>
      <ErrorBanner error={error} />
      {predictions.length === 0
        ? <p className="text-sm text-slate-400">{t('fk.noFkPreds')}</p>
        : <div className="overflow-x-auto max-h-96">
          <table className="min-w-full text-sm">
            <thead><tr>
              <th className="w-10 px-2 py-2.5 bg-dbx-oat border-b border-slate-200"></th>
              {[
                { k: 'sourceColumn' }, { k: 'targetColumn' }, { k: 'colSim' },
                { k: 'rule' }, { k: 'aiConf' }, { k: 'final' }, { k: 'reasoning' },
              ].map(h =>
                <th key={h.k} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider" title={t(`fk.tip.${h.k}`)}>{t(`fk.hdr.${h.k}`)}</th>)}
              <th className="w-10 px-2 py-2.5 bg-dbx-oat border-b border-slate-200"></th>
            </tr></thead>
            <tbody>
              {predictions.map((p, i) => (
                <tr key={i} className={`border-b border-slate-100 hover:bg-orange-50/30 transition-colors ${selected.has(i) ? 'bg-orange-50/50' : ''}`}>
                  <td className="px-2 py-2">
                    <input type="checkbox" checked={selected.has(i)} onChange={() => toggle(i)} className="rounded border-slate-300" />
                  </td>
                  <td className="px-3 py-2 text-slate-700 font-mono text-xs" title={p.src_column}>{tableColumn(p.src_column)}</td>
                  <td className="px-3 py-2 text-slate-700 font-mono text-xs" title={p.dst_column}>{tableColumn(p.dst_column)}</td>
                  <td className="px-3 py-2">{Number(p.col_similarity).toFixed(2)}</td>
                  <td className="px-3 py-2 font-medium text-red-700">{Number(p.rule_score).toFixed(2)}</td>
                  <td className="px-3 py-2 font-medium text-emerald-700">{Number(p.ai_confidence).toFixed(2)}</td>
                  <td className="px-3 py-2 font-bold text-slate-800">{Number(p.final_confidence).toFixed(2)}</td>
                  <td className="px-3 py-2 text-slate-500 text-xs max-w-xs truncate" title={p.ai_reasoning}>{p.ai_reasoning}</td>
                  <td className="px-2 py-2">
                    <button onClick={() => deletePreds([p])} disabled={deleting} title={t('fk.removeFkPred')}
                      className="text-red-400 hover:text-red-600 disabled:opacity-50 text-sm">&#x2715;</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      }
    </div>
  )
}

function ApplyModeToggle({ mode, setMode }) {
  const { t } = useLanguage()
  return (
    <div className="inline-flex rounded-md border border-slate-300 text-xs overflow-hidden" title={t('fk.applyModeTip')}>
      {[['constraint', t('fk.constraint')], ['tags', t('fk.tags')]].map(([v, label]) => (
        <button key={v} onClick={() => setMode(v)}
          className={`px-2.5 py-1 ${mode === v ? 'bg-dbx-lava text-white' : 'bg-white text-slate-600 hover:bg-slate-50'}`}>
          {label}
        </button>
      ))}
    </div>
  )
}

function parseFQColumn(fqCol) {
  const parts = (fqCol || '').split('.')
  if (parts.length >= 4) return { table: parts.slice(0, 3).join('.'), column: parts.slice(3).join('.') }
  if (parts.length === 2) return { table: parts[0], column: parts[1] }
  return { table: fqCol, column: fqCol }
}

async function applyRowsAsTags(rows) {
  const predictions = rows.map(r => {
    const src = parseFQColumn(r.src_column)
    const dst = parseFQColumn(r.dst_column)
    return { src_table: src.table, src_column: src.column, dst_table: dst.table, dst_column: dst.column }
  })
  const res = await fetch('/api/analytics/fk-apply-as-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ predictions }) })
  return await res.json().catch(() => ({}))
}

export function FKDDLTable({ onRefresh }) {
  const { t } = useLanguage()
  const [ddlRows, setDdlRows] = useState([])
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(new Set())
  const [applyResult, setApplyResult] = useState(null)
  const [applying, setApplying] = useState(false)
  const [applyMode, setApplyMode] = useState('constraint')

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
    const selectedRows = [...selected].map(i => ddlRows[i]).filter(Boolean)
    try {
      let j
      if (applyMode === 'tags') {
        j = await applyRowsAsTags(selectedRows)
      } else {
        const statements = selectedRows.map(r => r.ddl_statement).filter(Boolean)
        const r = await fetch('/api/analytics/fk-apply', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ statements }) })
        j = await r.json().catch(() => ({}))
      }
      setApplyResult(j.results ? j : { error: j.detail || JSON.stringify(j) })
      if ((j.results || []).every(r => r.ok)) setSelected(new Set())
    } catch (e) { setApplyResult({ error: e.message }) }
    setApplying(false)
  }

  return (
    <div className="bg-dbx-oat-light rounded-xl border border-slate-200 p-6 shadow-sm">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <h2 className="text-lg font-semibold text-slate-800">{t('fk.ddlStatements')}</h2>
          <ApplyModeToggle mode={applyMode} setMode={setApplyMode} />
        </div>
        <div className="flex items-center gap-2">
          <button type="button" onClick={load} className="text-sm text-dbx-lava hover:underline">{t('fk.refresh')}</button>
          {selected.size > 0 && (
            <button type="button" onClick={applySelected} disabled={applying}
              className="px-3 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 disabled:opacity-50">
              {applying ? t('common.applying') : t('fk.applyAs', { mode: applyMode === 'tags' ? t('fk.applyAsTags') : t('fk.applyAsConstraints'), n: selected.size })}
            </button>
          )}
        </div>
      </div>
      {applyMode === 'tags' && (
        <p className="text-xs text-slate-500 mb-3 bg-blue-50 border border-blue-200 rounded px-3 py-1.5">
          {t('fk.tagsModePre')}<code className="font-mono">fk_references</code>{t('fk.tagsModeMid')}<strong>APPLY_TAG</strong>{t('fk.tagsModePost')}
        </p>
      )}
      {applyResult && <div className="mb-4"><FKApplyResult result={applyResult} onSwitchToTags={applyMode === 'constraint' ? () => setApplyMode('tags') : undefined} /></div>}
      <ErrorBanner error={error} />
      {ddlRows.length === 0
        ? <p className="text-sm text-slate-400">{t('fk.noDdl')}</p>
        : <div className="overflow-x-auto max-h-96">
          <table className="min-w-full text-sm">
            <thead><tr>
              <th className="w-10 px-2 py-2.5 bg-dbx-oat border-b border-slate-200"></th>
              {[
                { k: 'sourceColumn', label: t('fk.hdr.sourceColumn') },
                { k: 'targetColumn', label: t('fk.hdr.targetColumn') },
                { k: 'confidence', label: t('fk.hdr.confidence') },
                { k: 'ddl', label: 'DDL' },
              ].map(h =>
                <th key={h.k} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h.label}</th>)}
            </tr></thead>
            <tbody>
              {ddlRows.map((row, i) => (
                <tr key={i} className={`border-b border-slate-100 hover:bg-orange-50/30 ${selected.has(i) ? 'bg-orange-50/50' : ''}`}>
                  <td className="px-2 py-2">
                    <input type="checkbox" checked={selected.has(i)} onChange={() => toggle(i)} className="rounded border-slate-300" />
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

export function FKApplyPanel() {
  const { t } = useLanguage()
  const [ddlCount, setDdlCount] = useState(null)
  const [applyingAll, setApplyingAll] = useState(false)
  const [applyAllResult, setApplyAllResult] = useState(null)
  const [applyMode, setApplyMode] = useState('constraint')
  const refreshDdlRef = useRef(null)

  useEffect(() => { loadDdlCount() }, [])
  const loadDdlCount = () => {
    safeFetch('/api/analytics/fk-ddl').then(r => { setDdlCount(Array.isArray(r.data) ? r.data.length : 0) })
  }
  const applyAllHighConfidence = async () => {
    setApplyingAll(true); setApplyAllResult(null)
    try {
      const ddlRes = await safeFetch('/api/analytics/fk-ddl')
      const ddlRows = Array.isArray(ddlRes.data) ? ddlRes.data : []
      const highConf = ddlRows.filter(r => Number(r.confidence) >= 0.8)
      if (highConf.length === 0) { setApplyAllResult({ error: t('fk.noHighConf') }); setApplyingAll(false); return }
      let j
      if (applyMode === 'tags') {
        j = await applyRowsAsTags(highConf)
      } else {
        const statements = highConf.map(r => r.ddl_statement).filter(Boolean)
        const res = await fetch('/api/analytics/fk-apply', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ statements }) })
        j = await res.json().catch(() => ({}))
      }
      setApplyAllResult(j.results ? j : { error: j.detail || JSON.stringify(j) })
      if (refreshDdlRef.current) refreshDdlRef.current()
    } catch (e) { setApplyAllResult({ error: e.message }) }
    setApplyingAll(false)
  }

  return (
    <div className="space-y-6">
      <section className="bg-dbx-oat-light rounded-lg border p-6">
        <div className="flex items-center gap-3 mb-2">
          <h2 className="text-lg font-semibold">{t('fk.ddlQuickActions')}</h2>
          <ApplyModeToggle mode={applyMode} setMode={setApplyMode} />
        </div>
        <p className="text-xs text-slate-500 mb-3">
          {ddlCount != null ? t('fk.ddlCountAvailable', { n: ddlCount }) : t('fk.loadingDdlCount')}.
          {applyMode === 'tags'
            ? t('fk.tagsModeHint')
            : t('fk.constraintModeHint')}
        </p>
        <div className="flex gap-2 mb-3">
          <button onClick={() => { loadDdlCount(); if (refreshDdlRef.current) refreshDdlRef.current() }}
            className="px-3 py-2 bg-slate-600 text-white rounded-md text-sm hover:bg-slate-700">{t('fk.refreshDdl')}</button>
          <button onClick={applyAllHighConfidence} disabled={applyingAll || ddlCount === 0}
            className="px-3 py-2 bg-dbx-lava text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50">
            {applyingAll ? t('common.applying') : t('fk.applyAllAs', { mode: applyMode === 'tags' ? t('fk.applyAllAsTags') : t('fk.applyAllAsConstraints') })}
          </button>
        </div>
        {applyAllResult && <FKApplyResult result={applyAllResult} onSwitchToTags={applyMode === 'constraint' ? () => setApplyMode('tags') : undefined} />}
      </section>
      <FKPredictionsTable />
      <FKDDLTable onRefresh={(fn) => { refreshDdlRef.current = fn }} />
    </div>
  )
}

export default function ForeignKeyGeneration() {
  const { t } = useLanguage()
  const [ddlCount, setDdlCount] = useState(null)
  const [applyingAll, setApplyingAll] = useState(false)
  const [applyAllResult, setApplyAllResult] = useState(null)
  const [applyMode, setApplyMode] = useState('constraint')
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
        setApplyAllResult({ error: t('fk.noHighConf') })
        setApplyingAll(false)
        return
      }
      let j
      if (applyMode === 'tags') {
        j = await applyRowsAsTags(highConf)
      } else {
        const statements = highConf.map(r => r.ddl_statement).filter(Boolean)
        const res = await fetch('/api/analytics/fk-apply', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ statements }),
        })
        j = await res.json().catch(() => ({}))
      }
      setApplyAllResult(j.results ? j : { error: j.detail || JSON.stringify(j) })
      if (refreshDdlRef.current) refreshDdlRef.current()
    } catch (e) {
      setApplyAllResult({ error: e.message })
    }
    setApplyingAll(false)
  }

  return (
    <div className="space-y-6">
      <PageHeader title={t('fk.foreignKeyPredictions')} subtitle={t('fk.pageSubtitle')} />
      <section className="card p-6">
        <div className="flex items-center gap-3 mb-2">
          <h2 className="heading-section">{t('fk.ddlQuickActions')}</h2>
          <ApplyModeToggle mode={applyMode} setMode={setApplyMode} />
        </div>
        <p className="text-xs text-slate-500 mb-3">
          {ddlCount != null ? t('fk.ddlCountAvailable', { n: ddlCount }) : t('fk.loadingDdlCount')}.
          {applyMode === 'tags'
            ? t('fk.tagsModeHint')
            : t('fk.constraintModeHint')}
        </p>
        <div className="flex gap-2 mb-3">
          <button onClick={() => { loadDdlCount(); if (refreshDdlRef.current) refreshDdlRef.current() }}
            className="btn-secondary btn-sm">
            {t('fk.refreshDdl')}
          </button>
          <button onClick={applyAllHighConfidence} disabled={applyingAll || ddlCount === 0}
            className="btn-primary btn-sm">
            {applyingAll ? t('fk.applying') : (applyMode === 'tags' ? t('fk.applyAllTags') : t('fk.applyAllConstraints'))}
          </button>
        </div>
        {applyAllResult && <FKApplyResult result={applyAllResult} onSwitchToTags={applyMode === 'constraint' ? () => setApplyMode('tags') : undefined} />}
      </section>

      <section>
        <h2 className="heading-section text-slate-800 mb-3">{t('fk.fkMap')}</h2>
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
