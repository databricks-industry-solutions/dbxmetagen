import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from 'recharts'
import { safeFetch, ErrorBanner } from '../App'

const PALETTE = [
  '#FF3621', '#0B2026', '#6366f1', '#10b981', '#f59e0b',
  '#3b82f6', '#ec4899', '#8b5cf6', '#14b8a6', '#f97316',
]
const BAND_COLORS = { '0-0.4': '#f97316', '0.4-0.6': '#eab308', '0.6-0.8': '#65a30d', '0.8-1.0': '#15803d' }
const EDGE_COLORS = { instance_of: '#6366f1', has_attribute: '#10b981', is_a: '#3b82f6', references: '#f59e0b' }
const shortName = id => (id || '').split('.').pop()

function HealthCards({ metrics }) {
  const m = useMemo(() => {
    const map = {}
    ;(metrics || []).forEach(r => { map[r.metric_name] = r.value })
    return map
  }, [metrics])

  if (!Object.keys(m).length) return null

  const cards = [
    { label: 'Total Entities', value: m.total_entities, sub: `${m.total_entity_types || 0} types` },
    { label: 'Avg Confidence', value: m.avg_confidence ? Number(m.avg_confidence).toFixed(2) : '--' },
    { label: 'Table Coverage', value: m.table_coverage_pct ? `${m.table_coverage_pct}%` : '--' },
    { label: 'Binding Rate', value: m.binding_completeness_pct ? `${m.binding_completeness_pct}%` : '--' },
    { label: 'Ontology Edges', value: m.ontology_edges_total || '0' },
    { label: 'Low Confidence', value: m.low_confidence_count || '0', warn: Number(m.low_confidence_count) > 0 },
  ]

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
      {cards.map((c, i) => (
        <div key={i} className="border border-slate-200 rounded-xl p-4 bg-gradient-to-br from-white to-slate-50">
          <p className="text-xs text-slate-500 uppercase tracking-wider">{c.label}</p>
          <p className={`text-2xl font-bold mt-1 ${c.warn ? 'text-amber-600' : 'text-red-700'}`}>{c.value ?? '--'}</p>
          {c.sub && <p className="text-xs text-slate-400 mt-0.5">{c.sub}</p>}
        </div>
      ))}
    </div>
  )
}

function ConfidenceChart({ data }) {
  if (!data || !data.length) return <p className="text-sm text-slate-400">No confidence data.</p>
  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart data={data} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis dataKey="band" tick={{ fontSize: 12 }} />
        <YAxis tick={{ fontSize: 12 }} />
        <Tooltip />
        <Bar dataKey="count" radius={[4, 4, 0, 0]}>
          {data.map((d, i) => <Cell key={i} fill={BAND_COLORS[d.band] || '#94a3b8'} />)}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

function EdgeBreakdownChart({ data }) {
  if (!data || !data.length) return <p className="text-sm text-slate-400">No edge data.</p>
  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart data={data} layout="vertical" margin={{ top: 5, right: 20, left: 80, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis type="number" tick={{ fontSize: 12 }} />
        <YAxis type="category" dataKey="relationship" tick={{ fontSize: 12 }} width={80} />
        <Tooltip />
        <Bar dataKey="count" radius={[0, 4, 4, 0]}>
          {data.map((d, i) => <Cell key={i} fill={EDGE_COLORS[d.relationship] || '#94a3b8'} />)}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

function EntityGraph({ entities, relationships, allRelationships }) {
  const graphRef = useRef()
  const [hovered, setHovered] = useState(null)
  const [selectedType, setSelectedType] = useState(null)

  const instanceOfLinks = useMemo(() => {
    if (!selectedType || !allRelationships) return []
    return allRelationships
      .filter(r => r.relationship === 'instance_of')
      .filter(r => {
        const matchingEntities = entities.filter(e => e.entity_type === selectedType)
        return matchingEntities.some(e => e.entity_id === r.dst)
      })
      .map(r => shortName(r.src))
  }, [selectedType, allRelationships, entities])

  const graphData = useMemo(() => {
    if (!entities.length) return { nodes: [], links: [] }

    const typeSet = new Map()
    entities.forEach(e => {
      if (!e.entity_type) return
      const existing = typeSet.get(e.entity_type)
      if (!existing || Number(e.confidence) > Number(existing.confidence))
        typeSet.set(e.entity_type, e)
    })

    const colorMap = {}
    let ci = 0
    typeSet.forEach((_, k) => { colorMap[k] = PALETTE[ci++ % PALETTE.length] })

    const nodes = [...typeSet.entries()].map(([type, e]) => ({
      id: type,
      label: type,
      confidence: Number(e.confidence) || 0,
      tableCount: entities.filter(x => x.entity_type === type).length,
      color: colorMap[type],
    }))

    const nodeIds = new Set(nodes.map(n => n.id))
    const links = (relationships || [])
      .filter(r => nodeIds.has(r.src) && nodeIds.has(r.dst) && r.src !== r.dst)
      .map(r => ({
        source: r.src,
        target: r.dst,
        label: r.relationship || '',
        weight: Number(r.weight) || 0.5,
      }))

    return { nodes, links }
  }, [entities, relationships])

  const paintNode = useCallback((node, ctx) => {
    const isSelected = selectedType === node.id
    const r = 6 + (node.tableCount || 1) * 2
    ctx.beginPath()
    ctx.arc(node.x, node.y, r, 0, 2 * Math.PI)
    ctx.fillStyle = isSelected ? '#FF3621' : (node.color || '#666')
    ctx.fill()
    if (hovered === node.id || isSelected) {
      ctx.strokeStyle = isSelected ? '#991b1b' : '#FF3621'
      ctx.lineWidth = 2.5
      ctx.stroke()
    }
    ctx.font = '10px Inter, sans-serif'
    ctx.textAlign = 'center'
    ctx.fillStyle = '#1e293b'
    ctx.fillText(node.label, node.x, node.y + r + 12)
  }, [hovered, selectedType])

  const paintLink = useCallback((link, ctx) => {
    ctx.strokeStyle = '#cbd5e1'
    ctx.lineWidth = Math.max(0.5, (link.weight || 0.5) * 2)
    ctx.beginPath()
    ctx.moveTo(link.source.x, link.source.y)
    ctx.lineTo(link.target.x, link.target.y)
    ctx.stroke()
    if (link.label) {
      const mx = (link.source.x + link.target.x) / 2
      const my = (link.source.y + link.target.y) / 2
      ctx.font = '8px Inter, sans-serif'
      ctx.textAlign = 'center'
      ctx.fillStyle = '#94a3b8'
      ctx.fillText(link.label, mx, my - 4)
    }
  }, [])

  if (!graphData.nodes.length) return <p className="text-sm text-slate-400">No entity relationships to display.</p>

  return (
    <div>
      <div className="border border-slate-200 rounded-xl overflow-hidden bg-dbx-oat-light" style={{ height: 420 }}>
        <ForceGraph2D
          ref={graphRef}
          graphData={graphData}
          width={800}
          height={420}
          nodeCanvasObject={paintNode}
          linkCanvasObject={paintLink}
          onNodeHover={n => setHovered(n?.id || null)}
          onNodeClick={n => setSelectedType(prev => prev === n?.id ? null : n?.id)}
          nodeLabel={n => `${n.label} (${n.tableCount} tables, conf ${n.confidence.toFixed(2)}) -- click to see linked tables`}
          linkLabel={l => `${l.label} (${l.weight.toFixed(2)})`}
          cooldownTicks={80}
          d3VelocityDecay={0.3}
        />
      </div>
      {selectedType && (
        <div className="mt-3 p-3 bg-indigo-50 border border-indigo-200 rounded-lg">
          <p className="text-sm font-semibold text-indigo-800">
            Tables linked to "{selectedType}" via instance_of:
          </p>
          {instanceOfLinks.length > 0 ? (
            <div className="flex flex-wrap gap-1.5 mt-2">
              {instanceOfLinks.map((t, i) => (
                <span key={i} className="inline-block bg-white text-indigo-700 text-xs px-2 py-0.5 rounded border border-indigo-200">{t}</span>
              ))}
            </div>
          ) : (
            <p className="text-xs text-indigo-500 mt-1">No instance_of edges found. Run the ontology pipeline to generate them.</p>
          )}
          <button onClick={() => setSelectedType(null)} className="mt-2 text-xs text-indigo-500 hover:underline">Clear selection</button>
        </div>
      )}
    </div>
  )
}

function CoverageHeatmap({ entities }) {
  const { types, tables, matrix } = useMemo(() => {
    const typeSet = new Set()
    const tableSet = new Set()
    const map = {}
    entities.forEach(e => {
      if (!e.entity_type) return
      typeSet.add(e.entity_type)
      const st = Array.isArray(e.source_tables) ? e.source_tables
        : typeof e.source_tables === 'string' ? (() => { try { return JSON.parse(e.source_tables) } catch { return [e.source_tables] } })()
        : []
      st.forEach(t => {
        const tn = shortName(t)
        tableSet.add(tn)
        const key = `${e.entity_type}::${tn}`
        map[key] = Math.max(map[key] || 0, Number(e.confidence) || 0)
      })
    })
    return { types: [...typeSet].sort(), tables: [...tableSet].sort(), matrix: map }
  }, [entities])

  if (!types.length || !tables.length) return <p className="text-sm text-slate-400">No coverage data available.</p>

  const cellColor = v => {
    if (!v) return '#f1f5f9'
    if (v >= 0.8) return '#15803d'
    if (v >= 0.6) return '#65a30d'
    if (v >= 0.4) return '#eab308'
    return '#f97316'
  }

  return (
    <div className="overflow-x-auto">
      <table className="text-xs border-collapse">
        <thead><tr>
          <th className="sticky left-0 bg-dbx-oat-light z-10 px-2 py-1 text-left font-semibold text-slate-600 border-b border-slate-200">Entity</th>
          {tables.map(t => (
            <th key={t} className="px-1 py-1 font-normal text-slate-500 border-b border-slate-200 whitespace-nowrap" style={{ writingMode: 'vertical-rl', maxHeight: 120 }}>{t}</th>
          ))}
        </tr></thead>
        <tbody>
          {types.map(type => (
            <tr key={type}>
              <td className="sticky left-0 bg-dbx-oat-light z-10 px-2 py-1 font-medium text-slate-700 border-b border-slate-100 whitespace-nowrap">{type}</td>
              {tables.map(t => {
                const v = matrix[`${type}::${t}`]
                return (
                  <td key={t} className="px-0.5 py-0.5 border-b border-slate-100" title={v ? `${type} / ${t}: ${v.toFixed(2)}` : ''}>
                    <div className="w-5 h-5 rounded-sm mx-auto" style={{ backgroundColor: cellColor(v) }} />
                  </td>
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="flex items-center gap-3 mt-3 text-xs text-slate-500">
        <span>Confidence:</span>
        {[['#f1f5f9', 'None'], ['#f97316', '<0.4'], ['#eab308', '0.4-0.6'], ['#65a30d', '0.6-0.8'], ['#15803d', '>=0.8']].map(([c, l]) => (
          <span key={l} className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: c }} />{l}</span>
        ))}
      </div>
    </div>
  )
}

function BindingDetail({ entities }) {
  const [expandedType, setExpandedType] = useState(null)

  const byType = useMemo(() => {
    const map = {}
    entities.forEach(e => {
      if (!e.entity_type) return
      if (!map[e.entity_type]) map[e.entity_type] = []
      map[e.entity_type].push(e)
    })
    return map
  }, [entities])

  const types = Object.keys(byType).sort()
  if (!types.length) return <p className="text-sm text-slate-400">No entities to show.</p>

  return (
    <div className="space-y-2">
      {types.map(type => {
        const ents = byType[type]
        const isOpen = expandedType === type
        const totalBindings = ents.reduce((s, e) => s + (Array.isArray(e.column_bindings) ? e.column_bindings.length : 0), 0)
        const withBindings = ents.filter(e => Array.isArray(e.column_bindings) && e.column_bindings.length > 0).length

        return (
          <div key={type} className="border border-slate-200 rounded-lg overflow-hidden">
            <button
              onClick={() => setExpandedType(isOpen ? null : type)}
              className="w-full flex items-center justify-between px-4 py-3 bg-slate-50 hover:bg-slate-100 transition-colors"
            >
              <div className="flex items-center gap-3">
                <span className="inline-block bg-orange-100 text-red-700 text-xs font-medium px-2 py-0.5 rounded-full">{type}</span>
                <span className="text-sm text-slate-600">{ents.length} entities</span>
              </div>
              <div className="flex items-center gap-3 text-xs text-slate-500">
                <span>{totalBindings} bindings</span>
                <span>{withBindings}/{ents.length} bound</span>
                <span>{isOpen ? '\u25B2' : '\u25BC'}</span>
              </div>
            </button>
            {isOpen && (
              <div className="p-3 space-y-2 bg-white">
                {ents.map((e, i) => {
                  const bindings = Array.isArray(e.column_bindings) ? e.column_bindings : []
                  const srcCols = Array.isArray(e.source_columns) ? e.source_columns : []
                  return (
                    <div key={i} className="border border-slate-100 rounded p-2">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="text-sm font-medium text-slate-700">{e.entity_name}</span>
                        <span className="text-xs text-slate-400">conf: {Number(e.confidence).toFixed(2)}</span>
                        {e.validated === true || e.validated === 'true' ? (
                          <span className="text-xs text-emerald-600 font-medium">validated</span>
                        ) : null}
                      </div>
                      {bindings.length > 0 ? (
                        <div className="flex flex-wrap gap-1">
                          {bindings.map((b, bi) => (
                            <span key={bi} className="inline-flex items-center gap-1 bg-indigo-50 text-indigo-700 text-xs px-1.5 py-0.5 rounded">
                              <span className="font-medium">{b.attribute_name || '?'}</span>
                              <span className="text-indigo-400">&larr;</span>
                              <span>{shortName(b.bound_column || '')}</span>
                            </span>
                          ))}
                        </div>
                      ) : srcCols.length > 0 ? (
                        <p className="text-xs text-slate-500">Columns: {srcCols.join(', ')}</p>
                      ) : (
                        <p className="text-xs text-slate-400 italic">Table-level entity -- column bindings available after column-level discovery</p>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

export default function Ontology() {
  const [entities, setEntities] = useState([])
  const [summary, setSummary] = useState([])
  const [relationships, setRelationships] = useState([])
  const [metrics, setMetrics] = useState([])
  const [edgeSummary, setEdgeSummary] = useState([])
  const [confDist, setConfDist] = useState([])
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(new Set())
  const [applyResult, setApplyResult] = useState(null)
  const [applying, setApplying] = useState(false)
  const [tab, setTab] = useState('graph')
  const [expanded, setExpanded] = useState(new Set())

  useEffect(() => {
    safeFetch('/api/ontology/entities').then(r => { setEntities(r.data || []); if (r.error) setError(r.error) })
    safeFetch('/api/ontology/summary').then(r => { setSummary(r.data || []); if (r.error) setError(r.error) })
    safeFetch('/api/ontology/relationships').then(r => { setRelationships(r.data || []) })
    safeFetch('/api/ontology/metrics').then(r => { setMetrics(r.data || []) })
    safeFetch('/api/ontology/edge-summary').then(r => { setEdgeSummary(r.data || []) })
    safeFetch('/api/ontology/confidence-distribution').then(r => { setConfDist(r.data || []) })
  }, [])

  const groupedEntities = useMemo(() => {
    const map = new Map()
    entities.forEach(e => {
      const st = Array.isArray(e.source_tables) ? e.source_tables : (typeof e.source_tables === 'string' ? (() => { try { return JSON.parse(e.source_tables) } catch { return [e.source_tables] } })() : [])
      const key = `${e.entity_type}::${st.sort().join(',')}`
      const existing = map.get(key)
      if (!existing || (Number(e.confidence) || 0) > (Number(existing.confidence) || 0)) {
        map.set(key, { ...e, source_tables: st, _count: (existing?._count || 0) + 1 })
      } else {
        map.set(key, { ...existing, _count: existing._count + 1 })
      }
    })
    return Array.from(map.values())
  }, [entities])

  const toggle = (i) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(i)) next.delete(i); else next.add(i)
      return next
    })
  }

  const sourceTablesList = (e) => {
    const st = e.source_tables
    if (Array.isArray(st)) return st
    if (typeof st === 'string') {
      try { const parsed = JSON.parse(st); return Array.isArray(parsed) ? parsed : [st] } catch { return [st] }
    }
    return []
  }

  const applyToTable = async () => {
    if (selected.size === 0) return
    setApplying(true)
    setApplyResult(null)
    const selections = []
    selected.forEach(i => {
      const e = entities[i]
      const tables = sourceTablesList(e)
      if (e?.entity_type && tables.length) selections.push({ entity_type: e.entity_type, source_tables: tables })
    })
    if (!selections.length) { setApplying(false); return }
    try {
      const r = await fetch('/api/ontology/apply-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ selections }) })
      const j = await r.json().catch(() => ({}))
      setApplyResult(r.ok ? j : { error: j.detail || j })
    } catch (e) { setApplyResult({ error: e.message }) }
    setApplying(false)
  }

  const tabs = [
    { key: 'graph', label: 'Relationship Graph' },
    { key: 'charts', label: 'Health & Charts' },
    { key: 'heatmap', label: 'Coverage Heatmap' },
    { key: 'bindings', label: 'Binding Detail' },
    { key: 'table', label: 'Entity Table' },
  ]

  return (
    <div className="space-y-6">
      <ErrorBanner error={error} />

      {/* Health cards */}
      <section className="bg-dbx-oat-light rounded-xl border border-slate-200 p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-800 mb-4">Ontology Health</h2>
        <HealthCards metrics={metrics} />
        {!metrics.length && (
          <p className="text-sm text-slate-400">No metrics computed yet. Run the ontology pipeline to generate health metrics.</p>
        )}
      </section>

      {/* Entity Type Summary */}
      <section className="bg-dbx-oat-light rounded-xl border border-slate-200 p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-800 mb-4">Entity Type Summary</h2>
        {summary.length === 0
          ? <p className="text-sm text-slate-400">No entity types discovered yet. Run the full analytics pipeline first.</p>
          : <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {summary.map((s, i) => {
                const relCount = (relationships || []).filter(r => r.src === s.entity_type || r.dst === s.entity_type).length
                return (
                  <div key={i} className="border border-slate-200 rounded-xl p-4 bg-gradient-to-br from-white to-slate-50">
                    <p className="font-semibold text-sm text-slate-700">{s.entity_type}</p>
                    <p className="text-3xl font-bold text-red-700 mt-1">{s.count}</p>
                    <p className="text-xs text-slate-400 mt-1">Avg conf: {s.avg_confidence} | Validated: {s.validated}{relCount > 0 ? ` | Rels: ${relCount}` : ''}</p>
                  </div>
                )
              })}
            </div>
        }
      </section>

      {/* Tabbed visualization */}
      <section className="bg-dbx-oat-light rounded-xl border border-slate-200 p-6 shadow-sm">
        <div className="flex items-center gap-1 mb-4 border-b border-slate-200 pb-2 overflow-x-auto">
          {tabs.map(t => (
            <button key={t.key} onClick={() => setTab(t.key)}
              className={`px-4 py-1.5 text-sm font-medium rounded-t-lg transition-colors whitespace-nowrap ${
                tab === t.key ? 'bg-dbx-navy text-white' : 'text-slate-500 hover:text-slate-700 hover:bg-dbx-oat'
              }`}>
              {t.label}
            </button>
          ))}
        </div>

        {tab === 'graph' && (
          <div>
            <p className="text-xs text-slate-500 mb-3">
              Nodes = entity types (sized by table count). Click a node to see tables linked via instance_of.
            </p>
            <EntityGraph entities={entities} relationships={relationships} allRelationships={relationships} />
          </div>
        )}

        {tab === 'charts' && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h3 className="text-sm font-semibold text-slate-700 mb-2">Confidence Distribution</h3>
              <p className="text-xs text-slate-500 mb-3">How entity confidence scores are distributed across bands.</p>
              <ConfidenceChart data={confDist} />
            </div>
            <div>
              <h3 className="text-sm font-semibold text-slate-700 mb-2">Edge Type Breakdown</h3>
              <p className="text-xs text-slate-500 mb-3">Ontology-specific relationship edge counts.</p>
              <EdgeBreakdownChart data={edgeSummary} />
            </div>
          </div>
        )}

        {tab === 'heatmap' && (
          <div>
            <p className="text-xs text-slate-500 mb-3">
              Each cell shows the confidence of an entity type's presence on a source table. Darker green = higher confidence.
            </p>
            <CoverageHeatmap entities={entities} />
          </div>
        )}

        {tab === 'bindings' && (
          <div>
            <p className="text-xs text-slate-500 mb-3">
              Column-to-attribute mappings per entity type. Expand to see which columns are bound to entity attributes.
            </p>
            <BindingDetail entities={entities} />
          </div>
        )}

        {tab === 'table' && (
          <div>
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-base font-semibold text-slate-800">Discovered Entities</h3>
              {selected.size > 0 && (
                <button onClick={applyToTable} disabled={applying}
                  className="px-4 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 disabled:opacity-50">
                  Apply tags ({selected.size})
                </button>
              )}
            </div>
            {applyResult && (
              <div className={`mb-4 text-sm ${applyResult.error ? 'text-red-600' : 'text-green-600'}`}>
                {applyResult.error ? JSON.stringify(applyResult.error) : `Applied: ${(applyResult.results || []).filter(r => r.ok).length} ok, ${(applyResult.results || []).filter(r => !r.ok).length} failed.`}
              </div>
            )}
            {groupedEntities.length === 0
              ? <p className="text-sm text-slate-400">No entities discovered yet.</p>
              : <div className="overflow-x-auto">
                  <table className="min-w-full text-sm">
                    <thead><tr>
                      <th className="w-10 px-2 py-2.5 bg-dbx-oat border-b border-slate-200"></th>
                      {['Entity', 'Type', 'Conf', 'Validated', 'Source Tables', 'Columns / Bindings'].map(h =>
                        <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
                    </tr></thead>
                    <tbody>
                      {groupedEntities.map((e, i) => {
                        const bindings = Array.isArray(e.column_bindings) ? e.column_bindings : []
                        const srcCols = Array.isArray(e.source_columns) ? e.source_columns : []
                        const isExpanded = expanded.has(i)
                        const hasDetail = bindings.length > 0 || srcCols.length > 0
                        return (
                          <React.Fragment key={i}>
                            <tr className={`border-b border-slate-100 hover:bg-orange-50/30 transition-colors ${selected.has(i) ? 'bg-orange-50/50' : ''}`}>
                              <td className="px-2 py-2">
                                <input type="checkbox" checked={selected.has(i)} onChange={() => toggle(i)} className="rounded border-slate-300" />
                              </td>
                              <td className="px-3 py-2 font-medium text-slate-700">{e.entity_name}</td>
                              <td className="px-3 py-2">
                                <span className="inline-block bg-orange-100 text-red-700 text-xs font-medium px-2 py-0.5 rounded-full">{e.entity_type}</span>
                              </td>
                              <td className="px-3 py-2 text-slate-600">{Number(e.confidence).toFixed(2)}</td>
                              <td className="px-3 py-2">
                                {e.validated === 'true' || e.validated === true
                                  ? <span className="text-emerald-600 font-medium">Yes</span>
                                  : <span className="text-slate-400">No</span>}
                              </td>
                              <td className="px-3 py-2 max-w-xs truncate text-slate-500">{Array.isArray(e.source_tables) ? e.source_tables.map(shortName).join(', ') : String(e.source_tables ?? '')}</td>
                              <td className="px-3 py-2">
                                {hasDetail ? (
                                  <button onClick={() => setExpanded(prev => { const n = new Set(prev); n.has(i) ? n.delete(i) : n.add(i); return n })}
                                    className="text-xs text-blue-600 hover:underline">
                                    {bindings.length > 0 ? `${bindings.length} mappings` : `${srcCols.length} cols`}{isExpanded ? ' (hide)' : ' (show)'}
                                  </button>
                                ) : <span className="text-xs text-slate-300">--</span>}
                              </td>
                            </tr>
                            {isExpanded && hasDetail && (
                              <tr className="bg-slate-50/60">
                                <td></td>
                                <td colSpan={6} className="px-3 py-2">
                                  {bindings.length > 0 && (
                                    <div className="mb-1">
                                      <span className="text-xs font-semibold text-slate-500 mr-2">Attribute mappings:</span>
                                      <span className="flex flex-wrap gap-1 mt-0.5">
                                        {bindings.map((b, bi) => (
                                          <span key={bi} className="inline-block bg-indigo-50 text-indigo-700 text-xs px-1.5 py-0.5 rounded">
                                            {b.attribute_name || '?'} &larr; {shortName(b.bound_column || '')}
                                          </span>
                                        ))}
                                      </span>
                                    </div>
                                  )}
                                  {srcCols.length > 0 && bindings.length === 0 && (
                                    <div>
                                      <span className="text-xs font-semibold text-slate-500 mr-2">Source columns:</span>
                                      <span className="text-xs text-slate-600">{srcCols.join(', ')}</span>
                                    </div>
                                  )}
                                </td>
                              </tr>
                            )}
                          </React.Fragment>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
            }
          </div>
        )}
      </section>
    </div>
  )
}
