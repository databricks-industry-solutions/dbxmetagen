import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { ErrorBanner } from '../App'
import { cachedFetch, TTL } from '../apiCache'
import { PageHeader, EmptyState, SkeletonCards } from './ui'

const PALETTE = [
  '#FF3621', '#0B2026', '#6366f1', '#10b981', '#f59e0b',
  '#3b82f6', '#ec4899', '#8b5cf6', '#14b8a6', '#f97316',
]
const shortName = id => (id || '').split('.').pop()

export function HealthCards({ metrics }) {
  const m = useMemo(() => {
    const map = {}
    ;(metrics || []).forEach(r => { map[r.metric_name] = r.value })
    return map
  }, [metrics])

  const lastUpdated = useMemo(() => {
    const dates = (metrics || []).map(r => r.updated_at).filter(Boolean).sort().reverse()
    if (!dates.length) return null
    try { return new Date(dates[0]).toLocaleString() } catch { return dates[0] }
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
    <div>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        {cards.map((c, i) => (
          <div key={i} className="border border-slate-200 dark:border-dbx-navy-400/25 rounded-xl p-4 bg-gradient-to-br from-white to-slate-50 dark:from-dbx-navy-650 dark:to-dbx-navy-700">
            <p className="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wider">{c.label}</p>
            <p className={`text-2xl font-bold mt-1 ${c.warn ? 'text-dbx-amber' : 'text-dbx-lava'}`}>{c.value ?? '--'}</p>
            {c.sub && <p className="text-xs text-slate-400 mt-0.5">{c.sub}</p>}
          </div>
        ))}
      </div>
      {lastUpdated && (
        <p className="text-[10px] text-slate-400 dark:text-slate-500 mt-2 text-right">Pre-computed metrics, last updated: {lastUpdated}</p>
      )}
    </div>
  )
}

export function EntityGraph({ entities, relationships, allRelationships }) {
  const graphRef = useRef()
  const containerRef = useRef()
  const [hovered, setHovered] = useState(null)
  const [selectedType, setSelectedType] = useState(null)
  const [containerWidth, setContainerWidth] = useState(800)

  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const ro = new ResizeObserver(entries => {
      for (const entry of entries) setContainerWidth(entry.contentRect.width)
    })
    ro.observe(el)
    setContainerWidth(el.offsetWidth)
    return () => ro.disconnect()
  }, [])

  const instanceOfLinks = useMemo(() => {
    if (!selectedType || !allRelationships) return []
    return allRelationships
      .filter(r => (r.relationship_name || r.relationship) === 'instance_of')
      .filter(r => {
        const matchingEntities = entities.filter(e => e.entity_type === selectedType)
        return matchingEntities.some(e => e.entity_id === (r.dst_entity_type || r.dst))
      })
      .map(r => shortName(r.src_entity_type || r.src))
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
      .filter(r => {
        const s = r.src_entity_type || r.src
        const d = r.dst_entity_type || r.dst
        return nodeIds.has(s) && nodeIds.has(d) && s !== d
      })
      .map(r => ({
        source: r.src_entity_type || r.src,
        target: r.dst_entity_type || r.dst,
        label: r.relationship_name || r.relationship || '',
        weight: Number(r.weight || r.confidence) || 0.5,
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
    const isDark = document.documentElement.classList.contains('dark')
    ctx.fillStyle = isDark ? '#e2e8f0' : '#1e293b'
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
    <div ref={containerRef}>
      <div className="border border-slate-200 dark:border-dbx-navy-400/30 rounded-xl overflow-hidden bg-dbx-oat-light dark:bg-dbx-navy-700" style={{ height: 420 }}>
        <ForceGraph2D
          ref={graphRef}
          graphData={graphData}
          width={containerWidth}
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

export function OntologyOverview() {
  const [entities, setEntities] = useState([])
  const [summary, setSummary] = useState([])
  const [relationships, setRelationships] = useState([])
  const [metrics, setMetrics] = useState([])
  const [error, setError] = useState(null)

  useEffect(() => {
    cachedFetch('/api/ontology/entities', {}, TTL.DASHBOARD).then(r => { setEntities(r.data || []); if (r.error) setError(r.error) })
    cachedFetch('/api/ontology/summary', {}, TTL.DASHBOARD).then(r => { setSummary(r.data || []); if (r.error) setError(r.error) })
    cachedFetch('/api/ontology/relationships', {}, TTL.DASHBOARD).then(r => { setRelationships(r.data || []) })
    cachedFetch('/api/ontology/metrics', {}, TTL.DASHBOARD).then(r => { setMetrics(r.data || []) })
  }, [])

  return (
    <div className="space-y-6">
      <PageHeader title="Ontology" subtitle="Entity types and relationships" />
      <ErrorBanner error={error} />
      <section className="card p-6">
        <h2 className="heading-section mb-4">Ontology Health</h2>
        <HealthCards metrics={metrics} />
        {!metrics.length && <EmptyState title="No metrics computed yet" description="Run the ontology pipeline to generate health metrics" />}
      </section>
      <section className="card p-6">
        <h2 className="heading-section mb-4">Entity Type Summary</h2>
        {summary.length === 0
          ? <EmptyState title="No entity types discovered yet" description="Run the full analytics pipeline first" />
          : <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {summary.map((s, i) => {
                const relCount = (relationships || []).filter(r => (r.src_entity_type || r.src) === s.entity_type || (r.dst_entity_type || r.dst) === s.entity_type).length
                return (
                  <div key={i} className="border border-slate-200 dark:border-dbx-navy-400/30 rounded-xl p-4 bg-gradient-to-br from-white to-slate-50 dark:from-dbx-navy-650 dark:to-dbx-navy-700">
                    <p className="font-semibold text-sm text-slate-700 dark:text-slate-200">{s.entity_type}</p>
                    <p className="text-3xl font-bold text-dbx-lava mt-1">{s.count}</p>
                    <p className="text-xs text-slate-400 mt-1">Avg conf: {s.avg_confidence} | Validated: {s.validated}{relCount > 0 ? ` | Rels: ${relCount}` : ''}</p>
                  </div>
                )
              })}
            </div>
        }
      </section>
      <section className="card p-6">
        <h2 className="heading-section mb-4">Per-Table Entity Predictions</h2>
        {entities.length === 0
          ? <p className="text-sm text-slate-400">No entity predictions yet. Run the ontology pipeline first.</p>
          : <div className="overflow-x-auto max-h-96">
              <table className="min-w-full text-sm">
                <thead><tr>
                  {['Table', 'Entity', 'Type', 'Confidence', 'Validated'].map(h =>
                    <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat dark:bg-dbx-navy-500 font-semibold text-slate-600 dark:text-slate-300 border-b border-slate-200 dark:border-dbx-navy-400/30 text-xs uppercase tracking-wider">{h}</th>)}
                </tr></thead>
                <tbody>
                  {entities.slice(0, 100).map((e, i) => {
                    const tables = Array.isArray(e.source_tables) ? e.source_tables : (typeof e.source_tables === 'string' ? (() => { try { return JSON.parse(e.source_tables) } catch { return [e.source_tables] } })() : [])
                    return (
                      <tr key={i} className="border-b border-slate-100 dark:border-dbx-navy-500/50 hover:bg-orange-50/30 dark:hover:bg-dbx-navy-500/30 transition-colors">
                        <td className="px-3 py-2 font-mono text-xs text-slate-600 dark:text-slate-300 max-w-xs truncate">{tables.map(shortName).join(', ') || '--'}</td>
                        <td className="px-3 py-2 font-medium text-slate-700 dark:text-slate-200">{e.entity_name}</td>
                        <td className="px-3 py-2"><span className="badge bg-orange-100 text-dbx-lava dark:bg-dbx-lava/20 dark:text-dbx-lava-light">{e.entity_type}</span></td>
                        <td className="px-3 py-2 text-slate-600 dark:text-slate-300 tabular-nums">{Number(e.confidence).toFixed(2)}</td>
                        <td className="px-3 py-2">{(e.validated === 'true' || e.validated === true) ? <span className="text-dbx-green font-medium">Yes</span> : <span className="text-slate-400">No</span>}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
              {entities.length > 100 && <p className="text-xs text-slate-400 mt-2 px-3">Showing first 100 of {entities.length} entities.</p>}
            </div>
        }
      </section>
      <section className="card p-6">
        <h2 className="heading-section mb-4">Entity Relationship Graph</h2>
        <p className="text-xs text-slate-500 mb-3">Nodes = entity types (sized by table count). Click a node to see tables linked via instance_of.</p>
        <EntityGraph entities={entities} relationships={relationships} allRelationships={relationships} />
      </section>
    </div>
  )
}

export default function Ontology() {
  const [entities, setEntities] = useState([])
  const [summary, setSummary] = useState([])
  const [relationships, setRelationships] = useState([])
  const [metrics, setMetrics] = useState([])
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(new Set())
  const [applyResult, setApplyResult] = useState(null)
  const [applying, setApplying] = useState(false)
  const [tab, setTab] = useState('graph')
  const [expanded, setExpanded] = useState(new Set())

  useEffect(() => {
    cachedFetch('/api/ontology/entities', {}, TTL.DASHBOARD).then(r => { setEntities(r.data || []); if (r.error) setError(r.error) })
    cachedFetch('/api/ontology/summary', {}, TTL.DASHBOARD).then(r => { setSummary(r.data || []); if (r.error) setError(r.error) })
    cachedFetch('/api/ontology/relationships', {}, TTL.DASHBOARD).then(r => { setRelationships(r.data || []) })
    cachedFetch('/api/ontology/metrics', {}, TTL.DASHBOARD).then(r => { setMetrics(r.data || []) })
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
    { key: 'table', label: 'Entity Table' },
  ]

  return (
    <div className="space-y-6">
      <PageHeader title="Ontology" subtitle="Entity types and relationships" />
      <ErrorBanner error={error} />

      {/* Health cards */}
      <section className="card p-6">
        <h2 className="heading-section mb-4">Ontology Health</h2>
        <HealthCards metrics={metrics} />
        {!metrics.length && (
          <EmptyState title="No metrics computed yet" description="Run the ontology pipeline to generate health metrics" />
        )}
      </section>

      {/* Entity Type Summary */}
      <section className="card p-6">
        <h2 className="heading-section mb-4">Entity Type Summary</h2>
        {summary.length === 0
          ? <EmptyState title="No entity types discovered yet" description="Run the full analytics pipeline first" />
          : <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {summary.map((s, i) => {
                const relCount = (relationships || []).filter(r => (r.src_entity_type || r.src) === s.entity_type || (r.dst_entity_type || r.dst) === s.entity_type).length
                return (
                  <div key={i} className="border border-slate-200 dark:border-dbx-navy-400/30 rounded-xl p-4 bg-gradient-to-br from-white to-slate-50 dark:from-dbx-navy-650 dark:to-dbx-navy-700">
                    <p className="font-semibold text-sm text-slate-700 dark:text-slate-200">{s.entity_type}</p>
                    <p className="text-3xl font-bold text-dbx-lava mt-1">{s.count}</p>
                    <p className="text-xs text-slate-400 mt-1">Avg conf: {s.avg_confidence} | Validated: {s.validated}{relCount > 0 ? ` | Rels: ${relCount}` : ''}</p>
                  </div>
                )
              })}
            </div>
        }
      </section>

      {/* Tabbed visualization */}
      <section className="card p-6">
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
