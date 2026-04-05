import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { ErrorBanner } from '../App'
import { cachedFetch, cachedFetchObj, TTL } from '../apiCache'
import { PageHeader, EmptyState, SkeletonCards } from './ui'

const PALETTE = [
  '#FF3621', '#0B2026', '#6366f1', '#10b981', '#f59e0b',
  '#3b82f6', '#ec4899', '#8b5cf6', '#14b8a6', '#f97316',
]
const shortName = id => (id || '').split('.').pop()

function BundleQualityStrip({ qualitySummary }) {
  if (!qualitySummary) return null
  const bi = qualitySummary.bundle_info
  const fe = qualitySummary.from_entities || {}
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
      <div className="border border-slate-200 dark:border-dbx-navy-400/30 rounded-xl p-4 bg-white dark:bg-dbx-navy-650">
        <p className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Active bundle</p>
        <p className="font-semibold text-slate-800 dark:text-slate-100 mt-0.5">{qualitySummary.active_bundle || '—'}</p>
        {bi?.format_version && <p className="text-xs text-slate-500 mt-1">format_version {bi.format_version}</p>}
        {bi?.tier_index_hash && (
          <p className="text-[10px] font-mono text-slate-400 mt-1 break-all" title="Tier index fingerprint">tier_index_hash {bi.tier_index_hash}</p>
        )}
      </div>
      <div className="border border-slate-200 dark:border-dbx-navy-400/30 rounded-xl p-4 bg-white dark:bg-dbx-navy-650 md:col-span-2">
        <p className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Coverage & confidence (ontology_entities)</p>
        {fe.entity_rows != null ? (
          <div className="flex flex-wrap gap-4 mt-2 text-sm">
            <span><span className="text-slate-500">rows</span> <span className="font-mono font-semibold">{fe.entity_rows}</span></span>
            <span><span className="text-slate-500">avg conf</span> <span className="font-mono font-semibold">{fe.avg_confidence ?? '—'}</span></span>
            <span><span className="text-slate-500">&lt;0.5</span> <span className="font-mono text-amber-600">{fe.below_05 ?? 0}</span></span>
            <span><span className="text-slate-500">&lt;0.6</span> <span className="font-mono text-amber-700">{fe.below_06 ?? 0}</span></span>
          </div>
        ) : (
          <p className="text-xs text-slate-400 mt-1">{qualitySummary.from_entities_error || 'No aggregate yet (table empty or unavailable).'}</p>
        )}
      </div>
    </div>
  )
}

function FormalSemanticsNote() {
  const [open, setOpen] = useState(false)
  return (
    <div className="border border-slate-200 dark:border-dbx-navy-400/25 rounded-lg">
      <button type="button" onClick={() => setOpen(!open)} className="w-full text-left px-3 py-2 text-sm text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-dbx-navy-600/50 rounded-lg">
        What is formalized here? {open ? '−' : '+'}
      </button>
      {open && (
        <div className="px-3 pb-3 text-xs text-slate-600 dark:text-slate-400 space-y-2 border-t border-slate-100 dark:border-dbx-navy-500/30 pt-2">
          <p>dbxmetagen ships an application profile (YAML + tier indexes) over published vocabularies, plus candidate table→class alignments with confidence and provenance. It does not perform OWL DL reasoning or replace Protégé.</p>
          <p className="text-slate-400">See repository docs: <code className="text-[11px]">docs/formal_semantics.md</code></p>
        </div>
      )}
    </div>
  )
}

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
    { label: 'Binding Rate', value: m.binding_completeness_pct ? `${m.binding_completeness_pct}%` : '--', title: 'Percentage of discovered entities that are bound to an ontology type' },
    { label: 'Ontology Edges', value: m.ontology_edges_total || '0', title: 'Total relationships between entity types in the ontology graph' },
    { label: 'Low Confidence', value: m.low_confidence_count || '0', warn: Number(m.low_confidence_count) > 0, title: 'Number of entity classifications below the confidence threshold' },
  ]

  return (
    <div>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        {cards.map((c, i) => (
          <div key={i} className="border border-slate-200 dark:border-dbx-navy-400/25 rounded-xl p-4 bg-gradient-to-br from-white to-slate-50 dark:from-dbx-navy-650 dark:to-dbx-navy-700" title={c.title || ''}>
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
      .filter(r => (r.dst_entity_type || r.dst) === selectedType)
      .map(r => shortName(r.src_entity_type || r.src))
  }, [selectedType, allRelationships])

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
            Tables classified as "{selectedType}":
          </p>
          {instanceOfLinks.length > 0 ? (
            <div className="flex flex-wrap gap-1.5 mt-2">
              {instanceOfLinks.map((t, i) => (
                <span key={i} className="inline-block bg-white text-indigo-700 text-xs px-2 py-0.5 rounded border border-indigo-200">{t}</span>
              ))}
            </div>
          ) : (
            <p className="text-xs text-indigo-500 mt-1">No entity classifications found. Run the ontology pipeline to generate them.</p>
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
                  {['Table', 'Entity', 'Type', 'Confidence', 'Standard', 'Validated'].map(h =>
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
                        <td className="px-3 py-2">
                          {e.source_ontology
                            ? <span className="text-xs px-1.5 py-0.5 rounded bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300">{e.source_ontology}</span>
                            : <span className="text-xs text-slate-300">--</span>}
                        </td>
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
        <p className="text-xs text-slate-500 mb-3">Nodes represent entity types, sized by table count. Click a node to see which tables are classified as that type.</p>
        <EntityGraph entities={entities} relationships={relationships} allRelationships={relationships} />
      </section>
    </div>
  )
}

function SourceStandardsBreakdown({ entities }) {
  const breakdown = useMemo(() => {
    const counts = {}
    ;(entities || []).forEach(e => {
      const src = e.source_ontology || 'Custom / Unlinked'
      counts[src] = (counts[src] || 0) + 1
    })
    return Object.entries(counts).sort((a, b) => b[1] - a[1])
  }, [entities])

  const [downloading, setDownloading] = useState(false)
  const [downloadError, setDownloadError] = useState(null)
  const downloadTurtle = async () => {
    setDownloading(true)
    setDownloadError(null)
    try {
      const r = await fetch('/api/ontology/turtle')
      if (!r.ok) { setDownloadError((await r.json().catch(() => ({}))).error || 'No Turtle file available'); return }
      const blob = await r.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a'); a.href = url; a.download = 'ontology.ttl'; a.click()
      URL.revokeObjectURL(url)
    } catch (e) { setDownloadError('Download failed: ' + e.message) }
    finally { setDownloading(false) }
  }

  if (breakdown.length <= 1 && breakdown[0]?.[0] === 'Custom / Unlinked') return null

  return (
    <div className="flex items-center gap-4 flex-wrap">
      <div className="flex items-center gap-2 flex-wrap">
        {breakdown.map(([src, cnt]) => (
          <span key={src} className={`text-xs px-2 py-1 rounded-full font-medium ${
            src === 'Custom / Unlinked' ? 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300'
              : 'bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300'
          }`}>
            {src}: {cnt}
          </span>
        ))}
      </div>
      <button onClick={downloadTurtle} disabled={downloading}
        className="text-xs px-3 py-1.5 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-dbx-navy-500 disabled:opacity-50">
        {downloading ? 'Downloading...' : 'Export Turtle (.ttl)'}
      </button>
      {downloadError && <span className="text-xs text-red-600 dark:text-red-400">{downloadError}</span>}
    </div>
  )
}

function SourceClassBrowser() {
  const [bundles, setBundles] = useState([])
  const [selectedBundle, setSelectedBundle] = useState('')
  const [classes, setClasses] = useState([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [filter, setFilter] = useState('')
  const [expandedCls, setExpandedCls] = useState(new Set())

  useEffect(() => {
    fetch('/api/ontology/bundles').then(r => r.ok ? r.json() : []).then(setBundles).catch(() => {})
  }, [])

  useEffect(() => {
    if (!selectedBundle) { setClasses([]); setTotal(0); return }
    setLoading(true)
    fetch(`/api/ontology/source-classes/${selectedBundle}?limit=1000`)
      .then(r => r.ok ? r.json() : { classes: [], total: 0 })
      .then(data => { setClasses(data.classes || []); setTotal(data.total || 0) })
      .catch(() => { setClasses([]); setTotal(0) })
      .finally(() => setLoading(false))
  }, [selectedBundle])

  const filtered = useMemo(() => {
    if (!filter) return classes
    const f = filter.toLowerCase()
    return classes.filter(c => c.name.toLowerCase().includes(f) || c.description.toLowerCase().includes(f))
  }, [classes, filter])

  const toggleCls = (name) => {
    setExpandedCls(prev => { const n = new Set(prev); n.has(name) ? n.delete(name) : n.add(name); return n })
  }

  return (
    <div>
      <div className="flex items-center gap-3 mb-4">
        <select value={selectedBundle} onChange={e => setSelectedBundle(e.target.value)}
          className="select-base max-w-xs">
          <option value="">Select a bundle...</option>
          {bundles.filter(b => b.has_tier_indexes).map(b => (
            <option key={b.key} value={b.key}>{b.name} ({b.entity_count} classes)</option>
          ))}
        </select>
        {classes.length > 0 && (
          <input type="text" placeholder="Filter classes..." value={filter} onChange={e => setFilter(e.target.value)}
            className="input-base max-w-xs" />
        )}
        {total > 0 && <span className="text-xs text-slate-400">{filtered.length} / {total} classes</span>}
      </div>
      {loading && <p className="text-sm text-slate-400">Loading...</p>}
      {!loading && filtered.length > 0 && (
        <div className="overflow-y-auto max-h-[600px] space-y-1">
          {filtered.map(c => (
            <div key={c.name} className="border border-slate-200 dark:border-dbx-navy-400/25 rounded-lg">
              <button onClick={() => toggleCls(c.name)}
                className="w-full flex items-center gap-2 px-3 py-2 text-left hover:bg-slate-50 dark:hover:bg-dbx-navy-600/50 transition-colors">
                <span className="text-xs text-slate-400 font-mono">{expandedCls.has(c.name) ? '-' : '+'}</span>
                <span className="text-sm font-medium text-slate-700 dark:text-slate-200">{c.name}</span>
                {c.parents?.length > 0 && (
                  <span className="text-[10px] text-slate-400 ml-1">extends {c.parents.join(', ')}</span>
                )}
                {c.source_ontology && (
                  <span className="ml-auto text-[10px] px-1.5 py-0.5 rounded bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300">{c.source_ontology}</span>
                )}
              </button>
              {expandedCls.has(c.name) && (
                <div className="px-4 pb-3 text-xs space-y-1.5 border-t border-slate-100 dark:border-dbx-navy-500/30 pt-2">
                  <p className="text-slate-600 dark:text-slate-300">{c.description}</p>
                  {c.uri && <p className="text-slate-400 font-mono break-all">{c.uri}</p>}
                  {c.relationships?.length > 0 && (
                    <div className="flex flex-wrap gap-1">
                      <span className="text-slate-500 font-semibold mr-1">Edges:</span>
                      {c.relationships.map(r => (
                        <span key={r} className="bg-orange-50 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300 px-1.5 py-0.5 rounded">{r}</span>
                      ))}
                    </div>
                  )}
                  {c.typical_attributes?.length > 0 && (
                    <div className="flex flex-wrap gap-1">
                      <span className="text-slate-500 font-semibold mr-1">Attributes:</span>
                      {c.typical_attributes.map(a => (
                        <span key={a} className="bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300 px-1.5 py-0.5 rounded">{a}</span>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
      {!loading && selectedBundle && filtered.length === 0 && (
        <p className="text-sm text-slate-400">No classes found. This bundle may not have tier indexes.</p>
      )}
    </div>
  )
}


export default function Ontology() {
  const [entities, setEntities] = useState([])
  const [summary, setSummary] = useState([])
  const [relationships, setRelationships] = useState([])
  const [metrics, setMetrics] = useState([])
  const [qualitySummary, setQualitySummary] = useState(null)
  const [reviewQueue, setReviewQueue] = useState([])
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
    cachedFetchObj('/api/ontology/quality-summary', {}, TTL.DASHBOARD).then(r => {
      if (!r.error) setQualitySummary(r.data)
    })
    cachedFetchObj('/api/ontology/review-queue?limit=25', {}, TTL.DASHBOARD).then(r => {
      if (!r.error && r.data) setReviewQueue(Array.isArray(r.data) ? r.data : [])
    })
  }, [])

  const groupedEntities = useMemo(() => {
    const map = new Map()
    entities.forEach(e => {
      const st = Array.isArray(e.source_tables) ? e.source_tables : (typeof e.source_tables === 'string' ? (() => { try { return JSON.parse(e.source_tables) } catch { return [e.source_tables] } })() : [])
      const key = `${e.entity_type}::${[...st].sort().join(',')}`
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

  const markReviewed = async (entityId) => {
    if (!entityId) return
    try {
      const r = await fetch('/api/ontology/entity-review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ entity_id: entityId, validated: true }),
      })
      if (r.ok) setReviewQueue(q => q.filter(row => row.entity_id !== entityId))
    } catch { /* ignore */ }
  }

  const applyToTable = async () => {
    if (selected.size === 0) return
    setApplying(true)
    setApplyResult(null)
    const selections = []
    selected.forEach(i => {
      const e = groupedEntities[i]
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
    { key: 'browser', label: 'Source Classes' },
  ]

  return (
    <div className="space-y-6">
      <PageHeader title="Ontology" subtitle="Entity types and relationships" />
      <ErrorBanner error={error} />

      <section className="card p-6">
        <h2 className="heading-section mb-4">Bundle &amp; quality</h2>
        <BundleQualityStrip qualitySummary={qualitySummary} />
        <div className="mt-3">
          <FormalSemanticsNote />
        </div>
      </section>

      {reviewQueue.length > 0 && (
        <section className="card p-6 border-amber-200 dark:border-amber-900/40">
          <h2 className="heading-section mb-4">Low-confidence review ({reviewQueue.length})</h2>
          <p className="text-xs text-slate-500 mb-3">Rows with confidence ≤ 0.6. Mark validated after manual check.</p>
          <div className="overflow-x-auto max-h-64">
            <table className="min-w-full text-sm">
              <thead>
                <tr>
                  {['Entity', 'Type', 'Conf', 'Method / URI', ''].map(h => (
                    <th key={h} className="text-left px-3 py-2 bg-dbx-oat dark:bg-dbx-navy-500 text-xs uppercase tracking-wider">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {reviewQueue.map((row, i) => {
                  let attrs = {}
                  try {
                    attrs = typeof row.attributes === 'string' ? JSON.parse(row.attributes) : (row.attributes || {})
                  } catch { attrs = {} }
                  const meth = attrs.discovery_method || '—'
                  const uri = row.entity_uri || '—'
                  return (
                    <tr key={row.entity_id || i} className="border-b border-slate-100 dark:border-dbx-navy-500/50">
                      <td className="px-3 py-2 font-mono text-xs max-w-[200px] truncate">{shortName((row.source_tables && row.source_tables[0]) || '')}</td>
                      <td className="px-3 py-2">{row.entity_type}</td>
                      <td className="px-3 py-2 tabular-nums">{Number(row.confidence).toFixed(2)}</td>
                      <td className="px-3 py-2 text-xs text-slate-500 max-w-xs truncate" title={`${meth} ${uri}`}>{meth} · {String(uri).slice(0, 40)}{String(uri).length > 40 ? '…' : ''}</td>
                      <td className="px-3 py-2">
                        {row.entity_id && (
                          <button type="button" onClick={() => markReviewed(row.entity_id)} className="text-xs px-2 py-1 rounded bg-emerald-600 text-white hover:bg-emerald-700">Validate</button>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Health cards */}
      <section className="card p-6">
        <h2 className="heading-section mb-4">Ontology Health</h2>
        <HealthCards metrics={metrics} />
        {!metrics.length && (
          <EmptyState title="No metrics computed yet" description="Run the ontology pipeline to generate health metrics" />
        )}
        {entities.length > 0 && (
          <div className="mt-4">
            <SourceStandardsBreakdown entities={entities} />
          </div>
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
              Nodes represent entity types, sized by table count. Click a node to see which tables are classified as that type.
            </p>
            <EntityGraph entities={entities} relationships={relationships} allRelationships={relationships} />
          </div>
        )}

        {tab === 'browser' && (
          <div>
            <p className="text-xs text-slate-500 mb-3">
              Browse the source ontology classes from a bundle's tier indexes. Select a bundle to explore its class hierarchy.
            </p>
            <SourceClassBrowser />
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
                      {['Entity', 'Type', 'Conf', 'Standard', 'Validated', 'Source Tables', 'Columns / Bindings'].map(h =>
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
                                {e.source_ontology
                                  ? <span className="text-xs px-1.5 py-0.5 rounded bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300">{e.source_ontology}</span>
                                  : <span className="text-xs text-slate-300">--</span>}
                              </td>
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
                                <td colSpan={7} className="px-3 py-2">
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
