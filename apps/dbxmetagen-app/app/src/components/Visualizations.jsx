import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { safeFetch, safeFetchObj, ErrorBanner } from '../App'

const CLUSTER_PALETTE = [
  '#6366f1', '#ec4899', '#10b981', '#f59e0b', '#3b82f6',
  '#ef4444', '#8b5cf6', '#14b8a6', '#f97316', '#06b6d4',
]
const shortName = id => (id || '').split('.').pop()

// ---------------------------------------------------------------------------
// 1. Domain Hierarchy
// ---------------------------------------------------------------------------
function DomainHierarchyViz() {
  const [nodes, setNodes] = useState([])
  const [error, setError] = useState(null)

  useEffect(() => {
    safeFetch('/api/graph/nodes?limit=1000').then(r => {
      setNodes(r.data); if (r.error) setError(r.error)
    })
  }, [])

  const tree = useMemo(() => {
    // Group: domain -> schemas -> tables
    const domains = {}
    const schemas = nodes.filter(n => n.node_type === 'schema')
    const tables = nodes.filter(n => n.node_type === 'table')

    schemas.forEach(s => {
      const d = s.domain || 'unknown'
      if (!domains[d]) domains[d] = {}
      domains[d][s.id] = []
    })

    tables.forEach(t => {
      // Derive parent schema from id: catalog.schema.table -> catalog.schema
      const parts = (t.id || '').split('.')
      const parentSchema = parts.length >= 3 ? parts.slice(0, 2).join('.') : null
      const d = t.domain || 'unknown'
      if (!domains[d]) domains[d] = {}
      if (parentSchema && domains[d][parentSchema]) {
        domains[d][parentSchema].push(t)
      } else if (parentSchema) {
        // schema might not be in this domain -- scan all
        let placed = false
        for (const dd of Object.keys(domains)) {
          if (domains[dd][parentSchema]) { domains[dd][parentSchema].push(t); placed = true; break }
        }
        if (!placed) {
          if (!domains[d][parentSchema]) domains[d][parentSchema] = []
          domains[d][parentSchema].push(t)
        }
      }
    })
    return domains
  }, [nodes])

  return (
    <div className="space-y-2">
      <ErrorBanner error={error} />
      {Object.keys(tree).length === 0
        ? <p className="text-sm text-slate-400">No graph nodes available.</p>
        : Object.entries(tree).sort(([a], [b]) => a.localeCompare(b)).map(([domain, schemas]) => (
          <details key={domain} className="bg-dbx-oat-light border border-slate-200 rounded-lg" open>
            <summary className="px-4 py-2.5 cursor-pointer font-semibold text-sm text-red-700 hover:bg-dbx-oat">
              {domain}
              <span className="ml-2 text-xs text-slate-400 font-normal">
                {Object.keys(schemas).length} schemas, {Object.values(schemas).reduce((s, t) => s + t.length, 0)} tables
              </span>
            </summary>
            <div className="pl-4 pb-2">
              {Object.entries(schemas).sort(([a], [b]) => a.localeCompare(b)).map(([schemaId, tables]) => (
                <details key={schemaId} className="ml-2 border-l-2 border-slate-200 pl-3 my-1">
                  <summary className="cursor-pointer text-sm text-slate-700 hover:text-dbx-lava py-1">
                    {schemaId}
                    <span className="ml-2 text-xs text-slate-400">{tables.length} tables</span>
                  </summary>
                  <ul className="ml-4 text-xs text-slate-600 space-y-0.5 py-1">
                    {tables.map(t => (
                      <li key={t.id} className="flex items-center gap-2">
                        <span className="w-1.5 h-1.5 rounded-full bg-orange-400 inline-block" />
                        {shortName(t.id)}
                        {t.security_level && <span className="text-[10px] bg-dbx-oat text-slate-500 px-1 rounded">{t.security_level}</span>}
                      </li>
                    ))}
                  </ul>
                </details>
              ))}
            </div>
          </details>
        ))
      }
    </div>
  )
}

// ---------------------------------------------------------------------------
// 2. PII / Security Map
// ---------------------------------------------------------------------------
const SEC_COLORS = {
  high: '#ef4444', pii: '#ef4444', phi: '#ef4444',
  medium: '#f59e0b', confidential: '#f59e0b',
  low: '#10b981', public: '#10b981', none: '#10b981',
  unclassified: '#94a3b8',
}
const secColor = level => {
  const l = (level || '').toLowerCase().trim()
  if (!l || l === 'unknown') return SEC_COLORS.unclassified
  return SEC_COLORS[l] || '#94a3b8'
}

function PIIMapViz() {
  const [nodes, setNodes] = useState([])
  const [edges, setEdges] = useState([])
  const [error, setError] = useState(null)

  useEffect(() => {
    safeFetch('/api/graph/nodes?node_type=table&limit=500').then(r => {
      setNodes(r.data); if (r.error) setError(r.error)
    })
    safeFetch('/api/graph/edges?limit=500').then(r => {
      setEdges(r.data); if (r.error) setError(r.error)
    })
  }, [])

  const graphData = useMemo(() => {
    const nodeIds = new Set(nodes.map(n => n.id))
    const gNodes = nodes.map(n => ({
      id: n.id, label: shortName(n.id),
      security_level: n.security_level || 'unknown', domain: n.domain,
    }))
    const SHOW_RELS = new Set(['contains', 'derives_from', 'same_schema', 'same_domain', 'similar_embedding', 'predicted_fk'])
    const links = edges
      .filter(e => nodeIds.has(e.src) && nodeIds.has(e.dst) && SHOW_RELS.has(e.relationship))
      .map(e => ({ source: e.src, target: e.dst, relationship: e.relationship, weight: e.weight }))
    return { nodes: gNodes, links }
  }, [nodes, edges])

  const nodeColor = useCallback(n => secColor(n.security_level), [])

  return (
    <div className="space-y-3">
      <ErrorBanner error={error} />
      {/* Legend */}
      <div className="flex flex-wrap gap-4 text-xs text-slate-500">
        {[['high / pii / phi', '#ef4444'], ['medium / confidential', '#f59e0b'], ['low / none (public)', '#10b981'], ['unclassified', '#94a3b8']].map(([label, color]) => (
          <span key={label} className="flex items-center gap-1.5">
            <span className="inline-block w-3 h-3 rounded-full" style={{ background: color }} />{label}
          </span>
        ))}
      </div>
      {graphData.nodes.length === 0
        ? <p className="text-sm text-slate-400">No table nodes available.</p>
        : <div className="bg-dbx-oat-light border border-slate-200 rounded-xl shadow-sm overflow-hidden" style={{ height: 500 }}>
          <ForceGraph2D
            graphData={graphData} width={900} height={500}
            nodeColor={nodeColor}
            nodeLabel={n => `${n.id}\nSecurity: ${n.security_level || 'unclassified'}\nDomain: ${n.domain || '?'}`}
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
            linkLabel={l => `${l.relationship} (${l.weight})`}
            linkColor={() => '#cbd5e1'}
            linkWidth={1}
            linkDirectionalArrowLength={3} linkDirectionalArrowRelPos={1}
          />
        </div>
      }
    </div>
  )
}

// ---------------------------------------------------------------------------
// 4. Similarity Heatmap (Canvas)
// ---------------------------------------------------------------------------
function SimilarityHeatmapViz() {
  const canvasRef = useRef()
  const tooltipRef = useRef()
  const [edges, setEdges] = useState([])
  const [error, setError] = useState(null)

  useEffect(() => {
    safeFetch('/api/analytics/similarity-edges?min_weight=0.5&limit=500').then(r => {
      setEdges(r.data); if (r.error) setError(r.error)
    })
  }, [])

  const { labels, matrix } = useMemo(() => {
    const idSet = new Set()
    edges.forEach(e => { idSet.add(e.src); idSet.add(e.dst) })
    const labels = [...idSet].sort()
    const idx = {}; labels.forEach((l, i) => { idx[l] = i })
    const matrix = labels.map(() => labels.map(() => 0))
    edges.forEach(e => {
      const i = idx[e.src], j = idx[e.dst]
      if (i !== undefined && j !== undefined) {
        const w = Number(e.weight) || 0
        matrix[i][j] = w; matrix[j][i] = w
      }
    })
    // diagonal = 1
    labels.forEach((_, i) => { matrix[i][i] = 1 })
    return { labels, matrix }
  }, [edges])

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || labels.length === 0) return
    const n = labels.length
    const cell = Math.max(12, Math.min(30, 600 / n))
    const margin = 120
    const w = margin + n * cell
    const h = margin + n * cell
    canvas.width = w; canvas.height = h
    const ctx = canvas.getContext('2d')
    ctx.clearRect(0, 0, w, h)

    // Draw cells
    for (let i = 0; i < n; i++) {
      for (let j = 0; j < n; j++) {
        const v = matrix[i][j]
        const intensity = Math.round(v * 255)
        ctx.fillStyle = `rgb(${255 - intensity * 0.6}, ${255 - intensity * 0.6}, 255)`
        ctx.fillRect(margin + j * cell, margin + i * cell, cell - 1, cell - 1)
      }
    }

    // Labels
    ctx.fillStyle = '#334155'
    ctx.font = `${Math.max(7, Math.min(10, cell * 0.7))}px sans-serif`
    labels.forEach((l, i) => {
      const short = shortName(l)
      // column labels (top, rotated)
      ctx.save()
      ctx.translate(margin + i * cell + cell / 2, margin - 4)
      ctx.rotate(-Math.PI / 4)
      ctx.textAlign = 'left'
      ctx.fillText(short, 0, 0)
      ctx.restore()
      // row labels (left)
      ctx.textAlign = 'right'
      ctx.fillText(short, margin - 4, margin + i * cell + cell / 2 + 3)
    })
  }, [labels, matrix])

  const handleMouse = useCallback((e) => {
    const canvas = canvasRef.current
    const tip = tooltipRef.current
    if (!canvas || !tip || labels.length === 0) return
    const rect = canvas.getBoundingClientRect()
    const x = e.clientX - rect.left, y = e.clientY - rect.top
    const n = labels.length
    const cell = Math.max(12, Math.min(30, 600 / n))
    const margin = 120
    const col = Math.floor((x - margin) / cell)
    const row = Math.floor((y - margin) / cell)
    if (row >= 0 && row < n && col >= 0 && col < n) {
      tip.style.display = 'block'
      tip.style.left = `${e.clientX - rect.left + 12}px`
      tip.style.top = `${e.clientY - rect.top - 10}px`
      tip.textContent = `${shortName(labels[row])} x ${shortName(labels[col])}: ${matrix[row][col].toFixed(3)}`
    } else {
      tip.style.display = 'none'
    }
  }, [labels, matrix])

  return (
    <div className="space-y-3">
      <ErrorBanner error={error} />
      {labels.length === 0
        ? <p className="text-sm text-slate-400">No similarity edges available (min_weight=0.5).</p>
        : <div className="relative bg-dbx-oat-light border border-slate-200 rounded-xl shadow-sm p-4 overflow-auto" style={{ maxHeight: 650 }}>
          <canvas ref={canvasRef} onMouseMove={handleMouse} onMouseLeave={() => { if (tooltipRef.current) tooltipRef.current.style.display = 'none' }}
            style={{ cursor: 'crosshair' }} />
          <div ref={tooltipRef}
            className="absolute bg-slate-800 text-white text-xs px-2 py-1 rounded pointer-events-none"
            style={{ display: 'none' }} />
        </div>
      }
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Visualizations wrapper with sub-tabs
// ---------------------------------------------------------------------------
const VIZ_TABS = [
  ['domain', 'Domain Hierarchy'],
  ['pii', 'PII Map'],
  ['heatmap', 'Similarity Heatmap'],
]

export default function Visualizations() {
  const [view, setView] = useState('domain')

  return (
    <div className="space-y-4">
      <div className="flex gap-2 flex-wrap">
        {VIZ_TABS.map(([k, l]) => (
          <button key={k} onClick={() => setView(k)}
            className={`px-3 py-1.5 text-xs rounded-lg font-medium transition-all ${view === k ? 'bg-purple-600 text-white shadow-sm' : 'bg-dbx-oat text-slate-600 hover:bg-dbx-oat-dark'}`}>
            {l}
          </button>
        ))}
      </div>
      {view === 'domain' && <DomainHierarchyViz />}
      {view === 'pii' && <PIIMapViz />}
      {view === 'heatmap' && <SimilarityHeatmapViz />}
    </div>
  )
}
