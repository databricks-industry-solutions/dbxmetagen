import React, { useRef, useEffect, useCallback, useMemo, useState } from 'react'
import ForceGraph2D from 'react-force-graph-2d'

const NODE_COLORS = { table: '#3b82f6', column: '#10b981', schema: '#f59e0b', entity: '#8b5cf6' }

const EDGE_COLORS = {
  references: '#22c55e', predicted_fk: '#22c55e', derives_from: '#f97316',
  instance_of: '#a855f7', has_attribute: '#a855f7', is_a: '#a855f7', has_property: '#a855f7',
  similar_embedding: '#06b6d4',
  same_domain: '#94a3b8', same_subdomain: '#94a3b8', same_schema: '#94a3b8',
  same_catalog: '#94a3b8', same_security_level: '#94a3b8',
}
const EDGE_DASH = {
  predicted_fk: null, references: null, derives_from: null,
  similar_embedding: [4, 4], same_domain: [2, 3], same_subdomain: [2, 3],
  same_catalog: [2, 3], same_schema: [2, 3], same_security_level: [1, 4],
  instance_of: [6, 3], has_attribute: [6, 3],
}
const DIRECTED_EDGES = new Set(['references', 'predicted_fk', 'derives_from', 'instance_of', 'has_attribute', 'is_a', 'has_property'])

const shortName = id => (id || '').split('.').pop()

function buildGraphData(nodes, edges, collapsedColumns, highlightedNodes) {
  const nodesMap = nodes || {}
  const hl = highlightedNodes ? new Set(highlightedNodes) : null

  const gNodes = Object.entries(nodesMap).map(([id, n]) => ({
    id,
    label: n.display_name || shortName(id),
    nodeType: n.node_type || 'table',
    domain: n.domain,
    sensitivity: n.sensitivity,
    desc: n.short_description,
    highlighted: hl ? hl.has(id) : true,
    collapsed: (collapsedColumns || {})[id] || 0,
  }))

  const nodeIds = new Set(gNodes.map(n => n.id))
  const pairCount = {}
  const gLinks = (edges || [])
    .filter(e => nodeIds.has(e.src) && nodeIds.has(e.dst))
    .map(e => {
      const key = [e.src, e.dst].sort().join('::')
      pairCount[key] = (pairCount[key] || 0) + 1
      return {
        source: e.src, target: e.dst,
        rel: e.relationship || e.edge_type || '',
        weight: Math.min(1, Math.max(0.1, Number(e.weight) || 0.5)),
        sourceSystem: e.source_system || '',
        highlighted: hl ? (hl.has(e.src) && hl.has(e.dst)) : true,
        pairKey: key,
        pairIndex: pairCount[key] - 1,
      }
    })
  for (const l of gLinks) l.pairTotal = pairCount[l.pairKey] || 1

  return { nodes: gNodes, links: gLinks }
}

export default function GraphSubgraph({
  nodes, edges, collapsedColumns, highlightedNodes, startNode,
  height = 380, onNodeClick, onNodeExpand, showLegend = false,
}) {
  const graphRef = useRef()
  const containerRef = useRef()
  const needsFit = useRef(true)
  const [hovered, setHovered] = useState(null)
  const [width, setWidth] = useState(600)

  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const ro = new ResizeObserver(entries => {
      for (const entry of entries) setWidth(entry.contentRect.width)
    })
    ro.observe(el)
    setWidth(el.offsetWidth)
    return () => ro.disconnect()
  }, [])

  const graphData = useMemo(
    () => buildGraphData(nodes, edges, collapsedColumns, highlightedNodes),
    [nodes, edges, collapsedColumns, highlightedNodes],
  )

  const nodeCount = graphData.nodes.length

  useEffect(() => { needsFit.current = true }, [graphData])

  // Adjacency set for hover neighborhood dimming
  const adjacency = useMemo(() => {
    const adj = {}
    for (const l of graphData.links) {
      const s = typeof l.source === 'object' ? l.source.id : l.source
      const t = typeof l.target === 'object' ? l.target.id : l.target
      ;(adj[s] ||= new Set()).add(t)
      ;(adj[t] ||= new Set()).add(s)
    }
    return adj
  }, [graphData])

  // Adaptive force params based on node count
  useEffect(() => {
    const fg = graphRef.current
    if (!fg) return
    const charge = nodeCount > 100 ? -80 : nodeCount > 50 ? -120 : -160
    const dist = nodeCount > 100 ? 50 : nodeCount > 50 ? 70 : 90
    fg.d3Force('charge')?.strength(charge)
    fg.d3Force('link')?.distance(dist)
  }, [graphData, nodeCount])

  const legendTypes = useMemo(() => {
    if (!showLegend) return []
    const seen = new Set()
    for (const n of graphData.nodes) seen.add(n.nodeType)
    return [...seen]
  }, [showLegend, graphData])

  const isNeighbor = useCallback((nodeId) => {
    if (!hovered) return true
    return nodeId === hovered || adjacency[hovered]?.has(nodeId)
  }, [hovered, adjacency])

  const paintNode = useCallback((node, ctx, globalScale) => {
    const r = node.nodeType === 'table' ? 8 : 5
    const inNeighborhood = isNeighbor(node.id)
    const alpha = !node.highlighted ? 0.15 : (hovered && !inNeighborhood ? 0.15 : 1.0)
    const isStart = startNode && node.id === startNode
    const isHover = hovered === node.id

    ctx.globalAlpha = alpha
    ctx.beginPath()
    ctx.arc(node.x, node.y, r, 0, 2 * Math.PI)
    ctx.fillStyle = isStart ? '#FF3621' : (NODE_COLORS[node.nodeType] || '#94a3b8')
    ctx.fill()
    if (isHover || isStart) {
      ctx.strokeStyle = isStart ? '#991b1b' : '#FF3621'
      ctx.lineWidth = 2
      ctx.stroke()
    }

    // Semantic zoom: only show labels when zoomed in enough
    if (globalScale > 0.7 || isHover || isStart) {
      const fontSize = Math.max(8, (node.nodeType === 'table' ? 10 : 8) / Math.max(1, globalScale * 0.8))
      ctx.font = `bold ${fontSize}px Inter, system-ui, sans-serif`
      ctx.textAlign = 'center'
      const isDark = document.documentElement.classList.contains('dark')
      ctx.fillStyle = isDark ? '#f8fafc' : '#0f172a'
      ctx.fillText(node.label, node.x, node.y + r + 11)
    }

    if (node.collapsed > 0 && isHover) {
      const badge = `+${node.collapsed} cols`
      ctx.font = '7px Inter, system-ui, sans-serif'
      const bw = ctx.measureText(badge).width + 6
      const isDark = document.documentElement.classList.contains('dark')
      ctx.fillStyle = isDark ? '#334155' : '#e2e8f0'
      ctx.beginPath()
      ctx.roundRect(node.x - bw / 2, node.y + r + 14, bw, 12, 3)
      ctx.fill()
      ctx.fillStyle = isDark ? '#94a3b8' : '#64748b'
      ctx.fillText(badge, node.x, node.y + r + 23)
    }
    ctx.globalAlpha = 1.0
  }, [hovered, startNode, isNeighbor])

  const paintLink = useCallback((link, ctx) => {
    const srcNode = typeof link.source === 'object' ? link.source : null
    const tgtNode = typeof link.target === 'object' ? link.target : null
    if (!srcNode || !tgtNode) return
    const srcId = srcNode.id
    const tgtId = tgtNode.id
    const isAdjacentToHover = hovered && (srcId === hovered || tgtId === hovered)
    const inNeighborhood = isNeighbor(srcId) && isNeighbor(tgtId)
    const baseColor = EDGE_COLORS[link.rel] || EDGE_COLORS[link.sourceSystem] || '#6366f1'

    const alpha = !link.highlighted ? 0.05
      : (hovered && !inNeighborhood) ? 0.06
      : isAdjacentToHover ? 1.0
      : Math.max(0.25, link.weight)
    ctx.globalAlpha = alpha
    ctx.strokeStyle = isAdjacentToHover ? baseColor : (link.highlighted ? baseColor : '#cbd5e1')
    ctx.lineWidth = isAdjacentToHover ? Math.max(1.5, link.weight * 4) : Math.max(0.5, link.weight * 2.5)

    const dash = EDGE_DASH[link.rel] || EDGE_DASH[link.sourceSystem] || null
    if (dash) ctx.setLineDash(dash)
    else ctx.setLineDash([])

    // Curvature for parallel edges
    const curvature = link.pairTotal > 1
      ? 0.15 * (link.pairIndex - (link.pairTotal - 1) / 2)
      : 0
    const dx = tgtNode.x - srcNode.x
    const dy = tgtNode.y - srcNode.y
    const mx = (srcNode.x + tgtNode.x) / 2 + curvature * -dy
    const my = (srcNode.y + tgtNode.y) / 2 + curvature * dx

    ctx.beginPath()
    if (curvature !== 0) {
      ctx.moveTo(srcNode.x, srcNode.y)
      ctx.quadraticCurveTo(mx, my, tgtNode.x, tgtNode.y)
    } else {
      ctx.moveTo(srcNode.x, srcNode.y)
      ctx.lineTo(tgtNode.x, tgtNode.y)
    }
    ctx.stroke()
    ctx.setLineDash([])

    // Directional arrow for directed edge types
    if (DIRECTED_EDGES.has(link.rel)) {
      const arrowLen = 6
      const dist = Math.sqrt(dx * dx + dy * dy)
      if (dist > 0) {
        const targetR = (tgtNode.nodeType === 'table' ? 8 : 5) + 2
        const ratio = (dist - targetR) / dist
        const ax = srcNode.x + dx * ratio
        const ay = srcNode.y + dy * ratio
        const angle = Math.atan2(dy, dx)
        ctx.beginPath()
        ctx.moveTo(ax, ay)
        ctx.lineTo(ax - arrowLen * Math.cos(angle - Math.PI / 6), ay - arrowLen * Math.sin(angle - Math.PI / 6))
        ctx.lineTo(ax - arrowLen * Math.cos(angle + Math.PI / 6), ay - arrowLen * Math.sin(angle + Math.PI / 6))
        ctx.closePath()
        ctx.fillStyle = ctx.strokeStyle
        ctx.fill()
      }
    }

    // Edge label on hover
    if (isAdjacentToHover && link.rel) {
      ctx.font = 'bold 8px Inter, system-ui, sans-serif'
      ctx.textAlign = 'center'
      const isDark = document.documentElement.classList.contains('dark')
      ctx.fillStyle = isDark ? '#c7d2fe' : '#312e81'
      ctx.fillText(link.rel, mx, my - 4)
    }
    ctx.globalAlpha = 1.0
  }, [hovered, isNeighbor])

  const handleClick = useCallback(node => {
    if (node.collapsed > 0 && onNodeExpand) onNodeExpand(node.id)
    else if (onNodeClick) onNodeClick(node.id)
  }, [onNodeClick, onNodeExpand])

  if (!graphData.nodes.length) {
    return <div className="flex items-center justify-center h-32 text-sm text-slate-400">No graph data to display.</div>
  }

  const warmup = nodeCount > 100 ? 10 : 30

  return (
    <div ref={containerRef}>
      <div className="relative rounded-xl overflow-hidden bg-dbx-oat-light dark:bg-dbx-navy-700" style={{ height }}>
        <ForceGraph2D
          ref={graphRef}
          graphData={graphData}
          width={width}
          height={height}
          nodeCanvasObject={paintNode}
          linkCanvasObject={paintLink}
          onNodeHover={n => setHovered(n?.id || null)}
          onNodeClick={(onNodeClick || onNodeExpand) ? handleClick : undefined}
          nodePointerAreaPaint={(node, color, ctx) => {
            const r = node.nodeType === 'table' ? 10 : 7
            ctx.fillStyle = color
            ctx.beginPath()
            ctx.arc(node.x, node.y, r, 0, 2 * Math.PI)
            ctx.fill()
          }}
          nodeLabel={n => `${n.id}\n${n.domain ? 'Domain: ' + n.domain : ''}${n.sensitivity ? ' | ' + n.sensitivity : ''}${n.desc ? '\n' + n.desc : ''}`}
          linkLabel={l => `${l.rel} (${l.weight.toFixed(2)})`}
          warmupTicks={warmup}
          cooldownTicks={200}
          d3AlphaDecay={0.04}
          d3VelocityDecay={0.3}
          minZoom={0.3}
          maxZoom={8}
          linkCurvature={0}
          onEngineStop={() => {
            if (needsFit.current) {
              graphRef.current?.zoomToFit(400, 40)
              needsFit.current = false
            }
          }}
        />
        {showLegend && legendTypes.length > 0 && (
          <div className="absolute bottom-2 left-2 flex items-center gap-2.5 px-2 py-1 rounded-md bg-white/80 dark:bg-slate-800/80 backdrop-blur-sm pointer-events-none">
            {legendTypes.map(t => (
              <span key={t} className="flex items-center gap-1 text-[10px] text-slate-600 dark:text-slate-300">
                <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: NODE_COLORS[t] || '#94a3b8' }} />
                {t.charAt(0).toUpperCase() + t.slice(1)}
              </span>
            ))}
            {startNode && (
              <span className="flex items-center gap-1 text-[10px] text-slate-600 dark:text-slate-300">
                <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: '#FF3621' }} />
                Start
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
