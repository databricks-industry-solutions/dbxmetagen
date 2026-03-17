import React, { useRef, useEffect, useCallback, useMemo, useState } from 'react'
import ForceGraph2D from 'react-force-graph-2d'

const NODE_COLORS = { table: '#3b82f6', column: '#10b981', schema: '#f59e0b', entity: '#8b5cf6' }
const EDGE_DASH = {
  predicted_fk: null, references: null, derives_from: null,
  similar_embedding: [4, 4], same_domain: [2, 3], same_subdomain: [2, 3],
  same_catalog: [2, 3], same_schema: [2, 3], same_security_level: [1, 4],
  same_classification: [1, 4], instance_of: [6, 3], has_attribute: [6, 3],
}
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
  const gLinks = (edges || [])
    .filter(e => nodeIds.has(e.src) && nodeIds.has(e.dst))
    .map(e => ({
      source: e.src, target: e.dst,
      rel: e.relationship || e.edge_type || '',
      weight: Math.min(1, Math.max(0.1, Number(e.weight) || 0.5)),
      sourceSystem: e.source_system || '',
      highlighted: hl ? (hl.has(e.src) && hl.has(e.dst)) : true,
    }))

  return { nodes: gNodes, links: gLinks }
}

export default function GraphSubgraph({
  nodes, edges, collapsedColumns, highlightedNodes, startNode,
  height = 380, onNodeClick, onNodeExpand,
}) {
  const graphRef = useRef()
  const containerRef = useRef()
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

  const paintNode = useCallback((node, ctx) => {
    const r = node.nodeType === 'table' ? 8 : 5
    const alpha = node.highlighted ? 1.0 : 0.2
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

    ctx.font = `bold ${node.nodeType === 'table' ? 10 : 8}px Inter, system-ui, sans-serif`
    ctx.textAlign = 'center'
    const isDark = document.documentElement.classList.contains('dark')
    ctx.fillStyle = isDark ? '#f8fafc' : '#0f172a'
    ctx.fillText(node.label, node.x, node.y + r + 11)

    if (node.collapsed > 0 && hovered === node.id) {
      const badge = `+${node.collapsed} cols`
      ctx.font = '7px Inter, system-ui, sans-serif'
      const bw = ctx.measureText(badge).width + 6
      ctx.fillStyle = isDark ? '#334155' : '#e2e8f0'
      ctx.beginPath()
      ctx.roundRect(node.x - bw / 2, node.y + r + 14, bw, 12, 3)
      ctx.fill()
      ctx.fillStyle = isDark ? '#94a3b8' : '#64748b'
      ctx.fillText(badge, node.x, node.y + r + 23)
    }
    ctx.globalAlpha = 1.0
  }, [hovered, startNode])

  const paintLink = useCallback((link, ctx) => {
    const srcId = typeof link.source === 'object' ? link.source.id : link.source
    const tgtId = typeof link.target === 'object' ? link.target.id : link.target
    const isAdjacentToHover = hovered && (srcId === hovered || tgtId === hovered)
    const alpha = link.highlighted ? Math.max(0.3, link.weight) : 0.08
    ctx.globalAlpha = isAdjacentToHover ? 1.0 : alpha
    ctx.strokeStyle = isAdjacentToHover ? '#4f46e5' : (link.highlighted ? '#6366f1' : '#cbd5e1')
    ctx.lineWidth = isAdjacentToHover ? Math.max(1.5, link.weight * 4) : Math.max(0.5, link.weight * 3)
    const dash = EDGE_DASH[link.rel] || EDGE_DASH[link.sourceSystem] || null
    if (dash) ctx.setLineDash(dash)
    else ctx.setLineDash([])
    ctx.beginPath()
    ctx.moveTo(link.source.x, link.source.y)
    ctx.lineTo(link.target.x, link.target.y)
    ctx.stroke()
    ctx.setLineDash([])

    if (isAdjacentToHover && link.rel) {
      const mx = (link.source.x + link.target.x) / 2
      const my = (link.source.y + link.target.y) / 2
      ctx.font = 'bold 8px Inter, system-ui, sans-serif'
      ctx.textAlign = 'center'
      ctx.fillStyle = '#312e81'
      ctx.fillText(link.rel, mx, my - 4)
    }
    ctx.globalAlpha = 1.0
  }, [hovered])

  const handleClick = useCallback(node => {
    if (node.collapsed > 0 && onNodeExpand) onNodeExpand(node.id)
    else if (onNodeClick) onNodeClick(node.id)
  }, [onNodeClick, onNodeExpand])

  if (!graphData.nodes.length) {
    return <div className="flex items-center justify-center h-32 text-sm text-slate-400">No graph data to display.</div>
  }

  return (
    <div ref={containerRef}>
      <div className="rounded-xl overflow-hidden bg-dbx-oat-light dark:bg-dbx-navy-700" style={{ height }}>
        <ForceGraph2D
          ref={graphRef}
          graphData={graphData}
          width={width}
          height={height}
          nodeCanvasObject={paintNode}
          linkCanvasObject={paintLink}
          onNodeHover={n => setHovered(n?.id || null)}
          onNodeClick={handleClick}
          nodeLabel={n => `${n.id}\n${n.domain ? 'Domain: ' + n.domain : ''}${n.sensitivity ? ' | ' + n.sensitivity : ''}${n.desc ? '\n' + n.desc : ''}`}
          linkLabel={l => `${l.rel} (${l.weight.toFixed(2)})`}
          cooldownTicks={80}
          d3VelocityDecay={0.3}
        />
      </div>
    </div>
  )
}
