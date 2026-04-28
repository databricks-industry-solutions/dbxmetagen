import React, { useState, useCallback, useEffect, useMemo, useRef } from 'react'
import { safeFetch, ErrorBanner } from '../App'
import { PageHeader, EmptyState } from './ui'
import GraphSubgraph from './GraphSubgraph'
import EdgeCatalogViewer from './EdgeCatalogViewer'

const NODE_TYPES = [
  { value: 'table', label: 'Table', color: '#3b82f6' },
  { value: 'column', label: 'Column', color: '#10b981' },
  { value: 'schema', label: 'Schema', color: '#f59e0b' },
  { value: 'entity', label: 'Entity', color: '#8b5cf6' },
]

const EDGE_TYPES = [
  { value: '', label: 'All edges' },
  { value: 'references', label: 'References (FK)' },
  { value: 'same_domain', label: 'Same domain' },
  { value: 'same_subdomain', label: 'Same subdomain' },
  { value: 'same_schema', label: 'Same schema' },
  { value: 'same_catalog', label: 'Same catalog' },
  { value: 'same_security_level', label: 'Same security level' },
  { value: 'similar_embedding', label: 'Similar (embedding)' },
  { value: 'derives_from', label: 'Derives from' },
  { value: 'contains', label: 'Contains' },
  { value: 'instance_of', label: 'Instance of (ontology)' },
  { value: 'has_attribute', label: 'Has attribute (ontology)' },
  { value: 'is_a', label: 'Is-a (ontology)' },
]

export default function GraphExplorer({ initialNode, initialEdgeType }) {
  const [search, setSearch] = useState('')
  const [nodePicker, setNodePicker] = useState([])
  const [pickerLoading, setPickerLoading] = useState(false)
  const [selectedNode, setSelectedNode] = useState(initialNode || '')
  const [maxHops, setMaxHops] = useState(2)
  const [edgeType, setEdgeType] = useState(initialEdgeType || '')
  const [hideContains, setHideContains] = useState(true)
  const [graphResult, setGraphResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [detailNode, setDetailNode] = useState(null)
  const [visibleNodeTypes, setVisibleNodeTypes] = useState(() => new Set(NODE_TYPES.map(t => t.value)))
  const [edgeCatalogOpen, setEdgeCatalogOpen] = useState(false)
  const autoTraversed = useRef(false)

  useEffect(() => {
    if (initialEdgeType != null) setEdgeType(initialEdgeType)
  }, [initialEdgeType])

  const doTraverse = useCallback(async (nodeId) => {
    const node = nodeId || selectedNode
    if (!node) return
    setLoading(true)
    setError(null)
    const params = new URLSearchParams({ start_node: node, max_hops: maxHops, direction: 'both', hide_contains: hideContains })
    if (edgeType) params.set('edge_type', edgeType)
    try {
      const res = await fetch(`/api/graph/traverse?${params}`)
      if (!res.ok) throw new Error(`Error ${res.status}`)
      const data = await res.json()
      setGraphResult(data)
      setSelectedNode(node)
    } catch (e) {
      setError(e.message)
    }
    setLoading(false)
  }, [selectedNode, maxHops, edgeType, hideContains])

  useEffect(() => {
    if (initialNode && !autoTraversed.current && !graphResult) {
      autoTraversed.current = true
      doTraverse(initialNode)
    }
  }, [initialNode, graphResult, doTraverse])

  useEffect(() => {
    if (!search || search.length < 2) { setNodePicker([]); return }
    const t = setTimeout(async () => {
      setPickerLoading(true)
      const { data } = await safeFetch(`/api/graph/nodes?search=${encodeURIComponent(search)}&node_type=table&limit=20`)
      setNodePicker(Array.isArray(data) ? data : [])
      setPickerLoading(false)
    }, 300)
    return () => clearTimeout(t)
  }, [search])

  const handleNodeClick = useCallback((nodeId) => {
    const info = graphResult?.nodes?.[nodeId]
    setDetailNode(info ? { id: nodeId, ...info } : { id: nodeId })
  }, [graphResult])

  const handleNodeExpand = useCallback((nodeId) => {
    setSelectedNode(nodeId)
    doTraverse(nodeId)
  }, [doTraverse])

  const handleViewEdgeInGraph = useCallback((edgeName) => {
    setEdgeType(edgeName)
    if (selectedNode) doTraverse()
  }, [selectedNode, doTraverse])

  const toggleNodeType = useCallback((type) => {
    setVisibleNodeTypes(prev => {
      const next = new Set(prev)
      next.has(type) ? next.delete(type) : next.add(type)
      return next
    })
  }, [])

  const availableNodeTypes = useMemo(() => {
    if (!graphResult?.nodes) return new Set()
    const types = new Set()
    for (const n of Object.values(graphResult.nodes)) types.add(n.node_type || 'table')
    return types
  }, [graphResult])

  const filteredGraph = useMemo(() => {
    if (!graphResult) return null
    const allNodes = graphResult.nodes || {}
    const nodes = {}
    for (const [id, n] of Object.entries(allNodes)) {
      if (visibleNodeTypes.has(n.node_type || 'table')) nodes[id] = n
    }
    const nodeIds = new Set(Object.keys(nodes))
    const edges = (graphResult.edges || []).filter(e => nodeIds.has(e.src) && nodeIds.has(e.dst))
    return { nodes, edges, collapsed_columns: graphResult.collapsed_columns, start_node: graphResult.start_node }
  }, [graphResult, visibleNodeTypes])

  const stats = useMemo(() => {
    if (!filteredGraph) return null
    const { nodes, edges } = filteredGraph
    const types = {}
    for (const e of edges) {
      const r = e.relationship || e.edge_type || 'unknown'
      types[r] = (types[r] || 0) + 1
    }
    return { nodeCount: Object.keys(nodes).length, edgeCount: edges.length, edgeTypes: types, collapsed: filteredGraph.collapsed_columns }
  }, [filteredGraph])

  return (
    <div className="space-y-4">
      <PageHeader
        title="Graph Explorer"
        subtitle="Browse and traverse the knowledge graph interactively"
      />

      <div className="card p-4">
        <div className="flex flex-wrap gap-3 items-end">
          <div className="flex-1 min-w-[220px] relative">
            <label className="section-title mb-1.5 block">Start node</label>
            <input
              type="text"
              value={search || selectedNode}
              onChange={e => { setSearch(e.target.value); setSelectedNode('') }}
              placeholder="Search tables..."
              aria-label="Search graph nodes"
              className="input-base"
            />
            {search && search.length >= 2 && (
              <>
                <div className="fixed inset-0 z-20" onClick={() => { setSearch(''); setNodePicker([]) }} />
                <div className="card absolute z-30 mt-1 w-80 max-h-60 overflow-auto p-0">
                  {pickerLoading && <p className="px-3 py-2 text-xs text-slate-400 animate-pulse">Searching...</p>}
                  {!pickerLoading && nodePicker.length === 0 && <p className="px-3 py-2 text-xs text-slate-400">No results. Build the knowledge graph first.</p>}
                  {nodePicker.map(n => (
                    <button key={n.id} className="w-full text-left px-3 py-2 text-sm hover:bg-dbx-oat dark:hover:bg-dbx-navy-600 truncate"
                      onClick={() => { setSelectedNode(n.id); setSearch(''); setNodePicker([]) }}>
                      <span className="font-medium">{n.display_name || n.id}</span>
                      {n.domain && <span className="ml-2 text-xs text-slate-400">{n.domain}</span>}
                    </button>
                  ))}
                  {nodePicker.length >= 20 && <p className="px-3 py-1.5 text-[10px] text-slate-400 border-t border-slate-200 dark:border-slate-600">Showing first 20 results. Refine your search for more.</p>}
                </div>
              </>
            )}
          </div>

          <div>
            <label className="section-title mb-1.5 block">Hops</label>
            <select value={maxHops} onChange={e => setMaxHops(Number(e.target.value))} className="select-base w-20" aria-label="Max hops">
              {[1, 2, 3, 4].map(h => <option key={h} value={h}>{h}</option>)}
            </select>
          </div>

          <div>
            <label className="section-title mb-1.5 block">Edge filter</label>
            <select value={edgeType} onChange={e => setEdgeType(e.target.value)} className="select-base w-44" aria-label="Edge type filter">
              {EDGE_TYPES.map(et => <option key={et.value} value={et.value}>{et.label}</option>)}
            </select>
          </div>

          {graphResult && availableNodeTypes.size > 1 && (
            <div>
              <label className="section-title mb-1.5 block">Node types</label>
              <div className="flex gap-1">
                {NODE_TYPES.filter(t => availableNodeTypes.has(t.value)).map(t => (
                  <button key={t.value} onClick={() => toggleNodeType(t.value)}
                    className={`px-2 py-1 text-xs rounded-full border transition-colors ${
                      visibleNodeTypes.has(t.value)
                        ? 'border-transparent text-white'
                        : 'border-slate-300 dark:border-slate-600 text-slate-400 bg-transparent'
                    }`}
                    style={visibleNodeTypes.has(t.value) ? { backgroundColor: t.color } : undefined}
                    aria-pressed={visibleNodeTypes.has(t.value)}
                  >
                    {t.label}
                  </button>
                ))}
              </div>
            </div>
          )}

          <label className="flex items-center gap-1.5 text-xs text-slate-600 dark:text-slate-400 cursor-pointer">
            <input type="checkbox" checked={hideContains} onChange={e => setHideContains(e.target.checked)} className="rounded" />
            Collapse columns
          </label>

          <button onClick={() => doTraverse()} disabled={!selectedNode || loading}
            className="btn-primary px-4 py-2 text-sm disabled:opacity-50">
            {loading ? 'Loading...' : 'Traverse'}
          </button>
        </div>
      </div>

      {error && <ErrorBanner error={error} onDismiss={() => setError(null)} />}

      {filteredGraph ? (
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4">
          <div className="lg:col-span-3">
            <div className="card overflow-hidden">
              <GraphSubgraph
                nodes={filteredGraph.nodes}
                edges={filteredGraph.edges}
                collapsedColumns={filteredGraph.collapsed_columns}
                startNode={filteredGraph.start_node}
                height={520}
                onNodeClick={handleNodeClick}
                onNodeExpand={handleNodeExpand}
                showLegend
              />
            </div>
          </div>

          <div className="space-y-3">
            {stats && (
              <div className="card p-3 space-y-2">
                <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">Summary</h4>
                <div className="grid grid-cols-2 gap-2 text-sm">
                  <div><span className="text-slate-500">Nodes</span> <span className="font-semibold">{stats.nodeCount}</span></div>
                  <div><span className="text-slate-500">Edges</span> <span className="font-semibold">{stats.edgeCount}</span></div>
                </div>
                <div className="space-y-1">
                  {Object.entries(stats.edgeTypes).sort((a, b) => b[1] - a[1]).map(([type, count]) => (
                    <div key={type} className="flex justify-between text-xs">
                      <span className="text-slate-600 dark:text-slate-400 truncate">{type}</span>
                      <span className="font-mono font-medium text-slate-700 dark:text-slate-300">{count}</span>
                    </div>
                  ))}
                </div>
                {stats.collapsed && Object.keys(stats.collapsed).length > 0 && (
                  <p className="text-[10px] text-slate-400">{Object.values(stats.collapsed).reduce((a, b) => a + b, 0)} columns collapsed</p>
                )}
              </div>
            )}

            {detailNode && (
              <div className="card p-3 space-y-2">
                <div className="flex items-center justify-between">
                  <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">Node Detail</h4>
                  <button onClick={() => setDetailNode(null)} className="text-xs text-slate-400 hover:text-slate-600" aria-label="Close detail">x</button>
                </div>
                <p className="text-sm font-medium text-slate-800 dark:text-slate-200 break-all">{detailNode.id}</p>
                {detailNode.node_type && <span className="badge text-[10px]">{detailNode.node_type}</span>}
                {detailNode.domain && <p className="text-xs text-slate-500">Domain: {detailNode.domain}</p>}
                {detailNode.sensitivity && <p className="text-xs text-amber-600">Sensitivity: {detailNode.sensitivity}</p>}
                {detailNode.short_description && <p className="text-xs text-slate-600 dark:text-slate-400">{detailNode.short_description}</p>}
                <button onClick={() => { setSelectedNode(detailNode.id); doTraverse(detailNode.id) }}
                  className="text-xs text-dbx-violet hover:underline font-medium">
                  Traverse from here
                </button>
              </div>
            )}
          </div>
        </div>
      ) : (
        !loading && <EmptyState title="No graph loaded" description="Search for a table above and click Traverse to explore the knowledge graph." />
      )}

      <div className="card overflow-hidden mt-4">
        <button
          onClick={() => setEdgeCatalogOpen(o => !o)}
          className="w-full flex items-center justify-between px-4 py-3 text-left hover:bg-slate-50 dark:hover:bg-dbx-navy-500/30 transition-colors"
          aria-expanded={edgeCatalogOpen}
        >
          <h4 className="text-sm font-semibold text-slate-700 dark:text-slate-200">Edge Catalog</h4>
          <span className="text-slate-400 text-sm">{edgeCatalogOpen ? '▼' : '▶'}</span>
        </button>
        {edgeCatalogOpen && (
          <EdgeCatalogViewer
            bundleKey="general"
            onViewInGraph={handleViewEdgeInGraph}
          />
        )}
      </div>
    </div>
  )
}
