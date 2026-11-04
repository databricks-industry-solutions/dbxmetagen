import React, { useState, useCallback, useEffect, useMemo, useRef } from 'react'
import { safeFetch, ErrorBanner } from '../App'
import { useLanguage } from '../contexts/LanguageContext'
import { PageHeader, EmptyState } from './ui'
import GraphSubgraph from './GraphSubgraph'
import EdgeCatalogViewer from './EdgeCatalogViewer'
import { NODE_COLORS } from '../constants/graphColors'

// Node-type codes; color comes from the shared NODE_COLORS map, label from i18n (gsg.nodeType.*).
const NODE_TYPE_VALUES = ['table', 'column', 'schema', 'entity']

const EDGE_PRESETS = [
  { value: 'references', label: 'ge.preset.references' },
  { value: 'semantic', label: 'ge.preset.semantic', edgeTypes: 'references,predicted_fk,derives_from,instance_of,has_property,similar_embedding' },
  { value: '', label: 'ge.preset.all' },
]

function getEdgeParams(edgeType) {
  const preset = EDGE_PRESETS.find(p => p.value === edgeType && p.edgeTypes)
  if (preset) return { edge_types: preset.edgeTypes }
  if (edgeType) return { edge_type: edgeType }
  return {}
}

export default function GraphExplorer({ initialNode, initialEdgeType }) {
  // `tr` alias -- component uses `t` as a local (timeout handle / node-type) variable.
  const { t: tr } = useLanguage()
  const [search, setSearch] = useState('')
  const [nodePicker, setNodePicker] = useState([])
  const [pickerLoading, setPickerLoading] = useState(false)
  const [selectedNode, setSelectedNode] = useState(initialNode || '')
  const [maxHops, setMaxHops] = useState(2)
  const [edgeType, setEdgeType] = useState(initialEdgeType || 'references')
  const [hideContains, setHideContains] = useState(true)
  const [graphResult, setGraphResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [detailNode, setDetailNode] = useState(null)
  const [visibleNodeTypes, setVisibleNodeTypes] = useState(() => new Set(NODE_TYPE_VALUES))
  const [edgeCatalogOpen, setEdgeCatalogOpen] = useState(false)
  const [edgeFilterPulse, setEdgeFilterPulse] = useState(false)
  const [dynamicEdgeTypes, setDynamicEdgeTypes] = useState([])
  const autoTraversed = useRef(false)
  const edgeFilterRef = useRef(null)
  const graphAreaRef = useRef(null)

  useEffect(() => {
    if (initialEdgeType != null) setEdgeType(initialEdgeType)
  }, [initialEdgeType])

  useEffect(() => {
    (async () => {
      const { data } = await safeFetch('/api/graph/edge-types')
      if (Array.isArray(data) && data.length) setDynamicEdgeTypes(data)
    })()
  }, [])

  const doTraverse = useCallback(async (nodeId, overrideEdgeType) => {
    const node = nodeId || selectedNode
    if (!node) return
    setLoading(true)
    setError(null)
    const params = new URLSearchParams({ start_node: node, max_hops: maxHops, direction: 'both', hide_contains: hideContains, max_nodes: 200 })
    const ep = getEdgeParams(overrideEdgeType ?? edgeType)
    if (ep.edge_type) params.set('edge_type', ep.edge_type)
    if (ep.edge_types) params.set('edge_types', ep.edge_types)
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
      const { data } = await safeFetch(`/api/graph/nodes?search=${encodeURIComponent(search)}&limit=20`)
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
    setEdgeFilterPulse(true)
    setTimeout(() => setEdgeFilterPulse(false), 1200)
    if (selectedNode) {
      doTraverse(null, edgeName)
      graphAreaRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
    }
  }, [selectedNode, doTraverse])

  const toggleNodeType = useCallback((type) => {
    setVisibleNodeTypes(prev => {
      const next = new Set(prev)
      next.has(type) ? next.delete(type) : next.add(type)
      return next
    })
  }, [])

  const nodeTypeCounts = useMemo(() => {
    if (!graphResult?.nodes) return {}
    const counts = {}
    for (const n of Object.values(graphResult.nodes)) {
      const t = n.node_type || 'table'
      counts[t] = (counts[t] || 0) + 1
    }
    return counts
  }, [graphResult])

  const availableNodeTypes = useMemo(() => new Set(Object.keys(nodeTypeCounts)), [nodeTypeCounts])

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
        title={tr('ge.pageTitle')}
        subtitle={tr('ge.pageSubtitle')}
      />

      <div className="card p-4">
        <div className="flex flex-wrap gap-3 items-end">
          <div className="flex-1 min-w-[220px] relative">
            <label className="section-title mb-1.5 block">{tr('ge.startNode')}</label>
            <input
              type="text"
              value={search || selectedNode}
              onChange={e => { setSearch(e.target.value); setSelectedNode('') }}
              placeholder={tr('ge.searchTablesPh')}
              aria-label={tr('ge.searchNodesAria')}
              className="input-base"
            />
            {search && search.length >= 2 && (
              <>
                <div className="fixed inset-0 z-20" onClick={() => { setSearch(''); setNodePicker([]) }} />
                <div className="card absolute z-30 mt-1 w-80 max-h-60 overflow-auto p-0">
                  {pickerLoading && <p className="px-3 py-2 text-xs text-slate-400 animate-pulse">{tr('ge.searching')}</p>}
                  {!pickerLoading && nodePicker.length === 0 && <p className="px-3 py-2 text-xs text-slate-400">{tr('ge.noResults')}</p>}
                  {nodePicker.map(n => (
                    <button key={n.id} className="w-full text-left px-3 py-2 text-sm hover:bg-dbx-oat dark:hover:bg-dbx-navy-600 truncate"
                      onClick={() => { setSelectedNode(n.id); setSearch(''); setNodePicker([]) }}>
                      <span className="font-medium">{n.display_name || n.id}</span>
                      {n.domain && <span className="ml-2 text-xs text-slate-400">{n.domain}</span>}
                    </button>
                  ))}
                  {nodePicker.length >= 20 && <p className="px-3 py-1.5 text-[10px] text-slate-400 border-t border-slate-200 dark:border-slate-600">{tr('ge.showingFirst20')}</p>}
                </div>
              </>
            )}
          </div>

          <div>
            <label className="section-title mb-1.5 block">{tr('ge.hops')}</label>
            <p className="text-[10px] text-slate-400 mb-1">{tr('ge.traversalDepth')}</p>
            <select value={maxHops} onChange={e => setMaxHops(Number(e.target.value))} className="select-base w-20" aria-label="Max hops">
              {[1, 2, 3, 4].map(h => <option key={h} value={h}>{h}</option>)}
            </select>
          </div>

          <div>
            <label className="section-title mb-1.5 block">{tr('ge.edgeFilter')}</label>
            <p className="text-[10px] text-slate-400 mb-1">{tr('ge.relTypeToFollow')}</p>
            <select
              ref={edgeFilterRef}
              value={edgeType}
              onChange={e => setEdgeType(e.target.value)}
              className={`select-base w-52 transition-shadow ${edgeFilterPulse ? 'ring-2 ring-dbx-lava/60' : ''}`}
              aria-label={tr('ge.edgeFilterAria')}
            >
              {EDGE_PRESETS.map(et => <option key={`p-${et.value}`} value={et.value}>{tr(et.label)}</option>)}
              {dynamicEdgeTypes.length > 0 && (
                <optgroup label={tr('ge.individualTypes')}>
                  {dynamicEdgeTypes.map(et => (
                    <option key={et.edge_type} value={et.edge_type}>
                      {et.edge_type} ({et.cnt})
                    </option>
                  ))}
                </optgroup>
              )}
            </select>
          </div>

          <label className="flex items-center gap-1.5 text-xs text-slate-600 dark:text-slate-400 cursor-pointer">
            <input type="checkbox" checked={hideContains} onChange={e => setHideContains(e.target.checked)} className="rounded" />
            Collapse columns
          </label>

          <button onClick={() => doTraverse()} disabled={!selectedNode || loading}
            className="btn-primary px-4 py-2 text-sm disabled:opacity-50">
            {loading ? tr('common.loading') : tr('ge.traverse')}
          </button>
        </div>
      </div>

      {error && <ErrorBanner error={error} onDismiss={() => setError(null)} />}

      {graphResult?.truncated && (
        <div className="rounded-lg border border-amber-200 dark:border-amber-800/40 bg-amber-50 dark:bg-amber-900/20 px-4 py-3 text-sm text-amber-700 dark:text-amber-300">
          {tr('ge.truncatedNote', { shown: graphResult.node_count, total: graphResult.total_nodes })}
        </div>
      )}

      {filteredGraph && availableNodeTypes.size > 0 && (
        <div ref={graphAreaRef} className="flex items-center gap-2 flex-wrap">
          <span className="text-xs font-medium text-slate-500 dark:text-slate-400">{tr('ge.show')}</span>
          {NODE_TYPE_VALUES.filter(v => availableNodeTypes.has(v)).map(v => (
            <button key={v} onClick={() => toggleNodeType(v)}
              className={`px-2.5 py-1 text-xs rounded-full border transition-colors ${
                visibleNodeTypes.has(v)
                  ? 'border-transparent text-white'
                  : 'border-slate-300 dark:border-slate-600 text-slate-400 bg-transparent'
              }`}
              style={visibleNodeTypes.has(v) ? { backgroundColor: NODE_COLORS[v] } : undefined}
              aria-pressed={visibleNodeTypes.has(v)}
            >
              {tr(`gsg.nodeType.${v}`)} ({nodeTypeCounts[v] || 0})
            </button>
          ))}
        </div>
      )}

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
                <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">{tr('ge.summary')}</h4>
                <p className="text-[10px] text-slate-400">{tr('ge.countsForView')}</p>
                <div className="grid grid-cols-2 gap-2 text-sm">
                  <div><span className="text-slate-500">{tr('ge.nodes')}</span> <span className="font-semibold">{stats.nodeCount}</span></div>
                  <div><span className="text-slate-500">{tr('ge.edges')}</span> <span className="font-semibold">{stats.edgeCount}</span></div>
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
                  <p className="text-[10px] text-slate-400">{tr('ge.columnsCollapsed', { n: Object.values(stats.collapsed).reduce((a, b) => a + b, 0) })}</p>
                )}
              </div>
            )}

            {detailNode && (
              <div className="card p-3 space-y-2">
                <div className="flex items-center justify-between">
                  <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">{tr('ge.nodeDetail')}</h4>
                  <button onClick={() => setDetailNode(null)} className="text-xs text-slate-400 hover:text-slate-600" aria-label="Close detail">x</button>
                </div>
                <p className="text-sm font-medium text-slate-800 dark:text-slate-200 break-all">{detailNode.id}</p>
                {detailNode.node_type && <span className="badge text-[10px]">{detailNode.node_type}</span>}
                {detailNode.domain && <p className="text-xs text-slate-500">{tr('ge.domainLabel', { d: detailNode.domain })}</p>}
                {detailNode.sensitivity && <p className="text-xs text-amber-600">{tr('ge.sensitivityLabel', { s: detailNode.sensitivity })}</p>}
                {detailNode.short_description && <p className="text-xs text-slate-600 dark:text-slate-400">{detailNode.short_description}</p>}
                <button onClick={() => { setSelectedNode(detailNode.id); doTraverse(detailNode.id) }}
                  className="text-xs text-dbx-violet hover:underline font-medium">
                  {tr('ge.traverseFromHere')}
                </button>
              </div>
            )}
          </div>
        </div>
      ) : (
        !loading && <EmptyState title={tr('ge.noGraphLoaded')} description={tr('ge.emptyDesc')} />
      )}

      <div className="card overflow-hidden mt-4">
        <button
          onClick={() => setEdgeCatalogOpen(o => !o)}
          className="w-full flex items-center justify-between px-4 py-3 text-left hover:bg-slate-50 dark:hover:bg-dbx-navy-500/30 transition-colors"
          aria-expanded={edgeCatalogOpen}
        >
          <div>
            <h4 className="text-sm font-semibold text-slate-700 dark:text-slate-200">{tr('ge.edgeCatalog')}</h4>
            <p className="text-[10px] text-slate-400 mt-0.5">{tr('ge.edgeCatalogDesc')}</p>
          </div>
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
