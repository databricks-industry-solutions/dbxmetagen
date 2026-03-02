import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { safeFetch, safeFetchObj, ErrorBanner } from '../App'
import Visualizations from './Visualizations'

const NODE_TYPES = ['', 'table', 'column', 'schema']
const EDGE_TYPES = ['', 'contains', 'same_domain', 'same_subdomain', 'same_catalog',
  'same_schema', 'same_security_level', 'derives_from', 'same_classification',
  'similar_embedding', 'predicted_fk']

const PREBUILT_QUESTIONS = [
  "What tables contain PII data?",
  "Which tables are most similar to each other?",
  "Show me the lineage for this catalog",
  "What columns might be foreign keys?",
]

const NODE_COLORS = { table: '#6366f1', column: '#8b5cf6', schema: '#0ea5e9' }

// ---------------------------------------------------------------------------
// Force graph visualization of neighbors
// ---------------------------------------------------------------------------
let ForceGraph2D = null
try { ForceGraph2D = require('react-force-graph-2d').default } catch { }

function NeighborGraph({ selectedNode, neighbors, onNodeClick }) {
  const graphRef = useRef()
  const graphData = useMemo(() => {
    if (!selectedNode || neighbors.length === 0) return { nodes: [], links: [] }
    const nodeMap = new Map()
    nodeMap.set(selectedNode, { id: selectedNode, node_type: 'selected', val: 3 })
    neighbors.forEach(n => {
      if (!nodeMap.has(n.neighbor))
        nodeMap.set(n.neighbor, { id: n.neighbor, node_type: n.node_type || 'unknown', val: 1 })
    })
    const links = neighbors.map(n => ({
      source: selectedNode, target: n.neighbor,
      relationship: n.relationship, weight: n.weight,
    }))
    return { nodes: Array.from(nodeMap.values()), links }
  }, [selectedNode, neighbors])

  const nodeColor = useCallback(n => n.id === selectedNode ? '#f59e0b' : (NODE_COLORS[n.node_type] || '#94a3b8'), [selectedNode])
  const nodeLabel = useCallback(n => `${n.id} (${n.node_type})`, [])
  const linkLabel = useCallback(l => `${l.relationship} (${l.weight})`, [])

  if (!ForceGraph2D || graphData.nodes.length === 0) return null
  return (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden" style={{ height: 350 }}>
      <ForceGraph2D ref={graphRef} graphData={graphData} width={600} height={350}
        nodeColor={nodeColor} nodeLabel={nodeLabel} linkLabel={linkLabel}
        linkDirectionalArrowLength={4} linkDirectionalArrowRelPos={1}
        onNodeClick={(node) => onNodeClick(node.id)}
        nodeCanvasObject={(node, ctx, globalScale) => {
          const r = node.val === 3 ? 6 : 4
          ctx.beginPath(); ctx.arc(node.x, node.y, r, 0, 2 * Math.PI)
          ctx.fillStyle = nodeColor(node); ctx.fill()
          if (globalScale > 1.5) {
            ctx.font = `${10 / globalScale}px sans-serif`
            ctx.fillStyle = '#334155'; ctx.textAlign = 'center'
            const label = node.id.split('.').pop()
            ctx.fillText(label, node.x, node.y + r + 10 / globalScale)
          }
        }}
      />
    </div>
  )
}

// ---------------------------------------------------------------------------
// Graph Explorer
// ---------------------------------------------------------------------------
function GraphExplorer() {
  const [question, setQuestion] = useState('')
  const [answer, setAnswer] = useState(null)
  const [loading, setLoading] = useState(false)
  const [nodes, setNodes] = useState([])
  const [selectedNode, setSelectedNode] = useState(null)
  const [neighbors, setNeighbors] = useState([])
  const [nodeError, setNodeError] = useState(null)

  // Filters
  const [filterNodeType, setFilterNodeType] = useState('')
  const [filterEdgeType, setFilterEdgeType] = useState('')

  const loadNodes = useCallback(() => {
    const params = new URLSearchParams({ limit: '50' })
    if (filterNodeType) params.set('node_type', filterNodeType)
    safeFetch(`/api/graph/nodes?${params}`).then(r => { setNodes(r.data); if (r.error) setNodeError(r.error) })
  }, [filterNodeType])

  useEffect(() => { loadNodes() }, [loadNodes])

  const exploreNode = async (nodeId) => {
    setSelectedNode(nodeId)
    const params = filterEdgeType ? `?relationship=${encodeURIComponent(filterEdgeType)}` : ''
    const { data } = await safeFetch(`/api/graph/neighbors/${encodeURIComponent(nodeId)}${params}`)
    setNeighbors(data)
  }

  const askQuestion = async (q) => {
    const text = q || question
    if (!text.trim()) return
    setQuestion(text)
    setLoading(true)
    try {
      const res = await fetch('/api/graph/query', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: text, max_hops: 3 }),
      })
      if (res.ok) {
        setAnswer(await res.json())
      } else {
        const body = await res.text().catch(() => '')
        let msg = `Error ${res.status}`
        try { const j = JSON.parse(body); if (j.detail) msg = j.detail } catch { }
        setAnswer({ answer: msg, steps: 0 })
      }
    } catch (e) { setAnswer({ answer: `Error: ${e.message}`, steps: 0 }) }
    finally { setLoading(false) }
  }

  return (
    <div className="space-y-4">
      {/* Pre-built question chips */}
      <div className="flex flex-wrap gap-2">
        {PREBUILT_QUESTIONS.map(q => (
          <button key={q} onClick={() => askQuestion(q)}
            className="px-3 py-1.5 text-xs bg-purple-50 text-purple-700 border border-purple-200 rounded-full hover:bg-purple-100 transition-colors">
            {q}
          </button>
        ))}
      </div>

      {/* Q&A input */}
      <div className="flex gap-2">
        <input value={question} onChange={e => setQuestion(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && askQuestion()}
          placeholder="Ask about your data catalog..."
          className="flex-1 border border-slate-300 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-purple-500 focus:border-purple-500" />
        <button onClick={() => askQuestion()} disabled={loading}
          className="px-5 py-2.5 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50 shadow-sm">
          {loading ? 'Thinking...' : 'Ask'}</button>
      </div>

      {answer && (
        <div className="bg-purple-50 border border-purple-200 rounded-xl p-5">
          <p className="text-sm whitespace-pre-wrap text-slate-700 leading-relaxed">{answer.answer}</p>
          <p className="text-xs text-purple-400 mt-3 font-medium">Completed in {answer.steps} step(s)</p>
        </div>
      )}

      {/* Filters */}
      <div className="flex gap-3 items-center">
        <label className="text-xs text-slate-500 font-medium">Node type</label>
        <select value={filterNodeType} onChange={e => setFilterNodeType(e.target.value)}
          className="border border-slate-300 rounded-lg px-2 py-1.5 text-xs">
          <option value="">All</option>
          {NODE_TYPES.filter(Boolean).map(t => <option key={t} value={t}>{t}</option>)}
        </select>
        <label className="text-xs text-slate-500 font-medium ml-2">Edge type</label>
        <select value={filterEdgeType} onChange={e => { setFilterEdgeType(e.target.value); if (selectedNode) exploreNode(selectedNode) }}
          className="border border-slate-300 rounded-lg px-2 py-1.5 text-xs">
          <option value="">All</option>
          {EDGE_TYPES.filter(Boolean).map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>

      {/* Neighbor visualization */}
      <NeighborGraph selectedNode={selectedNode} neighbors={neighbors} onNodeClick={exploreNode} />

      {/* Node list + neighbor list */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="bg-white border border-slate-200 rounded-xl p-4 max-h-96 overflow-y-auto shadow-sm">
          <h3 className="font-semibold text-sm text-slate-700 mb-3">Graph Nodes</h3>
          {nodeError
            ? <div className="text-sm py-2">
              <p className="text-amber-600 mb-1">Graph tables not found in Lakebase.</p>
              <p className="text-slate-400">Run the "Sync Graph to Lakebase" job from the Batch Jobs tab to create them.</p>
            </div>
            : nodes.length === 0
              ? <p className="text-sm text-slate-400 py-2">No graph nodes available.</p>
              : nodes.map((n, i) => (
                <div key={i} onClick={() => exploreNode(n.id)}
                  className={`p-2.5 border-b border-slate-100 cursor-pointer hover:bg-indigo-50 transition-colors text-xs ${selectedNode === n.id ? 'bg-indigo-50 border-l-2 border-l-indigo-500' : ''}`}>
                  <span className="font-semibold text-slate-700">{n.id}</span>
                  <span className="ml-2 text-slate-400">{n.node_type}</span>
                  {n.domain && <span className="ml-2 bg-slate-100 text-slate-500 px-1.5 py-0.5 rounded text-[10px]">{n.domain}</span>}
                </div>
              ))
          }
        </div>

        <div className="bg-white border border-slate-200 rounded-xl p-4 max-h-96 overflow-y-auto shadow-sm">
          <h3 className="font-semibold text-sm text-slate-700 mb-3">
            {selectedNode ? `Neighbors of ${selectedNode}` : 'Select a node to explore'}
          </h3>
          {neighbors.length === 0 && selectedNode && <p className="text-sm text-slate-400 py-2">No neighbors found.</p>}
          {neighbors.map((n, i) => (
            <div key={i} onClick={() => exploreNode(n.neighbor)}
              className="p-2.5 border-b border-slate-100 cursor-pointer hover:bg-purple-50 transition-colors text-xs">
              <span className="font-semibold text-slate-700">{n.neighbor}</span>
              <span className="ml-2 text-purple-600 font-medium">{n.relationship}</span>
              <span className="ml-2 text-slate-400">w={n.weight}</span>
              {n.comment && <p className="text-slate-500 mt-1 truncate">{n.comment}</p>}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// FK Predictions sub-view
// ---------------------------------------------------------------------------
function FKPredictions() {
  const [predictions, setPredictions] = useState([])
  const [error, setError] = useState(null)

  useEffect(() => {
    safeFetch('/api/analytics/fk-predictions').then(r => {
      setPredictions(r.data); if (r.error) setError(r.error)
    })
  }, [])

  return (
    <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
      <h2 className="text-lg font-semibold text-slate-800 mb-4">Predicted Foreign Keys</h2>
      <ErrorBanner error={error} />
      {predictions.length === 0
        ? <p className="text-sm text-slate-400">No FK predictions available. Run the FK prediction job first.</p>
        : <div className="overflow-x-auto max-h-96">
          <table className="min-w-full text-sm">
            <thead><tr>
              {['Source Column', 'Target Column', 'Rule Score', 'AI Confidence', 'Reasoning'].map(h =>
                <th key={h} className="text-left px-3 py-2.5 bg-slate-50 font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
            </tr></thead>
            <tbody>
              {predictions.map((p, i) => (
                <tr key={i} className="border-b border-slate-100 hover:bg-indigo-50/30 transition-colors">
                  <td className="px-3 py-2 text-slate-700">{p.src_column}</td>
                  <td className="px-3 py-2 text-slate-700">{p.dst_column}</td>
                  <td className="px-3 py-2 font-bold text-indigo-700">{Number(p.rule_score).toFixed(2)}</td>
                  <td className="px-3 py-2 font-bold text-emerald-700">{Number(p.ai_confidence).toFixed(2)}</td>
                  <td className="px-3 py-2 text-slate-500 text-xs max-w-xs truncate">{p.ai_reasoning}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      }
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Analytics component
// ---------------------------------------------------------------------------
export default function Analytics() {
  const [clusters, setClusters] = useState([])
  const [metrics, setMetrics] = useState([])
  const [simEdges, setSimEdges] = useState([])
  const [error, setError] = useState(null)
  const [view, setView] = useState('graph')

  useEffect(() => {
    safeFetch('/api/analytics/clusters?limit=200').then(r => { setClusters(r.data); if (r.error) setError(r.error) })
    safeFetch('/api/analytics/clustering-metrics').then(r => { setMetrics(r.data); if (r.error) setError(r.error) })
    safeFetch('/api/analytics/similarity-edges?limit=100').then(r => { setSimEdges(r.data); if (r.error) setError(r.error) })
  }, [])

  const clusterSummary = clusters.reduce((acc, c) => { acc[c.cluster] = (acc[c.cluster] || 0) + 1; return acc }, {})

  const tabs = [['graph', 'Graph Explorer'], ['viz', 'Visualizations'], ['fk', 'FK Predictions'], ['clusters', 'Clusters'], ['similarity', 'Similarity'], ['metrics', 'Metrics']]

  return (
    <div className="space-y-6">
      <ErrorBanner error={error} />
      <div className="flex gap-2 flex-wrap">
        {tabs.map(([k, l]) => (
          <button key={k} onClick={() => setView(k)}
            className={`px-4 py-2 text-sm rounded-lg font-medium transition-all ${view === k ? 'bg-indigo-600 text-white shadow-sm' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'}`}>{l}</button>
        ))}
      </div>

      {view === 'graph' && <GraphExplorer />}
      {view === 'viz' && <Visualizations />}
      {view === 'fk' && <FKPredictions />}

      {view === 'clusters' && (
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-slate-800 mb-4">Cluster Assignments</h2>
          {Object.keys(clusterSummary).length === 0
            ? <p className="text-sm text-slate-400">No clusters available. Run the analytics pipeline first.</p>
            : <>
              <div className="grid grid-cols-2 md:grid-cols-6 gap-3 mb-5">
                {Object.entries(clusterSummary).map(([k, v]) => (
                  <div key={k} className="border border-slate-200 rounded-xl p-3 text-center bg-gradient-to-br from-white to-indigo-50/30">
                    <p className="text-xs text-slate-400 font-medium">Cluster {k}</p>
                    <p className="text-xl font-bold text-indigo-700">{v}</p>
                  </div>
                ))}
              </div>
              <div className="overflow-x-auto max-h-80">
                <table className="min-w-full text-sm">
                  <thead><tr>
                    {['ID', 'Type', 'Domain', 'Cluster', 'K', 'Silhouette'].map(h =>
                      <th key={h} className="text-left px-3 py-2.5 bg-slate-50 font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
                  </tr></thead>
                  <tbody>
                    {clusters.slice(0, 100).map((c, i) => (
                      <tr key={i} className="border-b border-slate-100 hover:bg-indigo-50/30 transition-colors">
                        <td className="px-3 py-1.5 text-slate-700">{c.id}</td>
                        <td className="px-3 py-1.5 text-slate-600">{c.node_type}</td>
                        <td className="px-3 py-1.5 text-slate-600">{c.domain}</td>
                        <td className="px-3 py-1.5 font-bold text-indigo-700">{c.cluster}</td>
                        <td className="px-3 py-1.5 text-slate-600">{c.k_value}</td>
                        <td className="px-3 py-1.5 text-slate-600">{c.silhouette_score}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          }
        </div>
      )}

      {view === 'similarity' && (
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-slate-800 mb-4">Similarity Edges</h2>
          {simEdges.length === 0
            ? <p className="text-sm text-slate-400">No similarity edges available.</p>
            : <table className="min-w-full text-sm">
              <thead><tr>
                {['Source', 'Target', 'Weight'].map(h =>
                  <th key={h} className="text-left px-3 py-2.5 bg-slate-50 font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
              </tr></thead>
              <tbody>
                {simEdges.map((e, i) => (
                  <tr key={i} className="border-b border-slate-100 hover:bg-indigo-50/30 transition-colors">
                    <td className="px-3 py-2 text-slate-700">{e.src}</td>
                    <td className="px-3 py-2 text-slate-700">{e.dst}</td>
                    <td className="px-3 py-2 font-bold text-indigo-700">{e.weight}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          }
        </div>
      )}

      {view === 'metrics' && (
        <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-slate-800 mb-4">Clustering Metrics</h2>
          {metrics.length === 0
            ? <p className="text-sm text-slate-400">No clustering metrics available.</p>
            : <table className="min-w-full text-sm">
              <thead><tr>
                {['K', 'Phase', 'Silhouette Mean', 'WSSSE Mean', 'Runs', 'Sample Size', 'Timestamp'].map(h =>
                  <th key={h} className="text-left px-3 py-2.5 bg-slate-50 font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
              </tr></thead>
              <tbody>
                {metrics.map((m, i) => (
                  <tr key={i} className="border-b border-slate-100 hover:bg-indigo-50/30 transition-colors">
                    <td className="px-3 py-2 font-bold text-indigo-700">{m.k}</td>
                    <td className="px-3 py-2 text-slate-600">{m.phase}</td>
                    <td className="px-3 py-2 text-slate-600">{m.silhouette_mean}</td>
                    <td className="px-3 py-2 text-slate-600">{m.wssse_mean}</td>
                    <td className="px-3 py-2 text-slate-600">{m.n_runs}</td>
                    <td className="px-3 py-2 text-slate-600">{m.sample_size}</td>
                    <td className="px-3 py-2 text-slate-500 text-xs">{m.run_timestamp}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          }
        </div>
      )}
    </div>
  )
}
