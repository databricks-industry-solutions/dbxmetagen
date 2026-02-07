import React, { useState, useEffect } from 'react'

function GraphExplorer() {
  const [question, setQuestion] = useState('')
  const [answer, setAnswer] = useState(null)
  const [loading, setLoading] = useState(false)

  // Direct graph exploration
  const [nodes, setNodes] = useState([])
  const [edges, setEdges] = useState([])
  const [selectedNode, setSelectedNode] = useState(null)
  const [neighbors, setNeighbors] = useState([])

  const askQuestion = async () => {
    if (!question.trim()) return
    setLoading(true)
    try {
      const res = await fetch('/api/graph/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, max_hops: 3 }),
      })
      setAnswer(await res.json())
    } catch (e) { setAnswer({ answer: `Error: ${e.message}`, steps: 0 }) }
    finally { setLoading(false) }
  }

  const exploreNode = async (nodeId) => {
    setSelectedNode(nodeId)
    const res = await fetch(`/api/graph/neighbors/${encodeURIComponent(nodeId)}`)
    setNeighbors(await res.json())
  }

  useEffect(() => {
    fetch('/api/graph/nodes?limit=50').then(r => r.json()).then(setNodes).catch(() => {})
  }, [])

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <input
          value={question} onChange={e => setQuestion(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && askQuestion()}
          placeholder="Ask about your data catalog... (e.g., 'What tables contain patient data?')"
          className="flex-1 border rounded-md px-3 py-2 text-sm"
        />
        <button onClick={askQuestion} disabled={loading}
          className="px-4 py-2 bg-purple-600 text-white rounded-md text-sm hover:bg-purple-700 disabled:opacity-50">
          {loading ? 'Thinking...' : 'Ask'}
        </button>
      </div>

      {answer && (
        <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
          <p className="text-sm whitespace-pre-wrap">{answer.answer}</p>
          <p className="text-xs text-purple-400 mt-2">Completed in {answer.steps} steps</p>
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="bg-white border rounded-lg p-4 max-h-96 overflow-y-auto">
          <h3 className="font-medium text-sm mb-2">Graph Nodes</h3>
          {nodes.map((n, i) => (
            <div key={i} onClick={() => exploreNode(n.id)}
              className={`p-2 border-b cursor-pointer hover:bg-blue-50 text-xs ${selectedNode === n.id ? 'bg-blue-50' : ''}`}>
              <span className="font-medium">{n.id}</span>
              <span className="ml-2 text-gray-400">{n.node_type}</span>
              {n.domain && <span className="ml-2 bg-gray-100 px-1.5 py-0.5 rounded">{n.domain}</span>}
            </div>
          ))}
        </div>

        <div className="bg-white border rounded-lg p-4 max-h-96 overflow-y-auto">
          <h3 className="font-medium text-sm mb-2">
            {selectedNode ? `Neighbors of ${selectedNode}` : 'Select a node to explore'}
          </h3>
          {neighbors.map((n, i) => (
            <div key={i} onClick={() => exploreNode(n.neighbor)}
              className="p-2 border-b cursor-pointer hover:bg-blue-50 text-xs">
              <span className="font-medium">{n.neighbor}</span>
              <span className="ml-2 text-purple-600">{n.relationship}</span>
              <span className="ml-2 text-gray-400">w={n.weight}</span>
              {n.comment && <p className="text-gray-500 mt-1 truncate">{n.comment}</p>}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

export default function Analytics() {
  const [clusters, setClusters] = useState([])
  const [metrics, setMetrics] = useState([])
  const [simEdges, setSimEdges] = useState([])
  const [view, setView] = useState('graph')

  useEffect(() => {
    fetch('/api/analytics/clusters?limit=200').then(r => r.json()).then(setClusters).catch(() => {})
    fetch('/api/analytics/clustering-metrics').then(r => r.json()).then(setMetrics).catch(() => {})
    fetch('/api/analytics/similarity-edges?limit=100').then(r => r.json()).then(setSimEdges).catch(() => {})
  }, [])

  const clusterSummary = clusters.reduce((acc, c) => {
    acc[c.cluster] = (acc[c.cluster] || 0) + 1
    return acc
  }, {})

  return (
    <div className="space-y-6">
      <div className="flex gap-3">
        {[['graph','Graph Explorer'],['clusters','Clusters'],['similarity','Similarity'],['metrics','Metrics']].map(([k,l]) => (
          <button key={k} onClick={() => setView(k)}
            className={`px-3 py-1.5 text-sm rounded-md ${view === k ? 'bg-blue-600 text-white' : 'bg-gray-100'}`}>{l}</button>
        ))}
      </div>

      {view === 'graph' && <GraphExplorer />}

      {view === 'clusters' && (
        <div className="bg-white rounded-lg border p-6">
          <h2 className="text-lg font-semibold mb-4">Cluster Assignments</h2>
          <div className="grid grid-cols-2 md:grid-cols-6 gap-3 mb-4">
            {Object.entries(clusterSummary).map(([k, v]) => (
              <div key={k} className="border rounded-lg p-3 text-center">
                <p className="text-xs text-gray-500">Cluster {k}</p>
                <p className="text-xl font-bold">{v}</p>
              </div>
            ))}
          </div>
          <div className="overflow-x-auto max-h-80">
            <table className="min-w-full text-sm">
              <thead><tr>
                {['ID', 'Type', 'Domain', 'Cluster', 'K', 'Silhouette'].map(h =>
                  <th key={h} className="text-left px-3 py-2 bg-gray-50 font-medium text-gray-600 border-b">{h}</th>)}
              </tr></thead>
              <tbody>
                {clusters.slice(0, 100).map((c, i) => (
                  <tr key={i} className="border-b">
                    <td className="px-3 py-1">{c.id}</td>
                    <td className="px-3 py-1">{c.node_type}</td>
                    <td className="px-3 py-1">{c.domain}</td>
                    <td className="px-3 py-1 font-medium">{c.cluster}</td>
                    <td className="px-3 py-1">{c.k_value}</td>
                    <td className="px-3 py-1">{c.silhouette_score}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {view === 'similarity' && (
        <div className="bg-white rounded-lg border p-6">
          <h2 className="text-lg font-semibold mb-4">Similarity Edges</h2>
          <table className="min-w-full text-sm">
            <thead><tr>
              {['Source', 'Target', 'Weight'].map(h =>
                <th key={h} className="text-left px-3 py-2 bg-gray-50 font-medium text-gray-600 border-b">{h}</th>)}
            </tr></thead>
            <tbody>
              {simEdges.map((e, i) => (
                <tr key={i} className="border-b">
                  <td className="px-3 py-2">{e.src}</td>
                  <td className="px-3 py-2">{e.dst}</td>
                  <td className="px-3 py-2 font-medium">{e.weight}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {view === 'metrics' && (
        <div className="bg-white rounded-lg border p-6">
          <h2 className="text-lg font-semibold mb-4">Clustering Metrics</h2>
          <table className="min-w-full text-sm">
            <thead><tr>
              {['K', 'Phase', 'Silhouette Mean', 'WSSSE Mean', 'Runs', 'Sample Size', 'Timestamp'].map(h =>
                <th key={h} className="text-left px-3 py-2 bg-gray-50 font-medium text-gray-600 border-b">{h}</th>)}
            </tr></thead>
            <tbody>
              {metrics.map((m, i) => (
                <tr key={i} className="border-b">
                  <td className="px-3 py-2 font-medium">{m.k}</td>
                  <td className="px-3 py-2">{m.phase}</td>
                  <td className="px-3 py-2">{m.silhouette_mean}</td>
                  <td className="px-3 py-2">{m.wssse_mean}</td>
                  <td className="px-3 py-2">{m.n_runs}</td>
                  <td className="px-3 py-2">{m.sample_size}</td>
                  <td className="px-3 py-2">{m.run_timestamp}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
