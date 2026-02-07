import React, { useState, useEffect } from 'react'

function StatCard({ label, value, sub }) {
  return (
    <div className="bg-white border rounded-lg p-4">
      <p className="text-sm text-gray-500">{label}</p>
      <p className="text-2xl font-semibold mt-1">{value}</p>
      {sub && <p className="text-xs text-gray-400 mt-1">{sub}</p>}
    </div>
  )
}

export default function DataProfiling() {
  const [snapshots, setSnapshots] = useState([])
  const [quality, setQuality] = useState([])
  const [columnStats, setColumnStats] = useState([])
  const [view, setView] = useState('snapshots')

  useEffect(() => {
    fetch('/api/profiling/snapshots').then(r => r.json()).then(setSnapshots).catch(() => {})
    fetch('/api/profiling/quality-scores').then(r => r.json()).then(setQuality).catch(() => {})
  }, [])

  const loadColumnStats = (table) => {
    fetch(`/api/profiling/column-stats?table_name=${encodeURIComponent(table)}`).then(r => r.json()).then(d => {
      setColumnStats(d)
      setView('column-detail')
    })
  }

  const avgQuality = quality.length ? (quality.reduce((s, q) => s + parseFloat(q.overall_score || 0), 0) / quality.length).toFixed(2) : '--'

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <StatCard label="Tables Profiled" value={snapshots.length} />
        <StatCard label="Quality Scores" value={quality.length} />
        <StatCard label="Avg Quality" value={avgQuality} />
        <StatCard label="Column Stats Loaded" value={columnStats.length} />
      </div>

      <div className="flex gap-3">
        <button onClick={() => setView('snapshots')}
          className={`px-3 py-1.5 text-sm rounded-md ${view === 'snapshots' ? 'bg-blue-600 text-white' : 'bg-gray-100'}`}>
          Snapshots</button>
        <button onClick={() => setView('quality')}
          className={`px-3 py-1.5 text-sm rounded-md ${view === 'quality' ? 'bg-blue-600 text-white' : 'bg-gray-100'}`}>
          Quality Scores</button>
      </div>

      <div className="bg-white rounded-lg border p-4 overflow-x-auto">
        {view === 'snapshots' && (
          <table className="min-w-full text-sm">
            <thead><tr>
              {['Table', 'Rows', 'Columns', 'Size (bytes)', 'Profiled At'].map(h =>
                <th key={h} className="text-left px-3 py-2 bg-gray-50 font-medium text-gray-600 border-b">{h}</th>)}
            </tr></thead>
            <tbody>
              {snapshots.map((s, i) => (
                <tr key={i} className="border-b hover:bg-gray-50 cursor-pointer" onClick={() => loadColumnStats(s.table_name)}>
                  <td className="px-3 py-2 text-blue-600">{s.table_name}</td>
                  <td className="px-3 py-2">{s.row_count}</td>
                  <td className="px-3 py-2">{s.column_count}</td>
                  <td className="px-3 py-2">{s.table_size_bytes ?? '--'}</td>
                  <td className="px-3 py-2">{s.profiled_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {view === 'quality' && (
          <table className="min-w-full text-sm">
            <thead><tr>
              {['Table', 'Overall', 'Completeness', 'Freshness', 'Scored At'].map(h =>
                <th key={h} className="text-left px-3 py-2 bg-gray-50 font-medium text-gray-600 border-b">{h}</th>)}
            </tr></thead>
            <tbody>
              {quality.map((q, i) => (
                <tr key={i} className="border-b hover:bg-gray-50">
                  <td className="px-3 py-2">{q.table_name}</td>
                  <td className="px-3 py-2 font-medium">{q.overall_score}</td>
                  <td className="px-3 py-2">{q.completeness_score}</td>
                  <td className="px-3 py-2">{q.freshness_score}</td>
                  <td className="px-3 py-2">{q.scored_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {view === 'column-detail' && (
          <div>
            <h3 className="font-medium mb-2">Column Statistics</h3>
            <table className="min-w-full text-sm">
              <thead><tr>
                {['Column', 'Distinct', 'Null Rate', 'Min', 'Max', 'Pattern'].map(h =>
                  <th key={h} className="text-left px-3 py-2 bg-gray-50 font-medium text-gray-600 border-b">{h}</th>)}
              </tr></thead>
              <tbody>
                {columnStats.map((c, i) => (
                  <tr key={i} className="border-b hover:bg-gray-50">
                    <td className="px-3 py-2">{c.column_name}</td>
                    <td className="px-3 py-2">{c.distinct_count}</td>
                    <td className="px-3 py-2">{c.null_rate}</td>
                    <td className="px-3 py-2 max-w-xs truncate">{c.min_value}</td>
                    <td className="px-3 py-2 max-w-xs truncate">{c.max_value}</td>
                    <td className="px-3 py-2">{c.pattern_type}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
