import React, { useState, useEffect } from 'react'
import { safeFetch, ErrorBanner } from '../App'

function StatCard({ label, value, sub, accent = 'orange' }) {
  const accents = {
    orange: 'border-l-orange-500 bg-orange-50/30',
    purple: 'border-l-purple-500 bg-purple-50/30',
    emerald: 'border-l-emerald-500 bg-emerald-50/30',
    amber: 'border-l-amber-500 bg-amber-50/30',
  }
  return (
    <div className={`bg-dbx-oat-light border border-slate-200 border-l-4 ${accents[accent]} rounded-xl p-4 shadow-sm`}>
      <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">{label}</p>
      <p className="text-2xl font-bold text-slate-800 mt-1">{value}</p>
      {sub && <p className="text-xs text-slate-400 mt-1">{sub}</p>}
    </div>
  )
}

export default function DataProfiling() {
  const [snapshots, setSnapshots] = useState([])
  const [quality, setQuality] = useState([])
  const [columnStats, setColumnStats] = useState([])
  const [error, setError] = useState(null)
  const [view, setView] = useState('snapshots')

  useEffect(() => {
    safeFetch('/api/profiling/snapshots').then(r => { setSnapshots(r.data); if (r.error) setError(r.error) })
    safeFetch('/api/profiling/quality-scores').then(r => { setQuality(r.data); if (r.error) setError(r.error) })
  }, [])

  const loadColumnStats = async (table) => {
    const { data } = await safeFetch(`/api/profiling/column-stats?table_name=${encodeURIComponent(table)}`)
    setColumnStats(data)
    setView('column-detail')
  }

  const avgQuality = quality.length ? (quality.reduce((s, q) => s + parseFloat(q.overall_score || 0), 0) / quality.length).toFixed(2) : '--'

  return (
    <div className="space-y-6">
      <ErrorBanner error={error} />
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <StatCard label="Tables Profiled" value={snapshots.length} accent="orange" />
        <StatCard label="Quality Scores" value={quality.length} accent="purple" />
        <StatCard label="Avg Quality" value={avgQuality} accent="emerald" />
        <StatCard label="Column Stats" value={columnStats.length} accent="amber" />
      </div>

      <div className="flex gap-2">
        {[['snapshots','Snapshots'],['quality','Quality Scores']].map(([k,l]) => (
          <button key={k} onClick={() => setView(k)}
            className={`px-4 py-2 text-sm rounded-lg font-medium transition-all ${view === k ? 'bg-dbx-lava text-white shadow-sm' : 'bg-dbx-oat text-slate-600 hover:bg-dbx-oat-dark'}`}>{l}</button>
        ))}
      </div>

      <div className="bg-dbx-oat-light rounded-xl border border-slate-200 p-4 shadow-sm overflow-x-auto">
        {view === 'snapshots' && (snapshots.length === 0
          ? <p className="text-sm text-slate-400 py-4">No snapshots available. Run a profiling job first.</p>
          : <table className="min-w-full text-sm">
              <thead><tr>
                {['Table', 'Rows', 'Columns', 'Size (bytes)', 'Profiled At'].map(h =>
                  <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
              </tr></thead>
              <tbody>
                {snapshots.map((s, i) => (
                  <tr key={i} className="border-b border-slate-100 hover:bg-orange-50/30 cursor-pointer transition-colors" onClick={() => loadColumnStats(s.table_name)}>
                    <td className="px-3 py-2 text-dbx-lava font-medium">{s.table_name}</td>
                    <td className="px-3 py-2 text-slate-600">{s.row_count}</td>
                    <td className="px-3 py-2 text-slate-600">{s.column_count}</td>
                    <td className="px-3 py-2 text-slate-600">{s.table_size_bytes ?? '--'}</td>
                    <td className="px-3 py-2 text-slate-500 text-xs">{s.profiled_at}</td>
                  </tr>
                ))}
              </tbody>
            </table>
        )}
        {view === 'quality' && (quality.length === 0
          ? <p className="text-sm text-slate-400 py-4">No quality scores available.</p>
          : <table className="min-w-full text-sm">
              <thead><tr>
                {['Table', 'Overall', 'Completeness', 'Freshness', 'Scored At'].map(h =>
                  <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
              </tr></thead>
              <tbody>
                {quality.map((q, i) => (
                  <tr key={i} className="border-b border-slate-100 hover:bg-orange-50/30 transition-colors">
                    <td className="px-3 py-2 text-slate-700">{q.table_name}</td>
                    <td className="px-3 py-2 font-bold text-red-700">{q.overall_score}</td>
                    <td className="px-3 py-2 text-slate-600">{q.completeness_score}</td>
                    <td className="px-3 py-2 text-slate-600">{q.freshness_score}</td>
                    <td className="px-3 py-2 text-slate-500 text-xs">{q.scored_at}</td>
                  </tr>
                ))}
              </tbody>
            </table>
        )}
        {view === 'column-detail' && (
          <div>
            <h3 className="font-semibold text-slate-800 mb-3">Column Statistics</h3>
            {columnStats.length === 0 ? <p className="text-sm text-slate-400">No column stats.</p> :
            <table className="min-w-full text-sm">
              <thead><tr>
                {['Column', 'Distinct', 'Null Rate', 'Min', 'Max', 'Pattern'].map(h =>
                  <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
              </tr></thead>
              <tbody>
                {columnStats.map((c, i) => (
                  <tr key={i} className="border-b border-slate-100 hover:bg-orange-50/30 transition-colors">
                    <td className="px-3 py-2 font-medium text-slate-700">{c.column_name}</td>
                    <td className="px-3 py-2 text-slate-600">{c.distinct_count}</td>
                    <td className="px-3 py-2 text-slate-600">{c.null_rate}</td>
                    <td className="px-3 py-2 max-w-xs truncate text-slate-500">{c.min_value}</td>
                    <td className="px-3 py-2 max-w-xs truncate text-slate-500">{c.max_value}</td>
                    <td className="px-3 py-2 text-slate-600">{c.pattern_type}</td>
                  </tr>
                ))}
              </tbody>
            </table>}
          </div>
        )}
      </div>
    </div>
  )
}
