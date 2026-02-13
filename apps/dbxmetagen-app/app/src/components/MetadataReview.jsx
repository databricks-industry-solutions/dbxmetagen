import React, { useState, useEffect } from 'react'
import { safeFetch, ErrorBanner } from '../App'

function DataTable({ data, maxRows = 100 }) {
  if (!data || data.length === 0) return <p className="text-sm text-slate-400 py-4">No data available.</p>
  const cols = Object.keys(data[0])
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead><tr>{cols.map(c =>
          <th key={c} className="text-left px-3 py-2.5 bg-slate-50 font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{c}</th>
        )}</tr></thead>
        <tbody>
          {data.slice(0, maxRows).map((row, i) => (
            <tr key={i} className="border-b border-slate-100 hover:bg-indigo-50/30 transition-colors">
              {cols.map(c => <td key={c} className="px-3 py-2 max-w-xs truncate text-slate-600">{String(row[c] ?? '')}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default function MetadataReview() {
  const [tab, setTab] = useState('log')
  const [data, setData] = useState([])
  const [error, setError] = useState(null)
  const [filter, setFilter] = useState('')
  const [loading, setLoading] = useState(false)

  const endpoints = { log: '/api/metadata/log', kb: '/api/metadata/knowledge-base', columns: '/api/metadata/column-kb', schemas: '/api/metadata/schema-kb' }

  const load = async (key) => {
    setLoading(true)
    const url = filter ? `${endpoints[key]}?table_name=${encodeURIComponent(filter)}` : endpoints[key]
    const r = await safeFetch(url)
    setData(r.data)
    setError(r.error)
    setLoading(false)
  }

  useEffect(() => { load(tab) }, [tab])

  return (
    <div className="space-y-4">
      <ErrorBanner error={error} />
      <div className="flex items-center gap-4">
        <div className="flex bg-slate-100 rounded-lg p-1">
          {[['log','Generation Log'],['kb','Table KB'],['columns','Column KB'],['schemas','Schema KB']].map(([k,l]) => (
            <button key={k} onClick={() => setTab(k)}
              className={`px-3 py-1.5 text-sm rounded-md transition-all ${tab === k ? 'bg-white shadow-sm font-semibold text-indigo-700' : 'text-slate-500 hover:text-slate-700'}`}>{l}</button>
          ))}
        </div>
        <input value={filter} onChange={e => setFilter(e.target.value)}
          placeholder="Filter by table name..."
          className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm flex-1 max-w-xs focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500" />
        <button onClick={() => load(tab)} className="px-4 py-1.5 bg-indigo-600 text-white rounded-lg text-sm font-medium hover:bg-indigo-700 shadow-sm">Search</button>
      </div>
      <div className="bg-white rounded-xl border border-slate-200 p-4 shadow-sm">
        {loading ? <p className="text-sm text-slate-400 py-4">Loading...</p> : <DataTable data={data} />}
      </div>
    </div>
  )
}
