import React, { useState, useEffect } from 'react'

function DataTable({ data, maxRows = 100 }) {
  if (!data || data.length === 0) return <p className="text-sm text-gray-500">No data available.</p>
  const cols = Object.keys(data[0])
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead><tr>{cols.map(c => <th key={c} className="text-left px-3 py-2 bg-gray-50 font-medium text-gray-600 border-b">{c}</th>)}</tr></thead>
        <tbody>
          {data.slice(0, maxRows).map((row, i) => (
            <tr key={i} className="border-b hover:bg-gray-50">
              {cols.map(c => <td key={c} className="px-3 py-2 max-w-xs truncate">{String(row[c] ?? '')}</td>)}
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
  const [filter, setFilter] = useState('')
  const [loading, setLoading] = useState(false)

  const endpoints = {
    log: '/api/metadata/log',
    kb: '/api/metadata/knowledge-base',
    columns: '/api/metadata/column-kb',
    schemas: '/api/metadata/schema-kb',
  }

  const load = async (key) => {
    setLoading(true)
    try {
      const url = filter ? `${endpoints[key]}?table_name=${encodeURIComponent(filter)}` : endpoints[key]
      const res = await fetch(url)
      setData(await res.json())
    } catch { setData([]) }
    finally { setLoading(false) }
  }

  useEffect(() => { load(tab) }, [tab])

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4">
        <div className="flex bg-gray-100 rounded-md p-1">
          {[['log','Generation Log'],['kb','Table KB'],['columns','Column KB'],['schemas','Schema KB']].map(([k,l]) => (
            <button key={k} onClick={() => setTab(k)}
              className={`px-3 py-1.5 text-sm rounded ${tab === k ? 'bg-white shadow font-medium' : 'text-gray-600'}`}>{l}</button>
          ))}
        </div>
        <input
          value={filter} onChange={e => setFilter(e.target.value)}
          placeholder="Filter by table name..."
          className="border rounded-md px-3 py-1.5 text-sm flex-1 max-w-xs"
        />
        <button onClick={() => load(tab)} className="px-3 py-1.5 bg-blue-600 text-white rounded-md text-sm">Search</button>
      </div>
      <div className="bg-white rounded-lg border p-4">
        {loading ? <p className="text-sm text-gray-500">Loading...</p> : <DataTable data={data} />}
      </div>
    </div>
  )
}
