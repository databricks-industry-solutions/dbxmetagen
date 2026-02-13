import React, { useState, useEffect } from 'react'
import { safeFetch, ErrorBanner } from '../App'

export default function Coverage() {
  const [summary, setSummary] = useState([])
  const [tables, setTables] = useState([])
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(null)  // { catalog, schema }
  const [filter, setFilter] = useState('all') // all | profiled | unprofiled

  useEffect(() => {
    safeFetch('/api/coverage/summary').then(r => {
      setSummary(r.data)
      if (r.error) setError(r.error)
    })
  }, [])

  const drillDown = async (catalog, schema) => {
    setSelected({ catalog, schema })
    const { data, error: e } = await safeFetch(`/api/coverage/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`)
    setTables(data)
    if (e) setError(e)
  }

  const totals = summary.reduce((acc, r) => ({
    total: acc.total + (parseInt(r.total_tables) || 0),
    profiled: acc.profiled + (parseInt(r.profiled_tables) || 0),
    unprofiled: acc.unprofiled + (parseInt(r.unprofiled_tables) || 0),
  }), { total: 0, profiled: 0, unprofiled: 0 })

  const filteredTables = tables.filter(t => {
    if (filter === 'profiled') return t.is_profiled === 'true' || t.is_profiled === true
    if (filter === 'unprofiled') return t.is_profiled === 'false' || t.is_profiled === false
    return true
  })

  return (
    <div className="space-y-6">
      <ErrorBanner error={error} />

      {/* Summary cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-sm text-center">
          <p className="text-xs text-slate-400 font-medium uppercase tracking-wider">Total Tables</p>
          <p className="text-3xl font-bold text-slate-800 mt-1">{totals.total}</p>
        </div>
        <div className="bg-white rounded-xl border border-emerald-200 p-5 shadow-sm text-center">
          <p className="text-xs text-emerald-500 font-medium uppercase tracking-wider">Profiled</p>
          <p className="text-3xl font-bold text-emerald-700 mt-1">{totals.profiled}</p>
        </div>
        <div className="bg-white rounded-xl border border-amber-200 p-5 shadow-sm text-center">
          <p className="text-xs text-amber-500 font-medium uppercase tracking-wider">Unprofiled</p>
          <p className="text-3xl font-bold text-amber-700 mt-1">{totals.unprofiled}</p>
        </div>
      </div>

      {/* Schema breakdown */}
      <section className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-800 mb-4">Coverage by Schema</h2>
        {summary.length === 0
          ? <p className="text-sm text-slate-400">No table information available. Ensure the catalog is accessible.</p>
          : <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead><tr>
                  {['Catalog', 'Schema', 'Total', 'Profiled', 'Unprofiled', ''].map(h =>
                    <th key={h} className="text-left px-3 py-2.5 bg-slate-50 font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
                </tr></thead>
                <tbody>
                  {summary.map((r, i) => {
                    const pct = r.total_tables > 0 ? Math.round((r.profiled_tables / r.total_tables) * 100) : 0
                    return (
                      <tr key={i} className="border-b border-slate-100 hover:bg-indigo-50/30 transition-colors cursor-pointer"
                          onClick={() => drillDown(r.table_catalog, r.table_schema)}>
                        <td className="px-3 py-2 text-slate-700 font-medium">{r.table_catalog}</td>
                        <td className="px-3 py-2 text-slate-700">{r.table_schema}</td>
                        <td className="px-3 py-2 text-slate-600">{r.total_tables}</td>
                        <td className="px-3 py-2 text-emerald-600 font-semibold">{r.profiled_tables}</td>
                        <td className="px-3 py-2 text-amber-600 font-semibold">{r.unprofiled_tables}</td>
                        <td className="px-3 py-2">
                          <div className="w-24 bg-slate-100 rounded-full h-2">
                            <div className="bg-emerald-500 h-2 rounded-full" style={{ width: `${pct}%` }} />
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
        }
      </section>

      {/* Drill-down table list */}
      {selected && (
        <section className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-slate-800">
              {selected.catalog}.{selected.schema}
              <span className="text-sm font-normal text-slate-400 ml-2">({filteredTables.length} tables)</span>
            </h2>
            <div className="flex gap-1">
              {[['all','All'],['profiled','Profiled'],['unprofiled','Unprofiled']].map(([k,l]) => (
                <button key={k} onClick={() => setFilter(k)}
                  className={`px-3 py-1.5 text-xs rounded-lg font-medium transition-all ${
                    filter === k ? 'bg-indigo-600 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'}`}>{l}</button>
              ))}
            </div>
          </div>
          <div className="overflow-x-auto max-h-96">
            <table className="min-w-full text-sm">
              <thead><tr>
                {['Table', 'Type', 'Status'].map(h =>
                  <th key={h} className="text-left px-3 py-2.5 bg-slate-50 font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
              </tr></thead>
              <tbody>
                {filteredTables.map((t, i) => (
                  <tr key={i} className="border-b border-slate-100 hover:bg-indigo-50/30 transition-colors">
                    <td className="px-3 py-2 text-slate-700 font-mono text-xs">{t.table_name}</td>
                    <td className="px-3 py-2 text-slate-500 text-xs">{t.table_type}</td>
                    <td className="px-3 py-2">
                      {(t.is_profiled === 'true' || t.is_profiled === true)
                        ? <span className="px-2 py-0.5 bg-emerald-50 text-emerald-700 rounded text-xs font-medium">Profiled</span>
                        : <span className="px-2 py-0.5 bg-amber-50 text-amber-700 rounded text-xs font-medium">Unprofiled</span>
                      }
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  )
}
