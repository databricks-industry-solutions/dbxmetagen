import React, { useState, useEffect } from 'react'
import { safeFetch, ErrorBanner } from '../App'

export default function Ontology() {
  const [entities, setEntities] = useState([])
  const [summary, setSummary] = useState([])
  const [error, setError] = useState(null)

  useEffect(() => {
    safeFetch('/api/ontology/entities').then(r => { setEntities(r.data); if (r.error) setError(r.error) })
    safeFetch('/api/ontology/summary').then(r => { setSummary(r.data); if (r.error) setError(r.error) })
  }, [])

  return (
    <div className="space-y-6">
      <ErrorBanner error={error} />
      <section className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-800 mb-4">Entity Type Summary</h2>
        {summary.length === 0
          ? <p className="text-sm text-slate-400">No entity types discovered yet. Run the full analytics pipeline first.</p>
          : <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {summary.map((s, i) => (
                <div key={i} className="border border-slate-200 rounded-xl p-4 bg-gradient-to-br from-white to-slate-50">
                  <p className="font-semibold text-sm text-slate-700">{s.entity_type}</p>
                  <p className="text-3xl font-bold text-indigo-700 mt-1">{s.count}</p>
                  <p className="text-xs text-slate-400 mt-1">Avg conf: {s.avg_confidence} | Validated: {s.validated}</p>
                </div>
              ))}
            </div>
        }
      </section>

      <section className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-800 mb-4">Discovered Entities</h2>
        {entities.length === 0
          ? <p className="text-sm text-slate-400">No entities discovered yet.</p>
          : <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead><tr>
                  {['Entity', 'Type', 'Confidence', 'Validated', 'Source Tables', 'Description'].map(h =>
                    <th key={h} className="text-left px-3 py-2.5 bg-slate-50 font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
                </tr></thead>
                <tbody>
                  {entities.map((e, i) => (
                    <tr key={i} className="border-b border-slate-100 hover:bg-indigo-50/30 transition-colors">
                      <td className="px-3 py-2 font-medium text-slate-700">{e.entity_name}</td>
                      <td className="px-3 py-2">
                        <span className="inline-block bg-indigo-100 text-indigo-700 text-xs font-medium px-2 py-0.5 rounded-full">{e.entity_type}</span>
                      </td>
                      <td className="px-3 py-2 text-slate-600">{e.confidence}</td>
                      <td className="px-3 py-2">
                        {e.validated === 'true' || e.validated === true
                          ? <span className="text-emerald-600 font-medium">Yes</span>
                          : <span className="text-slate-400">No</span>}
                      </td>
                      <td className="px-3 py-2 max-w-xs truncate text-slate-500">{e.source_tables}</td>
                      <td className="px-3 py-2 max-w-sm truncate text-slate-500">{e.description}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
        }
      </section>
    </div>
  )
}
