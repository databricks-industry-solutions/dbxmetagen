import React, { useState, useEffect } from 'react'

export default function Ontology() {
  const [entities, setEntities] = useState([])
  const [summary, setSummary] = useState([])

  useEffect(() => {
    fetch('/api/ontology/entities').then(r => r.json()).then(setEntities).catch(() => {})
    fetch('/api/ontology/summary').then(r => r.json()).then(setSummary).catch(() => {})
  }, [])

  return (
    <div className="space-y-6">
      <section className="bg-white rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-4">Entity Type Summary</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {summary.map((s, i) => (
            <div key={i} className="border rounded-lg p-3">
              <p className="font-medium text-sm">{s.entity_type}</p>
              <p className="text-2xl font-bold mt-1">{s.count}</p>
              <p className="text-xs text-gray-500">Avg conf: {s.avg_confidence} | Validated: {s.validated}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="bg-white rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-4">Discovered Entities</h2>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead><tr>
              {['Entity', 'Type', 'Confidence', 'Validated', 'Source Tables', 'Description'].map(h =>
                <th key={h} className="text-left px-3 py-2 bg-gray-50 font-medium text-gray-600 border-b">{h}</th>)}
            </tr></thead>
            <tbody>
              {entities.map((e, i) => (
                <tr key={i} className="border-b hover:bg-gray-50">
                  <td className="px-3 py-2 font-medium">{e.entity_name}</td>
                  <td className="px-3 py-2">
                    <span className="inline-block bg-blue-100 text-blue-800 text-xs px-2 py-0.5 rounded">{e.entity_type}</span>
                  </td>
                  <td className="px-3 py-2">{e.confidence}</td>
                  <td className="px-3 py-2">
                    {e.validated === 'true' || e.validated === true
                      ? <span className="text-green-600">Yes</span>
                      : <span className="text-gray-400">No</span>}
                  </td>
                  <td className="px-3 py-2 max-w-xs truncate">{e.source_tables}</td>
                  <td className="px-3 py-2 max-w-sm truncate">{e.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <p className="text-xs text-gray-400">
        Note: ontology_metrics table is a stub for future Unity Catalog metric views integration.
      </p>
    </div>
  )
}
