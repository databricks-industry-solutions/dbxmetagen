import React, { useState, useEffect } from 'react'

export default function BatchJobs() {
  const [jobs, setJobs] = useState([])
  const [tableNames, setTableNames] = useState('')
  const [mode, setMode] = useState('comment')
  const [applyDdl, setApplyDdl] = useState(false)
  const [runStatus, setRunStatus] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    fetch('/api/jobs').then(r => r.json()).then(setJobs).catch(() => {})
  }, [])

  const runJob = async (jobName) => {
    if (!tableNames.trim()) return
    setLoading(true)
    try {
      const res = await fetch('/api/jobs/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_name: jobName, table_names: tableNames, mode, apply_ddl: applyDdl }),
      })
      const data = await res.json()
      setRunStatus({ ...data, job_name: jobName, state: 'PENDING' })
    } finally { setLoading(false) }
  }

  const checkStatus = async () => {
    if (!runStatus?.run_id) return
    const res = await fetch(`/api/jobs/${runStatus.run_id}/status`)
    const data = await res.json()
    setRunStatus(prev => ({ ...prev, ...data }))
  }

  return (
    <div className="space-y-6">
      <section className="bg-white rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-4">Run Metadata Generation</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Table Names</label>
            <textarea
              value={tableNames}
              onChange={e => setTableNames(e.target.value)}
              placeholder="catalog.schema.table1, catalog.schema.*"
              className="w-full border rounded-md p-2 text-sm h-24"
            />
          </div>
          <div className="space-y-3">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Mode</label>
              <select value={mode} onChange={e => setMode(e.target.value)} className="w-full border rounded-md p-2 text-sm">
                <option value="comment">Comment</option>
                <option value="pi">PI Classification</option>
                <option value="domain">Domain Classification</option>
              </select>
            </div>
            <label className="flex items-center gap-2 text-sm">
              <input type="checkbox" checked={applyDdl} onChange={e => setApplyDdl(e.target.checked)} />
              Apply DDL to tables
            </label>
          </div>
        </div>
        <div className="flex gap-3 mt-4">
          <button onClick={() => runJob('dbxmetagen_metadata_job')} disabled={loading}
            className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm hover:bg-blue-700 disabled:opacity-50">
            {loading ? 'Starting...' : 'Run Single Mode'}
          </button>
          <button onClick={() => runJob('dbxmetagen_parallel_modes_job')} disabled={loading}
            className="px-4 py-2 bg-indigo-600 text-white rounded-md text-sm hover:bg-indigo-700 disabled:opacity-50">
            Run All 3 Modes (Parallel)
          </button>
          <button onClick={() => runJob('dbxmetagen_full_analytics_pipeline')} disabled={loading}
            className="px-4 py-2 bg-emerald-600 text-white rounded-md text-sm hover:bg-emerald-700 disabled:opacity-50">
            Full Analytics Pipeline
          </button>
        </div>
      </section>

      {runStatus && (
        <section className="bg-white rounded-lg border p-6">
          <div className="flex items-center justify-between mb-2">
            <h3 className="font-medium">Run Status</h3>
            <button onClick={checkStatus} className="text-sm text-blue-600 hover:underline">Refresh</button>
          </div>
          <div className="text-sm space-y-1">
            <p><span className="text-gray-500">Job:</span> {runStatus.job_name}</p>
            <p><span className="text-gray-500">Run ID:</span> {runStatus.run_id}</p>
            <p><span className="text-gray-500">State:</span>{' '}
              <span className={runStatus.state === 'TERMINATED' ? 'text-green-600 font-medium' : 'text-yellow-600 font-medium'}>
                {runStatus.state}
              </span>
              {runStatus.result && ` - ${runStatus.result}`}
            </p>
          </div>
        </section>
      )}

      <section className="bg-white rounded-lg border p-6">
        <h2 className="text-lg font-semibold mb-3">Available Jobs</h2>
        {jobs.length === 0 ? (
          <p className="text-sm text-gray-500">No jobs found. Deploy with Databricks Asset Bundles first.</p>
        ) : (
          <div className="divide-y">
            {jobs.map(j => (
              <div key={j.job_id} className="py-2 flex justify-between items-center text-sm">
                <span>{j.name}</span>
                <span className="text-gray-400">#{j.job_id}</span>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  )
}
