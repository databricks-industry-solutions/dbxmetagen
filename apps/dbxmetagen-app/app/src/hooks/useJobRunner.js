import { useState, useEffect, useRef, useCallback, createContext, useContext, createElement } from 'react'

// Terminal Databricks run lifecycle states — a run in one of these is finished
// (success or otherwise) and no longer polled.
export const TERMINAL_STATES = new Set(['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR'])

/**
 * Shared job run + poll machinery for triggering dbxmetagen Databricks jobs and
 * tracking their live status. Extracted from BatchJobs so both the Generate
 * Metadata section and the Semantic Layer foundation gate drive jobs through one
 * implementation (single source of truth for POST /api/jobs/run and status polling).
 *
 * Loads the job list (/api/jobs) and recent run history (/api/jobs/runs) on mount,
 * polls active runs every 5s, and exposes helpers to start a job by name-suffix.
 *
 * @param {object} [opts]
 * @param {(msg: string) => void} [opts.onJobsError] - called if the job list fails to load
 * @returns {{
 *   jobs: any[], runHistory: any[], activeRuns: any[], completedRuns: any[],
 *   runningAction: string|null, runError: string|null, setRunError: Function,
 *   findJob: (suffix: string) => any, runJob: Function, refreshRuns: Function,
 * }}
 */
export function useJobRunner({ onJobsError } = {}) {
  const [jobs, setJobs] = useState([])
  const [runHistory, setRunHistory] = useState([])
  const [runningAction, setRunningAction] = useState(null)
  const [runError, setRunError] = useState(null)
  const [jobsError, setJobsError] = useState(null)
  const pollRef = useRef(null)
  const runHistoryRef = useRef(runHistory)
  const onJobsErrorRef = useRef(onJobsError)

  useEffect(() => { onJobsErrorRef.current = onJobsError }, [onJobsError])
  useEffect(() => { runHistoryRef.current = runHistory }, [runHistory])

  // Load job list + recent run history on mount.
  useEffect(() => {
    fetch('/api/jobs')
      .then(r => { if (!r.ok) throw new Error(`${r.status} ${r.statusText}`); return r.json() })
      .then(setJobs)
      .catch(e => { const m = `Failed to load jobs: ${e.message}`; setJobsError(m); onJobsErrorRef.current?.(m) })
    fetch('/api/jobs/runs')
      .then(r => r.ok ? r.json() : [])
      .then(runs => setRunHistory(runs.map(r => ({ ...r, _polling: false }))))
      .catch(() => { /* run history is non-critical; leave empty */ })
  }, [])

  const pollActiveRuns = useCallback(async () => {
    const active = runHistoryRef.current.filter(r => !TERMINAL_STATES.has(r.state))
    if (active.length === 0) return
    const updates = await Promise.all(active.map(async (r) => {
      try {
        const res = await fetch(`/api/jobs/${r.run_id}/status`)
        if (!res.ok) return null
        return await res.json()
      } catch { return null }
    }))
    setRunHistory(prev => prev.map(r => {
      const upd = updates.find(u => u && u.run_id === r.run_id)
      return upd ? { ...r, ...upd } : r
    }))
  }, [])

  // Poll active runs every 5s while any run is non-terminal.
  useEffect(() => {
    const hasActive = runHistory.some(r => !TERMINAL_STATES.has(r.state))
    if (hasActive && !pollRef.current) {
      pollRef.current = setInterval(pollActiveRuns, 5000)
    } else if (!hasActive && pollRef.current) {
      clearInterval(pollRef.current)
      pollRef.current = null
    }
    return () => { if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null } }
  }, [runHistory, pollActiveRuns])

  const findJob = useCallback((suffix) => jobs.find(j => j.name?.endsWith(suffix)), [jobs])

  // Start a job by job-name suffix (resolved to job_id when the job list is
  // loaded). Returns the optimistic run object (with run_id) on success, or null.
  const runJob = useCallback(async (jobNameSuffix, params = {}, actionKey = 'default') => {
    setRunningAction(actionKey)
    setRunError(null)
    let newRun = null
    try {
      const match = jobs.find(j => j.name?.endsWith(jobNameSuffix))
      const body = match ? { job_id: match.job_id, ...params } : { job_name: jobNameSuffix, ...params }
      const res = await fetch('/api/jobs/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) { setRunError(data.detail || `Failed to start job (${res.status})`); setRunningAction(null); return null }
      newRun = {
        ...data,
        job_name: match?.name || jobNameSuffix,
        state: 'PENDING', result: null, tasks: [],
        run_page_url: null, state_message: null,
      }
      setRunHistory(prev => [newRun, ...prev])
    } catch (e) { setRunError(e.message) }
    setRunningAction(null)
    return newRun
  }, [jobs])

  const refreshRuns = useCallback(() => {
    fetch('/api/jobs/runs')
      .then(r => r.ok ? r.json() : [])
      .then(runs => setRunHistory(runs.map(r => ({ ...r, _polling: false }))))
      .catch(() => {})
  }, [])

  const activeRuns = runHistory.filter(r => !TERMINAL_STATES.has(r.state))
  const completedRuns = runHistory.filter(r => TERMINAL_STATES.has(r.state))

  return {
    jobs, runHistory, activeRuns, completedRuns,
    runningAction, runError, setRunError, jobsError,
    findJob, runJob, refreshRuns, setRunHistory,
  }
}

const JobRunnerContext = createContext(null)

/**
 * App-level provider so every screen shares ONE job runner instance: one
 * /api/jobs + /api/jobs/runs load, one 5s poll, and a single run-history list
 * (a run launched anywhere shows up everywhere). Wrap the app once.
 * (createElement, not JSX, because this is a .js module.)
 */
export function JobRunnerProvider({ children }) {
  const runner = useJobRunner()
  return createElement(JobRunnerContext.Provider, { value: runner }, children)
}

/** Consume the shared runner. Requires <JobRunnerProvider> above it (mounted
 *  once in main.jsx); throws a clear error otherwise instead of a cryptic
 *  destructure-of-null. */
export function useSharedJobRunner() {
  const ctx = useContext(JobRunnerContext)
  if (!ctx) throw new Error('useSharedJobRunner must be used within <JobRunnerProvider>')
  return ctx
}

