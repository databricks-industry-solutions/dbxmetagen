import React, { useState } from 'react'
import { TERMINAL_STATES } from '../hooks/useJobRunner'

// Shared job-run row used by both the Generate Metadata page (core runs) and the
// Semantic Layer advanced section (pipeline runs). Includes a Stop control for
// in-progress runs (#229.2); pass onCancel to enable it.

const STATUS_LABELS = {
  'RUNNING': 'Running', 'PENDING': 'Queued', 'SKIPPED': 'Skipped', 'INTERNAL_ERROR': 'Internal Error',
  'SUCCESS': 'Succeeded', 'FAILED': 'Failed', 'TIMEDOUT': 'Timed Out', 'CANCELLED': 'Cancelled',
}

export function stateBadge(state, result) {
  if (!state) return null
  let color = 'bg-dbx-oat text-slate-600 dark:bg-dbx-navy-500 dark:text-slate-300'
  if (state === 'RUNNING' || state === 'PENDING') color = 'bg-blue-50 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300'
  if (state === 'TERMINATED' && result === 'SUCCESS') color = 'bg-emerald-50 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300'
  if (state === 'TERMINATED' && result === 'FAILED') color = 'bg-red-50 text-red-700 dark:bg-red-900/40 dark:text-red-300'
  if (state === 'TERMINATED' && result === 'TIMEDOUT') color = 'bg-amber-50 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300'
  if (state === 'TERMINATED' && result === 'CANCELLED') color = 'bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400'
  if (state === 'SKIPPED') color = 'bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400'
  if (state === 'INTERNAL_ERROR') color = 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300'
  const label = result ? (STATUS_LABELS[result] || result) : (STATUS_LABELS[state] || state)
  return <span className={`badge ${color}`}>{label}</span>
}

function runAccentColor(run) {
  if (!TERMINAL_STATES.has(run.state)) return 'border-l-blue-400'
  if (run.result === 'SUCCESS') return 'border-l-emerald-400'
  if (run.result === 'FAILED') return 'border-l-red-400'
  return 'border-l-slate-300 dark:border-l-slate-600'
}

function TaskProgress({ tasks }) {
  if (!tasks || tasks.length === 0) return null
  const done = tasks.filter(t => TERMINAL_STATES.has(t.state)).length
  const failed = tasks.filter(t => t.result === 'FAILED').length
  return (
    <div className="mt-2.5">
      <div className="flex items-center gap-2 mb-1.5">
        <span className="text-xs text-slate-500 dark:text-slate-400">Tasks: {done}/{tasks.length}</span>
        {failed > 0 && <span className="text-xs text-red-600 dark:text-red-400 font-medium">{failed} failed</span>}
      </div>
      <div className="flex gap-1">
        {tasks.map(t => {
          let bg = 'bg-slate-200 dark:bg-dbx-navy-500'
          if (t.state === 'RUNNING' || t.state === 'PENDING') bg = 'bg-blue-400 animate-pulse'
          if (t.result === 'SUCCESS') bg = 'bg-emerald-500'
          if (t.result === 'FAILED') bg = 'bg-red-500'
          if (t.result === 'EXCLUDED') bg = 'bg-slate-300 dark:bg-dbx-navy-400'
          return <div key={t.task_key} className={`h-2.5 flex-1 rounded-full ${bg}`} title={`${t.task_key}: ${t.state}${t.result ? ' / ' + t.result : ''}`} />
        })}
      </div>
    </div>
  )
}

export default function RunEntry({ run, onCancel }) {
  const [expanded, setExpanded] = useState(false)
  const hasTasks = run.tasks && run.tasks.length > 0
  const cancellable = onCancel && !TERMINAL_STATES.has(run.state)
  return (
    <div className={`py-3 px-4 border-l-4 ${runAccentColor(run)} border-b border-dbx-oat-dark/30 dark:border-dbx-navy-400/20 last:border-b-0`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2.5 min-w-0">
          <span className="text-sm font-medium truncate text-slate-800 dark:text-slate-200">{run.job_name || 'Unnamed Job'}</span>
          {stateBadge(run.state, run.result)}
          {!TERMINAL_STATES.has(run.state) && (
            <span className="inline-block w-2 h-2 bg-blue-500 rounded-full animate-pulse" title="Checking status..." />
          )}
        </div>
        <div className="flex items-center gap-3 shrink-0">
          {cancellable && (
            <button onClick={() => { if (confirm(`Stop run #${run.run_id}? Work already applied is not rolled back.`)) onCancel(run.run_id) }}
              disabled={run._cancelling}
              className="text-xs font-medium text-red-600 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300 disabled:opacity-50"
              aria-label={`Stop run ${run.run_id}`}>
              {run._cancelling ? 'Stopping…' : 'Stop'}
            </button>
          )}
          {run.run_page_url && (
            <a href={run.run_page_url} target="_blank" rel="noopener noreferrer"
              className="text-xs text-dbx-teal hover:text-dbx-teal/80 font-medium">View in Databricks</a>
          )}
          {hasTasks && (
            <button onClick={() => setExpanded(prev => !prev)} className="text-xs text-slate-500 hover:text-slate-700 dark:hover:text-slate-300 font-medium" aria-label={expanded ? 'Hide tasks' : 'Show tasks'}>
              {expanded ? 'Hide tasks' : `${run.tasks.length} tasks`}
            </button>
          )}
          <span className="text-xs text-slate-400 font-mono">#{run.run_id}</span>
        </div>
      </div>
      {run.state_message && run.result === 'FAILED' && (
        <div className="mt-2 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-lg px-3 py-2 break-words">
          {run.state_message}
        </div>
      )}
      {hasTasks && !expanded && <TaskProgress tasks={run.tasks} />}
      {hasTasks && expanded && (
        <div className="mt-2.5 space-y-1 animate-slide-up">
          {run.tasks.map(t => (
            <div key={t.task_key} className="flex items-center justify-between text-xs px-3 py-1.5 bg-dbx-oat-light dark:bg-dbx-navy-500/50 rounded-lg">
              <span className="font-mono text-slate-600 dark:text-slate-300">{t.task_key}</span>
              {stateBadge(t.state, t.result)}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
