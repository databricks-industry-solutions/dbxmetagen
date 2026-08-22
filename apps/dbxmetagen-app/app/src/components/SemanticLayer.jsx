import React, { useState, useEffect, useRef } from 'react'
import { ErrorBanner } from '../App'
import { cachedFetch, cachedFetchObj, invalidateCache, TTL } from '../apiCache'
import { PageHeader, EmptyState, Skeleton, Section } from './ui'
import { useCatalogSchemaTables } from '../hooks/useCatalogSchemaTables'
import { useSharedJobRunner } from '../hooks/useJobRunner'
import AdvancedPipelinePanel from './AdvancedPipelinePanel'
import ErdDesigner from './ErdDesigner'

const STAGES = {
  starting: 'Starting...',
  building_context: 'Building metadata context...',
  planning: 'Planning generation strategy...',
  generating: 'Generating metric view definitions...',
  retrying: 'Retrying failed views with simplified prompts...',
  checking_coverage: 'Checking question coverage...',
  validating: 'Validating expressions...',
  done: 'Complete',
}

const MAT_STORAGE_KEY = 'dbxmetagen.sl.materialize'
const MAT_SCHEDULE_KEY = 'dbxmetagen.sl.matSchedule'
const MAT_PRESET_KEY = 'dbxmetagen.sl.matPreset'
const MAT_SCHEDULE_PRESETS = [
  { id: '6h', label: 'Every 6 hours', value: 'every 6 hours' },
  { id: '12h', label: 'Every 12 hours', value: 'every 12 hours' },
  { id: '1d', label: 'Every 1 day', value: 'every 1 day' },
  { id: 'manual', label: 'Manual only', value: '' },
  { id: 'custom', label: 'Custom...', value: '__custom__' },
]

function matBadge(hasMat, schedule) {
  if (hasMat) {
    const sched = schedule || 'manual refresh'
    return (
      <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-sky-100 text-sky-700 dark:bg-sky-900/30 dark:text-sky-300"
        title={`Materialized baseline MV; schedule: ${sched}`}>
        Materialized{schedule ? ` (${schedule})` : ''}
      </span>
    )
  }
  return (
    <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400"
      title="No materialization block in definition">
      No materialization
    </span>
  )
}

// ---------------------------------------------------------------------------
// MV Analysis Panel
// ---------------------------------------------------------------------------

const _issueSevStyles = {
  high: 'text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30',
  medium: 'text-amber-700 dark:text-amber-400 bg-amber-100 dark:bg-amber-900/30',
  low: 'text-slate-700 dark:text-slate-400 bg-slate-100 dark:bg-slate-900/30',
}

// Refinement actions Analyze can surface (issue.action -> button label + focus).
const _REFINE_ACTIONS = {
  add_measures: { label: 'Add measures', focus: 'add_measures' },
  add_dimensions: { label: 'Add dimensions', focus: 'add_dimensions' },
  check_filters: { label: 'Check filters', focus: 'check_filters' },
}

function MvAnalysisPanel({ issues, onClose, onApplyFix, onRefine, appliedFields, busy }) {
  const applied = appliedFields || new Set()
  if (!issues || issues.length === 0) return (
    <div className="mt-2 p-3 bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-200 dark:border-emerald-700 rounded text-xs text-emerald-700 dark:text-emerald-400">
      No issues found.
      <button onClick={onClose} className="ml-2 text-slate-400 hover:text-slate-600">Close</button>
    </div>
  )
  const fixable = issues.filter(iss => iss.field && iss.fix_value && !applied.has(iss.field))
  // `busy` is the raw actionLoading value (a string like 'apply-fix' / a focus, or
  // falsy). applyBusy is true only while an Apply-fix is in flight.
  const applyBusy = busy === 'apply-fix'
  const anyBusy = !!busy
  // Distinct refinement actions present in the issues, in a stable order.
  const refineActions = onRefine
    ? Object.keys(_REFINE_ACTIONS).filter(a => issues.some(iss => iss.action === a))
    : []
  return (
    <div className="mt-2 p-3 border border-cyan-200 dark:border-cyan-700 rounded space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium text-slate-700 dark:text-slate-300">Analysis Issues ({issues.length})</span>
        <div className="flex items-center gap-2">
          {fixable.length > 0 && onApplyFix && (
            <button onClick={() => onApplyFix('__all__')} disabled={anyBusy}
              className="px-2 py-0.5 text-[10px] bg-cyan-600 text-white rounded hover:bg-cyan-700 disabled:opacity-50">
              {applyBusy ? 'Applying...' : `Apply All (${fixable.length})`}
            </button>
          )}
          <button onClick={onClose} className="text-xs text-slate-400 hover:text-slate-600">Close</button>
        </div>
      </div>
      {refineActions.length > 0 && (
        <div className="flex flex-wrap items-center gap-2 pb-1 border-b border-slate-100 dark:border-slate-700">
          <span className="text-[10px] text-slate-400 dark:text-slate-500">Refine:</span>
          {refineActions.map(a => (
            <button key={a} onClick={() => onRefine(_REFINE_ACTIONS[a].focus)} disabled={!!busy}
              className="px-2 py-0.5 text-[10px] rounded border border-cyan-400 text-cyan-700 dark:text-cyan-300 hover:bg-cyan-50 dark:hover:bg-cyan-900/30 disabled:opacity-50">
              {busy === _REFINE_ACTIONS[a].focus ? 'Working…' : _REFINE_ACTIONS[a].label}
            </button>
          ))}
        </div>
      )}
      {issues.map((iss, i) => {
        const isApplied = iss.field && applied.has(iss.field)
        return (
          <div key={i} className={`flex items-start gap-2 text-xs py-1 ${isApplied ? 'opacity-50' : ''}`}>
            {isApplied ? (
              <span className="px-1.5 py-0.5 rounded font-medium shrink-0 text-emerald-700 dark:text-emerald-400 bg-emerald-100 dark:bg-emerald-900/30">fixed</span>
            ) : (
              <span className={`px-1.5 py-0.5 rounded font-medium shrink-0 ${_issueSevStyles[iss.severity] || _issueSevStyles.low}`}>{iss.severity}</span>
            )}
            <div className="flex-1 min-w-0">
              <p className={`text-slate-700 dark:text-slate-300 ${isApplied ? 'line-through' : ''}`}>{iss.message}</p>
              {iss.field && <p className="text-slate-400 text-[10px]">Field: {iss.field}</p>}
              {iss.suggestion && !isApplied && (
                <div className="flex items-center gap-2 mt-1">
                  <p className="text-slate-500 dark:text-slate-400 italic flex-1">{iss.suggestion}</p>
                  {iss.field && iss.fix_value && onApplyFix && (
                    <button onClick={() => onApplyFix(iss.field, iss.fix_value)} disabled={anyBusy}
                      className="shrink-0 px-1.5 py-0.5 text-[10px] bg-cyan-600 text-white rounded hover:bg-cyan-700 disabled:opacity-50">
                      {applyBusy ? '...' : 'Apply'}</button>
                  )}
                </div>
              )}
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ---------------------------------------------------------------------------
// MV Test-query results panel (item 23)
// ---------------------------------------------------------------------------

const _testStatusStyles = {
  ok: 'text-emerald-700 dark:text-emerald-400 bg-emerald-100 dark:bg-emerald-900/30',
  warn: 'text-amber-700 dark:text-amber-400 bg-amber-100 dark:bg-amber-900/30',
  fail: 'text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30',
}

function MvTestResultsPanel({ data, onClose, onRunFederatedFull, busy }) {
  const [openSql, setOpenSql] = useState(null)
  const results = data.results || []
  const summary = data.summary || {}
  const running = data.status === 'running'
  const overallCls = _testStatusStyles[data.overall] || _testStatusStyles.warn
  return (
    <div className="mt-2 p-3 border border-teal-200 dark:border-teal-700 rounded space-y-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-slate-700 dark:text-slate-300">Test Queries</span>
          {running ? (
            <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400 inline-flex items-center gap-1">
              <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg>
              Running {data.done || 0}/{data.total || 0}
            </span>
          ) : (
            <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${overallCls}`}>
              {summary.passed || 0} passed · {summary.warned || 0} warn · {summary.failed || 0} failed
            </span>
          )}
        </div>
        <button onClick={onClose} className="text-xs text-slate-400 hover:text-slate-600">Close</button>
      </div>
      {data.federation_note && (
        <div className="text-[10px] text-amber-700 dark:text-amber-400 bg-amber-50 dark:bg-amber-900/20 rounded px-2 py-1 flex items-center justify-between gap-2">
          <span>{data.federation_note}</span>
          {!data.allow_federated_full && onRunFederatedFull && !running && (
            <button onClick={onRunFederatedFull} disabled={busy}
              title="Run all drills against the federated source. Each aggregation may pull the remote table if it does not push down."
              className="shrink-0 px-1.5 py-0.5 rounded border border-amber-400 text-amber-700 dark:text-amber-300 hover:bg-amber-100 dark:hover:bg-amber-800/30 disabled:opacity-50">
              Run full set anyway
            </button>
          )}
        </div>
      )}
      {data.stopped && (
        <p className="text-[10px] text-amber-700 dark:text-amber-400 bg-amber-50 dark:bg-amber-900/20 rounded px-2 py-1">
          {data.stopped}
        </p>
      )}
      {!running && results.length === 0 && !data.stopped && (
        <p className="text-xs text-slate-400">No test queries were run.</p>
      )}
      {results.map((r, i) => {
        const st = (r.health && r.health.status) || (r.error ? 'fail' : 'ok')
        const cols = r.sample_result && r.sample_result.length ? Object.keys(r.sample_result[0]) : []
        return (
          <div key={i} className="text-xs border-t border-slate-100 dark:border-slate-700 pt-1.5 first:border-t-0">
            <div className="flex items-start gap-2">
              <span className={`px-1.5 py-0.5 rounded font-medium shrink-0 ${_testStatusStyles[st] || _testStatusStyles.warn}`}>{st}</span>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="text-slate-700 dark:text-slate-300 font-medium">{r.label}</span>
                  <span className="text-slate-400 text-[10px]">{r.row_count} row(s)</span>
                  <button onClick={() => setOpenSql(openSql === i ? null : i)}
                    className="text-teal-600 dark:text-teal-400 text-[10px] hover:underline">
                    {openSql === i ? 'hide SQL' : 'show SQL'}
                  </button>
                </div>
                {(r.health?.notes || []).map((n, j) => (
                  <p key={j} className="text-slate-500 dark:text-slate-400 text-[10px]">{n}</p>
                ))}
                {r.error && (
                  <p className="text-red-600 dark:text-red-400 text-[10px] whitespace-pre-wrap break-words mt-0.5">{r.error}</p>
                )}
                {openSql === i && (
                  <pre className="mt-1 bg-dbx-oat dark:bg-gray-900 border dark:border-gray-600 rounded p-2 text-[10px] overflow-x-auto dark:text-gray-200">{r.sql}</pre>
                )}
                {cols.length > 0 && (
                  <div className="mt-1 overflow-x-auto">
                    <table className="text-[10px] border-collapse">
                      <thead>
                        <tr>{cols.map(c => (
                          <th key={c} className="text-left px-1.5 py-0.5 border dark:border-gray-600 text-slate-500 dark:text-slate-400 font-medium">{c}</th>
                        ))}</tr>
                      </thead>
                      <tbody>
                        {r.sample_result.slice(0, 5).map((row, ri) => (
                          <tr key={ri}>{cols.map(c => (
                            <td key={c} className="px-1.5 py-0.5 border dark:border-gray-700 text-slate-600 dark:text-slate-300 whitespace-nowrap">
                              {row[c] === null || row[c] === undefined ? <span className="text-slate-300 dark:text-slate-600 italic">null</span> : String(row[c])}
                            </td>
                          ))}</tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ---------------------------------------------------------------------------
// MV Structured Editor
// ---------------------------------------------------------------------------

function MvStructuredEditor({ defn, setDefn, onSave, onCancel }) {
  const update = (path, val) => {
    const d = JSON.parse(JSON.stringify(defn))
    const tokens = path.split('.')
    let cur = d
    for (let i = 0; i < tokens.length - 1; i++) {
      const t = tokens[i]
      const m = t.match(/^(\w+)\[(\d+)\]$/)
      if (m) { cur = cur[m[1]][parseInt(m[2])] }
      else { cur = cur[t] }
    }
    const last = tokens[tokens.length - 1]
    const lm = last.match(/^(\w+)\[(\d+)\]$/)
    if (lm) { cur[lm[1]][parseInt(lm[2])] = val }
    else { cur[last] = val }
    setDefn(d)
  }

  const SmallInput = ({ label, value, onChange, className = '', textarea = false }) => {
    const Tag = textarea ? 'textarea' : 'input'
    return (
      <label className="block text-[10px] text-slate-500 dark:text-slate-400">
        {label}
        <Tag value={value || ''} onChange={e => onChange(e.target.value)}
          className={`mt-0.5 block w-full px-2 py-1 text-xs border rounded dark:bg-gray-900 dark:border-gray-600 dark:text-gray-200 ${textarea ? 'min-h-[48px] resize-y' : ''} ${className}`}
          rows={textarea ? 2 : undefined} />
      </label>
    )
  }

  const ItemRow = ({ type, idx, item }) => (
    <div className="flex flex-wrap gap-2 py-1.5 border-b border-slate-100 dark:border-gray-700 last:border-0">
      <SmallInput label="Name" value={item.name} onChange={v => update(`${type}[${idx}].name`, v)} className="w-28" />
      <SmallInput label="Expression" value={item.expr} onChange={v => update(`${type}[${idx}].expr`, v)} className="w-48 font-mono" />
      <SmallInput label="Comment" value={item.comment} onChange={v => update(`${type}[${idx}].comment`, v)} className="flex-1" />
      <SmallInput label="Synonyms (comma-sep)" value={Array.isArray(item.synonyms) ? item.synonyms.join(', ') : (item.synonyms || '')}
        onChange={v => update(`${type}[${idx}].synonyms`, v.split(',').map(s => s.trim()).filter(Boolean))} className="w-36" />
    </div>
  )

  const addItem = (type) => {
    const d = JSON.parse(JSON.stringify(defn))
    if (!d[type]) d[type] = []
    d[type].push({ name: '', expr: '', comment: '' })
    setDefn(d)
  }

  const removeItem = (type, idx) => {
    const d = JSON.parse(JSON.stringify(defn))
    d[type].splice(idx, 1)
    setDefn(d)
  }

  return (
    <div className="bg-dbx-oat dark:bg-gray-900 border dark:border-gray-600 rounded p-3 space-y-3 text-xs">
      <SmallInput label="Source table" value={defn.source} className="font-mono bg-slate-50 dark:bg-gray-800 cursor-default" onChange={() => {}} />
      <SmallInput label="Comment" value={defn.comment} onChange={v => update('comment', v)} textarea />

      {/* Dimensions */}
      <div>
        <div className="flex items-center justify-between mb-1">
          <span className="font-medium text-slate-700 dark:text-slate-300">Dimensions ({(defn.dimensions || []).length})</span>
          <button onClick={() => addItem('dimensions')} className="text-[10px] text-blue-600 hover:underline">+ Add</button>
        </div>
        {(defn.dimensions || []).map((dim, i) => (
          <div key={i} className="relative group">
            <ItemRow type="dimensions" idx={i} item={dim} />
            <button onClick={() => removeItem('dimensions', i)} className="absolute -right-1 top-0 hidden group-hover:block text-red-400 hover:text-red-600 text-xs">x</button>
          </div>
        ))}
      </div>

      {/* Measures */}
      <div>
        <div className="flex items-center justify-between mb-1">
          <span className="font-medium text-slate-700 dark:text-slate-300">Measures ({(defn.measures || []).length})</span>
          <button onClick={() => addItem('measures')} className="text-[10px] text-blue-600 hover:underline">+ Add</button>
        </div>
        {(defn.measures || []).map((meas, i) => (
          <div key={i} className="relative group">
            <ItemRow type="measures" idx={i} item={meas} />
            <button onClick={() => removeItem('measures', i)} className="absolute -right-1 top-0 hidden group-hover:block text-red-400 hover:text-red-600 text-xs">x</button>
          </div>
        ))}
      </div>

      {/* Joins */}
      {(defn.joins || []).length > 0 && (
        <div>
          <span className="font-medium text-slate-700 dark:text-slate-300">Joins ({defn.joins.length})</span>
          {defn.joins.map((j, i) => (
            <div key={i} className="flex flex-wrap gap-2 py-1.5 border-b border-slate-100 dark:border-gray-700">
              <SmallInput label="Name" value={j.name} onChange={v => update(`joins[${i}].name`, v)} className="w-28" />
              <SmallInput label="Source" value={j.source} onChange={v => update(`joins[${i}].source`, v)} className="w-48 font-mono" />
              <SmallInput label="On" value={j.on} onChange={v => update(`joins[${i}].on`, v)} className="flex-1 font-mono" />
            </div>
          ))}
        </div>
      )}

      {/* Filter */}
      <SmallInput label="Filter" value={defn.filter} onChange={v => update('filter', v)} className="font-mono" />

      <div className="flex justify-end gap-2 pt-2">
        <button onClick={onCancel} className="px-3 py-1 text-xs border rounded text-slate-600 hover:bg-slate-50 dark:text-slate-300 dark:hover:bg-gray-800">Cancel</button>
        <button onClick={onSave} className="px-3 py-1 text-xs bg-blue-600 text-white rounded hover:bg-blue-700">Save</button>
      </div>
    </div>
  )
}


function kpiMatchesTables(k, selectedTables) {
  const kt = Array.isArray(k.target_tables) ? k.target_tables : []
  if (!kt.length) return true
  const sel = selectedTables.map(t => t.toLowerCase())
  return kt.some(t => {
    const tl = t.toLowerCase()
    return sel.includes(tl) || sel.some(s => s.endsWith('.' + tl) || s === tl)
  })
}

/**
 * Derive the state of the two non-optional prerequisites for metric-view
 * generation from the holistic coverage stats. Metric views build on core
 * metadata (descriptions) AND the analytics pipeline (ontology / FK / vector
 * index), so both must be complete before generation is allowed.
 *
 * Returns null while stats are still loading (so the rail can stay quiet
 * rather than flash a false "not run" state).
 */
function deriveFoundation(pipelineStats) {
  if (!pipelineStats) return null
  const s = pipelineStats
  const metadataDone = (s.profiled || 0) > 0 && (s.with_comments || 0) > 0
  // The analytics pipeline's outputs: ontology entities, FK predictions, or a
  // populated vector index. Any one present means it has run for this scope.
  const analyticsDone =
    (s.entity_type_count || 0) > 0 ||
    (s.fk_count || 0) > 0 ||
    (s.vs_documents || 0) > 0
  return { metadataDone, analyticsDone, ready: metadataDone && analyticsDone, stats: s }
}

/**
 * Three-step readiness rail pinned above the Metric Views workflow. Shows the
 * fixed foundation → generate order (core metadata → analytics pipeline →
 * metric views) and deep-links to the Generate Metadata tab to resolve any
 * unmet prerequisite. Collapses to a thin confirmation once ready.
 */
function FoundationRail({ foundation, onNavigate, step2Runner }) {
  const [showRerun, setShowRerun] = useState(false)
  if (!foundation) return null
  const { metadataDone, analyticsDone, ready, stats } = foundation

  if (ready) {
    // Collapsed once the foundation is set — but keep a way to re-run the
    // analytics pipeline (e.g. after adding tables, switching ontology bundle,
    // or a partial prior run), since the inline first-run runner is gone here.
    return (
      <div className="rounded-lg border border-emerald-200 dark:border-emerald-800/40 bg-emerald-50/60 dark:bg-emerald-900/15 px-4 py-2 text-sm">
        <div className="flex items-center gap-2">
          <svg className="w-4 h-4 text-emerald-500 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
          </svg>
          <span className="text-slate-600 dark:text-slate-300">
            Foundation ready &mdash; core metadata and the analytics pipeline are in place. Generate metric views below.
          </span>
          {step2Runner && (
            <button onClick={() => setShowRerun(v => !v)}
              className="ml-auto text-xs font-semibold text-dbx-lava hover:underline shrink-0">
              {showRerun ? 'Hide' : 'Re-run advanced metadata'}
            </button>
          )}
        </div>
        {showRerun && step2Runner && (
          <div className="mt-3 pt-3 border-t border-emerald-200/70 dark:border-emerald-700/30">
            {step2Runner}
          </div>
        )}
      </div>
    )
  }

  const Step = ({ n, title, done, detail, actionLabel, actionTab, current }) => (
    <div className="flex-1 min-w-[200px]">
      <div className="flex items-center gap-2">
        <span className={`inline-flex items-center justify-center w-6 h-6 rounded-full text-xs font-semibold shrink-0 ${
          done ? 'bg-emerald-500 text-white'
            : current ? 'bg-dbx-lava text-white'
            : 'bg-slate-200 dark:bg-dbx-navy-500 text-slate-500 dark:text-slate-400'}`}>
          {done ? '✓' : n}
        </span>
        <span className={`text-sm font-medium ${done ? 'text-slate-700 dark:text-slate-200' : current ? 'text-dbx-lava' : 'text-slate-500 dark:text-slate-400'}`}>{title}</span>
      </div>
      <div className="pl-8 mt-0.5">
        <p className={`text-xs ${done ? 'text-emerald-600 dark:text-emerald-400' : 'text-amber-600 dark:text-amber-400'}`}>{detail}</p>
        {!done && actionLabel && actionTab && (
          <button onClick={() => onNavigate?.(actionTab)} className="mt-1 text-xs font-semibold text-dbx-lava hover:underline">
            {actionLabel} &rarr;
          </button>
        )}
      </div>
    </div>
  )

  const total = stats.total_tables || 0
  const profiled = stats.profiled || 0

  return (
    <div className="rounded-xl border border-amber-200 dark:border-amber-700/40 bg-amber-50/60 dark:bg-amber-900/10 px-4 py-3">
      <p className="text-xs font-semibold uppercase tracking-wider text-slate-400 dark:text-slate-500 mb-2">
        Metric views build on this foundation
      </p>
      <div className="flex flex-wrap gap-4 items-start">
        <Step
          n={1}
          title="Core metadata"
          done={metadataDone}
          current={!metadataDone}
          detail={metadataDone ? `${profiled}${total ? `/${total}` : ''} tables described` : 'Not generated yet'}
          actionLabel="Generate metadata"
          actionTab="jobs"
        />
        <div className="hidden sm:flex items-center text-slate-300 dark:text-slate-600 self-center">&rarr;</div>
        <Step
          n={2}
          title="Analytics pipeline"
          done={analyticsDone}
          current={metadataDone && !analyticsDone}
          detail={analyticsDone ? 'Ontology, FKs & index built' : (metadataDone ? 'Run it below' : 'Run it below (core metadata not detected — you can still run it)')}
        />
        <div className="hidden sm:flex items-center text-slate-300 dark:text-slate-600 self-center">&rarr;</div>
        <Step
          n={3}
          title="Metric views"
          done={false}
          current={metadataDone && analyticsDone}
          detail={metadataDone && analyticsDone ? 'Ready to generate' : 'Unlocks when 1 & 2 are done'}
        />
      </div>
      {/* Inline analytics-pipeline runner — shown whenever the pipeline hasn't
          produced its outputs yet, so the user never leaves the Semantic Layer to
          satisfy step 2. NOT hard-gated on core-metadata detection: the "core
          metadata" signal reads the knowledge base and can legitimately come back
          empty even when the user HAS run core metadata (e.g. an app service
          principal without system-catalog access, or a KB not yet built), so we
          always let them run advanced metadata — we just don't present it as
          emphatically as "the next step" until core metadata is detected. */}
      {!analyticsDone && step2Runner && (
        <div className="mt-3 pt-3 border-t border-amber-200/70 dark:border-amber-700/30">
          {step2Runner}
        </div>
      )}
    </div>
  )
}

export default function SemanticLayer({ onNavigate, pipelineStats, onRefreshPipelineStats }) {
  // Shared job runner — lets the foundation gate start + track the analytics
  // pipeline inline (see AdvancedPipelinePanel below).
  const jobRunner = useSharedJobRunner()
  const [pipelineServerless, setPipelineServerless] = useState(true)
  // Projects
  const [projects, setProjects] = useState([])
  const [selectedProjectId, setSelectedProjectId] = useState('')
  const [newProjectName, setNewProjectName] = useState('')
  const [showNewProject, setShowNewProject] = useState(false)

  // Table selection (shared cascade hook -- kbOnly filters to knowledge-base tables)
  const cst = useCatalogSchemaTables('', '', { kbOnly: true })
  const { catalogs, schemas, filtered: filteredTables, allSchemaTableCount, catalog: selectedCatalog, schema: selectedSchema, filter: tableFilter, setCatalog: setSelectedCatalog, setSchema: setSelectedSchema, setFilter: setTableFilter } = cst
  const allTables = cst.tables
  const [selectedTables, setSelectedTables] = useState([])

  // Profiles
  const [profiles, setProfiles] = useState([])
  const [activeProfileId, setActiveProfileId] = useState('')
  const [profileName, setProfileName] = useState('')
  const [questionsText, setQuestionsText] = useState('')
  const [businessContext, setBusinessContext] = useState('')
  const [bizCtxLoading, setBizCtxLoading] = useState(false)
  const [bizCtxUseKb, setBizCtxUseKb] = useState(true)
  // Genie SQL pull (items 14/15): pick an existing Genie space and pull its
  // curated example SQL to seed metric-view generation with proven query patterns.
  const [genieSpaces, setGenieSpaces] = useState(null)  // null=unloaded, []=loaded-empty
  const [genieSpacesLoading, setGenieSpacesLoading] = useState(false)
  const [genieSelectedSpaces, setGenieSelectedSpaces] = useState([])
  const [geniePullLoading, setGeniePullLoading] = useState(false)
  const [geniePullStatus, setGeniePullStatus] = useState(null)
  const [generationStyle, setGenerationStyle] = useState('comprehensive')
  const [maxViews, setMaxViews] = useState(null)
  const [erdSufficiency, setErdSufficiency] = useState(null)  // {metric_views_recommended, reasons, ...}
  const [genSufficiency, setGenSufficiency] = useState(null)  // {questions:{...}, kpis:{...}}
  const [materialize, setMaterialize] = useState(false)
  const [materializationSchedule, setMaterializationSchedule] = useState('every 6 hours')
  const [matSchedulePreset, setMatSchedulePreset] = useState('6h')
  const [defMatFilter, setDefMatFilter] = useState('all')
  const [deployMatPrefs, setDeployMatPrefs] = useState({})

  // Generation
  const [taskId, setTaskId] = useState(null)
  const [taskStatus, setTaskStatus] = useState(null)
  const pollRef = useRef(null)

  // Results
  const [definitions, setDefinitions] = useState([])
  const [expandedDef, setExpandedDef] = useState(null)
  const [expandedJson, setExpandedJson] = useState(null)

  // Per-definition action state
  const [actionLoading, setActionLoading] = useState({})
  const [globalTargetOverride, setGlobalTargetOverride] = useState('')
  const [customTargetMode, setCustomTargetMode] = useState(false)
  // The app's OUTPUT schema (metadata_results), read once from /api/config and kept stable.
  // This is where ALL generated artifacts are written (metric-view definitions, knowledge
  // base, graph, vector index) and where metric views + the analytics pipeline output. It is
  // DISTINCT from selectedCatalog/selectedSchema, which is the SOURCE scope the user browses
  // to pick tables. For a federated source those differ, and outputs must NEVER be written to
  // the (read-only) source schema. See issue: build_knowledge_base into a foreign catalog.
  const [outputCatalog, setOutputCatalog] = useState('')
  const [outputSchema, setOutputSchema] = useState('')

  // MV health + analysis per definition
  const [mvHealth, setMvHealth] = useState({})
  const [mvAnalysis, setMvAnalysis] = useState({})
  const [mvAnalysisExpanded, setMvAnalysisExpanded] = useState(null)
  const [mvAppliedFields, setMvAppliedFields] = useState({})
  const [mvTestResults, setMvTestResults] = useState({})
  const [mvTestExpanded, setMvTestExpanded] = useState(null)
  const mvTestPollRef = useRef({})   // defId -> interval id, so we can stop/replace polls
  const [structuredEditing, setStructuredEditing] = useState(null)
  const [structuredDraft, setStructuredDraft] = useState(null)
  const userEditedTargetRef = useRef(false)
  const [perMvTargets, setPerMvTargets] = useState({})
  const [createError, setCreateError] = useState({})
  const [editDefId, setEditDefId] = useState(null)
  const [editJson, setEditJson] = useState('')
  const [suggestLoading, setSuggestLoading] = useState(false)
  const [suggestQLoading, setSuggestQLoading] = useState(false)
  const [suggestQProgress, setSuggestQProgress] = useState('')
  const [userIdentity, setUserIdentity] = useState(null)
  const [bulkCreating, setBulkCreating] = useState(false)
  const [bulkDeleting, setBulkDeleting] = useState(null)
  const [vectorSyncing, setVectorSyncing] = useState(false)
  const [sgSyncing, setSgSyncing] = useState(false)
  const [dupLoading, setDupLoading] = useState(false)
  const [dupGroups, setDupGroups] = useState(null)
  const [dupKeep, setDupKeep] = useState({})
  const [kpiCoverage, setKpiCoverage] = useState(null)
  const [tableSaveStatus, setTableSaveStatus] = useState(null)
  const [openMenuId, setOpenMenuId] = useState(null)
  const menuRef = useRef(null)

  // KPI Library
  const [kpis, setKpis] = useState([])
  const [kpiFilter, setKpiFilter] = useState('all')  // all | valid | invalid | empty
  const [showKpiForm, setShowKpiForm] = useState(false)
  const [kpiDraft, setKpiDraft] = useState({ name: '', description: '', formula: '', domain: '', target_tables: [] })
  const [kpiEditId, setKpiEditId] = useState(null)
  const [kpiSuggesting, setKpiSuggesting] = useState(false)
  const [expandedKpiSections, setExpandedKpiSections] = useState(new Set())

  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [activeTab, setActiveTab] = useState('setup')
  const [defFilter, setDefFilter] = useState('')
  const [defStatusFilter, setDefStatusFilter] = useState('validated')

  useEffect(() => {
    const handler = (e) => {
      if (menuRef.current && !menuRef.current.contains(e.target)) setOpenMenuId(null)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  useEffect(() => {
    try {
      const savedMat = localStorage.getItem(MAT_STORAGE_KEY)
      const savedSched = localStorage.getItem(MAT_SCHEDULE_KEY)
      const savedPreset = localStorage.getItem(MAT_PRESET_KEY)
      if (savedMat != null) setMaterialize(savedMat === 'true')
      if (savedSched != null) setMaterializationSchedule(savedSched)
      if (savedPreset) setMatSchedulePreset(savedPreset)
    } catch {}
  }, [])

  useEffect(() => {
    try {
      localStorage.setItem(MAT_STORAGE_KEY, String(materialize))
      localStorage.setItem(MAT_SCHEDULE_KEY, materializationSchedule)
      localStorage.setItem(MAT_PRESET_KEY, matSchedulePreset)
    } catch {}
  }, [materialize, materializationSchedule, matSchedulePreset])

  // The app's configured output schema (metadata_results). This is the deploy-target
  // default -- we NEVER derive the deploy location from the source table's schema (that
  // silently landed metric views in the source, and fails outright on a read-only
  // federated source). The user may change it, but a deploy always requires an
  // explicitly-chosen output catalog.schema.
  const configOutputTarget = (outputCatalog && outputSchema) ? `${outputCatalog}.${outputSchema}` : ''
  const isValidTarget = (t) => /^[^.]+\.[^.]+$/.test(t || '')
  const getDefaultTarget = (d) => {
    if (d.deployed_catalog && d.deployed_schema) return `${d.deployed_catalog}.${d.deployed_schema}`
    return ''  // no source fallback -- an output schema must be chosen explicitly
  }
  const getEffectiveTarget = (d) => {
    if (globalTargetOverride) return globalTargetOverride
    return perMvTargets[d.definition_id] || getDefaultTarget(d)
  }

  const getDeployMatEnabled = (d) => {
    const pref = deployMatPrefs[d.definition_id]
    if (pref && typeof pref.enabled === 'boolean') return pref.enabled
    return !!d.has_materialization
  }

  const getDeployMatSchedule = (d) => {
    const pref = deployMatPrefs[d.definition_id]
    return pref?.schedule ?? d.materialization_schedule ?? materializationSchedule
  }

  const buildCreateBody = (d, tCat, tSch) => {
    const body = { target_catalog: tCat, target_schema: tSch }
    const enabled = getDeployMatEnabled(d)
    const schedule = getDeployMatSchedule(d)
    if (enabled && !d.has_materialization) {
      body.materialize = true
      body.materialization_schedule = schedule
    } else if (!enabled && d.has_materialization) {
      body.materialize = false
      body.strip_materialization = true
    }
    return body
  }

  const schemaOptions = [...new Set([
    ...(configOutputTarget ? [configOutputTarget] : []),
    ...definitions.map(d => getDefaultTarget(d)).filter(Boolean),
  ])]

  // Default the deploy target to the app's OUTPUT schema (metadata_results), never the
  // source tables' schema. The user can override it; if config exposes no schema it stays
  // empty and the deploy actions are gated until an output catalog.schema is chosen.
  useEffect(() => {
    if (userEditedTargetRef.current) return
    if (!globalTargetOverride && configOutputTarget) setGlobalTargetOverride(configOutputTarget)
  }, [configOutputTarget])

  // --- Init ---
  useEffect(() => {
    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data: cfg }) => {
      if (cfg) {
        setSelectedCatalog(cfg.catalog_name || '')
        setSelectedSchema(cfg.schema_name || '')
        // OUTPUT schema is fixed to the app config -- kept separate from the source
        // browse scope so browsing a (federated) source never redirects our writes.
        setOutputCatalog(cfg.catalog_name || '')
        setOutputSchema(cfg.schema_name || '')
      }
    })
    cachedFetchObj('/api/auth/check', {}, TTL.CONFIG).then(({ data }) => {
      if (data?.user_identity) setUserIdentity(data.user_identity)
    })
    loadProjects()
    loadProfiles()
    refreshDefinitions()
    loadKpis()
  }, [])

  // Refresh definitions and pre-populate tables when project changes
  const isRestoringTablesRef = useRef(true)
  useEffect(() => {
    refreshDefinitions()
    userEditedTargetRef.current = false
    if (selectedProjectId) {
      const proj = projects.find(p => p.project_id === selectedProjectId)
      let restored = false
      if (proj?.selected_tables) {
        try {
          const tbls = JSON.parse(proj.selected_tables)
          if (Array.isArray(tbls) && tbls.length) {
            isRestoringTablesRef.current = true
            setSelectedTables(tbls)
            setTimeout(() => { isRestoringTablesRef.current = false }, 150)
            restored = true
          }
        } catch { /* ignore */ }
      }
      if (!restored) {
        isRestoringTablesRef.current = true
        setSelectedTables([])
        setTimeout(() => { isRestoringTablesRef.current = false }, 150)
      }
    }
  }, [selectedProjectId])

  // Polling
  useEffect(() => {
    if (!taskId) return
    pollRef.current = setInterval(async () => {
      try {
        const res = await fetch(`/api/semantic-layer/generate/${taskId}`)
        const data = await res.json()
        setTaskStatus(data)
        if (data.status === 'done' || data.status === 'error') {
          clearInterval(pollRef.current)
          if (data.status === 'done') { refreshDefinitions(); loadProjects(); setDefStatusFilter('all'); setActiveTab('definitions') }
        }
      } catch { clearInterval(pollRef.current); setTaskStatus(prev => prev?.status === 'done' || prev?.status === 'error' ? prev : { status: 'error', error: 'Lost connection to server. Try generating again.' }) }
    }, 2000)
    return () => clearInterval(pollRef.current)
  }, [taskId])

  // Auto-save table selection to the project whenever it changes (debounced 1s)
  const saveTimerRef = useRef(null)
  useEffect(() => {
    if (!selectedProjectId) return
    if (isRestoringTablesRef.current) return
    setTableSaveStatus('pending')
    clearTimeout(saveTimerRef.current)
    saveTimerRef.current = setTimeout(() => {
      const tablesSnapshot = [...selectedTables]
      const pid = selectedProjectId
      fetch(`/api/semantic-layer/projects/${pid}/tables`, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ selected_tables: tablesSnapshot }),
      }).then(r => {
        if (r.ok) {
          setProjects(prev => prev.map(p =>
            p.project_id === pid ? { ...p, selected_tables: JSON.stringify(tablesSnapshot) } : p
          ))
          setTableSaveStatus('saved')
          setTimeout(() => setTableSaveStatus(null), 2000)
        } else {
          console.error('Failed to auto-save tables:', r.status, r.statusText)
          setTableSaveStatus('error')
        }
      }).catch(err => {
        console.error('Failed to auto-save tables:', err)
        setTableSaveStatus('error')
      })
    }, 1000)
    return () => clearTimeout(saveTimerRef.current)
  }, [selectedTables, selectedProjectId])

  const loadProjects = () => {
    fetch('/api/semantic-layer/projects').then(r => r.json()).then(data => {
      if (Array.isArray(data)) setProjects(data)
    }).catch(() => {})
  }

  const loadProfiles = () => {
    cachedFetch('/api/semantic-layer/profiles', {}, TTL.CONFIG).then(({ data }) => {
      if (data) setProfiles(data)
    })
  }

  const refreshDefinitions = () => {
    const url = selectedProjectId
      ? `/api/semantic-layer/definitions?project_id=${selectedProjectId}`
      : '/api/semantic-layer/definitions'
    cachedFetch(url, {}, TTL.CONFIG).then(({ data }) => {
      if (data) setDefinitions(data)
    })
    const covParams = new URLSearchParams()
    if (selectedProjectId) covParams.set('project_id', selectedProjectId)
    if (activeProfileId) covParams.set('profile_id', activeProfileId)
    const covUrl = covParams.toString()
      ? `/api/semantic-layer/kpi-coverage?${covParams}`
      : '/api/semantic-layer/kpi-coverage'
    cachedFetchObj(covUrl, {}, TTL.CONFIG).then(({ data }) => {
      if (data?.kpi_coverage?.total) setKpiCoverage(data.kpi_coverage)
      else setKpiCoverage(null)
    })
    // Coverage-aware "how many views" recommendation (drives the Generate banner
    // + maxViews default). Cheap + server-cached; keyed on the current scope.
    if (selectedTables.length) {
      const recParams = new URLSearchParams({ tables: selectedTables.join(',') })
      if (selectedProjectId) recParams.set('project_id', selectedProjectId)
      if (activeProfileId) recParams.set('profile_id', activeProfileId)
      cachedFetchObj(`/api/semantic-layer/erd-recommendation?${recParams}`, {}, TTL.CONFIG)
        .then(({ data }) => setErdSufficiency(data?.sufficiency || null))
        .catch(() => setErdSufficiency(null))
      // Questions/KPIs "generate more?" recommendation (Questions & KPIs tab).
      cachedFetchObj(`/api/semantic-layer/generation-sufficiency?${recParams}`, {}, TTL.CONFIG)
        .then(({ data }) => setGenSufficiency(data && (data.questions || data.kpis) ? data : null))
        .catch(() => setGenSufficiency(null))
    } else {
      setErdSufficiency(null)
      setGenSufficiency(null)
    }
  }

  const createNewProject = async () => {
    if (!newProjectName.trim()) return
    setLoading(true); setError(null)
    try {
      const res = await fetch('/api/semantic-layer/projects', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ project_name: newProjectName.trim() }),
      })
      const data = await res.json()
      if (!res.ok) { setError(data.detail || 'Failed to create project'); setLoading(false); return }
      setSelectedProjectId(data.project_id)
      setNewProjectName('')
      setShowNewProject(false)
      loadProjects()
    } catch (e) { setError(e.message) }
    setLoading(false)
  }

  const deleteProject = async (pid) => {
    if (!confirm('Delete this project?')) return
    setLoading(true); setError(null)
    try {
      await fetch(`/api/semantic-layer/projects/${pid}`, { method: 'DELETE' })
      if (selectedProjectId === pid) setSelectedProjectId('')
      loadProjects()
      refreshDefinitions()
    } catch (e) { setError(e.message) }
    setLoading(false)
  }

  // --- Profile actions ---
  const selectProfile = (pid) => {
    setActiveProfileId(pid)
    if (!pid) { setProfileName(''); setQuestionsText(''); setBusinessContext(''); return }
    const p = profiles.find(x => x.profile_id === pid)
    if (!p) return
    setProfileName(p.profile_name || '')
    setBusinessContext(p.business_context || '')
    try {
      const qs = JSON.parse(p.questions || '[]')
      setQuestionsText(qs.join('\n'))
    } catch { setQuestionsText(p.questions || '') }
  }

  const saveProfile = async () => {
    const lines = questionsText.split('\n').filter(l => l.trim())
    if (!profileName.trim() || !lines.length) return
    setLoading(true); setError(null)
    try {
      const res = await fetch('/api/semantic-layer/profiles', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ profile_name: profileName, questions: lines, table_patterns: selectedTables, business_context: businessContext || undefined }),
      })
      const data = await res.json()
      if (!res.ok) { setError(data.detail || 'Failed to save profile'); setLoading(false); return }
      setActiveProfileId(data.profile_id)
      loadProfiles()
    } catch (e) { setError(e.message) }
    setLoading(false)
  }

  const deleteProfile = async () => {
    if (!activeProfileId) return
    if (!confirm('Delete this profile?')) return
    setLoading(true); setError(null)
    try {
      await fetch(`/api/semantic-layer/profiles/${activeProfileId}`, { method: 'DELETE' })
      setActiveProfileId('')
      setProfileName('')
      setQuestionsText('')
      setBusinessContext('')
      loadProfiles()
    } catch (e) { setError(e.message) }
    setLoading(false)
  }

  // --- Table selection (stores FQ names: catalog.schema.table) ---
  const fqTable = (t) => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`
  const toggleTable = (t) => {
    const fq = fqTable(t)
    setSelectedTables(prev => prev.includes(fq) ? prev.filter(x => x !== fq) : [...prev, fq])
  }
  const selectAll = () => setSelectedTables(prev => [...new Set([...prev, ...filteredTables.map(fqTable)])])
  const selectNone = () => {
    const prefix = `${selectedCatalog}.${selectedSchema}.`.toLowerCase()
    setSelectedTables(prev => prev.filter(t => !t.toLowerCase().startsWith(prefix)))
  }
  const isTableSelected = (t) => selectedTables.includes(fqTable(t))
  const removeTable = (fq) => setSelectedTables(prev => prev.filter(x => x !== fq))

  const saveProjectTables = async () => {
    if (!selectedProjectId) return
    clearTimeout(saveTimerRef.current)
    setTableSaveStatus('saving')
    try {
      const res = await fetch(`/api/semantic-layer/projects/${selectedProjectId}/tables`, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ selected_tables: selectedTables }),
      })
      if (res.ok) {
        setProjects(prev => prev.map(p =>
          p.project_id === selectedProjectId ? { ...p, selected_tables: JSON.stringify(selectedTables) } : p
        ))
        setTableSaveStatus('saved')
        setTimeout(() => setTableSaveStatus(null), 2000)
      }
    } catch { setTableSaveStatus(null) }
  }



  // Normalize a question for duplicate detection: lowercase, strip punctuation,
  // collapse whitespace. Used to avoid appending near-identical suggested questions.
  const normalizeQ = (q) => q.toLowerCase().replace(/[^\w\s]/g, ' ').replace(/\s+/g, ' ').trim()

  // Append only the generated questions that aren't near-duplicates of what's already
  // in the box; return how many were skipped so the caller can report it (no silent drop).
  const appendUniqueQuestions = (generated) => {
    let skipped = 0
    setQuestionsText(prev => {
      const existingLines = prev.split('\n').filter(l => l.trim())
      const seen = new Set(existingLines.map(normalizeQ))
      const toAdd = []
      for (const q of generated) {
        const n = normalizeQ(q)
        if (!n) continue
        if (seen.has(n)) { skipped++; continue }
        seen.add(n); toAdd.push(q)
      }
      if (!toAdd.length) return prev
      return prev ? prev + '\n' + toAdd.join('\n') : toAdd.join('\n')
    })
    return skipped
  }

  // Draft a business-context paragraph from the project's table descriptions.
  // Default source is the knowledge-base descriptions (richer, dbxmetagen-generated);
  // unchecking bizCtxUseKb falls back to the live UC table comments. Replaces the
  // field's contents (editable after).
  const suggestBusinessContext = async () => {
    if (!selectedTables.length) { setError('Select tables first'); return }
    if (businessContext.trim() && !confirm('Replace the current business context with an AI-drafted one?')) return
    setBizCtxLoading(true); setError(null)
    try {
      const fqTables = selectedTables.map(t => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`)
      const res = await fetch('/api/semantic-layer/suggest-business-context', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_identifiers: fqTables, use_kb: bizCtxUseKb }),
      })
      if (!res.ok) { const d = await res.json().catch(() => ({})); setError(d.detail || 'Failed to suggest business context'); return }
      const data = await res.json()
      if (data.business_context) {
        setBusinessContext(data.business_context)
      } else if (data.message) {
        setError(data.message)  // e.g. no descriptions found — no silent no-op
      }
    } catch (e) {
      setError(e.message || 'Request failed')
    } finally {
      setBizCtxLoading(false)
    }
  }

  // Lazy-load the list of Genie spaces the first time the picker is opened.
  const loadGenieSpaces = async () => {
    if (genieSpaces !== null || genieSpacesLoading) return
    setGenieSpacesLoading(true)
    try {
      const res = await fetch('/api/genie/available-spaces')
      if (!res.ok) { const d = await res.json().catch(() => ({})); setError(d.detail || 'Could not list Genie spaces'); setGenieSpaces([]); return }
      const data = await res.json()
      setGenieSpaces(data.spaces || [])
    } catch (e) {
      setError(e.message || 'Could not list Genie spaces'); setGenieSpaces([])
    } finally {
      setGenieSpacesLoading(false)
    }
  }

  // Pull curated example SQL from the selected Genie space(s) into the
  // genie_examples index so generation can retrieve proven query patterns.
  const pullGenieSql = async () => {
    if (!genieSelectedSpaces.length) { setError('Select at least one Genie space'); return }
    setGeniePullLoading(true); setGeniePullStatus(null); setError(null)
    try {
      const res = await fetch('/api/semantic-layer/pull-genie-sql', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ space_ids: genieSelectedSpaces }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) { setError(data.detail || 'Genie SQL pull failed'); return }
      if (data.examples_written > 0) {
        setGeniePullStatus(`Pulled ${data.examples_written} example SQL statement${data.examples_written !== 1 ? 's' : ''} from ${data.spaces_pulled} space${data.spaces_pulled !== 1 ? 's' : ''}. They'll seed metric-view generation.`)
      } else {
        setGeniePullStatus(data.message || 'No curated SQL found in the selected space(s).')
      }
    } catch (e) {
      setError(e.message || 'Request failed')
    } finally {
      setGeniePullLoading(false)
    }
  }

  const suggestQuestions = async () => {
    if (!selectedTables.length) { setError('Select tables first'); return }
    setSuggestQLoading(true); setError(null); setSuggestQProgress('')
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 180000)
    try {
      const fqTables = selectedTables.map(t => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`)
      const existing = questionsText.split('\n').filter(l => l.trim())
      const res = await fetch('/api/genie/generate-questions', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_identifiers: fqTables, count: 6, purpose: 'metric_views', business_context: businessContext || undefined, existing_questions: existing }),
        signal: controller.signal,
      })
      clearTimeout(timeout)
      if (!res.ok) {
        const text = await res.text()
        let detail = 'Failed to suggest questions'
        try { detail = JSON.parse(text).detail || detail } catch {}
        setError(detail); setSuggestQLoading(false); setSuggestQProgress(''); return
      }
      const contentType = res.headers.get('content-type') || ''
      if (contentType.includes('text/event-stream')) {
        // SSE streaming response (chunked generation for >20 tables)
        const reader = res.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ''
        let finalData = null
        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          buffer += decoder.decode(value, { stream: true })
          const lines = buffer.split('\n')
          buffer = lines.pop() || ''
          for (const line of lines) {
            if (!line.startsWith('data: ')) continue
            try {
              const evt = JSON.parse(line.slice(6))
              if (evt.event === 'status') setSuggestQProgress(evt.message)
              else if (evt.event === 'themes') {
                const names = evt.themes.map(t => `${t.name} (${t.table_count} tables)`).join(', ')
                setSuggestQProgress(`Themes: ${names}`)
              } else if (evt.event === 'done') { finalData = evt }
              else if (evt.event === 'error') { setError(evt.message); break }
            } catch {}
          }
        }
        if (finalData) {
          const skipped = appendUniqueQuestions(finalData.questions || [])
          if (skipped > 0) setError(`Skipped ${skipped} generated question${skipped !== 1 ? 's' : ''} that duplicated existing ones.`)
        }
      } else {
        // Standard JSON response (single-call for <=20 tables)
        const data = await res.json()
        if (data.warning) setError(data.warning)
        const skipped = appendUniqueQuestions(data.questions || [])
        if (skipped > 0) setError(`Skipped ${skipped} generated question${skipped !== 1 ? 's' : ''} that duplicated existing ones.`)
      }
    } catch (e) {
      clearTimeout(timeout)
      if (e.name === 'AbortError') setError('Request timed out. Try selecting fewer tables.')
      else setError(e.message || 'Request failed')
    }
    setSuggestQLoading(false); setSuggestQProgress('')
  }

  // --- KPIs ---
  // Retro-heal stale-'invalid' KPIs at most ONCE per mount, via the explicit POST
  // (list is now a pure read — review finding #2). Fire-and-forget: if it heals
  // anything, refresh the list to show the flipped statuses.
  const kpiRevalidatedRef = useRef(false)
  const loadKpis = async () => {
    if (!kpiRevalidatedRef.current) {
      kpiRevalidatedRef.current = true
      try {
        const res = await fetch(`/api/kpis/revalidate${activeProfileId ? `?profile_id=${encodeURIComponent(activeProfileId)}` : ''}`, { method: 'POST' })
        const d = await res.json().catch(() => ({}))
        if (d.healed > 0) invalidateCache('/api/kpis')  // stale statuses changed
      } catch { /* non-blocking */ }
    }
    const { data } = await cachedFetch('/api/kpis', {}, TTL.CONFIG)
    setKpis(data || [])
  }
  const saveKpi = async (overrideDuplicate = false) => {
    try {
      // Bind the KPI to the source table(s) the user picked; fall back to all selected
      // tables ("Auto") only when none is chosen, preserving the pre-picker behavior.
      const picked = Array.isArray(kpiDraft.target_tables) ? kpiDraft.target_tables.filter(Boolean) : []
      const body = { ...kpiDraft, target_tables: picked.length ? picked : selectedTables, profile_id: activeProfileId || undefined, override_duplicate: overrideDuplicate }
      const res = kpiEditId
        ? await fetch(`/api/kpis/${kpiEditId}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
        : await fetch('/api/kpis', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      // 409 = likely-duplicate warning (create path only). Confirm, then re-save with override.
      if (res.status === 409 && !kpiEditId) {
        const d = await res.json().catch(() => ({}))
        const warn = d.detail?.warning || 'This looks like an existing KPI. Save anyway?'
        if (confirm(warn)) { await saveKpi(true) }
        return
      }
      if (!res.ok) { const d = await res.json().catch(() => ({})); setError(d.detail?.warning || d.detail || 'Save KPI failed'); return }
      setKpiDraft({ name: '', description: '', formula: '', domain: '', target_tables: [] }); setKpiEditId(null); setShowKpiForm(false)
      loadKpis()
    } catch (e) { setError(e.message || 'Save KPI failed') }
  }
  const deleteKpi = async (id) => {
    if (!confirm('Delete this KPI?')) return
    try {
      const res = await fetch(`/api/kpis/${id}`, { method: 'DELETE' })
      if (!res.ok) { const d = await res.json().catch(() => ({})); setError(d.detail || 'Delete KPI failed'); return }
      loadKpis()
    } catch (e) { setError(e.message || 'Delete KPI failed') }
  }
  const deleteAllKpis = async () => {
    if (!confirm(`Delete all ${kpis.length} KPIs? This cannot be undone.`)) return
    try {
      const res = await fetch('/api/kpis', { method: 'DELETE' })
      if (!res.ok) { const d = await res.json().catch(() => ({})); setError(d.detail || 'Delete all KPIs failed'); return }
      loadKpis()
    } catch (e) { setError(e.message || 'Delete all KPIs failed') }
  }
  const deleteInvalidKpis = async () => {
    const n = kpis.filter(k => (k.validation_status || '').toLowerCase() === 'invalid').length
    if (!n || !confirm(`Delete all ${n} invalid KPI(s)? This cannot be undone.`)) return
    try {
      const res = await fetch('/api/kpis?status=invalid', { method: 'DELETE' })
      if (!res.ok) { const d = await res.json().catch(() => ({})); setError(d.detail || 'Delete invalid KPIs failed'); return }
      invalidateCache('/api/kpis')
      loadKpis()
    } catch (e) { setError(e.message || 'Delete invalid KPIs failed') }
  }
  const suggestKpis = async () => {
    if (!selectedTables.length) return
    setKpiSuggesting(true); setError(null)
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 120000)
    try {
      const fqTables = selectedTables.map(t => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`)
      const qLines = questionsText.split('\n').filter(l => l.trim())
      const existingNames = kpis.map(k => k.name)
      const res = await fetch('/api/kpis/suggest', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_identifiers: fqTables, business_context: businessContext || undefined, questions: qLines.length ? qLines : undefined, profile_id: activeProfileId || undefined, existing_kpi_names: existingNames }),
        signal: controller.signal })
      clearTimeout(timeout)
      if (!res.ok) {
        const text = await res.text()
        let detail = 'Failed to suggest KPIs'
        try { detail = JSON.parse(text).detail || detail } catch {}
        setError(detail); setKpiSuggesting(false); return
      }
      const j = await res.json()
      if (j.warning) setError(j.warning)
      let skippedDupes = 0
      for (const k of (j.kpis || [])) {
        // Preserve the backend-resolved single target_tables (suggest_kpis binds each
        // KPI to the one table its formula belongs to). Do NOT broaden to all tables.
        const r = await fetch('/api/kpis', { method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...k, source: 'suggested', profile_id: activeProfileId || undefined }) })
        // 409 = the backend flagged this suggestion as a near-duplicate of an existing
        // KPI. In bulk we skip it (rather than pop a dialog per KPI) and report the count.
        if (r.status === 409) skippedDupes++
      }
      if (skippedDupes > 0) {
        setError(`Skipped ${skippedDupes} suggested KPI${skippedDupes !== 1 ? 's' : ''} that duplicated existing ones.`)
      }
      loadKpis()
    } catch (e) {
      clearTimeout(timeout)
      if (e.name === 'AbortError') setError('Request timed out. Try selecting fewer tables.')
      else setError(e.message || 'Request failed')
    }
    setKpiSuggesting(false)
  }

  // --- Generate ---
  const startGeneration = async (mode = 'replace') => {
    const lines = questionsText.split('\n').filter(l => l.trim())
    if (!selectedTables.length || !lines.length) return
    // Soft gate: core metadata + analytics pipeline are RECOMMENDED, not required.
    // The readiness signal reads the knowledge base and can be empty even when the
    // user has run them (KB not yet built, or an app SP that can't see a table), so
    // we let generation proceed rather than hard-block. The amber hint above informs.
    setLoading(true); setError(null); setTaskId(null); setTaskStatus(null)
    try {
      const fqTables = selectedTables.map(t => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`)
      const body = {
        tables: fqTables, questions: lines, mode,
        // OUTPUT goes to the config schema; source tables are carried in `tables` (FQ above).
        catalog_name: outputCatalog, schema_name: outputSchema,
        business_context: businessContext || undefined,
        profile_id: activeProfileId || undefined,
        generation_style: generationStyle,
        max_views: maxViews || undefined,
        materialize,
        materialization_schedule: materializationSchedule,
      }
      if (selectedProjectId) body.project_id = selectedProjectId
      const res = await fetch('/api/semantic-layer/generate', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await res.json()
      if (!res.ok) { setError(data.detail || 'Failed to start generation'); setLoading(false); return }
      setTaskId(data.task_id)
      setTaskStatus({ status: 'running', stage: 'starting' })
    } catch (e) { setError(e.message) }
    setLoading(false)
  }

  // --- Definitions ---
  const loadDefinitionJson = async (defId) => {
    if (expandedDef === defId) { setExpandedDef(null); setExpandedJson(null); setStructuredEditing(null); setStructuredDraft(null); return }
    const { data, error: err } = await cachedFetchObj(`/api/semantic-layer/definitions/${defId}/json`, {}, TTL.CONFIG)
    if (err) { setError(err); return }
    setExpandedDef(defId)
    try { setExpandedJson(JSON.stringify(JSON.parse(data.json_definition), null, 2)) }
    catch { setExpandedJson(data.json_definition) }
    fetchMvHealth(defId)
  }

  const retryDefinition = async (defId) => {
    setActionLoading(prev => ({ ...prev, [defId]: 'retry' }))
    setError(null)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/retry`, { method: 'POST' })
      const data = await res.json()
      if (!res.ok) setError(data.detail || 'Retry failed')
      invalidateCache('/api/semantic-layer/definitions')
      refreshDefinitions()
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  // `focus` (add_measures | add_dimensions | check_filters) comes from an Analyze
  // refinement button and steers what the LLM expands; null = general improve.
  const improveDefinition = async (defId, focus = null) => {
    setActionLoading(prev => ({ ...prev, [defId]: focus || 'improve' }))
    setError(null)
    try {
      const analysisIssues = mvAnalysis[defId] || null
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/improve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ analysis_issues: analysisIssues, focus }),
      })
      const data = await res.json()
      if (!res.ok) setError(data.detail || 'Improve failed')
      invalidateCache('/api/semantic-layer/definitions')
      refreshDefinitions()
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  // Incremental add of new measures/dimensions -- cheaper than improve (only new
  // items are generated, then merged + de-duplicated server-side).
  const addItems = async (defId, kind) => {
    const focus = kind === 'measures' ? 'add_measures' : 'add_dimensions'
    setActionLoading(prev => ({ ...prev, [defId]: focus }))
    setError(null)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/add-items`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ kind }),
      })
      const data = await res.json()
      if (!res.ok) {
        setError(data.detail || `Add ${kind} failed`)
      } else if (!(data.added || []).length) {
        const skipped = (data.skipped_duplicates || []).length
        setError(skipped
          ? `No new ${kind} added -- ${skipped} suggestion(s) duplicated existing items.`
          : `No new ${kind} were suggested.`)
      }
      invalidateCache('/api/semantic-layer/definitions')
      refreshDefinitions()
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  const fetchMvHealth = async (defId) => {
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/health-check`, { method: 'POST' })
      if (res.ok) {
        const data = await res.json()
        setMvHealth(prev => ({ ...prev, [defId]: data }))
      }
    } catch {}
  }

  const analyzeMv = async (defId) => {
    setActionLoading(prev => ({ ...prev, [defId]: 'analyze' }))
    setMvAnalysis(prev => ({ ...prev, [defId]: null }))
    setMvAppliedFields(prev => ({ ...prev, [defId]: new Set() }))
    setMvAnalysisExpanded(defId)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/analyze`, { method: 'POST' })
      if (res.ok) {
        const data = await res.json()
        setMvHealth(prev => ({ ...prev, [defId]: data.health }))
        setMvAnalysis(prev => ({ ...prev, [defId]: data.issues || [] }))
      } else {
        const data = await res.json().catch(() => ({}))
        setMvAnalysis(prev => ({ ...prev, [defId]: [] }))
        setError(data.detail || 'Analysis failed')
      }
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  // Stop any in-flight poll for a definition (re-click, unmount, or hard stop).
  const stopMvTestPoll = (defId) => {
    const id = mvTestPollRef.current[defId]
    if (id) { clearInterval(id); delete mvTestPollRef.current[defId] }
  }

  // POST to start an async test-query run, then poll for progress + results.
  // Bounded server-side; here we add a hard client stop so the UI never spins forever.
  const runTestQueries = async (defId, allowFederatedFull = false) => {
    stopMvTestPoll(defId)
    setActionLoading(prev => ({ ...prev, [defId]: 'test' }))
    setMvTestResults(prev => ({ ...prev, [defId]: { status: 'running', done: 0, total: 0, results: [] } }))
    setMvTestExpanded(defId)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/test-queries`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ allow_federated_full: allowFederatedFull }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        setError(data.detail || 'Test queries failed')
        setMvTestExpanded(null)
        setActionLoading(prev => ({ ...prev, [defId]: null }))
        return
      }
      setMvTestResults(prev => ({ ...prev, [defId]: data }))
      // Synchronous completion (cached, empty, or immediate) -- no polling needed.
      if (data.status !== 'running' || !data.task_id) {
        setActionLoading(prev => ({ ...prev, [defId]: null }))
        return
      }
      // Poll the task; hard client-side stop at ~200s so we never spin forever.
      const startedAt = Date.now()
      mvTestPollRef.current[defId] = setInterval(async () => {
        if (Date.now() - startedAt > 200000) {
          stopMvTestPoll(defId)
          setMvTestResults(prev => ({
            ...prev,
            [defId]: { ...(prev[defId] || {}), status: 'done',
              stopped: 'Stopped waiting after 200s — the queries may still be running on the warehouse. Results shown are partial.' },
          }))
          setActionLoading(prev => ({ ...prev, [defId]: null }))
          return
        }
        try {
          const pr = await fetch(`/api/semantic-layer/definitions/${defId}/test-queries/${data.task_id}`)
          if (!pr.ok) return  // transient; keep polling until the hard stop
          const pd = await pr.json()
          setMvTestResults(prev => ({ ...prev, [defId]: pd }))
          if (pd.status !== 'running') {
            stopMvTestPoll(defId)
            setActionLoading(prev => ({ ...prev, [defId]: null }))
          }
        } catch { /* transient; keep polling until the hard stop */ }
      }, 2000)
    } catch (e) {
      setError(e.message); setMvTestExpanded(null)
      setActionLoading(prev => ({ ...prev, [defId]: null }))
    }
  }

  // Clean up any live poll intervals on unmount.
  useEffect(() => () => { Object.values(mvTestPollRef.current).forEach(clearInterval) }, [])

  const applyFieldFix = async (defId, pathOrAll, value) => {
    setActionLoading(prev => ({ ...prev, [defId]: 'apply-fix' }))
    if (pathOrAll === '__all__') {
      const issues = mvAnalysis[defId] || []
      const existing = mvAppliedFields[defId] || new Set()
      const fixable = issues.filter(iss => iss.field && iss.fix_value && !existing.has(iss.field))
      let failures = 0
      let currentId = defId
      for (const iss of fixable) {
        try {
          const res = await fetch(`/api/semantic-layer/definitions/${currentId}/field`, {
            method: 'PUT', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path: iss.field, value: iss.fix_value }),
          })
          if (res.ok) {
            const data = await res.json()
            currentId = data.definition_id
            setMvAppliedFields(prev => {
              const s = new Set(prev[defId] || [])
              s.add(iss.field)
              return { ...prev, [defId]: s }
            })
          } else { failures++ }
        } catch { failures++ }
      }
      if (failures) setError(`${failures} of ${fixable.length} fixes failed to apply`)
      invalidateCache('/api/semantic-layer/definitions')
      refreshDefinitions()
      fetchMvHealth(currentId)
      setActionLoading(prev => ({ ...prev, [defId]: null }))
      return
    }
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/field`, {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: pathOrAll, value }),
      })
      if (res.ok) {
        setMvAppliedFields(prev => {
          const s = new Set(prev[defId] || [])
          s.add(pathOrAll)
          return { ...prev, [defId]: s }
        })
        invalidateCache('/api/semantic-layer/definitions')
        refreshDefinitions()
        fetchMvHealth(defId)
      } else {
        const data = await res.json().catch(() => ({}))
        setError(data.detail || `Failed to apply fix for ${pathOrAll}`)
      }
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  const createDefinition = async (defId) => {
    const d = definitions.find(x => x.definition_id === defId)
    const target = getEffectiveTarget(d || {})
    const [tCat, tSch] = target.includes('.') ? target.split('.') : ['', '']
    if (!tCat || !tSch) {
      setError('Select an output catalog.schema before deploying (use the Output schema selector).')
      return
    }
    setActionLoading(prev => ({ ...prev, [defId]: 'create' }))
    setError(null)
    setCreateError(prev => ({ ...prev, [defId]: null }))
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/create`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildCreateBody(d, tCat, tSch)),
      })
      const text = await res.text()
      let data
      try { data = JSON.parse(text) } catch { data = { detail: text } }
      if (!res.ok) {
        const msg = typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || data)
        setError(msg)
        setCreateError(prev => ({ ...prev, [defId]: msg }))
      } else {
        setError(null)
        setCreateError(prev => ({ ...prev, [defId]: null }))
      }
      invalidateCache('/api/semantic-layer/definitions')
      refreshDefinitions()
    } catch (e) { setError(e.message); setCreateError(prev => ({ ...prev, [defId]: e.message })) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  const createAllValidated = async () => {
    const validated = definitions.filter(d => d.status === 'validated')
    if (!validated.length) return
    const anyMissing = validated.some(d => !isValidTarget(getEffectiveTarget(d)))
    if (anyMissing) { setError('Select an output catalog.schema before deploying (use the Output schema selector).'); return }
    setBulkCreating(true)
    setError(null)
    for (const d of validated) setActionLoading(prev => ({ ...prev, [d.definition_id]: 'create' }))
    const results = await Promise.allSettled(validated.map(async (d) => {
      const res = await fetch(`/api/semantic-layer/definitions/${d.definition_id}/create`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildCreateBody(d, getEffectiveTarget(d).split('.')[0], getEffectiveTarget(d).split('.')[1])),
      })
      const text = await res.text()
      let data; try { data = JSON.parse(text) } catch { data = { detail: text } }
      if (!res.ok) {
        const msg = typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || data)
        setCreateError(prev => ({ ...prev, [d.definition_id]: msg }))
        throw new Error(msg)
      }
      setCreateError(prev => ({ ...prev, [d.definition_id]: null }))
    }))
    const failed = results.filter(r => r.status === 'rejected')
    if (failed.length) setError(`${failed.length} of ${validated.length} creates failed`)
    for (const d of validated) setActionLoading(prev => ({ ...prev, [d.definition_id]: null }))
    invalidateCache('/api/semantic-layer/definitions')
    refreshDefinitions()
    setBulkCreating(false)
  }

  const deleteAllApplied = async () => {
    const applied = definitions.filter(d => d.status === 'applied')
    if (!applied.length) return
    if (!confirm(`Drop and delete all ${applied.length} applied metric views?`)) return
    setBulkDeleting('applied')
    setError(null)
    for (const d of applied) {
      try {
        const dt = getEffectiveTarget(d)
        const [dCat, dSch] = dt.includes('.') ? dt.split('.') : ['', '']
        if (dCat && dSch) {
          await fetch(`/api/semantic-layer/definitions/${d.definition_id}/drop`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ target_catalog: dCat, target_schema: dSch }),
          })
        }
        await fetch(`/api/semantic-layer/definitions/${d.definition_id}`, { method: 'DELETE' })
      } catch (e) { setError(e.message) }
    }
    invalidateCache('/api/semantic-layer/definitions')
    refreshDefinitions()
    setBulkDeleting(null)
  }

  const syncToVectorStore = async () => {
    setVectorSyncing(true)
    try {
      const res = await fetch('/api/vector/sync-metric-views', { method: 'POST' })
      const { task_id } = await res.json()
      const poll = async () => {
        const r = await fetch(`/api/vector/sync-metric-views/${task_id}`)
        const data = await r.json()
        if (data.status === 'running') { setTimeout(poll, 2000); return }
        setVectorSyncing(false)
        if (data.status === 'done') {
          alert(`Synced ${data.docs_total || 0} metric view docs to vector store`)
        } else {
          alert(`Sync failed: ${data.error || 'unknown error'}`)
        }
      }
      poll()
    } catch (e) {
      setVectorSyncing(false)
      alert(`Sync failed: ${e.message}`)
    }
  }

  const syncToSemanticGraph = async () => {
    setSgSyncing(true)
    try {
      const res = await fetch('/api/semantic-graph/sync', { method: 'POST' })
      const { task_id } = await res.json()
      const poll = async () => {
        const r = await fetch(`/api/semantic-graph/sync/${task_id}`)
        const data = await r.json()
        if (data.status === 'running') { setTimeout(poll, 2000); return }
        setSgSyncing(false)
        if (data.status === 'done') {
          alert(`Semantic graph synced: ${data.nodes || 0} nodes, ${data.edges || 0} edges`)
        } else {
          alert(`Sync failed: ${data.error || 'unknown error'}`)
        }
      }
      poll()
    } catch (e) {
      setSgSyncing(false)
      alert(`Sync failed: ${e.message}`)
    }
  }

  const findDuplicates = async () => {
    setDupLoading(true)
    try {
      const url = selectedProjectId
        ? `/api/semantic-layer/duplicates?project_id=${selectedProjectId}`
        : '/api/semantic-layer/duplicates'
      const res = await fetch(url)
      const data = await res.json()
      const groups = data.duplicate_groups || []
      if (groups.length === 0) {
        alert('No duplicates found.')
        setDupGroups(null)
      } else {
        const initialKeep = {}
        groups.forEach((g, i) => {
          const rec = g.definitions.find(d => d.recommended)
          initialKeep[i] = rec ? rec.definition_id : g.definitions[0]?.definition_id
        })
        setDupKeep(initialKeep)
        setDupGroups(groups)
      }
    } catch (e) {
      alert(`Failed to find duplicates: ${e.message}`)
    } finally {
      setDupLoading(false)
    }
  }

  const resolveDuplicates = async () => {
    if (!dupGroups) return
    const keepIds = Object.values(dupKeep)
    const supersede = []
    dupGroups.forEach((g, i) => {
      g.definitions.forEach(d => {
        if (d.definition_id !== dupKeep[i]) supersede.push(d.definition_id)
      })
    })
    if (supersede.length === 0) { setDupGroups(null); return }
    try {
      await fetch('/api/semantic-layer/resolve-duplicates', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keep_ids: keepIds, supersede_ids: supersede })
      })
      setDupGroups(null)
      refreshDefinitions()
    } catch (e) {
      alert(`Failed to resolve: ${e.message}`)
    }
  }

  const deleteAllNonApplied = async () => {
    const targets = definitions.filter(d => d.status !== 'applied')
    if (!targets.length) return
    if (!confirm(`Delete all ${targets.length} non-applied definitions (generated, validated, failed)?`)) return
    setBulkDeleting('non-applied')
    setError(null)
    for (const d of targets) {
      try { await fetch(`/api/semantic-layer/definitions/${d.definition_id}`, { method: 'DELETE' }) }
      catch (e) { setError(e.message) }
    }
    invalidateCache('/api/semantic-layer/definitions')
    refreshDefinitions()
    setBulkDeleting(null)
  }

  const getSuggestion = async (defId) => {
    const err = createError[defId]
    if (!err) return
    setSuggestLoading(true)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/suggest-fix`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ error_message: err }),
      })
      const data = await res.json().catch(() => ({}))
      setEditJson(_prettyJson(data.suggested_json))
      setEditDefId(defId)
    } catch (e) { setError(e.message) }
    setSuggestLoading(false)
  }

  const _prettyJson = (raw) => {
    try { return JSON.stringify(JSON.parse(raw), null, 2) }
    catch { return raw || '{}' }
  }

  const openEdit = (defId) => {
    setEditDefId(defId)
    fetch(`/api/semantic-layer/definitions/${defId}/json`)
      .then(r => r.json())
      .then(data => setEditJson(_prettyJson(data.json_definition)))
      .catch(() => setEditJson('{}'))
  }

  const saveEdit = async () => {
    if (!editDefId || !editJson.trim()) return
    setSuggestLoading(true)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${editDefId}`, {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ json_definition: editJson }),
      })
      if (!res.ok) { const d = await res.json().catch(() => ({})); setError(d.detail || 'Save failed') }
      else {
        setCreateError(prev => ({ ...prev, [editDefId]: null }))
        setEditDefId(null)
        setEditJson('')
        invalidateCache('/api/semantic-layer/definitions')
        refreshDefinitions()
      }
    } catch (e) { setError(e.message) }
    setSuggestLoading(false)
  }

  const deleteDefinition = async (defId, status) => {
    if (!confirm('Delete this definition?')) return
    setActionLoading(prev => ({ ...prev, [defId]: 'delete' }))
    setError(null)
    try {
      let url = `/api/semantic-layer/definitions/${defId}`
      if (status === 'applied') {
        const dd = definitions.find(x => x.definition_id === defId)
        const dt = getEffectiveTarget(dd || {})
        const [dCat, dSch] = dt.includes('.') ? dt.split('.') : ['', '']
        if (dCat && dSch) {
          url += `?drop_view=true&catalog=${encodeURIComponent(dCat)}&schema=${encodeURIComponent(dSch)}`
        }
      }
      const res = await fetch(url, { method: 'DELETE' })
      if (!res.ok) { const data = await res.json(); setError(data.detail || 'Delete failed') }
      invalidateCache('/api/semantic-layer/definitions')
      refreshDefinitions()
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  const dropDefinition = async (defId) => {
    if (!confirm('Drop this view from Unity Catalog?')) return
    const dd = definitions.find(x => x.definition_id === defId)
    const dropTarget = getEffectiveTarget(dd || {})
    const [dropCat, dropSch] = dropTarget.includes('.') ? dropTarget.split('.') : ['', '']
    if (!dropCat || !dropSch) {
      setError('No target schema resolved for dropping')
      return
    }
    setActionLoading(prev => ({ ...prev, [defId]: 'drop' }))
    setError(null)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/drop`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target_catalog: dropCat, target_schema: dropSch }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) setError(data.detail || 'Drop failed')
      invalidateCache('/api/semantic-layer/definitions')
      refreshDefinitions()
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  const transferOwnership = async (defId) => {
    if (!confirm('Transfer ownership of this metric view to you? The app will no longer be able to edit or drop this view.')) return
    const dd = definitions.find(x => x.definition_id === defId)
    const dt = getEffectiveTarget(dd || {})
    const [tCat, tSch] = dt.includes('.') ? dt.split('.') : ['', '']
    if (!tCat || !tSch) { setError('No target schema resolved'); return }
    setActionLoading(prev => ({ ...prev, [defId]: 'transfer' }))
    setError(null)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/transfer-ownership`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target_catalog: tCat, target_schema: tSch }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) setError(data.detail || 'Transfer failed')
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  const statusBadge = (status) => {
    const colors = {
      pending: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200',
      processed: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
      validated: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200',
      applied: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-200',
      failed: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
    }
    return <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${colors[status] || 'bg-dbx-oat text-gray-600 dark:bg-gray-700 dark:text-gray-300'}`}>{status}</span>
  }

  const isGenerating = taskStatus && taskStatus.status === 'running'
  const questionLines = questionsText.split('\n').filter(l => l.trim())
  const section = "card p-6"
  const label = "block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
  const input = "input-base"
  const btnPrimary = "px-4 py-2 bg-dbx-lava text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50"

  // Non-optional foundation gate: metric-view generation requires both core
  // metadata and the analytics pipeline to be complete (in that order).
  const foundation = deriveFoundation(pipelineStats)
  const foundationReady = !foundation || foundation.ready  // null (loading) is permissive

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <PageHeader title="Metric Views" subtitle="Build governed business metrics as Unity Catalog views" />
        <button onClick={async () => {
          const res = await fetch(`/api/semantic-layer/export-sql${globalTargetOverride ? `?catalog=${encodeURIComponent(globalTargetOverride.split('.')[0])}&schema=${encodeURIComponent(globalTargetOverride.split('.')[1])}` : ''}`)
          if (!res.ok) { setError((await res.json()).detail || 'Export failed'); return }
          const blob = await res.blob()
          const url = URL.createObjectURL(blob)
          const a = document.createElement('a'); a.href = url; a.download = 'metric_views.sql'; a.click()
          URL.revokeObjectURL(url)
        }} className="px-4 py-2 text-sm rounded-md border border-slate-300 dark:border-dbx-navy-400 text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-dbx-navy-500 transition-colors whitespace-nowrap">
          Export SQL
        </button>
      </div>
      <FoundationRail
        foundation={foundation}
        onNavigate={onNavigate}
        step2Runner={
          <AdvancedPipelinePanel
            variant="gate"
            catalogName={outputCatalog}
            schemaName={outputSchema}
            tableNames={selectedTables.map(t => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`).join(', ')}
            runJob={jobRunner.runJob}
            runningAction={jobRunner.runningAction}
            runError={jobRunner.runError}
            runHistory={jobRunner.runHistory}
            pipelineStats={pipelineStats}
            useServerless={pipelineServerless}
            onServerlessChange={setPipelineServerless}
            onCompleted={() => onRefreshPipelineStats?.()}
            onNavigate={onNavigate}
          />
        }
      />
      {cst.error && (
        <div className="rounded-lg border border-red-200 dark:border-red-800/40 bg-red-50 dark:bg-red-900/20 px-4 py-3 text-sm text-red-700 dark:text-red-300">
          Could not load catalogs or tables. Check that the SQL warehouse is running and the app service principal has USE permissions on the target catalog. <span className="font-mono text-red-500 dark:text-red-400">{cst.error}</span>
        </div>
      )}
      <ErrorBanner error={error} />

      {/* Unified catalog/schema display */}
      {(selectedCatalog || selectedSchema) && (
        <div className="text-xs text-slate-500 dark:text-slate-400 px-1">
          Scope: <span className="font-medium text-slate-700 dark:text-slate-200">{selectedCatalog || '?'}.{selectedSchema || '?'}</span>
          {globalTargetOverride && (
            <span className="ml-3">Override: <span className="font-medium text-slate-700 dark:text-slate-200">{globalTargetOverride}</span></span>
          )}
        </div>
      )}

      {/* Tab Bar */}
      <div className="inline-flex bg-dbx-oat/60 dark:bg-dbx-navy-600 rounded-xl p-1 shadow-inner-soft">
        {[['setup', 'Setup'], ['questions', 'Questions & KPIs'], ['model', 'Model'], ['generate', 'Generate'], ['definitions', 'Definitions']].map(([k, l]) => {
          const count = k === 'setup' && selectedTables.length ? `${selectedTables.length} tables`
            : k === 'questions' ? [questionLines.length && `${questionLines.length}q`, kpis.length && `${kpis.length} KPIs`].filter(Boolean).join(', ') || ''
            : k === 'definitions' && definitions.length ? `${definitions.length}` : ''
          const genReady = k === 'generate' && selectedTables.length > 0 && questionLines.length > 0
          const genNotReady = k === 'generate' && (!selectedTables.length || !questionLines.length)
          return (
            <button key={k} onClick={() => setActiveTab(k)}
              className={`px-3.5 py-1.5 text-sm rounded-lg transition-all duration-200 ${activeTab === k ? 'bg-white dark:bg-dbx-navy-500 shadow-sm font-semibold text-dbx-lava' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`}>
              {l}{count && <span className="ml-1 text-[10px] text-slate-400">({count})</span>}
              {genReady && <span className="ml-1 inline-block w-1.5 h-1.5 rounded-full bg-green-500" />}
              {genNotReady && <span className="ml-1 inline-block w-1.5 h-1.5 rounded-full bg-amber-400" />}
            </button>
          )
        })}
      </div>

      {/* Workflow guide */}
      <div className="text-xs text-slate-500 dark:text-slate-400 leading-relaxed px-1">
        <span className="font-semibold text-slate-600 dark:text-slate-300">How it works:</span>{' '}
        <span className={activeTab === 'setup' ? 'font-semibold text-dbx-lava' : ''}>1. Setup</span> &mdash; pick a project and select tables &rarr;{' '}
        <span className={activeTab === 'questions' ? 'font-semibold text-dbx-lava' : ''}>2. Questions</span> &mdash; define business questions and KPIs &rarr;{' '}
        <span className={activeTab === 'model' ? 'font-semibold text-dbx-lava' : ''}>3. Model</span> &mdash; review the recommended ERD (facts, joins) &rarr;{' '}
        <span className={activeTab === 'generate' ? 'font-semibold text-dbx-lava' : ''}>4. Generate</span> &mdash; AI creates metric view definitions &rarr;{' '}
        <span className={activeTab === 'definitions' ? 'font-semibold text-dbx-lava' : ''}>5. Definitions</span> &mdash; review, validate, and deploy as UC views.
        Then query them in <span className="font-medium text-amber-600 dark:text-amber-400">Explore &rarr; Metric View Agent</span>
      </div>

      {/* === Setup Tab === */}
      {activeTab === 'setup' && <>

      {/* Project Selector */}
      <section className={section}>
        <div className="flex items-center gap-4 mb-1">
          <h2 className="text-lg font-semibold dark:text-gray-100 whitespace-nowrap">Project</h2>
          <select value={selectedProjectId} onChange={e => setSelectedProjectId(e.target.value)} className={`${input} max-w-xs`}>
            <option value="">All definitions (no project)</option>
            {projects.map(p => <option key={p.project_id} value={p.project_id}>{p.project_name}</option>)}
          </select>
          {!showNewProject ? (
            <button onClick={() => setShowNewProject(true)} className="text-sm text-blue-600 dark:text-blue-400 hover:underline whitespace-nowrap">
              + New Project
            </button>
          ) : (
            <div className="flex items-center gap-2">
              <input value={newProjectName} onChange={e => setNewProjectName(e.target.value)}
                placeholder="Project name" className="input-base w-48"
                onKeyDown={e => e.key === 'Enter' && createNewProject()} />
              <button onClick={createNewProject} disabled={loading || !newProjectName.trim()}
                className="px-3 py-1 bg-dbx-lava text-white rounded text-sm hover:bg-red-700 disabled:opacity-50">Create</button>
              <button onClick={() => { setShowNewProject(false); setNewProjectName('') }}
                className="text-sm text-gray-500 dark:text-gray-400 hover:underline">Cancel</button>
            </div>
          )}
          {selectedProjectId && (
            <button onClick={() => deleteProject(selectedProjectId)} disabled={loading}
              className="text-sm text-red-500 hover:underline ml-auto">Delete Project</button>
          )}
        </div>
        <p className="text-xs text-slate-500 dark:text-slate-400">Projects group your table selections and generated definitions. Create one per use-case or team.</p>
        {selectedProjectId && selectedTables.length > 0 && (
          <div className="mt-2 text-xs text-gray-500 dark:text-gray-400">
            <span className="font-medium">Tables:</span> {selectedTables.join(', ')}
          </div>
        )}
      </section>

      {/* Table Selection */}
      <section className={section}>
        <h2 className="text-lg font-semibold mb-1 dark:text-gray-100">Select Tables</h2>
        <p className="text-xs text-slate-500 dark:text-slate-400 mb-3">Choose the source tables you want to create metric views for. Tables can span multiple schemas.</p>
        <details className="mb-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-700/50 rounded-md text-xs text-blue-800 dark:text-blue-200 leading-relaxed group">
          <summary className="px-3 py-2 font-semibold cursor-pointer select-none flex items-center gap-1.5">
            <svg className="w-3 h-3 transition-transform group-open:rotate-90 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
            Which tables make good metric views?
          </summary>
          <div className="px-3 pb-2 space-y-1.5">
            <ul className="list-disc ml-4 space-y-1">
              <li><span className="font-medium">Star schema (richest output):</span> select fact / transaction tables (<code className="px-1 bg-blue-100 dark:bg-blue-800/40 rounded">fct_</code>, <code className="px-1 bg-blue-100 dark:bg-blue-800/40 rounded">fact_</code>) together with their related dimension tables (<code className="px-1 bg-blue-100 dark:bg-blue-800/40 rounded">dim_</code>). The generator uses approved foreign keys to join them, producing multi-dimensional breakdowns (e.g. revenue by region, product, and month). Run and approve FK predictions first &mdash; without FKs it can only build single-table views.</li>
              <li><span className="font-medium">Pre-aggregated gold / data marts:</span> if a table is already heavily aggregated (one row per summarized grain), select it on its own. You'll get a simple single-table metric view with direct aggregations and no joins. Don't expect joins, and don't mix marts with raw facts in the same selection.</li>
              <li>Avoid raw / bronze / staging, audit / log, and purely operational tables.</li>
              <li>Don't select dimension tables by themselves &mdash; they carry no measures.</li>
            </ul>
            <div className="text-blue-700/80 dark:text-blue-300/80">Only tables that already have core metadata appear in the list below.</div>
          </div>
        </details>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
          <div>
            <label className={label}>Catalog</label>
            <select value={selectedCatalog} onChange={e => { setSelectedCatalog(e.target.value); setSelectedSchema('') }}
              className={input}>
              <option value="">-- select --</option>
              {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div>
            <label className={label}>Schema</label>
            <select value={selectedSchema} onChange={e => setSelectedSchema(e.target.value)}
              className={input} disabled={!selectedCatalog}>
              <option value="">-- select --</option>
              {schemas.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
          <div>
            <label className={label}>Filter tables</label>
            <input value={tableFilter} onChange={e => setTableFilter(e.target.value)}
              placeholder="Type to filter..." className={input} aria-label="Filter tables" />
          </div>
        </div>
        {allTables.length > 0 && (
          <>
            <div className="flex gap-2 mb-2 text-xs">
              <button onClick={selectAll} className="text-blue-600 dark:text-blue-400 hover:underline">Select all ({filteredTables.length})</button>
              <button onClick={selectNone} className="text-blue-600 dark:text-blue-400 hover:underline">Clear schema</button>
              <span className="text-gray-400 ml-auto">{selectedTables.length} selected total</span>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-1 max-h-48 overflow-y-auto border dark:border-gray-600 rounded-md p-2">
              {filteredTables.map(t => (
                <label key={t} className="flex items-center gap-1.5 text-xs cursor-pointer py-0.5 dark:text-gray-200">
                  <input type="checkbox" checked={isTableSelected(t)} onChange={() => toggleTable(t)} className="rounded" />
                  {t}
                </label>
              ))}
            </div>
          </>
        )}
        {selectedCatalog && selectedSchema && allTables.length === 0 && allSchemaTableCount === 0 && (
          <p className="text-sm text-gray-500 dark:text-gray-400 italic">No managed tables found in {selectedCatalog}.{selectedSchema}</p>
        )}
        {selectedCatalog && selectedSchema && allTables.length === 0 && allSchemaTableCount > 0 && (
          <p className="text-sm text-amber-600 dark:text-amber-400 italic">
            {allSchemaTableCount} table{allSchemaTableCount !== 1 ? 's' : ''} found in {selectedCatalog}.{selectedSchema}, but none have core metadata yet. Run <strong>Generate Core Metadata</strong> first, then return here to select tables.
          </p>
        )}
        {allTables.length > 0 && allSchemaTableCount > allTables.length && (
          <p className="text-xs text-slate-500 dark:text-slate-400 mt-1.5">
            Showing {allTables.length} of {allSchemaTableCount} tables &mdash; {allSchemaTableCount - allTables.length} table{allSchemaTableCount - allTables.length !== 1 ? 's' : ''} not shown because core metadata has not been generated for them yet.
          </p>
        )}
        {selectedTables.length > 0 && (
          <div className="mt-4">
            <div className="flex items-center gap-2 mb-2">
              <span className="text-xs font-medium text-slate-600 dark:text-slate-300">Selected Tables ({selectedTables.length})</span>
              <button onClick={() => setSelectedTables([])} className="text-xs text-red-500 hover:underline">Clear all</button>
              {selectedProjectId && (
                <button onClick={saveProjectTables} disabled={tableSaveStatus === 'saving'}
                  className="text-xs px-2 py-0.5 rounded bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 ml-auto">
                  {tableSaveStatus === 'saving' ? 'Saving...' : 'Save Tables'}
                </button>
              )}
              {tableSaveStatus === 'saved' && <span className="text-xs text-green-600 dark:text-green-400">Saved</span>}
              {tableSaveStatus === 'pending' && <span className="text-xs text-amber-500 dark:text-amber-400">Unsaved</span>}
              {tableSaveStatus === 'error' && <span className="text-xs text-red-500">Failed to save table selection</span>}
            </div>
            <div className="flex flex-wrap gap-1.5">
              {selectedTables.map(t => (
                <span key={t} className="inline-flex items-center gap-1 px-2 py-1 text-xs bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-md text-slate-700 dark:text-slate-300">
                  {t}
                  <button onClick={() => removeTable(t)} className="text-slate-400 hover:text-red-500 ml-0.5">&times;</button>
                </span>
              ))}
            </div>
          </div>
        )}
      </section>

      </>}

      {/* === Questions & KPIs Tab === */}
      {activeTab === 'questions' && <>

      {/* Question Profiles */}
      <section className={section}>
        <h2 className="text-lg font-semibold mb-1 dark:text-gray-100">Question Profile</h2>
        <p className="text-xs text-slate-500 dark:text-slate-400 mb-3">Write the business questions your analysts would ask. These drive which metric views get generated. Profiles let you save and reuse question sets across projects.</p>
        <div className="px-3 py-2 mb-3 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700/50 rounded-md text-xs text-amber-800 dark:text-amber-300 leading-relaxed">
          <span className="font-semibold">Coverage tip:</span> For best results, ensure your questions span all selected tables and key business areas. Each table should be referenced by at least one question. Tables without questions won't produce metric views.
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div>
            <label className={label}>Load Profile</label>
            <select value={activeProfileId} onChange={e => selectProfile(e.target.value)} className={input}>
              <option value="">-- new profile --</option>
              {profiles.map(p => (
                <option key={p.profile_id} value={p.profile_id}>
                  {p.profile_name} ({(() => { try { return JSON.parse(p.questions || '[]').length } catch { return '?' } })()}q)
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className={label}>Profile Name</label>
            <input value={profileName} onChange={e => setProfileName(e.target.value)}
              placeholder="e.g. Sales Analytics Questions" className={input} />
          </div>
        </div>
        <div className="flex items-center flex-wrap gap-x-3 gap-y-1 mb-1">
          <label className={`${label} !mb-0`}>Business Context <span className="text-gray-400 font-normal">(optional -- describe your industry, strategic priorities, or key terminology to steer all generation)</span></label>
          <button
            type="button"
            onClick={suggestBusinessContext}
            disabled={bizCtxLoading || !selectedTables.length}
            title={!selectedTables.length ? 'Select tables first' : `Draft from ${bizCtxUseKb ? 'knowledge-base descriptions' : 'table comments'}`}
            className="text-xs px-2 py-1 rounded-md bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300 hover:bg-amber-200 dark:hover:bg-amber-800 disabled:opacity-50">
            {bizCtxLoading ? 'Drafting…' : '✨ Suggest from tables'}
          </button>
          <label className="inline-flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400 cursor-pointer" title="Use the generated descriptions from the knowledge base instead of the live table comments.">
            <input type="checkbox" checked={bizCtxUseKb} onChange={e => setBizCtxUseKb(e.target.checked)} className="rounded" />
            Use knowledge-base descriptions
          </label>
        </div>
        <textarea value={businessContext} onChange={e => setBusinessContext(e.target.value)}
          placeholder={"e.g. We are a B2B SaaS company focused on enterprise sales. Key metrics: ARR, net revenue retention, pipeline velocity. Our fiscal year starts in February."}
          className={`${input} h-20 mb-4`} />

        {/* Seed from an existing Genie space (items 14/15): pull its curated
            example SQL so generation reuses proven measures/dimensions/joins. */}
        <details className="mb-4 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50/60 dark:bg-slate-800/30 group" onToggle={e => { if (e.target.open) loadGenieSpaces() }}>
          <summary className="px-3 py-2 text-sm font-medium cursor-pointer select-none flex items-center gap-1.5 text-slate-700 dark:text-slate-200">
            <svg className="w-3 h-3 transition-transform group-open:rotate-90 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" /></svg>
            Seed from a Genie space <span className="text-xs font-normal text-slate-400">(optional — reuse curated SQL as proven patterns)</span>
          </summary>
          <div className="px-3 pb-3 space-y-2">
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Pull the curated example SQL &amp; benchmarks from an existing Genie space. Generation then treats them as proven query patterns — reusing the measures, dimensions, grains, and joins that already work — to build metric views that cover a data-mart layer without changing the Genie space.
            </p>
            {genieSpacesLoading && <p className="text-xs text-slate-400">Loading Genie spaces…</p>}
            {genieSpaces !== null && genieSpaces.length === 0 && !genieSpacesLoading && (
              <p className="text-xs text-amber-600 dark:text-amber-400">No Genie spaces found in this workspace.</p>
            )}
            {genieSpaces !== null && genieSpaces.length > 0 && (
              <div className="max-h-40 overflow-y-auto border dark:border-slate-600 rounded-md p-2 space-y-0.5">
                {genieSpaces.map(sp => (
                  <label key={sp.space_id} className="flex items-start gap-1.5 text-xs cursor-pointer py-0.5 dark:text-slate-200">
                    <input type="checkbox" className="rounded mt-0.5"
                      checked={genieSelectedSpaces.includes(sp.space_id)}
                      onChange={e => setGenieSelectedSpaces(prev => e.target.checked ? [...prev, sp.space_id] : prev.filter(x => x !== sp.space_id))} />
                    <span className="truncate" title={sp.description || sp.title}>{sp.title || sp.space_id}</span>
                  </label>
                ))}
              </div>
            )}
            <div className="flex items-center gap-2">
              <button type="button" onClick={pullGenieSql}
                disabled={geniePullLoading || !genieSelectedSpaces.length}
                className="text-xs px-2 py-1 rounded-md bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300 hover:bg-blue-200 dark:hover:bg-blue-800 disabled:opacity-50">
                {geniePullLoading ? 'Pulling…' : `Pull example SQL${genieSelectedSpaces.length ? ` (${genieSelectedSpaces.length})` : ''}`}
              </button>
              {geniePullStatus && <span className="text-xs text-green-600 dark:text-green-400">{geniePullStatus}</span>}
            </div>
          </div>
        </details>

        <label className={label}>Business Questions (one per line)</label>
        <textarea value={questionsText} onChange={e => setQuestionsText(e.target.value)}
          placeholder={"What was total revenue by region last quarter?\nHow many orders per month by product category?\nWhat is the average deal size by sales rep?"}
          className={`${input} h-32 mb-3`} />
        <div className="flex gap-3 items-center flex-wrap">
          <button onClick={suggestQuestions} disabled={suggestQLoading || !selectedTables.length}
            className="px-4 py-2 bg-amber-600 text-white rounded-md text-sm hover:bg-amber-700 disabled:opacity-50">
            {suggestQLoading ? 'Suggesting...' : 'Suggest Questions'}
          </button>
          {suggestQProgress && <span className="text-xs text-slate-500 dark:text-slate-400 italic">{suggestQProgress}</span>}
          <button onClick={saveProfile} disabled={loading || !profileName.trim() || !questionLines.length}
            className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm hover:bg-blue-700 disabled:opacity-50">
            {activeProfileId ? 'Update Profile' : 'Save Profile'}
          </button>
          {activeProfileId && (
            <button onClick={deleteProfile} disabled={loading}
              className="px-4 py-2 bg-red-600 text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50">
              Delete
            </button>
          )}
          {questionLines.length > 0 && (
            <span className="text-xs text-gray-500 dark:text-gray-400">{questionLines.length} question{questionLines.length !== 1 ? 's' : ''}</span>
          )}
        </div>
        <p className="text-xs text-gray-400 dark:text-gray-500 mt-2">Click multiple times to expand coverage -- each run generates new questions that complement the existing ones.</p>
        {/* Drive `current` from the questions actually shown (questionLines), not the
            backend's separately-counted value — the backend counts by profile scope and
            can read 0 while the list shows several, producing "0 of ~N". Only recommend
            more when the visible count is below the backend's target. */}
        {genSufficiency?.questions?.recommended && questionLines.length < genSufficiency.questions.recommended && (
          <div className="mt-2 px-3 py-2 rounded-md bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700/50 text-xs text-amber-800 dark:text-amber-300 leading-relaxed">
            <span className="font-semibold">Recommendation:</span>{' '}
            generate more questions &mdash; {questionLines.length} of ~{genSufficiency.questions.recommended} suggested
            {genSufficiency.questions.reasons?.length > 0 && <> ({genSufficiency.questions.reasons.join(', ')})</>}.
          </div>
        )}
      </section>

      {/* KPI Library */}
      <section className={section}>
        <div className="flex items-center justify-between mb-3">
          <div>
            <h2 className="text-lg font-semibold dark:text-gray-100">KPI Library</h2>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Define or auto-suggest business KPIs from your selected tables. KPIs feed into metric view generation and Genie space configuration. Run multiple times for broader coverage -- each pass generates different KPIs.</p>
            {/* Use the visible KPI count (kpis.length) for `current`, not the backend's
                profile-scoped COUNT(*) which can read 0 while the library shows several
                (the "0 of ~8" bug). Recommend more only when the visible count is under
                the backend target. */}
            {genSufficiency?.kpis?.recommended && kpis.length < genSufficiency.kpis.recommended ? (
              <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">
                Recommendation: generate more KPIs &mdash; {kpis.length} of ~{genSufficiency.kpis.recommended}
                {genSufficiency.kpis.reasons?.length > 0 && <> ({genSufficiency.kpis.reasons.join(', ')})</>}.
              </p>
            ) : (
              <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">The more validated KPIs per fact table, the richer the generated metric views will be.</p>
            )}
          </div>
          <div className="flex gap-2">
            <button onClick={suggestKpis} disabled={kpiSuggesting || !selectedTables.length}
              className="px-3 py-1.5 bg-teal-600 text-white rounded text-xs hover:bg-teal-700 disabled:opacity-50">
              {kpiSuggesting ? 'Suggesting...' : 'Auto-Suggest KPIs'}
            </button>
            <button onClick={() => { setShowKpiForm(true); setKpiEditId(null); setKpiDraft({ name: '', description: '', formula: '', domain: '', target_tables: [] }) }}
              className="px-3 py-1.5 bg-dbx-blue text-white rounded text-xs hover:bg-blue-700">+ Add KPI</button>
            {kpis.some(k => (k.validation_status || '').toLowerCase() === 'invalid') && (
              <button onClick={deleteInvalidKpis}
                title="Delete every KPI whose formula failed to validate against its source table(s)."
                className="px-3 py-1.5 bg-amber-600 text-white rounded text-xs hover:bg-amber-700">
                Delete all invalid ({kpis.filter(k => (k.validation_status || '').toLowerCase() === 'invalid').length})
              </button>
            )}
            {kpis.length > 0 && (
              <button onClick={deleteAllKpis}
                className="px-3 py-1.5 bg-red-600 text-white rounded text-xs hover:bg-red-700">Delete All</button>
            )}
          </div>
        </div>

        {/* Validation-status filter. Counts reflect the full library; selecting a
            filter narrows what's rendered below (invalid KPIs carry their failure
            reason in the status-badge tooltip). */}
        {kpis.length > 0 && (() => {
          const counts = kpis.reduce((acc, k) => {
            const s = (k.validation_status || '').toLowerCase()
            acc.all += 1
            if (s === 'valid') acc.valid += 1
            else if (s === 'invalid') acc.invalid += 1
            else if (s === 'empty') acc.empty += 1
            return acc
          }, { all: 0, valid: 0, invalid: 0, empty: 0 })
          const tabs = [
            ['all', 'All', counts.all],
            ['valid', 'Valid', counts.valid],
            ['invalid', 'Invalid', counts.invalid],
            ['empty', 'No data', counts.empty],
          ]
          return (
            <div className="flex gap-1 mb-3">
              {tabs.map(([key, label, n]) => (
                <button key={key} onClick={() => setKpiFilter(key)}
                  className={`px-2.5 py-1 text-xs rounded-full border ${
                    kpiFilter === key
                      ? 'bg-slate-800 text-white border-slate-800 dark:bg-slate-200 dark:text-slate-900 dark:border-slate-200'
                      : 'border-slate-300 dark:border-slate-600 text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700'
                  }`}>
                  {label} <span className="opacity-70">({n})</span>
                </button>
              ))}
            </div>
          )
        })()}

        {showKpiForm && (
          <div className="border border-slate-200 dark:border-slate-700 rounded-lg p-4 mb-3 space-y-2 bg-slate-50 dark:bg-slate-800/50">
            <input value={kpiDraft.name} onChange={e => setKpiDraft(d => ({ ...d, name: e.target.value }))}
              placeholder="KPI Name (e.g. Monthly Revenue Growth)" className="input-base w-full" />
            <textarea value={kpiDraft.description} onChange={e => setKpiDraft(d => ({ ...d, description: e.target.value }))}
              placeholder="Business description" rows={2} className="input-base w-full" />
            <input value={kpiDraft.formula} onChange={e => setKpiDraft(d => ({ ...d, formula: e.target.value }))}
              placeholder="SQL formula (e.g. SUM(orders.total_amount))" className="input-base w-full" />
            <div>
              <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">Source table &mdash; which table this KPI's formula runs against</label>
              <select value={(kpiDraft.target_tables && kpiDraft.target_tables[0]) || ''}
                onChange={e => setKpiDraft(d => ({ ...d, target_tables: e.target.value ? [e.target.value] : [] }))}
                className="input-base w-full">
                <option value="">Auto (validate against all selected tables)</option>
                {selectedTables.map(t => <option key={t} value={t}>{t.split('.').slice(-1)[0]}</option>)}
              </select>
            </div>
            <div className="flex gap-2">
              <input value={kpiDraft.domain} onChange={e => setKpiDraft(d => ({ ...d, domain: e.target.value }))}
                placeholder="Domain (e.g. sales)" className="input-base flex-1" />
              <button onClick={() => saveKpi()} disabled={!kpiDraft.name.trim()} className={btnPrimary}>{kpiEditId ? 'Update' : 'Save'}</button>
              <button onClick={() => setShowKpiForm(false)} className="px-3 py-1.5 bg-slate-200 dark:bg-slate-700 rounded text-xs">Cancel</button>
            </div>
          </div>
        )}
        {kpis.length > 0 && (() => {
          // Surface validation status so users don't have to query kpi_definitions
          // directly. validation_error goes in the tooltip. resolved_table (set by
          // any-table-valid validation) shows which table the formula validated against.
          const kpiStatusBadge = (status, error) => {
            const s = (status || '').toLowerCase()
            const styles = {
              valid: 'bg-green-100 dark:bg-green-900/40 text-green-700 dark:text-green-300',
              empty: 'bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300',
              invalid: 'bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300',
              unchecked: 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300',
            }
            const labels = { valid: 'Valid', empty: 'No data', invalid: 'Invalid', unchecked: 'Unchecked' }
            if (!labels[s]) return null  // skipped / unknown -> no badge
            return (
              <span className={`ml-2 text-xs px-1.5 py-0.5 rounded ${styles[s]}`}
                title={error || labels[s]}>{labels[s]}</span>
            )
          }
          const KpiRow = ({ k, dimmed }) => (
            <div className={`flex items-start justify-between gap-3 border rounded-lg px-3 py-2 text-sm ${dimmed ? 'border-slate-200/60 dark:border-slate-700/50 opacity-60' : 'border-slate-200 dark:border-slate-700'}`}>
              <div className="flex-1 min-w-0">
                <span className="font-medium dark:text-gray-200">{k.name}</span>
                {k.domain && <span className="ml-2 text-xs px-1.5 py-0.5 rounded bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300">{k.domain}</span>}
                {kpiStatusBadge(k.validation_status, k.validation_error)}
                {k.description && <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5 truncate">{k.description}</p>}
                {k.formula && <code className="text-xs text-gray-400 dark:text-gray-500 block mt-0.5 truncate">{k.formula}</code>}
                {k.resolved_table && <p className="text-[10px] text-slate-400 dark:text-slate-500 mt-0.5">validated against {k.resolved_table.split('.').slice(-1)[0]}</p>}
              </div>
              <div className="flex gap-1 shrink-0">
                <button onClick={() => { setKpiEditId(k.kpi_id); setKpiDraft({ name: k.name, description: k.description || '', formula: k.formula || '', domain: k.domain || '', target_tables: Array.isArray(k.target_tables) ? k.target_tables : [] }); setShowKpiForm(true) }}
                  className="text-xs text-blue-600 hover:underline">Edit</button>
                <button onClick={() => deleteKpi(k.kpi_id)} className="text-xs text-red-500 hover:underline">Del</button>
              </div>
            </div>
          )
          const toggleSection = (key) => setExpandedKpiSections(prev => {
            const next = new Set(prev)
            next.has(key) ? next.delete(key) : next.add(key)
            return next
          })
          // Apply the validation-status filter to what's rendered (counts above
          // stay full-library). 'all' is the identity filter.
          const visibleKpis = kpiFilter === 'all'
            ? kpis
            : kpis.filter(k => (k.validation_status || '').toLowerCase() === kpiFilter)
          if (visibleKpis.length === 0) return (
            <p className="text-xs text-slate-400 dark:text-slate-500">No {kpiFilter} KPIs.</p>
          )
          if (!profiles.length) return (
            <div>
              <p className="text-xs text-slate-400 dark:text-slate-500 mb-2">Create a Question Profile above to organize KPIs.</p>
              <div className="space-y-1.5">{visibleKpis.map(k => <KpiRow key={k.kpi_id} k={k} />)}</div>
            </div>
          )
          const assigned = new Set()
          const sections = profiles.map(p => {
            const matched = visibleKpis.filter(k => k.profile_id === p.profile_id)
            matched.forEach(k => assigned.add(k.kpi_id))
            return { key: p.profile_id, label: p.profile_name, kpis: matched }
          }).filter(s => s.kpis.length > 0)
          const unassigned = visibleKpis.filter(k => !assigned.has(k.kpi_id))
          return (
            <div className="space-y-3">
              {sections.map(s => {
                const open = expandedKpiSections.has(s.key) || s.key === activeProfileId
                return (
                  <div key={s.key}>
                    <button onClick={() => toggleSection(s.key)} className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide hover:text-slate-600 dark:hover:text-slate-300 flex items-center gap-1">
                      <span className={`transition-transform ${open ? 'rotate-90' : ''}`}>&#9654;</span>
                      {s.label} ({s.kpis.length})
                    </button>
                    {open && (
                      <div className="space-y-1.5 mt-1.5">{s.kpis.map(k => <KpiRow key={k.kpi_id} k={k} />)}</div>
                    )}
                  </div>
                )
              })}
              {unassigned.length > 0 && (
                <div>
                  <button onClick={() => toggleSection('__unassigned')} className="text-xs font-semibold text-slate-400 dark:text-slate-500 uppercase tracking-wide hover:text-slate-600 dark:hover:text-slate-300 flex items-center gap-1">
                    <span className={`transition-transform ${expandedKpiSections.has('__unassigned') ? 'rotate-90' : ''}`}>&#9654;</span>
                    Unassigned ({unassigned.length})
                  </button>
                  <p className="text-[10px] text-slate-400 dark:text-slate-500 ml-4 mt-0.5">Created before profile support. Re-save from a profile to assign.</p>
                  {expandedKpiSections.has('__unassigned') && (
                    <div className="space-y-1.5 mt-1.5">{unassigned.map(k => <KpiRow key={k.kpi_id} k={k} dimmed />)}</div>
                  )}
                </div>
              )}
            </div>
          )
        })()}
        {kpis.length === 0 && !showKpiForm && (
          <EmptyState title="No KPIs defined yet" description="Add manually or use auto-suggest above" />
        )}
      </section>

      </>}

      {/* === Model Tab (recommended, editable ERD) === */}
      {activeTab === 'model' && (
        <section className={section}>
          <div className="flex items-center gap-2 mb-1">
            <h2 className="text-lg font-semibold dark:text-gray-100">Data model</h2>
            <span className="text-xs text-slate-400">recommended from your metadata</span>
          </div>
          <p className="text-xs text-slate-500 dark:text-slate-400 mb-3 leading-relaxed">
            dbxmetagen inferred this star schema from FK predictions, ontology, and column profiling.
            Confirm which tables are <span className="font-medium">facts</span>, adjust the joins, and save &mdash;
            the model seeds metric-view generation (facts become view sources; confirmed joins are used directly).
          </p>
          <ErdDesigner
            tables={selectedTables}
            projectId={selectedProjectId}
            profileId={activeProfileId}
            businessContext={businessContext}
            onSaved={() => { refreshDefinitions(); onRefreshPipelineStats?.() }}
          />
        </section>
      )}

      {/* === Generate Tab === */}
      {activeTab === 'generate' && (() => {
        const profileFiltered = activeProfileId ? kpis.filter(k => k.profile_id === activeProfileId) : kpis
        const relevantKpis = profileFiltered.filter(k => kpiMatchesTables(k, selectedTables))
        return <>

      {/* Generation inputs summary */}
      <section className={section}>
        <h2 className="text-lg font-semibold mb-1 dark:text-gray-100">Generation Inputs</h2>
        <p className="text-xs text-slate-500 dark:text-slate-400 mb-3">Review your inputs below, then scroll down and click Generate. AI will create YAML metric view definitions from your tables, questions, and KPIs.</p>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3 text-sm">
          <div className="p-3 rounded-lg bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700">
            <span className="text-xs text-slate-500 dark:text-slate-400 block mb-1">Project</span>
            <span className="font-medium dark:text-gray-200">
              {selectedProjectId ? projects.find(p => p.project_id === selectedProjectId)?.project_name || 'Unknown' : 'None'}
            </span>
          </div>
          <div className="p-3 rounded-lg bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700">
            <span className="text-xs text-slate-500 dark:text-slate-400 block mb-1">Tables</span>
            <span className={`font-medium ${selectedTables.length ? 'dark:text-gray-200' : 'text-amber-600 dark:text-amber-400'}`}>
              {selectedTables.length || 'None selected'}
            </span>
            {selectedTables.length > 0 && selectedTables.length <= 5 && (
              <p className="text-xs text-slate-400 mt-0.5 truncate">{selectedTables.join(', ')}</p>
            )}
          </div>
          <div className="p-3 rounded-lg bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700">
            <span className="text-xs text-slate-500 dark:text-slate-400 block mb-1">Questions</span>
            <span className={`font-medium ${questionLines.length ? 'dark:text-gray-200' : 'text-amber-600 dark:text-amber-400'}`}>
              {questionLines.length || 'None'}
            </span>
          </div>
          <div className="p-3 rounded-lg bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700">
            <span className="text-xs text-slate-500 dark:text-slate-400 block mb-1">Business Context</span>
            <span className="font-medium dark:text-gray-200 text-xs">
              {businessContext ? (businessContext.length > 80 ? businessContext.slice(0, 80) + '...' : businessContext) : 'None'}
            </span>
          </div>
          <div className="p-3 rounded-lg bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-700">
            <span className="text-xs text-slate-500 dark:text-slate-400 block mb-1">Matching KPIs{activeProfileId ? ' (profile)' : ''}</span>
            <span className="font-medium dark:text-gray-200">
              {selectedTables.length ? `${relevantKpis.length} of ${profileFiltered.length}` : `${profileFiltered.length} total`}
            </span>
          </div>
        </div>
        {(!selectedTables.length || !questionLines.length) && (
          <div className="flex gap-2 mt-3">
            {!selectedTables.length && (
              <button onClick={() => setActiveTab('setup')} className="text-xs text-blue-600 dark:text-blue-400 hover:underline">
                Select tables in Setup
              </button>
            )}
            {!questionLines.length && (
              <button onClick={() => setActiveTab('questions')} className="text-xs text-blue-600 dark:text-blue-400 hover:underline">
                Add questions in Questions & KPIs
              </button>
            )}
          </div>
        )}
      </section>

      {/* Generate actions */}
      <section className={section}>
        <h2 className="text-lg font-semibold mb-2 dark:text-gray-100">Generate Metric Views</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-3">
          Uses AI to analyze your questions against the catalog metadata for the selected tables and generate metric view definitions.
        </p>
        {/* Targeted (theme-based) generation is hidden for now -- the fact-grain
            ("comprehensive") strategy is the recommended approach and the one we're
            hardening. generationStyle stays 'comprehensive' (its default) so the
            payload is unchanged; restore this toggle to re-expose the theme mode. */}
        <p className="text-xs text-slate-400 dark:text-slate-500 mb-4">
          Best practice: one comprehensive metric view per fact-table grain.
        </p>
        {(() => {
          const recommended = erdSufficiency?.metric_views_recommended
            || Math.min(Math.max(Math.floor(selectedTables.length / 3), 2), 15)
          return <>
        <div className="flex items-center gap-2 mb-2">
          <label className="text-sm font-medium text-slate-700 dark:text-slate-200 whitespace-nowrap">Max views</label>
          <input type="number" min={1}
            max={Math.max(Math.floor(selectedTables.length / 2), 2)}
            placeholder={String(recommended)}
            value={maxViews ?? ''}
            onChange={e => setMaxViews(e.target.value ? parseInt(e.target.value) : null)}
            className="w-16 px-2 py-1 border rounded text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" />
          <span className="text-xs text-slate-400 dark:text-slate-500"
            title="Each metric view costs 1-2 AI_QUERY calls. More views = more cost and generation time.">
            of {selectedTables.length} tables (recommended: {recommended})
          </span>
          {maxViews == null && (
            <button onClick={() => setMaxViews(recommended)}
              className="text-xs text-dbx-lava hover:underline">use {recommended}</button>
          )}
        </div>
        {erdSufficiency && (erdSufficiency.reasons?.length > 0 || erdSufficiency.missing_kpis?.length > 0) && (
          <div className="mb-4 px-3 py-2 rounded-md bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700/50 text-xs text-amber-800 dark:text-amber-300 leading-relaxed">
            <span className="font-semibold">Coverage recommendation:</span>{' '}
            {erdSufficiency.reasons?.join(' · ')}
            {erdSufficiency.missing_kpis?.length > 0 && <> — {erdSufficiency.missing_kpis.length} KPI(s) not yet covered by a measure.</>}
            {onNavigate && <> Refine the data model in <button onClick={() => setActiveTab('model')} className="font-semibold underline">Model</button>.</>}
          </div>
        )}
        {erdSufficiency?.fanout_warnings?.length > 0 && (
          <div className="mb-4 px-3 py-2 rounded-md bg-rose-50 dark:bg-rose-900/20 border border-rose-200 dark:border-rose-700/50 text-xs text-rose-800 dark:text-rose-300 leading-relaxed">
            <span className="font-semibold">Fan-out risk ({erdSufficiency.fanout_warnings.length}):</span>{' '}
            <ul className="list-disc ml-4 mt-1 space-y-0.5">
              {erdSufficiency.fanout_warnings.map((w, i) => <li key={i}>{w}</li>)}
            </ul>
          </div>
        )}
          </>
        })()}
        <div className="mb-4 p-4 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50/50 dark:bg-slate-900/30">
          <div className="flex items-center gap-2 mb-2">
            <h3 className="text-sm font-semibold text-slate-800 dark:text-slate-100">Query acceleration (Materialization)</h3>
            <span className="px-1.5 py-0.5 rounded text-[10px] font-semibold bg-violet-100 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300">Public Preview</span>
          </div>
          <p className="text-xs text-slate-500 dark:text-slate-400 mb-3">
            Pre-compute joins and filters via an unaggregated materialized view; Databricks routes eligible queries automatically.
          </p>
          <label className="flex items-center gap-2 mb-3 cursor-pointer select-none">
            <input type="checkbox" checked={materialize}
              onChange={e => setMaterialize(e.target.checked)}
              className="accent-dbx-lava w-4 h-4" />
            <span className="text-sm font-medium text-slate-700 dark:text-slate-200">Enable materialization for generated views</span>
          </label>
          {materialize && (
            <div className="space-y-3">
              <div className="flex items-center gap-2 flex-wrap">
                <label className="text-sm font-medium text-slate-700 dark:text-slate-200 whitespace-nowrap">Refresh schedule</label>
                <select value={matSchedulePreset}
                  onChange={e => {
                    const preset = e.target.value
                    setMatSchedulePreset(preset)
                    const p = MAT_SCHEDULE_PRESETS.find(x => x.id === preset)
                    if (p && p.value !== '__custom__') setMaterializationSchedule(p.value)
                  }}
                  className="px-2 py-1 border rounded text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white">
                  {MAT_SCHEDULE_PRESETS.map(p => <option key={p.id} value={p.id}>{p.label}</option>)}
                </select>
                {(matSchedulePreset === 'custom' || !MAT_SCHEDULE_PRESETS.some(p => p.id !== 'custom' && p.value === materializationSchedule)) && (
                  <input type="text"
                    value={materializationSchedule}
                    onChange={e => { setMaterializationSchedule(e.target.value); setMatSchedulePreset('custom') }}
                    placeholder="every 6 hours"
                    className="w-48 px-2 py-1 border rounded text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" />
                )}
              </div>
              <div className="p-3 rounded-md border border-amber-200 dark:border-amber-800 bg-amber-50/80 dark:bg-amber-900/20 text-xs text-amber-900 dark:text-amber-200 space-y-1">
                <p className="font-medium">Requirements</p>
                <ul className="list-disc ml-4 space-y-0.5">
                  <li>Serverless compute enabled in workspace</li>
                  <li>SQL warehouse on DBR 17.3+</li>
                  <li>Creates a Databricks-managed Lakeflow pipeline per metric view</li>
                </ul>
                <p>
                  <a href="https://docs.databricks.com/en/metric-views/materialization" target="_blank" rel="noopener noreferrer"
                    className="text-amber-800 dark:text-amber-300 underline">Databricks materialization docs</a>
                  {' '}&mdash; unaggregated baseline may not help simple single-table views.
                </p>
              </div>
            </div>
          )}
        </div>
        <p className="text-xs text-slate-500 dark:text-slate-400 mb-4">
          Materialization: <strong>{materialize ? `ON (${materializationSchedule || 'manual refresh'})` : 'OFF'}</strong>
        </p>
        {!foundationReady && (
          <div className="rounded-lg border border-amber-200 dark:border-amber-700/40 bg-amber-50/80 dark:bg-amber-900/15 px-4 py-3 text-sm text-slate-600 dark:text-slate-300 mb-3">
            {foundation && !foundation.metadataDone
              ? 'For best results, generate core metadata first — metric views reference table and column descriptions. You can still generate now. '
              : 'For best results, run the analytics pipeline first — metric views build on the ontology, foreign keys, and vector index it produces. You can still generate now. '}
            <button onClick={() => onNavigate?.('jobs')} className="font-semibold text-dbx-lava hover:underline">
              Go to Generate Metadata &rarr;
            </button>
          </div>
        )}
        <div className="flex gap-3 flex-wrap">
          <button onClick={() => startGeneration('replace')}
            disabled={loading || isGenerating || !selectedTables.length || !questionLines.length}
            title={!foundationReady ? 'Recommended after core metadata + analytics pipeline, but you can generate now' : 'Create new metric view definitions (replaces any pending ones)'}
            className={btnPrimary}>
            {isGenerating ? 'Generating...' : 'Generate'}
          </button>
          {selectedProjectId && (
            <button onClick={() => { if (!confirm('Regenerate all definitions in this project? Applied metric views are preserved.')) return; startGeneration('replace_all') }}
              disabled={loading || isGenerating || !selectedTables.length || !questionLines.length}
              title={!foundationReady ? 'Recommended after core metadata + analytics pipeline, but you can generate now' : 'Replace all draft, validated, and failed definitions in this project and regenerate. Applied metric views are preserved.'}
              className="px-4 py-2 bg-dbx-lava text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50">
              Regenerate All
            </button>
          )}
        </div>
      </section>

      {/* Generation Progress */}
      {taskStatus && (
        <section className={section}>
          <h3 className="font-medium mb-2 dark:text-gray-100">Generation Progress</h3>
          <div className="flex items-center gap-3">
            {taskStatus.status === 'running' && (
              <div className="h-4 w-4 border-2 border-dbx-lava border-t-transparent rounded-full animate-spin" />
            )}
            <span className={`text-sm ${taskStatus.status === 'error' ? 'text-red-600 dark:text-red-400' : taskStatus.status === 'done' ? 'text-green-600 dark:text-green-400' : 'text-gray-700 dark:text-gray-300'}`}>
              {STAGES[taskStatus.stage] || taskStatus.stage}
            </span>
            {taskStatus.planned && (
              <span className="text-xs text-gray-500 dark:text-gray-400">
                ({taskStatus.planned} views{taskStatus.effective_max ? ` of ${taskStatus.effective_max} max` : ''}{taskStatus.retry_count ? `, retrying ${taskStatus.retry_count}` : ''})
              </span>
            )}
            {taskStatus.generated && (
              <span className="text-xs text-gray-500 dark:text-gray-400">({taskStatus.generated} definitions)</span>
            )}
          </div>
          {taskStatus.status === 'error' && taskStatus.error && (
            <div className="mt-2 space-y-2">
              <p className="text-sm text-red-600 dark:text-red-400">{taskStatus.error}</p>
              {taskStatus.phase2_failures?.length > 0 && (
                <details className="text-xs text-red-500 dark:text-red-400">
                  <summary className="cursor-pointer hover:underline">
                    {taskStatus.phase2_failures.length} view(s) failed -- details
                  </summary>
                  <ul className="mt-1 ml-3 list-disc space-y-0.5 text-gray-600 dark:text-gray-400">
                    {taskStatus.phase2_failures.map((f, i) => (
                      <li key={i}><strong>{f.name}</strong>: {f.error?.slice(0, 200)}</li>
                    ))}
                  </ul>
                </details>
              )}
            </div>
          )}
          {taskStatus.status === 'done' && taskStatus.result && (
            <div className="text-sm mt-2 space-y-2 text-gray-700 dark:text-gray-300">
              <div className="flex flex-wrap gap-3">
                <span>Generated: <strong>{taskStatus.result.generated}</strong></span>
                <span>Validated: <strong className="text-green-600 dark:text-green-400">{taskStatus.result.validated}</strong></span>
                <span>Failed: <strong className={taskStatus.result.failed ? 'text-red-600 dark:text-red-400' : ''}>{taskStatus.result.failed}</strong></span>
                {taskStatus.result.repaired > 0 && <span>Repaired: <strong className="text-amber-600 dark:text-amber-400">{taskStatus.result.repaired}</strong></span>}
                {taskStatus.result.skipped > 0 && <span>Skipped: <strong>{taskStatus.result.skipped}</strong></span>}
              </div>
              {taskStatus.result.materialize != null && (
                <p className="text-xs text-slate-500 dark:text-slate-400">
                  Materialization: <strong>{taskStatus.result.materialize ? 'enabled' : 'disabled'}</strong>
                  {taskStatus.result.materialize && (
                    <> &mdash; schedule: {taskStatus.result.materialization_schedule || 'manual refresh'};
                    {' '}{taskStatus.result.materialized_count ?? 0} of {taskStatus.result.generated} views materialized</>
                  )}
                </p>
              )}
              {taskStatus.retry_recovered > 0 && (
                <span className="text-xs text-amber-600 dark:text-amber-400">
                  {taskStatus.retry_recovered} view(s) recovered with simplified retry
                </span>
              )}
              {taskStatus.phase2_failures?.length > 0 && (
                <details className="text-xs text-amber-600 dark:text-amber-400 mt-1">
                  <summary className="cursor-pointer hover:underline">{taskStatus.phase2_failures.length} view(s) failed both attempts</summary>
                  <ul className="mt-1 ml-3 list-disc space-y-0.5 text-gray-600 dark:text-gray-400">
                    {taskStatus.phase2_failures.map((f, i) => (
                      <li key={i}><strong>{f.name}</strong>: {f.error?.slice(0, 200)}</li>
                    ))}
                  </ul>
                </details>
              )}
              {taskStatus.result.warnings?.length > 0 && (
                <div className="text-xs text-amber-700 dark:text-amber-400 space-y-0.5">
                  {taskStatus.result.warnings.map((w, i) => <p key={i}>{w}</p>)}
                </div>
              )}
              {taskStatus.result.coverage && (
                <p className="text-xs text-gray-500 dark:text-gray-400">
                  Question coverage: {taskStatus.result.coverage.covered?.length || 0} of {(taskStatus.result.coverage.covered?.length || 0) + (taskStatus.result.coverage.not_covered?.length || 0)} covered
                </p>
              )}
              {taskStatus.result.kpi_coverage && (
                <p className="text-xs text-gray-500 dark:text-gray-400">
                  KPI coverage: {(taskStatus.result.kpi_coverage.implemented || []).length} of {taskStatus.result.kpi_coverage.total || 0} KPIs implemented
                </p>
              )}
              {taskStatus.result.definitions?.length > 0 && (
                <details className="text-xs">
                  <summary className="cursor-pointer text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200">
                    Per-definition breakdown ({taskStatus.result.definitions.length})
                  </summary>
                  <div className="mt-1 space-y-1 pl-2 border-l-2 border-slate-200 dark:border-slate-700">
                    {taskStatus.result.definitions.map((d, i) => (
                      <div key={i} className="flex items-start gap-2">
                        <span className={`shrink-0 px-1 rounded ${d.status === 'validated' ? 'bg-green-100 dark:bg-green-900/40 text-green-700 dark:text-green-300' : d.status === 'failed' ? 'bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300' : 'bg-slate-100 dark:bg-slate-800 text-slate-500'}`}>{d.status}</span>
                        <span className={`shrink-0 px-1 rounded text-[10px] ${d.has_materialization ? 'bg-sky-100 dark:bg-sky-900/40 text-sky-700 dark:text-sky-300' : 'bg-slate-100 dark:bg-slate-800 text-slate-400'}`}>
                          {d.has_materialization ? 'mat' : 'no mat'}
                        </span>
                        <span className="font-medium">{d.name}</span>
                        {d.source && <span className="text-gray-400">({d.source.split('.').pop()})</span>}
                        {d.validation_errors?.length > 0 && <span className="text-red-500 truncate">{d.validation_errors[0]}</span>}
                      </div>
                    ))}
                  </div>
                </details>
              )}
              <p className="text-xs text-gray-500 dark:text-gray-400">
                Validated and applied metric views will be added to the vector index on the next full analytics pipeline or standalone vector index build run.
              </p>
            </div>
          )}
        </section>
      )}

      </>})()}

      {/* === Definitions Tab === */}
      {activeTab === 'definitions' && <>

      <div className="text-xs text-slate-500 dark:text-slate-400 space-y-1">
        <p>Each definition below is a metric view. Lifecycle:
          {' '}<span className="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300">validated</span> (SQL checked, stored, ready to deploy)
          {' '}&rarr; <span className="px-1.5 py-0.5 rounded bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300">applied</span> (deployed as a UC metric view via <strong>Deploy as UC View</strong>)
          {' '}&mdash; a <span className="px-1.5 py-0.5 rounded bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300">failed</span> one couldn't validate or deploy.
          Use <strong>Improve</strong> to re-generate a definition with AI feedback; after deploying, use <strong>Test Queries</strong> to confirm it returns sensible results.</p>
        <p className="text-sky-700 dark:text-sky-400">
          After applying metric views, sync them to the vector store so the agents and Genie can find them:{' '}
          {onNavigate
            ? <button onClick={() => onNavigate('syncops')} className="font-semibold underline">Refresh KG / Index in Sync &amp; Ops</button>
            : <strong>Refresh KG / Index in Sync &amp; Ops</strong>}
          {' '}(or the next full analytics pipeline run).
        </p>
        <p className="text-amber-700 dark:text-amber-400">Note: Only the owner of a metric view can edit it. {userIdentity
          ? <>Views created by this app are owned by you (<strong>{userIdentity}</strong>) via on-behalf-of authentication.</>
          : <>Views created by this app are owned by the app service principal. Use <strong>Transfer Ownership</strong> to take ownership &mdash; this is irreversible for the app.</>
        }</p>
        <div className="flex items-center gap-2">
          <span>Showing definitions for:</span>
          <span className="font-medium text-slate-700 dark:text-slate-200">
            {selectedProjectId ? projects.find(p => p.project_id === selectedProjectId)?.project_name : 'All projects'}
          </span>
          <button onClick={() => setActiveTab('setup')} className="text-blue-600 dark:text-blue-400 hover:underline ml-1">Change</button>
        </div>
      </div>

      {/* Definitions */}
      {definitions.length === 0 ? (
        <section className={section}>
          <div className="text-sm text-slate-500 dark:text-slate-400 text-center py-8 space-y-2">
            <p className="font-medium text-slate-600 dark:text-slate-300">No metric view definitions yet.</p>
            <p className="text-xs">
              1. <button onClick={() => setActiveTab('setup')} className="text-blue-600 dark:text-blue-400 hover:underline font-medium">Setup</button> — pick your tables ·
              {' '}2. <button onClick={() => setActiveTab('questions')} className="text-blue-600 dark:text-blue-400 hover:underline font-medium">Questions &amp; KPIs</button> — define what to measure ·
              {' '}3. <button onClick={() => setActiveTab('generate')} className="text-blue-600 dark:text-blue-400 hover:underline font-medium">Generate</button> — create definitions
            </p>
          </div>
        </section>
      ) : (() => {
        const filtered = definitions.filter(d => {
          if (defStatusFilter !== 'all' && d.status !== defStatusFilter) return false
          if (defMatFilter === 'materialized' && !d.has_materialization) return false
          if (defMatFilter === 'not_materialized' && d.has_materialization) return false
          if (defFilter && !d.metric_view_name?.toLowerCase().includes(defFilter.toLowerCase()) && !d.source_table?.toLowerCase().includes(defFilter.toLowerCase())) return false
          return true
        })
        return (
        <section className={section}>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-lg font-semibold dark:text-gray-100">Metric View Definitions ({definitions.length})</h2>
            <button onClick={refreshDefinitions} className="text-sm text-blue-600 dark:text-blue-400 hover:underline">Refresh</button>
          </div>

          {/* Deploy target override */}
          <div className="flex items-center gap-3 mb-4 p-3 bg-dbx-oat dark:bg-gray-900 rounded-md border dark:border-gray-700 flex-wrap">
            <span className="text-xs font-medium text-gray-600 dark:text-gray-400 whitespace-nowrap" title="Metric views deploy to this output catalog.schema. Required -- results never land in the source table's (possibly read-only/federated) schema.">Output schema <span className="text-red-500">*</span>:</span>
            <select value={schemaOptions.includes(globalTargetOverride) ? globalTargetOverride : (globalTargetOverride || customTargetMode ? '__custom__' : '')}
              onChange={e => { userEditedTargetRef.current = true; if (e.target.value === '__custom__') { setCustomTargetMode(true); setGlobalTargetOverride('') } else { setCustomTargetMode(false); setGlobalTargetOverride(e.target.value) } }}
              className="input-base !text-xs w-56">
              <option value="">(Select an output schema — required)</option>
              {schemaOptions.map(s => <option key={s} value={s}>{s}</option>)}
              <option value="__custom__">Custom catalog.schema...</option>
            </select>
            <input type="text" placeholder="catalog.schema"
              value={!schemaOptions.includes(globalTargetOverride) ? globalTargetOverride : ''}
              onChange={e => { userEditedTargetRef.current = true; setCustomTargetMode(true); setGlobalTargetOverride(e.target.value) }}
              className={`input-base !text-xs w-48 ${schemaOptions.includes(globalTargetOverride) && globalTargetOverride ? 'opacity-40 pointer-events-none' : ''}`}
              title="Type a custom catalog.schema for federated or cross-catalog deploys" />
            {globalTargetOverride && !schemaOptions.includes(globalTargetOverride) && !globalTargetOverride.match(/^[^.]+\.[^.]+$/) && (
              <span className="text-[10px] text-red-500">Format: catalog.schema</span>
            )}
            {!isValidTarget(globalTargetOverride) && (
              <span className="text-[10px] text-amber-600 dark:text-amber-400">Pick an output catalog.schema to enable deploy</span>
            )}
            {definitions.filter(d => d.status === 'validated').length > 0 && (
              <button onClick={createAllValidated}
                disabled={bulkCreating || definitions.filter(d => d.status === 'validated').some(d => !isValidTarget(getEffectiveTarget(d)))}
                title={definitions.filter(d => d.status === 'validated').some(d => !isValidTarget(getEffectiveTarget(d))) ? 'Choose an output catalog.schema first' : ''}
                className="ml-auto px-3 py-1.5 bg-green-600 text-white rounded text-xs hover:bg-green-700 disabled:opacity-50 whitespace-nowrap">
                {bulkCreating ? 'Creating...' : `Create All Validated (${definitions.filter(d => d.status === 'validated').length})`}
              </button>
            )}
          </div>

          {definitions.length > 0 && (
            <div className="flex items-center gap-2 mb-3 -mt-1">
              {definitions.filter(d => d.status !== 'applied').length > 0 && (
                <button onClick={deleteAllNonApplied} disabled={!!bulkDeleting}
                  className="px-2.5 py-1 text-xs text-red-600 dark:text-red-400 border border-red-300 dark:border-red-700 rounded hover:bg-red-50 dark:hover:bg-red-900/20 disabled:opacity-50 whitespace-nowrap">
                  {bulkDeleting === 'non-applied' ? 'Deleting...' : `Delete All Non-Applied (${definitions.filter(d => d.status !== 'applied').length})`}
                </button>
              )}
              {definitions.filter(d => d.status === 'applied').length > 0 && (
                <button onClick={deleteAllApplied} disabled={!!bulkDeleting}
                  className="px-2.5 py-1 text-xs text-red-600 dark:text-red-400 border border-red-300 dark:border-red-700 rounded hover:bg-red-50 dark:hover:bg-red-900/20 disabled:opacity-50 whitespace-nowrap">
                  {bulkDeleting === 'applied' ? 'Dropping & Deleting...' : `Drop & Delete All Applied (${definitions.filter(d => d.status === 'applied').length})`}
                </button>
              )}
              <button onClick={syncToVectorStore} disabled={vectorSyncing}
                title="Indexes metric view definitions into the Vector Search index so the Metric View Agent can find them via semantic search. Run after creating or updating metric views."
                className="px-2.5 py-1 text-xs text-blue-600 dark:text-blue-400 border border-blue-300 dark:border-blue-700 rounded hover:bg-blue-50 dark:hover:bg-blue-900/20 disabled:opacity-50 whitespace-nowrap">
                {vectorSyncing ? 'Syncing...' : 'Sync to Vector Store'}
              </button>
              <button onClick={syncToSemanticGraph} disabled={sgSyncing}
                title="Builds a relationship graph of metric views, their source tables, dimensions, and measures. Powers the Metric View Agent's ability to find related metrics and trace lineage between KPIs."
                className="px-2.5 py-1 text-xs text-purple-600 dark:text-purple-400 border border-purple-300 dark:border-purple-700 rounded hover:bg-purple-50 dark:hover:bg-purple-900/20 disabled:opacity-50 whitespace-nowrap">
                {sgSyncing ? 'Syncing...' : 'Sync to Semantic Graph'}
              </button>
              <button onClick={findDuplicates} disabled={dupLoading}
                className="px-2.5 py-1 text-xs text-amber-600 dark:text-amber-400 border border-amber-300 dark:border-amber-700 rounded hover:bg-amber-50 dark:hover:bg-amber-900/20 disabled:opacity-50 whitespace-nowrap">
                {dupLoading ? 'Scanning...' : 'Find Duplicates'}
              </button>
            </div>
          )}

          {/* Filters (4.2) */}
          <div className="flex items-center gap-2 mb-3">
            <input value={defFilter} onChange={e => setDefFilter(e.target.value)} placeholder="Filter by name or table..."
              className="input-base !text-xs w-56" />
            <div className="inline-flex bg-dbx-oat/60 dark:bg-dbx-navy-600 rounded-lg p-0.5">
              {['all', 'validated', 'applied', 'failed'].map(s => (
                <button key={s} onClick={() => setDefStatusFilter(s)}
                  className={`px-2 py-0.5 text-[10px] rounded-md transition-colors ${defStatusFilter === s ? 'bg-white dark:bg-dbx-navy-500 shadow-sm font-semibold' : 'text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}`}>
                  {s === 'all' ? 'All' : s.charAt(0).toUpperCase() + s.slice(1)}
                </button>
              ))}
            </div>
            <div className="inline-flex bg-dbx-oat/60 dark:bg-dbx-navy-600 rounded-lg p-0.5">
              {[
                { id: 'all', label: 'All mat' },
                { id: 'materialized', label: 'Materialized' },
                { id: 'not_materialized', label: 'No mat' },
              ].map(s => (
                <button key={s.id} onClick={() => setDefMatFilter(s.id)}
                  className={`px-2 py-0.5 text-[10px] rounded-md transition-colors ${defMatFilter === s.id ? 'bg-white dark:bg-dbx-navy-500 shadow-sm font-semibold' : 'text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}`}>
                  {s.label}
                </button>
              ))}
            </div>
            {(defFilter || defStatusFilter !== 'all' || defMatFilter !== 'all') && <span className="text-xs text-slate-400">{filtered.length} of {definitions.length}</span>}
          </div>

          {kpiCoverage && kpiCoverage.total > 0 && (
            <div className="flex items-center gap-2 mb-3 px-2.5 py-1.5 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-xs">
              <span className="font-medium text-slate-600 dark:text-slate-300">KPI Coverage:</span>
              <span className="text-emerald-600 dark:text-emerald-400">{(kpiCoverage.implemented || []).length}/{kpiCoverage.total} implemented</span>
              {(kpiCoverage.missing || []).length > 0 && (
                <span className="text-amber-600 dark:text-amber-400" title={`Missing: ${kpiCoverage.missing.join(', ')}`}>
                  {kpiCoverage.missing.length} missing
                </span>
              )}
            </div>
          )}

          {dupGroups && dupGroups.length > 0 && (
            <div className="mb-4 p-4 border border-amber-300 dark:border-amber-700 rounded-lg bg-amber-50/50 dark:bg-amber-900/10">
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-sm font-semibold text-amber-800 dark:text-amber-300">
                  Duplicate Groups ({dupGroups.length})
                </h3>
                <div className="flex gap-2">
                  <button onClick={resolveDuplicates}
                    className="px-3 py-1 text-xs bg-amber-600 text-white rounded hover:bg-amber-700">
                    Supersede Unselected
                  </button>
                  <button onClick={() => setDupGroups(null)}
                    className="px-2 py-1 text-xs text-slate-500 border border-slate-300 dark:border-slate-600 rounded hover:bg-slate-100 dark:hover:bg-slate-800">
                    Dismiss
                  </button>
                </div>
              </div>
              {dupGroups.map((g, gi) => (
                <div key={gi} className="mb-3 p-3 rounded bg-white dark:bg-dbx-navy-600 border border-slate-200 dark:border-slate-700">
                  <div className="text-xs text-slate-500 dark:text-slate-400 mb-1">
                    <span className="font-medium">{g.source_table}</span> &mdash; {Math.round(g.overlap_score * 100)}% overlap
                  </div>
                  <div className="space-y-1">
                    {g.definitions.map(d => (
                      <label key={d.definition_id} className="flex items-center gap-2 text-xs cursor-pointer">
                        <input type="radio" name={`dup-${gi}`} checked={dupKeep[gi] === d.definition_id}
                          onChange={() => setDupKeep(prev => ({ ...prev, [gi]: d.definition_id }))} />
                        <span className={dupKeep[gi] === d.definition_id ? 'font-semibold' : 'text-slate-600 dark:text-slate-400'}>
                          {d.metric_view_name}
                        </span>
                        <span className="text-[10px] px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-700">{d.status}</span>
                        <span className="text-[10px] text-slate-400">{d.measure_count} measures</span>
                        {d.recommended && <span className="text-[10px] text-emerald-600 dark:text-emerald-400 font-medium">recommended</span>}
                      </label>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}

          <div className="divide-y dark:divide-gray-700">
            {filtered.map(d => {
              const busy = actionLoading[d.definition_id]
              return (
                <div key={d.definition_id} className="py-3">
                  <div className="flex justify-between items-start gap-2">
                    <div className="min-w-0">
                      <span className="font-medium text-sm dark:text-gray-100">{d.metric_view_name}</span>
                      {d.version && d.version > 1 && <span className="text-purple-500 dark:text-purple-400 text-xs ml-1">v{d.version}</span>}
                      <span className="text-gray-400 dark:text-gray-500 text-xs ml-2">{d.source_table}</span>
                      <span className="text-gray-400 dark:text-gray-500 text-[10px] ml-2">
                        <select value={perMvTargets[d.definition_id] || ''} onChange={e => setPerMvTargets(prev => ({...prev, [d.definition_id]: e.target.value}))}
                          disabled={!!globalTargetOverride}
                          className="bg-transparent border border-slate-200 dark:border-gray-600 rounded px-1 py-0 text-[10px] cursor-pointer disabled:opacity-50"
                          title={globalTargetOverride ? `Overridden to ${globalTargetOverride}` : 'Deploy target for this metric view'}>
                          <option value="">{getDefaultTarget(d) || '(no default)'}</option>
                          {schemaOptions.filter(s => s !== getDefaultTarget(d)).map(s => <option key={s} value={s}>{s}</option>)}
                        </select>
                      </span>
                      {(d.applied_at || d.created_at) && (
                        <span className="text-gray-400 dark:text-gray-500 text-[10px] ml-2" title={d.applied_at ? `Applied: ${d.applied_at}` : `Created: ${d.created_at}`}>
                          {d.applied_at ? `Applied ${new Date(d.applied_at).toLocaleDateString()}` : `Created ${new Date(d.created_at).toLocaleDateString()}`}
                        </span>
                      )}
                      {d.deployed_catalog && d.deployed_schema && (
                        <span className={`text-[10px] ml-2 px-1 py-0.5 rounded ${d.deployed_exists === false ? 'bg-amber-50 dark:bg-amber-900/20 text-amber-600 dark:text-amber-400' : 'bg-emerald-50 dark:bg-emerald-900/20 text-emerald-600 dark:text-emerald-400'}`}>
                          {d.deployed_exists === false ? 'Missing from UC: ' : 'Deployed: '}{d.deployed_catalog}.{d.deployed_schema}
                        </span>
                      )}
                    </div>
                    <div className="flex items-center gap-1 flex-shrink-0">
                      {busy && (
                        <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400 animate-pulse">
                          <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg>
                          {busy === 'improve' ? 'Improving...' : busy === 'create' ? 'Creating...' : busy === 'retry' ? 'Retrying...' : busy === 'delete' ? 'Deleting...' : busy === 'drop' ? 'Dropping...' : 'Working...'}
                        </span>
                      )}
                      {statusBadge(d.status)}
                      {matBadge(d.has_materialization, d.materialization_schedule)}
                      {(d.complexity_score != null || d.quality_score != null) && (() => {
                        const combined = (Number(d.complexity_score) || 0) + (Number(d.quality_score) || 0)
                        const pct = combined / 50
                        const cls = pct >= 0.6 ? 'text-emerald-700 dark:text-emerald-400 bg-emerald-100 dark:bg-emerald-900/30'
                          : pct >= 0.3 ? 'text-amber-700 dark:text-amber-400 bg-amber-100 dark:bg-amber-900/30'
                          : 'text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30'
                        return <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${cls}`} title={`Complexity: ${d.complexity_score ?? '?'}/30, Quality: ${d.quality_score ?? '?'}/20`}>{combined}/50</span>
                      })()}
                      {d.complexity_level === 'basic' && (
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400" title={`Complexity: ${d.complexity_score ?? '?'}/30 -- few measures, no joins`}>Basic</span>
                      )}
                      {d.complexity_level === 'standard' && (
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-sky-100 text-sky-700 dark:bg-sky-900/30 dark:text-sky-400" title={`Complexity: ${d.complexity_score ?? '?'}/30 -- joins or ratios present`}>Standard</span>
                      )}
                      {d.complexity_level === 'rich' && (
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400" title={`Complexity: ${d.complexity_score ?? '?'}/30 -- multi-join, ratios, filters`}>Rich</span>
                      )}
                      {d.quality_level && d.quality_level !== 'ready' && (
                        <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${
                          d.quality_level === 'production' ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                          : 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400'
                        }`} title={`Quality: ${d.quality_score ?? '?'}/20 - ${d.quality_level === 'production' ? 'Full metadata, synonyms, format, agent-ready' : 'Missing metadata (synonyms, format, or display_name)'}`}>{d.quality_level === 'production' ? 'Production' : 'Draft'}</span>
                      )}
                      {/* Compact action menu */}
                      <div className="relative" ref={openMenuId === d.definition_id ? menuRef : undefined}>
                        <button onClick={() => setOpenMenuId(prev => prev === d.definition_id ? null : d.definition_id)}
                          className="px-1.5 py-0.5 text-xs text-slate-500 hover:text-slate-700 dark:hover:text-slate-300 cursor-pointer select-none rounded hover:bg-slate-100 dark:hover:bg-dbx-navy-500">Actions</button>
                        {openMenuId === d.definition_id && (
                        <div className="absolute right-0 mt-1 z-20 bg-white dark:bg-dbx-navy-600 border dark:border-gray-600 rounded-lg shadow-lg py-1 min-w-[140px]">
                          <button onClick={() => { loadDefinitionJson(d.definition_id); setOpenMenuId(null) }} className="w-full text-left px-3 py-1.5 text-xs hover:bg-slate-50 dark:hover:bg-dbx-navy-500">
                            {expandedDef === d.definition_id ? 'Hide JSON' : 'View JSON'}
                          </button>
                          <button onClick={() => { openEdit(d.definition_id); setOpenMenuId(null) }} className="w-full text-left px-3 py-1.5 text-xs hover:bg-slate-50 dark:hover:bg-dbx-navy-500">Edit JSON</button>
                          <button onClick={() => { analyzeMv(d.definition_id); setOpenMenuId(null) }} disabled={!!busy}
                            className="w-full text-left px-3 py-1.5 text-xs text-cyan-600 hover:bg-cyan-50 dark:hover:bg-cyan-900/20 disabled:opacity-50">
                            {busy === 'analyze' ? 'Analyzing...' : 'Analyze'}
                          </button>
                          {d.status === 'failed' && (
                            <button onClick={() => { retryDefinition(d.definition_id); setOpenMenuId(null) }} disabled={!!busy}
                              className="w-full text-left px-3 py-1.5 text-xs text-dbx-lava hover:bg-red-50 dark:hover:bg-red-900/20 disabled:opacity-50">
                              {busy === 'retry' ? 'Retrying...' : 'Retry'}
                            </button>
                          )}
                          {(d.status === 'validated' || d.status === 'applied') && (
                            <>
                              <button onClick={() => { improveDefinition(d.definition_id); setOpenMenuId(null) }} disabled={!!busy}
                                title="Re-generate this definition with AI feedback (replaces the current definition)."
                                className="w-full text-left px-3 py-1.5 text-xs text-blue-600 hover:bg-blue-50 dark:hover:bg-blue-900/20 disabled:opacity-50">
                                {busy === 'improve' ? 'Re-generating...' : 'Improve (re-generate)'}
                              </button>
                              {d.status === 'validated' && (
                                <>
                                  <button onClick={() => { addItems(d.definition_id, 'measures'); setOpenMenuId(null) }} disabled={!!busy}
                                    title="Ask AI for additional measures and merge them in (existing measures are kept and duplicates skipped)."
                                    className="w-full text-left px-3 py-1.5 text-xs text-blue-600 hover:bg-blue-50 dark:hover:bg-blue-900/20 disabled:opacity-50">
                                    {busy === 'add_measures' ? 'Adding measures...' : 'Add measures'}
                                  </button>
                                  <button onClick={() => { addItems(d.definition_id, 'dimensions'); setOpenMenuId(null) }} disabled={!!busy}
                                    title="Ask AI for additional dimensions and merge them in (existing dimensions are kept and duplicates skipped)."
                                    className="w-full text-left px-3 py-1.5 text-xs text-blue-600 hover:bg-blue-50 dark:hover:bg-blue-900/20 disabled:opacity-50">
                                    {busy === 'add_dimensions' ? 'Adding dimensions...' : 'Add dimensions'}
                                  </button>
                                  <button onClick={() => { improveDefinition(d.definition_id, 'check_filters'); setOpenMenuId(null) }} disabled={!!busy}
                                    title="Ask AI to review and set the scope filter for this view."
                                    className="w-full text-left px-3 py-1.5 text-xs text-blue-600 hover:bg-blue-50 dark:hover:bg-blue-900/20 disabled:opacity-50">
                                    {busy === 'check_filters' ? 'Checking filters...' : 'Check filters'}
                                  </button>
                                </>
                              )}
                              <button onClick={() => { createDefinition(d.definition_id); setOpenMenuId(null) }}
                                disabled={!!busy || !isValidTarget(getEffectiveTarget(d))}
                                title={!isValidTarget(getEffectiveTarget(d)) ? 'Select an output catalog.schema first (Output schema selector above)' : (d.status === 'applied' ? 'Re-run CREATE OR REPLACE VIEW in Unity Catalog' : 'Deploy this definition as a UC metric view (CREATE OR REPLACE VIEW)')}
                                className="w-full text-left px-3 py-1.5 text-xs text-green-600 hover:bg-green-50 dark:hover:bg-green-900/20 disabled:opacity-50">
                                {busy === 'create' ? 'Deploying...' : d.status === 'applied' ? 'Redeploy' : 'Deploy as UC View'}
                              </button>
                            </>
                          )}
                          {d.status === 'applied' && (
                            <>
                            <button onClick={() => { runTestQueries(d.definition_id); setOpenMenuId(null) }} disabled={!!busy}
                              title="Run auto-generated MEASURE() drill queries against the deployed view to confirm it returns sensible results."
                              className="w-full text-left px-3 py-1.5 text-xs text-teal-600 hover:bg-teal-50 dark:hover:bg-teal-900/20 disabled:opacity-50">
                              {busy === 'test' ? 'Testing...' : 'Test Queries'}
                            </button>
                            <button onClick={() => { dropDefinition(d.definition_id); setOpenMenuId(null) }} disabled={!!busy}
                              className="w-full text-left px-3 py-1.5 text-xs text-amber-600 hover:bg-amber-50 dark:hover:bg-amber-900/20 disabled:opacity-50">
                              {busy === 'drop' ? 'Dropping...' : 'Drop from UC'}
                            </button>
                            <button onClick={() => { transferOwnership(d.definition_id); setOpenMenuId(null) }} disabled={!!busy}
                              className="w-full text-left px-3 py-1.5 text-xs text-purple-600 hover:bg-purple-50 dark:hover:bg-purple-900/20 disabled:opacity-50">
                              {busy === 'transfer' ? 'Transferring...' : 'Transfer Ownership'}
                            </button>
                            </>
                          )}
                          <hr className="my-1 dark:border-gray-600" />
                          <button onClick={() => { deleteDefinition(d.definition_id, d.status); setOpenMenuId(null) }} disabled={!!busy}
                            className="w-full text-left px-3 py-1.5 text-xs text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 disabled:opacity-50">
                            {busy === 'delete' ? 'Deleting...' : 'Delete'}
                          </button>
                        </div>
                        )}
                      </div>
                    </div>
                  </div>
                  {d.validation_errors && (
                    <p className="text-xs text-red-600 dark:text-red-400 mt-1">{d.validation_errors}</p>
                  )}
                  {(d.status === 'validated' || d.status === 'applied') && (
                    <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
                      <label className="flex items-center gap-1.5 cursor-pointer select-none text-slate-600 dark:text-slate-300">
                        <input type="checkbox"
                          checked={getDeployMatEnabled(d)}
                          onChange={e => setDeployMatPrefs(prev => ({
                            ...prev,
                            [d.definition_id]: { ...prev[d.definition_id], enabled: e.target.checked },
                          }))}
                          className="accent-dbx-lava w-3.5 h-3.5" />
                        Include materialization on deploy
                      </label>
                      {getDeployMatEnabled(d) && (
                        <input type="text"
                          value={getDeployMatSchedule(d)}
                          onChange={e => setDeployMatPrefs(prev => ({
                            ...prev,
                            [d.definition_id]: { ...prev[d.definition_id], enabled: true, schedule: e.target.value },
                          }))}
                          placeholder="every 6 hours"
                          className="w-40 px-2 py-0.5 border rounded text-[10px] dark:bg-slate-700 dark:border-slate-600 dark:text-white" />
                      )}
                    </div>
                  )}
                  {createError[d.definition_id] && (
                    <div className="mt-2 p-2 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded text-xs">
                      <p className="text-red-700 dark:text-red-300 font-medium mb-1">Create failed:</p>
                      <p className="text-red-600 dark:text-red-400 whitespace-pre-wrap break-words">{createError[d.definition_id]}</p>
                      <div className="flex gap-2 mt-2">
                        <button onClick={() => getSuggestion(d.definition_id)} disabled={suggestLoading}
                          className="px-2 py-1 bg-amber-600 text-white rounded text-xs hover:bg-amber-700 disabled:opacity-50">Get suggestion</button>
                        <button onClick={() => openEdit(d.definition_id)}
                          className="px-2 py-1 bg-slate-600 text-white rounded text-xs hover:bg-slate-700">Edit definition</button>
                      </div>
                    </div>
                  )}
                  {d.genie_space_id && (
                    <p className="text-xs text-emerald-600 dark:text-emerald-400 mt-1">Genie space: {d.genie_space_id}</p>
                  )}
                  {expandedDef === d.definition_id && expandedJson && (
                    <div className="mt-2 space-y-2">
                      {d.has_materialization && (
                        <p className="text-xs text-sky-600 dark:text-sky-400">
                          Materialization schedule: {d.materialization_schedule || 'manual refresh only'}
                          {d.status === 'applied' && ' — check DESCRIBE EXTENDED on the deployed view for pipeline refresh status.'}
                        </p>
                      )}
                      {/* Toggle between raw JSON and structured view */}
                      <div className="flex items-center gap-2">
                        <label className="flex items-center gap-1 text-xs text-slate-500 cursor-pointer">
                          <input type="checkbox" checked={structuredEditing === d.definition_id}
                            onChange={e => {
                              if (e.target.checked) {
                                try { setStructuredDraft(JSON.parse(expandedJson)); setStructuredEditing(d.definition_id) }
                                catch { setStructuredDraft(null) }
                              } else { setStructuredEditing(null); setStructuredDraft(null) }
                            }} />
                          Structured editor
                        </label>
                      </div>
                      {structuredEditing === d.definition_id && structuredDraft ? (
                        <MvStructuredEditor defn={structuredDraft} setDefn={setStructuredDraft}
                          onSave={async () => {
                            try {
                              const res = await fetch(`/api/semantic-layer/definitions/${d.definition_id}`, {
                                method: 'PUT', headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ json_definition: JSON.stringify(structuredDraft) }),
                              })
                              if (res.ok) {
                                invalidateCache('/api/semantic-layer/definitions')
                                refreshDefinitions()
                                setStructuredEditing(null); setStructuredDraft(null)
                                setExpandedDef(null); setExpandedJson(null)
                              }
                            } catch (e) { setError(e.message) }
                          }}
                          onCancel={() => { setStructuredEditing(null); setStructuredDraft(null) }} />
                      ) : (
                        <pre className="bg-dbx-oat dark:bg-gray-900 border dark:border-gray-600 rounded p-3 text-xs overflow-x-auto max-h-64 dark:text-gray-200">{expandedJson}</pre>
                      )}
                    </div>
                  )}

                  {/* MV Analysis panel */}
                  {mvAnalysisExpanded === d.definition_id && mvAnalysis[d.definition_id] && (
                    <MvAnalysisPanel issues={mvAnalysis[d.definition_id]}
                      onClose={() => setMvAnalysisExpanded(null)}
                      appliedFields={mvAppliedFields[d.definition_id]}
                      busy={actionLoading[d.definition_id]}
                      onApplyFix={(path, value) => applyFieldFix(d.definition_id, path, value)}
                      onRefine={(focus) => {
                        // measures/dimensions use the cheaper incremental endpoint;
                        // check_filters edits a scalar so it stays on improve.
                        if (focus === 'add_measures') addItems(d.definition_id, 'measures')
                        else if (focus === 'add_dimensions') addItems(d.definition_id, 'dimensions')
                        else improveDefinition(d.definition_id, focus)
                      }} />
                  )}

                  {/* MV Test-query results panel */}
                  {mvTestExpanded === d.definition_id && mvTestResults[d.definition_id] && (
                    <MvTestResultsPanel data={mvTestResults[d.definition_id]}
                      busy={actionLoading[d.definition_id] === 'test'}
                      onRunFederatedFull={() => runTestQueries(d.definition_id, true)}
                      onClose={() => { stopMvTestPoll(d.definition_id); setMvTestExpanded(null) }} />
                  )}
                </div>
              )
            })}
            {filtered.length === 0 && <p className="text-xs text-slate-400 py-4 text-center">No definitions match this filter.</p>}
          </div>
        </section>
      )})()}

      </>}

      {editDefId && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onClick={() => !suggestLoading && setEditDefId(null)} role="dialog" aria-modal="true">
          <div className="bg-white dark:bg-dbx-navy-600 rounded-2xl shadow-elevated max-w-3xl w-full max-h-[90vh] flex flex-col animate-slide-up" onClick={e => e.stopPropagation()}>
            <div className="p-3 border-b dark:border-gray-700 font-medium dark:text-gray-100">Edit definition JSON</div>
            <textarea value={editJson} onChange={e => setEditJson(e.target.value)}
              className="flex-1 p-3 font-mono text-xs border-0 dark:bg-gray-900 dark:text-gray-200 resize-none min-h-[200px]"
              spellCheck={false} />
            <div className="p-3 border-t dark:border-gray-700 flex justify-end gap-2">
              <button onClick={() => setEditDefId(null)} disabled={suggestLoading}
                className="px-3 py-1.5 border dark:border-gray-600 rounded text-sm">Cancel</button>
              <button onClick={saveEdit} disabled={suggestLoading}
                className="px-3 py-1.5 bg-dbx-lava text-white rounded text-sm hover:bg-red-700 disabled:opacity-50">
                {suggestLoading ? 'Saving...' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
