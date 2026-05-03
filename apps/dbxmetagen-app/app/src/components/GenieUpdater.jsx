import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { ErrorBanner } from '../App'
import { PageHeader } from './ui'

function uuid() { return crypto.randomUUID?.() || Math.random().toString(36).slice(2) }

function descToStr(d) {
  if (!d) return ''
  if (Array.isArray(d)) return d.join(', ')
  if (typeof d === 'string') return d
  return String(d)
}

function strToDesc(v) { return v ? [v] : null }

// ---------------------------------------------------------------------------
// Shared small components
// ---------------------------------------------------------------------------

function SectionHeader({ title, count, defaultOpen = true, forceOpen, actions, children }) {
  const [open, setOpen] = useState(defaultOpen)
  useEffect(() => { if (forceOpen) setOpen(true) }, [forceOpen])
  return (
    <div className="card overflow-hidden">
      <button onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-5 py-3 bg-dbx-oat dark:bg-slate-700/50 hover:bg-dbx-oat-dark dark:hover:bg-slate-700 transition-colors">
        <span className="text-sm font-medium text-slate-700 dark:text-slate-300">
          {title}{count != null && <span className="ml-1.5 text-xs text-slate-400">({count})</span>}
        </span>
        <span className="flex items-center gap-2">
          {actions}
          <span className={`text-xs transition-transform ${open ? 'rotate-90' : ''}`}>&#9654;</span>
        </span>
      </button>
      {open && <div className="p-5 space-y-3">{children}</div>}
    </div>
  )
}

function AIAssistButton({ section, tableIds, existingItems, onResult }) {
  const [loading, setLoading] = useState(false)
  const [prompt, setPrompt] = useState('')
  const [open, setOpen] = useState(false)
  const [errorMsg, setErrorMsg] = useState(null)

  const run = async () => {
    setLoading(true); setErrorMsg(null)
    try {
      const res = await fetch('/api/genie/update-assist', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ section, table_identifiers: tableIds, existing_items: existingItems, user_prompt: prompt }),
      })
      if (!res.ok) { const b = await res.json().catch(() => ({})); setErrorMsg(b.detail || 'AI assist failed'); return }
      const data = await res.json()
      onResult(data)
      setOpen(false); setPrompt('')
    } catch (e) { setErrorMsg(e.message) }
    finally { setLoading(false) }
  }

  return (
    <div className="inline-block">
      <button onClick={() => setOpen(!open)} className="text-xs px-2.5 py-1 rounded border border-blue-300 dark:border-blue-700 text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 transition-colors">
        AI Assist
      </button>
      {open && (
        <div className="mt-2 p-3 border border-blue-200 dark:border-blue-800 rounded-lg bg-blue-50/50 dark:bg-blue-950/30 space-y-2">
          <input value={prompt} onChange={e => setPrompt(e.target.value)} placeholder="Optional: describe what to generate..."
            className="w-full text-xs border border-slate-200 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-200" />
          <div className="flex gap-2">
            <button onClick={run} disabled={loading}
              className="text-xs px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 flex items-center gap-1">
              {loading && <span className="w-3 h-3 border-2 border-white border-t-transparent rounded-full animate-spin" />}
              {loading ? 'Generating...' : 'Generate'}
            </button>
            <button onClick={() => setOpen(false)} className="text-xs px-2 py-1 text-slate-500 hover:text-slate-700">Cancel</button>
          </div>
          {errorMsg && <p className="text-xs text-red-600 dark:text-red-400">{errorMsg}</p>}
        </div>
      )}
    </div>
  )
}

function EditableList({ items, setItems, renderItem, emptyLabel, addLabel, newItem }) {
  return (
    <div className="space-y-2">
      {items.length === 0 && <p className="text-xs text-slate-400 italic">{emptyLabel}</p>}
      {items.map((item, i) => (
        <div key={item._key || i} className="flex items-start gap-2 group">
          <div className="flex-1">{renderItem(item, i, val => { const next = [...items]; next[i] = val; setItems(next) })}</div>
          <button onClick={() => setItems(items.filter((_, j) => j !== i))}
            className="mt-1 text-xs text-red-400 hover:text-red-600 opacity-0 group-hover:opacity-100 transition-opacity">Remove</button>
        </div>
      ))}
      <button onClick={() => setItems([...items, { ...newItem(), _key: uuid() }])}
        className="text-xs px-2 py-1 border border-dashed border-slate-300 dark:border-slate-600 rounded text-slate-500 hover:text-slate-700 hover:border-slate-400 transition-colors">
        + {addLabel}
      </button>
    </div>
  )
}

function SmallInput({ value, onChange, placeholder, className = '' }) {
  return <input value={value || ''} onChange={e => onChange(e.target.value)} placeholder={placeholder}
    className={`text-xs border border-slate-200 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-200 ${className}`} />
}

// ---------------------------------------------------------------------------
// SQL Test button -- calls POST /api/genie/validate-sql
// ---------------------------------------------------------------------------

function SqlTestButton({ sql }) {
  const [state, setState] = useState(null) // null | 'loading' | 'pass' | 'fail'
  const [error, setError] = useState(null)

  const test = async () => {
    if (!sql?.trim()) return
    setState('loading'); setError(null)
    try {
      const res = await fetch('/api/genie/validate-sql', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql }),
      })
      const data = await res.json()
      if (data.valid) { setState('pass') } else { setState('fail'); setError(data.error || 'Invalid SQL') }
    } catch (e) { setState('fail'); setError(e.message) }
  }

  const icon = state === 'pass' ? '\u2713' : state === 'fail' ? '\u2717' : null
  const color = state === 'pass' ? 'text-emerald-600' : state === 'fail' ? 'text-red-500' : ''

  return (
    <span className="inline-flex items-center gap-1">
      <button onClick={test} disabled={state === 'loading' || !sql?.trim()} title="Test SQL"
        className="text-xs px-1.5 py-0.5 rounded border border-slate-300 dark:border-slate-600 text-slate-500 hover:text-slate-700 hover:border-slate-400 disabled:opacity-40 transition-colors">
        {state === 'loading' ? <span className="w-3 h-3 border-2 border-slate-400 border-t-transparent rounded-full animate-spin inline-block" /> : 'Test'}
      </button>
      {icon && <span className={`text-xs font-bold ${color}`} title={error || ''}>{icon}</span>}
      {state === 'fail' && error && <span className="text-xs text-red-500 truncate max-w-xs" title={error}>{error.slice(0, 60)}</span>}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Test All SQL button -- validates multiple SQL statements in parallel
// ---------------------------------------------------------------------------

function TestAllSqlButton({ sqls }) {
  const [state, setState] = useState(null) // null | 'loading' | {pass: n, fail: n, errors: []}
  const test = async () => {
    if (!sqls?.length) return
    setState('loading')
    const results = await Promise.all(sqls.map(sql =>
      fetch('/api/genie/validate-sql', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql }),
      }).then(r => r.json()).catch(() => ({ valid: false, error: 'Request failed' }))
    ))
    const pass = results.filter(r => r.valid).length
    const fail = results.length - pass
    const errors = results.map((r, i) => r.valid ? null : `#${i + 1}: ${r.error || 'Invalid'}`).filter(Boolean)
    setState({ pass, fail, errors })
  }
  return (
    <span className="inline-flex items-center gap-1.5" onClick={e => e.stopPropagation()}>
      <button onClick={test} disabled={state === 'loading' || !sqls?.length} title="Test all SQL queries"
        className="text-xs px-2 py-0.5 rounded border border-slate-300 dark:border-slate-600 text-slate-500 hover:text-slate-700 hover:border-slate-400 disabled:opacity-40 transition-colors">
        {state === 'loading' ? <span className="w-3 h-3 border-2 border-slate-400 border-t-transparent rounded-full animate-spin inline-block" /> : `Test All (${sqls.length})`}
      </button>
      {state && state !== 'loading' && (
        <span className="text-xs">
          <span className="text-emerald-600 font-bold">{state.pass} pass</span>
          {state.fail > 0 && <span className="text-red-500 font-bold ml-1">{state.fail} fail</span>}
        </span>
      )}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Column autocomplete input for join/snippet SQL fields
// ---------------------------------------------------------------------------

function SqlInputWithAutocomplete({ value, onChange, placeholder, columnsMap, className = '' }) {
  const [suggestions, setSuggestions] = useState([])
  const [showSugs, setShowSugs] = useState(false)
  const [caretPos, setCaretPos] = useState(0)
  const inputRef = useRef(null)

  const handleChange = (e) => {
    const val = e.target.value
    const pos = e.target.selectionStart || 0
    setCaretPos(pos)
    onChange(val)

    if (!columnsMap || Object.keys(columnsMap).length === 0) { setShowSugs(false); return }

    const textBefore = val.slice(0, pos)
    const dotMatch = textBefore.match(/(\S+)\.(\w*)$/)
    if (dotMatch) {
      const prefix = dotMatch[1].toLowerCase()
      const colPrefix = dotMatch[2].toLowerCase()
      const matchingCols = []
      for (const [tbl, cols] of Object.entries(columnsMap)) {
        const shortName = tbl.split('.').pop().toLowerCase()
        if (shortName.includes(prefix) || tbl.toLowerCase().includes(prefix)) {
          for (const c of cols) {
            if (!colPrefix || c.name.toLowerCase().startsWith(colPrefix)) {
              matchingCols.push({ table: tbl, col: c.name, type: c.type })
            }
          }
        }
      }
      setSuggestions(matchingCols.slice(0, 12))
      setShowSugs(matchingCols.length > 0)
    } else {
      setShowSugs(false)
    }
  }

  const applySuggestion = (sug) => {
    const textBefore = value.slice(0, caretPos)
    const dotMatch = textBefore.match(/(\S+)\.(\w*)$/)
    if (dotMatch) {
      const replaceStart = caretPos - dotMatch[2].length
      const newVal = value.slice(0, replaceStart) + sug.col + value.slice(caretPos)
      onChange(newVal)
    }
    setShowSugs(false)
    inputRef.current?.focus()
  }

  return (
    <div className="relative">
      <input ref={inputRef} value={value || ''} onChange={handleChange} onBlur={() => setTimeout(() => setShowSugs(false), 200)}
        placeholder={placeholder}
        className={`text-xs font-mono border border-slate-200 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-200 ${className}`} />
      {showSugs && suggestions.length > 0 && (
        <div className="absolute z-20 top-full left-0 mt-1 w-80 max-h-48 overflow-y-auto border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 shadow-lg">
          {suggestions.map((s, i) => (
            <button key={i} onMouseDown={() => applySuggestion(s)}
              className="w-full text-left px-3 py-1.5 text-xs hover:bg-blue-50 dark:hover:bg-blue-900/30 flex justify-between">
              <span className="font-mono text-slate-700 dark:text-slate-300">{s.col}</span>
              <span className="text-slate-400">{s.type}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Table description helpers
// ---------------------------------------------------------------------------

function TableDescButtons({ identifier, currentDesc, onDescription }) {
  const [enriching, setEnriching] = useState(false)
  const [pulling, setPulling] = useState(false)
  const [errorMsg, setErrorMsg] = useState(null)

  const enrich = async () => {
    if (!identifier) return
    setEnriching(true); setErrorMsg(null)
    try {
      const res = await fetch('/api/genie/enrich-description', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_identifier: identifier, existing_description: currentDesc || null }),
      })
      if (!res.ok) { const b = await res.json().catch(() => ({})); setErrorMsg(b.detail || 'Enrich failed'); return }
      const data = await res.json()
      if (data.description) onDescription(data.description)
    } catch (e) { setErrorMsg(e.message) }
    finally { setEnriching(false) }
  }

  const pullUC = async () => {
    if (!identifier) return
    setPulling(true); setErrorMsg(null)
    try {
      const res = await fetch(`/api/genie/uc-comment?table_identifier=${encodeURIComponent(identifier)}`)
      if (!res.ok) { const b = await res.json().catch(() => ({})); setErrorMsg(b.detail || 'Could not fetch comment from Unity Catalog'); return }
      const data = await res.json()
      onDescription(data.comment || '')
    } catch (e) { setErrorMsg(e.message) }
    finally { setPulling(false) }
  }

  const btnClass = "text-xs px-2 py-1 rounded border transition-colors disabled:opacity-50 flex items-center gap-1 whitespace-nowrap"
  const spinner = <span className="w-3 h-3 border-2 border-current border-t-transparent rounded-full animate-spin" />

  return (
    <div className="space-y-1 mt-1">
      <div className="flex gap-1.5">
        <button onClick={enrich} disabled={enriching || !identifier}
          className={`${btnClass} border-purple-300 dark:border-purple-700 text-purple-600 dark:text-purple-400 hover:bg-purple-50 dark:hover:bg-purple-900/30`}>
          {enriching ? spinner : null}{enriching ? 'Enriching...' : 'Enrich from KB'}
        </button>
        <button onClick={pullUC} disabled={pulling || !identifier}
          className={`${btnClass} border-amber-300 dark:border-amber-700 text-amber-600 dark:text-amber-400 hover:bg-amber-50 dark:hover:bg-amber-900/30`}>
          {pulling ? spinner : null}{pulling ? 'Fetching...' : 'Pull from UC'}
        </button>
      </div>
      {errorMsg && <p className="text-xs text-red-600 dark:text-red-400">{errorMsg}</p>}
    </div>
  )
}

function TableColumns({ identifier, columns, onColumns }) {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [errorMsg, setErrorMsg] = useState(null)

  const loadFromKB = async () => {
    if (!identifier) return
    setLoading(true); setErrorMsg(null)
    try {
      const res = await fetch(`/api/genie/table-columns?table_identifier=${encodeURIComponent(identifier)}`)
      if (!res.ok) { const b = await res.json().catch(() => ({})); setErrorMsg(b.detail || 'Failed to load columns'); return }
      const data = await res.json()
      onColumns((data.columns || []).map(c => ({ name: c.column_name, type: c.data_type || '', desc: c.comment || '' })))
      setOpen(true)
    } catch (e) { setErrorMsg(e.message) }
    finally { setLoading(false) }
  }

  const updateCol = (idx, field, val) => {
    const next = [...columns]
    next[idx] = { ...next[idx], [field]: val }
    onColumns(next)
  }

  const spinner = <span className="w-3 h-3 border-2 border-current border-t-transparent rounded-full animate-spin" />
  const btnClass = "text-xs px-2 py-1 rounded border transition-colors disabled:opacity-50 flex items-center gap-1 whitespace-nowrap"

  return (
    <div className="mt-1">
      <div className="flex items-center gap-2">
        {columns.length > 0 && (
          <button onClick={() => setOpen(!open)}
            className="text-xs text-slate-500 hover:text-slate-700 dark:hover:text-slate-300 flex items-center gap-1">
            <span className={`transition-transform ${open ? 'rotate-90' : ''}`}>&#9654;</span>
            Columns ({columns.length})
          </button>
        )}
        <button onClick={loadFromKB} disabled={loading || !identifier}
          className={`${btnClass} border-teal-300 dark:border-teal-700 text-teal-600 dark:text-teal-400 hover:bg-teal-50 dark:hover:bg-teal-900/30`}>
          {loading ? spinner : null}{loading ? 'Loading...' : columns.length ? 'Refresh Columns' : 'Load Columns from KB'}
        </button>
        {errorMsg && <span className="text-xs text-red-600 dark:text-red-400">{errorMsg}</span>}
      </div>
      {open && columns.length > 0 && (
        <div className="mt-2 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
          <div className="grid grid-cols-[minmax(120px,1fr)_80px_2fr] gap-px bg-slate-200 dark:bg-slate-700 text-xs font-medium text-slate-600 dark:text-slate-400">
            <div className="bg-slate-50 dark:bg-slate-800 px-2 py-1">Column</div>
            <div className="bg-slate-50 dark:bg-slate-800 px-2 py-1">Type</div>
            <div className="bg-slate-50 dark:bg-slate-800 px-2 py-1">Description</div>
          </div>
          <div className="max-h-60 overflow-y-auto">
            {columns.map((c, idx) => (
              <div key={c.name} className="grid grid-cols-[minmax(120px,1fr)_80px_2fr] gap-px bg-slate-200 dark:bg-slate-700 text-xs">
                <div className="bg-white dark:bg-slate-800 px-2 py-1 font-mono text-slate-700 dark:text-slate-300 truncate">{c.name}</div>
                <div className="bg-white dark:bg-slate-800 px-2 py-1 text-slate-500 truncate">{c.type}</div>
                <input value={c.desc} onChange={e => updateCol(idx, 'desc', e.target.value)}
                  className="bg-white dark:bg-slate-800 px-2 py-1 text-slate-700 dark:text-slate-300 border-0 outline-none focus:bg-blue-50 dark:focus:bg-blue-900/20" />
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Health Badge component
// ---------------------------------------------------------------------------

const _healthStyles = {
  good: 'border-emerald-300 dark:border-emerald-700 text-emerald-700 dark:text-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-900/30',
  ok: 'border-amber-300 dark:border-amber-700 text-amber-700 dark:text-amber-400 hover:bg-amber-50 dark:hover:bg-amber-900/30',
  bad: 'border-red-300 dark:border-red-700 text-red-700 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/30',
}

function HealthBadge({ score, max, onClick }) {
  const pct = max > 0 ? score / max : 0
  const tier = pct >= 0.8 ? 'good' : pct >= 0.5 ? 'ok' : 'bad'
  return (
    <button onClick={onClick} title="Click for details"
      className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border transition-colors ${_healthStyles[tier]}`}>
      Health: {score}/{max}
    </button>
  )
}

function HealthDetails({ health, onClose }) {
  if (!health) return null
  const evalScore = Object.values(health.dimensions || {}).reduce((s, d) => s + (d.score ?? 0), 0)
  return (
    <div className="card p-4 space-y-2 border-l-4 border-blue-400">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-slate-700 dark:text-slate-300">Space Health: {evalScore}/{health.max}</span>
        <button onClick={onClose} className="text-xs text-slate-400 hover:text-slate-600">Close</button>
      </div>
      {Object.entries(health.dimensions || {}).map(([key, dim]) => (
        <div key={key} className="flex items-center gap-2 text-xs">
          <span className={`w-5 text-center font-bold ${dim.score == null ? 'text-slate-400' : dim.score >= dim.max ? 'text-emerald-600' : dim.score > 0 ? 'text-amber-600' : 'text-red-500'}`}>
            {dim.score == null ? '-' : dim.score}/{dim.max}
          </span>
          <span className="text-slate-600 dark:text-slate-400 capitalize">{key.replace(/_/g, ' ')}</span>
          <span className="text-slate-400 ml-auto">{dim.detail}</span>
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Question verdict panel (from semantic gap analysis)
// ---------------------------------------------------------------------------

const _verdictStyles = {
  2: 'text-emerald-700 dark:text-emerald-400 bg-emerald-100 dark:bg-emerald-900/30',
  1: 'text-amber-700 dark:text-amber-400 bg-amber-100 dark:bg-amber-900/30',
  0: 'text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30',
}

function QuestionVerdicts({ verdicts, onClose }) {
  if (!verdicts || verdicts.length === 0) return null
  const labels = { 2: 'answerable', 1: 'partial', 0: 'unanswerable' }
  return (
    <div className="card p-4 space-y-2 border-l-4 border-cyan-400">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-slate-700 dark:text-slate-300">Question Answerability ({verdicts.length})</span>
        <button onClick={onClose} className="text-xs text-slate-400 hover:text-slate-600">Close</button>
      </div>
      {verdicts.map((v, i) => (
        <div key={i} className="flex items-start gap-2 text-xs py-1.5">
          <span className={`px-1.5 py-0.5 rounded font-medium ${_verdictStyles[v.score] || _verdictStyles[0]}`}>
            {labels[v.score] || 'unknown'}
          </span>
          <div className="flex-1">
            <p className="text-slate-700 dark:text-slate-300">{v.question}</p>
            {v.reason && <p className="text-slate-400 mt-0.5">{v.reason}</p>}
          </div>
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Version History panel
// ---------------------------------------------------------------------------

function VersionHistory({ spaceId, onRestore, onClose }) {
  const [versions, setVersions] = useState([])
  const [loading, setLoading] = useState(true)
  const [selectedVer, setSelectedVer] = useState(null)
  const [selectedData, setSelectedData] = useState(null)
  const [loadingVer, setLoadingVer] = useState(false)

  useEffect(() => {
    fetch(`/api/genie/spaces/${spaceId}/versions`)
      .then(r => r.json())
      .then(d => setVersions(d.versions || []))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [spaceId])

  const loadVersion = async (ver) => {
    setSelectedVer(ver); setLoadingVer(true)
    try {
      const res = await fetch(`/api/genie/spaces/${spaceId}/versions/${ver}`)
      const data = await res.json()
      setSelectedData(data)
    } catch { setSelectedData(null) }
    finally { setLoadingVer(false) }
  }

  return (
    <div className="card p-4 space-y-3 border-l-4 border-indigo-400">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-slate-700 dark:text-slate-300">Version History</span>
        <button onClick={onClose} className="text-xs text-slate-400 hover:text-slate-600">Close</button>
      </div>
      {loading && <p className="text-xs text-slate-400">Loading...</p>}
      {!loading && versions.length === 0 && <p className="text-xs text-slate-400 italic">No version history yet. History is saved on each update.</p>}
      {versions.map(v => (
        <div key={v.version} className={`flex items-center gap-3 text-xs px-3 py-2 rounded cursor-pointer transition-colors ${selectedVer === v.version ? 'bg-indigo-50 dark:bg-indigo-900/30 border border-indigo-200 dark:border-indigo-700' : 'hover:bg-slate-50 dark:hover:bg-slate-800'}`}
          onClick={() => loadVersion(v.version)}>
          <span className="font-medium text-slate-700 dark:text-slate-300">v{v.version}</span>
          <span className="text-slate-400">{v.title}</span>
          <span className="text-slate-400 ml-auto">{v.updated_at}</span>
        </div>
      ))}
      {selectedData && (
        <div className="space-y-2 mt-2 pt-2 border-t border-slate-200 dark:border-slate-700">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-slate-600 dark:text-slate-400">Version {selectedData.version} snapshot</span>
            <button onClick={() => onRestore(selectedData.serialized_space, selectedData.title)}
              className="text-xs px-3 py-1 bg-indigo-600 text-white rounded hover:bg-indigo-700 transition-colors">
              Restore This Version
            </button>
          </div>
          <pre className="text-xs font-mono bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded p-3 overflow-auto max-h-48 text-slate-600 dark:text-slate-400">
            {JSON.stringify(selectedData.serialized_space, null, 2).slice(0, 3000)}
          </pre>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Drift detection banner
// ---------------------------------------------------------------------------

function DriftBanner({ spaceId, localSs, onPullRemote }) {
  const [drift, setDrift] = useState(null) // null | 'checking' | 'clean' | 'drifted'
  const [liveSs, setLiveSs] = useState(null)
  const [showDiff, setShowDiff] = useState(false)

  useEffect(() => {
    if (!spaceId) return
    setDrift('checking')
    fetch(`/api/genie/spaces/${spaceId}/live`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(data => {
        const live = data.serialized_space || {}
        setLiveSs(live)
        const localStr = JSON.stringify(localSs || {}, Object.keys(localSs || {}).sort())
        const liveStr = JSON.stringify(live, Object.keys(live).sort())
        setDrift(localStr === liveStr ? 'clean' : 'drifted')
      })
      .catch(() => setDrift(null))
  }, [spaceId])

  if (drift !== 'drifted') return null

  return (
    <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-300 dark:border-amber-700 rounded-lg px-4 py-3 space-y-2">
      <div className="flex items-center gap-3 text-sm text-amber-800 dark:text-amber-300">
        <span className="font-medium">This space was modified outside dbxmetagen.</span>
        <button onClick={() => { onPullRemote(liveSs); setDrift('clean') }}
          className="text-xs px-2.5 py-1 rounded border border-amber-400 text-amber-700 hover:bg-amber-100 dark:hover:bg-amber-800/30 transition-colors">
          Pull Remote Changes
        </button>
        <button onClick={() => setDrift('clean')}
          className="text-xs px-2.5 py-1 text-amber-600 hover:text-amber-800">Dismiss</button>
        <button onClick={() => setShowDiff(!showDiff)}
          className="text-xs px-2.5 py-1 text-amber-600 hover:text-amber-800">{showDiff ? 'Hide' : 'Show'} Diff</button>
      </div>
      {showDiff && liveSs && (
        <pre className="text-xs font-mono bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded p-3 overflow-auto max-h-60 text-slate-600 dark:text-slate-400">
          {JSON.stringify(liveSs, null, 2).slice(0, 4000)}
        </pre>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Suggestions panel (from /api/genie/analyze)
// ---------------------------------------------------------------------------

const _sevStyles = {
  high: 'text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30',
  medium: 'text-amber-700 dark:text-amber-400 bg-amber-100 dark:bg-amber-900/30',
  low: 'text-slate-700 dark:text-slate-400 bg-slate-100 dark:bg-slate-900/30',
}

function SuggestionsPanel({ suggestions, onClose }) {
  if (!suggestions || suggestions.length === 0) return null
  return (
    <div className="card p-4 space-y-2 border-l-4 border-violet-400">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-slate-700 dark:text-slate-300">Analysis Suggestions ({suggestions.length})</span>
        <button onClick={onClose} className="text-xs text-slate-400 hover:text-slate-600">Close</button>
      </div>
      {suggestions.map((s, i) => (
        <div key={i} className="flex items-start gap-2 text-xs py-1.5">
          <span className={`px-1.5 py-0.5 rounded font-medium uppercase ${_sevStyles[s.severity] || _sevStyles.low}`}>{s.severity}</span>
          <div>
            <p className="text-slate-700 dark:text-slate-300">{s.message}</p>
            {s.action && <p className="text-slate-400 mt-0.5">{s.action}</p>}
          </div>
          <span className="ml-auto text-slate-400 capitalize whitespace-nowrap">{s.section?.replace(/_/g, ' ')}</span>
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Dry-run validation panel
// ---------------------------------------------------------------------------

function DryRunPanel({ results, onClose }) {
  if (!results) return null
  return (
    <div className={`card p-4 space-y-2 border-l-4 ${results.passed ? 'border-emerald-400' : 'border-red-400'}`}>
      <div className="flex items-center justify-between">
        <span className={`text-sm font-medium ${results.passed ? 'text-emerald-700 dark:text-emerald-400' : 'text-red-700 dark:text-red-400'}`}>
          {results.passed ? 'All checks passed' : 'Validation issues found'}
        </span>
        <button onClick={onClose} className="text-xs text-slate-400 hover:text-slate-600">Close</button>
      </div>
      {(results.checks || []).map((check, i) => (
        <div key={i} className="text-xs space-y-0.5">
          <div className="flex items-center gap-2">
            <span className={`font-bold ${check.passed ? 'text-emerald-600' : 'text-red-500'}`}>{check.passed ? '\u2713' : '\u2717'}</span>
            <span className="font-medium text-slate-700 dark:text-slate-300 capitalize">{check.name.replace(/_/g, ' ')}</span>
          </div>
          {check.details?.map((d, j) => <p key={j} className="pl-5 text-slate-500">{d}</p>)}
        </div>
      ))}
    </div>
  )
}


// ===========================================================================
// Main GenieUpdater component
// ===========================================================================

export default function GenieUpdater({ spaceId, onBack }) {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [tracked, setTracked] = useState(false)
  const [version, setVersion] = useState(1)
  const [title, setTitle] = useState('')
  const [description, setDescription] = useState('')

  // serialized_space sections
  const [tables, setTables] = useState([])
  const [metricViews, setMetricViews] = useState([])
  const [textInstructions, setTextInstructions] = useState('')
  const [exampleSqls, setExampleSqls] = useState([])
  const [joinSpecs, setJoinSpecs] = useState([])
  const [measures, setMeasures] = useState([])
  const [filters, setFilters] = useState([])
  const [expressions, setExpressions] = useState([])
  const [sampleQuestions, setSampleQuestions] = useState([])

  const [rawJson, setRawJson] = useState(null)
  const [rawJsonEditing, setRawJsonEditing] = useState(false)
  const [rawJsonText, setRawJsonText] = useState('')
  const [deploying, setDeploying] = useState(false)
  const [deployResult, setDeployResult] = useState(null)
  const [deployError, setDeployError] = useState(null)

  const [parseInfo, setParseInfo] = useState(null)

  // Column metadata cache for autocomplete
  const [columnsMap, setColumnsMap] = useState({})

  // Health score
  const [health, setHealth] = useState(null)
  const [showHealth, setShowHealth] = useState(false)

  // Dry-run
  const [dryRunResults, setDryRunResults] = useState(null)
  const [dryRunning, setDryRunning] = useState(false)

  // Analysis
  const [analysisSuggestions, setAnalysisSuggestions] = useState(null)
  const [analyzing, setAnalyzing] = useState(false)
  const [questionVerdicts, setQuestionVerdicts] = useState(null)

  // Version history
  const [showHistory, setShowHistory] = useState(false)

  // ---------------------------------------------------------------------------
  // Parse serialized space from API
  // ---------------------------------------------------------------------------

  const loadSpaceIntoState = useCallback((ss, spaceTitle, spaceDesc) => {
    if (!ss || typeof ss !== 'object') ss = {}
    setRawJson(ss)
    setRawJsonText(JSON.stringify(ss, null, 2))
    if (spaceTitle !== undefined) setTitle(spaceTitle)
    if (spaceDesc !== undefined) setDescription(spaceDesc)

    const info = { keys: Object.keys(ss) }

    try {
      const ds = ss.data_sources || {}
      const t = (ds.tables || []).map(t => ({ identifier: t.identifier || '', description: t.description, columns: [], _key: uuid() }))
      setTables(t)
      info.tables = t.length
      const mv = (ds.metric_views || []).map(m => ({ identifier: m.identifier || '', description: m.description, _key: uuid() }))
      setMetricViews(mv)
      info.metricViews = mv.length
    } catch (e) { info.tablesErr = e.message }

    try {
      const inst = ss.instructions || {}
      const tiList = inst.text_instructions || []
      if (tiList.length) {
        const textParts = tiList.map(t => {
          const c = t.content || []
          return Array.isArray(c) ? c.join('\n') : String(c)
        }).join('\n\n')
        setTextInstructions(textParts)
        info.textInstructions = textParts.length
      } else {
        setTextInstructions(inst.text || '')
        info.textInstructions = (inst.text || '').length
      }
    } catch (e) { info.textErr = e.message }

    try {
      const inst = ss.instructions || {}
      const eq = (inst.example_question_sqls || inst.example_sql || []).map(e => ({
        question: Array.isArray(e.question) ? e.question.join('\n') : (e.question || ''),
        sql: Array.isArray(e.sql) ? e.sql.join('\n') : (e.sql || ''),
        _key: uuid(),
      }))
      setExampleSqls(eq)
      info.exampleSqls = eq.length
    } catch (e) { info.exSqlErr = e.message }

    try {
      const inst = ss.instructions || {}
      const js = (inst.join_specs || []).map(j => ({
        left: j.left?.identifier || (typeof j.left === 'string' ? j.left : ''),
        right: j.right?.identifier || (typeof j.right === 'string' ? j.right : ''),
        sql: Array.isArray(j.sql) ? j.sql.join(' AND ') : (j.sql || ''),
        _key: uuid(),
      }))
      setJoinSpecs(js)
      info.joinSpecs = js.length
    } catch (e) { info.joinErr = e.message }

    try {
      const inst = ss.instructions || {}
      const snip = inst.sql_snippets || {}
      const arrJoin = v => Array.isArray(v) ? v.join('\n') : (v || '')
      const synJoin = v => Array.isArray(v) ? v.join(', ') : (v || '')
      const m = (snip.measures || []).map(m => ({
        alias: m.alias || '', display_name: m.display_name || m.alias || '',
        sql: arrJoin(m.sql), synonyms: synJoin(m.synonyms), description: m.description || '', _key: uuid(),
      }))
      setMeasures(m)
      info.measures = m.length
      const f = (snip.filters || []).map(f => ({
        display_name: f.display_name || '', sql: arrJoin(f.sql), _key: uuid(),
      }))
      setFilters(f)
      info.filters = f.length
      const x = (snip.expressions || []).map(x => ({
        alias: x.alias || '', display_name: x.display_name || x.alias || '',
        sql: arrJoin(x.sql), synonyms: synJoin(x.synonyms), _key: uuid(),
      }))
      setExpressions(x)
      info.expressions = x.length
    } catch (e) { info.snippetErr = e.message }

    try {
      const cfg = ss.config || {}
      const sq = (cfg.sample_questions || ss.sample_questions || []).map(q => ({
        text: Array.isArray(q.question) ? q.question[0] : (typeof q === 'string' ? q : q.question || q.text || ''),
        _key: uuid(),
      }))
      setSampleQuestions(sq)
      info.sampleQuestions = sq.length
    } catch (e) { info.sqErr = e.message }

    setParseInfo(info)
  }, [])

  // ---------------------------------------------------------------------------
  // Initial load
  // ---------------------------------------------------------------------------

  useEffect(() => {
    if (!spaceId) return
    setLoading(true); setError(null); setParseInfo(null)
    fetch(`/api/genie/spaces/${spaceId}/definition`)
      .then(r => r.ok ? r.json() : r.json().then(b => Promise.reject(b.detail || `Error ${r.status}`)))
      .then(data => {
        setTracked(data.tracked)
        setVersion(data.version || 1)

        let ss = data.serialized_space
        if (typeof ss === 'string') { try { ss = JSON.parse(ss) } catch { ss = {} } }
        if (typeof ss === 'string') { try { ss = JSON.parse(ss) } catch { ss = {} } }
        if (!ss || typeof ss !== 'object') ss = {}
        loadSpaceIntoState(ss, data.title || '', data.description || '')
      })
      .catch(e => setError(typeof e === 'string' ? e : e.message || 'Failed to load'))
      .finally(() => setLoading(false))
  }, [spaceId, loadSpaceIntoState])

  // ---------------------------------------------------------------------------
  // Load column metadata for all tables (Phase 3a: column cache)
  // ---------------------------------------------------------------------------

  const tableIdKey = useMemo(() => tables.map(t => t.identifier).filter(Boolean).join(','), [tables])
  useEffect(() => {
    const ids = tableIdKey.split(',').filter(Boolean)
    if (ids.length === 0) return
    const newMap = {}
    Promise.all(ids.map(id =>
      fetch(`/api/genie/table-columns?table_identifier=${encodeURIComponent(id)}`)
        .then(r => r.ok ? r.json() : { columns: [] })
        .then(d => { newMap[id] = (d.columns || []).map(c => ({ name: c.column_name, type: c.data_type || '' })) })
        .catch(() => {})
    )).then(() => setColumnsMap(prev => ({ ...prev, ...newMap })))
  }, [tableIdKey])

  // ---------------------------------------------------------------------------
  // Compute health score on state changes
  // ---------------------------------------------------------------------------

  const assembledSpace = useMemo(() => {
    const ss = {
      data_sources: {
        tables: tables.map(t => ({ identifier: t.identifier, description: t.description })).filter(t => t.identifier),
        metric_views: metricViews.map(m => ({ identifier: m.identifier, description: m.description })).filter(m => m.identifier),
      },
      instructions: {
        text: textInstructions || undefined,
        example_sql: exampleSqls.map(e => ({ question: e.question, sql: e.sql })).filter(e => e.question && e.sql),
        join_specs: joinSpecs.map(j => ({ left: { identifier: j.left }, right: { identifier: j.right }, sql: j.sql ? j.sql.split(/\s+AND\s+/i) : [] })).filter(j => j.left.identifier && j.right.identifier),
        sql_snippets: {
          measures: measures.map(m => ({ alias: m.alias, display_name: m.display_name || m.alias, sql: m.sql ? [m.sql] : [], synonyms: m.synonyms ? m.synonyms.split(',').map(s => s.trim()).filter(Boolean) : [], description: m.description })).filter(m => m.alias && m.sql.length),
          filters: filters.map(f => ({ display_name: f.display_name, sql: f.sql ? [f.sql] : [] })).filter(f => f.display_name && f.sql.length),
          expressions: expressions.map(x => ({ alias: x.alias, display_name: x.display_name || x.alias, sql: x.sql ? [x.sql] : [], synonyms: x.synonyms ? x.synonyms.split(',').map(s => s.trim()).filter(Boolean) : [] })).filter(x => x.alias && x.sql.length),
        },
      },
      sample_questions: sampleQuestions.map(q => q.text).filter(Boolean),
      description: description || undefined,
    }
    return ss
  }, [tables, metricViews, textInstructions, exampleSqls, joinSpecs, measures, filters, expressions, sampleQuestions, description])

  const healthTimer = useRef(null)
  useEffect(() => {
    if (healthTimer.current) clearTimeout(healthTimer.current)
    healthTimer.current = setTimeout(() => {
      fetch('/api/genie/health-check', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ serialized_space: assembledSpace }),
      }).then(r => r.ok ? r.json() : null).then(d => { if (d) setHealth(d) }).catch(() => {})
    }, 800)
    return () => { if (healthTimer.current) clearTimeout(healthTimer.current) }
  }, [assembledSpace])

  const tableIds = tables.map(t => t.identifier).filter(Boolean)

  // ---------------------------------------------------------------------------
  // Assemble for deploy (with column descriptions inlined)
  // ---------------------------------------------------------------------------

  const assemble = () => {
    const ss = { ...assembledSpace }
    ss.data_sources = {
      ...ss.data_sources,
      tables: tables.map(t => {
        const entry = { identifier: t.identifier, description: t.description }
        const cols = (t.columns || []).filter(c => c.desc)
        if (cols.length) {
          const colLines = cols.map(c => `${c.name} (${c.type}): ${c.desc}`)
          const descArr = Array.isArray(entry.description) ? [...entry.description] : entry.description ? [entry.description] : []
          descArr.push('Column descriptions: ' + colLines.join('; '))
          entry.description = descArr
        }
        return entry
      }).filter(t => t.identifier),
    }
    return ss
  }

  // ---------------------------------------------------------------------------
  // Dry-run
  // ---------------------------------------------------------------------------

  const runDryRun = async () => {
    setDryRunning(true); setDryRunResults(null)
    try {
      const res = await fetch('/api/genie/dry-run', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ serialized_space: assemble(), table_identifiers: tableIds }),
      })
      const data = await res.json()
      setDryRunResults(data)
    } catch (e) { setDryRunResults({ passed: false, checks: [{ name: 'request', passed: false, details: [e.message] }] }) }
    finally { setDryRunning(false) }
  }

  // ---------------------------------------------------------------------------
  // Analyze
  // ---------------------------------------------------------------------------

  const runAnalysis = async () => {
    setAnalyzing(true); setAnalysisSuggestions(null); setQuestionVerdicts(null)
    try {
      const res = await fetch('/api/genie/analyze', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ serialized_space: assemble(), table_identifiers: tableIds }),
      })
      const data = await res.json()
      setHealth(data.health)
      setAnalysisSuggestions(data.suggestions || [])
      if (data.question_verdicts?.length) setQuestionVerdicts(data.question_verdicts)
    } catch (e) { setAnalysisSuggestions([{ section: 'error', severity: 'high', message: e.message }]) }
    finally { setAnalyzing(false) }
  }

  // ---------------------------------------------------------------------------
  // Deploy
  // ---------------------------------------------------------------------------

  const [conflictWarning, setConflictWarning] = useState(null)

  const deploy = async (asNew) => {
    if (!title.trim()) { setDeployError('Enter a title'); return }
    setDeploying(true); setDeployError(null); setDeployResult(null); setConflictWarning(null)
    try {
      // Check for external modifications before updating (not needed for new spaces)
      if (!asNew && spaceId) {
        try {
          const liveRes = await fetch(`/api/genie/spaces/${spaceId}/live`)
          if (liveRes.ok) {
            const liveData = await liveRes.json()
            const liveSs = liveData.serialized_space || {}
            const localStr = JSON.stringify(rawJson || {}, Object.keys(rawJson || {}).sort())
            const liveStr = JSON.stringify(liveSs, Object.keys(liveSs).sort())
            if (localStr !== liveStr && !conflictWarning) {
              setConflictWarning('This space was modified externally since you loaded it. Click Update again to overwrite.')
              setDeploying(false)
              return
            }
          }
        } catch { /* live check failed, proceed anyway */ }
      }

      const payload = rawJsonEditing ? JSON.parse(rawJsonText) : assemble()
      const body = { title: title.trim(), description: description || undefined, serialized_space: payload }
      if (!asNew) body.space_id = spaceId
      const res = await fetch('/api/genie/create', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      if (!res.ok) { const b = await res.json().catch(() => ({})); setDeployError(b.detail || `Error ${res.status}`); return }
      const result = await res.json()
      setDeployResult(result)
      setConflictWarning(null)
      if (result.updated) setVersion(prev => prev + 1)
    } catch (e) { setDeployError(e.message) }
    finally { setDeploying(false) }
  }

  // ---------------------------------------------------------------------------
  // Restore from version
  // ---------------------------------------------------------------------------

  const restoreVersion = (ss, vTitle) => {
    loadSpaceIntoState(ss, vTitle)
    setShowHistory(false)
  }

  // Pull remote changes (drift)
  const pullRemote = (liveSs) => {
    loadSpaceIntoState(liveSs)
  }

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  if (loading) return (
    <div className="space-y-6">
      <PageHeader title="Loading Space..." />
      <div className="flex items-center gap-2 text-sm text-slate-500">
        <span className="w-4 h-4 border-2 border-slate-400 border-t-transparent rounded-full animate-spin" />
        Fetching definition...
      </div>
    </div>
  )

  return (
    <div className="space-y-4">
      <PageHeader title="Edit Genie Space"
        subtitle={`Space ID: ${spaceId}`}
        badge={tracked ? 'Tracked' : 'Untracked'}
        actions={
          <div className="flex items-center gap-2">
            {health && <HealthBadge score={health.score} max={health.max} dimensions={health.dimensions} onClick={() => setShowHealth(!showHealth)} />}
            <button onClick={() => setShowHistory(!showHistory)}
              className="text-xs px-3 py-1.5 rounded border border-indigo-300 dark:border-indigo-700 text-indigo-600 dark:text-indigo-400 hover:bg-indigo-50 dark:hover:bg-indigo-900/30 transition-colors">
              History
            </button>
            <button onClick={runAnalysis} disabled={analyzing}
              className="text-xs px-3 py-1.5 rounded border border-violet-300 dark:border-violet-700 text-violet-600 dark:text-violet-400 hover:bg-violet-50 dark:hover:bg-violet-900/30 transition-colors disabled:opacity-50 flex items-center gap-1">
              {analyzing && <span className="w-3 h-3 border-2 border-violet-400 border-t-transparent rounded-full animate-spin" />}
              {analyzing ? 'Analyzing...' : 'Analyze Space'}
            </button>
            <button onClick={onBack} className="text-xs px-3 py-1.5 rounded border border-slate-300 dark:border-slate-600 text-slate-600 dark:text-slate-300 hover:bg-dbx-oat dark:hover:bg-slate-700">Back to Builder</button>
          </div>
        }
      />
      <ErrorBanner error={error} />

      {/* Drift detection */}
      <DriftBanner spaceId={spaceId} localSs={rawJson} onPullRemote={pullRemote} />

      {/* Health details */}
      {showHealth && <HealthDetails health={health} onClose={() => setShowHealth(false)} />}

      {/* Analysis suggestions */}
      {analysisSuggestions && <SuggestionsPanel suggestions={analysisSuggestions} onClose={() => setAnalysisSuggestions(null)} />}

      {/* Question answerability verdicts (from semantic gap analysis) */}
      {questionVerdicts && <QuestionVerdicts verdicts={questionVerdicts} onClose={() => setQuestionVerdicts(null)} />}

      {/* Version history */}
      {showHistory && <VersionHistory spaceId={spaceId} onRestore={restoreVersion} onClose={() => setShowHistory(false)} />}

      {/* Dry-run results */}
      {dryRunResults && <DryRunPanel results={dryRunResults} onClose={() => setDryRunResults(null)} />}

      {/* Space summary */}
      {parseInfo && (
        <div className="card p-3 text-xs">
          <div className="flex flex-wrap gap-3 text-slate-600 dark:text-slate-400">
            <span>Tables: <b>{parseInfo.tables ?? '-'}</b></span>
            <span>Metric views: <b>{parseInfo.metricViews ?? '-'}</b></span>
            <span>Example SQL: <b>{parseInfo.exampleSqls ?? '-'}</b></span>
            <span>Joins: <b>{parseInfo.joinSpecs ?? '-'}</b></span>
            <span>Snippets: <b>{(parseInfo.measures ?? 0) + (parseInfo.filters ?? 0) + (parseInfo.expressions ?? 0)}</b></span>
            <span>Sample Qs: <b>{parseInfo.sampleQuestions ?? '-'}</b></span>
          </div>
          {Object.entries(parseInfo).filter(([k]) => k.endsWith('Err')).length > 0 && (
            <p className="text-amber-600 dark:text-amber-400 mt-1">Some sections could not be parsed from the Genie API response. You can still edit and deploy.</p>
          )}
        </div>
      )}

      {/* Title / Description */}
      <div className="card p-5 space-y-3">
        <div>
          <label className="block text-xs font-medium text-slate-600 dark:text-slate-400 mb-1">Title</label>
          <input value={title} onChange={e => setTitle(e.target.value)}
            className="w-full border border-slate-200 dark:border-slate-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-200" />
        </div>
        <div>
          <label className="block text-xs font-medium text-slate-600 dark:text-slate-400 mb-1">Description</label>
          <textarea value={description} onChange={e => setDescription(e.target.value)} rows={2}
            className="w-full border border-slate-200 dark:border-slate-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-200" />
        </div>
        <div className="flex gap-4 text-xs text-slate-400">
          <span>Version: {version}</span>
          <span>{tracked ? 'Tracked in Delta' : 'Fetched from Databricks API'}</span>
        </div>
      </div>

      {/* Raw JSON (Phase 6: editable toggle) */}
      <SectionHeader title="Raw Space JSON" count={null} defaultOpen={false} forceOpen={rawJsonEditing}
        actions={
          <label className="flex items-center gap-1.5 text-xs text-slate-500 cursor-pointer" onClick={e => e.stopPropagation()}>
            <input type="checkbox" checked={rawJsonEditing} onChange={e => { setRawJsonEditing(e.target.checked); if (e.target.checked) setRawJsonText(JSON.stringify(assemble(), null, 2)) }}
              className="rounded border-slate-300" />
            Edit raw JSON
          </label>
        }>
        {rawJsonEditing ? (
          <div className="space-y-2">
            <p className="text-xs text-amber-600 dark:text-amber-400">Raw JSON mode: deploy will use this JSON directly instead of structured fields above.</p>
            <textarea value={rawJsonText} onChange={e => setRawJsonText(e.target.value)} rows={20}
              className="w-full font-mono text-xs border border-slate-200 dark:border-slate-600 rounded-lg p-4 bg-white dark:bg-slate-900 text-slate-700 dark:text-slate-300" />
          </div>
        ) : rawJson ? (
          <pre className="text-xs font-mono bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg p-4 overflow-auto max-h-96 text-slate-700 dark:text-slate-300 whitespace-pre-wrap break-words">
            {JSON.stringify(rawJson, null, 2)}
          </pre>
        ) : <p className="text-xs text-slate-400 italic">No data loaded</p>}
      </SectionHeader>

      {/* Data Sources: Tables */}
      <SectionHeader title="Tables" count={tables.length}>
        <EditableList items={tables} setItems={setTables}
          emptyLabel="No tables" addLabel="Add Table"
          newItem={() => ({ identifier: '', description: null, columns: [] })}
          renderItem={(t, i, update) => (
            <div className="space-y-1.5">
              <SmallInput value={t.identifier} onChange={v => update({ ...t, identifier: v })} placeholder="catalog.schema.table" className="w-full" />
              <textarea value={descToStr(t.description)} onChange={e => update({ ...t, description: strToDesc(e.target.value) })}
                placeholder="Table description (optional)" rows={3}
                className="w-full text-xs border border-slate-200 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-200" />
              <TableDescButtons identifier={t.identifier} currentDesc={descToStr(t.description)}
                onDescription={desc => update({ ...t, description: strToDesc(desc) })} />
              <TableColumns identifier={t.identifier} columns={t.columns || []}
                onColumns={cols => update({ ...t, columns: cols })} />
            </div>
          )}
        />
      </SectionHeader>

      {/* Data Sources: Metric Views */}
      <SectionHeader title="Metric Views" count={metricViews.length}>
        <EditableList items={metricViews} setItems={setMetricViews}
          emptyLabel="No metric views" addLabel="Add Metric View"
          newItem={() => ({ identifier: '', description: null })}
          renderItem={(m, i, update) => (
            <div className="flex gap-2">
              <SmallInput value={m.identifier} onChange={v => update({ ...m, identifier: v })} placeholder="catalog.schema.metric_view" className="flex-1" />
              <SmallInput value={descToStr(m.description)} onChange={v => update({ ...m, description: strToDesc(v) })} placeholder="Description (optional)" className="flex-1" />
            </div>
          )}
        />
      </SectionHeader>

      {/* Text Instructions */}
      <SectionHeader title="Text Instructions" count={textInstructions ? 1 : 0}>
        <textarea value={textInstructions} onChange={e => setTextInstructions(e.target.value)} rows={6}
          placeholder="Markdown instructions describing data, relationships, business rules..."
          className="w-full font-mono text-xs border border-slate-200 dark:border-slate-600 rounded-md p-3 bg-white dark:bg-slate-900 text-slate-800 dark:text-slate-200" />
        <AIAssistButton section="instructions" tableIds={tableIds} existingItems={textInstructions ? { text: textInstructions } : null}
          onResult={data => { if (data.text_instructions) setTextInstructions(prev => prev ? prev + '\n\n' + data.text_instructions : data.text_instructions) }} />
      </SectionHeader>

      {/* Example SQL */}
      <SectionHeader title="Example SQL" count={exampleSqls.length}
        actions={exampleSqls.length > 0 && <TestAllSqlButton sqls={exampleSqls.map(e => e.sql).filter(Boolean)} />}>
        <EditableList items={exampleSqls} setItems={setExampleSqls}
          emptyLabel="No example SQL pairs" addLabel="Add Example"
          newItem={() => ({ question: '', sql: '' })}
          renderItem={(e, i, update) => (
            <div className="space-y-1">
              <SmallInput value={e.question} onChange={v => update({ ...e, question: v })} placeholder="Question" className="w-full" />
              <div className="flex items-start gap-2">
                <textarea value={e.sql} onChange={ev => update({ ...e, sql: ev.target.value })} rows={2} placeholder="SELECT ..."
                  className="flex-1 font-mono text-xs border border-slate-200 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-200" />
                <SqlTestButton sql={e.sql} />
              </div>
            </div>
          )}
        />
        <AIAssistButton section="example_sql" tableIds={tableIds} existingItems={exampleSqls.map(e => ({ question: e.question, sql: e.sql }))}
          onResult={data => {
            const newItems = (data.example_question_sqls || data.example_sql || []).map(e => ({
              question: Array.isArray(e.question) ? e.question[0] : e.question,
              sql: Array.isArray(e.sql) ? e.sql[0] : e.sql,
              _key: uuid(),
            }))
            setExampleSqls(prev => [...prev, ...newItems])
          }} />
      </SectionHeader>

      {/* Join Specs */}
      <SectionHeader title="Join Specs" count={joinSpecs.length}>
        <EditableList items={joinSpecs} setItems={setJoinSpecs}
          emptyLabel="No join specs" addLabel="Add Join"
          newItem={() => ({ left: '', right: '', sql: '' })}
          renderItem={(j, i, update) => (
            <div className="grid grid-cols-3 gap-2">
              <SmallInput value={j.left} onChange={v => update({ ...j, left: v })} placeholder="Left table" />
              <SmallInput value={j.right} onChange={v => update({ ...j, right: v })} placeholder="Right table" />
              <div className="flex items-center gap-1">
                <SqlInputWithAutocomplete value={j.sql} onChange={v => update({ ...j, sql: v })} placeholder="left.col = right.col" columnsMap={columnsMap} className="flex-1 w-full" />
                <SqlTestButton sql={j.left && j.right && j.sql ? `SELECT 1 FROM ${j.left} JOIN ${j.right} ON ${j.sql} LIMIT 0` : ''} />
              </div>
            </div>
          )}
        />
        <AIAssistButton section="joins" tableIds={tableIds} existingItems={joinSpecs.map(j => ({ left: j.left, right: j.right, sql: j.sql }))}
          onResult={data => {
            const newItems = (Array.isArray(data) ? data : data.join_specs || []).map(j => ({
              left: j.left?.identifier || j.left || '',
              right: j.right?.identifier || j.right || '',
              sql: Array.isArray(j.sql) ? j.sql.join(' AND ') : (j.sql || ''),
              _key: uuid(),
            }))
            setJoinSpecs(prev => [...prev, ...newItems])
          }} />
      </SectionHeader>

      {/* SQL Snippets: Measures */}
      <SectionHeader title="Measures" count={measures.length}>
        <EditableList items={measures} setItems={setMeasures}
          emptyLabel="No measures" addLabel="Add Measure"
          newItem={() => ({ alias: '', display_name: '', sql: '', synonyms: '', description: '' })}
          renderItem={(m, i, update) => (
            <div className="space-y-1">
              <div className="grid grid-cols-2 gap-2">
                <SmallInput value={m.alias} onChange={v => update({ ...m, alias: v })} placeholder="Alias" />
                <SmallInput value={m.display_name} onChange={v => update({ ...m, display_name: v })} placeholder="Display name" />
              </div>
              <SqlInputWithAutocomplete value={m.sql} onChange={v => update({ ...m, sql: v })} placeholder="SQL expression (fragment)" columnsMap={columnsMap} className="w-full" />
              <div className="grid grid-cols-2 gap-2">
                <SmallInput value={m.synonyms} onChange={v => update({ ...m, synonyms: v })} placeholder="Synonyms (comma-separated)" className="w-full" />
                <SmallInput value={m.description} onChange={v => update({ ...m, description: v })} placeholder="Description" className="w-full" />
              </div>
            </div>
          )}
        />
        <AIAssistButton section="measures" tableIds={tableIds} existingItems={measures.map(m => ({ alias: m.alias, sql: m.sql }))}
          onResult={data => {
            const newItems = (data.measures || []).map(m => ({
              alias: m.alias || '', display_name: m.display_name || m.alias || '',
              sql: Array.isArray(m.sql) ? m.sql[0] : (m.sql || ''),
              synonyms: (m.synonyms || []).join(', '), description: m.description || '', _key: uuid(),
            }))
            setMeasures(prev => [...prev, ...newItems])
          }} />
      </SectionHeader>

      {/* SQL Snippets: Filters */}
      <SectionHeader title="Filters" count={filters.length}>
        <EditableList items={filters} setItems={setFilters}
          emptyLabel="No filters" addLabel="Add Filter"
          newItem={() => ({ display_name: '', sql: '' })}
          renderItem={(f, i, update) => (
            <div className="grid grid-cols-2 gap-2">
              <SmallInput value={f.display_name} onChange={v => update({ ...f, display_name: v })} placeholder="Display name" />
              <div className="flex items-center gap-1">
                <SqlInputWithAutocomplete value={f.sql} onChange={v => update({ ...f, sql: v })} placeholder="SQL WHERE clause" columnsMap={columnsMap} className="flex-1 w-full" />
              </div>
            </div>
          )}
        />
        <AIAssistButton section="filters" tableIds={tableIds} existingItems={filters.map(f => ({ display_name: f.display_name, sql: f.sql }))}
          onResult={data => {
            const newItems = (data.filters || []).map(f => ({
              display_name: f.display_name || '', sql: Array.isArray(f.sql) ? f.sql[0] : (f.sql || ''), _key: uuid(),
            }))
            setFilters(prev => [...prev, ...newItems])
          }} />
      </SectionHeader>

      {/* SQL Snippets: Expressions */}
      <SectionHeader title="Expressions" count={expressions.length}>
        <EditableList items={expressions} setItems={setExpressions}
          emptyLabel="No expressions" addLabel="Add Expression"
          newItem={() => ({ alias: '', display_name: '', sql: '', synonyms: '' })}
          renderItem={(x, i, update) => (
            <div className="space-y-1">
              <div className="grid grid-cols-2 gap-2">
                <SmallInput value={x.alias} onChange={v => update({ ...x, alias: v })} placeholder="Alias" />
                <SmallInput value={x.display_name} onChange={v => update({ ...x, display_name: v })} placeholder="Display name" />
              </div>
              <div className="flex items-center gap-1">
                <SqlInputWithAutocomplete value={x.sql} onChange={v => update({ ...x, sql: v })} placeholder="SQL expression" columnsMap={columnsMap} className="flex-1 w-full" />
              </div>
              <SmallInput value={x.synonyms} onChange={v => update({ ...x, synonyms: v })} placeholder="Synonyms (comma-separated)" className="w-full" />
            </div>
          )}
        />
        <AIAssistButton section="expressions" tableIds={tableIds} existingItems={expressions.map(x => ({ alias: x.alias, sql: x.sql }))}
          onResult={data => {
            const newItems = (data.expressions || []).map(x => ({
              alias: x.alias || '', display_name: x.display_name || x.alias || '',
              sql: Array.isArray(x.sql) ? x.sql[0] : (x.sql || ''),
              synonyms: (x.synonyms || []).join(', '), _key: uuid(),
            }))
            setExpressions(prev => [...prev, ...newItems])
          }} />
      </SectionHeader>

      {/* Sample Questions */}
      <SectionHeader title="Sample Questions" count={sampleQuestions.length}>
        <EditableList items={sampleQuestions} setItems={setSampleQuestions}
          emptyLabel="No sample questions" addLabel="Add Question"
          newItem={() => ({ text: '' })}
          renderItem={(q, i, update) => (
            <SmallInput value={q.text} onChange={v => update({ ...q, text: v })} placeholder="Business question..." className="w-full" />
          )}
        />
        <AIAssistButton section="questions" tableIds={tableIds} existingItems={sampleQuestions.map(q => q.text).filter(Boolean)}
          onResult={data => {
            const newQs = (data.sample_questions || []).map(q => ({ text: typeof q === 'string' ? q : (q.question || ''), _key: uuid() }))
            setSampleQuestions(prev => [...prev, ...newQs])
          }} />
      </SectionHeader>

      {/* Deploy Bar */}
      <div className="card p-5 space-y-3">
        <div className="flex items-center gap-3">
          <button onClick={runDryRun} disabled={dryRunning}
            className="px-4 py-2.5 border border-blue-300 dark:border-blue-700 text-blue-600 dark:text-blue-400 text-sm font-medium rounded-lg hover:bg-blue-50 dark:hover:bg-blue-900/30 disabled:opacity-50 transition-colors flex items-center gap-1.5">
            {dryRunning && <span className="w-3 h-3 border-2 border-blue-400 border-t-transparent rounded-full animate-spin" />}
            {dryRunning ? 'Validating...' : 'Validate'}
          </button>
          <button onClick={() => deploy(false)} disabled={deploying || !title.trim()}
            className="px-5 py-2.5 bg-emerald-600 text-white text-sm font-medium rounded-lg hover:bg-emerald-700 disabled:opacity-50 transition-colors flex items-center gap-1.5">
            {deploying && <span className="w-3 h-3 border-2 border-white border-t-transparent rounded-full animate-spin" />}
            Update Space
          </button>
          <button onClick={() => deploy(true)} disabled={deploying || !title.trim()}
            className="px-5 py-2.5 bg-dbx-navy text-white text-sm font-medium rounded-lg hover:bg-slate-700 disabled:opacity-50 transition-colors">
            Create as New Space
          </button>
          {rawJsonEditing && <span className="text-xs text-amber-600">Deploying from raw JSON</span>}
        </div>
        {conflictWarning && (
          <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-300 dark:border-amber-700 rounded-lg px-4 py-2.5 text-sm text-amber-700 dark:text-amber-300">
            {conflictWarning}
          </div>
        )}
        {deployError && (
          <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg px-4 py-2.5 text-sm text-red-700 dark:text-red-300">
            {deployError}
          </div>
        )}
        {deployResult && (
          <div className="bg-emerald-50 dark:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-700 rounded-lg px-4 py-2.5 text-sm text-emerald-700 dark:text-emerald-300">
            {deployResult.updated ? 'Space updated' : 'New space created'}! ID: <span className="font-mono">{deployResult.space_id}</span>
          </div>
        )}
      </div>
    </div>
  )
}
