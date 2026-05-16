import React, { useState, useEffect, useRef } from 'react'
import { ErrorBanner } from '../App'
import { cachedFetch, cachedFetchObj, invalidateCache, TTL } from '../apiCache'
import { PageHeader, EmptyState, Skeleton, Section } from './ui'
import { useCatalogSchemaTables } from '../hooks/useCatalogSchemaTables'

const STAGES = {
  starting: 'Starting...',
  building_context: 'Building metadata context...',
  planning: 'Planning generation strategy...',
  generating: 'Generating metric view definitions...',
  calling_ai: 'Generating metric views with AI...',
  checking_coverage: 'Checking question coverage...',
  validating: 'Validating expressions...',
  done: 'Complete',
}

// ---------------------------------------------------------------------------
// MV Analysis Panel
// ---------------------------------------------------------------------------

const _issueSevStyles = {
  high: 'text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30',
  medium: 'text-amber-700 dark:text-amber-400 bg-amber-100 dark:bg-amber-900/30',
  low: 'text-slate-700 dark:text-slate-400 bg-slate-100 dark:bg-slate-900/30',
}

function MvAnalysisPanel({ issues, onClose, onApplyFix, appliedFields }) {
  const applied = appliedFields || new Set()
  if (!issues || issues.length === 0) return (
    <div className="mt-2 p-3 bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-200 dark:border-emerald-700 rounded text-xs text-emerald-700 dark:text-emerald-400">
      No issues found.
      <button onClick={onClose} className="ml-2 text-slate-400 hover:text-slate-600">Close</button>
    </div>
  )
  const fixable = issues.filter(iss => iss.field && iss.fix_value && !applied.has(iss.field))
  return (
    <div className="mt-2 p-3 border border-cyan-200 dark:border-cyan-700 rounded space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium text-slate-700 dark:text-slate-300">Analysis Issues ({issues.length})</span>
        <div className="flex items-center gap-2">
          {fixable.length > 0 && onApplyFix && (
            <button onClick={() => onApplyFix('__all__')}
              className="px-2 py-0.5 text-[10px] bg-cyan-600 text-white rounded hover:bg-cyan-700">
              Apply All ({fixable.length})
            </button>
          )}
          <button onClick={onClose} className="text-xs text-slate-400 hover:text-slate-600">Close</button>
        </div>
      </div>
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
                    <button onClick={() => onApplyFix(iss.field, iss.fix_value)}
                      className="shrink-0 px-1.5 py-0.5 text-[10px] bg-cyan-600 text-white rounded hover:bg-cyan-700">Apply</button>
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

export default function SemanticLayer() {
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

  // MV health + analysis per definition
  const [mvHealth, setMvHealth] = useState({})
  const [mvAnalysis, setMvAnalysis] = useState({})
  const [mvAnalysisExpanded, setMvAnalysisExpanded] = useState(null)
  const [mvAppliedFields, setMvAppliedFields] = useState({})
  const [structuredEditing, setStructuredEditing] = useState(null)
  const [structuredDraft, setStructuredDraft] = useState(null)
  const userEditedTargetRef = useRef(false)
  const [perMvTargets, setPerMvTargets] = useState({})
  const [createError, setCreateError] = useState({})
  const [editDefId, setEditDefId] = useState(null)
  const [editJson, setEditJson] = useState('')
  const [suggestLoading, setSuggestLoading] = useState(false)
  const [suggestQLoading, setSuggestQLoading] = useState(false)
  const [bulkCreating, setBulkCreating] = useState(false)
  const [bulkDeleting, setBulkDeleting] = useState(null)
  const [vectorSyncing, setVectorSyncing] = useState(false)
  const [chatMessages, setChatMessages] = useState([])
  const [chatInput, setChatInput] = useState('')
  const [chatLoading, setChatLoading] = useState(false)
  const [chatSessionId] = useState(() => crypto.randomUUID())
  const chatEndRef = useRef(null)
  const [kpiCoverage, setKpiCoverage] = useState(null)
  const [tableSaveStatus, setTableSaveStatus] = useState(null)
  const [openMenuId, setOpenMenuId] = useState(null)
  const menuRef = useRef(null)

  // KPI Library
  const [kpis, setKpis] = useState([])
  const [showKpiForm, setShowKpiForm] = useState(false)
  const [kpiDraft, setKpiDraft] = useState({ name: '', description: '', formula: '', domain: '' })
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

  const getDefaultTarget = (d) => {
    if (d.deployed_catalog && d.deployed_schema) return `${d.deployed_catalog}.${d.deployed_schema}`
    const parts = d.source_table?.split('.') || []
    return parts.length >= 3 ? `${parts[0]}.${parts[1]}` : ''
  }
  const getEffectiveTarget = (d) => {
    if (globalTargetOverride) return globalTargetOverride
    return perMvTargets[d.definition_id] || getDefaultTarget(d)
  }
  const schemaOptions = [...new Set([
    ...definitions.map(d => getDefaultTarget(d)),
    ...(globalTargetOverride ? [globalTargetOverride] : []),
  ].filter(Boolean))]

  // Auto-set "Create target" to most common catalog.schema from selected tables
  useEffect(() => {
    if (userEditedTargetRef.current || !selectedTables.length) return
    const counts = {}
    for (const t of selectedTables) {
      const parts = t.split('.')
      if (parts.length >= 3) {
        const key = `${parts[0]}.${parts[1]}`
        counts[key] = (counts[key] || 0) + 1
      }
    }
    const best = Object.entries(counts).sort((a, b) => b[1] - a[1])[0]
    if (best) setGlobalTargetOverride(best[0])
  }, [selectedTables])

  // --- Init ---
  useEffect(() => {
    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data: cfg }) => {
      if (cfg) {
        setSelectedCatalog(cfg.catalog_name || '')
        setSelectedSchema(cfg.schema_name || '')
      }
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
          if (data.status === 'done') { refreshDefinitions(); loadProjects(); setActiveTab('definitions') }
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
    const covUrl = selectedProjectId
      ? `/api/semantic-layer/kpi-coverage?project_id=${selectedProjectId}`
      : '/api/semantic-layer/kpi-coverage'
    cachedFetchObj(covUrl, {}, TTL.CONFIG).then(({ data }) => {
      if (data?.kpi_coverage?.total) setKpiCoverage(data.kpi_coverage)
      else setKpiCoverage(null)
    })
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

  const sendChatMessage = async () => {
    const q = chatInput.trim()
    if (!q || chatLoading) return
    setChatInput('')
    setChatMessages(prev => [...prev, { role: 'user', content: q }])
    setChatLoading(true)
    try {
      const history = chatMessages.map(m => ({ role: m.role, content: m.content }))
      const res = await fetch('/api/metric-view-agent/stream', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: q, history, session_id: chatSessionId }),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        setChatMessages(prev => [...prev, { role: 'assistant', content: err.detail || 'Request failed' }])
        setChatLoading(false)
        return
      }
      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buf = ''
      let assistantContent = ''
      let hasPlaceholder = false
      const ensurePlaceholder = () => {
        if (!hasPlaceholder) {
          setChatMessages(prev => [...prev, { role: 'assistant', content: '' }])
          hasPlaceholder = true
        }
      }
      const updateLast = (msg) => setChatMessages(prev => {
        const next = [...prev]
        next[next.length - 1] = msg
        return next
      })
      ensurePlaceholder()
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buf += decoder.decode(value, { stream: true })
        const lines = buf.split('\n')
        buf = lines.pop() || ''
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const evt = JSON.parse(line.slice(6))
              if (evt.event === 'stage') {
                updateLast({ role: 'assistant', content: evt.stage === 'processing' ? 'Thinking...' : 'Searching metric views...' })
              } else if (evt.event === 'done') {
                assistantContent = evt.result?.answer || 'No answer returned'
                updateLast({ role: 'assistant', content: assistantContent, data: { tool_calls: evt.result?.tool_calls } })
              } else if (evt.event === 'error') {
                updateLast({ role: 'assistant', content: evt.error || 'An error occurred' })
              }
            } catch {}
          }
        }
      }
    } catch (e) {
      setChatMessages(prev => {
        if (prev.length && prev[prev.length - 1].role === 'assistant' && !prev[prev.length - 1].content) {
          const next = [...prev]
          next[next.length - 1] = { role: 'assistant', content: `Error: ${e.message}` }
          return next
        }
        return [...prev, { role: 'assistant', content: `Error: ${e.message}` }]
      })
    }
    setChatLoading(false)
    setTimeout(() => chatEndRef.current?.scrollIntoView({ behavior: 'smooth' }), 100)
  }

  const suggestQuestions = async () => {
    if (!selectedTables.length) { setError('Select tables first'); return }
    setSuggestQLoading(true); setError(null)
    try {
      const fqTables = selectedTables.map(t => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`)
      const res = await fetch('/api/genie/generate-questions', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_identifiers: fqTables, count: 6, purpose: 'metric_views', business_context: businessContext || undefined }),
      })
      const data = await res.json()
      if (!res.ok) { setError(data.detail || 'Failed to suggest questions'); setSuggestQLoading(false); return }
      const newQs = (data.questions || []).join('\n')
      setQuestionsText(prev => prev ? prev + '\n' + newQs : newQs)
    } catch (e) { setError(e.message) }
    setSuggestQLoading(false)
  }

  // --- KPIs ---
  const loadKpis = async () => {
    const { data } = await cachedFetch('/api/kpis', {}, TTL.CONFIG)
    setKpis(data || [])
  }
  const saveKpi = async () => {
    try {
      const body = { ...kpiDraft, target_tables: selectedTables, profile_id: activeProfileId || undefined }
      const res = kpiEditId
        ? await fetch(`/api/kpis/${kpiEditId}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
        : await fetch('/api/kpis', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      if (!res.ok) { const d = await res.json().catch(() => ({})); setError(d.detail || 'Save KPI failed'); return }
      setKpiDraft({ name: '', description: '', formula: '', domain: '' }); setKpiEditId(null); setShowKpiForm(false)
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
  const suggestKpis = async () => {
    if (!selectedTables.length) return
    setKpiSuggesting(true)
    try {
      const fqTables = selectedTables.map(t => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`)
      const qLines = questionsText.split('\n').filter(l => l.trim())
      const res = await fetch('/api/kpis/suggest', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_identifiers: fqTables, business_context: businessContext || undefined, questions: qLines.length ? qLines : undefined, profile_id: activeProfileId || undefined }) })
      const j = await res.json()
      for (const k of (j.kpis || [])) {
        await fetch('/api/kpis', { method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...k, target_tables: fqTables, source: 'suggested', profile_id: activeProfileId || undefined }) })
      }
      loadKpis()
    } catch (e) { setError(e.message) }
    setKpiSuggesting(false)
  }

  // --- Generate ---
  const startGeneration = async (mode = 'replace') => {
    const lines = questionsText.split('\n').filter(l => l.trim())
    if (!selectedTables.length || !lines.length) return
    setLoading(true); setError(null); setTaskId(null); setTaskStatus(null)
    try {
      const fqTables = selectedTables.map(t => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`)
      const body = {
        tables: fqTables, questions: lines, mode,
        catalog_name: selectedCatalog, schema_name: selectedSchema,
        business_context: businessContext || undefined,
        profile_id: activeProfileId || undefined,
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

  const improveDefinition = async (defId) => {
    setActionLoading(prev => ({ ...prev, [defId]: 'improve' }))
    setError(null)
    try {
      const analysisIssues = mvAnalysis[defId] || null
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/improve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ analysis_issues: analysisIssues }),
      })
      const data = await res.json()
      if (!res.ok) setError(data.detail || 'Improve failed')
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
      }
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  const applyFieldFix = async (defId, pathOrAll, value) => {
    if (pathOrAll === '__all__') {
      const issues = mvAnalysis[defId] || []
      const existing = mvAppliedFields[defId] || new Set()
      const fixable = issues.filter(iss => iss.field && iss.fix_value && !existing.has(iss.field))
      for (const iss of fixable) {
        try {
          const res = await fetch(`/api/semantic-layer/definitions/${defId}/field`, {
            method: 'PUT', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path: iss.field, value: iss.fix_value }),
          })
          if (res.ok) {
            setMvAppliedFields(prev => {
              const s = new Set(prev[defId] || [])
              s.add(iss.field)
              return { ...prev, [defId]: s }
            })
          }
        } catch {}
      }
      invalidateCache('/api/semantic-layer/definitions')
      refreshDefinitions()
      fetchMvHealth(defId)
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
      }
    } catch (e) { setError(e.message) }
  }

  const createDefinition = async (defId) => {
    const d = definitions.find(x => x.definition_id === defId)
    const target = getEffectiveTarget(d || {})
    const [tCat, tSch] = target.includes('.') ? target.split('.') : ['', '']
    if (!tCat || !tSch) {
      setError('No target schema resolved for this metric view')
      return
    }
    setActionLoading(prev => ({ ...prev, [defId]: 'create' }))
    setError(null)
    setCreateError(prev => ({ ...prev, [defId]: null }))
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/create`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target_catalog: tCat, target_schema: tSch }),
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
    const anyMissing = validated.some(d => !getEffectiveTarget(d))
    if (anyMissing) { setError('Some metric views have no target schema resolved'); return }
    setBulkCreating(true)
    setError(null)
    for (const d of validated) setActionLoading(prev => ({ ...prev, [d.definition_id]: 'create' }))
    const results = await Promise.allSettled(validated.map(async (d) => {
      const res = await fetch(`/api/semantic-layer/definitions/${d.definition_id}/create`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target_catalog: getEffectiveTarget(d).split('.')[0], target_schema: getEffectiveTarget(d).split('.')[1] }),
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
        {[['setup', 'Setup'], ['questions', 'Questions & KPIs'], ['generate', 'Generate'], ['definitions', 'Definitions'], ['agent', 'Agent']].map(([k, l]) => {
          const count = k === 'setup' && selectedTables.length ? `${selectedTables.length} tables`
            : k === 'questions' ? [questionLines.length && `${questionLines.length}q`, kpis.length && `${kpis.length} KPIs`].filter(Boolean).join(', ') || ''
            : k === 'definitions' && definitions.length ? `${definitions.length}`
            : k === 'agent' && chatMessages.length ? `${chatMessages.length}` : ''
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
        <span className={activeTab === 'generate' ? 'font-semibold text-dbx-lava' : ''}>3. Generate</span> &mdash; AI creates metric view definitions &rarr;{' '}
        <span className={activeTab === 'definitions' ? 'font-semibold text-dbx-lava' : ''}>4. Definitions</span> &mdash; review, validate, and deploy as UC views &rarr;{' '}
        <span className={activeTab === 'agent' ? 'font-semibold text-dbx-lava' : ''}>5. Agent</span> &mdash; ask business questions answered by your metric views
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
        <label className={label}>Business Context <span className="text-gray-400 font-normal">(optional -- describe your industry, strategic priorities, or key terminology to steer all generation)</span></label>
        <textarea value={businessContext} onChange={e => setBusinessContext(e.target.value)}
          placeholder={"e.g. We are a B2B SaaS company focused on enterprise sales. Key metrics: ARR, net revenue retention, pipeline velocity. Our fiscal year starts in February."}
          className={`${input} h-20 mb-4`} />
        <label className={label}>Business Questions (one per line)</label>
        <textarea value={questionsText} onChange={e => setQuestionsText(e.target.value)}
          placeholder={"What was total revenue by region last quarter?\nHow many orders per month by product category?\nWhat is the average deal size by sales rep?"}
          className={`${input} h-32 mb-3`} />
        <div className="flex gap-3 items-center">
          <button onClick={suggestQuestions} disabled={suggestQLoading || !selectedTables.length}
            className="px-4 py-2 bg-amber-600 text-white rounded-md text-sm hover:bg-amber-700 disabled:opacity-50">
            {suggestQLoading ? 'Suggesting...' : 'Suggest Questions'}
          </button>
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
      </section>

      {/* KPI Library */}
      <section className={section}>
        <div className="flex items-center justify-between mb-3">
          <div>
            <h2 className="text-lg font-semibold dark:text-gray-100">KPI Library</h2>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Define or auto-suggest business KPIs from your selected tables. KPIs feed into metric view generation and Genie space configuration.</p>
          </div>
          <div className="flex gap-2">
            <button onClick={suggestKpis} disabled={kpiSuggesting || !selectedTables.length}
              className="px-3 py-1.5 bg-teal-600 text-white rounded text-xs hover:bg-teal-700 disabled:opacity-50">
              {kpiSuggesting ? 'Suggesting...' : 'Auto-Suggest KPIs'}
            </button>
            <button onClick={() => { setShowKpiForm(true); setKpiEditId(null); setKpiDraft({ name: '', description: '', formula: '', domain: '' }) }}
              className="px-3 py-1.5 bg-dbx-blue text-white rounded text-xs hover:bg-blue-700">+ Add KPI</button>
            {kpis.length > 0 && (
              <button onClick={deleteAllKpis}
                className="px-3 py-1.5 bg-red-600 text-white rounded text-xs hover:bg-red-700">Delete All</button>
            )}
          </div>
        </div>
        
        {showKpiForm && (
          <div className="border border-slate-200 dark:border-slate-700 rounded-lg p-4 mb-3 space-y-2 bg-slate-50 dark:bg-slate-800/50">
            <input value={kpiDraft.name} onChange={e => setKpiDraft(d => ({ ...d, name: e.target.value }))}
              placeholder="KPI Name (e.g. Monthly Revenue Growth)" className="input-base w-full" />
            <textarea value={kpiDraft.description} onChange={e => setKpiDraft(d => ({ ...d, description: e.target.value }))}
              placeholder="Business description" rows={2} className="input-base w-full" />
            <input value={kpiDraft.formula} onChange={e => setKpiDraft(d => ({ ...d, formula: e.target.value }))}
              placeholder="SQL formula (e.g. SUM(orders.total_amount))" className="input-base w-full" />
            <div className="flex gap-2">
              <input value={kpiDraft.domain} onChange={e => setKpiDraft(d => ({ ...d, domain: e.target.value }))}
                placeholder="Domain (e.g. sales)" className="input-base flex-1" />
              <button onClick={saveKpi} disabled={!kpiDraft.name.trim()} className={btnPrimary}>{kpiEditId ? 'Update' : 'Save'}</button>
              <button onClick={() => setShowKpiForm(false)} className="px-3 py-1.5 bg-slate-200 dark:bg-slate-700 rounded text-xs">Cancel</button>
            </div>
          </div>
        )}
        {kpis.length > 0 && (() => {
          const KpiRow = ({ k, dimmed }) => (
            <div className={`flex items-start justify-between gap-3 border rounded-lg px-3 py-2 text-sm ${dimmed ? 'border-slate-200/60 dark:border-slate-700/50 opacity-60' : 'border-slate-200 dark:border-slate-700'}`}>
              <div className="flex-1 min-w-0">
                <span className="font-medium dark:text-gray-200">{k.name}</span>
                {k.domain && <span className="ml-2 text-xs px-1.5 py-0.5 rounded bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300">{k.domain}</span>}
                {k.description && <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5 truncate">{k.description}</p>}
                {k.formula && <code className="text-xs text-gray-400 dark:text-gray-500 block mt-0.5 truncate">{k.formula}</code>}
              </div>
              <div className="flex gap-1 shrink-0">
                <button onClick={() => { setKpiEditId(k.kpi_id); setKpiDraft({ name: k.name, description: k.description || '', formula: k.formula || '', domain: k.domain || '' }); setShowKpiForm(true) }}
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
          if (!profiles.length) return (
            <div>
              <p className="text-xs text-slate-400 dark:text-slate-500 mb-2">Create a Question Profile above to organize KPIs.</p>
              <div className="space-y-1.5">{kpis.map(k => <KpiRow key={k.kpi_id} k={k} />)}</div>
            </div>
          )
          const assigned = new Set()
          const sections = profiles.map(p => {
            const matched = kpis.filter(k => k.profile_id === p.profile_id)
            matched.forEach(k => assigned.add(k.kpi_id))
            return { key: p.profile_id, label: p.profile_name, kpis: matched }
          }).filter(s => s.kpis.length > 0)
          const unassigned = kpis.filter(k => !assigned.has(k.kpi_id))
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
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
          Uses AI to analyze your questions against the catalog metadata for the selected tables and generate metric view definitions.
        </p>
        <div className="flex gap-3 flex-wrap">
          <button onClick={() => startGeneration('replace')}
            disabled={loading || isGenerating || !selectedTables.length || !questionLines.length}
            title="Create new metric view definitions (replaces any pending ones)"
            className={btnPrimary}>
            {isGenerating ? 'Generating...' : 'Generate'}
          </button>
          <button onClick={() => startGeneration('additive')}
            disabled={loading || isGenerating || !selectedTables.length || !questionLines.length}
            title="Add more definitions without replacing existing ones"
            className="px-4 py-2 bg-teal-600 text-white rounded-md text-sm hover:bg-teal-700 disabled:opacity-50">
            Generate More
          </button>
          {selectedProjectId && (
            <button onClick={() => { if (!confirm('Regenerate all definitions? This will replace existing ones.')) return; startGeneration('replace_all') }}
              disabled={loading || isGenerating || !selectedTables.length || !questionLines.length}
              title="Delete all existing definitions in this project and regenerate from scratch"
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
            {taskStatus.generated && (
              <span className="text-xs text-gray-500 dark:text-gray-400">({taskStatus.generated} definitions)</span>
            )}
          </div>
          {taskStatus.status === 'error' && taskStatus.error && (
            <p className="text-sm text-red-600 dark:text-red-400 mt-2">{taskStatus.error}</p>
          )}
          {taskStatus.status === 'done' && taskStatus.result && (
            <div className="text-sm mt-2 text-gray-700 dark:text-gray-300">
              Generated: {taskStatus.result.generated}, Validated: {taskStatus.result.validated}, Failed: {taskStatus.result.failed}
              <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">
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
        <p>Each definition below is a metric view. The lifecycle is: <strong>Generated</strong> &rarr; <strong>Validated</strong> (SQL checked) &rarr; <strong>Applied</strong> (created as a UC view). Use <strong>Improve</strong> to re-generate a definition with AI feedback.</p>
        <p className="text-amber-700 dark:text-amber-400">Note: Only the owner of a metric view can edit it. Views created by this app are owned by the app service principal. Use <strong>Transfer Ownership</strong> to take ownership &mdash; this is irreversible for the app.</p>
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
          <p className="text-sm text-slate-400 text-center py-6">No metric view definitions yet. Generate some from the Generate tab.</p>
        </section>
      ) : (() => {
        const filtered = definitions.filter(d => {
          if (defStatusFilter !== 'all' && d.status !== defStatusFilter) return false
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
          <div className="flex items-center gap-3 mb-4 p-3 bg-dbx-oat dark:bg-gray-900 rounded-md border dark:border-gray-700">
            <span className="text-xs font-medium text-gray-600 dark:text-gray-400 whitespace-nowrap" title="Override deploys all metric views to this schema instead of each view's source table schema">Deploy target override:</span>
            <select value={globalTargetOverride} onChange={e => { userEditedTargetRef.current = true; setGlobalTargetOverride(e.target.value) }}
              className="input-base !text-xs w-64">
              <option value="">(None - use each view's source schema)</option>
              {schemaOptions.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            {definitions.filter(d => d.status === 'validated').length > 0 && (
              <button onClick={createAllValidated} disabled={bulkCreating}
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
                className="px-2.5 py-1 text-xs text-blue-600 dark:text-blue-400 border border-blue-300 dark:border-blue-700 rounded hover:bg-blue-50 dark:hover:bg-blue-900/20 disabled:opacity-50 whitespace-nowrap">
                {vectorSyncing ? 'Syncing...' : 'Sync to Vector Store'}
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
            {(defFilter || defStatusFilter !== 'all') && <span className="text-xs text-slate-400">{filtered.length} of {definitions.length}</span>}
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
                        <span className="text-[10px] ml-2 px-1 py-0.5 rounded bg-emerald-50 dark:bg-emerald-900/20 text-emerald-600 dark:text-emerald-400">
                          Deployed: {d.deployed_catalog}.{d.deployed_schema}
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
                      {mvHealth[d.definition_id] && (() => {
                        const h = mvHealth[d.definition_id]
                        const pct = h.max > 0 ? h.score / h.max : 0
                        const cls = pct >= 0.8 ? 'text-emerald-700 dark:text-emerald-400 bg-emerald-100 dark:bg-emerald-900/30'
                          : pct >= 0.5 ? 'text-amber-700 dark:text-amber-400 bg-amber-100 dark:bg-amber-900/30'
                          : 'text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30'
                        return <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${cls}`}>{h.score}/{h.max}</span>
                      })()}
                      {d.complexity_level === 'basic' && (
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400" title="No joins -- single-table only">Basic</span>
                      )}
                      {d.complexity_level === 'standard' && (
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-sky-100 text-sky-700 dark:bg-sky-900/30 dark:text-sky-400" title="Single join">Standard</span>
                      )}
                      {d.complexity_level === 'rich' && (
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400" title="Multi-join star/hierarchical schema">Rich</span>
                      )}
                      {d.quality_level && d.quality_level !== 'ready' && (
                        <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${
                          d.quality_level === 'production' ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                          : 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400'
                        }`} title={`Quality: ${d.quality_score ?? '?'}/7 - ${d.quality_level === 'production' ? 'Full metadata, synonyms, format' : 'Missing metadata (synonyms, format, or display_name)'}`}>{d.quality_level === 'production' ? 'Production' : 'Draft'}</span>
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
                                className="w-full text-left px-3 py-1.5 text-xs text-blue-600 hover:bg-blue-50 dark:hover:bg-blue-900/20 disabled:opacity-50">
                                {busy === 'improve' ? 'Improving...' : 'Improve'}
                              </button>
                              <button onClick={() => { createDefinition(d.definition_id); setOpenMenuId(null) }} disabled={!!busy}
                                className="w-full text-left px-3 py-1.5 text-xs text-green-600 hover:bg-green-50 dark:hover:bg-green-900/20 disabled:opacity-50">
                                {busy === 'create' ? 'Creating...' : d.status === 'applied' ? 'Re-apply' : 'Create in UC'}
                              </button>
                            </>
                          )}
                          {d.status === 'applied' && (
                            <>
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
                      onApplyFix={(path, value) => applyFieldFix(d.definition_id, path, value)} />
                  )}
                </div>
              )
            })}
            {filtered.length === 0 && <p className="text-xs text-slate-400 py-4 text-center">No definitions match this filter.</p>}
          </div>
        </section>
      )})()}

      </>}

      {/* === Agent Tab === */}
      {activeTab === 'agent' && <>
        <section className={section}>
          <h2 className="text-lg font-semibold mb-2 dark:text-gray-100">Metric View Agent</h2>
          <p className="text-xs text-slate-500 dark:text-slate-400 mb-3">Ask business questions and get answers from your deployed metric views. The agent searches, queries, and interprets metric view data.</p>

          <div className="border dark:border-gray-700 rounded-xl overflow-hidden flex flex-col" style={{ height: '500px' }}>
            <div className="flex-1 overflow-y-auto p-4 space-y-3 bg-slate-50 dark:bg-gray-900">
              {chatMessages.length === 0 && (
                <div className="text-center text-slate-400 dark:text-slate-500 py-12">
                  <p className="text-sm mb-2">No messages yet</p>
                  <p className="text-xs">Try: "What is total revenue by region?" or "Show me top KPIs for clinical trials"</p>
                </div>
              )}
              {chatMessages.map((msg, i) => (
                <div key={i} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                  <div className={`max-w-[80%] rounded-xl px-3 py-2 text-sm ${
                    msg.role === 'user'
                      ? 'bg-dbx-lava text-white'
                      : 'bg-white dark:bg-gray-800 border dark:border-gray-700 text-slate-700 dark:text-slate-200'
                  }`}>
                    <div className="whitespace-pre-wrap break-words">{msg.content || (chatLoading && i === chatMessages.length - 1 ? 'Thinking...' : '')}</div>
                    {msg.data?.sql && (
                      <details className="mt-2 text-xs">
                        <summary className="cursor-pointer text-slate-400 hover:text-slate-600">SQL Query</summary>
                        <pre className="mt-1 p-2 bg-slate-100 dark:bg-gray-900 rounded text-[10px] overflow-x-auto">{msg.data.sql}</pre>
                      </details>
                    )}
                    {msg.data?.results && (
                      <details className="mt-1 text-xs">
                        <summary className="cursor-pointer text-slate-400 hover:text-slate-600">Results ({msg.data.results.length} rows)</summary>
                        <pre className="mt-1 p-2 bg-slate-100 dark:bg-gray-900 rounded text-[10px] overflow-x-auto max-h-[200px]">{JSON.stringify(msg.data.results.slice(0, 20), null, 2)}</pre>
                      </details>
                    )}
                  </div>
                </div>
              ))}
              <div ref={chatEndRef} />
            </div>
            <div className="border-t dark:border-gray-700 p-3 bg-white dark:bg-gray-800 flex gap-2">
              <input
                value={chatInput}
                onChange={e => setChatInput(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && !e.shiftKey && sendChatMessage()}
                placeholder="Ask a business question..."
                className="flex-1 px-3 py-2 text-sm border dark:border-gray-600 rounded-lg bg-white dark:bg-gray-900 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-dbx-lava"
                disabled={chatLoading}
              />
              <button onClick={sendChatMessage} disabled={chatLoading || !chatInput.trim()}
                className="px-4 py-2 bg-dbx-lava text-white rounded-lg text-sm hover:bg-red-700 disabled:opacity-50 shrink-0">
                {chatLoading ? 'Sending...' : 'Send'}
              </button>
            </div>
          </div>

          {chatMessages.length > 0 && (
            <button onClick={() => setChatMessages([])} className="mt-2 text-xs text-slate-400 hover:text-slate-600">Clear conversation</button>
          )}
        </section>
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
