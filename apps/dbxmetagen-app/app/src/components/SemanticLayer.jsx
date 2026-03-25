import React, { useState, useEffect, useRef } from 'react'
import { ErrorBanner } from '../App'
import { cachedFetch, cachedFetchObj, TTL } from '../apiCache'
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

  // Table selection (shared cascade hook)
  const cst = useCatalogSchemaTables()
  const { catalogs, schemas, filtered: filteredTables, catalog: selectedCatalog, schema: selectedSchema, filter: tableFilter, setCatalog: setSelectedCatalog, setSchema: setSelectedSchema, setFilter: setTableFilter } = cst
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
  const [createTarget, setCreateTarget] = useState({ catalog: '', schema: '' })
  const [createError, setCreateError] = useState({})
  const [editDefId, setEditDefId] = useState(null)
  const [editJson, setEditJson] = useState('')
  const [suggestLoading, setSuggestLoading] = useState(false)
  const [suggestQLoading, setSuggestQLoading] = useState(false)

  // KPI Library
  const [kpis, setKpis] = useState([])
  const [showKpiForm, setShowKpiForm] = useState(false)
  const [kpiDraft, setKpiDraft] = useState({ name: '', description: '', formula: '', domain: '' })
  const [kpiEditId, setKpiEditId] = useState(null)
  const [kpiSuggesting, setKpiSuggesting] = useState(false)
  const [showOtherKpis, setShowOtherKpis] = useState(false)

  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [activeTab, setActiveTab] = useState('setup')
  const [defFilter, setDefFilter] = useState('')
  const [defStatusFilter, setDefStatusFilter] = useState('all')

  // --- Init ---
  useEffect(() => {
    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data: cfg }) => {
      if (cfg) {
        setSelectedCatalog(cfg.catalog_name || '')
        setSelectedSchema(cfg.schema_name || '')
        setCreateTarget({ catalog: cfg.catalog_name || '', schema: cfg.schema_name || '' })
      }
    })
    loadProjects()
    loadProfiles()
    refreshDefinitions()
    loadKpis()
  }, [])

  // Refresh definitions and pre-populate tables when project changes
  const skipNextSaveRef = useRef(true)
  useEffect(() => {
    refreshDefinitions()
    if (selectedProjectId) {
      const proj = projects.find(p => p.project_id === selectedProjectId)
      if (proj?.selected_tables) {
        try {
          const tbls = JSON.parse(proj.selected_tables)
          if (Array.isArray(tbls) && tbls.length) {
            skipNextSaveRef.current = true
            setSelectedTables(tbls)
          }
        } catch { /* ignore */ }
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
          if (data.status === 'done') { refreshDefinitions(); loadProjects() }
        }
      } catch { clearInterval(pollRef.current) }
    }, 2000)
    return () => clearInterval(pollRef.current)
  }, [taskId])

  // Auto-save table selection to the project whenever it changes (debounced 500ms)
  const saveTimerRef = useRef(null)
  useEffect(() => {
    if (!selectedProjectId) return
    if (skipNextSaveRef.current) { skipNextSaveRef.current = false; return }
    clearTimeout(saveTimerRef.current)
    saveTimerRef.current = setTimeout(() => {
      fetch(`/api/semantic-layer/projects/${selectedProjectId}/tables`, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ selected_tables: selectedTables }),
      }).catch(() => {})
    }, 500)
    return () => clearTimeout(saveTimerRef.current)
  }, [selectedTables, selectedProjectId])

  const loadProjects = () => {
    cachedFetch('/api/semantic-layer/projects', {}, TTL.CONFIG).then(({ data }) => {
      if (data) setProjects(data)
    })
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
    try {
      const tbls = JSON.parse(p.table_patterns || '[]')
      if (tbls.length) { skipNextSaveRef.current = true; setSelectedTables(tbls) }
    } catch { /* ignore */ }
  }

  const saveProfile = async () => {
    const lines = questionsText.split('\n').filter(l => l.trim())
    if (!profileName.trim() || !lines.length) return
    setLoading(true); setError(null)
    try {
      const fqTables = selectedTables.map(t => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`)
      const res = await fetch('/api/semantic-layer/profiles', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ profile_name: profileName, questions: lines, table_patterns: fqTables, business_context: businessContext || undefined }),
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
      const body = { ...kpiDraft, target_tables: selectedTables }
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
        body: JSON.stringify({ table_identifiers: fqTables, business_context: businessContext || undefined, questions: qLines.length ? qLines : undefined }) })
      const j = await res.json()
      for (const k of (j.kpis || [])) {
        await fetch('/api/kpis', { method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...k, target_tables: fqTables, source: 'suggested' }) })
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
    if (expandedDef === defId) { setExpandedDef(null); setExpandedJson(null); return }
    const { data, error: err } = await cachedFetchObj(`/api/semantic-layer/definitions/${defId}/json`, {}, TTL.CONFIG)
    if (err) { setError(err); return }
    setExpandedDef(defId)
    try { setExpandedJson(JSON.stringify(JSON.parse(data.json_definition), null, 2)) }
    catch { setExpandedJson(data.json_definition) }
  }

  const retryDefinition = async (defId) => {
    setActionLoading(prev => ({ ...prev, [defId]: 'retry' }))
    setError(null)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/retry`, { method: 'POST' })
      const data = await res.json()
      if (!res.ok) setError(data.detail || 'Retry failed')
      refreshDefinitions()
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  const improveDefinition = async (defId) => {
    setActionLoading(prev => ({ ...prev, [defId]: 'improve' }))
    setError(null)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/improve`, { method: 'POST' })
      const data = await res.json()
      if (!res.ok) setError(data.detail || 'Improve failed')
      refreshDefinitions()
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  const createDefinition = async (defId) => {
    if (!createTarget.catalog || !createTarget.schema) {
      setError('Set a target catalog and schema before creating')
      return
    }
    setActionLoading(prev => ({ ...prev, [defId]: 'create' }))
    setError(null)
    setCreateError(prev => ({ ...prev, [defId]: null }))
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/create`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target_catalog: createTarget.catalog, target_schema: createTarget.schema }),
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
      refreshDefinitions()
    } catch (e) { setError(e.message); setCreateError(prev => ({ ...prev, [defId]: e.message })) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
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
      setEditJson(data.suggested_json || '')
      setEditDefId(defId)
    } catch (e) { setError(e.message) }
    setSuggestLoading(false)
  }

  const openEdit = (defId) => {
    setEditDefId(defId)
    fetch(`/api/semantic-layer/definitions/${defId}/json`)
      .then(r => r.json())
      .then(data => setEditJson(data.json_definition || ''))
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
      if (status === 'applied' && createTarget.catalog && createTarget.schema) {
        url += `?drop_view=true&catalog=${encodeURIComponent(createTarget.catalog)}&schema=${encodeURIComponent(createTarget.schema)}`
      }
      const res = await fetch(url, { method: 'DELETE' })
      if (!res.ok) { const data = await res.json(); setError(data.detail || 'Delete failed') }
      refreshDefinitions()
    } catch (e) { setError(e.message) }
    setActionLoading(prev => ({ ...prev, [defId]: null }))
  }

  const dropDefinition = async (defId) => {
    if (!confirm('Drop this view from Unity Catalog?')) return
    if (!createTarget.catalog || !createTarget.schema) {
      setError('Set a target catalog and schema before dropping')
      return
    }
    setActionLoading(prev => ({ ...prev, [defId]: 'drop' }))
    setError(null)
    try {
      const res = await fetch(`/api/semantic-layer/definitions/${defId}/drop`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target_catalog: createTarget.catalog, target_schema: createTarget.schema }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) setError(data.detail || 'Drop failed')
      refreshDefinitions()
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
        <PageHeader title="Metric Views" subtitle="Define semantic layer definitions" />
        <button onClick={async () => {
          const res = await fetch(`/api/semantic-layer/export-sql?catalog=${encodeURIComponent(createTarget.catalog)}&schema=${encodeURIComponent(createTarget.schema)}`)
          if (!res.ok) { setError((await res.json()).detail || 'Export failed'); return }
          const blob = await res.blob()
          const url = URL.createObjectURL(blob)
          const a = document.createElement('a'); a.href = url; a.download = 'metric_views.sql'; a.click()
          URL.revokeObjectURL(url)
        }} className="px-4 py-2 text-sm rounded-md border border-slate-300 dark:border-dbx-navy-400 text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-dbx-navy-500 transition-colors whitespace-nowrap">
          Export SQL
        </button>
      </div>
      <ErrorBanner error={error} />

      {/* Unified catalog/schema display */}
      {(selectedCatalog || selectedSchema) && (
        <div className="text-xs text-slate-500 dark:text-slate-400 px-1">
          Scope: <span className="font-medium text-slate-700 dark:text-slate-200">{selectedCatalog || '?'}.{selectedSchema || '?'}</span>
          {createTarget.catalog && createTarget.catalog !== selectedCatalog && (
            <span className="ml-3">Target: <span className="font-medium text-slate-700 dark:text-slate-200">{createTarget.catalog}.{createTarget.schema}</span></span>
          )}
        </div>
      )}

      {/* Tab Bar */}
      <div className="inline-flex bg-dbx-oat/60 dark:bg-dbx-navy-600 rounded-xl p-1 shadow-inner-soft">
        {[['setup', 'Setup'], ['questions', 'Questions & KPIs'], ['generate', 'Generate'], ['definitions', 'Definitions']].map(([k, l]) => {
          const count = k === 'setup' && selectedTables.length ? `${selectedTables.length} tables`
            : k === 'questions' ? [questionLines.length && `${questionLines.length}q`, kpis.length && `${kpis.length} KPIs`].filter(Boolean).join(', ') || ''
            : k === 'definitions' && definitions.length ? `${definitions.length}` : ''
          return (
            <button key={k} onClick={() => setActiveTab(k)}
              className={`px-3.5 py-1.5 text-sm rounded-lg transition-all duration-200 ${activeTab === k ? 'bg-white dark:bg-dbx-navy-500 shadow-sm font-semibold text-dbx-lava' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`}>
              {l}{count && <span className="ml-1 text-[10px] text-slate-400">({count})</span>}
            </button>
          )
        })}
      </div>

      {/* === Setup Tab === */}
      {activeTab === 'setup' && <>

      {/* Project Selector */}
      <section className={section}>
        <div className="flex items-center gap-4">
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
        {selectedProjectId && (() => {
          const proj = projects.find(p => p.project_id === selectedProjectId)
          if (!proj?.selected_tables) return null
          try {
            const tbls = JSON.parse(proj.selected_tables)
            if (!tbls.length) return null
            return (
              <div className="mt-2 text-xs text-gray-500 dark:text-gray-400">
                <span className="font-medium">Tables:</span> {tbls.join(', ')}
              </div>
            )
          } catch { return null }
        })()}
      </section>

      {/* Table Selection */}
      <section className={section}>
        <h2 className="text-lg font-semibold mb-3 dark:text-gray-100">Select Tables</h2>
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
        {selectedCatalog && selectedSchema && allTables.length === 0 && (
          <p className="text-sm text-gray-500 dark:text-gray-400 italic">No managed tables found in {selectedCatalog}.{selectedSchema}</p>
        )}
        {selectedTables.length > 0 && (
          <div className="mt-4">
            <div className="flex items-center gap-2 mb-2">
              <span className="text-xs font-medium text-slate-600 dark:text-slate-300">Selected Tables ({selectedTables.length})</span>
              <button onClick={() => setSelectedTables([])} className="text-xs text-red-500 hover:underline">Clear all</button>
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
        <h2 className="text-lg font-semibold mb-3 dark:text-gray-100">Question Profile</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div>
            <label className={label}>Load Profile</label>
            <select value={activeProfileId} onChange={e => selectProfile(e.target.value)} className={input}>
              <option value="">-- new profile --</option>
              {profiles.map(p => (
                <option key={p.profile_id} value={p.profile_id}>
                  {p.profile_name} ({JSON.parse(p.questions || '[]').length}q)
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
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">KPIs are generated based on the selected tables and their discovered ontological entities (domains, relationships, entity types).</p>
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
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-3">
          Define or auto-suggest business KPIs. These feed into metric view generation and Genie space configuration.
        </p>
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
          if (!selectedTables.length) return (
            <div>
              <p className="text-xs text-slate-400 dark:text-slate-500 mb-2">Select tables in Setup to see which KPIs are relevant.</p>
              <div className="space-y-1.5">{kpis.map(k => <KpiRow key={k.kpi_id} k={k} />)}</div>
            </div>
          )
          const relevant = kpis.filter(k => kpiMatchesTables(k, selectedTables))
          const other = kpis.filter(k => !kpiMatchesTables(k, selectedTables))
          return (
            <div className="space-y-3">
              {relevant.length > 0 && (
                <div>
                  <h4 className="text-xs font-semibold text-slate-500 dark:text-slate-400 mb-1.5 uppercase tracking-wide">Relevant to selected tables ({relevant.length})</h4>
                  <div className="space-y-1.5">{relevant.map(k => <KpiRow key={k.kpi_id} k={k} />)}</div>
                </div>
              )}
              {other.length > 0 && (
                <div>
                  <button onClick={() => setShowOtherKpis(p => !p)} className="text-xs font-semibold text-slate-400 dark:text-slate-500 uppercase tracking-wide hover:text-slate-600 dark:hover:text-slate-300 flex items-center gap-1">
                    <span className={`transition-transform ${showOtherKpis ? 'rotate-90' : ''}`}>&#9654;</span>
                    Other KPIs ({other.length})
                  </button>
                  {showOtherKpis && (
                    <div className="space-y-1.5 mt-1.5">{other.map(k => <KpiRow key={k.kpi_id} k={k} dimmed />)}</div>
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
        const relevantKpis = kpis.filter(k => kpiMatchesTables(k, selectedTables))
        return <>

      {/* Generation inputs summary */}
      <section className={section}>
        <h2 className="text-lg font-semibold mb-3 dark:text-gray-100">Generation Inputs</h2>
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
            <span className="text-xs text-slate-500 dark:text-slate-400 block mb-1">Matching KPIs</span>
            <span className="font-medium dark:text-gray-200">
              {selectedTables.length ? `${relevantKpis.length} of ${kpis.length}` : `${kpis.length} total`}
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
            className={btnPrimary}>
            {isGenerating ? 'Generating...' : 'Generate'}
          </button>
          <button onClick={() => startGeneration('additive')}
            disabled={loading || isGenerating || !selectedTables.length || !questionLines.length}
            className="px-4 py-2 bg-teal-600 text-white rounded-md text-sm hover:bg-teal-700 disabled:opacity-50">
            Generate More
          </button>
          {selectedProjectId && (
            <button onClick={() => { if (!confirm('Regenerate all definitions? This will replace existing ones.')) return; startGeneration('replace_all') }}
              disabled={loading || isGenerating || !selectedTables.length || !questionLines.length}
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
            </div>
          )}
        </section>
      )}

      </>})()}

      {/* === Definitions Tab === */}
      {activeTab === 'definitions' && <>

      <div className="flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
        <span>Showing definitions for:</span>
        <span className="font-medium text-slate-700 dark:text-slate-200">
          {selectedProjectId ? projects.find(p => p.project_id === selectedProjectId)?.project_name : 'All projects'}
        </span>
        <button onClick={() => setActiveTab('setup')} className="text-blue-600 dark:text-blue-400 hover:underline ml-1">Change</button>
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

          {/* Create Target */}
          <div className="flex items-center gap-3 mb-4 p-3 bg-dbx-oat dark:bg-gray-900 rounded-md border dark:border-gray-700">
            <span className="text-xs font-medium text-gray-600 dark:text-gray-400 whitespace-nowrap">Create target:</span>
            <input value={createTarget.catalog} onChange={e => setCreateTarget(prev => ({ ...prev, catalog: e.target.value }))}
              placeholder="catalog" className="input-base !text-xs w-40" />
            <span className="text-gray-400">.</span>
            <input value={createTarget.schema} onChange={e => setCreateTarget(prev => ({ ...prev, schema: e.target.value }))}
              placeholder="schema" className="input-base !text-xs w-40" />
          </div>

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
                    </div>
                    <div className="flex items-center gap-1 flex-shrink-0">
                      {statusBadge(d.status)}
                      {d.complexity_level === 'trivial' && (
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400" title="Only basic COUNT/SUM measures">Trivial</span>
                      )}
                      {d.complexity_level === 'rich' && (
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400" title="Includes ratios, rates, or filtered aggregates">Rich</span>
                      )}
                      {/* Compact action menu */}
                      <details className="relative">
                        <summary className="px-1.5 py-0.5 text-xs text-slate-500 hover:text-slate-700 dark:hover:text-slate-300 cursor-pointer select-none rounded hover:bg-slate-100 dark:hover:bg-dbx-navy-500">Actions</summary>
                        <div className="absolute right-0 mt-1 z-20 bg-white dark:bg-dbx-navy-600 border dark:border-gray-600 rounded-lg shadow-lg py-1 min-w-[140px]">
                          <button onClick={() => loadDefinitionJson(d.definition_id)} className="w-full text-left px-3 py-1.5 text-xs hover:bg-slate-50 dark:hover:bg-dbx-navy-500">
                            {expandedDef === d.definition_id ? 'Hide JSON' : 'View JSON'}
                          </button>
                          <button onClick={() => openEdit(d.definition_id)} className="w-full text-left px-3 py-1.5 text-xs hover:bg-slate-50 dark:hover:bg-dbx-navy-500">Edit JSON</button>
                          {d.status === 'failed' && (
                            <button onClick={() => retryDefinition(d.definition_id)} disabled={!!busy}
                              className="w-full text-left px-3 py-1.5 text-xs text-dbx-lava hover:bg-red-50 dark:hover:bg-red-900/20 disabled:opacity-50">
                              {busy === 'retry' ? 'Retrying...' : 'Retry'}
                            </button>
                          )}
                          {(d.status === 'validated' || d.status === 'applied') && (
                            <>
                              <button onClick={() => improveDefinition(d.definition_id)} disabled={!!busy}
                                className="w-full text-left px-3 py-1.5 text-xs text-blue-600 hover:bg-blue-50 dark:hover:bg-blue-900/20 disabled:opacity-50">
                                {busy === 'improve' ? 'Improving...' : 'Improve'}
                              </button>
                              <button onClick={() => createDefinition(d.definition_id)} disabled={!!busy}
                                className="w-full text-left px-3 py-1.5 text-xs text-green-600 hover:bg-green-50 dark:hover:bg-green-900/20 disabled:opacity-50">
                                {busy === 'create' ? 'Creating...' : d.status === 'applied' ? 'Re-apply' : 'Create in UC'}
                              </button>
                            </>
                          )}
                          {d.status === 'applied' && (
                            <button onClick={() => dropDefinition(d.definition_id)} disabled={!!busy}
                              className="w-full text-left px-3 py-1.5 text-xs text-amber-600 hover:bg-amber-50 dark:hover:bg-amber-900/20 disabled:opacity-50">
                              {busy === 'drop' ? 'Dropping...' : 'Drop from UC'}
                            </button>
                          )}
                          <hr className="my-1 dark:border-gray-600" />
                          <button onClick={() => deleteDefinition(d.definition_id, d.status)} disabled={!!busy}
                            className="w-full text-left px-3 py-1.5 text-xs text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 disabled:opacity-50">
                            {busy === 'delete' ? 'Deleting...' : 'Delete'}
                          </button>
                        </div>
                      </details>
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
                    <pre className="mt-2 bg-dbx-oat dark:bg-gray-900 border dark:border-gray-600 rounded p-3 text-xs overflow-x-auto max-h-64 dark:text-gray-200">{expandedJson}</pre>
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
