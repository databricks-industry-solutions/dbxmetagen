import React, { useState, useEffect, useRef } from 'react'
import { safeFetch, safeFetchObj, ErrorBanner } from '../App'

const STAGES = {
  starting: 'Starting...',
  building_context: 'Building metadata context...',
  calling_ai: 'Generating metric views with AI...',
  validating: 'Validating expressions...',
  done: 'Complete',
}

export default function SemanticLayer() {
  // Projects
  const [projects, setProjects] = useState([])
  const [selectedProjectId, setSelectedProjectId] = useState('')
  const [newProjectName, setNewProjectName] = useState('')
  const [showNewProject, setShowNewProject] = useState(false)

  // Table selection
  const [catalogs, setCatalogs] = useState([])
  const [schemas, setSchemas] = useState([])
  const [allTables, setAllTables] = useState([])
  const [selectedCatalog, setSelectedCatalog] = useState('')
  const [selectedSchema, setSelectedSchema] = useState('')
  const [selectedTables, setSelectedTables] = useState([])
  const [tableFilter, setTableFilter] = useState('')

  // Profiles
  const [profiles, setProfiles] = useState([])
  const [activeProfileId, setActiveProfileId] = useState('')
  const [profileName, setProfileName] = useState('')
  const [questionsText, setQuestionsText] = useState('')

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

  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  // --- Init ---
  useEffect(() => {
    safeFetchObj('/api/config').then(({ data: cfg }) => {
      if (cfg) {
        setSelectedCatalog(cfg.catalog_name || '')
        setSelectedSchema(cfg.schema_name || '')
        setCreateTarget({ catalog: cfg.catalog_name || '', schema: cfg.schema_name || '' })
      }
    })
    loadProjects()
    loadProfiles()
    refreshDefinitions()
    fetch('/api/catalogs').then(r => r.json()).then(setCatalogs).catch(() => { })
  }, [])

  // Refresh definitions and pre-populate tables when project changes
  useEffect(() => {
    refreshDefinitions()
    if (selectedProjectId) {
      const proj = projects.find(p => p.project_id === selectedProjectId)
      if (proj?.selected_tables) {
        try {
          const tbls = JSON.parse(proj.selected_tables)
          if (Array.isArray(tbls) && tbls.length) setSelectedTables(tbls)
        } catch { /* ignore */ }
      }
    }
  }, [selectedProjectId])

  // Cascade: catalog -> schemas
  useEffect(() => {
    if (!selectedCatalog) { setSchemas([]); return }
    fetch(`/api/schemas?catalog=${selectedCatalog}`).then(r => r.json()).then(setSchemas).catch(() => setSchemas([]))
  }, [selectedCatalog])

  // Cascade: schema -> tables
  useEffect(() => {
    if (!selectedCatalog || !selectedSchema) { setAllTables([]); return }
    fetch(`/api/tables?catalog=${selectedCatalog}&schema=${selectedSchema}`)
      .then(r => r.json()).then(setAllTables).catch(() => setAllTables([]))
  }, [selectedCatalog, selectedSchema])

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
          if (data.status === 'done') refreshDefinitions()
        }
      } catch { clearInterval(pollRef.current) }
    }, 2000)
    return () => clearInterval(pollRef.current)
  }, [taskId])

  const loadProjects = () => {
    safeFetch('/api/semantic-layer/projects').then(({ data }) => {
      if (data) setProjects(data)
    })
  }

  const loadProfiles = () => {
    safeFetch('/api/semantic-layer/profiles').then(({ data }) => {
      if (data) setProfiles(data)
    })
  }

  const refreshDefinitions = () => {
    const url = selectedProjectId
      ? `/api/semantic-layer/definitions?project_id=${selectedProjectId}`
      : '/api/semantic-layer/definitions'
    safeFetch(url).then(({ data }) => {
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
    if (!pid) { setProfileName(''); setQuestionsText(''); return }
    const p = profiles.find(x => x.profile_id === pid)
    if (!p) return
    setProfileName(p.profile_name || '')
    try {
      const qs = JSON.parse(p.questions || '[]')
      setQuestionsText(qs.join('\n'))
    } catch { setQuestionsText(p.questions || '') }
    try {
      const tbls = JSON.parse(p.table_patterns || '[]')
      if (tbls.length) setSelectedTables(prev => [...new Set([...prev, ...tbls])])
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
        body: JSON.stringify({ profile_name: profileName, questions: lines, table_patterns: fqTables }),
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
    setLoading(true); setError(null)
    try {
      await fetch(`/api/semantic-layer/profiles/${activeProfileId}`, { method: 'DELETE' })
      setActiveProfileId('')
      setProfileName('')
      setQuestionsText('')
      loadProfiles()
    } catch (e) { setError(e.message) }
    setLoading(false)
  }

  // --- Table selection ---
  const toggleTable = (t) => {
    setSelectedTables(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t])
  }
  const selectAll = () => setSelectedTables([...filteredTables])
  const selectNone = () => setSelectedTables([])

  const filteredTables = allTables.filter(t =>
    !tableFilter || t.toLowerCase().includes(tableFilter.toLowerCase())
  )

  const suggestQuestions = async () => {
    if (!selectedTables.length) { setError('Select tables first'); return }
    setSuggestQLoading(true); setError(null)
    try {
      const fqTables = selectedTables.map(t => t.includes('.') ? t : `${selectedCatalog}.${selectedSchema}.${t}`)
      const res = await fetch('/api/genie/generate-questions', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_identifiers: fqTables, count: 6, purpose: 'metric_views' }),
      })
      const data = await res.json()
      if (!res.ok) { setError(data.detail || 'Failed to suggest questions'); setSuggestQLoading(false); return }
      const newQs = (data.questions || []).join('\n')
      setQuestionsText(prev => prev ? prev + '\n' + newQs : newQs)
    } catch (e) { setError(e.message) }
    setSuggestQLoading(false)
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
    const { data, error: err } = await safeFetchObj(`/api/semantic-layer/definitions/${defId}/json`)
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
      <ErrorBanner error={error} />

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
            <select value={selectedCatalog} onChange={e => { setSelectedCatalog(e.target.value); setSelectedSchema(''); setSelectedTables([]) }}
              className={input}>
              <option value="">-- select --</option>
              {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div>
            <label className={label}>Schema</label>
            <select value={selectedSchema} onChange={e => { setSelectedSchema(e.target.value); setSelectedTables([]) }}
              className={input} disabled={!selectedCatalog}>
              <option value="">-- select --</option>
              {schemas.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
          <div>
            <label className={label}>Filter tables</label>
            <input value={tableFilter} onChange={e => setTableFilter(e.target.value)}
              placeholder="Type to filter..." className={input} />
          </div>
        </div>
        {allTables.length > 0 && (
          <>
            <div className="flex gap-2 mb-2 text-xs">
              <button onClick={selectAll} className="text-blue-600 dark:text-blue-400 hover:underline">Select all ({filteredTables.length})</button>
              <button onClick={selectNone} className="text-blue-600 dark:text-blue-400 hover:underline">Clear</button>
              <span className="text-gray-400 ml-auto">{selectedTables.length} selected</span>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-1 max-h-48 overflow-y-auto border dark:border-gray-600 rounded-md p-2">
              {filteredTables.map(t => (
                <label key={t} className="flex items-center gap-1.5 text-xs cursor-pointer py-0.5 dark:text-gray-200">
                  <input type="checkbox" checked={selectedTables.includes(t)} onChange={() => toggleTable(t)} className="rounded" />
                  {t}
                </label>
              ))}
            </div>
          </>
        )}
        {selectedCatalog && selectedSchema && allTables.length === 0 && (
          <p className="text-sm text-gray-500 dark:text-gray-400 italic">No managed tables found in {selectedCatalog}.{selectedSchema}</p>
        )}
      </section>

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

      {/* Generate */}
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
            <button onClick={() => startGeneration('replace_all')}
              disabled={loading || isGenerating || !selectedTables.length || !questionLines.length}
              className="px-4 py-2 bg-dbx-lava text-white rounded-md text-sm hover:bg-red-700 disabled:opacity-50">
              Regenerate All
            </button>
          )}
        </div>
        {!selectedTables.length && questionLines.length > 0 && (
          <span className="text-xs text-amber-600 dark:text-amber-400 mt-2 block">Select tables above first</span>
        )}
        {selectedTables.length > 0 && !questionLines.length && (
          <span className="text-xs text-amber-600 dark:text-amber-400 mt-2 block">Add questions above first</span>
        )}
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

      {/* Definitions */}
      {definitions.length > 0 && (
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

          <div className="divide-y dark:divide-gray-700">
            {definitions.map(d => {
              const busy = actionLoading[d.definition_id]
              return (
                <div key={d.definition_id} className="py-3">
                  <div className="flex justify-between items-center">
                    <div>
                      <span className="font-medium text-sm dark:text-gray-100">{d.metric_view_name}</span>
                      {d.version && d.version > 1 && <span className="text-purple-500 dark:text-purple-400 text-xs ml-1">v{d.version}</span>}
                      <span className="text-gray-400 dark:text-gray-500 text-xs ml-2">{d.source_table}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      {statusBadge(d.status)}
                      <button onClick={() => loadDefinitionJson(d.definition_id)}
                        className="text-xs text-blue-600 dark:text-blue-400 hover:underline">
                        {expandedDef === d.definition_id ? 'Hide' : 'View JSON'}
                      </button>
                      <button onClick={() => openEdit(d.definition_id)}
                        className="text-xs text-slate-600 dark:text-slate-400 hover:underline">
                        Edit JSON
                      </button>

                      {d.status === 'failed' && (
                        <button onClick={() => retryDefinition(d.definition_id)} disabled={!!busy}
                          className="px-2 py-0.5 text-xs rounded bg-dbx-lava text-white hover:bg-red-700 disabled:opacity-50">
                          {busy === 'retry' ? 'Retrying...' : 'Retry'}
                        </button>
                      )}
                      {(d.status === 'validated' || d.status === 'applied') && (
                        <>
                          <button onClick={() => improveDefinition(d.definition_id)} disabled={!!busy}
                            className="px-2 py-0.5 text-xs rounded bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">
                            {busy === 'improve' ? 'Improving...' : 'Improve'}
                          </button>
                          <button onClick={() => createDefinition(d.definition_id)} disabled={!!busy}
                            className="px-2 py-0.5 text-xs rounded bg-green-600 text-white hover:bg-green-700 disabled:opacity-50">
                            {busy === 'create' ? 'Creating...' : d.status === 'applied' ? 'Re-apply' : 'Create'}
                          </button>
                        </>
                      )}
                      {d.status === 'applied' && (
                        <button onClick={() => dropDefinition(d.definition_id)} disabled={!!busy}
                          className="px-2 py-0.5 text-xs rounded bg-amber-600 text-white hover:bg-amber-700 disabled:opacity-50">
                          {busy === 'drop' ? 'Dropping...' : 'Drop from UC'}
                        </button>
                      )}
                      <button onClick={() => deleteDefinition(d.definition_id, d.status)} disabled={!!busy}
                        className="px-2 py-0.5 text-xs rounded bg-red-600 text-white hover:bg-red-700 disabled:opacity-50">
                        {busy === 'delete' ? '...' : 'Delete'}
                      </button>
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
          </div>
        </section>
      )}

      {editDefId && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onClick={() => !suggestLoading && setEditDefId(null)}>
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
