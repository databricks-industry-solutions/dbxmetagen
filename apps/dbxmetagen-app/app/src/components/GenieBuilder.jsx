import React, { useState, useEffect, useRef } from 'react'
import { safeFetch, safeFetchObj, ErrorBanner } from '../App'

const STAGES = {
  starting: 'Starting...',
  gathering_context: 'Gathering metadata context...',
  initializing: 'Initializing agent...',
  agent_running: 'Agent generating configuration...',
  generating: 'Generating SQL & instructions...',
  parsing: 'Parsing output...',
  done: 'Complete',
  error: 'Failed',
}

export default function GenieBuilder() {
  const [tables, setTables] = useState([])
  const [selectedTables, setSelectedTables] = useState([])
  const [questions, setQuestions] = useState('')
  const [taskId, setTaskId] = useState(null)
  const [taskStatus, setTaskStatus] = useState(null)
  const [result, setResult] = useState(null)
  const [editedJson, setEditedJson] = useState('')
  const [title, setTitle] = useState('')
  const [createdSpace, setCreatedSpace] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const pollRef = useRef(null)

  useEffect(() => {
    (async () => {
      const { data } = await safeFetch('/api/coverage/tables')
      if (data.length) {
        setTables(data.map(r => ({
          id: `${r.table_catalog}.${r.table_schema}.${r.table_name}`,
          label: r.table_name,
          schema: r.table_schema,
          tableType: r.table_type,
        })))
      }
    })()
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [])

  const toggleTable = (id) => {
    setSelectedTables(prev =>
      prev.includes(id) ? prev.filter(t => t !== id) : [...prev, id]
    )
  }

  const startGeneration = async () => {
    if (!selectedTables.length) return setError('Select at least one table')
    setError(null); setResult(null); setCreatedSpace(null); setLoading(true)
    const qs = questions.split('\n').map(q => q.trim()).filter(Boolean)
    const res = await fetch('/api/genie/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ table_identifiers: selectedTables, questions: qs }),
    })
    if (!res.ok) {
      const body = await res.json().catch(() => ({}))
      setError(body.detail || `Error ${res.status}`); setLoading(false); return
    }
    const { task_id } = await res.json()
    setTaskId(task_id); setTaskStatus({ status: 'running', stage: 'starting' })

    let missCount = 0
    pollRef.current = setInterval(async () => {
      const { data } = await safeFetchObj(`/api/genie/tasks/${task_id}`)
      if (!data) {
        missCount++
        if (missCount >= 3) {
          clearInterval(pollRef.current)
          setError('Task lost (app may have restarted). Please try again.')
          setLoading(false)
        }
        return
      }
      missCount = 0
      setTaskStatus(data)
      if (data.status === 'done') {
        clearInterval(pollRef.current)
        setResult(data.result)
        setEditedJson(JSON.stringify(data.result, null, 2))
        setLoading(false)
      } else if (data.status === 'error') {
        clearInterval(pollRef.current)
        setError(data.error || 'Agent failed')
        setLoading(false)
      }
    }, 3000)
  }

  const createSpace = async () => {
    if (!title.trim()) return setError('Enter a title for the Genie space')
    setError(null)
    let parsed
    try { parsed = JSON.parse(editedJson) } catch { return setError('Invalid JSON') }
    const res = await fetch('/api/genie/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: title.trim(), serialized_space: parsed }),
    })
    if (!res.ok) {
      const body = await res.json().catch(() => ({}))
      return setError(body.detail || `Error ${res.status}`)
    }
    setCreatedSpace(await res.json())
  }

  const schemas = [...new Set(tables.map(t => t.schema))]
  const [openSchemas, setOpenSchemas] = useState(new Set())

  const toggleSchema = (schema) => {
    setOpenSchemas(prev => {
      const next = new Set(prev)
      next.has(schema) ? next.delete(schema) : next.add(schema)
      return next
    })
  }

  const toggleAllInSchema = (schema) => {
    const ids = tables.filter(t => t.schema === schema).map(t => t.id)
    const allSelected = ids.every(id => selectedTables.includes(id))
    setSelectedTables(prev =>
      allSelected ? prev.filter(id => !ids.includes(id)) : [...new Set([...prev, ...ids])]
    )
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-lg font-semibold text-slate-800 dark:text-slate-200">Genie Space Builder</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
          Select tables and optionally provide business questions. An agent will generate
          a comprehensive Genie space configuration with instructions, SQL, filters, and measures.
        </p>
      </div>

      <ErrorBanner error={error} />

      {/* Table selection */}
      <div className="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
        <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-3">Select Tables</h3>
        {schemas.map(schema => {
          const schemaTables = tables.filter(t => t.schema === schema)
          const schemaIds = schemaTables.map(t => t.id)
          const selectedCount = schemaIds.filter(id => selectedTables.includes(id)).length
          const allSelected = selectedCount === schemaIds.length
          const isOpen = openSchemas.has(schema)

          return (
            <div key={schema} className="mb-2 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
              <button
                onClick={() => toggleSchema(schema)}
                className="w-full flex items-center justify-between px-3 py-2 bg-slate-50 dark:bg-slate-700/50 hover:bg-slate-100 dark:hover:bg-slate-700 transition-colors"
              >
                <div className="flex items-center gap-2">
                  <span className={`text-xs transition-transform ${isOpen ? 'rotate-90' : ''}`}>&#9654;</span>
                  <span className="text-xs font-medium text-slate-600 dark:text-slate-300">{schema}</span>
                  <span className="text-[10px] text-slate-400">({selectedCount}/{schemaIds.length})</span>
                </div>
                <span
                  onClick={e => { e.stopPropagation(); toggleAllInSchema(schema) }}
                  className="text-[10px] px-2 py-0.5 rounded border border-slate-300 dark:border-slate-600 text-slate-500 dark:text-slate-400 hover:bg-slate-200 dark:hover:bg-slate-600 cursor-pointer"
                >
                  {allSelected ? 'Deselect all' : 'Select all'}
                </span>
              </button>
              {isOpen && (
                <div className="flex flex-wrap gap-2 p-3">
                  {schemaTables.map(t => (
                    <button
                      key={t.id}
                      onClick={() => toggleTable(t.id)}
                      className={`px-3 py-1.5 text-xs rounded-md border transition-colors ${selectedTables.includes(t.id)
                          ? 'bg-indigo-100 dark:bg-indigo-900 border-indigo-400 text-indigo-700 dark:text-indigo-300'
                          : 'bg-white dark:bg-slate-700 border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-600'
                        }`}
                    >
                      {t.label}
                      {t.tableType && t.tableType !== 'MANAGED' && (
                        <span className="ml-1 text-[10px] text-purple-500 dark:text-purple-400">
                          {({ VIEW: 'V', EXTERNAL: 'EXT', STREAMING_TABLE: 'ST', MATERIALIZED_VIEW: 'MV' })[t.tableType] || t.tableType}
                        </span>
                      )}
                    </button>
                  ))}
                </div>
              )}
            </div>
          )
        })}
        {!tables.length && <p className="text-sm text-slate-400">Loading tables...</p>}
      </div>

      {/* Questions */}
      <div className="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
        <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">Business Questions (optional)</h3>
        <textarea
          value={questions}
          onChange={e => setQuestions(e.target.value)}
          rows={3}
          placeholder="One question per line..."
          className="w-full border border-slate-200 dark:border-slate-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-slate-700 text-slate-800 dark:text-slate-200 placeholder-slate-400"
        />
      </div>

      {/* Generate button */}
      <button
        onClick={startGeneration}
        disabled={loading || !selectedTables.length}
        className="px-5 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      >
        {loading ? 'Generating...' : 'Generate Genie Config'}
      </button>

      {/* Progress */}
      {taskStatus && taskStatus.status === 'running' && (
        <div className="bg-indigo-50 dark:bg-indigo-900/30 border border-indigo-200 dark:border-indigo-700 rounded-lg px-4 py-3">
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 border-2 border-indigo-400 border-t-transparent rounded-full animate-spin" />
            <span className="text-sm font-medium text-indigo-700 dark:text-indigo-300">
              {STAGES[taskStatus.stage] || taskStatus.stage}
            </span>
          </div>
        </div>
      )}

      {/* Result JSON */}
      {result && (
        <div className="space-y-4">
          <div className="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">Generated Configuration</h3>
            <textarea
              value={editedJson}
              onChange={e => setEditedJson(e.target.value)}
              rows={20}
              className="w-full font-mono text-xs border border-slate-200 dark:border-slate-600 rounded-md p-3 bg-slate-50 dark:bg-slate-900 text-slate-800 dark:text-slate-200"
            />
          </div>

          <div className="flex items-end gap-3">
            <div className="flex-1">
              <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Genie Space Title</label>
              <input
                type="text"
                value={title}
                onChange={e => setTitle(e.target.value)}
                placeholder="My Analytics Space"
                className="w-full border border-slate-200 dark:border-slate-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-slate-700 text-slate-800 dark:text-slate-200"
              />
            </div>
            <button
              onClick={createSpace}
              disabled={!title.trim()}
              className="px-5 py-2.5 bg-emerald-600 text-white text-sm font-medium rounded-lg hover:bg-emerald-700 disabled:opacity-50 transition-colors"
            >
              Create Genie Space
            </button>
          </div>

          {createdSpace && (
            <div className="bg-emerald-50 dark:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-700 rounded-lg px-4 py-3 text-sm text-emerald-700 dark:text-emerald-300">
              Genie space created! ID: <span className="font-mono">{createdSpace.space_id}</span>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
