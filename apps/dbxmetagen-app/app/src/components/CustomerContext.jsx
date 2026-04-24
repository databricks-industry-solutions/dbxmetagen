import React, { useState, useEffect, useCallback } from 'react'
import { safeFetch } from '../App'

const SCOPE_TYPES = ['catalog', 'schema', 'table', 'pattern']
const MAX_WORDS = 500

function wordCount(text) { return text ? text.trim().split(/\s+/).filter(Boolean).length : 0 }

export default function CustomerContext() {
  const [entries, setEntries] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [editing, setEditing] = useState(null)
  const [form, setForm] = useState({ scope: '', scope_type: 'schema', context_text: '', context_label: '', priority: 0 })
  const [preview, setPreview] = useState({ table: '', result: null })
  const [showUpload, setShowUpload] = useState(false)
  const [filterType, setFilterType] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    const url = filterType ? `/api/customer-context?scope_type=${filterType}` : '/api/customer-context'
    const { data, error: err } = await safeFetch(url)
    if (err) setError(err); else setEntries(data || [])
    setLoading(false)
  }, [filterType])

  useEffect(() => { load() }, [load])

  const handleSave = async () => {
    setError(null)
    const { data, error: err } = await safeFetch('/api/customer-context', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(form),
    })
    if (err) { setError(err); return }
    setEditing(null)
    setForm({ scope: '', scope_type: 'schema', context_text: '', context_label: '', priority: 0 })
    load()
  }

  const handleDelete = async (id) => {
    const { error: err } = await safeFetch(`/api/customer-context/${id}`, { method: 'DELETE' })
    if (err) setError(err); else load()
  }

  const handleEdit = (entry) => {
    setForm({
      scope: entry.scope, scope_type: entry.scope_type,
      context_text: entry.context_text, context_label: entry.context_label || '',
      priority: entry.priority || 0,
    })
    setEditing(entry.context_id)
  }

  const handleUpload = async (e) => {
    const file = e.target.files[0]
    if (!file) return
    const fd = new FormData()
    fd.append('file', file)
    const { data, error: err } = await safeFetch('/api/customer-context/upload', { method: 'POST', body: fd })
    if (err) setError(err)
    else setError(null)
    setShowUpload(false)
    load()
  }

  const handleResolve = async () => {
    if (!preview.table) return
    const { data, error: err } = await safeFetch(`/api/customer-context/resolve/${preview.table}`)
    if (err) setError(err)
    else setPreview(p => ({ ...p, result: data }))
  }

  const wc = wordCount(form.context_text)
  const overLimit = wc > MAX_WORDS

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-bold text-slate-800 dark:text-slate-100">Customer Context</h2>
          <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
            Add domain knowledge that enriches LLM prompts during metadata generation. Scoped hierarchically: catalog &gt; schema &gt; table &gt; pattern.
            Each entry is limited to {MAX_WORDS} words.
          </p>
        </div>
        <div className="flex gap-2">
          <button onClick={() => setShowUpload(!showUpload)} className="btn-ghost btn-sm">Upload YAML</button>
          <button onClick={() => { setEditing('new'); setForm({ scope: '', scope_type: 'schema', context_text: '', context_label: '', priority: 0 }) }}
            className="btn-primary btn-sm">Add Context</button>
        </div>
      </div>

      {error && (
        <div className="card border-l-4 border-l-red-500 px-4 py-3 text-sm flex justify-between items-start">
          <span className="text-red-600 dark:text-red-400">{error}</span>
          <button onClick={() => setError(null)} className="text-slate-400 hover:text-slate-600 ml-2">&times;</button>
        </div>
      )}

      {showUpload && (
        <div className="card p-4">
          <label className="text-sm font-medium text-slate-700 dark:text-slate-300">Upload YAML file</label>
          <input type="file" accept=".yaml,.yml" onChange={handleUpload}
            className="mt-2 block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-medium file:bg-dbx-lava/10 file:text-dbx-lava hover:file:bg-dbx-lava/20" />
        </div>
      )}

      {editing && (
        <div className="card p-5 space-y-4">
          <h3 className="font-semibold text-slate-800 dark:text-slate-100">
            {editing === 'new' ? 'Add Context Entry' : 'Edit Context Entry'}
          </h3>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Scope</label>
              <input value={form.scope} onChange={e => setForm(f => ({ ...f, scope: e.target.value }))}
                placeholder="e.g. my_catalog.my_schema"
                className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Scope Type</label>
              <select value={form.scope_type} onChange={e => setForm(f => ({ ...f, scope_type: e.target.value }))}
                className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm">
                {SCOPE_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Label (optional)</label>
              <input value={form.context_label} onChange={e => setForm(f => ({ ...f, context_label: e.target.value }))}
                placeholder="e.g. Claims schema context"
                className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Priority</label>
              <input type="number" value={form.priority} onChange={e => setForm(f => ({ ...f, priority: parseInt(e.target.value) || 0 }))}
                className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
            </div>
          </div>
          <div>
            <div className="flex justify-between items-center mb-1">
              <label className="text-sm font-medium text-slate-600 dark:text-slate-300">Context Text</label>
              <span className={`text-xs ${overLimit ? 'text-red-500 font-medium' : 'text-slate-400'}`}>
                {wc} / {MAX_WORDS} words
              </span>
            </div>
            <textarea value={form.context_text} onChange={e => setForm(f => ({ ...f, context_text: e.target.value }))}
              rows={5} placeholder="Domain-specific context that will be injected into LLM prompts..."
              className={`w-full px-3 py-2 rounded-lg border bg-white dark:bg-dbx-navy-600 text-sm ${overLimit ? 'border-red-400' : 'border-slate-300 dark:border-dbx-navy-400'}`} />
          </div>
          <div className="flex gap-2 justify-end">
            <button onClick={() => setEditing(null)} className="btn-ghost btn-sm">Cancel</button>
            <button onClick={handleSave} disabled={overLimit || !form.scope || !form.context_text.trim()}
              className="btn-primary btn-sm disabled:opacity-50">Save</button>
          </div>
        </div>
      )}

      {/* Resolve Preview */}
      <div className="card p-4">
        <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-2">Preview Resolution</h3>
        <div className="flex gap-2">
          <input value={preview.table} onChange={e => setPreview({ table: e.target.value, result: null })}
            placeholder="catalog.schema.table_name"
            className="flex-1 px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm"
            onKeyDown={e => e.key === 'Enter' && handleResolve()} />
          <button onClick={handleResolve} disabled={!preview.table} className="btn-primary btn-sm disabled:opacity-50">Resolve</button>
        </div>
        {preview.result && (
          <div className="mt-3 p-3 bg-slate-50 dark:bg-dbx-navy-600 rounded-lg text-sm space-y-2">
            <div className="flex gap-4 text-xs text-slate-500">
              <span>Matching scopes: {preview.result.matching_scopes?.join(', ') || 'none'}</span>
              <span>Words: {preview.result.word_count}</span>
            </div>
            {preview.result.resolved_context ? (
              <p className="text-slate-700 dark:text-slate-300 whitespace-pre-wrap">{preview.result.resolved_context}</p>
            ) : (
              <p className="text-slate-400 italic">No context matches this table.</p>
            )}
          </div>
        )}
      </div>

      {/* Filter */}
      <div className="flex gap-2 items-center">
        <span className="text-sm text-slate-500 dark:text-slate-400">Filter:</span>
        <button onClick={() => setFilterType('')}
          className={`px-2.5 py-1 rounded-md text-xs font-medium transition-colors ${!filterType ? 'bg-dbx-lava text-white' : 'bg-slate-100 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300 hover:bg-slate-200'}`}>
          All
        </button>
        {SCOPE_TYPES.map(t => (
          <button key={t} onClick={() => setFilterType(t)}
            className={`px-2.5 py-1 rounded-md text-xs font-medium transition-colors ${filterType === t ? 'bg-dbx-lava text-white' : 'bg-slate-100 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300 hover:bg-slate-200'}`}>
            {t}
          </button>
        ))}
      </div>

      {/* Table */}
      {loading ? (
        <div className="text-center py-8 text-slate-400">Loading...</div>
      ) : entries.length === 0 ? (
        <div className="card p-8 text-center text-slate-400">
          No customer context entries yet. Click "Add Context" or "Upload YAML" to get started.
        </div>
      ) : (
        <div className="card overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 dark:bg-dbx-navy-600">
              <tr>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Scope</th>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Type</th>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Label</th>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Words</th>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Priority</th>
                <th className="px-4 py-2.5 text-right font-medium text-slate-500 dark:text-slate-400">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-dbx-navy-500">
              {entries.map(e => (
                <tr key={e.context_id} className="hover:bg-slate-50 dark:hover:bg-dbx-navy-600/50">
                  <td className="px-4 py-3 font-mono text-xs text-slate-700 dark:text-slate-300">{e.scope}</td>
                  <td className="px-4 py-3">
                    <span className="inline-flex px-2 py-0.5 rounded-full text-xs font-medium bg-slate-100 dark:bg-dbx-navy-500 text-slate-600 dark:text-slate-300">
                      {e.scope_type}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-slate-600 dark:text-slate-400">{e.context_label || '-'}</td>
                  <td className="px-4 py-3 text-slate-500">{e.word_count}</td>
                  <td className="px-4 py-3 text-slate-500">{e.priority}</td>
                  <td className="px-4 py-3 text-right">
                    <button onClick={() => handleEdit(e)} className="text-xs text-dbx-sky hover:underline mr-3">Edit</button>
                    <button onClick={() => handleDelete(e.context_id)} className="text-xs text-red-500 hover:underline">Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
