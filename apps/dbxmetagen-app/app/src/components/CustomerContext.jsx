import React, { useState, useEffect, useCallback } from 'react'
import { safeFetch } from '../App'

const SCOPE_TYPES = ['catalog', 'schema', 'table', 'pattern']
// Per-entry word cap (mirrors customer_context.MAX_WORDS_PER_ENTRY on the backend).
// The combined per-table budget across all matching scopes is larger and reported by
// the resolve endpoint (budget_words); if it's exceeded the least-specific entries are
// dropped and surfaced in the preview below.
const MAX_WORDS = 500

function wordCount(text) { return text ? text.trim().split(/\s+/).filter(Boolean).length : 0 }

// Document lane (#249): these need a target scope; structured .yaml/.json carry their own.
const DOC_EXTS = ['txt', 'md', 'markdown', 'pdf', 'docx', 'pptx']
function isDocFile(name) {
  const ext = (name || '').split('.').pop().toLowerCase()
  return DOC_EXTS.includes(ext)
}

function ContextManager() {
  const [entries, setEntries] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [saving, setSaving] = useState(false)
  const [editing, setEditing] = useState(null)
  const [form, setForm] = useState({ scope: '', scope_type: 'schema', context_text: '', context_label: '', priority: 0 })
  const [preview, setPreview] = useState({ table: '', result: null })
  const [showUpload, setShowUpload] = useState(false)
  const [filterType, setFilterType] = useState('')
  const [uploadFile, setUploadFile] = useState(null)
  const [uploadScope, setUploadScope] = useState('')
  const [uploadScopeType, setUploadScopeType] = useState('schema')
  const [uploading, setUploading] = useState(false)
  const [uploadResult, setUploadResult] = useState(null)

  const load = useCallback(async () => {
    setLoading(true)
    const url = filterType ? `/api/customer-context?scope_type=${filterType}` : '/api/customer-context'
    const { data, error: err } = await safeFetch(url)
    if (err) setError(err); else setEntries(data || [])
    setLoading(false)
  }, [filterType])

  useEffect(() => { load() }, [load])

  const handleSave = async () => {
    if (saving) return   // ignore double-clicks -- a second concurrent MERGE conflicts
    setSaving(true)
    setError(null)
    const { data, error: err } = await safeFetch('/api/customer-context', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(form),
    })
    if (err) { setError(err); setSaving(false); return }
    setEditing(null)
    setForm({ scope: '', scope_type: 'schema', context_text: '', context_label: '', priority: 0 })
    setSaving(false)
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

  const docUpload = uploadFile && isDocFile(uploadFile.name)

  const handleUpload = async () => {
    if (!uploadFile || uploading) return
    if (docUpload && !uploadScope.trim()) return
    setUploading(true)
    setError(null)
    const fd = new FormData()
    fd.append('file', uploadFile)
    if (docUpload) {
      fd.append('scope', uploadScope.trim())
      fd.append('scope_type', uploadScopeType)
    }
    const { data, error: err } = await safeFetch('/api/customer-context/upload', { method: 'POST', body: fd })
    setUploading(false)
    if (err) { setError(err); return }
    setUploadResult(data)
    setUploadFile(null)
    setUploadScope('')
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
            Add domain knowledge that enriches LLM prompts during metadata generation. Scoped hierarchically: catalog &gt; schema &gt; pattern &gt; table.
            Each entry is limited to {MAX_WORDS} words. When a table matches several scopes, the more-specific context (table &gt; pattern &gt; schema &gt; catalog) is kept first if the combined budget is exceeded &mdash; use &ldquo;Preview Resolution&rdquo; below to see exactly what gets injected and whether anything is dropped.
            Keep entries concise: this text is injected into every prompt. For large or table-specific knowledge, prefer vector search / the knowledge base over long free-text here.
          </p>
        </div>
        <div className="flex gap-2">
          <button onClick={() => setShowUpload(!showUpload)} className="btn-ghost btn-sm">Upload File</button>
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
        <div className="card p-4 space-y-3">
          <div>
            <label className="text-sm font-medium text-slate-700 dark:text-slate-300">Upload file</label>
            <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
              Structured <code>.yaml</code>/<code>.json</code> (a <code>contexts</code> list) is parsed directly into entries.
              A document (<code>.txt</code>, <code>.md</code>, <code>.pdf</code>, <code>.docx</code>) is attached to the scope you choose below &mdash; long docs are summarized automatically, and <code>.pdf</code>/<code>.docx</code> need the AI document functions enabled on the SQL warehouse.
            </p>
            <input type="file" accept=".yaml,.yml,.json,.txt,.md,.pdf,.docx" onChange={e => { setUploadFile(e.target.files[0] || null); setUploadResult(null); setError(null) }}
              className="mt-2 block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-medium file:bg-dbx-lava/10 file:text-dbx-lava hover:file:bg-dbx-lava/20" />
          </div>
          {docUpload && (
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Scope <span className="text-red-500">*</span></label>
                <input value={uploadScope} onChange={e => setUploadScope(e.target.value)}
                  placeholder="e.g. my_catalog.my_schema"
                  className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Scope Type</label>
                <select value={uploadScopeType} onChange={e => setUploadScopeType(e.target.value)}
                  className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm">
                  {SCOPE_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
            </div>
          )}
          <div className="flex items-center justify-between">
            <span className="text-xs text-slate-400">{uploadFile ? uploadFile.name : 'No file selected'}</span>
            <div className="flex gap-2">
              <button onClick={() => { setShowUpload(false); setUploadFile(null); setUploadScope(''); setUploadResult(null) }} className="btn-ghost btn-sm">Cancel</button>
              <button onClick={handleUpload} disabled={!uploadFile || uploading || (docUpload && !uploadScope.trim())}
                className="btn-primary btn-sm disabled:opacity-50">{uploading ? 'Uploading…' : 'Upload'}</button>
            </div>
          </div>
          {uploadResult && (
            <div className="text-xs px-3 py-2 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300 space-y-1">
              <div>
                Uploaded {uploadResult.uploaded} {uploadResult.lane === 'document' ? 'document' : (uploadResult.uploaded === 1 ? 'entry' : 'entries')}.
                {uploadResult.results?.some(r => r.extract_method === 'ai_extract') && ' Summarized with AI.'}
                {uploadResult.results?.some(r => r.truncated) && ' Trimmed to the word limit.'}
              </div>
              {uploadResult.results?.filter(r => r.error).map((r, i) => (
                <div key={i} className="text-red-500">· {r.scope}: {r.error}</div>
              ))}
            </div>
          )}
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
            <button onClick={handleSave} disabled={saving || overLimit || !form.scope || !form.context_text.trim()}
              className="btn-primary btn-sm disabled:opacity-50">{saving ? 'Saving...' : 'Save'}</button>
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
              <span>Words: {preview.result.word_count}{preview.result.budget_words ? ` / ${preview.result.budget_words}` : ''}</span>
            </div>
            {preview.result.truncated && (
              <div className="text-xs px-2.5 py-2 rounded-md bg-amber-500/10 text-amber-700 dark:text-amber-300 border border-amber-400/30">
                ⚠ Combined context exceeds the {preview.result.budget_words}-word budget. Kept the most-specific context that fits.
                {preview.result.dropped_scopes?.length > 0 && (
                  <> Dropped (NOT injected): {preview.result.dropped_scopes.map(d => `${d.scope_type}:${d.scope}`).join(', ')}.</>
                )}
                {preview.result.truncated_scope && (
                  <> Partially injected (head kept): {preview.result.truncated_scope.scope_type}:{preview.result.truncated_scope.scope}.</>
                )}
                {' '}Trim entries or move detail to vector search / the knowledge base.
              </div>
            )}
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
          No customer context entries yet. Click "Add Context" or "Upload File" to get started.
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

// ---------------------------------------------------------------------------
// Acronyms: scope-aware abbreviations overlaid onto variables.yml acro_content at run time.
// ---------------------------------------------------------------------------
const ACRO_SCOPE_TYPES = ['schema', 'catalog']

function AcronymsManager() {
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [adding, setAdding] = useState(false)
  const [saving, setSaving] = useState(false)
  const [form, setForm] = useState({ scope: '', scope_type: 'schema', abbreviation: '', expansion: '', notes: '' })
  const [file, setFile] = useState(null)
  const [upScope, setUpScope] = useState('')
  const [upScopeType, setUpScopeType] = useState('schema')
  const [uploading, setUploading] = useState(false)
  const [upResult, setUpResult] = useState(null)

  const load = useCallback(async () => {
    setLoading(true)
    const { data, error: err } = await safeFetch('/api/customer-acronyms')
    if (err) setError(err); else setItems(data || [])
    setLoading(false)
  }, [])
  useEffect(() => { load() }, [load])

  const save = async () => {
    if (saving) return
    setSaving(true); setError(null)
    const { error: err } = await safeFetch('/api/customer-acronyms', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(form),
    })
    setSaving(false)
    if (err) { setError(err); return }
    setForm({ scope: '', scope_type: 'schema', abbreviation: '', expansion: '', notes: '' })
    setAdding(false); load()
  }
  const del = async (id) => {
    const { error: err } = await safeFetch(`/api/customer-acronyms/${id}`, { method: 'DELETE' })
    if (err) setError(err); else load()
  }
  const upload = async () => {
    if (!file || uploading) return
    setUploading(true); setError(null)
    const fd = new FormData()
    fd.append('file', file)
    fd.append('scope_type', upScopeType)
    if (upScope.trim()) fd.append('scope', upScope.trim())
    const { data, error: err } = await safeFetch('/api/customer-acronyms/upload', { method: 'POST', body: fd })
    setUploading(false)
    if (err) { setError(err); return }
    setUpResult(data); setFile(null); setUpScope(''); load()
  }

  return (
    <div className="space-y-5">
      <div className="flex items-start justify-between">
        <p className="text-sm text-slate-500 dark:text-slate-400 max-w-2xl">
          Abbreviations the model should expand in descriptions and classifications. Uploaded as a layer
          on top of the built-in global list, scoped to a <strong>schema</strong> or <strong>catalog</strong>;
          the more-specific scope wins. Upload an <code>.xlsx</code>/<code>.csv</code> with columns like
          <em> Abbreviation</em> and <em>Stands For</em>, or add one below.
        </p>
        <button onClick={() => setAdding(a => !a)} className="btn-primary btn-sm shrink-0">Add Acronym</button>
      </div>

      {error && (
        <div className="card border-l-4 border-l-red-500 px-4 py-3 text-sm flex justify-between">
          <span className="text-red-600 dark:text-red-400">{error}</span>
          <button onClick={() => setError(null)} className="text-slate-400 ml-2">&times;</button>
        </div>
      )}

      <div className="card p-4 space-y-3">
        <label className="text-sm font-medium text-slate-700 dark:text-slate-300">Upload acronyms (.xlsx / .csv)</label>
        <input type="file" accept=".xlsx,.xls,.csv" onChange={e => { setFile(e.target.files[0] || null); setUpResult(null); setError(null) }}
          className="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-medium file:bg-dbx-lava/10 file:text-dbx-lava hover:file:bg-dbx-lava/20" />
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Scope type</label>
            <select value={upScopeType} onChange={e => setUpScopeType(e.target.value)}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm">
              {ACRO_SCOPE_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Scope <span className="text-slate-400">(blank = this app's {upScopeType})</span></label>
            <input value={upScope} onChange={e => setUpScope(e.target.value)} placeholder={upScopeType === 'schema' ? 'catalog.schema' : 'catalog'}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-xs text-slate-400">{file ? file.name : 'No file selected'}</span>
          <button onClick={upload} disabled={!file || uploading} className="btn-primary btn-sm disabled:opacity-50">{uploading ? 'Uploading…' : 'Upload'}</button>
        </div>
        {upResult && <div className="text-xs px-3 py-2 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300">
          Uploaded {upResult.uploaded} acronym(s) to {upResult.scope_type}:{upResult.scope}.
          {upResult.skipped > 0 && <span className="text-amber-600 dark:text-amber-400"> Skipped {upResult.skipped} row(s){upResult.skipped_reasons?.length ? ` (${upResult.skipped_reasons.join('; ')})` : ''}.</span>}
          {upResult.errors?.length > 0 && <span className="text-red-500"> {upResult.errors.length} error(s).</span>}
        </div>}
      </div>

      {adding && (
        <div className="card p-4 grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Scope type</label>
            <select value={form.scope_type} onChange={e => setForm(f => ({ ...f, scope_type: e.target.value }))}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm">
              {ACRO_SCOPE_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Scope</label>
            <input value={form.scope} onChange={e => setForm(f => ({ ...f, scope: e.target.value }))} placeholder={form.scope_type === 'schema' ? 'catalog.schema' : 'catalog'}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Abbreviation</label>
            <input value={form.abbreviation} onChange={e => setForm(f => ({ ...f, abbreviation: e.target.value }))} placeholder="e.g. CD"
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Expansion</label>
            <input value={form.expansion} onChange={e => setForm(f => ({ ...f, expansion: e.target.value }))} placeholder="e.g. clinical data"
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div className="col-span-2">
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Notes (optional)</label>
            <input value={form.notes} onChange={e => setForm(f => ({ ...f, notes: e.target.value }))}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div className="col-span-2 flex gap-2 justify-end">
            <button onClick={() => setAdding(false)} className="btn-ghost btn-sm">Cancel</button>
            <button onClick={save} disabled={saving || !form.scope.trim() || !form.abbreviation.trim() || !form.expansion.trim()}
              className="btn-primary btn-sm disabled:opacity-50">{saving ? 'Saving…' : 'Save'}</button>
          </div>
        </div>
      )}

      {loading ? <div className="text-center py-8 text-slate-400">Loading…</div>
        : items.length === 0 ? <div className="card p-8 text-center text-slate-400">No acronyms yet. Upload a file or add one above.</div>
        : (
          <div className="card overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-slate-50 dark:bg-dbx-navy-600"><tr>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Abbreviation</th>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Expands to</th>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Scope</th>
                <th className="px-4 py-2.5 text-right font-medium text-slate-500 dark:text-slate-400">Actions</th>
              </tr></thead>
              <tbody className="divide-y divide-slate-100 dark:divide-dbx-navy-500">
                {items.map(a => (
                  <tr key={a.acronym_id} className="hover:bg-slate-50 dark:hover:bg-dbx-navy-600/50">
                    <td className="px-4 py-3 font-mono text-xs text-slate-700 dark:text-slate-300">{a.abbreviation}</td>
                    <td className="px-4 py-3 text-slate-600 dark:text-slate-400">{a.expansion}{a.notes ? ` (${a.notes})` : ''}</td>
                    <td className="px-4 py-3 text-xs text-slate-500">{a.scope_type}:{a.scope}</td>
                    <td className="px-4 py-3 text-right"><button onClick={() => del(a.acronym_id)} className="text-xs text-red-500 hover:underline">Delete</button></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Descriptions: per-column verbatim comment overrides, layered UNDER metadata_overrides.csv.
// ---------------------------------------------------------------------------
function OverridesManager() {
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [cfg, setCfg] = useState({ catalog_name: '', schema_name: '' })
  const [adding, setAdding] = useState(false)
  const [saving, setSaving] = useState(false)
  const [form, setForm] = useState({ catalog_name: '', schema_name: '', table_name: '', column_name: '', comment: '' })
  const [file, setFile] = useState(null)
  const [upCatalog, setUpCatalog] = useState('')
  const [upSchema, setUpSchema] = useState('')
  const [upTable, setUpTable] = useState('')
  const [uploading, setUploading] = useState(false)
  const [upResult, setUpResult] = useState(null)

  const load = useCallback(async () => {
    setLoading(true)
    const { data, error: err } = await safeFetch('/api/customer-overrides')
    if (err) setError(err); else setItems(data || [])
    setLoading(false)
  }, [])
  useEffect(() => { load() }, [load])
  useEffect(() => {
    safeFetch('/api/config').then(({ data }) => {
      if (data) {
        setCfg({ catalog_name: data.catalog_name || '', schema_name: data.schema_name || '' })
        setForm(f => ({ ...f, catalog_name: data.catalog_name || '', schema_name: data.schema_name || '' }))
      }
    })
  }, [])

  const save = async () => {
    if (saving) return
    setSaving(true); setError(null)
    const { error: err } = await safeFetch('/api/customer-overrides', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(form),
    })
    setSaving(false)
    if (err) { setError(err); return }
    setForm(f => ({ ...f, table_name: '', column_name: '', comment: '' }))
    setAdding(false); load()
  }
  const del = async (id) => {
    const { error: err } = await safeFetch(`/api/customer-overrides/${id}`, { method: 'DELETE' })
    if (err) setError(err); else load()
  }
  const upload = async () => {
    if (!file || uploading) return
    setUploading(true); setError(null)
    const fd = new FormData()
    fd.append('file', file)
    if (upCatalog.trim()) fd.append('catalog_name', upCatalog.trim())
    if (upSchema.trim()) fd.append('schema_name', upSchema.trim())
    if (upTable.trim()) fd.append('table_name', upTable.trim())
    const { data, error: err } = await safeFetch('/api/customer-overrides/upload', { method: 'POST', body: fd })
    setUploading(false)
    if (err) { setError(err); return }
    setUpResult(data); setFile(null); load()
  }

  return (
    <div className="space-y-5">
      <div className="flex items-start justify-between">
        <p className="text-sm text-slate-500 dark:text-slate-400 max-w-2xl">
          Pre-written column descriptions applied <strong>verbatim</strong> — the model is skipped for these columns.
          Layered under any <code>metadata_overrides.csv</code> (the file wins on conflict). Upload an
          <code> .xlsx</code>/<code>.csv</code> with <em>Table_Name</em>, <em>Column_Name</em>, and
          <em> Description</em> columns, or add one below. Applies to the next <em>descriptions</em> generation run.
        </p>
        <button onClick={() => setAdding(a => !a)} className="btn-primary btn-sm shrink-0">Add Description</button>
      </div>

      {error && (
        <div className="card border-l-4 border-l-red-500 px-4 py-3 text-sm flex justify-between">
          <span className="text-red-600 dark:text-red-400">{error}</span>
          <button onClick={() => setError(null)} className="text-slate-400 ml-2">&times;</button>
        </div>
      )}

      <div className="card p-4 space-y-3">
        <label className="text-sm font-medium text-slate-700 dark:text-slate-300">Upload descriptions (.xlsx / .csv)</label>
        <input type="file" accept=".xlsx,.xls,.csv" onChange={e => { setFile(e.target.files[0] || null); setUpResult(null); setError(null) }}
          className="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-medium file:bg-dbx-lava/10 file:text-dbx-lava hover:file:bg-dbx-lava/20" />
        <div className="grid grid-cols-3 gap-3">
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Catalog</label>
            <input value={upCatalog} onChange={e => setUpCatalog(e.target.value)} placeholder={cfg.catalog_name || 'catalog'}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Schema</label>
            <input value={upSchema} onChange={e => setUpSchema(e.target.value)} placeholder={cfg.schema_name || 'schema'}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Table <span className="text-slate-400">(if not in file)</span></label>
            <input value={upTable} onChange={e => setUpTable(e.target.value)} placeholder="blank = per-row / all"
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-xs text-slate-400">{file ? file.name : 'No file selected'}</span>
          <button onClick={upload} disabled={!file || uploading} className="btn-primary btn-sm disabled:opacity-50">{uploading ? 'Uploading…' : 'Upload'}</button>
        </div>
        {upResult && <div className="text-xs px-3 py-2 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300">
          Uploaded {upResult.uploaded} description(s) to {upResult.catalog_name}.{upResult.schema_name}.
          {upResult.skipped > 0 && <span className="text-amber-600 dark:text-amber-400"> Skipped {upResult.skipped} row(s){upResult.skipped_reasons?.length ? ` (${upResult.skipped_reasons.join('; ')})` : ''}.</span>}
          {upResult.errors?.length > 0 && <span className="text-red-500"> {upResult.errors.length} error(s).</span>}
        </div>}
      </div>

      {adding && (
        <div className="card p-4 grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Catalog</label>
            <input value={form.catalog_name} onChange={e => setForm(f => ({ ...f, catalog_name: e.target.value }))}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Schema</label>
            <input value={form.schema_name} onChange={e => setForm(f => ({ ...f, schema_name: e.target.value }))}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Table <span className="text-slate-400">(blank = schema-wide)</span></label>
            <input value={form.table_name} onChange={e => setForm(f => ({ ...f, table_name: e.target.value }))}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Column</label>
            <input value={form.column_name} onChange={e => setForm(f => ({ ...f, column_name: e.target.value }))}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div className="col-span-2">
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300 mb-1">Description (used verbatim)</label>
            <textarea value={form.comment} onChange={e => setForm(f => ({ ...f, comment: e.target.value }))} rows={3}
              className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
          </div>
          <div className="col-span-2 flex gap-2 justify-end">
            <button onClick={() => setAdding(false)} className="btn-ghost btn-sm">Cancel</button>
            <button onClick={save} disabled={saving || !form.catalog_name.trim() || !form.schema_name.trim() || !form.column_name.trim() || !form.comment.trim()}
              className="btn-primary btn-sm disabled:opacity-50">{saving ? 'Saving…' : 'Save'}</button>
          </div>
        </div>
      )}

      {loading ? <div className="text-center py-8 text-slate-400">Loading…</div>
        : items.length === 0 ? <div className="card p-8 text-center text-slate-400">No description overrides yet. Upload a file or add one above.</div>
        : (
          <div className="card overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-slate-50 dark:bg-dbx-navy-600"><tr>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Table</th>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Column</th>
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Description</th>
                <th className="px-4 py-2.5 text-right font-medium text-slate-500 dark:text-slate-400">Actions</th>
              </tr></thead>
              <tbody className="divide-y divide-slate-100 dark:divide-dbx-navy-500">
                {items.map(o => (
                  <tr key={o.override_id} className="hover:bg-slate-50 dark:hover:bg-dbx-navy-600/50">
                    <td className="px-4 py-3 font-mono text-xs text-slate-700 dark:text-slate-300">{o.catalog_name}.{o.schema_name}.{o.table_name || '*'}</td>
                    <td className="px-4 py-3 font-mono text-xs text-slate-600 dark:text-slate-400">{o.column_name}</td>
                    <td className="px-4 py-3 text-slate-600 dark:text-slate-400 max-w-md truncate" title={o.comment}>{o.comment}</td>
                    <td className="px-4 py-3 text-right"><button onClick={() => del(o.override_id)} className="text-xs text-red-500 hover:underline">Delete</button></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
    </div>
  )
}

const CC_TABS = [
  { id: 'context', label: 'Context' },
  { id: 'acronyms', label: 'Acronyms' },
  { id: 'descriptions', label: 'Descriptions' },
]

export default function CustomerContext() {
  const [tab, setTab] = useState('context')
  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-lg font-bold text-slate-800 dark:text-slate-100">Customer Context</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
          Domain knowledge that steers metadata generation: free-text context, acronym expansions, and
          verbatim column descriptions.
        </p>
      </div>
      <div className="flex gap-1 border-b border-slate-200 dark:border-dbx-navy-400/30">
        {CC_TABS.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors ${tab === t.id ? 'border-dbx-lava text-dbx-lava' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`}>
            {t.label}
          </button>
        ))}
      </div>
      {tab === 'context' && <ContextManager />}
      {tab === 'acronyms' && <AcronymsManager />}
      {tab === 'descriptions' && <OverridesManager />}
    </div>
  )
}
