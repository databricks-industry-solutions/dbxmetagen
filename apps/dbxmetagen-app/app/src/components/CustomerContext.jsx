import React, { useState, useEffect, useCallback, useMemo } from 'react'
import { safeFetch } from '../App'

const SCOPE_TYPES = ['catalog', 'schema', 'table', 'pattern']
// Per-entry word cap (mirrors customer_context.MAX_WORDS_PER_ENTRY on the backend).
// The combined per-table budget across all matching scopes is larger and reported by
// the resolve endpoint (budget_words); if it's exceeded the least-specific entries are
// dropped and surfaced in the preview below.
const MAX_WORDS = 500
// Upload guidance (enforced server-side; this is the generous default — see MAX_UPLOAD_MB env).
const MAX_UPLOAD_MB = 25

function wordCount(text) { return text ? text.trim().split(/\s+/).filter(Boolean).length : 0 }

// Pick a file from a UC Volume the app can read (OBO user if OBO, else the app SP). Browses via
// /api/volumes/browse; the path box is editable to jump to any accessible volume.
function VolumePicker({ onSelect, selected }) {
  const [path, setPath] = useState('')
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)
  const browse = useCallback(async (p) => {
    setLoading(true); setErr(null)
    const { data, error } = await safeFetch(`/api/volumes/browse?path=${encodeURIComponent(p || '')}`)
    setLoading(false)
    if (error) { setErr(error); return }
    setData(data); setPath(data.path || '')
  }, [])
  useEffect(() => { browse('') }, [browse])
  return (
    <div className="border border-slate-300 dark:border-dbx-navy-400 rounded-lg p-2 space-y-2 bg-white dark:bg-dbx-navy-600">
      <div className="flex gap-2">
        <input value={path} onChange={e => setPath(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && browse(path)}
          placeholder="/Volumes/catalog/schema/volume"
          className="flex-1 px-2 py-1.5 rounded-md border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-500 text-sm font-mono" />
        <button type="button" onClick={() => browse(path)} className="btn-ghost btn-sm shrink-0">{loading ? '…' : 'Browse'}</button>
      </div>
      {err && <div className="text-xs text-red-500">{err}</div>}
      <div className="max-h-52 overflow-auto text-sm divide-y divide-slate-100 dark:divide-dbx-navy-500">
        {data?.parent != null && (
          <button type="button" onClick={() => browse(data.parent)} className="block w-full text-left px-2 py-1.5 hover:bg-slate-50 dark:hover:bg-dbx-navy-500 text-slate-500">⬆ ..</button>
        )}
        {(data?.entries || []).map(e => e.is_directory ? (
          <button type="button" key={e.path} onClick={() => browse(e.path)} className="block w-full text-left px-2 py-1.5 hover:bg-slate-50 dark:hover:bg-dbx-navy-500">📁 {e.name}</button>
        ) : (
          <button type="button" key={e.path} onClick={() => onSelect(e.path)}
            className={`block w-full text-left px-2 py-1.5 hover:bg-slate-50 dark:hover:bg-dbx-navy-500 ${selected === e.path ? 'bg-dbx-lava/10 text-dbx-lava font-medium' : ''}`}>📄 {e.name}</button>
        ))}
        {data && (data.entries || []).length === 0 && !loading && <div className="px-2 py-2 text-xs text-slate-400">Empty folder.</div>}
      </div>
      {selected && <div className="text-xs text-slate-500 dark:text-slate-400 truncate">Selected: <span className="font-mono">{selected}</span></div>}
    </div>
  )
}

// Document lane (#249): these need a target scope; structured .yaml/.json carry their own.
// Document lane (#249) = the AI ingestion path (split/decompose or single-scope distill).
// Spreadsheets (.csv/.xlsx) join it: they carry free-form business context, not clean entries.
// Structured .yaml/.json carry their own scope and are parsed deterministically instead.
const DOC_EXTS = ['txt', 'md', 'markdown', 'pdf', 'docx', 'pptx', 'csv', 'xlsx', 'xls']
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
  const [uploadSrc, setUploadSrc] = useState('local')   // 'local' (file) or 'volume'
  const [uploadVol, setUploadVol] = useState('')         // selected UC Volume file path
  const [uploadScope, setUploadScope] = useState('')
  const [uploadScopeType, setUploadScopeType] = useState('schema')
  const [uploading, setUploading] = useState(false)
  const [uploadResult, setUploadResult] = useState(null)
  // For a DOCUMENT upload, how to apply it: 'split' = decompose into many table-scoped
  // entries grounded on real tables (best for a glossary / data dictionary); 'single' =
  // attach the whole summarized doc to one scope. Structured .yaml/.json ignore this.
  const [docMode, setDocMode] = useState('split')
  const [dec, setDec] = useState({ catalog: '', schema: '', table: '', description: '', hasEmbedded: false })
  const [decomposing, setDecomposing] = useState(false)
  const [decResult, setDecResult] = useState(null)
  // Disambiguation: every ingest path merely APPENDS, so a scope can accumulate >1 active entry
  // ("needs disambiguation"). `resolve` holds the group being reconciled; `proposal` is the
  // editable combined text (generated on demand); nothing is overwritten until the user saves.
  // See openResolve / generateProposal / applyResolve.
  const [resolve, setResolve] = useState(null)
  const [proposal, setProposal] = useState('')
  const [genProposal, setGenProposal] = useState(false)
  const [applying, setApplying] = useState(false)
  // Opt-in customer-context vector index (gated by /api/config.enable_context_vector_index).
  // The index is app + downstream only; metadata generation never queries it.
  const [vsEnabled, setVsEnabled] = useState(false)
  const [vsStatus, setVsStatus] = useState(null)
  const [vsSyncing, setVsSyncing] = useState(false)
  const [vsMsg, setVsMsg] = useState(null)

  const loadVsStatus = useCallback(async () => {
    const { data } = await safeFetch('/api/customer-context/vector/status')
    if (data) { setVsEnabled(!!data.enabled); setVsStatus(data.enabled ? data : null) }
  }, [])

  useEffect(() => { loadVsStatus() }, [loadVsStatus])

  const handleVsSync = async () => {
    if (vsSyncing) return
    setVsSyncing(true); setVsMsg(null); setError(null)
    const { data, error: err } = await safeFetch('/api/customer-context/vector/sync', { method: 'POST' })
    setVsSyncing(false)
    if (err) { setError(err); return }
    setVsMsg(data?.detail || 'Sync triggered.')
    loadVsStatus()
  }

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
    // Editing an existing row sends its context_id so it updates in place; a new entry omits it
    // and the server appends (never blocks). If the scope then has >1 entry it shows up flagged.
    const payload = { ...form, context_id: (editing && editing !== 'new') ? editing : '' }
    const { error: err } = await safeFetch('/api/customer-context', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
    setSaving(false)
    if (err) { setError(err); return }
    setEditing(null)
    setForm({ scope: '', scope_type: 'schema', context_text: '', context_label: '', priority: 0 })
    load()
  }

  // Open the resolve panel for a scope that has multiple active entries (needs disambiguation).
  const openResolve = (group) => {
    setResolve({ scope: group.scope, scope_type: group.scope_type, sources: group.entries })
    setProposal(''); setError(null)
  }

  // Generate (on demand) an AI-merged proposal for the scope. Pure preview -- writes nothing.
  const generateProposal = async () => {
    if (!resolve || genProposal) return
    setGenProposal(true); setError(null)
    const { data, error: err } = await safeFetch('/api/customer-context/combine-preview', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ scope: resolve.scope, scope_type: resolve.scope_type }),
    })
    setGenProposal(false)
    if (err) { setError(err); return }
    setProposal(data?.merged_text || '')
  }

  // Collapse the scope to ONE canonical entry from `text` (the edited proposal or a chosen
  // original): on_conflict='replace' retires every active row here and writes the single row.
  const applyResolve = async (text) => {
    if (!resolve || applying || !text || !text.trim()) return
    setApplying(true); setError(null)
    const { error: err } = await safeFetch('/api/customer-context', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        scope: resolve.scope, scope_type: resolve.scope_type,
        context_text: text, context_label: '', on_conflict: 'replace',
      }),
    })
    setApplying(false)
    if (err) { setError(err); return }
    setResolve(null); setProposal('')
    load()
  }

  const handleDelete = async (id) => {
    const { error: err } = await safeFetch(`/api/customer-context/${id}`, { method: 'DELETE' })
    if (err) setError(err); else load()
  }

  // Delete EVERY active entry at a scope (the Delete action on a multi-entry/grouped row).
  const handleDeleteScope = async (group) => {
    for (const e of group.entries) {
      const { error: err } = await safeFetch(`/api/customer-context/${e.context_id}`, { method: 'DELETE' })
      if (err) { setError(err); return }
    }
    load()
  }

  const handleEdit = (entry) => {
    setForm({
      scope: entry.scope, scope_type: entry.scope_type,
      context_text: entry.context_text, context_label: entry.context_label || '',
      priority: entry.priority || 0,
    })
    setEditing(entry.context_id)
  }

  // Source can be a local file OR a file chosen from a UC Volume; downstream logic keys off a
  // unified name/presence so the two paths share the same lane detection and submit flow.
  const srcName = uploadSrc === 'volume' ? uploadVol : (uploadFile?.name || '')
  const hasSrc = uploadSrc === 'volume' ? !!uploadVol : !!uploadFile
  const appendSource = (fd) => {
    if (uploadSrc === 'volume') fd.append('volume_path', uploadVol)
    else fd.append('file', uploadFile)
  }
  const docUpload = hasSrc && isDocFile(srcName)

  const handleUpload = async () => {
    if (!hasSrc || uploading) return
    if (docUpload && !uploadScope.trim()) return
    setUploading(true)
    setError(null)
    const fd = new FormData()
    appendSource(fd)
    if (docUpload) {
      fd.append('scope', uploadScope.trim())
      fd.append('scope_type', uploadScopeType)
    }
    const { data, error: err } = await safeFetch('/api/customer-context/upload', { method: 'POST', body: fd })
    setUploading(false)
    if (err) { setError(err); return }
    setUploadResult(data)
    setUploadFile(null); setUploadVol('')
    setUploadScope('')
    load()
  }

  // A document upload is applied one of two ways (docMode); a structured .yaml/.json is
  // parsed directly and ignores the mode. Scope description is FORCED (>= 5 words) for the
  // split path, matching the server guard -- the model needs a steer to place pieces.
  const structuredUpload = hasSrc && !docUpload
  const decWordCount = wordCount(dec.description)
  const canSubmitUpload = () => {
    if (!hasSrc || uploading || decomposing) return false
    if (structuredUpload) return true
    if (docMode === 'single') return !!uploadScope.trim()
    return !!dec.catalog.trim() && decWordCount >= 5   // split
  }
  const submitUpload = () => {
    if (docUpload && docMode === 'split') return handleDecompose()
    return handleUpload()
  }
  const resetUpload = () => {
    setShowUpload(false); setUploadFile(null); setUploadVol(''); setUploadSrc('local'); setUploadScope('')
    setDec({ catalog: '', schema: '', table: '', description: '', hasEmbedded: false })
    setUploadResult(null); setDecResult(null)
  }

  const handleDecompose = async () => {
    if (!hasSrc || decomposing) return
    setDecomposing(true)
    setError(null)
    setDecResult(null)
    const fd = new FormData()
    appendSource(fd)
    fd.append('catalog', dec.catalog.trim())
    if (dec.schema.trim()) fd.append('schema', dec.schema.trim())
    if (dec.table.trim()) fd.append('table', dec.table.trim())
    fd.append('scope_description', dec.description.trim())
    fd.append('has_embedded_scope', dec.hasEmbedded ? 'true' : 'false')
    const { data, error: err } = await safeFetch('/api/customer-context/decompose', { method: 'POST', body: fd })
    setDecomposing(false)
    if (err) { setError(err); return }
    setDecResult(data)
    setUploadFile(null); setUploadVol('')
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

  // Group active entries by (scope_type, scope) so each scope appears ONCE. A scope with >1
  // entry "needs disambiguation" -- derived purely from the stored rows, so every path that
  // merely appends participates automatically. Resolving collapses it to one canonical entry.
  const groups = useMemo(() => {
    const m = new Map()
    for (const e of entries) {
      const key = `${e.scope_type}\u0000${e.scope}`
      if (!m.has(key)) m.set(key, { scope: e.scope, scope_type: e.scope_type, entries: [] })
      m.get(key).entries.push(e)
    }
    return Array.from(m.values()).map(g => ({
      ...g,
      count: g.entries.length,
      totalWords: g.entries.reduce((s, e) => s + (e.word_count || 0), 0),
      labels: Array.from(new Set(g.entries.map(e => e.context_label).filter(Boolean))),
    }))
  }, [entries])

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
          <button onClick={() => { setShowUpload(!showUpload); setEditing(null) }} className="btn-ghost btn-sm">Add from a File</button>
          <button onClick={() => { setEditing('new'); setShowUpload(false); setForm({ scope: '', scope_type: 'schema', context_text: '', context_label: '', priority: 0 }) }}
            className="btn-primary btn-sm">Write an Entry</button>
        </div>
      </div>

      {error && (
        <div className="card border-l-4 border-l-red-500 px-4 py-3 text-sm flex justify-between items-start">
          <span className="text-red-600 dark:text-red-400">{error}</span>
          <button onClick={() => setError(null)} className="text-slate-400 hover:text-slate-600 ml-2">&times;</button>
        </div>
      )}

      {resolve && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={() => !applying && !genProposal && setResolve(null)}>
          <div className="card max-w-2xl w-full max-h-[85vh] overflow-y-auto p-5 space-y-4" onClick={e => e.stopPropagation()}>
            <div>
              <h3 className="font-semibold text-slate-800 dark:text-slate-100">Disambiguate this scope</h3>
              <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
                <span className="font-mono text-slate-700 dark:text-slate-300">{resolve.scope}</span> has{' '}
                {resolve.sources.length} active entries. Keep just one, or generate an AI-combined summary you can edit &mdash; nothing is overwritten until you save. The originals are kept as recoverable history.
              </p>
            </div>

            <div className="space-y-2">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-400">Current entries ({resolve.sources.length})</div>
              {resolve.sources.map((e, i) => (
                <div key={e.context_id || i} className="text-xs p-2.5 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300">
                  <div className="flex items-center justify-between gap-2 mb-1">
                    <span className="font-medium text-slate-500 dark:text-slate-400">{e.context_label || 'entry'} · {e.word_count}w</span>
                    <button onClick={() => applyResolve(e.context_text)} disabled={applying || genProposal}
                      className="text-xs text-dbx-sky hover:underline disabled:opacity-50 shrink-0">Keep only this</button>
                  </div>
                  <div className="whitespace-pre-wrap">{e.context_text}</div>
                </div>
              ))}
            </div>

            <div className="space-y-2 pt-1">
              <div className="flex items-center justify-between gap-2">
                <div className="text-xs font-semibold uppercase tracking-wide text-dbx-sky">Combined proposal</div>
                <button onClick={generateProposal} disabled={genProposal || applying}
                  className="btn-ghost btn-sm disabled:opacity-50">{genProposal ? 'Generating…' : (proposal ? 'Regenerate' : 'Generate combined proposal')}</button>
              </div>
              <textarea value={proposal} onChange={e => setProposal(e.target.value)} rows={6}
                placeholder="Click “Generate combined proposal” to merge all entries with AI, then edit it here — or paste your own canonical text."
                className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-700 text-sm" />
              <div className={`text-xs ${wordCount(proposal) > MAX_WORDS ? 'text-red-500 font-medium' : 'text-slate-400'}`}>
                {wordCount(proposal)} / {MAX_WORDS} words — saving replaces all entries at this scope with this single text.
              </div>
            </div>

            <div className="flex items-center justify-between pt-1">
              <span className="text-xs text-slate-400">{applying ? 'Applying…' : ''}</span>
              <div className="flex gap-2">
                <button onClick={() => { setResolve(null); setProposal('') }} disabled={applying || genProposal} className="btn-ghost btn-sm disabled:opacity-50">Cancel</button>
                <button onClick={() => applyResolve(proposal)} disabled={applying || genProposal || wordCount(proposal) === 0 || wordCount(proposal) > MAX_WORDS}
                  className="btn-primary btn-sm disabled:opacity-50">Save combined</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {showUpload && (
        <div className="card p-4 space-y-3">
          <div>
            <label className="text-sm font-medium text-slate-700 dark:text-slate-300">Add context from a file</label>
            <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
              Pick a file. A structured <code>.yaml</code>/<code>.json</code> (a <code>contexts</code> list) is parsed
              straight into entries. A document or spreadsheet (<code>.txt</code>, <code>.md</code>, <code>.pdf</code>,
              <code>.docx</code>, <code>.csv</code>, <code>.xlsx</code>) carries free-form business context and can be
              <strong> split across the tables it describes</strong> or <strong>attached whole to one scope</strong>
              &mdash; you&rsquo;ll choose below. (<code>.pdf</code>/<code>.docx</code> need the AI document functions enabled on the SQL warehouse.)
            </p>
            <div className="flex gap-1 mt-2 mb-2 bg-slate-100 dark:bg-dbx-navy-600 rounded-lg p-1 w-fit">
              <button type="button" onClick={() => { setUploadSrc('local'); setUploadVol(''); setError(null) }}
                className={`px-3 py-1 rounded-md text-xs font-medium ${uploadSrc === 'local' ? 'bg-white dark:bg-dbx-navy-500 shadow text-dbx-lava' : 'text-slate-500'}`}>From my computer</button>
              <button type="button" onClick={() => { setUploadSrc('volume'); setUploadFile(null); setError(null) }}
                className={`px-3 py-1 rounded-md text-xs font-medium ${uploadSrc === 'volume' ? 'bg-white dark:bg-dbx-navy-500 shadow text-dbx-lava' : 'text-slate-500'}`}>From a volume</button>
            </div>
            {uploadSrc === 'local' ? (
              <input type="file" accept=".yaml,.yml,.json,.txt,.md,.pdf,.docx,.csv,.xlsx,.xls"
                onChange={e => { setUploadFile(e.target.files[0] || null); setUploadResult(null); setDecResult(null); setError(null) }}
                className="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-medium file:bg-dbx-lava/10 file:text-dbx-lava hover:file:bg-dbx-lava/20" />
            ) : (
              <VolumePicker selected={uploadVol} onSelect={p => { setUploadVol(p); setUploadResult(null); setDecResult(null); setError(null) }} />
            )}
            <p className="text-xs text-slate-400 dark:text-slate-500 mt-1">
              Up to {MAX_UPLOAD_MB}&nbsp;MB per file. Long documents are summarized into a concise entry
              (&le;{MAX_WORDS} words); split very large glossaries by schema for the best results.
              A volume file is read with the app&rsquo;s access (your identity under OBO, otherwise the app service principal).
            </p>
          </div>

          {structuredUpload && (
            <div className="text-xs px-3 py-2 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300">
              Structured file detected &mdash; its <code>contexts</code> entries are imported directly (each carries its own scope). No further input needed.
            </div>
          )}

          {docUpload && (
            <>
              <div className="space-y-1.5">
                <label className="text-sm font-medium text-slate-700 dark:text-slate-300">How should this document be applied?</label>
                <label className="flex items-start gap-2 text-sm text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="radio" name="docmode" className="mt-1" checked={docMode === 'split'} onChange={() => setDocMode('split')} />
                  <span><strong>Split across the tables it describes</strong> <span className="text-slate-400">(recommended for a glossary / data dictionary / spec)</span><br />
                    <span className="text-xs text-slate-500 dark:text-slate-400">Each piece is auto-assigned to a <strong>real table</strong> under the catalog/schema you pick &mdash; names are validated (invented tables are never used), and unplaceable pieces fall back to your scope. <strong>Leave Table blank</strong> to let pieces spread across the schema; <strong>set Table</strong> to pin <em>every</em> piece to that one table.</span></span>
                </label>
                <label className="flex items-start gap-2 text-sm text-slate-600 dark:text-slate-300 cursor-pointer">
                  <input type="radio" name="docmode" className="mt-1" checked={docMode === 'single'} onChange={() => setDocMode('single')} />
                  <span><strong>Attach the whole document to one scope</strong><br />
                    <span className="text-xs text-slate-500 dark:text-slate-400">Summarized and stored as a single entry on the scope you choose. Use when the document is about one area &mdash; a single blob is injected into every table in that scope.</span></span>
                </label>
              </div>

              {docMode === 'split' && (
                <div className="space-y-3 border-l-2 border-l-dbx-sky/50 pl-3">
                  <div className="text-xs px-3 py-2 rounded-md bg-amber-500/10 text-amber-700 dark:text-amber-300 border border-amber-400/30">
                    Give each upload a clear, narrow scope. Don&rsquo;t dump many unrelated files or one giant file where the
                    scope can&rsquo;t be determined &mdash; prefer a file with scope headers, a file whose scope you describe
                    below, or <strong>multiple files, each with its own scope &amp; purpose</strong>.
                  </div>
                  <div className="grid grid-cols-3 gap-3">
                    <div>
                      <label className="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Catalog <span className="text-red-500">*</span></label>
                      <input value={dec.catalog} onChange={e => setDec(d => ({ ...d, catalog: e.target.value }))} placeholder="my_catalog"
                        className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Schema</label>
                      <input value={dec.schema} onChange={e => setDec(d => ({ ...d, schema: e.target.value }))} placeholder="(recommended)"
                        className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Table</label>
                      <input value={dec.table} onChange={e => setDec(d => ({ ...d, table: e.target.value }))} placeholder="(optional — pins all to this table)"
                        className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
                    </div>
                  </div>
                  <label className="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
                    <input type="checkbox" checked={dec.hasEmbedded} onChange={e => setDec(d => ({ ...d, hasEmbedded: e.target.checked }))} />
                    This file names specific catalogs / schemas / tables / columns inside it (use those to place content)
                  </label>
                  <div>
                    <div className="flex justify-between items-center mb-1">
                      <label className="text-sm font-medium text-slate-600 dark:text-slate-300">
                        Scope description <span className="text-red-500">*</span>
                        {!dec.hasEmbedded && <span className="text-xs text-slate-400 ml-1">(required &mdash; the file has no embedded scope, so describe it well)</span>}
                      </label>
                      <span className={`text-xs ${decWordCount < 5 ? 'text-red-500' : 'text-slate-400'}`}>{decWordCount} words (min 5)</span>
                    </div>
                    <textarea value={dec.description} onChange={e => setDec(d => ({ ...d, description: e.target.value }))}
                      rows={3} placeholder="What does this document cover, and which tables/areas is it about? e.g. 'Clinical trials glossary for the gold CDM schema: defines study, study_site, and quality-event tables and their status codes.'"
                      className="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-sm" />
                  </div>
                </div>
              )}

              {docMode === 'single' && (
                <div className="grid grid-cols-2 gap-3 border-l-2 border-l-slate-300 dark:border-l-dbx-navy-400 pl-3">
                  <div>
                    <label className="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Scope <span className="text-red-500">*</span></label>
                    <input value={uploadScope} onChange={e => setUploadScope(e.target.value)} placeholder="e.g. my_catalog.my_schema"
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
            </>
          )}

          <div className="flex items-center justify-between">
            <span className="text-xs text-slate-400 truncate max-w-xs">{hasSrc ? (uploadSrc === 'volume' ? srcName : uploadFile.name) : 'No file selected'}</span>
            <div className="flex gap-2">
              <button onClick={resetUpload} className="btn-ghost btn-sm">Cancel</button>
              <button onClick={submitUpload} disabled={!canSubmitUpload()} className="btn-primary btn-sm disabled:opacity-50">
                {decomposing ? 'Splitting… (this can take a minute)'
                  : uploading ? 'Adding…'
                  : (docUpload && docMode === 'split') ? 'Split & Save'
                  : 'Add'}
              </button>
            </div>
          </div>

          {decResult && (
            <div className="text-xs px-3 py-2 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300 space-y-1">
              <div>
                Created <strong>{decResult.uploaded}</strong> scoped {decResult.uploaded === 1 ? 'entry' : 'entries'} from {decResult.grounded_tables} grounded table(s).
                {decResult.coerced > 0 && <span className="text-amber-600 dark:text-amber-400"> {decResult.coerced} couldn&rsquo;t be tied to a specific table and fell back to your scope &mdash; review below.</span>}
              </div>
              {(decResult.entries || []).slice(0, 12).map((e, i) => (
                <div key={i} className="flex gap-2">
                  <span className={`inline-block px-1.5 rounded ${e.coerced ? 'bg-amber-500/15 text-amber-700 dark:text-amber-300' : 'bg-slate-200/60 dark:bg-dbx-navy-500'}`}>{e.scope_type}</span>
                  <span className="font-mono">{e.scope}</span>
                  {e.confidence != null && <span className="text-slate-400">· conf {Number(e.confidence).toFixed(2)}</span>}
                  {e.error && <span className="text-red-500">· {e.error}</span>}
                </div>
              ))}
              {(decResult.entries || []).length > 12 && <div className="text-slate-400">…and {decResult.entries.length - 12} more (see the table below)</div>}
            </div>
          )}
          {uploadResult && (
            <div className="text-xs px-3 py-2 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300 space-y-1">
              <div>
                Added {uploadResult.uploaded} {uploadResult.lane === 'document' ? 'document' : (uploadResult.uploaded === 1 ? 'entry' : 'entries')}.
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

      {/* Semantic vector index (opt-in; app + downstream only — generation never queries it) */}
      {vsEnabled && (
        <div className="card p-4">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300">Semantic context index</h3>
              <p className="text-xs text-slate-500 dark:text-slate-400 mt-1 max-w-2xl">
                Maintains a Vector Search index over your context entries for <strong>semantic</strong> retrieval —
                used by the app&rsquo;s context search and preview, and available to downstream consumers (agents/RAG,
                Genie). Using it as a scalable, scope-filtered retrieval mode <em>during metadata and semantic-layer
                generation</em> is planned (issue&nbsp;#260) and not yet wired into the generation jobs — today
                generation uses the scope hierarchy. Click sync after adding or editing entries.
              </p>
            </div>
            <button onClick={handleVsSync} disabled={vsSyncing} className="btn-primary btn-sm shrink-0 disabled:opacity-50">
              {vsSyncing ? 'Syncing…' : 'Sync to vector index'}
            </button>
          </div>
          {vsStatus && (
            <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-500 dark:text-slate-400">
              <span>Endpoint: <span className="font-mono">{vsStatus.endpoint_state || '—'}</span></span>
              <span>Index: <span className="font-mono">{vsStatus.index_ready ? 'ready' : (vsStatus.index_status === 'NOT_FOUND' ? 'not built' : 'provisioning')}</span></span>
              {vsStatus.indexed_rows != null && <span>Indexed rows: {vsStatus.indexed_rows}</span>}
              {vsStatus.active_entries != null && <span>Active entries: {vsStatus.active_entries}</span>}
            </div>
          )}
          {vsMsg && <div className="mt-2 text-xs px-2.5 py-2 rounded-md bg-dbx-sky/10 text-dbx-sky border border-dbx-sky/30">{vsMsg}</div>}
        </div>
      )}

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
      ) : groups.length === 0 ? (
        <div className="card p-8 text-center text-slate-400">
          No customer context entries yet. Click "Write an Entry" or "Add from a File" to get started.
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
                <th className="px-4 py-2.5 text-left font-medium text-slate-500 dark:text-slate-400">Entries</th>
                <th className="px-4 py-2.5 text-right font-medium text-slate-500 dark:text-slate-400">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-dbx-navy-500">
              {groups.map(g => {
                const multi = g.count > 1
                const only = g.entries[0]
                return (
                  <tr key={`${g.scope_type}:${g.scope}`} className="hover:bg-slate-50 dark:hover:bg-dbx-navy-600/50">
                    <td className="px-4 py-3 font-mono text-xs text-slate-700 dark:text-slate-300">
                      {g.scope}
                      {multi && (
                        <span className="ml-2 inline-flex px-2 py-0.5 rounded-full text-[10px] font-semibold bg-amber-500/15 text-amber-700 dark:text-amber-400 align-middle whitespace-nowrap">
                          needs disambiguation
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <span className="inline-flex px-2 py-0.5 rounded-full text-xs font-medium bg-slate-100 dark:bg-dbx-navy-500 text-slate-600 dark:text-slate-300">
                        {g.scope_type}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-slate-600 dark:text-slate-400">{g.labels.length ? g.labels.join(', ') : '-'}</td>
                    <td className="px-4 py-3 text-slate-500">{g.totalWords}</td>
                    <td className="px-4 py-3 text-slate-500">{g.count}</td>
                    <td className="px-4 py-3 text-right whitespace-nowrap">
                      {multi ? (
                        <button onClick={() => openResolve(g)} className="text-xs text-amber-600 dark:text-amber-400 font-medium hover:underline mr-3">Resolve</button>
                      ) : (
                        <button onClick={() => handleEdit(only)} className="text-xs text-dbx-sky hover:underline mr-3">Edit</button>
                      )}
                      <button onClick={() => multi ? handleDeleteScope(g) : handleDelete(only.context_id)} className="text-xs text-red-500 hover:underline">Delete{multi ? ' all' : ''}</button>
                    </td>
                  </tr>
                )
              })}
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
  // When editing, `editingId` is the row's original acronym_id, sent back so the server retires
  // that exact row (the canonical id is derived from scope+abbreviation) -- an edit is a
  // move/in-place update, never a duplicate, regardless of the original row's id scheme.
  const [editingId, setEditingId] = useState(null)
  const [file, setFile] = useState(null)
  const [src, setSrc] = useState('local')
  const [vol, setVol] = useState('')
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

  const blankForm = { scope: '', scope_type: 'schema', abbreviation: '', expansion: '', notes: '' }
  const startAdd = () => { setForm(blankForm); setEditingId(null); setAdding(true); setError(null) }
  const startEdit = (a) => {
    setForm({ scope: a.scope, scope_type: a.scope_type, abbreviation: a.abbreviation, expansion: a.expansion, notes: a.notes || '' })
    setEditingId(a.acronym_id); setAdding(true); setError(null)
  }
  const cancelForm = () => { setAdding(false); setEditingId(null); setForm(blankForm) }

  const save = async () => {
    if (saving) return
    setSaving(true); setError(null)
    // On edit, send the original acronym_id so the server retires that exact row and writes the
    // canonical one -- a move, not a duplicate (the server handles any id-scheme mismatch).
    const payload = editingId ? { ...form, acronym_id: editingId } : form
    const { error: err } = await safeFetch('/api/customer-acronyms', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
    })
    setSaving(false)
    if (err) { setError(err); return }
    cancelForm(); load()
  }
  const del = async (id) => {
    const { error: err } = await safeFetch(`/api/customer-acronyms/${id}`, { method: 'DELETE' })
    if (err) setError(err); else load()
  }
  const upload = async () => {
    const hasSrc = src === 'volume' ? !!vol : !!file
    if (!hasSrc || uploading) return
    setUploading(true); setError(null)
    const fd = new FormData()
    if (src === 'volume') fd.append('volume_path', vol); else fd.append('file', file)
    fd.append('scope_type', upScopeType)
    if (upScope.trim()) fd.append('scope', upScope.trim())
    const { data, error: err } = await safeFetch('/api/customer-acronyms/upload', { method: 'POST', body: fd })
    setUploading(false)
    if (err) { setError(err); return }
    setUpResult(data); setFile(null); setVol(''); setUpScope(''); load()
  }

  return (
    <div className="space-y-5">
      <div className="flex items-start justify-between">
        <p className="text-sm text-slate-500 dark:text-slate-400 max-w-2xl">
          Abbreviations the model should expand in descriptions and classifications. Uploaded as a layer
          on top of the built-in global list, scoped to a <strong>schema</strong> or <strong>catalog</strong>;
          the more-specific scope wins. Upload a structured file (<code>.xlsx</code>, <code>.csv</code>,
          <code> .json</code>, <code>.yaml</code>) with columns/fields like <em>Abbreviation</em> and
          <em> Stands For</em>, or add one below.
        </p>
        <button onClick={() => adding ? cancelForm() : startAdd()} className="btn-primary btn-sm shrink-0">Add Acronym</button>
      </div>

      {error && (
        <div className="card border-l-4 border-l-red-500 px-4 py-3 text-sm flex justify-between">
          <span className="text-red-600 dark:text-red-400">{error}</span>
          <button onClick={() => setError(null)} className="text-slate-400 ml-2">&times;</button>
        </div>
      )}

      <div className="card p-4 space-y-3">
        <label className="text-sm font-medium text-slate-700 dark:text-slate-300">Upload acronyms (.xlsx / .csv / .json / .yaml)</label>
        <div className="flex gap-1 bg-slate-100 dark:bg-dbx-navy-600 rounded-lg p-1 w-fit">
          <button type="button" onClick={() => { setSrc('local'); setVol('') }} className={`px-3 py-1 rounded-md text-xs font-medium ${src === 'local' ? 'bg-white dark:bg-dbx-navy-500 shadow text-dbx-lava' : 'text-slate-500'}`}>From my computer</button>
          <button type="button" onClick={() => { setSrc('volume'); setFile(null) }} className={`px-3 py-1 rounded-md text-xs font-medium ${src === 'volume' ? 'bg-white dark:bg-dbx-navy-500 shadow text-dbx-lava' : 'text-slate-500'}`}>From a volume</button>
        </div>
        {src === 'local' ? (
          <input type="file" accept=".xlsx,.xls,.csv,.json,.yaml,.yml" onChange={e => { setFile(e.target.files[0] || null); setUpResult(null); setError(null) }}
            className="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-medium file:bg-dbx-lava/10 file:text-dbx-lava hover:file:bg-dbx-lava/20" />
        ) : (
          <VolumePicker selected={vol} onSelect={p => { setVol(p); setUpResult(null); setError(null) }} />
        )}
        <p className="text-xs text-slate-400 dark:text-slate-500">Up to {MAX_UPLOAD_MB}&nbsp;MB and 1,000 rows per upload &mdash; split larger lists into separate files.</p>
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
          <span className="text-xs text-slate-400 truncate max-w-xs">{src === 'volume' ? (vol || 'No file selected') : (file ? file.name : 'No file selected')}</span>
          <button onClick={upload} disabled={(src === 'volume' ? !vol : !file) || uploading} className="btn-primary btn-sm disabled:opacity-50">{uploading ? 'Uploading…' : 'Upload'}</button>
        </div>
        {upResult && <div className="text-xs px-3 py-2 rounded-md bg-slate-50 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300">
          Uploaded {upResult.uploaded} acronym(s) to {upResult.scope_type}:{upResult.scope}.
          {upResult.skipped > 0 && <span className="text-amber-600 dark:text-amber-400"> Skipped {upResult.skipped} row(s){upResult.skipped_reasons?.length ? ` (${upResult.skipped_reasons.join('; ')})` : ''}.</span>}
          {upResult.errors?.length > 0 && <span className="text-red-500"> {upResult.errors.length} error(s).</span>}
        </div>}
      </div>

      {adding && (
        <div className="card p-4 grid grid-cols-2 gap-3">
          <div className="col-span-2 text-sm font-semibold text-slate-800 dark:text-slate-100">{editingId ? 'Edit acronym' : 'Add acronym'}</div>
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
            <button onClick={cancelForm} className="btn-ghost btn-sm">Cancel</button>
            <button onClick={save} disabled={saving || !form.scope.trim() || !form.abbreviation.trim() || !form.expansion.trim()}
              className="btn-primary btn-sm disabled:opacity-50">{saving ? 'Saving…' : (editingId ? 'Save changes' : 'Save')}</button>
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
                    <td className="px-4 py-3 text-right whitespace-nowrap">
                      <button onClick={() => startEdit(a)} className="text-xs text-dbx-sky hover:underline mr-3">Edit</button>
                      <button onClick={() => del(a.acronym_id)} className="text-xs text-red-500 hover:underline">Delete</button>
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
  const [src, setSrc] = useState('local')
  const [vol, setVol] = useState('')
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
    const hasSrc = src === 'volume' ? !!vol : !!file
    if (!hasSrc || uploading) return
    setUploading(true); setError(null)
    const fd = new FormData()
    if (src === 'volume') fd.append('volume_path', vol); else fd.append('file', file)
    if (upCatalog.trim()) fd.append('catalog_name', upCatalog.trim())
    if (upSchema.trim()) fd.append('schema_name', upSchema.trim())
    if (upTable.trim()) fd.append('table_name', upTable.trim())
    const { data, error: err } = await safeFetch('/api/customer-overrides/upload', { method: 'POST', body: fd })
    setUploading(false)
    if (err) { setError(err); return }
    setUpResult(data); setFile(null); setVol(''); load()
  }

  return (
    <div className="space-y-5">
      <div className="flex items-start justify-between">
        <p className="text-sm text-slate-500 dark:text-slate-400 max-w-2xl">
          Pre-written column descriptions applied <strong>verbatim</strong> — the model is skipped for these columns.
          Layered under any <code>metadata_overrides.csv</code> (the file wins on conflict). Upload a structured
          file (<code>.xlsx</code>, <code>.csv</code>, <code>.json</code>, <code>.yaml</code>) with
          <em> Table_Name</em>, <em>Column_Name</em>, and <em>Description</em> columns/fields, or add one below.
          Applies to the next <em>descriptions</em> generation run.
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
        <label className="text-sm font-medium text-slate-700 dark:text-slate-300">Upload descriptions (.xlsx / .csv / .json / .yaml)</label>
        <div className="flex gap-1 bg-slate-100 dark:bg-dbx-navy-600 rounded-lg p-1 w-fit">
          <button type="button" onClick={() => { setSrc('local'); setVol('') }} className={`px-3 py-1 rounded-md text-xs font-medium ${src === 'local' ? 'bg-white dark:bg-dbx-navy-500 shadow text-dbx-lava' : 'text-slate-500'}`}>From my computer</button>
          <button type="button" onClick={() => { setSrc('volume'); setFile(null) }} className={`px-3 py-1 rounded-md text-xs font-medium ${src === 'volume' ? 'bg-white dark:bg-dbx-navy-500 shadow text-dbx-lava' : 'text-slate-500'}`}>From a volume</button>
        </div>
        {src === 'local' ? (
          <input type="file" accept=".xlsx,.xls,.csv,.json,.yaml,.yml" onChange={e => { setFile(e.target.files[0] || null); setUpResult(null); setError(null) }}
            className="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-medium file:bg-dbx-lava/10 file:text-dbx-lava hover:file:bg-dbx-lava/20" />
        ) : (
          <VolumePicker selected={vol} onSelect={p => { setVol(p); setUpResult(null); setError(null) }} />
        )}
        <p className="text-xs text-slate-400 dark:text-slate-500">Up to {MAX_UPLOAD_MB}&nbsp;MB and 1,000 rows per upload &mdash; split larger lists into separate files.</p>
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
          <span className="text-xs text-slate-400 truncate max-w-xs">{src === 'volume' ? (vol || 'No file selected') : (file ? file.name : 'No file selected')}</span>
          <button onClick={upload} disabled={(src === 'volume' ? !vol : !file) || uploading} className="btn-primary btn-sm disabled:opacity-50">{uploading ? 'Uploading…' : 'Upload'}</button>
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
