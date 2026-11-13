import React, { useState, useEffect, useMemo, useCallback } from 'react'
import { safeFetch } from '../App'
import { PageHeader } from './ui'
import { useCatalogSchemaTables } from '../hooks/useCatalogSchemaTables'
import useTaskPolling from '../hooks/useTaskPolling'
import simpleMarkdown from '../utils/simpleMarkdown'

// Generate UC Pages. Describe → Curate → Review → Publish. Every step persists to the editable
// page_* tables (the system of record); only Publish writes to Unity Catalog.
// Backend: pages_routes.py + src/dbxmetagen/uc_pages*.py

const LEVEL_STYLE = {
  human: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300',
  governed: 'bg-sky-100 text-sky-800 dark:bg-sky-900/40 dark:text-sky-300',
  inferred: 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300',
  none: 'bg-slate-200 text-slate-700 dark:bg-slate-700 dark:text-slate-300',
}
const LEVEL_HELP = {
  human: 'Backed by human-provided evidence: business context, documents, acronyms, steward-reviewed metadata.',
  governed: 'Backed by governed objects (deployed metric views, confirmed relationships) but no human description.',
  inferred: 'Backed only by AI-generated metadata. Unselected by default: confirm before publishing.',
  none: 'No valid evidence was cited.',
}
const STEPS = ['Describe', 'Curate', 'Review', 'Publish']

const jsonPost = (url, body, method = 'POST') => safeFetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })

function LevelBadge({ level }) {
  return <span title={LEVEL_HELP[level] || ''} className={`text-[10px] font-bold uppercase px-1.5 py-0.5 rounded ${LEVEL_STYLE[level] || LEVEL_STYLE.none}`}>{level || 'none'}</span>
}

function ScoreBadge({ q }) {
  if (q?.score == null) return null
  const s = q.score
  const color = q.blocking?.length ? 'bg-red-100 text-red-700' : s >= 80 ? 'bg-emerald-100 text-emerald-700' : s >= 65 ? 'bg-amber-100 text-amber-700' : 'bg-red-100 text-red-700'
  return <span className={`text-xs font-semibold px-2 py-0.5 rounded ${color}`} title="0-100: deterministic checks + rubric judge">Quality {Math.round(s)}</span>
}

function DomainPicker({ domains, value, onChange, placeholder = 'Search domains…' }) {
  const [q, setQ] = useState('')
  const [open, setOpen] = useState(false)
  const current = domains.find(d => d.name === value)
  const matches = useMemo(() => {
    const f = q.toLowerCase()
    return (f ? domains.filter(d => d.label.toLowerCase().includes(f)) : domains).slice(0, 50)
  }, [q, domains])
  return (
    <div className="relative">
      <input className="input-base" placeholder={current ? current.label : placeholder} value={open ? q : (current?.label || '')}
        onFocus={() => { setOpen(true); setQ('') }} onBlur={() => setTimeout(() => setOpen(false), 150)} onChange={e => setQ(e.target.value)} />
      {open && (
        <div className="absolute z-20 mt-1 w-full max-h-64 overflow-auto card p-1 text-sm">
          <button type="button" className="block w-full text-left px-2 py-1 hover:bg-dbx-oat dark:hover:bg-dbx-navy-600 text-slate-500" onMouseDown={() => onChange('')}>— none —</button>
          {matches.map(d => (
            <button key={d.name} type="button" className="block w-full text-left px-2 py-1 hover:bg-dbx-oat dark:hover:bg-dbx-navy-600" onMouseDown={() => onChange(d.name, d.label)}>{d.label}</button>
          ))}
          {domains.length > 50 && !q && <div className="px-2 py-1 text-xs text-slate-400">{domains.length} domains: type to search</div>}
        </div>
      )}
    </div>
  )
}

function Sources({ sources, reload, selected, setSelected }) {
  const [mode, setMode] = useState('upload')
  const [file, setFile] = useState(null)
  const [volumePath, setVolumePath] = useState('')
  const [text, setText] = useState('')
  const [title, setTitle] = useState('')
  const [url, setUrl] = useState('')
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState(null)
  const add = async () => {
    setBusy(true); setErr(null)
    const fd = new FormData()
    if (mode === 'upload' && file) fd.append('file', file)
    if (mode === 'volume') fd.append('volume_path', volumePath)
    if (mode === 'paste') fd.append('text', text)
    fd.append('title', title); fd.append('url', url)
    const { error } = await safeFetch('/api/pages/sources', { method: 'POST', body: fd })
    setBusy(false)
    if (error) { setErr(error); return }
    setFile(null); setVolumePath(''); setText(''); setTitle(''); setUrl(''); reload()
  }
  return (
    <div className="space-y-2">
      <p className="text-xs text-slate-500 dark:text-slate-400">
        Documents are the strongest evidence a Page can have: glossaries, data dictionaries, policy or SOP docs. They are split by heading so each
        statement can cite the exact section. Customer context and acronyms you already maintain are used automatically.
      </p>
      {sources.length > 0 && (
        <div className="border rounded-lg divide-y border-slate-200 dark:border-dbx-navy-400/40 max-h-40 overflow-auto">
          {sources.map(s => (
            <label key={s.source_id} className="flex items-center gap-2 px-2 py-1 text-xs">
              <input type="checkbox" checked={selected === null || selected.includes(s.source_id)}
                onChange={e => {
                  const all = selected === null ? sources.map(x => x.source_id) : selected
                  setSelected(e.target.checked ? [...all, s.source_id] : all.filter(x => x !== s.source_id))
                }} />
              <span className="font-medium flex-1 truncate">{s.title}</span>
              <span className="text-slate-400">{s.chunk_count} sections</span>
              <button type="button" className="text-slate-400 hover:text-red-500" onClick={async () => { await safeFetch(`/api/pages/sources/${s.source_id}`, { method: 'DELETE' }); reload() }}>remove</button>
            </label>
          ))}
        </div>
      )}
      <div className="flex gap-3 text-xs">
        {['upload', 'volume', 'paste'].map(m => (
          <label key={m} className="flex items-center gap-1"><input type="radio" checked={mode === m} onChange={() => setMode(m)} />{{ upload: 'Upload file', volume: 'UC Volume path', paste: 'Paste text' }[m]}</label>
        ))}
      </div>
      {mode === 'upload' && <input type="file" accept=".txt,.md,.pdf,.docx,.pptx,.csv,.xlsx,.json,.yaml,.yml" onChange={e => setFile(e.target.files?.[0] || null)} className="text-xs" />}
      {mode === 'volume' && <input className="input-base font-mono text-xs" placeholder="/Volumes/catalog/schema/volume/glossary.pdf" value={volumePath} onChange={e => setVolumePath(e.target.value)} />}
      {mode === 'paste' && <textarea className="input-base text-xs" rows={4} value={text} onChange={e => setText(e.target.value)} placeholder="Paste glossary or policy text…" />}
      <div className="grid md:grid-cols-2 gap-2">
        <input className="input-base text-xs" placeholder="Title (optional)" value={title} onChange={e => setTitle(e.target.value)} />
        <input className="input-base text-xs" placeholder="Source URL (optional; becomes a Source link on the Page)" value={url} onChange={e => setUrl(e.target.value)} />
      </div>
      <button type="button" className="btn-secondary btn-sm" disabled={busy || (mode === 'upload' ? !file : mode === 'volume' ? !volumePath : !text.trim())} onClick={add}>{busy ? 'Adding…' : 'Add source'}</button>
      {err && <div className="text-xs text-red-500">{err}</div>}
    </div>
  )
}

function Provenance({ draft }) {
  const [rows, setRows] = useState(null)
  useEffect(() => { safeFetch(`/api/pages/drafts/${draft.page_id}/evidence`).then(({ data }) => setRows(data?.evidence || [])) }, [draft.page_id, draft.updated_at])
  if (!rows) return <div className="text-xs text-slate-400">Loading provenance…</div>
  if (!rows.length) return <div className="text-xs text-slate-400">No provenance recorded.</div>
  const bySection = rows.reduce((acc, r) => { (acc[r.section || '(plan)'] ||= []).push(r); return acc }, {})
  return (
    <div className="space-y-3 text-xs">
      {draft.edited_by_user && <div className="text-amber-600">Edited after generation: provenance reflects the generated text.</div>}
      {Object.entries(bySection).map(([sec, items]) => (
        <div key={sec}>
          <div className="font-semibold text-slate-600 dark:text-slate-300 mb-1">{sec}</div>
          {items.map((r, i) => (
            <details key={i} className="ml-2 mb-1">
              <summary className="cursor-pointer">
                <LevelBadge level={r.authority} /> <span className="text-slate-500">{r.label}</span>
                {r.claim && <span className="ml-1 text-slate-700 dark:text-slate-200">: “{r.claim.slice(0, 140)}{r.claim.length > 140 ? '…' : ''}”</span>}
              </summary>
              <pre className="whitespace-pre-wrap bg-slate-50 dark:bg-dbx-navy-600 p-2 rounded mt-1 text-[11px]">{r.excerpt}</pre>
            </details>
          ))}
        </div>
      ))}
    </div>
  )
}

function QualityPanel({ q }) {
  if (!q || q.score == null) return null
  const failed = Object.entries(q.checks || {}).filter(([, c]) => c.applicable && !c.pass)
  const judge = q.judge_detail?.scores || {}
  return (
    <div className="text-xs space-y-1">
      <div>Deterministic {q.deterministic ?? '–'} · Judge {q.judge_score ?? 'n/a'}{q.revision?.attempted && <span className="ml-2 text-slate-500">· revised {q.revision.kept ? `(kept: ${q.revision.score_before} → ${q.revision.score_after})` : '(original kept)'}</span>}</div>
      {failed.length > 0 && <ul className="list-disc ml-4 text-amber-700 dark:text-amber-400">{failed.map(([k, c]) => <li key={k}>{k.replaceAll('_', ' ')}{c.detail ? `: ${c.detail}` : ''}</li>)}</ul>}
      {Object.keys(judge).length > 0 && <div className="text-slate-500">{Object.entries(judge).filter(([, v]) => v != null).map(([k, v]) => `${k.replaceAll('_', ' ')} ${v}/5`).join(' · ')}</div>}
      {q.judge_detail?.unsupported_claims?.length > 0 && (
        <details><summary className="cursor-pointer text-red-600">Reviewer flagged {q.judge_detail.unsupported_claims.length} statement(s)</summary>
          <ul className="list-disc ml-4">{q.judge_detail.unsupported_claims.map((c, i) => <li key={i}>{c}</li>)}</ul></details>
      )}
    </div>
  )
}

function DraftEditor({ draft, domains, onSaved }) {
  const [d, setD] = useState(draft)
  const [tab, setTab] = useState('edit')
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState(null)
  useEffect(() => setD(draft), [draft])
  const dirty = ['display_name', 'description', 'body', 'domain'].some(k => d[k] !== draft[k]) || (d.synonyms || []).join('|') !== (draft.synonyms || []).join('|')
  const save = async () => {
    setSaving(true); setErr(null)
    const { data, error } = await jsonPost(`/api/pages/drafts/${d.page_id}`, { display_name: d.display_name, description: d.description, body: d.body, synonyms: d.synonyms, domain: d.domain || null, domain_label: d.domain_label || null }, 'PATCH')
    setSaving(false)
    if (error) { setErr(error); return }
    onSaved(data.draft)
  }
  return (
    <div className="card p-4 space-y-3">
      <div className="flex items-center gap-2 flex-wrap">
        <input className="input-base font-semibold flex-1 min-w-[12rem]" maxLength={200} value={d.display_name} onChange={e => setD({ ...d, display_name: e.target.value })} />
        <LevelBadge level={d.evidence_level} /><ScoreBadge q={d.quality} />
        {d.uc_page_name && <span className="text-[10px] uppercase font-bold px-1.5 py-0.5 rounded bg-slate-200 dark:bg-slate-700">{d.status === 'edited_in_uc' ? 'edited in UC' : d.needs_republish ? 'changed since publish' : `in UC (${d.uc_state || 'DRAFT'})`}</span>}
      </div>
      <textarea className="input-base text-sm" rows={2} value={d.description || ''} onChange={e => setD({ ...d, description: e.target.value })} placeholder="One-sentence definition" />
      <div className="grid md:grid-cols-2 gap-2">
        <label className="text-xs text-slate-500">Domain <DomainPicker domains={domains} value={d.domain} onChange={(name, label) => setD({ ...d, domain: name, domain_label: label || '' })} /></label>
        <label className="text-xs text-slate-500">Synonyms
          <input className="input-base mt-1" value={(d.synonyms || []).join(', ')} onChange={e => setD({ ...d, synonyms: e.target.value.split(',').map(s => s.trim()).filter(Boolean) })} /></label>
      </div>
      <div className="flex gap-3 text-xs border-b border-slate-200 dark:border-dbx-navy-400/40">
        {['edit', 'preview', 'provenance', 'quality'].map(t => (
          <button key={t} type="button" onClick={() => setTab(t)} className={`pb-1 ${tab === t ? 'border-b-2 border-dbx-lava text-dbx-lava' : 'text-slate-500'}`}>{t[0].toUpperCase() + t.slice(1)}</button>
        ))}
      </div>
      {tab === 'edit' && <textarea className="input-base font-mono text-xs" rows={14} value={d.body || ''} onChange={e => setD({ ...d, body: e.target.value })} />}
      {tab === 'preview' && <div className="prose prose-sm dark:prose-invert max-w-none" dangerouslySetInnerHTML={{ __html: simpleMarkdown(d.body || '') }} />}
      {tab === 'provenance' && <Provenance draft={draft} />}
      {tab === 'quality' && <QualityPanel q={draft.quality} />}
      {(d.related_tables?.length || d.related_metric_views?.length) > 0 && (
        <div className="text-xs text-slate-500">Links: {[...(d.related_tables || []), ...(d.related_metric_views || [])].map(t => <code key={t} className="mr-1">{t}</code>)}</div>
      )}
      <div className="flex items-center gap-2">
        <button type="button" className="btn-primary btn-sm" disabled={!dirty || saving} onClick={save}>{saving ? 'Saving…' : 'Save'}</button>
        {dirty && <button type="button" className="btn-ghost btn-sm" onClick={() => setD(draft)}>Revert</button>}
        {err && <span className="text-xs text-red-500">{err}</span>}
      </div>
    </div>
  )
}

export default function UCPages() {
  const [status, setStatus] = useState(null)
  const [kinds, setKinds] = useState([])
  const [step, setStep] = useState(0)
  const [prompt, setPrompt] = useState('')
  const [audience, setAudience] = useState('')
  const [domain, setDomain] = useState('')
  const [enabledKinds, setEnabledKinds] = useState(null)
  const [dataProducts, setDataProducts] = useState(false)
  const [maxCand, setMaxCand] = useState(15)
  const [picked, setPicked] = useState([])
  const [sources, setSources] = useState([])
  const [sourceSel, setSourceSel] = useState(null)
  const [busy, setBusy] = useState('')
  const [error, setError] = useState(null)
  const [runId, setRunId] = useState('')
  const [runs, setRuns] = useState([])
  const [drafts, setDrafts] = useState([])
  const [summary, setSummary] = useState(null)
  const [warnings, setWarnings] = useState([])
  const [publishState, setPublishState] = useState('DRAFT')
  const [confirm, setConfirm] = useState(null)
  const [pubResults, setPubResults] = useState(null)
  const scope = useCatalogSchemaTables('', '', { kbOnly: true })
  const domains = status?.domains || []

  const loadStatus = (refresh) => safeFetch(`/api/pages/status${refresh ? '?refresh=true' : ''}`).then(({ data, error }) => setStatus(data || { available: false, reason: error }))
  const loadSources = useCallback(() => safeFetch('/api/pages/sources').then(({ data }) => setSources(data?.sources || [])), [])
  const loadRuns = useCallback(() => safeFetch('/api/pages/runs').then(({ data }) => setRuns(data?.runs || [])), [])
  const loadDrafts = useCallback(async (rid) => {
    if (!rid) return
    const { data, error } = await safeFetch(`/api/pages/drafts?run_id=${encodeURIComponent(rid)}`)
    if (error) setError(error); else setDrafts(data.drafts)
  }, [])
  useEffect(() => {
    loadStatus(false)
    safeFetch('/api/pages/kinds').then(({ data }) => { setKinds(data?.kinds || []); setEnabledKinds((data?.kinds || []).filter(k => k.default).map(k => k.key)) })
  }, [])
  useEffect(() => { if (status?.enabled) { loadSources(); loadRuns() } }, [status?.enabled, loadSources, loadRuns])
  useEffect(() => { setPicked([]) }, [scope.catalog, scope.schema])

  const poll = useTaskPolling({
    onDone: (t) => { setBusy(''); if (t.result?.errors?.length) setError(`${t.result.errors.length} page(s) failed: ${t.result.errors.map(e => e.display_name).join(', ')}`); loadDrafts(runId); setStep(2) },
    onError: (e) => { setBusy(''); setError(e) },
    onStage: () => {},
    interval: 3000,
  })

  const plan = async () => {
    setBusy('Gathering evidence and planning Pages…'); setError(null); setPubResults(null)
    const { data, error } = await jsonPost('/api/pages/plan', {
      prompt, catalog: scope.catalog, schema_name: scope.schema, tables: picked.map(t => `${scope.catalog}.${scope.schema}.${t}`),
      domain, kinds: enabledKinds, include_data_products: dataProducts, source_ids: sourceSel, max_candidates: maxCand, audience,
    })
    setBusy('')
    if (error) { setError(error); return }
    setRunId(data.run_id); setDrafts(data.candidates); setSummary(data.context_summary); setWarnings(data.warnings || []); setStep(1); loadRuns()
  }

  const patch = async (id, body) => {
    const { data, error } = await jsonPost(`/api/pages/drafts/${id}`, body, 'PATCH')
    if (error) { setError(error); return }
    setDrafts(ds => ds.map(x => x.page_id === id ? data.draft : x))
  }
  const selected = drafts.filter(d => d.selected && d.status !== 'discarded')
  const written = drafts.filter(d => d.body && d.status !== 'discarded')

  const write = async (ids) => {
    setError(null); setBusy(`Writing and evaluating ${ids.length} Page(s)…`)
    const { data, error } = await jsonPost('/api/pages/write', { page_ids: ids })
    if (error) { setBusy(''); setError(error); return }
    poll.start(`/api/pages/tasks/${data.task_id}`)
  }
  const reevaluate = async (ids) => {
    setBusy('Re-evaluating…')
    const { data, error } = await jsonPost('/api/pages/evaluate', { page_ids: ids })
    if (error) { setBusy(''); setError(error); return }
    poll.start(`/api/pages/tasks/${data.task_id}`)
  }
  const merge = async () => {
    const [keep, ...rest] = drafts.filter(d => d._mergePick)
    if (!keep || !rest.length) return
    const { error } = await jsonPost('/api/pages/drafts/merge', { keep_id: keep.page_id, merge_ids: rest.map(r => r.page_id) })
    if (error) setError(error); else loadDrafts(runId)
  }
  const discard = async (ids) => { await jsonPost('/api/pages/drafts/discard', { page_ids: ids }); loadDrafts(runId) }
  const publish = async (ids, force = false) => {
    setConfirm(null); setBusy('Publishing to Unity Catalog…'); setError(null)
    const { data, error } = await jsonPost('/api/pages/publish', { page_ids: ids, state: publishState, force })
    setBusy('')
    if (error) { setError(error); return }
    setPubResults(data); loadDrafts(runId); loadRuns()
  }

  if (status && !status.enabled) {
    return (<div className="space-y-4"><PageHeader title="Generate UC Pages" subtitle="Governed business definitions from your metadata and documents" />
      <div className="card p-5 text-sm">This tool is turned off. Set the bundle variable <code>enable_uc_pages=true</code> and redeploy the app.</div></div>)
  }

  return (
    <div className="space-y-5">
      <PageHeader title="Generate UC Pages"
        subtitle="A Page is the governed definition of one business concept: an entity, a term, a metric's meaning, a rule, or a process. Drafts are grounded in your business context, documents and metadata, with every statement traceable to its evidence." />
      {status && !status.available && (
        <div className="card p-4 border-l-4 border-l-amber-500 text-sm flex gap-3"><div className="flex-1">{status.reason}</div><button type="button" className="btn-ghost btn-sm" onClick={() => loadStatus(true)}>Re-check</button></div>
      )}
      <div className="flex items-center gap-2 text-sm">
        {STEPS.map((s, i) => (
          <button key={s} type="button" disabled={i > 0 && !runId} onClick={() => setStep(i)}
            className={`px-3 py-1 rounded-full ${step === i ? 'bg-dbx-lava text-white' : 'bg-slate-100 dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300'} disabled:opacity-40`}>{i + 1}. {s}</button>
        ))}
        <div className="flex-1" />
        {runs.length > 0 && (
          <select className="select-base w-auto text-xs" value={runId} onChange={e => { setRunId(e.target.value); loadDrafts(e.target.value); setStep(1) }}>
            <option value="">Open a previous run…</option>
            {runs.map(r => <option key={r.run_id} value={r.run_id}>{(r.prompt || '').slice(0, 50)} · {r.pages} pages{r.published > 0 ? `, ${r.published} published` : ''}</option>)}
          </select>
        )}
      </div>
      {busy && <div className="text-sm text-slate-500 animate-pulse">{busy}</div>}
      {error && <div className="text-sm text-red-500">{error}</div>}

      {step === 0 && (
        <div className="card p-5 space-y-4">
          <label className="block text-sm font-medium">What Pages do you need?
            <textarea className="input-base mt-1" rows={3} value={prompt} onChange={e => setPrompt(e.target.value)}
              placeholder="e.g. The core clinical concepts analysts need in this EHR data, including allergies, encounters and the rules for interpreting them." />
          </label>
          <div className="grid md:grid-cols-3 gap-3">
            <label className="text-xs text-slate-500">Catalog
              <select className="select-base mt-1" value={scope.catalog} onChange={e => scope.setCatalog(e.target.value)}><option value="">— choose —</option>{scope.catalogs.map(c => <option key={c}>{c}</option>)}</select></label>
            <label className="text-xs text-slate-500">Schema (optional)
              <select className="select-base mt-1" value={scope.schema} onChange={e => scope.setSchema(e.target.value)} disabled={!scope.catalog}><option value="">All schemas</option>{scope.schemas.map(s => <option key={s}>{s}</option>)}</select></label>
            <label className="text-xs text-slate-500">Domain for these Pages<DomainPicker domains={domains} value={domain} onChange={n => setDomain(n)} placeholder="Search or leave blank to suggest per Page" /></label>
          </div>
          {scope.schema && scope.tables.length > 0 && (
            <details className="text-xs"><summary className="cursor-pointer text-slate-500">Limit to specific tables ({picked.length || 'all'} of {scope.tables.length})</summary>
              <div className="grid md:grid-cols-3 gap-1 mt-2 max-h-40 overflow-auto">{scope.tables.map(t => (
                <label key={t} className="font-mono flex gap-1 items-center"><input type="checkbox" checked={picked.includes(t)} onChange={e => setPicked(e.target.checked ? [...picked, t] : picked.filter(x => x !== t))} />{t}</label>))}</div>
            </details>
          )}
          <div>
            <div className="text-sm font-medium mb-1">Kinds of Pages</div>
            <div className="grid md:grid-cols-2 gap-2">
              {kinds.filter(k => k.key !== 'data_product').map(k => (
                <label key={k.key} className="text-xs flex gap-2 items-start"><input type="checkbox" className="mt-0.5" checked={enabledKinds?.includes(k.key)} onChange={e => setEnabledKinds(e.target.checked ? [...enabledKinds, k.key] : enabledKinds.filter(x => x !== k.key))} />
                  <span><b>{k.label}</b>: <span className="text-slate-500">{k.help}</span></span></label>
              ))}
            </div>
            {kinds.find(k => k.key === 'data_product') && (
              <label className="mt-2 text-xs flex gap-2 items-start p-2 rounded bg-slate-50 dark:bg-dbx-navy-600">
                <input type="checkbox" className="mt-0.5" checked={dataProducts} onChange={e => setDataProducts(e.target.checked)} />
                <span><b>Also allow data product overview Pages (opt-in)</b>: <span className="text-slate-500">{kinds.find(k => k.key === 'data_product').help}</span></span>
              </label>
            )}
          </div>
          <details className="text-sm" open={sources.length === 0}>
            <summary className="cursor-pointer font-medium">Source documents ({sources.length})</summary>
            <div className="mt-2"><Sources sources={sources} reload={loadSources} selected={sourceSel} setSelected={setSourceSel} /></div>
          </details>
          <details className="text-xs"><summary className="cursor-pointer text-slate-500">Advanced</summary>
            <div className="grid md:grid-cols-2 gap-3 mt-2">
              <label className="text-slate-500">Audience<input className="input-base mt-1" value={audience} onChange={e => setAudience(e.target.value)} placeholder="e.g. analysts new to clinical data" /></label>
              <label className="text-slate-500">Max candidates<input type="number" min={1} max={40} className="input-base mt-1" value={maxCand} onChange={e => setMaxCand(Number(e.target.value) || 15)} /></label>
            </div>
          </details>
          <button type="button" className="btn-primary btn-md" disabled={!!busy || !status?.available || !scope.catalog || prompt.trim().split(/\s+/).length < 3} onClick={plan}>Plan Pages</button>
        </div>
      )}

      {step === 1 && (
        <div className="space-y-3">
          {summary && (
            <div className="text-xs text-slate-500">Evidence: {Object.entries(summary.evidence_by_authority || {}).map(([k, v]) => `${v} ${k}`).join(' · ')} across {summary.tables?.length || 0} tables{summary.source_chunks ? `, ${summary.source_chunks} document sections` : ''}.</div>
          )}
          {warnings.map((w, i) => <div key={i} className="text-xs text-amber-600">{w}</div>)}
          <div className="text-xs text-slate-500">Pick the concepts to write. Candidates backed only by AI-generated metadata, table-shaped candidates, and duplicates start unselected.
            Tick two or more in the Merge column and click Merge if they are the same concept; the first ticked is kept.</div>
          <div className="card overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="text-xs text-slate-500 text-left"><tr><th className="p-2">Write</th><th className="p-2">Concept</th><th className="p-2">Kind</th><th className="p-2">Evidence</th><th className="p-2">Domain</th><th className="p-2">Flags</th><th className="p-2">Merge</th><th className="p-2"></th></tr></thead>
              <tbody>
                {drafts.filter(d => d.status !== 'discarded').map(d => (
                  <tr key={d.page_id} className="border-t border-slate-100 dark:border-dbx-navy-500 align-top">
                    <td className="p-2"><input type="checkbox" checked={!!d.selected} onChange={e => patch(d.page_id, { selected: e.target.checked })} /></td>
                    <td className="p-2"><div className="font-medium">{d.display_name}{d.body && <span className="ml-1 text-[10px] text-emerald-600">written</span>}</div><div className="text-xs text-slate-500 max-w-md">{d.description}</div>
                      {d.synonyms?.length > 0 && <div className="text-[11px] text-slate-400">aka {d.synonyms.join(', ')}</div>}</td>
                    <td className="p-2"><select className="select-base text-xs w-auto" value={d.kind} onChange={e => patch(d.page_id, { kind: e.target.value })}>{kinds.filter(k => k.key !== 'data_product' || dataProducts || d.kind === 'data_product').map(k => <option key={k.key} value={k.key}>{k.label.replace(' (opt-in)', '')}</option>)}</select></td>
                    <td className="p-2"><LevelBadge level={d.evidence_level} /></td>
                    <td className="p-2 text-xs min-w-[10rem]"><DomainPicker domains={domains} value={d.domain} onChange={(n, l) => patch(d.page_id, { domain: n, domain_label: l || '' })} /></td>
                    <td className="p-2 text-[11px] max-w-xs">{(d.flags || []).map((f, i) => <div key={i} title={f.message} className="text-amber-700 dark:text-amber-400">• {f.message}</div>)}</td>
                    <td className="p-2"><input type="checkbox" checked={!!d._mergePick} onChange={e => setDrafts(ds => ds.map(x => x.page_id === d.page_id ? { ...x, _mergePick: e.target.checked } : x))} /></td>
                    <td className="p-2"><button type="button" className="text-xs text-slate-400 hover:text-red-500" onClick={() => discard([d.page_id])}>discard</button></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="flex gap-2 items-center">
            <button type="button" className="btn-primary btn-md" disabled={!!busy || !selected.length} onClick={() => write(selected.map(d => d.page_id))}>Write {selected.length} selected</button>
            <button type="button" className="btn-ghost btn-sm" disabled={drafts.filter(d => d._mergePick).length < 2} onClick={merge}>Merge ticked</button>
            {written.length > 0 && <button type="button" className="btn-ghost btn-sm" onClick={() => setStep(2)}>Review {written.length} written →</button>}
          </div>
        </div>
      )}

      {step === 2 && (
        <div className="space-y-4">
          <div className="text-xs text-slate-500">Edit anything: this table is your system of record. Each Page shows its quality score (deterministic checks plus an independent reviewer model) and the evidence behind every statement.</div>
          {written.length === 0 && <div className="text-sm text-slate-500">No written Pages yet. Select candidates and click Write.</div>}
          {written.map(d => <DraftEditor key={d.page_id} draft={d} domains={domains} onSaved={nd => setDrafts(ds => ds.map(x => x.page_id === nd.page_id ? nd : x))} />)}
          {written.length > 0 && (
            <div className="flex gap-2">
              <button type="button" className="btn-ghost btn-sm" disabled={!!busy} onClick={() => reevaluate(written.map(d => d.page_id))}>Re-evaluate all</button>
              <button type="button" className="btn-primary btn-sm" onClick={() => setStep(3)}>Continue to publish →</button>
            </div>
          )}
        </div>
      )}

      {step === 3 && (
        <div className="card p-5 space-y-3">
          <div className="text-sm">Publishing creates or updates Pages in Unity Catalog and links them to their tables, metric views, related Pages and source URLs. It runs as <b>{status?.identity}</b>.</div>
          <table className="min-w-full text-sm">
            <tbody>{written.map(d => (
              <tr key={d.page_id} className="border-t border-slate-100 dark:border-dbx-navy-500">
                <td className="p-2"><input type="checkbox" checked={d._pub ?? (!d.quality?.blocking?.length && !!d.domain)} onChange={e => setDrafts(ds => ds.map(x => x.page_id === d.page_id ? { ...x, _pub: e.target.checked } : x))} /></td>
                <td className="p-2">{d.display_name}</td><td className="p-2"><ScoreBadge q={d.quality} /></td><td className="p-2"><LevelBadge level={d.evidence_level} /></td>
                <td className="p-2 text-xs">{!d.domain ? <span className="text-red-500">needs a domain</span> : d.domain_label || d.domain}</td>
                <td className="p-2 text-xs text-slate-500">{d.uc_page_name ? (d.needs_republish ? 'will update' : 'up to date') : 'will create'}</td>
              </tr>))}</tbody>
          </table>
          <div className="flex items-center gap-2">
            <select className="select-base w-auto" value={publishState} onChange={e => setPublishState(e.target.value)}>
              <option value="DRAFT">As Draft (recommended: visible only to you in Genie until a curator publishes)</option>
              <option value="PUBLISHED">As Published (visible in every Genie One conversation)</option>
            </select>
            <button type="button" className="btn-primary btn-md" disabled={!!busy}
              onClick={() => setConfirm(written.filter(d => d._pub ?? (!d.quality?.blocking?.length && !!d.domain)).map(d => d.page_id))}>Publish…</button>
          </div>
          {pubResults && (
            <div className="text-sm space-y-1">
              <div>{pubResults.ok} published, {pubResults.partial} with link issues, {pubResults.edited_in_uc} skipped (edited in UC), {pubResults.failed} failed.</div>
              {pubResults.results.filter(r => r.status !== 'ok').map(r => (
                <div key={r.page_id} className="text-xs text-amber-700">{r.display_name}: {r.error || r.link_errors?.join('; ')}
                  {r.status === 'edited_in_uc' && <button type="button" className="ml-2 underline" onClick={() => publish([r.page_id], true)}>overwrite the UC version</button>}</div>
              ))}
            </div>
          )}
        </div>
      )}

      {confirm && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50" role="dialog" aria-modal="true">
          <div className="card p-5 max-w-md space-y-3">
            <h3 className="font-semibold">Publish {confirm.length} Page(s) to Unity Catalog as {publishState}?</h3>
            <p className="text-sm text-slate-600 dark:text-slate-300">This writes to your account's UC Pages as {status?.identity}. Pages edited in UC since dbxmetagen last published them are skipped, not overwritten.</p>
            <div className="flex justify-end gap-2"><button type="button" className="btn-ghost btn-sm" onClick={() => setConfirm(null)}>Cancel</button><button type="button" className="btn-primary btn-sm" onClick={() => publish(confirm)}>Publish</button></div>
          </div>
        </div>
      )}
    </div>
  )
}
