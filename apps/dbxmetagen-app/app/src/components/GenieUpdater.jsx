import React, { useState, useEffect } from 'react'
import { safeFetchObj, ErrorBanner } from '../App'
import { PageHeader } from './ui'

function uuid() { return crypto.randomUUID?.() || Math.random().toString(36).slice(2) }

function descToStr(d) {
  if (!d) return ''
  if (Array.isArray(d)) return d.join(', ')
  if (typeof d === 'string') return d
  return String(d)
}

function strToDesc(v) { return v ? [v] : null }

function SectionHeader({ title, count, defaultOpen = true, children }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="card overflow-hidden">
      <button onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-5 py-3 bg-dbx-oat dark:bg-slate-700/50 hover:bg-dbx-oat-dark dark:hover:bg-slate-700 transition-colors">
        <span className="text-sm font-medium text-slate-700 dark:text-slate-300">
          {title}{count != null && <span className="ml-1.5 text-xs text-slate-400">({count})</span>}
        </span>
        <span className={`text-xs transition-transform ${open ? 'rotate-90' : ''}`}>&#9654;</span>
      </button>
      {open && <div className="p-5 space-y-3">{children}</div>}
    </div>
  )
}

function AIAssistButton({ section, tableIds, existingItems, onResult }) {
  const [loading, setLoading] = useState(false)
  const [prompt, setPrompt] = useState('')
  const [open, setOpen] = useState(false)

  const run = async () => {
    setLoading(true)
    try {
      const res = await fetch('/api/genie/update-assist', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ section, table_identifiers: tableIds, existing_items: existingItems, user_prompt: prompt }),
      })
      if (!res.ok) { const b = await res.json().catch(() => ({})); alert(b.detail || 'AI assist failed'); return }
      const data = await res.json()
      onResult(data)
      setOpen(false); setPrompt('')
    } catch (e) { alert(e.message) }
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
  const [deploying, setDeploying] = useState(false)
  const [deployResult, setDeployResult] = useState(null)
  const [deployError, setDeployError] = useState(null)

  const [parseInfo, setParseInfo] = useState(null)

  useEffect(() => {
    if (!spaceId) return
    setLoading(true); setError(null); setParseInfo(null)
    fetch(`/api/genie/spaces/${spaceId}/definition`)
      .then(r => r.ok ? r.json() : r.json().then(b => Promise.reject(b.detail || `Error ${r.status}`)))
      .then(data => {
        setTracked(data.tracked)
        setVersion(data.version || 1)
        setTitle(data.title || '')
        setDescription(data.description || '')

        let ss = data.serialized_space
        if (typeof ss === 'string') { try { ss = JSON.parse(ss) } catch { ss = {} } }
        if (typeof ss === 'string') { try { ss = JSON.parse(ss) } catch { ss = {} } }
        if (!ss || typeof ss !== 'object') ss = {}
        setRawJson(ss)

        const info = { ssType: typeof data.serialized_space, keys: Object.keys(ss), ...(data._debug || {}) }

        try {
          const ds = ss.data_sources || {}
          const t = (ds.tables || []).map(t => ({ identifier: t.identifier || '', description: t.description, _key: uuid() }))
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
      })
      .catch(e => setError(typeof e === 'string' ? e : e.message || 'Failed to load'))
      .finally(() => setLoading(false))
  }, [spaceId])

  const tableIds = tables.map(t => t.identifier).filter(Boolean)

  const assemble = () => ({
    data_sources: {
      tables: tables.map(t => ({ identifier: t.identifier, description: t.description })).filter(t => t.identifier),
      metric_views: metricViews.map(m => ({ identifier: m.identifier, description: m.description })).filter(m => m.identifier),
    },
    instructions: {
      text: textInstructions || undefined,
      example_sql: exampleSqls.map(e => ({ question: e.question, sql: e.sql })).filter(e => e.question && e.sql),
      join_specs: joinSpecs.map(j => ({ left: { identifier: j.left }, right: { identifier: j.right }, sql: j.sql ? j.sql.split(/\s+AND\s+/i) : [] })).filter(j => j.left && j.right),
      sql_snippets: {
        measures: measures.map(m => ({ alias: m.alias, display_name: m.display_name || m.alias, sql: m.sql ? [m.sql] : [], synonyms: m.synonyms ? m.synonyms.split(',').map(s => s.trim()).filter(Boolean) : [], description: m.description })).filter(m => m.alias && m.sql.length),
        filters: filters.map(f => ({ display_name: f.display_name, sql: f.sql ? [f.sql] : [] })).filter(f => f.display_name && f.sql.length),
        expressions: expressions.map(x => ({ alias: x.alias, display_name: x.display_name || x.alias, sql: x.sql ? [x.sql] : [], synonyms: x.synonyms ? x.synonyms.split(',').map(s => s.trim()).filter(Boolean) : [] })).filter(x => x.alias && x.sql.length),
      },
    },
    sample_questions: sampleQuestions.map(q => q.text).filter(Boolean),
    description: description || undefined,
  })

  const deploy = async (asNew) => {
    if (!title.trim()) { setDeployError('Enter a title'); return }
    setDeploying(true); setDeployError(null); setDeployResult(null)
    try {
      const body = { title: title.trim(), description: description || undefined, serialized_space: assemble() }
      if (!asNew) body.space_id = spaceId
      const res = await fetch('/api/genie/create', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      if (!res.ok) { const b = await res.json().catch(() => ({})); setDeployError(b.detail || `Error ${res.status}`); return }
      const result = await res.json()
      setDeployResult(result)
    } catch (e) { setDeployError(e.message) }
    finally { setDeploying(false) }
  }

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
        actions={<button onClick={onBack} className="text-xs px-3 py-1.5 rounded border border-slate-300 dark:border-slate-600 text-slate-600 dark:text-slate-300 hover:bg-dbx-oat dark:hover:bg-slate-700">Back to Builder</button>}
      />
      <ErrorBanner error={error} />

      {/* Parse diagnostic */}
      {parseInfo && (
        <div className="card p-3 text-xs space-y-1">
          <div className="flex flex-wrap gap-3 text-slate-600 dark:text-slate-400">
            {parseInfo.source && <span>source: <b>{parseInfo.source}</b></span>}
            <span>ss type: <b>{parseInfo.ssType}</b></span>
            <span>keys: <b>{parseInfo.keys?.join(', ') || 'none'}</b></span>
            <span>tables: <b>{parseInfo.tables ?? '-'}</b></span>
            <span>MVs: <b>{parseInfo.metricViews ?? '-'}</b></span>
            <span>instructions: <b>{parseInfo.textInstructions ?? '-'}</b> chars</span>
            <span>example SQL: <b>{parseInfo.exampleSqls ?? '-'}</b></span>
            <span>joins: <b>{parseInfo.joinSpecs ?? '-'}</b></span>
            <span>measures: <b>{parseInfo.measures ?? '-'}</b></span>
            <span>filters: <b>{parseInfo.filters ?? '-'}</b></span>
            <span>expressions: <b>{parseInfo.expressions ?? '-'}</b></span>
            <span>sample Qs: <b>{parseInfo.sampleQuestions ?? '-'}</b></span>
            {parseInfo.config_json_type && <span>cj_type: <b>{parseInfo.config_json_type}</b></span>}
            {parseInfo.config_json_len != null && <span>cj_len: <b>{parseInfo.config_json_len}</b></span>}
          </div>
          {Object.entries(parseInfo).filter(([k]) => k.endsWith('Err')).map(([k, v]) => (
            <div key={k} className="text-red-500">Parse error ({k}): {v}</div>
          ))}
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

      {/* Raw JSON */}
      <SectionHeader title="Raw Space JSON" count={null} defaultOpen={true}>
        {rawJson ? (
          <pre className="text-xs font-mono bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg p-4 overflow-auto max-h-96 text-slate-700 dark:text-slate-300 whitespace-pre-wrap break-words">
            {JSON.stringify(rawJson, null, 2)}
          </pre>
        ) : <p className="text-xs text-slate-400 italic">No data loaded</p>}
      </SectionHeader>

      {/* Data Sources: Tables */}
      <SectionHeader title="Tables" count={tables.length}>
        <EditableList items={tables} setItems={setTables}
          emptyLabel="No tables" addLabel="Add Table"
          newItem={() => ({ identifier: '', description: null })}
          renderItem={(t, i, update) => (
            <div className="flex gap-2">
              <SmallInput value={t.identifier} onChange={v => update({ ...t, identifier: v })} placeholder="catalog.schema.table" className="flex-1" />
              <SmallInput value={descToStr(t.description)} onChange={v => update({ ...t, description: strToDesc(v) })} placeholder="Description (optional)" className="flex-1" />
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
      <SectionHeader title="Example SQL" count={exampleSqls.length}>
        <EditableList items={exampleSqls} setItems={setExampleSqls}
          emptyLabel="No example SQL pairs" addLabel="Add Example"
          newItem={() => ({ question: '', sql: '' })}
          renderItem={(e, i, update) => (
            <div className="space-y-1">
              <SmallInput value={e.question} onChange={v => update({ ...e, question: v })} placeholder="Question" className="w-full" />
              <textarea value={e.sql} onChange={ev => update({ ...e, sql: ev.target.value })} rows={2} placeholder="SELECT ..."
                className="w-full font-mono text-xs border border-slate-200 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-200" />
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
              <SmallInput value={j.sql} onChange={v => update({ ...j, sql: v })} placeholder="left.col = right.col" />
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
              <textarea value={m.sql} onChange={e => update({ ...m, sql: e.target.value })} rows={1} placeholder="SQL expression"
                className="w-full font-mono text-xs border border-slate-200 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-200" />
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
              <SmallInput value={f.sql} onChange={v => update({ ...f, sql: v })} placeholder="SQL WHERE clause" />
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
              <SmallInput value={x.sql} onChange={v => update({ ...x, sql: v })} placeholder="SQL expression" className="w-full" />
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
          <button onClick={() => deploy(false)} disabled={deploying || !title.trim()}
            className="px-5 py-2.5 bg-emerald-600 text-white text-sm font-medium rounded-lg hover:bg-emerald-700 disabled:opacity-50 transition-colors flex items-center gap-1.5">
            {deploying && <span className="w-3 h-3 border-2 border-white border-t-transparent rounded-full animate-spin" />}
            Update Space
          </button>
          <button onClick={() => deploy(true)} disabled={deploying || !title.trim()}
            className="px-5 py-2.5 bg-dbx-navy text-white text-sm font-medium rounded-lg hover:bg-slate-700 disabled:opacity-50 transition-colors">
            Create as New Space
          </button>
        </div>
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
