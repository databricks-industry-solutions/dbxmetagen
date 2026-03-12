import React, { useState, useEffect, useCallback, useMemo } from 'react'
import { safeFetch, ErrorBanner } from '../App'

function DataTable({ data, maxRows = 100 }) {
  if (!data || data.length === 0) return <p className="text-sm text-slate-400 py-4">No data available.</p>
  const cols = Object.keys(data[0])
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead><tr>{cols.map(c =>
          <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase tracking-wider">{c}</th>
        )}</tr></thead>
        <tbody>
          {data.slice(0, maxRows).map((row, i) => (
            <tr key={i} className="border-b border-slate-100 hover:bg-orange-50/30 transition-colors">
              {cols.map(c => <td key={c} className="px-3 py-2 max-w-xs truncate text-slate-600">{String(row[c] ?? '')}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Review Editor -- scope picker, metadata type filter, inline edit, export
// ---------------------------------------------------------------------------
const META_TYPES = [
  { key: 'comments', label: 'Comments' },
  { key: 'pii', label: 'PII / PHI' },
  { key: 'domain', label: 'Domain' },
  { key: 'ontology', label: 'Ontology' },
  { key: 'fk', label: 'Foreign Keys' },
]

function ReviewEditor() {
  const [catalogs, setCatalogs] = useState([])
  const [schemas, setSchemas] = useState([])
  const [allTables, setAllTables] = useState([])
  const [selectedCatalog, setSelectedCatalog] = useState('')
  const [selectedSchema, setSelectedSchema] = useState('')
  const [scopeMode, setScopeMode] = useState('schema')
  const [selectedTables, setSelectedTables] = useState([])
  const [tableFilter, setTableFilter] = useState('')
  const [activeType, setActiveType] = useState('comments')
  const [reviewData, setReviewData] = useState([])
  const [original, setOriginal] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [expanded, setExpanded] = useState({})
  const [saving, setSaving] = useState(false)
  const [ddlSql, setDdlSql] = useState('')
  const [ddlLoading, setDdlLoading] = useState(false)
  const [ddlApplyResult, setDdlApplyResult] = useState(null)
  const [exportLoading, setExportLoading] = useState(false)
  const [exportResult, setExportResult] = useState(null)
  const [entityTypeOptions, setEntityTypeOptions] = useState([])
  const [entityTypeOverrides, setEntityTypeOverrides] = useState({})
  const [expandedEntities, setExpandedEntities] = useState({})
  const [ontoApplyResults, setOntoApplyResults] = useState({})
  const [ontoApplying, setOntoApplying] = useState({})
  const [selectedFKs, setSelectedFKs] = useState({})
  const [fkApplyResult, setFkApplyResult] = useState(null)
  const [fkApplying, setFkApplying] = useState(false)
  const [expandedFKs, setExpandedFKs] = useState({})

  useEffect(() => { fetch('/api/ontology/entity-type-options').then(r => r.json()).then(d => setEntityTypeOptions(Array.isArray(d) ? d : [])).catch(() => {}) }, [])
  useEffect(() => { fetch('/api/catalogs').then(r => r.json()).then(setCatalogs).catch(() => {}) }, [])
  useEffect(() => {
    if (!selectedCatalog) { setSchemas([]); return }
    fetch(`/api/schemas?catalog=${selectedCatalog}`).then(r => r.json()).then(setSchemas).catch(() => setSchemas([]))
  }, [selectedCatalog])
  useEffect(() => {
    if (!selectedCatalog || !selectedSchema) { setAllTables([]); return }
    fetch(`/api/tables?catalog=${selectedCatalog}&schema=${selectedSchema}`)
      .then(r => r.json()).then(setAllTables).catch(() => setAllTables([]))
  }, [selectedCatalog, selectedSchema])

  const filteredTables = useMemo(() => {
    if (!tableFilter) return allTables
    const f = tableFilter.toLowerCase()
    return allTables.filter(t => t.toLowerCase().includes(f))
  }, [allTables, tableFilter])

  const toggleTable = t => setSelectedTables(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t])

  const loadData = async () => {
    setLoading(true); setError(null); setDdlSql(''); setDdlApplyResult(null); setExportResult(null)
    const body = scopeMode === 'schema'
      ? { schemas: [`${selectedCatalog}.${selectedSchema}`] }
      : { tables: selectedTables.map(t => `${selectedCatalog}.${selectedSchema}.${t}`) }
    try {
      const res = await fetch('/api/metadata/review-combined', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
      })
      const j = await res.json()
      if (!res.ok) throw new Error(j.detail || res.status)
      const tables = j.tables || []
      setReviewData(tables)
      setOriginal(JSON.parse(JSON.stringify(tables)))
      const exp = {}; tables.forEach(t => { exp[t.table_name] = true }); setExpanded(exp)
    } catch (e) { setError(e.message) }
    setLoading(false)
  }

  const onTableChange = (tblIdx, field, value) => {
    setReviewData(prev => prev.map((t, i) => i === tblIdx ? { ...t, [field]: value } : t))
  }
  const onColChange = (tblIdx, colIdx, field, value) => {
    setReviewData(prev => prev.map((t, ti) => ti === tblIdx
      ? { ...t, columns: t.columns.map((c, ci) => ci === colIdx ? { ...c, [field]: value } : c) }
      : t))
  }
  const onTableCheckbox = (tblIdx, field) => {
    setReviewData(prev => prev.map((t, i) => i === tblIdx ? { ...t, [field]: !t[field] } : t))
  }

  const dirtyTables = useMemo(() => reviewData.filter((t, i) => {
    const o = original[i]
    if (!o) return false
    return ['comment', 'domain', 'subdomain', 'has_pii', 'has_phi'].some(f => t[f] !== o[f])
  }), [reviewData, original])

  const dirtyCols = useMemo(() => {
    const list = []
    reviewData.forEach((t, ti) => {
      t.columns?.forEach((c, ci) => {
        const oc = original[ti]?.columns?.[ci]
        if (oc && ['comment', 'classification', 'classification_type'].some(f => c[f] !== oc[f]))
          list.push(c)
      })
    })
    return list
  }, [reviewData, original])

  const totalDirty = dirtyTables.length + dirtyCols.length

  const saveChanges = async () => {
    setSaving(true); setError(null)
    try {
      if (dirtyTables.length) {
        const body = dirtyTables.map(r => ({ table_name: r.table_name, comment: r.comment, domain: r.domain, subdomain: r.subdomain, has_pii: r.has_pii, has_phi: r.has_phi }))
        const res = await fetch('/api/metadata/knowledge-base', { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
        if (!res.ok) { const j = await res.json().catch(() => ({})); throw new Error(j.detail || res.status) }
      }
      if (dirtyCols.length) {
        const body = dirtyCols.map(r => ({ column_id: r.column_id, comment: r.comment, classification: r.classification, classification_type: r.classification_type }))
        const res = await fetch('/api/metadata/column-kb', { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
        if (!res.ok) { const j = await res.json().catch(() => ({})); throw new Error(j.detail || res.status) }
      }
      setOriginal(JSON.parse(JSON.stringify(reviewData)))
    } catch (e) { setError(e.message) }
    setSaving(false)
  }

  const tableNames = useMemo(() => reviewData.map(t => t.table_name), [reviewData])
  const [ddlCategory, setDdlCategory] = useState('comments')
  const [customDomainKey, setCustomDomainKey] = useState('')
  const [customSubdomainKey, setCustomSubdomainKey] = useState('')
  const [customSensitivityKey, setCustomSensitivityKey] = useState('')
  const [customSensitivityTypeKey, setCustomSensitivityTypeKey] = useState('')

  const ddlPayload = (extra = {}) => ({
    scope: 'table', identifiers: tableNames, ddl_type: ddlCategory,
    ...(ddlCategory === 'domain' && customDomainKey ? { domain_tag_key: customDomainKey } : {}),
    ...(ddlCategory === 'domain' && customSubdomainKey ? { subdomain_tag_key: customSubdomainKey } : {}),
    ...(ddlCategory === 'sensitivity' && customSensitivityKey ? { sensitivity_tag_key: customSensitivityKey } : {}),
    ...(ddlCategory === 'sensitivity' && customSensitivityTypeKey ? { sensitivity_type_tag_key: customSensitivityTypeKey } : {}),
    ...extra,
  })

  const generateDdl = async () => {
    setDdlLoading(true); setDdlApplyResult(null)
    const res = await fetch('/api/metadata/generate-ddl', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(ddlPayload()) })
    const j = await res.json().catch(() => ({}))
    setDdlSql(j.sql || j.detail || ''); setDdlLoading(false)
  }
  const applyDdl = async () => {
    setDdlLoading(true); setDdlApplyResult(null)
    const res = await fetch('/api/metadata/apply-ddl', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(ddlPayload()) })
    const j = await res.json().catch(() => ({}))
    const hasErrors = j.errors && j.errors.length > 0
    setDdlApplyResult(hasErrors ? { ok: false, applied: j.applied || 0, errors: j.errors } : { ok: true, applied: j.applied })
    setDdlLoading(false)
  }
  const exportVolume = async (fmt) => {
    setExportLoading(true); setExportResult(null)
    try {
      const res = await fetch('/api/metadata/export-volume', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tables: tableNames, format: fmt, include_columns: true, metadata_type: ddlCategory }) })
      const j = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(j.detail || res.status)
      setExportResult({ ok: true, path: j.path, rows: j.rows })
    } catch (e) { setExportResult({ ok: false, detail: e.message }) }
    setExportLoading(false)
  }

  const show = k => activeType === k
  const chip = 'px-3 py-1 rounded-full text-xs font-medium border cursor-pointer select-none transition-all'
  const chipOn = 'bg-dbx-lava text-white border-red-700'
  const chipOff = 'bg-dbx-oat text-slate-600 border-slate-300 hover:border-slate-400'
  const inp = 'w-full border border-slate-300 rounded px-2 py-1 text-sm focus:ring-2 focus:ring-orange-500'

  const classLabel = (val) => {
    const v = (val ?? '').trim().toLowerCase()
    if (!v) return { text: 'Unclassified', cls: 'bg-slate-100 text-slate-500' }
    if (v === 'none') return { text: 'Public', cls: 'bg-green-100 text-green-700' }
    return { text: val, cls: 'bg-red-100 text-red-700' }
  }

  const isTableDirty = (tblIdx) => {
    const o = original[tblIdx]
    return o && ['comment', 'domain', 'subdomain', 'has_pii', 'has_phi'].some(f => reviewData[tblIdx][f] !== o[f])
  }
  const isColDirty = (tblIdx, colIdx) => {
    const oc = original[tblIdx]?.columns?.[colIdx]
    return oc && ['comment', 'classification', 'classification_type'].some(f => reviewData[tblIdx].columns[colIdx][f] !== oc[f])
  }

  return (
    <div className="space-y-4">
      <ErrorBanner error={error} />

      {/* Scope Picker */}
      <div className="bg-dbx-oat-light rounded-xl border border-slate-200 p-4 shadow-sm space-y-3">
        <h3 className="text-sm font-semibold text-slate-700">Scope</h3>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          <div>
            <label className="block text-xs font-medium text-slate-500 mb-1">Catalog</label>
            <select value={selectedCatalog} onChange={e => { setSelectedCatalog(e.target.value); setSelectedSchema(''); setSelectedTables([]) }}
              className={inp}><option value="">-- select --</option>{catalogs.map(c => <option key={c}>{c}</option>)}</select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-500 mb-1">Schema</label>
            <select value={selectedSchema} onChange={e => { setSelectedSchema(e.target.value); setSelectedTables([]) }}
              className={inp} disabled={!selectedCatalog}><option value="">-- select --</option>{schemas.map(s => <option key={s}>{s}</option>)}</select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-500 mb-1">Mode</label>
            <div className="flex gap-2 mt-1">
              {[['schema', 'Entire Schema'], ['table', 'Pick Tables']].map(([k, l]) =>
                <label key={k} className="inline-flex items-center gap-1 text-xs">
                  <input type="radio" name="scopeMode" checked={scopeMode === k} onChange={() => setScopeMode(k)} className="rounded" />{l}
                </label>)}
            </div>
          </div>
          <div className="flex items-end">
            <button onClick={loadData} disabled={loading || !selectedCatalog || !selectedSchema || (scopeMode === 'table' && !selectedTables.length)}
              className="px-5 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 disabled:opacity-50 shadow-sm w-full">
              {loading ? 'Loading...' : 'Load'}
            </button>
          </div>
        </div>
        {scopeMode === 'table' && selectedSchema && (
          <div>
            <input value={tableFilter} onChange={e => setTableFilter(e.target.value)} placeholder="Filter tables..." className={inp + ' mb-2 max-w-xs'} />
            {filteredTables.length > 0 && (
              <>
                <div className="flex gap-2 mb-1 text-xs">
                  <button onClick={() => setSelectedTables(filteredTables)} className="text-blue-600 hover:underline">Select all ({filteredTables.length})</button>
                  <button onClick={() => setSelectedTables([])} className="text-blue-600 hover:underline">Clear</button>
                  <span className="text-slate-400 ml-auto">{selectedTables.length} selected</span>
                </div>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-1 max-h-36 overflow-y-auto border border-slate-200 rounded-md p-2">
                  {filteredTables.map(t => (
                    <label key={t} className="flex items-center gap-1.5 text-xs cursor-pointer py-0.5">
                      <input type="checkbox" checked={selectedTables.includes(t)} onChange={() => toggleTable(t)} className="rounded" />{t}
                    </label>))}
                </div>
              </>
            )}
          </div>
        )}
      </div>

      {/* Metadata Type Filter */}
      {reviewData.length > 0 && (
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs font-medium text-slate-500">Show:</span>
          {META_TYPES.map(({ key, label }) => (
            <span key={key} onClick={() => setActiveType(key)} className={`${chip} ${activeType === key ? chipOn : chipOff}`}>{label}</span>
          ))}
          {totalDirty > 0 && (
            <button onClick={saveChanges} disabled={saving}
              className="ml-auto px-4 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 shadow-sm disabled:opacity-50">
              {saving ? 'Saving...' : `Save Changes (${totalDirty})`}
            </button>
          )}
        </div>
      )}

      {/* Combined Data View */}
      {reviewData.length > 0 && (
        <div className="space-y-3">
          {reviewData.map((tbl, tblIdx) => (
            <div key={tbl.table_name} className="bg-dbx-oat-light rounded-xl border border-slate-200 shadow-sm overflow-hidden">
              {/* Table header row */}
              <div className={`flex items-center gap-3 px-4 py-2.5 cursor-pointer select-none ${isTableDirty(tblIdx) ? 'bg-amber-50' : 'bg-dbx-oat'}`}
                onClick={() => setExpanded(p => ({ ...p, [tbl.table_name]: !p[tbl.table_name] }))}>
                <span className="text-xs text-slate-400">{expanded[tbl.table_name] ? '\u25BC' : '\u25B6'}</span>
                <span className="font-semibold text-sm text-slate-700">{tbl.table_name}</span>
                <span className="text-xs text-slate-400 ml-auto">{tbl.columns?.length || 0} columns</span>
              </div>

              {expanded[tbl.table_name] && (
                <div className="px-4 pb-4 space-y-3">
                  {/* Table-level fields */}
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 pt-3">
                    {show('comments') && (
                      <div className="col-span-4">
                        <label className="block text-xs font-medium text-slate-500 mb-1">Table Comment</label>
                        <textarea value={tbl.comment ?? ''} onChange={e => onTableChange(tblIdx, 'comment', e.target.value)} rows={2}
                          className={inp + ' resize-y'} />
                      </div>
                    )}
                    {show('domain') && (
                      <>
                        <div className="col-span-2"><label className="block text-xs font-medium text-slate-500 mb-1">Domain</label>
                          <input value={tbl.domain ?? ''} onChange={e => onTableChange(tblIdx, 'domain', e.target.value)} className={inp} /></div>
                        <div className="col-span-2"><label className="block text-xs font-medium text-slate-500 mb-1">Subdomain</label>
                          <input value={tbl.subdomain ?? ''} onChange={e => onTableChange(tblIdx, 'subdomain', e.target.value)} className={inp} /></div>
                      </>
                    )}
                    {show('pii') && (
                      <>
                        <div className="flex items-center gap-2"><input type="checkbox" checked={tbl.has_pii === true} onChange={() => onTableCheckbox(tblIdx, 'has_pii')} className="rounded" />
                          <span className="text-xs text-slate-600">Has PII</span></div>
                        <div className="flex items-center gap-2"><input type="checkbox" checked={tbl.has_phi === true} onChange={() => onTableCheckbox(tblIdx, 'has_phi')} className="rounded" />
                          <span className="text-xs text-slate-600">Has PHI</span></div>
                      </>
                    )}
                  </div>

                  {/* Column rows */}
                  {(show('comments') || show('pii') || show('ontology')) && tbl.columns?.length > 0 && (
                    <div className="overflow-x-auto">
                      <table className="min-w-full text-sm table-fixed">
                        <thead><tr className="bg-dbx-oat/50">
                          <th className="text-left px-3 py-2 text-xs font-semibold text-slate-500 uppercase w-40">Column</th>
                          <th className="text-left px-3 py-2 text-xs font-semibold text-slate-500 uppercase w-24">Type</th>
                          {show('comments') && <th className="text-left px-3 py-2 text-xs font-semibold text-slate-500 uppercase">Comment</th>}
                          {show('pii') && <th className="text-left px-3 py-2 text-xs font-semibold text-slate-500 uppercase">Classification</th>}
                          {show('pii') && <th className="text-left px-3 py-2 text-xs font-semibold text-slate-500 uppercase">Class. Type</th>}
                          {show('ontology') && <th className="text-left px-3 py-2 text-xs font-semibold text-slate-500 uppercase">Entity Types</th>}
                        </tr></thead>
                        <tbody>
                          {tbl.columns.map((col, ci) => (
                            <tr key={col.column_id || ci} className={`border-b border-slate-100 ${isColDirty(tblIdx, ci) ? 'bg-amber-50' : ''} hover:bg-orange-50/30`}>
                              <td className="px-3 py-1.5 text-slate-600 font-mono text-xs truncate">{col.column_name}</td>
                              <td className="px-3 py-1.5 text-slate-400 text-xs truncate">{col.data_type}</td>
                              {show('comments') && <td className="px-2 py-1"><input value={col.comment ?? ''} onChange={e => onColChange(tblIdx, ci, 'comment', e.target.value)} className={inp} /></td>}
                              {show('pii') && <td className="px-2 py-1">
                                <div className="flex items-center gap-1.5">
                                  <input value={col.classification ?? ''} onChange={e => onColChange(tblIdx, ci, 'classification', e.target.value)} className={inp} placeholder="Unclassified" />
                                  {(() => { const b = classLabel(col.classification); return <span className={`whitespace-nowrap text-[10px] px-1.5 py-0.5 rounded-full font-medium ${b.cls}`}>{b.text}</span> })()}
                                </div>
                              </td>}
                              {show('pii') && <td className="px-2 py-1"><input value={col.classification_type ?? ''} onChange={e => onColChange(tblIdx, ci, 'classification_type', e.target.value)} className={inp} /></td>}
                              {show('ontology') && <td className="px-2 py-1">{(() => {
                                const ents = (tbl.ontology_entities || []).filter(e => {
                                  let sc = e.source_columns
                                  if (typeof sc === 'string') { try { sc = JSON.parse(sc) } catch { sc = [] } }
                                  return Array.isArray(sc) && sc.includes(col.column_name)
                                })
                                return ents.length > 0 ? (
                                  <div className="flex flex-wrap gap-1">{ents.map((e, ei) => {
                                    const c = Number(e.confidence ?? 0)
                                    const cls = c <= 0 ? 'bg-red-100 text-red-700' : c < 0.5 ? 'bg-yellow-100 text-yellow-700' : 'bg-green-100 text-green-700'
                                    return <span key={ei} className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${cls}`}>{e.entity_type} ({c.toFixed(2)})</span>
                                  })}</div>
                                ) : <span className="text-[10px] text-slate-300">--</span>
                              })()}</td>}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}

                  {/* Ontology Entities */}
                  {show('ontology') && (
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <h4 className="text-xs font-semibold text-slate-500">Ontology Entities</h4>
                        {Array.isArray(tbl.ontology_entities) && tbl.ontology_entities.filter(e => Number(e.confidence ?? 0) > 0).length > 0 && (
                          <button onClick={async () => {
                            setOntoApplying(p => ({ ...p, [tbl.table_name]: true })); setOntoApplyResults(p => ({ ...p, [tbl.table_name]: null }))
                            const selections = tbl.ontology_entities.filter(e => Number(e.confidence ?? 0) > 0).map(e => {
                              let sc = e.source_columns
                              if (typeof sc === 'string') { try { sc = JSON.parse(sc) } catch {} }
                              return { entity_type: entityTypeOverrides[e.entity_id] || e.entity_type, source_tables: [tbl.table_name], source_columns: Array.isArray(sc) ? sc : [] }
                            })
                            try {
                              const r = await fetch('/api/ontology/apply-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ selections }) })
                              const j = await r.json().catch(() => ({}))
                              setOntoApplyResults(p => ({ ...p, [tbl.table_name]: j }))
                            } catch (err) { setOntoApplyResults(p => ({ ...p, [tbl.table_name]: { error: err.message } })) }
                            setOntoApplying(p => ({ ...p, [tbl.table_name]: false }))
                          }} disabled={ontoApplying[tbl.table_name]} className="text-xs px-2 py-0.5 bg-purple-600 text-white rounded hover:bg-purple-700 disabled:opacity-50">
                            {ontoApplying[tbl.table_name] ? 'Applying...' : 'Apply All Tags (conf > 0)'}
                          </button>
                        )}
                      </div>
                      {ontoApplyResults[tbl.table_name] && (() => {
                        const ar = ontoApplyResults[tbl.table_name]
                        if (ar.error) return <div className="text-xs text-red-600">{String(ar.error)}</div>
                        const tblOk = (ar.results || []).filter(r => r.ok)
                        const tblFail = (ar.results || []).filter(r => !r.ok)
                        const colOk = (ar.column_results || []).filter(r => r.ok)
                        const colFail = (ar.column_results || []).filter(r => !r.ok)
                        const allSql = [...(ar.results || []), ...(ar.column_results || [])].map(r => r.sql).filter(Boolean)
                        const allFail = [...tblFail, ...colFail]
                        return (
                          <div className="text-xs space-y-1">
                            {tblOk.length > 0 && <div className="text-green-600">Table: {tblOk.length} tag(s) verified.{tblOk.map(r => r.verified ? ` [${r.verified}]` : '').join('')}</div>}
                            {colOk.length > 0 && <div className="text-green-600">Columns: {colOk.length} tag(s) verified.{colOk.map(r => ` ${r.column}=[${r.verified}]`).join(',')}</div>}
                            {allFail.length > 0 && allFail.map((r, ri) => (
                              <div key={ri} className="text-red-600">{r.column ? `${r.table}.${r.column}` : r.table}: {r.error || 'unknown error'}</div>
                            ))}
                            {allSql.length > 0 && (
                              <details className="text-slate-500"><summary className="cursor-pointer">SQL details</summary>
                                <pre className="mt-1 text-[10px] bg-slate-100 p-1 rounded overflow-x-auto whitespace-pre-wrap">{allSql.join('\n')}</pre>
                              </details>
                            )}
                            {allFail.some(r => /PERMISSION_DENIED/i.test(r.error || '')) && (
                              <span className="block text-orange-600">Tag permission denied -- try using a custom tag key or check governed tag policies.</span>
                            )}
                          </div>
                        )
                      })()}
                      {!Array.isArray(tbl.ontology_entities) ? (
                        <p className="text-xs text-slate-400 italic">Ontology data not available for this table.</p>
                      ) : tbl.ontology_entities.length === 0 ? (
                        <p className="text-xs text-slate-400 italic">No ontology entities mapped to this table.</p>
                      ) : (
                        <div className="space-y-2">
                          {tbl.ontology_entities.map((e, i) => {
                            const conf = Number(e.confidence ?? 0)
                            const confCls = conf <= 0 ? 'bg-red-100 text-red-700' : conf < 0.5 ? 'bg-yellow-100 text-yellow-700' : 'bg-green-100 text-green-700'
                            const eid = e.entity_id || `${tblIdx}-${i}`
                            const isOpen = expandedEntities[eid]
                            const srcCols = (() => {
                              if (Array.isArray(e.source_columns)) return e.source_columns
                              if (typeof e.source_columns === 'string') { try { const p = JSON.parse(e.source_columns); if (Array.isArray(p)) return p } catch {} }
                              return []
                            })()
                            return (
                              <div key={eid} className="border border-purple-200 rounded-lg bg-purple-50/50">
                                <div className="flex items-center gap-2 px-3 py-1.5 cursor-pointer flex-wrap" onClick={() => setExpandedEntities(p => ({ ...p, [eid]: !p[eid] }))}>
                                  <span className="text-xs text-slate-400">{isOpen ? '\u25BC' : '\u25B6'}</span>
                                  {e.entity_id ? (
                                    <select value={entityTypeOverrides[e.entity_id] || e.entity_type}
                                      onClick={ev => ev.stopPropagation()}
                                      onChange={ev => {
                                        const val = ev.target.value
                                        setEntityTypeOverrides(p => ({ ...p, [e.entity_id]: val }))
                                        fetch('/api/ontology/update-entity-type', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                                          body: JSON.stringify({ entity_id: e.entity_id, new_entity_type: val }) }).catch(() => {})
                                      }}
                                      className="text-xs border border-purple-300 rounded px-1.5 py-0.5 bg-white">
                                      {entityTypeOptions.map(t => <option key={t} value={t}>{t}</option>)}
                                      {!entityTypeOptions.includes(e.entity_type) && <option value={e.entity_type}>{e.entity_type}</option>}
                                    </select>
                                  ) : (
                                    <span className="text-xs font-medium text-purple-700">{e.entity_type}</span>
                                  )}
                                  <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${confCls}`}
                                    title="Negative confidence means the AI validator rejected this entity mapping. The more negative, the stronger the rejection.">
                                    {conf.toFixed(2)}
                                  </span>
                                  {e.validated !== undefined && (
                                    <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${e.validated === true || e.validated === 'true' ? 'bg-blue-100 text-blue-700' : 'bg-slate-100 text-slate-500'}`}>
                                      {e.validated === true || e.validated === 'true' ? 'validated' : 'unvalidated'}
                                    </span>
                                  )}
                                  {srcCols.length > 0 && (
                                    <span className="text-[10px] text-slate-500">
                                      {srcCols.map((c, ci) => <code key={ci} className="bg-white/80 border border-slate-200 rounded px-1 py-0.5 text-slate-600 mr-1">{c}</code>)}
                                    </span>
                                  )}
                                  <button onClick={async (ev) => {
                                    ev.stopPropagation()
                                    const et = entityTypeOverrides[e.entity_id] || e.entity_type
                                    try {
                                      const r = await fetch('/api/ontology/apply-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({ selections: [{ entity_type: et, source_tables: [tbl.table_name], source_columns: srcCols }] }) })
                                      const j = await r.json().catch(() => ({}))
                                      setOntoApplyResults(p => ({ ...p, [tbl.table_name]: j }))
                                    } catch (err) { setOntoApplyResults(p => ({ ...p, [tbl.table_name]: { error: err.message } })) }
                                  }} className="ml-auto text-[10px] px-1.5 py-0.5 bg-purple-600 text-white rounded hover:bg-purple-700" title="Apply entity_type tag to table and columns">
                                    Apply Tag
                                  </button>
                                </div>
                                {isOpen && e.validation_notes && (
                                  <div className="px-3 pb-2 border-t border-purple-200 mt-0">
                                    <div className="mt-1.5">
                                      <span className="text-[10px] font-semibold text-slate-500">Validation Notes:</span>
                                      <p className="text-xs text-slate-600 italic bg-white/60 rounded p-1.5 mt-0.5">{e.validation_notes}</p>
                                    </div>
                                  </div>
                                )}
                              </div>
                            )
                          })}
                        </div>
                      )}
                    </div>
                  )}

                  {/* FK predictions */}
                  {show('fk') && (
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <h4 className="text-xs font-semibold text-slate-500">Foreign Key Predictions</h4>
                        {Array.isArray(tbl.fk_predictions) && tbl.fk_predictions.length > 0 && (() => {
                          const tblKey = tbl.table_name
                          const selSet = selectedFKs[tblKey] || new Set()
                          const selCount = selSet.size
                          return selCount > 0 ? (
                            <button onClick={async () => {
                              setFkApplying(true); setFkApplyResult(null)
                              const preds = [...selSet].map(idx => tbl.fk_predictions[idx]).filter(Boolean).map(fk => ({
                                src_table: fk.src_table, src_column: fk.src_column, dst_table: fk.dst_table, dst_column: fk.dst_column,
                              }))
                              try {
                                const r = await fetch('/api/analytics/fk-apply-from-predictions', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ predictions: preds }) })
                                const j = await r.json().catch(() => ({}))
                                setFkApplyResult(j)
                                setSelectedFKs(p => ({ ...p, [tblKey]: new Set() }))
                              } catch (err) { setFkApplyResult({ error: err.message }) }
                              setFkApplying(false)
                            }} disabled={fkApplying} className="text-xs px-2 py-0.5 bg-indigo-600 text-white rounded hover:bg-indigo-700 disabled:opacity-50">
                              {fkApplying ? 'Applying...' : `Apply Selected (${selCount})`}
                            </button>
                          ) : null
                        })()}
                      </div>
                      {fkApplyResult && (
                        <div className={`text-xs ${fkApplyResult.error ? 'text-red-600' : 'text-green-600'}`}>
                          {fkApplyResult.error ? String(fkApplyResult.error) : `Applied: ${(fkApplyResult.results || []).filter(r => r.ok).length} succeeded, ${(fkApplyResult.results || []).filter(r => !r.ok).length} failed.`}
                          {(fkApplyResult.results || []).filter(r => !r.ok).map((r, ri) => (
                            <div key={ri} className="text-red-500 text-[10px] mt-0.5 truncate" title={r.error}>{r.ddl}: {r.error}</div>
                          ))}
                        </div>
                      )}
                      {!Array.isArray(tbl.fk_predictions) ? (
                        <p className="text-xs text-slate-400 italic">FK prediction data not available for this table.</p>
                      ) : tbl.fk_predictions.length === 0 ? (
                        <p className="text-xs text-slate-400 italic">No foreign key predictions for this table.</p>
                      ) : (
                        <div className="overflow-x-auto">
                          <table className="min-w-full text-xs">
                            <thead><tr className="bg-dbx-oat/50">
                              <th className="w-8 px-1 py-1"></th>
                              <th className="text-left px-2 py-1 font-semibold text-slate-500">Src Column</th>
                              <th className="text-left px-2 py-1 font-semibold text-slate-500">Dst Table.Column</th>
                              <th className="text-left px-2 py-1 font-semibold text-slate-500" title="Combined final confidence">Final</th>
                              <th className="text-left px-2 py-1 font-semibold text-slate-500" title="AI model confidence">AI</th>
                              <th className="text-left px-2 py-1 font-semibold text-slate-500" title="Column embedding similarity">Sim</th>
                              <th className="text-left px-2 py-1 font-semibold text-slate-500">Reasoning</th>
                            </tr></thead>
                            <tbody>
                              {tbl.fk_predictions.map((fk, i) => {
                                const fconf = Number(fk.final_confidence ?? 0)
                                const confCls = fconf < 0.3 ? 'bg-red-100 text-red-700' : fconf < 0.6 ? 'bg-yellow-100 text-yellow-700' : 'bg-green-100 text-green-700'
                                const tblKey = tbl.table_name
                                const selSet = selectedFKs[tblKey] || new Set()
                                const fkKey = `${tblKey}-${i}`
                                const isExpReasoning = expandedFKs[fkKey]
                                return (
                                  <React.Fragment key={i}>
                                    <tr className={`border-b border-slate-100 hover:bg-indigo-50/30 ${selSet.has(i) ? 'bg-indigo-50/50' : ''}`}>
                                      <td className="px-1 py-1">
                                        <input type="checkbox" checked={selSet.has(i)} onChange={() => {
                                          setSelectedFKs(prev => {
                                            const ns = new Set(prev[tblKey] || [])
                                            if (ns.has(i)) ns.delete(i); else ns.add(i)
                                            return { ...prev, [tblKey]: ns }
                                          })
                                        }} className="rounded border-slate-300" />
                                      </td>
                                      <td className="px-2 py-1 text-slate-600 font-mono">{(fk.src_column || '').split('.').pop()}</td>
                                      <td className="px-2 py-1 text-slate-600 font-mono">{fk.dst_table}.{(fk.dst_column || '').split('.').pop()}</td>
                                      <td className="px-2 py-1"><span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${confCls}`}>{fconf.toFixed(2)}</span></td>
                                      <td className="px-2 py-1 text-slate-500">{Number(fk.ai_confidence ?? 0).toFixed(2)}</td>
                                      <td className="px-2 py-1 text-slate-500">{Number(fk.col_similarity ?? 0).toFixed(2)}</td>
                                      <td className="px-2 py-1 text-slate-500 max-w-xs truncate cursor-pointer" title="Click to expand"
                                        onClick={() => setExpandedFKs(p => ({ ...p, [fkKey]: !p[fkKey] }))}>
                                        {fk.ai_reasoning || '--'}
                                      </td>
                                    </tr>
                                    {isExpReasoning && fk.ai_reasoning && (
                                      <tr><td colSpan={7} className="px-3 py-2 bg-slate-50 text-xs text-slate-600 italic">{fk.ai_reasoning}</td></tr>
                                    )}
                                  </React.Fragment>
                                )
                              })}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Export bar */}
      {reviewData.length > 0 && (
        <div className="bg-dbx-oat-light rounded-xl border border-slate-200 p-4 shadow-sm space-y-3">
          <h3 className="text-sm font-semibold text-slate-700">Export & Apply</h3>
          <div className="flex flex-wrap items-center gap-3">
            <select value={ddlCategory} onChange={e => { setDdlCategory(e.target.value); setDdlSql(''); setDdlApplyResult(null) }}
              className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm bg-white">
              <option value="comments">Comments</option>
              <option value="domain">Domain Tags</option>
              <option value="sensitivity">Sensitivity Tags</option>
            </select>
            {ddlCategory === 'domain' && (
              <>
                <input value={customDomainKey} onChange={e => setCustomDomainKey(e.target.value)} placeholder="domain tag key (default: domain)"
                  className="border border-slate-300 rounded px-2 py-1.5 text-sm w-52" />
                <input value={customSubdomainKey} onChange={e => setCustomSubdomainKey(e.target.value)} placeholder="subdomain tag key (default: subdomain)"
                  className="border border-slate-300 rounded px-2 py-1.5 text-sm w-56" />
              </>
            )}
            {ddlCategory === 'sensitivity' && (
              <>
                <input value={customSensitivityKey} onChange={e => setCustomSensitivityKey(e.target.value)} placeholder="classification tag key (default: data_classification)"
                  className="border border-slate-300 rounded px-2 py-1.5 text-sm w-64" />
                <input value={customSensitivityTypeKey} onChange={e => setCustomSensitivityTypeKey(e.target.value)} placeholder="subclassification tag key (default: data_subclassification)"
                  className="border border-slate-300 rounded px-2 py-1.5 text-sm w-68" />
              </>
            )}
            <button onClick={generateDdl} disabled={ddlLoading} className="px-4 py-1.5 bg-dbx-oat text-slate-700 rounded-lg text-sm font-medium hover:bg-dbx-oat-dark disabled:opacity-50">Generate DDL</button>
            <button onClick={applyDdl} disabled={ddlLoading} className="px-4 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 disabled:opacity-50">Apply DDL</button>
            <span className="border-l border-slate-300 h-6" />
            <button onClick={() => exportVolume('tsv')} disabled={exportLoading} className="px-4 py-1.5 bg-dbx-oat text-slate-700 rounded-lg text-sm font-medium hover:bg-dbx-oat-dark disabled:opacity-50">Export TSV</button>
            <button onClick={() => exportVolume('excel')} disabled={exportLoading} className="px-4 py-1.5 bg-dbx-oat text-slate-700 rounded-lg text-sm font-medium hover:bg-dbx-oat-dark disabled:opacity-50">Export Excel</button>
          </div>
          {ddlApplyResult && (ddlApplyResult.ok
            ? <p className="text-sm text-green-600">Applied {ddlApplyResult.applied} statement(s).</p>
            : <div className="space-y-2">
                <p className="text-sm text-amber-700">Applied {ddlApplyResult.applied} of {ddlApplyResult.applied + (ddlApplyResult.errors?.length || 0)} statement(s).</p>
                {ddlApplyResult.errors?.map((e, i) => (
                  <div key={i} className="text-xs bg-red-50 border border-red-200 rounded p-2">
                    <p className="text-red-700 font-mono truncate">{e.statement}</p>
                    <p className="text-red-600 mt-1">{e.error}</p>
                    {e.governed_tag && (
                      <p className="text-amber-700 mt-1 font-medium">{e.hint}</p>
                    )}
                  </div>
                ))}
              </div>
          )}
          {exportResult && (exportResult.ok
            ? <p className="text-sm text-green-600">Exported {exportResult.rows} rows to <span className="font-mono text-xs">{exportResult.path}</span></p>
            : <p className="text-sm text-red-600">{exportResult.detail}</p>)}
          {ddlSql && (
            <div className="relative">
              <pre className="text-xs bg-dbx-oat border border-slate-200 rounded-lg p-3 overflow-auto max-h-64 whitespace-pre-wrap">{ddlSql}</pre>
              <button onClick={() => navigator.clipboard.writeText(ddlSql)} className="absolute top-2 right-2 px-2 py-1 bg-dbx-oat-light border border-slate-200 rounded text-xs hover:bg-dbx-oat">Copy</button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function EditableTableKB({ rows, original, setRows }) {
  if (!rows?.length) return <p className="text-sm text-slate-400 py-4">No data available.</p>
  const editable = ['comment', 'domain', 'subdomain']
  const otherCols = ['table_name', 'catalog', 'schema', 'table_short_name', 'has_pii', 'has_phi', 'created_at', 'updated_at', 'review_updated_at'].filter(c => rows[0] && c in rows[0])
  const onChange = (i, field, value) => {
    setRows(prev => prev.map((r, j) => j === i ? { ...r, [field]: value } : r))
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr>
            {otherCols.slice(0, 4).map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase">{c}</th>)}
            {editable.map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase">{c}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={`border-b border-slate-100 ${original && (editable.some(f => row[f] !== original[i]?.[f])) ? 'bg-amber-50' : ''} hover:bg-orange-50/30`}>
              {otherCols.slice(0, 4).map(c => <td key={c} className="px-3 py-2 max-w-[12rem] truncate text-slate-600">{String(row[c] ?? '')}</td>)}
              {editable.map(f => (
                <td key={f} className="px-2 py-1">
                  <input value={row[f] ?? ''} onChange={e => onChange(i, f, e.target.value)}
                    className="w-full min-w-[8rem] border border-slate-300 rounded px-2 py-1 text-sm focus:ring-2 focus:ring-orange-500" />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function EditableColumnKB({ rows, original, setRows }) {
  if (!rows?.length) return <p className="text-sm text-slate-400 py-4">No data available.</p>
  const editable = ['comment', 'classification']
  const otherCols = ['column_id', 'table_name', 'schema', 'column_name', 'data_type', 'confidence', 'created_at', 'updated_at', 'review_updated_at'].filter(c => rows[0] && c in rows[0])
  const onChange = (i, field, value) => {
    setRows(prev => prev.map((r, j) => j === i ? { ...r, [field]: value } : r))
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr>
            {otherCols.slice(0, 5).map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase">{c}</th>)}
            {editable.map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-slate-200 text-xs uppercase">{c}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={`border-b border-slate-100 ${original && editable.some(f => row[f] !== original[i]?.[f]) ? 'bg-amber-50' : ''} hover:bg-orange-50/30`}>
              {otherCols.slice(0, 5).map(c => <td key={c} className="px-3 py-2 max-w-[10rem] truncate text-slate-600">{String(row[c] ?? '')}</td>)}
              {editable.map(f => (
                <td key={f} className="px-2 py-1">
                  <input value={row[f] ?? ''} onChange={e => onChange(i, f, e.target.value)}
                    className="w-full min-w-[8rem] border border-slate-300 rounded px-2 py-1 text-sm focus:ring-2 focus:ring-orange-500" />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function EditableSchemaKB({ rows, original, setRows }) {
  if (!rows?.length) return <p className="text-sm text-slate-400 py-4">No data available.</p>
  const editable = ['comment', 'domain']
  const otherCols = ['schema_id', 'catalog', 'schema_name', 'has_pii', 'has_phi', 'table_count', 'created_at', 'updated_at', 'review_updated_at'].filter(c => rows[0] && c in rows[0])
  const onChange = (i, field, value) => {
    setRows(prev => prev.map((r, j) => j === i ? { ...r, [field]: value } : r))
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr>
            {otherCols.slice(0, 4).map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase">{c}</th>)}
            {editable.map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-600 border-b border-slate-200 text-xs uppercase">{c}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={`border-b border-slate-100 ${original && editable.some(f => row[f] !== original[i]?.[f]) ? 'bg-amber-50' : ''} hover:bg-orange-50/30`}>
              {otherCols.slice(0, 4).map(c => <td key={c} className="px-3 py-2 max-w-[10rem] truncate text-slate-600">{String(row[c] ?? '')}</td>)}
              {editable.map(f => (
                <td key={f} className="px-2 py-1">
                  <input value={row[f] ?? ''} onChange={e => onChange(i, f, e.target.value)}
                    className="w-full min-w-[8rem] border border-slate-300 rounded px-2 py-1 text-sm focus:ring-2 focus:ring-orange-500" />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default function MetadataReview() {
  const [tab, setTab] = useState('editor')
  const [data, setData] = useState([])
  const [original, setOriginal] = useState([])
  const [error, setError] = useState(null)
  const [filterTable, setFilterTable] = useState('')
  const [filterExtra, setFilterExtra] = useState('')
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)

  const endpoints = { log: '/api/metadata/log', kb: '/api/metadata/knowledge-base', columns: '/api/metadata/column-kb', schemas: '/api/metadata/schema-kb' }
  const patchEndpoints = { kb: '/api/metadata/knowledge-base', columns: '/api/metadata/column-kb', schemas: '/api/metadata/schema-kb' }

  const buildUrl = useCallback((key) => {
    const base = endpoints[key]
    const params = new URLSearchParams()
    if (key === 'kb') {
      if (filterTable) params.set('table_name', filterTable)
      if (filterExtra) params.set('schema_name', filterExtra)
    } else if (key === 'columns') {
      if (filterTable) params.set('table_name', filterTable)
      if (filterExtra) params.set('column_name', filterExtra)
    } else if (key === 'schemas' && filterExtra) {
      params.set('schema_name', filterExtra)
    } else if (key === 'log' && filterTable) {
      params.set('table_name', filterTable)
    }
    const q = params.toString()
    return q ? `${base}?${q}` : base
  }, [filterTable, filterExtra])

  const load = async (key) => {
    setLoading(true)
    const r = await safeFetch(buildUrl(key))
    setData(r.data || [])
    setOriginal(r.data ? r.data.map(row => ({ ...row })) : [])
    setError(r.error)
    setLoading(false)
  }

  useEffect(() => { if (tab !== 'editor') load(tab) }, [tab])

  const getModifiedRows = () => {
    if (tab === 'kb') {
      return data.filter((row, i) => ['comment', 'domain', 'subdomain'].some(f => row[f] !== original[i]?.[f]))
    }
    if (tab === 'columns') {
      return data.filter((row, i) => ['comment', 'classification'].some(f => row[f] !== original[i]?.[f]))
    }
    if (tab === 'schemas') {
      return data.filter((row, i) => ['comment', 'domain'].some(f => row[f] !== original[i]?.[f]))
    }
    return []
  }

  const handleUpdate = async () => {
    const modified = getModifiedRows()
    if (modified.length === 0) return
    const ep = patchEndpoints[tab]
    if (!ep) return
    setSaving(true)
    setError(null)
    let body
    if (tab === 'kb') {
      body = modified.map(r => ({ table_name: r.table_name, comment: r.comment ?? undefined, domain: r.domain ?? undefined, subdomain: r.subdomain ?? undefined }))
    } else if (tab === 'columns') {
      body = modified.map(r => ({ column_id: r.column_id, comment: r.comment ?? undefined, classification: r.classification ?? undefined }))
    } else if (tab === 'schemas') {
      body = modified.map(r => ({ schema_id: r.schema_id, comment: r.comment ?? undefined, domain: r.domain ?? undefined }))
    }
    try {
      const res = await fetch(ep, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      if (!res.ok) {
        const t = await res.text()
        let msg = res.status + ''
        try { const j = JSON.parse(t); if (j.detail) msg = j.detail } catch {}
        setError(msg)
        setSaving(false)
        return
      }
      setOriginal(data.map(row => ({ ...row })))
      setSaving(false)
    } catch (e) {
      setError(e.message)
      setSaving(false)
    }
  }

  const modifiedCount = (tab === 'log' || !patchEndpoints[tab]) ? 0 : getModifiedRows().length
  const isEditable = tab === 'kb' || tab === 'columns' || tab === 'schemas'

  const [ddlScope, setDdlScope] = useState('table')
  const [ddlIdentifiers, setDdlIdentifiers] = useState('')
  const [ddlSql, setDdlSql] = useState('')
  const [ddlLoading, setDdlLoading] = useState(false)
  const [ddlApplyResult, setDdlApplyResult] = useState(null)
  const [ddlTagKey, setDdlTagKey] = useState('geo_classification')

  const _buildDdlBody = () => {
    const identifiers = ddlIdentifiers.trim() ? ddlIdentifiers.split(/[\n,]/).map(s => s.trim()).filter(Boolean) : []
    const body = { scope: ddlScope, identifiers: identifiers.length ? identifiers : undefined }
    if (ddlScope === 'geo' && ddlTagKey.trim()) body.tag_key = ddlTagKey.trim()
    return body
  }

  const runGenerateDdl = async () => {
    setDdlLoading(true)
    setDdlApplyResult(null)
    const r = await fetch('/api/metadata/generate-ddl', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(_buildDdlBody())
    })
    const j = await r.json().catch(() => ({}))
    setDdlSql(j.sql || j.detail || '')
    setDdlLoading(false)
  }

  const runApplyDdl = async () => {
    setDdlLoading(true)
    setDdlApplyResult(null)
    const r = await fetch('/api/metadata/apply-ddl', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(_buildDdlBody())
    })
    const j = await r.json().catch(() => ({}))
    if (r.ok) {
      setDdlApplyResult({ ok: true, applied: j.applied })
    } else {
      setDdlApplyResult({ ok: false, detail: j.detail || j })
    }
    setDdlLoading(false)
  }

  return (
    <div className="space-y-4">
      <ErrorBanner error={error} />
      <div className="flex flex-wrap items-center gap-4">
        <div className="flex bg-dbx-oat rounded-lg p-1">
          {[['editor', 'Review Editor'], ['kb', 'Table KB'], ['columns', 'Column KB'], ['schemas', 'Schema KB'], ['log', 'Generation Log']].map(([k, l]) => (
            <button key={k} onClick={() => setTab(k)}
              className={`px-3 py-1.5 text-sm rounded-md transition-all ${tab === k ? 'bg-dbx-oat-light shadow-sm font-semibold text-red-700' : 'text-slate-500 hover:text-slate-700'}`}>{l}</button>
          ))}
        </div>
        {tab !== 'editor' && <>
          {tab === 'schemas' && (
            <input value={filterExtra} onChange={e => setFilterExtra(e.target.value)} placeholder="Filter by schema name..."
              className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm w-48 focus:ring-2 focus:ring-orange-500" />
          )}
          {(tab === 'kb' || tab === 'log') && (
            <>
              <input value={filterTable} onChange={e => setFilterTable(e.target.value)} placeholder="Filter by table name..."
                className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm w-48 focus:ring-2 focus:ring-orange-500" />
              {tab === 'kb' && <input value={filterExtra} onChange={e => setFilterExtra(e.target.value)} placeholder="Schema name (exact)"
                className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm w-40 focus:ring-2 focus:ring-orange-500" />}
            </>
          )}
          {tab === 'columns' && (
            <>
              <input value={filterTable} onChange={e => setFilterTable(e.target.value)} placeholder="Filter by table name..."
                className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm w-48 focus:ring-2 focus:ring-orange-500" />
              <input value={filterExtra} onChange={e => setFilterExtra(e.target.value)} placeholder="Filter by column name..."
                className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm w-48 focus:ring-2 focus:ring-orange-500" />
            </>
          )}
          <button onClick={() => load(tab)} className="px-4 py-1.5 bg-dbx-oat text-slate-700 rounded-lg text-sm font-medium hover:bg-dbx-oat-dark">Search</button>
          {isEditable && modifiedCount > 0 && (
            <button onClick={handleUpdate} disabled={saving}
              className="px-4 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 shadow-sm disabled:opacity-50">Save changes ({modifiedCount})</button>
          )}
        </>}
      </div>
      {tab === 'editor' ? <ReviewEditor /> : (
        <div className="bg-dbx-oat-light rounded-xl border border-slate-200 p-4 shadow-sm">
          {loading ? <p className="text-sm text-slate-400 py-4">Loading...</p> : (
            tab === 'log' ? <DataTable data={data} /> : (
              tab === 'kb' ? <EditableTableKB rows={data} original={original} setRows={setData} /> :
              tab === 'columns' ? <EditableColumnKB rows={data} original={original} setRows={setData} /> :
              tab === 'schemas' ? <EditableSchemaKB rows={data} original={original} setRows={setData} /> : <DataTable data={data} />
            )
          )}
        </div>
      )}

      {(tab === 'kb' || tab === 'columns' || tab === 'schemas') && (
        <div className="bg-dbx-oat-light rounded-xl border border-slate-200 p-4 shadow-sm space-y-3">
          <h3 className="text-sm font-semibold text-slate-700">Generate SQL / Apply DDL</h3>
          <div className="flex flex-wrap items-center gap-4">
            <span className="text-sm text-slate-600">Scope:</span>
            {['table', 'schema', 'column'].map(s => (
              <label key={s} className="inline-flex items-center gap-1.5">
                <input type="radio" name="ddlScope" checked={ddlScope === s} onChange={() => setDdlScope(s)} className="rounded border-slate-300" />
                <span className="text-sm capitalize">{s}</span>
              </label>
            ))}
            <input value={ddlIdentifiers} onChange={e => setDdlIdentifiers(e.target.value)}
              placeholder={ddlScope === 'table' ? 'Table names (optional, comma/newline)' : ddlScope === 'schema' ? 'Schema names (optional)' : 'Column IDs or table names (optional)'}
              className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm flex-1 min-w-[12rem] focus:ring-2 focus:ring-orange-500" />
            <button onClick={runGenerateDdl} disabled={ddlLoading} className="px-4 py-1.5 bg-dbx-oat text-slate-700 rounded-lg text-sm font-medium hover:bg-dbx-oat-dark disabled:opacity-50">Generate SQL</button>
            <button onClick={runApplyDdl} disabled={ddlLoading} className="px-4 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 disabled:opacity-50">Apply DDL</button>
          </div>
          {ddlApplyResult && (ddlApplyResult.ok ? <p className="text-sm text-green-600">Applied {ddlApplyResult.applied} statement(s).</p> : <p className="text-sm text-red-600">{JSON.stringify(ddlApplyResult.detail)}</p>)}
          {ddlSql && (
            <div className="relative">
              <pre className="text-xs bg-dbx-oat border border-slate-200 rounded-lg p-3 overflow-auto max-h-64 whitespace-pre-wrap">{ddlSql}</pre>
              <button type="button" onClick={() => navigator.clipboard.writeText(ddlSql)} className="absolute top-2 right-2 px-2 py-1 bg-dbx-oat-light border border-slate-200 rounded text-xs hover:bg-dbx-oat">Copy</button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
