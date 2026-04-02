import React, { useState, useEffect, useCallback, useMemo } from 'react'
import { safeFetch, ErrorBanner } from '../App'
import { FKApplyPanel } from './ForeignKeyGeneration'
import { PageHeader, EmptyState, SkeletonTable } from './ui'
import { useCatalogSchemaTables } from '../hooks/useCatalogSchemaTables'

function DataTable({ data, maxRows = 100 }) {
  if (!data || data.length === 0) return <p className="text-sm text-slate-400 py-4">No data available.</p>
  const cols = Object.keys(data[0])
  return (
    <div className="overflow-x-auto rounded-xl shadow-card border border-slate-300 dark:border-slate-600">
      <table className="min-w-full text-sm bg-white dark:bg-slate-800">
        <thead><tr>{cols.map(c =>
          <th key={c} className="text-left px-3 py-2.5 bg-slate-100 dark:bg-slate-700 font-semibold text-slate-800 dark:text-slate-100 border-b border-slate-300 dark:border-slate-600 text-xs uppercase tracking-wider sticky top-0">{c}</th>
        )}</tr></thead>
        <tbody>
          {data.slice(0, maxRows).map((row, i) => (
            <tr key={i} className="border-b border-slate-200 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors">
              {cols.map(c => <td key={c} className="px-3 py-2 max-w-xs truncate text-slate-800 dark:text-slate-200">{String(row[c] ?? '')}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const Tip = ({ text }) => (
  <span className="relative group cursor-help ml-1.5 inline-flex">
    <span className="text-slate-400 dark:text-slate-500 text-xs font-bold border border-slate-300 dark:border-dbx-navy-400 rounded-full w-4 h-4 inline-flex items-center justify-center hover:text-dbx-teal hover:border-dbx-teal transition-colors">?</span>
    <span className="absolute z-50 hidden group-hover:block bottom-full left-1/2 -translate-x-1/2 mb-1.5 w-60 text-xs bg-dbx-navy dark:bg-dbx-navy-600 text-white rounded-xl px-3 py-2 shadow-elevated pointer-events-none animate-fade-in">{text}</span>
  </span>
)

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

const PROPERTY_ROLE_GROUPS = [
  { label: 'Identifiers', roles: ['primary_key', 'business_key'] },
  { label: 'Measures', roles: ['measure', 'derived'] },
  { label: 'Dimensions', roles: ['dimension', 'temporal', 'geographic', 'label'] },
  { label: 'Relationships', roles: ['object_property', 'composite_component'] },
  { label: 'Governance', roles: ['pii', 'audit'] },
  { label: 'Other', roles: ['attribute'] },
]

const LEGACY_ROLE_MAP = {
  'link': 'object_property',
  'foreign_key': 'object_property',
  'identifier': 'primary_key',
  'boolean_flag': 'dimension',
  'code': 'dimension',
  'geo': 'composite_component',
  'system_metadata': 'audit',
  'timestamp': 'temporal',
  'hierarchy_level': 'dimension',
  'text_freeform': 'label',
}

function ReviewEditor() {
  const cst = useCatalogSchemaTables()
  const { catalogs, schemas, filtered: filteredTables, catalog: selectedCatalog, schema: selectedSchema, filter: tableFilter, setCatalog: setSelectedCatalog, setSchema: setSelectedSchema, setFilter: setTableFilter } = cst
  const allTables = cst.tables
  const [scopeMode, setScopeMode] = useState('schema')
  const [selectedTables, setSelectedTables] = useState([])
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
  const [recommendedEntity, setRecommendedEntity] = useState({})
  const [recEntLoading, setRecEntLoading] = useState({})
  const [colPropOverrides, setColPropOverrides] = useState({})
  const [propTagApplying, setPropTagApplying] = useState({})
  const [propTagResults, setPropTagResults] = useState({})
  const [ontoConfThreshold, setOntoConfThreshold] = useState(0.5)
  const [tableTagApplying, setTableTagApplying] = useState({})
  const [tableTagResults, setTableTagResults] = useState({})
  const [statusFilter, setStatusFilter] = useState('all')
  const [importLoading, setImportLoading] = useState(false)
  const [importResult, setImportResult] = useState(null)
  const [showVolumeBrowser, setShowVolumeBrowser] = useState(false)
  const [volumeFiles, setVolumeFiles] = useState([])
  const [volumeFilesLoading, setVolumeFilesLoading] = useState(false)
  const [volumeFilesError, setVolumeFilesError] = useState(null)
  const [ddlError, setDdlError] = useState(null)
  const [globalOntoApplying, setGlobalOntoApplying] = useState(false)
  const [globalOntoResult, setGlobalOntoResult] = useState(null)
  const [globalFkTagApplying, setGlobalFkTagApplying] = useState(false)
  const [globalFkTagResult, setGlobalFkTagResult] = useState(null)
  const [fkConfThreshold, setFkConfThreshold] = useState(0.5)
  const [fkGenSql, setFkGenSql] = useState(null)

  useEffect(() => { fetch('/api/ontology/entity-type-options').then(r => r.json()).then(d => setEntityTypeOptions(Array.isArray(d) ? d : [])).catch(() => {}) }, [])

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
      if (!res.ok) {
        let detail = `Server error (${res.status})`
        try { const ej = await res.json(); detail = ej.detail || detail } catch {}
        throw new Error(detail)
      }
      const j = await res.json()
      const tables = j.tables || []
      setReviewData(tables)
      setOriginal(JSON.parse(JSON.stringify(tables)))
      const exp = {}; tables.forEach(t => { exp[t.table_name] = true }); setExpanded(exp)
      if (j.truncated) setError(`Showing 200 of ${j.total_count} tables. Use the filter to narrow results.`)
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
    setDdlLoading(true); setDdlApplyResult(null); setDdlError(null)
    try {
      const res = await fetch('/api/metadata/generate-ddl', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(ddlPayload()) })
      const j = await res.json().catch(() => ({}))
      if (!res.ok) { setDdlError(errMsg(j.detail || `Request failed (${res.status})`)); return }
      const volNote = j.volume_path ? `\n-- Saved to: ${j.volume_path}` : ''
      setDdlSql((j.sql || '') + volNote)
    } catch (e) { setDdlError(e.message || 'Network error') }
    finally { setDdlLoading(false) }
  }
  const applyDdl = async () => {
    if (!confirm('Apply DDL changes to your catalog? This modifies table/column metadata.')) return
    setDdlLoading(true); setDdlApplyResult(null); setDdlError(null)
    try {
      const res = await fetch('/api/metadata/apply-ddl', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(ddlPayload()) })
      const j = await res.json().catch(() => ({}))
      if (!res.ok) { setDdlApplyResult({ ok: false, detail: errMsg(j.detail || `Request failed (${res.status})`) }); return }
      const hasErrors = j.errors && j.errors.length > 0
      setDdlApplyResult(hasErrors ? { ok: false, applied: j.applied || 0, errors: j.errors } : { ok: true, applied: j.applied })
    } catch (e) { setDdlApplyResult({ ok: false, detail: e.message || 'Network error' }) }
    finally { setDdlLoading(false) }
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

  const openVolumeBrowser = async () => {
    setShowVolumeBrowser(true)
    setVolumeFilesLoading(true)
    setVolumeFilesError(null)
    try {
      const res = await fetch('/api/metadata/volume-files')
      const j = await res.json().catch(() => ({}))
      if (!res.ok) { setVolumeFilesError(errMsg(j.detail || `Failed to list files (${res.status})`)); setVolumeFiles([]); return }
      setVolumeFiles(Array.isArray(j) ? j : [])
    } catch (e) { setVolumeFilesError(e.message || 'Network error'); setVolumeFiles([]) }
    finally { setVolumeFilesLoading(false) }
  }

  const importFromVolume = async (volumePath) => {
    setShowVolumeBrowser(false)
    setImportLoading(true); setImportResult(null)
    try {
      const res = await fetch('/api/metadata/import-reviewed', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ volume_path: volumePath })
      })
      const j = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(errMsg(j.detail) || res.status)
      setImportResult({ ok: true, ...j })
      if (reviewData.length > 0) loadData()
    } catch (e) { setImportResult({ ok: false, detail: e.message }) }
    setImportLoading(false)
  }

  const importUpload = async (e) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    const ext = file.name.split('.').pop()?.toLowerCase()
    if (!['tsv', 'xlsx', 'xls'].includes(ext)) {
      setImportResult({ ok: false, detail: 'Only .tsv, .xlsx, and .xls files are supported' }); return
    }
    setImportLoading(true); setImportResult(null)
    try {
      const fd = new FormData()
      fd.append('file', file)
      const res = await fetch('/api/metadata/import-reviewed-upload', { method: 'POST', body: fd })
      const j = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(errMsg(j.detail) || res.status)
      setImportResult({ ok: true, ...j })
      if (reviewData.length > 0) loadData()
    } catch (err) { setImportResult({ ok: false, detail: err.message }) }
    setImportLoading(false)
  }

  const errMsg = d => typeof d === 'string' ? d : (d ? JSON.stringify(d) : 'Unknown error')
  const show = k => activeType === k
  const chip = 'px-3 py-1.5 rounded-lg text-xs font-medium border cursor-pointer select-none transition-all duration-200'
  const chipOn = 'bg-dbx-lava text-white border-dbx-lava shadow-sm'
  const chipOff = 'bg-white dark:bg-dbx-navy/50 text-slate-600 dark:text-slate-300 border-dbx-oat-dark dark:border-dbx-navy-400/30 hover:border-dbx-navy-400 dark:hover:border-dbx-navy-400 hover:shadow-sm'
  const inp = 'w-full border border-slate-200 dark:border-dbx-navy-400/40 rounded-lg px-2.5 py-1.5 text-sm bg-white dark:bg-dbx-navy/60 focus:outline-none focus:ring-2 focus:ring-dbx-teal/30 focus:border-dbx-teal transition-all'

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
      <div className="card p-5 space-y-3">
        <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">Scope</h3>
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
            <input value={tableFilter} onChange={e => setTableFilter(e.target.value)} placeholder="Filter tables..." className={inp + ' mb-2 max-w-xs'} aria-label="Filter tables" />
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

      {/* Metadata Type Filter + Review Status Filter */}
      {reviewData.length > 0 && (
        <>
          <div className="text-[10px] text-slate-400 dark:text-slate-500 flex items-center gap-3 px-1">
            <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-green-400" /> Review status &amp; property roles save instantly</span>
            <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-amber-400" /> Comments &amp; classifications require Save Changes</span>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs font-medium text-slate-500">Show:</span>
            {META_TYPES.map(({ key, label }) => (
              <span key={key} onClick={() => setActiveType(key)} className={`${chip} ${activeType === key ? chipOn : chipOff}`}>{label}</span>
            ))}
            <span className="border-l border-slate-300 h-5 mx-1" />
            <span className="text-xs font-medium text-slate-500">Status:</span>
            {['all', 'unreviewed', 'in_review', 'approved'].map(s => (
              <span key={s} onClick={() => setStatusFilter(s)} className={`${chip} ${statusFilter === s ? chipOn : chipOff}`}>
                {s === 'all' ? 'All' : s === 'in_review' ? 'In Review' : s.charAt(0).toUpperCase() + s.slice(1)}
              </span>
            ))}
            {totalDirty > 0 && (
              <button onClick={saveChanges} disabled={saving}
                title="Batch-saves all edited table comments/domains and column comments/classifications to the knowledge bases."
                className="ml-auto px-4 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 shadow-sm disabled:opacity-50">
                {saving ? 'Saving...' : `Save Changes (${totalDirty})`}
              </button>
            )}
          </div>
        </>
      )}

      {/* Global Ontology Apply Bar */}
      {show('ontology') && reviewData.length > 0 && (() => {
        const allSelections = reviewData.flatMap(tbl =>
          (tbl.ontology_entities || [])
            .filter(e => Number(e.confidence ?? 0) >= ontoConfThreshold)
            .map(e => {
              let sc = e.source_columns
              if (typeof sc === 'string') { try { sc = JSON.parse(sc) } catch {} }
              return { entity_type: entityTypeOverrides[e.entity_id] || e.entity_type, source_tables: [tbl.table_name], source_columns: Array.isArray(sc) ? sc : [], entity_role: e.entity_role || 'primary' }
            })
        )
        return allSelections.length > 0 ? (
          <div className="card p-4 flex flex-wrap items-center gap-3">
            <span className="text-sm font-semibold text-slate-700 dark:text-slate-200">Ontology Tags</span>
            <label className="text-xs text-slate-500 flex items-center gap-1">
              Min Confidence:
              <input type="number" min="0" max="1" step="0.05" value={ontoConfThreshold}
                onChange={ev => setOntoConfThreshold(Number(ev.target.value))}
                className="text-xs border border-slate-300 rounded px-1.5 py-0.5 bg-white dark:bg-dbx-navy/60 dark:text-slate-200 w-16" />
            </label>
            <button onClick={async () => {
              setGlobalOntoApplying(true); setGlobalOntoResult(null)
              try {
                const r = await fetch('/api/ontology/apply-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ selections: allSelections }) })
                const j = await r.json().catch(() => ({}))
                setGlobalOntoResult(r.ok ? j : { error: j.detail || j })
              } catch (err) { setGlobalOntoResult({ error: err.message }) }
              setGlobalOntoApplying(false)
            }} disabled={globalOntoApplying}
              className="px-4 py-1.5 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50 shadow-sm">
              {globalOntoApplying ? 'Applying...' : `Apply All Ontology Tags (${allSelections.length} entities, conf \u2265 ${ontoConfThreshold})`}
            </button>
            {globalOntoResult && (
              <span className={`text-xs ${globalOntoResult.error ? 'text-red-600' : 'text-green-600'}`}>
                {globalOntoResult.error
                  ? String(typeof globalOntoResult.error === 'object' ? JSON.stringify(globalOntoResult.error) : globalOntoResult.error)
                  : `Done: ${(globalOntoResult.results || []).filter(r => r.ok).length} table tags, ${(globalOntoResult.column_results || []).filter(r => r.ok).length} column tags applied`}
              </span>
            )}
          </div>
        ) : null
      })()}

      {/* Global FK Tag All Bar */}
      {show('fk') && reviewData.length > 0 && (() => {
        const allFkPreds = reviewData.flatMap(tbl =>
          (tbl.fk_predictions || [])
            .filter(fk => Number(fk.final_confidence ?? 0) >= fkConfThreshold)
            .map(fk => ({
              src_table: fk.src_table, src_column: fk.src_column,
              dst_table: fk.dst_table, dst_column: fk.dst_column,
            }))
        )
        return allFkPreds.length > 0 ? (
          <>
            <div className="card p-4 flex flex-wrap items-center gap-3">
              <span className="text-sm font-semibold text-slate-700 dark:text-slate-200">FK Tags</span>
              <label className="text-xs text-slate-500 flex items-center gap-1">
                Min Confidence:
                <input type="number" min="0" max="1" step="0.05" value={fkConfThreshold}
                  onChange={ev => setFkConfThreshold(Number(ev.target.value))}
                  className="text-xs border border-slate-300 rounded px-1.5 py-0.5 bg-white dark:bg-dbx-navy/60 dark:text-slate-200 w-16" />
              </label>
              <button onClick={async () => {
                setGlobalFkTagApplying(true); setGlobalFkTagResult(null)
                try {
                  const r = await fetch('/api/analytics/fk-apply-as-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ predictions: allFkPreds }) })
                  const j = await r.json().catch(() => ({}))
                  setGlobalFkTagResult(r.ok ? j : { error: j.detail || j })
                } catch (err) { setGlobalFkTagResult({ error: err.message }) }
                setGlobalFkTagApplying(false)
              }} disabled={globalFkTagApplying}
                className="px-4 py-1.5 bg-indigo-600 text-white rounded-lg text-sm font-medium hover:bg-indigo-700 disabled:opacity-50 shadow-sm"
                title="Only requires APPLY_TAG permission">
                {globalFkTagApplying ? 'Tagging...' : `Tag All (${allFkPreds.length} pairs, conf \u2265 ${fkConfThreshold})`}
              </button>
              <button onClick={async () => {
                try {
                  const r = await fetch('/api/analytics/fk-generate-sql', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ predictions: allFkPreds }) })
                  const j = await r.json().catch(() => ({}))
                  setFkGenSql(j.sql || '')
                } catch (err) { setFkGenSql(`-- Error: ${err.message}`) }
              }} className="px-4 py-1.5 bg-slate-600 text-white rounded-lg text-sm font-medium hover:bg-slate-700 shadow-sm"
                title="Generate ALTER TABLE ADD CONSTRAINT DDL (requires MANAGE to execute)">
                Generate SQL ({allFkPreds.length})
              </button>
              {globalFkTagResult && (
                <span className={`text-xs ${globalFkTagResult.error ? 'text-red-600' : 'text-green-600'}`}>
                  {globalFkTagResult.error
                    ? String(typeof globalFkTagResult.error === 'object' ? JSON.stringify(globalFkTagResult.error) : globalFkTagResult.error)
                    : `Done: ${(globalFkTagResult.results || []).filter(r => r.ok).length} tagged, ${(globalFkTagResult.results || []).filter(r => !r.ok).length} failed`}
                </span>
              )}
            </div>
            {fkGenSql != null && (
              <div className="card p-3 relative border border-slate-300 dark:border-dbx-navy-400/40">
                <pre className="text-[11px] bg-slate-800 dark:bg-slate-900 text-green-300 dark:text-green-400 rounded p-3 overflow-x-auto whitespace-pre-wrap max-h-60 font-mono">{fkGenSql}</pre>
                <div className="absolute top-2 right-2 flex gap-1">
                  <button onClick={() => { navigator.clipboard.writeText(fkGenSql) }}
                    className="text-[10px] px-2 py-0.5 bg-slate-700 border border-slate-600 rounded hover:bg-slate-600 text-slate-200">Copy</button>
                  <button onClick={() => setFkGenSql(null)}
                    className="text-[10px] px-2 py-0.5 bg-slate-700 border border-slate-600 rounded hover:bg-slate-600 text-slate-200">Close</button>
                </div>
              </div>
            )}
          </>
        ) : null
      })()}

      {/* Combined Data View */}
      {reviewData.length > 0 && (
        <div className="space-y-3">
          {reviewData.filter(tbl => statusFilter === 'all' || (tbl.review_status || 'unreviewed') === statusFilter).map((tbl, _fi) => {
            const tblIdx = reviewData.indexOf(tbl)
            return (
            <div key={tbl.table_name} className="bg-dbx-oat-light dark:bg-dbx-navy-650 rounded-xl border border-slate-200 dark:border-dbx-navy-400/25 shadow-sm overflow-hidden">
              {/* Table header row */}
              <div className={`flex items-center gap-2 px-4 py-2.5 cursor-pointer select-none flex-wrap ${isTableDirty(tblIdx) ? 'bg-amber-50 dark:bg-amber-900/20' : 'bg-dbx-oat dark:bg-dbx-navy-500/50'}`}
                onClick={() => setExpanded(p => ({ ...p, [tbl.table_name]: !p[tbl.table_name] }))}>
                <span className="text-xs text-slate-400">{expanded[tbl.table_name] ? '\u25BC' : '\u25B6'}</span>
                <span className="font-semibold text-sm text-slate-700 dark:text-slate-200">{tbl.table_name}</span>
                {tbl.domain && <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300 font-medium">{tbl.domain}{tbl.subdomain ? ` / ${tbl.subdomain}` : ''}</span>}
                {tbl.primary_entity && (<span className="inline-flex items-center gap-1">
                  <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300 font-medium">{tbl.primary_entity.entity_type} ({Number(tbl.primary_entity.confidence ?? 0).toFixed(2)})</span>
                  {tbl.primary_entity.source_ontology && <span className="text-[9px] px-1 py-0 rounded bg-indigo-100 dark:bg-indigo-900/40 text-indigo-600 dark:text-indigo-300">{tbl.primary_entity.source_ontology}</span>}
                </span>)}
                {show('ontology') && (() => {
                  const rs = tbl.review_status || 'unreviewed'
                  const rsCls = rs === 'approved' ? 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300' : rs === 'in_review' ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/40 dark:text-yellow-300' : 'bg-slate-100 text-slate-500 dark:bg-slate-800/40 dark:text-slate-400'
                  return (
                    <select value={rs} title="Saves immediately to table_knowledge_base. Tracks whether a human has reviewed this table's ontology metadata." onClick={ev => ev.stopPropagation()} onChange={ev => {
                      ev.stopPropagation()
                      const newStatus = ev.target.value
                      const sel = ev.target
                      setReviewData(prev => prev.map((t, i) => i === tblIdx ? { ...t, review_status: newStatus } : t))
                      fetch('/api/ontology/set-review-status', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ table_name: tbl.table_name, review_status: newStatus }) })
                        .then(r => { sel.style.outline = r.ok ? '2px solid #22c55e' : '2px solid #ef4444'; setTimeout(() => { sel.style.outline = '' }, 1200) })
                        .catch(() => { sel.style.outline = '2px solid #ef4444'; setTimeout(() => { sel.style.outline = '' }, 1200) })
                    }} className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium border-0 cursor-pointer ${rsCls}`}>
                      <option value="unreviewed">unreviewed</option>
                      <option value="in_review">in review</option>
                      <option value="approved">approved</option>
                    </select>
                  )
                })()}
                <span className="text-xs text-slate-400 ml-auto">{tbl.columns?.length || 0} columns</span>
              </div>

              {expanded[tbl.table_name] && (
                <div className="px-4 pb-4 space-y-1">
                  {/* Table-level fields */}
                  <details open className="group/tbl">
                    <summary className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide cursor-pointer select-none py-2 hover:text-slate-700 dark:hover:text-slate-200">
                      Table Properties
                    </summary>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 pt-1 pb-2">
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
                        <div className="flex items-center gap-2"><input type="checkbox" checked={tbl.has_pii === true} onChange={() => onTableCheckbox(tblIdx, 'has_pii')} className="rounded" aria-label="Mark table as having PII" />
                          <span className="text-xs text-slate-600">Has PII</span></div>
                        <div className="flex items-center gap-2"><input type="checkbox" checked={tbl.has_phi === true} onChange={() => onTableCheckbox(tblIdx, 'has_phi')} className="rounded" aria-label="Mark table as having PHI" />
                          <span className="text-xs text-slate-600">Has PHI</span></div>
                      </>
                    )}
                  </div>
                  </details>

                  {/* Column rows */}
                  {(show('comments') || show('pii') || show('ontology')) && tbl.columns?.length > 0 && (
                  <details open className="group/cols">
                    <summary className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide cursor-pointer select-none py-2 hover:text-slate-700 dark:hover:text-slate-200">
                      Columns ({tbl.columns.length})
                    </summary>
                    <div className="overflow-x-auto">
                      <table className="min-w-full text-sm table-fixed">
                        <thead><tr className="bg-dbx-oat/50 dark:bg-dbx-navy-500/50">
                          <th className="text-left px-3 py-2 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase w-40">Column</th>
                          <th className="text-left px-3 py-2 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase w-24">Type</th>
                          {show('comments') && <th title="Edits saved in batch via Save Changes to column_knowledge_base" className="text-left px-3 py-2 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase">Comment</th>}
                          {show('pii') && <th title="Edits saved in batch via Save Changes to column_knowledge_base" className="text-left px-3 py-2 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase">Classification</th>}
                          {show('pii') && <th title="Edits saved in batch via Save Changes to column_knowledge_base" className="text-left px-3 py-2 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase">Class. Type</th>}
                          {show('ontology') && <th className="text-left px-3 py-2 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase">Entity Types</th>}
                          {show('ontology') && <th title="Saves immediately to ontology_column_properties" className="text-left px-3 py-2 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase">Property Role</th>}
                        </tr></thead>
                        <tbody>
                          {tbl.columns.map((col, ci) => {
                            const colProp = (tbl.column_properties || []).find(p => p.column_name === col.column_name)
                            return (
                            <tr key={col.column_id || ci} className={`border-b border-slate-100 dark:border-dbx-navy-400/20 ${isColDirty(tblIdx, ci) ? 'bg-amber-50 dark:bg-amber-900/20' : ''} hover:bg-orange-50/30 dark:hover:bg-dbx-navy-500/30`}>
                              <td className="px-3 py-1.5 text-slate-600 dark:text-slate-300 font-mono text-xs truncate">{col.column_name}</td>
                              <td className="px-3 py-1.5 text-slate-400 dark:text-slate-500 text-xs truncate">{col.data_type}</td>
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
                                    const role = e.entity_role || 'primary'
                                    const cls = role === 'primary' ? 'bg-purple-100 text-purple-700' : c <= 0 ? 'bg-red-100 text-red-700' : c < 0.5 ? 'bg-yellow-100 text-yellow-700' : 'bg-green-100 text-green-700'
                                    return (<span key={ei} className="inline-flex items-center gap-1">
                                      <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${cls}`}>{e.entity_type} ({c.toFixed(2)})</span>
                                      {e.source_ontology && <span className="text-[9px] px-1 py-0 rounded bg-indigo-100 dark:bg-indigo-900/40 text-indigo-600 dark:text-indigo-300">{e.source_ontology}</span>}
                                    </span>)
                                  })}</div>
                                ) : <span className="text-[10px] text-slate-300">--</span>
                              })()}</td>}
                              {show('ontology') && <td className="px-2 py-1">{colProp ? (() => {
                                const cpKey = colProp.property_id
                                const rawRole = colPropOverrides[cpKey]?.property_role ?? colProp.property_role
                                const curRole = (rawRole && LEGACY_ROLE_MAP[rawRole]) ?? rawRole
                                const curLinked = colPropOverrides[cpKey]?.linked_entity_type ?? colProp.linked_entity_type ?? ''
                                return (
                                  <div className="flex items-center gap-1">
                                    <select value={curRole ?? ''} title="Saves immediately to ontology_column_properties. Defines how this column is used (grouping, aggregation, joining, etc.)." onChange={ev => {
                                      const nr = ev.target.value
                                      setColPropOverrides(p => ({ ...p, [cpKey]: { ...(p[cpKey] || {}), property_role: nr } }))
                                      fetch('/api/ontology/update-column-property', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({ property_id: cpKey, property_role: nr, linked_entity_type: curLinked || null }) }).catch(() => {})
                                    }} className="text-[10px] border border-slate-300 dark:border-dbx-navy-400/40 rounded px-1 py-0.5 bg-white dark:bg-dbx-navy/60 dark:text-slate-200">
                                      <option value="">Select role...</option>
                                      {PROPERTY_ROLE_GROUPS.map(group => (
                                        <optgroup key={group.label} label={group.label}>
                                          {group.roles.map(role => (
                                            <option key={role} value={role}>{role.replace(/_/g, ' ')}</option>
                                          ))}
                                        </optgroup>
                                      ))}
                                    </select>
                                    {curRole === 'object_property' && (
                                      <input value={curLinked} placeholder="linked entity"
                                        onChange={ev => setColPropOverrides(p => ({ ...p, [cpKey]: { ...(p[cpKey] || {}), linked_entity_type: ev.target.value } }))}
                                        onBlur={ev => {
                                          fetch('/api/ontology/update-column-property', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                                            body: JSON.stringify({ property_id: cpKey, property_role: curRole, linked_entity_type: ev.target.value || null }) }).catch(() => {})
                                        }}
                                        className="text-[10px] border border-slate-300 dark:border-dbx-navy-400/40 rounded px-1 py-0.5 bg-white dark:bg-dbx-navy/60 dark:text-slate-200 w-20" />
                                    )}
                                  </div>
                                )
                              })() : <span className="text-[10px] text-slate-300">--</span>}</td>}
                            </tr>
                          )})}
                        </tbody>
                      </table>
                    </div>
                  </details>
                  )}

                  {/* ═══ ZONE 1: Table Entity ═══ */}
                  {show('ontology') && (
                    <div className="border-t border-slate-200 dark:border-dbx-navy-400/30 pt-3 mt-3 space-y-2">
                      <div className="flex items-center justify-between">
                        <h4 className="text-xs font-semibold text-slate-600 dark:text-slate-300 uppercase tracking-wider flex items-center">
                          Table Entity{tbl.primary_entity ? ` \u2014 ${tbl.primary_entity.entity_type}` : ''}
                          <Tip text="Applies the primary entity as an entity_type tag on the table itself. Does not affect columns. Also saved to the knowledge base." />
                        </h4>
                        {tbl.primary_entity && (
                          <button onClick={async () => {
                            setTableTagApplying(p => ({ ...p, [tbl.table_name]: true }))
                            const et = tbl.primary_entity.entity_type
                            try {
                              const r = await fetch('/api/ontology/apply-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ selections: [{ entity_type: et, source_tables: [tbl.table_name], source_columns: [], entity_role: 'primary' }] }) })
                              const j = await r.json().catch(() => ({}))
                              setTableTagResults(p => ({ ...p, [tbl.table_name]: j }))
                            } catch (err) { setTableTagResults(p => ({ ...p, [tbl.table_name]: { error: err.message } })) }
                            setTableTagApplying(p => ({ ...p, [tbl.table_name]: false }))
                          }} disabled={tableTagApplying[tbl.table_name]}
                            className="text-[10px] px-2.5 py-1 bg-purple-600 text-white rounded hover:bg-purple-700 disabled:opacity-50 font-medium">
                            {tableTagApplying[tbl.table_name] ? 'Applying...' : 'Apply Table Entity Tag'}
                          </button>
                        )}
                      </div>
                      {tbl.primary_entity && (
                        <div className="flex items-center gap-2 px-3 py-2 bg-purple-100 dark:bg-purple-900/30 border border-purple-300 dark:border-purple-700/50 rounded-lg">
                          <span className="text-[10px] font-bold uppercase tracking-wider text-purple-500 dark:text-purple-400">Primary Entity</span>
                          <span className="text-sm font-semibold text-purple-800 dark:text-purple-200">{tbl.primary_entity.entity_type}</span>
                          <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${Number(tbl.primary_entity.confidence ?? 0) > 0 ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
                            {Number(tbl.primary_entity.confidence ?? 0).toFixed(2)}
                          </span>
                          {(tbl.primary_entity.validated === true || tbl.primary_entity.validated === 'true') && (
                            <span className="text-[10px] px-1.5 py-0.5 rounded-full font-medium bg-blue-100 text-blue-700">validated</span>
                          )}
                        </div>
                      )}
                      {tableTagResults[tbl.table_name] && (() => {
                        const ar = tableTagResults[tbl.table_name]
                        if (ar.error) return <div className="text-xs text-red-600">{String(ar.error)}</div>
                        const ok = (ar.results || []).filter(r => r.ok)
                        const fail = (ar.results || []).filter(r => !r.ok)
                        return (
                          <div className="text-xs space-y-0.5">
                            {ok.length > 0 && <div className="text-green-600">Table tag applied.{ok.map(r => r.verified ? ` [${r.verified}]` : '').join('')}</div>}
                            {fail.map((r, ri) => <div key={ri} className="text-red-600">{r.error || 'unknown error'}</div>)}
                          </div>
                        )
                      })()}
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-[10px] font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Override Primary:</span>
                        <input value={recommendedEntity[tbl.table_name] || ''} placeholder="e.g. OrderLineItem"
                          onChange={ev => setRecommendedEntity(p => ({ ...p, [tbl.table_name]: ev.target.value }))}
                          className="text-xs border border-purple-300 dark:border-purple-700/50 rounded px-2 py-0.5 bg-white dark:bg-dbx-navy/60 dark:text-slate-200 w-40" />
                        <button onClick={async () => {
                          const et = (recommendedEntity[tbl.table_name] || '').trim()
                          if (!et) return
                          setRecEntLoading(p => ({ ...p, [tbl.table_name]: true }))
                          try {
                            const r = await fetch('/api/ontology/set-recommended-entity', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                              body: JSON.stringify({ table_name: tbl.table_name, entity_type: et }) })
                            if (r.ok) { setRecommendedEntity(p => ({ ...p, [tbl.table_name]: '' })); loadData() }
                          } catch {}
                          setRecEntLoading(p => ({ ...p, [tbl.table_name]: false }))
                        }} disabled={recEntLoading[tbl.table_name] || !(recommendedEntity[tbl.table_name] || '').trim()}
                          className="text-[10px] px-2 py-0.5 bg-purple-600 text-white rounded hover:bg-purple-700 disabled:opacity-50">
                          {recEntLoading[tbl.table_name] ? '...' : 'Set as Primary'}
                        </button>
                      </div>
                    </div>
                  )}

                  {/* ═══ ZONE 2: Business Concepts ═══ */}
                  {show('ontology') && (
                    <div className="border-t border-slate-200 dark:border-dbx-navy-400/30 pt-3 mt-3 space-y-2">
                      <div className="flex items-center justify-between flex-wrap gap-2">
                        <h4 className="text-xs font-semibold text-slate-600 dark:text-slate-300 uppercase tracking-wider flex items-center">
                          Business Concepts
                          {Array.isArray(tbl.ontology_entities) && (() => {
                            const total = tbl.ontology_entities.length
                            const above = tbl.ontology_entities.filter(e => Number(e.confidence ?? 0) >= ontoConfThreshold).length
                            return total > 0 ? <span className="ml-1.5 text-slate-400 font-normal normal-case">{total} entities ({above} above threshold)</span> : null
                          })()}
                          <Tip text="Applies entity_type tags to the specific columns listed in each entity's source_columns. Individual Apply buttons bypass the confidence threshold." />
                        </h4>
                        <div className="flex items-center gap-2">
                          <label className="text-[10px] text-slate-500">Min Confidence:</label>
                          <input type="number" min="0" max="1" step="0.05" value={ontoConfThreshold}
                            onChange={ev => setOntoConfThreshold(Number(ev.target.value))}
                            className="text-[10px] border border-slate-300 rounded px-1.5 py-0.5 bg-white w-16" />
                          {Array.isArray(tbl.ontology_entities) && tbl.ontology_entities.filter(e => Number(e.confidence ?? 0) >= ontoConfThreshold).length > 0 && (
                            <button onClick={async () => {
                              setOntoApplying(p => ({ ...p, [tbl.table_name]: true })); setOntoApplyResults(p => ({ ...p, [tbl.table_name]: null }))
                              const selections = tbl.ontology_entities.filter(e => Number(e.confidence ?? 0) >= ontoConfThreshold).map(e => {
                                let sc = e.source_columns
                                if (typeof sc === 'string') { try { sc = JSON.parse(sc) } catch {} }
                                return { entity_type: entityTypeOverrides[e.entity_id] || e.entity_type, source_tables: [tbl.table_name], source_columns: Array.isArray(sc) ? sc : [], entity_role: e.entity_role || 'primary' }
                              })
                              try {
                                const r = await fetch('/api/ontology/apply-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ selections }) })
                                const j = await r.json().catch(() => ({}))
                                setOntoApplyResults(p => ({ ...p, [tbl.table_name]: j }))
                              } catch (err) { setOntoApplyResults(p => ({ ...p, [tbl.table_name]: { error: err.message } })) }
                              setOntoApplying(p => ({ ...p, [tbl.table_name]: false }))
                            }} disabled={ontoApplying[tbl.table_name]}
                              className="text-[10px] px-2.5 py-1 bg-purple-600 text-white rounded hover:bg-purple-700 disabled:opacity-50 font-medium">
                              {ontoApplying[tbl.table_name] ? 'Applying...' : `Apply All (conf \u2265 ${ontoConfThreshold})`}
                            </button>
                          )}
                        </div>
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
                            const role = e.entity_role || 'primary'
                            const isPrimary = role === 'primary'
                            const confCls = conf <= 0 ? 'bg-red-100 text-red-700' : conf < 0.5 ? 'bg-yellow-100 text-yellow-700' : 'bg-green-100 text-green-700'
                            const eid = e.entity_id || `${tblIdx}-${i}`
                            const isOpen = expandedEntities[eid]
                            const srcCols = (() => {
                              if (Array.isArray(e.source_columns)) return e.source_columns
                              if (typeof e.source_columns === 'string') { try { const p = JSON.parse(e.source_columns); if (Array.isArray(p)) return p } catch {} }
                              return []
                            })()
                            return (
                              <div key={eid} className={`border rounded-lg ${isPrimary ? 'border-purple-300 bg-purple-50 dark:border-purple-700/50 dark:bg-purple-900/20' : 'border-slate-200 bg-slate-50/50 dark:border-dbx-navy-400/30 dark:bg-dbx-navy-500/30'}`}>
                                <div className="flex items-center gap-2 px-3 py-1.5 cursor-pointer flex-wrap" onClick={() => setExpandedEntities(p => ({ ...p, [eid]: !p[eid] }))}>
                                  <span className="text-xs text-slate-400">{isOpen ? '\u25BC' : '\u25B6'}</span>
                                  <span className={`text-[9px] px-1 py-0.5 rounded font-bold uppercase tracking-wider ${isPrimary ? 'bg-purple-200 text-purple-700' : 'bg-slate-200 text-slate-500'}`}>
                                    {role}
                                  </span>
                                  {e.entity_id ? (
                                    <>
                                      <input list={`eto-${eid}`} value={entityTypeOverrides[e.entity_id] ?? e.entity_type}
                                        onClick={ev => ev.stopPropagation()}
                                        onChange={ev => setEntityTypeOverrides(p => ({ ...p, [e.entity_id]: ev.target.value }))}
                                        onBlur={ev => {
                                          const val = (ev.target.value || '').trim()
                                          if (val && val !== e.entity_type) {
                                            fetch('/api/ontology/update-entity-type', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                                              body: JSON.stringify({ entity_id: e.entity_id, new_entity_type: val }) }).catch(() => {})
                                          }
                                        }}
                                        className="text-xs border border-purple-300 dark:border-purple-700/50 rounded px-1.5 py-0.5 bg-white dark:bg-dbx-navy/60 dark:text-slate-200 w-28" />
                                      <datalist id={`eto-${eid}`}>
                                        {entityTypeOptions.map(t => <option key={t} value={t} />)}
                                      </datalist>
                                    </>
                                  ) : (
                                    <span className="text-xs font-medium text-purple-700">{e.entity_type}</span>
                                  )}
                                  <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${confCls}`}
                                    title="Negative confidence means the AI validator rejected this entity mapping.">
                                    {conf.toFixed(2)}
                                  </span>
                                  {e.validated !== undefined && (
                                    <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${e.validated === true || e.validated === 'true' ? 'bg-blue-100 text-blue-700' : 'bg-slate-100 text-slate-500'}`}>
                                      {e.validated === true || e.validated === 'true' ? 'validated' : 'unvalidated'}
                                    </span>
                                  )}
                                  {srcCols.length > 0 && (
                                    <span className="text-[10px] text-slate-500 dark:text-slate-400">
                                      {srcCols.map((c, ci) => <code key={ci} className="bg-white/80 dark:bg-dbx-navy/40 border border-slate-200 dark:border-dbx-navy-400/30 rounded px-1 py-0.5 text-slate-600 dark:text-slate-300 mr-1">{c}</code>)}
                                    </span>
                                  )}
                                  <button onClick={async (ev) => {
                                    ev.stopPropagation()
                                    const et = entityTypeOverrides[e.entity_id] || e.entity_type
                                    try {
                                      const r = await fetch('/api/ontology/apply-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({ selections: [{ entity_type: et, source_tables: [tbl.table_name], source_columns: srcCols, entity_role: role }] }) })
                                      const j = await r.json().catch(() => ({}))
                                      setOntoApplyResults(p => ({ ...p, [tbl.table_name]: j }))
                                    } catch (err) { setOntoApplyResults(p => ({ ...p, [tbl.table_name]: { error: err.message } })) }
                                  }} className="ml-auto text-[10px] px-1.5 py-0.5 bg-purple-600 text-white rounded hover:bg-purple-700"
                                    title={isPrimary ? 'Apply entity_type tag to table and columns (ignores threshold)' : 'Apply entity_type tag to columns only (ignores threshold)'}>
                                    Apply Tag
                                  </button>
                                </div>
                                {isOpen && e.validation_notes && (
                                  <div className="px-3 pb-2 border-t border-purple-200 dark:border-purple-700/30 mt-0">
                                    <div className="mt-1.5">
                                      <span className="text-[10px] font-semibold text-slate-500 dark:text-slate-400">Validation Notes:</span>
                                      <p className="text-xs text-slate-600 dark:text-slate-300 italic bg-white/60 dark:bg-dbx-navy/40 rounded p-1.5 mt-0.5">{e.validation_notes}</p>
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

                  {/* ═══ ZONE 3: Column Properties ═══ */}
                  {show('ontology') && (tbl.column_properties || []).length > 0 && (
                    <div className="border-t border-slate-200 dark:border-dbx-navy-400/30 pt-3 mt-3 space-y-2">
                      <div className="flex items-center justify-between">
                        <h4 className="text-xs font-semibold text-slate-600 dark:text-slate-300 uppercase tracking-wider flex items-center">
                          Column Properties
                          <span className="ml-1.5 text-slate-400 font-normal normal-case">{(tbl.column_properties || []).length} columns classified</span>
                          <Tip text="Applies property_role (and linked_entity_type for links/FKs) as tags on each column. Reflects how the column functions within the entity model (identifier, attribute, measure, link, etc.). Also saved to the knowledge base." />
                        </h4>
                        <div className="flex items-center gap-2">
                          <button onClick={async () => {
                            setPropTagApplying(p => ({ ...p, [tbl.table_name]: true }))
                            const items = (tbl.column_properties || []).map(cp => ({
                              table_name: cp.table_name, column_name: cp.column_name,
                              property_role: colPropOverrides[cp.property_id]?.property_role ?? cp.property_role,
                              linked_entity_type: colPropOverrides[cp.property_id]?.linked_entity_type ?? cp.linked_entity_type,
                            }))
                            try {
                              const r = await fetch('/api/ontology/apply-property-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ items }) })
                              const j = await r.json().catch(() => ({}))
                              setPropTagResults(p => ({ ...p, [tbl.table_name]: j }))
                            } catch (err) { setPropTagResults(p => ({ ...p, [tbl.table_name]: { error: err.message } })) }
                            setPropTagApplying(p => ({ ...p, [tbl.table_name]: false }))
                          }} disabled={propTagApplying[tbl.table_name]}
                            className="text-[10px] px-2.5 py-1 bg-emerald-600 text-white rounded hover:bg-emerald-700 disabled:opacity-50 font-medium">
                            {propTagApplying[tbl.table_name] ? 'Applying...' : 'Apply Property Role Tags'}
                          </button>
                        </div>
                      </div>
                      {propTagResults[tbl.table_name] && (() => {
                        const pr = propTagResults[tbl.table_name]
                        if (pr.error) return <span className="text-[10px] text-red-600">{pr.error}</span>
                        const ok = (pr.results || []).filter(r => r.ok).length
                        const fail = (pr.results || []).filter(r => !r.ok).length
                        return <span className={`text-[10px] ${fail ? 'text-amber-600' : 'text-green-600'}`}>{ok} tagged{fail ? `, ${fail} failed` : ''}</span>
                      })()}
                    </div>
                  )}

                  {/* FK predictions */}
                  {show('fk') && (
                    <div className="space-y-2">
                      <div className="flex items-center justify-between flex-wrap gap-2">
                        <h4 className="text-xs font-semibold text-slate-500 dark:text-slate-400">Foreign Key Predictions</h4>
                        {Array.isArray(tbl.fk_predictions) && tbl.fk_predictions.length > 0 && (() => {
                          const tblKey = tbl.table_name
                          const selSet = selectedFKs[tblKey] || new Set()
                          const selCount = selSet.size
                          const selPreds = () => [...selSet].map(idx => tbl.fk_predictions[idx]).filter(Boolean).map(fk => ({
                            src_table: fk.src_table, src_column: fk.src_column, dst_table: fk.dst_table, dst_column: fk.dst_column,
                          }))
                          return selCount > 0 ? (
                            <div className="flex items-center gap-1.5">
                              <button onClick={async () => {
                                setFkApplying(true); setFkApplyResult(null)
                                try {
                                  const r = await fetch('/api/analytics/fk-apply-from-predictions', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ predictions: selPreds() }) })
                                  const j = await r.json().catch(() => ({}))
                                  setFkApplyResult(j)
                                  setSelectedFKs(p => ({ ...p, [tblKey]: new Set() }))
                                } catch (err) { setFkApplyResult({ error: err.message }) }
                                setFkApplying(false)
                              }} disabled={fkApplying} className="text-xs px-2 py-0.5 bg-indigo-600 text-white rounded hover:bg-indigo-700 disabled:opacity-50"
                                title="Requires MANAGE on source tables">
                                {fkApplying ? 'Applying...' : `Apply FK Constraints (${selCount})`}
                              </button>
                              <button onClick={async () => {
                                try {
                                  const r = await fetch('/api/analytics/fk-generate-sql', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ predictions: selPreds() }) })
                                  const j = await r.json().catch(() => ({}))
                                  setFkGenSql(j.sql || '')
                                } catch (err) { setFkGenSql(`-- Error: ${err.message}`) }
                              }} className="text-xs px-2 py-0.5 bg-slate-600 text-white rounded hover:bg-slate-700">
                                Generate SQL ({selCount})
                              </button>
                            </div>
                          ) : null
                        })()}
                      </div>
                      <p className="text-[10px] text-slate-400 dark:text-slate-500 italic">
                        Applying FK constraints requires MANAGE on the source table. If dbxmetagen only has APPLY_TAG, use "Tag All" above instead -- it sets <code className="text-[10px]">fk_references</code> column tags which only require APPLY_TAG.
                      </p>
                      {fkGenSql != null && (
                        <div className="relative">
                          <pre className="text-[10px] bg-slate-800 dark:bg-slate-900 text-green-300 dark:text-green-400 rounded p-2 overflow-x-auto whitespace-pre-wrap max-h-40 font-mono">{fkGenSql}</pre>
                          <div className="absolute top-1 right-1 flex gap-1">
                            <button onClick={() => { navigator.clipboard.writeText(fkGenSql) }}
                              className="text-[9px] px-1.5 py-0.5 bg-slate-700 border border-slate-600 rounded hover:bg-slate-600 text-slate-200">Copy</button>
                            <button onClick={() => setFkGenSql(null)}
                              className="text-[9px] px-1.5 py-0.5 bg-slate-700 border border-slate-600 rounded hover:bg-slate-600 text-slate-200">Close</button>
                          </div>
                        </div>
                      )}
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
                      ) : (() => {
                        const shortTbl = (name) => (name || '').split('.').pop()
                        const tblKey = tbl.table_name
                        const selSet = selectedFKs[tblKey] || new Set()
                        const outgoing = tbl.fk_predictions.map((fk, i) => ({ fk, origIdx: i })).filter(({ fk }) => fk.src_table === tbl.table_name)
                        const incoming = tbl.fk_predictions.map((fk, i) => ({ fk, origIdx: i })).filter(({ fk }) => fk.dst_table === tbl.table_name && fk.src_table !== tbl.table_name)

                        const renderRow = ({ fk, origIdx }, localCol, remoteCol) => {
                          const fconf = Number(fk.final_confidence ?? 0)
                          const confCls = fconf < 0.3 ? 'bg-red-100 text-red-700' : fconf < 0.6 ? 'bg-yellow-100 text-yellow-700' : 'bg-green-100 text-green-700'
                          const fkKey = `${tblKey}-${origIdx}`
                          const isExpReasoning = expandedFKs[fkKey]
                          return (
                            <React.Fragment key={origIdx}>
                              <tr className={`border-b border-slate-100 dark:border-dbx-navy-400/20 hover:bg-indigo-50/30 dark:hover:bg-dbx-navy-500/30 ${selSet.has(origIdx) ? 'bg-indigo-50/50 dark:bg-indigo-900/20' : ''}`}>
                                <td className="px-1 py-1">
                                  <input type="checkbox" checked={selSet.has(origIdx)} onChange={() => {
                                    setSelectedFKs(prev => {
                                      const ns = new Set(prev[tblKey] || [])
                                      if (ns.has(origIdx)) ns.delete(origIdx); else ns.add(origIdx)
                                      return { ...prev, [tblKey]: ns }
                                    })
                                  }} className="rounded border-slate-300" />
                                </td>
                                <td className="px-2 py-1 text-slate-600 dark:text-slate-300 font-mono">{localCol}</td>
                                <td className="px-2 py-1 text-slate-600 dark:text-slate-300 font-mono">{remoteCol}</td>
                                <td className="px-2 py-1"><span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${confCls}`}>{fconf.toFixed(2)}</span></td>
                                <td className="px-2 py-1 text-slate-500 dark:text-slate-400">{Number(fk.ai_confidence ?? 0).toFixed(2)}</td>
                                <td className="px-2 py-1 text-slate-500 dark:text-slate-400">{Number(fk.col_similarity ?? 0).toFixed(2)}</td>
                                <td className="px-2 py-1 text-slate-500 dark:text-slate-400 max-w-xs truncate cursor-pointer" title="Click to expand"
                                  onClick={() => setExpandedFKs(p => ({ ...p, [fkKey]: !p[fkKey] }))}>
                                  {fk.ai_reasoning || '--'}
                                </td>
                              </tr>
                              {isExpReasoning && fk.ai_reasoning && (
                                <tr><td colSpan={7} className="px-3 py-2 bg-slate-50 dark:bg-dbx-navy-500/30 text-xs text-slate-600 dark:text-slate-300 italic">{fk.ai_reasoning}</td></tr>
                              )}
                            </React.Fragment>
                          )
                        }

                        return (
                          <div className="space-y-3">
                            {outgoing.length > 0 && (
                              <div className="overflow-x-auto">
                                <p className="text-[10px] font-semibold text-indigo-500 dark:text-indigo-400 uppercase tracking-wider mb-1">&#8594; Outgoing (this table references)</p>
                                <table className="min-w-full text-xs">
                                  <thead><tr className="bg-dbx-oat/50 dark:bg-dbx-navy-500/50">
                                    <th className="w-8 px-1 py-1"></th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400">Column</th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400">References</th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400" title="Combined final confidence">Final</th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400" title="AI model confidence">AI</th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400" title="Column embedding similarity">Sim</th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400">Reasoning</th>
                                  </tr></thead>
                                  <tbody>
                                    {outgoing.map(item => renderRow(item,
                                      (item.fk.src_column || '').split('.').pop(),
                                      `${shortTbl(item.fk.dst_table)}.${(item.fk.dst_column || '').split('.').pop()}`
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            )}
                            {incoming.length > 0 && (
                              <div className="overflow-x-auto">
                                <p className="text-[10px] font-semibold text-teal-500 dark:text-teal-400 uppercase tracking-wider mb-1">&#8592; Incoming (referenced by)</p>
                                <table className="min-w-full text-xs">
                                  <thead><tr className="bg-dbx-oat/50 dark:bg-dbx-navy-500/50">
                                    <th className="w-8 px-1 py-1"></th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400">From</th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400">Column</th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400" title="Combined final confidence">Final</th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400" title="AI model confidence">AI</th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400" title="Column embedding similarity">Sim</th>
                                    <th className="text-left px-2 py-1 font-semibold text-slate-500 dark:text-slate-400">Reasoning</th>
                                  </tr></thead>
                                  <tbody>
                                    {incoming.map(item => renderRow(item,
                                      `${shortTbl(item.fk.src_table)}.${(item.fk.src_column || '').split('.').pop()}`,
                                      (item.fk.dst_column || '').split('.').pop()
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            )}
                            {outgoing.length === 0 && incoming.length === 0 && (
                              <p className="text-xs text-slate-400 italic">No foreign key predictions for this table.</p>
                            )}
                          </div>
                        )
                      })()}
                    </div>
                  )}
                </div>
              )}
            </div>
          )})}
        </div>
      )}

      {/* Export bar */}
      {reviewData.length > 0 && (
        <div className="card p-5 space-y-3">
          <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">Export & Apply</h3>
          <div className="flex flex-wrap items-center gap-3">
            <select value={ddlCategory} onChange={e => { setDdlCategory(e.target.value); setDdlSql(''); setDdlApplyResult(null) }}
              className="border border-slate-300 dark:border-dbx-navy-400/40 rounded-lg px-3 py-1.5 text-sm bg-white dark:bg-dbx-navy/60 dark:text-slate-200">
              <option value="comments">Comments</option>
              <option value="domain">Domain Tags</option>
              <option value="sensitivity">Sensitivity Tags</option>
            </select>
            {ddlCategory === 'domain' && (
              <>
                <input value={customDomainKey} onChange={e => setCustomDomainKey(e.target.value)} placeholder="domain tag key (default: domain)"
                  className="border border-slate-300 dark:border-dbx-navy-400/40 rounded px-2 py-1.5 text-sm w-52 bg-white dark:bg-dbx-navy/60 dark:text-slate-200" />
                <input value={customSubdomainKey} onChange={e => setCustomSubdomainKey(e.target.value)} placeholder="subdomain tag key (default: subdomain)"
                  className="border border-slate-300 dark:border-dbx-navy-400/40 rounded px-2 py-1.5 text-sm w-56 bg-white dark:bg-dbx-navy/60 dark:text-slate-200" />
              </>
            )}
            {ddlCategory === 'sensitivity' && (
              <>
                <input value={customSensitivityKey} onChange={e => setCustomSensitivityKey(e.target.value)} placeholder="classification tag key (default: data_classification)"
                  className="border border-slate-300 dark:border-dbx-navy-400/40 rounded px-2 py-1.5 text-sm w-64 bg-white dark:bg-dbx-navy/60 dark:text-slate-200" />
                <input value={customSensitivityTypeKey} onChange={e => setCustomSensitivityTypeKey(e.target.value)} placeholder="subclassification tag key (default: data_subclassification)"
                  className="border border-slate-300 dark:border-dbx-navy-400/40 rounded px-2 py-1.5 text-sm w-68 bg-white dark:bg-dbx-navy/60 dark:text-slate-200" />
              </>
            )}
            <button onClick={generateDdl} disabled={ddlLoading} className="px-4 py-1.5 bg-dbx-oat dark:bg-dbx-navy-500 text-slate-700 dark:text-slate-200 rounded-lg text-sm font-medium hover:bg-dbx-oat-dark dark:hover:bg-dbx-navy-400 disabled:opacity-50">Generate DDL</button>
            <button onClick={applyDdl} disabled={ddlLoading} className="px-4 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 disabled:opacity-50">Generate &amp; Apply DDL</button>
            <span className="border-l border-slate-300 dark:border-dbx-navy-400/40 h-6" />
            <button onClick={() => exportVolume('tsv')} disabled={exportLoading} className="px-4 py-1.5 bg-dbx-oat dark:bg-dbx-navy-500 text-slate-700 dark:text-slate-200 rounded-lg text-sm font-medium hover:bg-dbx-oat-dark dark:hover:bg-dbx-navy-400 disabled:opacity-50">Export TSV</button>
            <button onClick={() => exportVolume('excel')} disabled={exportLoading} className="px-4 py-1.5 bg-dbx-oat dark:bg-dbx-navy-500 text-slate-700 dark:text-slate-200 rounded-lg text-sm font-medium hover:bg-dbx-oat-dark dark:hover:bg-dbx-navy-400 disabled:opacity-50">Export Excel</button>
            <span className="border-l border-slate-300 h-6" />
            <button onClick={openVolumeBrowser} disabled={importLoading} className="px-4 py-1.5 bg-slate-600 text-white rounded-lg text-sm font-medium hover:bg-slate-700 disabled:opacity-50">Import from Volume</button>
            <label className={`px-4 py-1.5 bg-slate-600 text-white rounded-lg text-sm font-medium hover:bg-slate-700 cursor-pointer ${importLoading ? 'opacity-50 pointer-events-none' : ''}`}>
              Upload File
              <input type="file" accept=".tsv,.xlsx,.xls" onChange={importUpload} className="hidden" />
            </label>
          </div>
          {ddlError && <p className="text-sm text-red-600">{ddlError}</p>}
          {ddlApplyResult && (ddlApplyResult.ok
            ? <p className="text-sm text-green-600">Applied {ddlApplyResult.applied} statement(s).</p>
            : ddlApplyResult.detail
              ? <p className="text-sm text-red-600">{ddlApplyResult.detail}</p>
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
            : <p className="text-sm text-red-600">{errMsg(exportResult.detail)}</p>)}
          {importResult && (importResult.ok
            ? <p className="text-sm text-green-600">Imported {importResult.total_rows} rows: {importResult.tables_updated} tables, {importResult.columns_updated} columns updated{importResult.skipped ? `, ${importResult.skipped} skipped` : ''}{importResult.saved_to ? ` (saved to ${importResult.saved_to})` : ''}</p>
            : <p className="text-sm text-red-600">Import failed: {errMsg(importResult.detail)}</p>)}
          {ddlSql && (
            <div className="relative">
              <pre className="text-xs text-slate-800 dark:text-slate-200 bg-dbx-oat dark:bg-dbx-navy-600 border border-slate-200 dark:border-dbx-navy-400/30 rounded-lg p-3 overflow-auto max-h-64 whitespace-pre-wrap">{ddlSql}</pre>
              <button onClick={() => navigator.clipboard.writeText(ddlSql)} className="absolute top-2 right-2 px-2 py-1 bg-dbx-oat-light dark:bg-dbx-navy-500 border border-slate-200 dark:border-dbx-navy-400/30 rounded text-xs text-slate-700 dark:text-slate-200 hover:bg-dbx-oat dark:hover:bg-dbx-navy-400">Copy</button>
            </div>
          )}
        </div>
      )}

      {/* Standalone import section (always visible) */}
      {reviewData.length === 0 && (
        <div className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-600 p-4 shadow-sm space-y-3">
          <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">Import Reviewed Metadata</h3>
          <p className="text-xs text-slate-500 dark:text-slate-400">Import a previously exported and reviewed TSV or Excel file to update the knowledge base tables.</p>
          <div className="flex items-center gap-3">
            <button onClick={openVolumeBrowser} disabled={importLoading} className="px-4 py-1.5 bg-slate-600 text-white rounded-lg text-sm font-medium hover:bg-slate-700 disabled:opacity-50">Import from Volume</button>
            <label className={`px-4 py-1.5 bg-slate-600 text-white rounded-lg text-sm font-medium hover:bg-slate-700 cursor-pointer ${importLoading ? 'opacity-50 pointer-events-none' : ''}`}>
              Upload File
              <input type="file" accept=".tsv,.xlsx,.xls" onChange={importUpload} className="hidden" />
            </label>
            {importLoading && <span className="text-sm text-slate-500">Importing...</span>}
          </div>
          {importResult && (importResult.ok
            ? <p className="text-sm text-green-600">Imported {importResult.total_rows} rows: {importResult.tables_updated} tables, {importResult.columns_updated} columns updated{importResult.skipped ? `, ${importResult.skipped} skipped` : ''}</p>
            : <p className="text-sm text-red-600">Import failed: {errMsg(importResult.detail)}</p>)}
        </div>
      )}

      {/* Volume file browser modal */}
      {showVolumeBrowser && (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-6 animate-fade-in" role="dialog" aria-modal="true">
          <div className="bg-white dark:bg-dbx-navy-650 rounded-2xl shadow-elevated max-w-lg w-full max-h-[70vh] flex flex-col animate-slide-up">
            <div className="flex items-center justify-between px-6 pt-5 pb-3 border-b border-slate-200 dark:border-slate-600">
              <h3 className="text-sm font-bold text-slate-800 dark:text-slate-100">Select File from Volume</h3>
              <button onClick={() => setShowVolumeBrowser(false)} className="text-slate-400 hover:text-slate-600 dark:hover:text-slate-300" aria-label="Close">
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
              </button>
            </div>
            <div className="flex-1 overflow-y-auto px-6 py-3">
              {volumeFilesLoading ? (
                <p className="text-sm text-slate-500 py-4">Loading files...</p>
              ) : volumeFilesError ? (
                <p className="text-sm text-red-600 py-4">{volumeFilesError}</p>
              ) : volumeFiles.length === 0 ? (
                <p className="text-sm text-slate-400 py-4">No TSV or Excel files found in the volume.</p>
              ) : (
                <div className="space-y-1">
                  {volumeFiles.map((f, i) => (
                    <button key={i} onClick={() => importFromVolume(f.path)}
                      className="w-full text-left px-3 py-2 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 transition-colors">
                      <div className="text-sm font-medium text-slate-700 dark:text-slate-200 truncate">{f.name}</div>
                      <div className="text-xs text-slate-400 dark:text-slate-500 truncate">{f.path}</div>
                    </button>
                  ))}
                </div>
              )}
            </div>
            <div className="px-6 py-3 border-t border-slate-200 dark:border-slate-600">
              <button onClick={() => setShowVolumeBrowser(false)} className="px-4 py-1.5 text-sm text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-lg">Cancel</button>
            </div>
          </div>
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
            {otherCols.slice(0, 4).map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-800 dark:text-slate-200 border-b border-slate-200 text-xs uppercase">{c}</th>)}
            {editable.map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-800 dark:text-slate-200 border-b border-slate-200 text-xs uppercase">{c}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={`border-b border-slate-100 ${original && (editable.some(f => row[f] !== original[i]?.[f])) ? 'bg-amber-50' : ''} hover:bg-orange-50/30`}>
              {otherCols.slice(0, 4).map(c => <td key={c} className="px-3 py-2 max-w-[12rem] truncate text-slate-700 dark:text-slate-300">{String(row[c] ?? '')}</td>)}
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
            {otherCols.slice(0, 5).map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-800 dark:text-slate-200 border-b border-slate-200 text-xs uppercase">{c}</th>)}
            {editable.map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-800 dark:text-slate-200 border-slate-200 text-xs uppercase">{c}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={`border-b border-slate-100 ${original && editable.some(f => row[f] !== original[i]?.[f]) ? 'bg-amber-50' : ''} hover:bg-orange-50/30`}>
              {otherCols.slice(0, 5).map(c => <td key={c} className="px-3 py-2 max-w-[10rem] truncate text-slate-700 dark:text-slate-300">{String(row[c] ?? '')}</td>)}
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
            {otherCols.slice(0, 4).map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-800 dark:text-slate-200 border-b border-slate-200 text-xs uppercase">{c}</th>)}
            {editable.map(c => <th key={c} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-800 dark:text-slate-200 border-b border-slate-200 text-xs uppercase">{c}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={`border-b border-slate-100 ${original && editable.some(f => row[f] !== original[i]?.[f]) ? 'bg-amber-50' : ''} hover:bg-orange-50/30`}>
              {otherCols.slice(0, 4).map(c => <td key={c} className="px-3 py-2 max-w-[10rem] truncate text-slate-700 dark:text-slate-300">{String(row[c] ?? '')}</td>)}
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

function EntityTagsPanel() {
  const [entities, setEntities] = useState([])
  const [relationships, setRelationships] = useState([])
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(new Set())
  const [applyResult, setApplyResult] = useState(null)
  const [applying, setApplying] = useState(false)
  const [expanded, setExpanded] = useState(new Set())
  const shortName = id => (id || '').split('.').pop()

  useEffect(() => {
    safeFetch('/api/ontology/entities').then(r => { setEntities(r.data || []); if (r.error) setError(r.error) })
    safeFetch('/api/ontology/relationships').then(r => { setRelationships(r.data || []) })
  }, [])

  const groupedEntities = useMemo(() => {
    const map = new Map()
    entities.forEach(e => {
      const st = Array.isArray(e.source_tables) ? e.source_tables : (typeof e.source_tables === 'string' ? (() => { try { return JSON.parse(e.source_tables) } catch { return [e.source_tables] } })() : [])
      const key = `${e.entity_type}::${st.sort().join(',')}`
      const existing = map.get(key)
      if (!existing || (Number(e.confidence) || 0) > (Number(existing.confidence) || 0))
        map.set(key, { ...e, source_tables: st, _count: (existing?._count || 0) + 1 })
      else map.set(key, { ...existing, _count: existing._count + 1 })
    })
    return Array.from(map.values())
  }, [entities])

  const sourceTablesList = (e) => {
    const st = e.source_tables
    if (Array.isArray(st)) return st
    if (typeof st === 'string') { try { const p = JSON.parse(st); return Array.isArray(p) ? p : [st] } catch { return [st] } }
    return []
  }

  const applyToTable = async () => {
    if (selected.size === 0) return
    setApplying(true); setApplyResult(null)
    const selections = []
    selected.forEach(i => {
      const e = groupedEntities[i]
      const tables = sourceTablesList(e)
      if (e?.entity_type && tables.length) selections.push({ entity_type: e.entity_type, source_tables: tables })
    })
    if (!selections.length) { setApplying(false); return }
    try {
      const r = await fetch('/api/ontology/apply-tags', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ selections }) })
      const j = await r.json().catch(() => ({}))
      setApplyResult(r.ok ? j : { error: j.detail || j })
    } catch (e) { setApplyResult({ error: e.message }) }
    setApplying(false)
  }

  return (
    <div className="card p-6">
      <ErrorBanner error={error} />
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-slate-800">Discovered Entities</h2>
        {selected.size > 0 && (
          <button onClick={applyToTable} disabled={applying}
            className="px-4 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 disabled:opacity-50">
            Apply tags ({selected.size})
          </button>
        )}
      </div>
      {applyResult && (
        <div className={`mb-4 text-sm ${applyResult.error ? 'text-red-600' : 'text-green-600'}`}>
          {applyResult.error ? JSON.stringify(applyResult.error) : `Applied: ${(applyResult.results || []).filter(r => r.ok).length} ok, ${(applyResult.results || []).filter(r => !r.ok).length} failed.`}
        </div>
      )}
      {groupedEntities.length === 0
        ? <p className="text-sm text-slate-400">No entities discovered yet.</p>
        : <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead><tr>
                <th className="w-10 px-2 py-2.5 bg-dbx-oat border-b border-slate-200"></th>
                {['Entity', 'Type', 'Conf', 'Validated', 'Source Tables', 'Columns / Bindings'].map(h =>
                  <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat font-semibold text-slate-800 dark:text-slate-200 border-b border-slate-200 text-xs uppercase tracking-wider">{h}</th>)}
              </tr></thead>
              <tbody>
                {groupedEntities.map((e, i) => {
                  const bindings = Array.isArray(e.column_bindings) ? e.column_bindings
                    : typeof e.column_bindings === 'string' ? (() => { try { const p = JSON.parse(e.column_bindings); return Array.isArray(p) ? p : [] } catch { return [] } })()
                    : []
                  const srcCols = Array.isArray(e.source_columns) ? e.source_columns
                    : typeof e.source_columns === 'string' ? (() => { try { const p = JSON.parse(e.source_columns); return Array.isArray(p) ? p : [] } catch { return [] } })()
                    : []
                  const isExpanded = expanded.has(i)
                  const hasDetail = bindings.length > 0 || srcCols.length > 0
                  return (
                    <React.Fragment key={i}>
                      <tr className={`border-b border-slate-100 hover:bg-orange-50/30 ${selected.has(i) ? 'bg-orange-50/50' : ''}`}>
                        <td className="px-2 py-2"><input type="checkbox" checked={selected.has(i)} onChange={() => setSelected(prev => { const n = new Set(prev); n.has(i) ? n.delete(i) : n.add(i); return n })} className="rounded border-slate-300" /></td>
                        <td className="px-3 py-2 font-medium text-slate-700">{e.entity_name}</td>
                        <td className="px-3 py-2"><span className="inline-block bg-orange-100 text-red-700 text-xs font-medium px-2 py-0.5 rounded-full">{e.entity_type}</span></td>
                        <td className="px-3 py-2 text-slate-600">{Number(e.confidence).toFixed(2)}</td>
                        <td className="px-3 py-2">{e.validated === 'true' || e.validated === true ? <span className="text-emerald-600 font-medium">Yes</span> : <span className="text-slate-400">No</span>}</td>
                        <td className="px-3 py-2 max-w-xs truncate text-slate-500">{Array.isArray(e.source_tables) ? e.source_tables.map(shortName).join(', ') : String(e.source_tables ?? '')}</td>
                        <td className="px-3 py-2">{hasDetail ? (
                          <button onClick={() => setExpanded(prev => { const n = new Set(prev); n.has(i) ? n.delete(i) : n.add(i); return n })}
                            className="text-xs text-blue-600 hover:underline">
                            {bindings.length > 0 ? `${bindings.length} mappings` : `${srcCols.length} cols`}{isExpanded ? ' (hide)' : ' (show)'}
                          </button>
                        ) : <span className="text-xs text-slate-400">--</span>}</td>
                      </tr>
                      {isExpanded && hasDetail && (
                        <tr className="bg-slate-50/60"><td></td><td colSpan={6} className="px-3 py-2">
                          {bindings.length > 0 && <div className="mb-1"><span className="text-xs font-semibold text-slate-500 mr-2">Attribute mappings:</span>
                            <span className="flex flex-wrap gap-1 mt-0.5">{bindings.map((b, bi) => <span key={bi} className="inline-block bg-indigo-50 text-indigo-700 text-xs px-1.5 py-0.5 rounded">{b.attribute_name || '?'} &larr; {shortName(b.bound_column || '')}</span>)}</span></div>}
                          {srcCols.length > 0 && bindings.length === 0 && <div><span className="text-xs font-semibold text-slate-500 mr-2">Source columns:</span><span className="text-xs text-slate-600">{srcCols.join(', ')}</span></div>}
                        </td></tr>
                      )}
                    </React.Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>
      }
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

  useEffect(() => { if (!['editor', 'fk_apply', 'entity_tags'].includes(tab)) load(tab) }, [tab])

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
    if (!confirm('Apply DDL changes to your catalog? This modifies table/column metadata.')) return
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
      <PageHeader title="Review & Apply" subtitle="Review, edit, and apply metadata changes" />
      <ErrorBanner error={error} />
      <div className="flex flex-wrap items-center gap-4">
        <div className="inline-flex bg-dbx-oat/60 dark:bg-dbx-navy-650 rounded-xl p-1 shadow-inner-soft">
          {[['editor', 'Review Editor'], ['fk_apply', 'FK Apply'], ['entity_tags', 'Entity Tags'], ['kb', 'Table KB'], ['columns', 'Column KB'], ['schemas', 'Schema KB'], ['log', 'Generation Log']]
            // NOTE: Only 'editor' and 'log' are shipped for now.
            // The other tabs (fk_apply, entity_tags, kb, columns, schemas) are stubbed out
            // for future release -- keep the code but hide the tabs until ready.
            .filter(([k]) => ['editor', 'log'].includes(k))
            .map(([k, l]) => (
            <button key={k} onClick={() => setTab(k)}
              className={`px-3.5 py-1.5 text-sm rounded-lg transition-all duration-200 ${tab === k ? 'bg-white dark:bg-dbx-navy-500 shadow-sm font-semibold text-dbx-lava' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`}>{l}</button>
          ))}
        </div>
        {!['editor', 'fk_apply', 'entity_tags'].includes(tab) && <>
          {tab === 'schemas' && (
            <input value={filterExtra} onChange={e => setFilterExtra(e.target.value)} placeholder="Filter by schema name..."
              className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm w-48 focus:ring-2 focus:ring-orange-500" aria-label="Filter by schema name" />
          )}
          {(tab === 'kb' || tab === 'log') && (
            <>
              <input value={filterTable} onChange={e => setFilterTable(e.target.value)} placeholder="Filter by table name..."
                className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm w-48 focus:ring-2 focus:ring-orange-500" aria-label="Filter by table name" />
              {tab === 'kb' && <input value={filterExtra} onChange={e => setFilterExtra(e.target.value)} placeholder="Schema name (exact)"
                className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm w-40 focus:ring-2 focus:ring-orange-500" aria-label="Filter by schema name" />}
            </>
          )}
          {tab === 'columns' && (
            <>
              <input value={filterTable} onChange={e => setFilterTable(e.target.value)} placeholder="Filter by table name..."
                className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm w-48 focus:ring-2 focus:ring-orange-500" aria-label="Filter by table name" />
              <input value={filterExtra} onChange={e => setFilterExtra(e.target.value)} placeholder="Filter by column name..."
                className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm w-48 focus:ring-2 focus:ring-orange-500" aria-label="Filter by column name" />
            </>
          )}
          <button onClick={() => load(tab)} className="px-4 py-1.5 bg-dbx-oat text-slate-700 rounded-lg text-sm font-medium hover:bg-dbx-oat-dark">Search</button>
          {isEditable && modifiedCount > 0 && (
            <button onClick={handleUpdate} disabled={saving}
              className="px-4 py-1.5 bg-dbx-lava text-white rounded-lg text-sm font-medium hover:bg-red-700 shadow-sm disabled:opacity-50">Save changes ({modifiedCount})</button>
          )}
        </>}
      </div>
      {tab === 'editor' ? <ReviewEditor /> :
       tab === 'fk_apply' ? <FKApplyPanel /> :
       tab === 'entity_tags' ? <EntityTagsPanel /> : (
        <div className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-600 p-4 shadow-sm">
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
