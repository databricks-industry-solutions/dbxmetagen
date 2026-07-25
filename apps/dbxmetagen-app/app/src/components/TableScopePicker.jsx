import React, { useEffect, useMemo, useRef } from 'react'
import { useCatalogSchemaTables } from '../hooks/useCatalogSchemaTables'

/**
 * Shared table-scope control used by the core-metadata screen and the analytics
 * pipeline. Replaces the ambiguous "blank = all tables" convention with an
 * EXPLICIT segmented choice:
 *
 *   ( All tables )  ( Selected tables )
 *
 * "All" is a visible, deliberate choice (scope = {mode:'all', tables:[]}).
 * "Selected" reveals a catalog/schema/filter picker with a checkbox list and
 * removable selected-badges. Scope is always one or the other -- never an empty
 * string the caller has to guess about.
 *
 * Props:
 *   value      - {mode:'all'|'selected', tables:string[]} (controlled)
 *   onChange   - (scope) => void
 *   kbOnly     - restrict the picker to knowledge-base tables (default false)
 *   seedTables - string[] to seed the selection with on first mount when the
 *                caller starts in 'selected' mode with no tables yet
 *   allLabel / selectedLabel - optional segment labels
 */
export default function TableScopePicker({
  value,
  onChange,
  kbOnly = false,
  seedTables = [],
  allowAll = true,
  allLabel = 'All tables',
  selectedLabel = 'Selected tables',
}) {
  const mode = allowAll ? (value?.mode || 'all') : 'selected'
  const tables = value?.tables || []

  const cst = useCatalogSchemaTables('', '', { kbOnly })
  const {
    catalogs, schemas, filtered: filteredTables, tables: allTables,
    allSchemaTableCount, catalog, schema, filter,
    setCatalog, setSchema, setFilter, error,
  } = cst

  // Seed once when entering 'selected' with an empty set (e.g. carried over from
  // another screen's selection).
  const seeded = useRef(false)
  useEffect(() => {
    if (mode === 'selected' && !seeded.current && tables.length === 0 && seedTables.length > 0) {
      seeded.current = true
      onChange({ mode: 'selected', tables: [...seedTables] })
    }
  }, [mode, tables.length, seedTables, onChange])

  const selectedSet = useMemo(() => new Set(tables), [tables])
  const fq = (t) => (t.includes('.') ? t : `${catalog}.${schema}.${t}`)

  const setMode = (m) => {
    if (m === 'all') onChange({ mode: 'all', tables: [] })
    else onChange({ mode: 'selected', tables })
  }
  const toggle = (t) => {
    const name = fq(t)
    const next = selectedSet.has(name) ? tables.filter(x => x !== name) : [...tables, name]
    onChange({ mode: 'selected', tables: next })
  }
  const selectAllInSchema = () => {
    const names = filteredTables.map(fq)
    const merged = Array.from(new Set([...tables, ...names]))
    onChange({ mode: 'selected', tables: merged })
  }
  const clearSchema = () => {
    const inSchema = new Set(filteredTables.map(fq))
    onChange({ mode: 'selected', tables: tables.filter(t => !inSchema.has(t)) })
  }
  const remove = (t) => onChange({ mode: 'selected', tables: tables.filter(x => x !== t) })
  const clearAll = () => onChange({ mode: 'selected', tables: [] })

  const seg = (active) =>
    `px-3 py-1 text-xs rounded-md transition ${active
      ? 'bg-white dark:bg-dbx-navy-500 shadow-sm font-semibold text-dbx-lava'
      : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`

  return (
    <div className="space-y-2">
      {/* Segmented All / Selected (All hidden when the caller disallows it) */}
      {allowAll && (
        <div className="inline-flex bg-slate-100 dark:bg-dbx-navy-600 rounded-lg p-0.5">
          <button type="button" onClick={() => setMode('all')} className={seg(mode === 'all')}>{allLabel}</button>
          <button type="button" onClick={() => setMode('selected')} className={seg(mode === 'selected')}>
            {selectedLabel}{tables.length > 0 ? ` (${tables.length})` : ''}
          </button>
        </div>
      )}

      {mode === 'all' ? (
        <p className="text-xs text-slate-500 dark:text-slate-400">
          Runs against <strong>all {kbOnly ? 'knowledge-base ' : ''}tables in scope</strong>.
        </p>
      ) : (
        <div className="space-y-2">
          {error && (
            <p className="text-xs text-amber-600 dark:text-amber-400">
              Could not load catalogs/schemas — check the SQL warehouse is running and permissions are granted.
            </p>
          )}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
            <select value={catalog} onChange={e => { setCatalog(e.target.value); setSchema('') }} className="input-base !text-xs">
              <option value="">Select catalog…</option>
              {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
            <select value={schema} onChange={e => setSchema(e.target.value)} disabled={!catalog} className="input-base !text-xs">
              <option value="">Select schema…</option>
              {schemas.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            <input value={filter} onChange={e => setFilter(e.target.value)} placeholder="Filter tables…" className="input-base !text-xs" aria-label="Filter tables" />
          </div>

          {allTables.length > 0 && (
            <>
              <div className="flex gap-2 text-xs">
                <button type="button" onClick={selectAllInSchema} className="text-blue-600 dark:text-blue-400 hover:underline">Select all ({filteredTables.length})</button>
                <button type="button" onClick={clearSchema} className="text-blue-600 dark:text-blue-400 hover:underline">Clear schema</button>
                <span className="text-slate-400 ml-auto">{tables.length} selected total</span>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-1 max-h-40 overflow-y-auto border dark:border-slate-600 rounded-md p-2">
                {filteredTables.map(t => (
                  <label key={t} className="flex items-center gap-1.5 text-xs cursor-pointer py-0.5 dark:text-slate-200">
                    <input type="checkbox" checked={selectedSet.has(fq(t))} onChange={() => toggle(t)} className="rounded" />
                    <span className="truncate" title={t}>{t}</span>
                  </label>
                ))}
              </div>
            </>
          )}
          {catalog && schema && allTables.length === 0 && allSchemaTableCount > 0 && (
            <p className="text-xs text-amber-600 dark:text-amber-400">
              {allSchemaTableCount} table{allSchemaTableCount !== 1 ? 's' : ''} in {catalog}.{schema}, but none have core metadata yet.
            </p>
          )}

          {tables.length > 0 && (
            <div>
              <div className="flex items-center gap-2 mb-1">
                <span className="text-xs font-medium text-slate-600 dark:text-slate-300">Selected ({tables.length})</span>
                <button type="button" onClick={clearAll} className="text-xs text-red-500 hover:underline">Clear all</button>
              </div>
              <div className="flex flex-wrap gap-1.5">
                {tables.map(t => (
                  <span key={t} className="inline-flex items-center gap-1 px-2 py-0.5 text-xs bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-md text-slate-700 dark:text-slate-300">
                    <span className="truncate max-w-[220px]" title={t}>{t}</span>
                    <button type="button" onClick={() => remove(t)} className="text-slate-400 hover:text-red-500">&times;</button>
                  </span>
                ))}
              </div>
            </div>
          )}
          {tables.length === 0 && (
            <p className="text-xs text-amber-600 dark:text-amber-400">Select at least one table, or switch to “{allLabel}”.</p>
          )}
        </div>
      )}
    </div>
  )
}

/** Derive the job `table_names` param from a scope object.
 *  'all' -> undefined (caller omits it, meaning all-in-scope);
 *  'selected' -> comma-joined FQNs (or undefined if somehow empty). */
export function scopeToTableNames(scope) {
  if (!scope || scope.mode === 'all') return undefined
  const t = (scope.tables || []).filter(Boolean)
  return t.length ? t.join(', ') : undefined
}
