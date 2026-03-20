import { useState, useEffect } from 'react'
import { useCatalogSchemaTables } from '../hooks/useCatalogSchemaTables'

function RoleBar({ roles }) {
  const entries = Object.entries(roles || {}).filter(([, v]) => v > 0)
  if (entries.length === 0) return null
  const total = entries.reduce((s, [, v]) => s + v, 0)
  const colors = ['bg-dbx-lava', 'bg-dbx-sky', 'bg-dbx-teal', 'bg-amber-500', 'bg-violet-500', 'bg-slate-400']
  return (
    <div className="flex h-1.5 rounded-full overflow-hidden bg-slate-200 dark:bg-dbx-navy-500" title={entries.map(([r, v]) => `${r}: ${v}`).join(', ')}>
      {entries.slice(0, 6).map(([role, cnt], i) => (
        <div key={role} className={colors[i % colors.length]} style={{ width: `${(cnt / total) * 100}%` }} />
      ))}
    </div>
  )
}

function EntityCard({ entity, expanded, onToggle }) {
  const avg = entity.avg_confidence ?? 0
  const confColor = avg > 0.8 ? 'text-green-600 dark:text-green-400' : avg > 0.6 ? 'text-amber-600 dark:text-amber-400' : 'text-red-600 dark:text-red-400'
  const total = (entity.bundle_matches ?? 0) + (entity.heuristic_matches ?? 0)
  const bundlePct = total > 0 ? Math.round((entity.bundle_matches / total) * 100) : 0

  return (
    <div
      className="card border border-slate-200 dark:border-dbx-navy-400/30 hover:border-dbx-lava/40 cursor-pointer transition-all"
      onClick={onToggle}
    >
      <div className="p-4">
        <div className="flex items-center justify-between">
          <h3 className="font-semibold text-slate-800 dark:text-slate-200">{entity.entity_type}</h3>
          <span className={`text-sm font-medium tabular-nums ${confColor}`}>
            {(avg * 100).toFixed(0)}% avg
          </span>
        </div>
        <div className="mt-2 flex items-center gap-4 text-xs text-slate-500 dark:text-slate-400">
          <span>{entity.table_count ?? 0} tables</span>
          <span>{entity.column_count ?? 0} columns</span>
          <span className="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-dbx-navy-500 text-slate-600 dark:text-slate-300" title="Bundle match vs heuristic">
            {bundlePct}% bundle
          </span>
        </div>
        <div className="mt-2">
          <RoleBar roles={entity.roles} />
        </div>
      </div>
      {expanded && (
        <EntityExpanded entity={entity} />
      )}
    </div>
  )
}

function EntityExpanded({ entity }) {
  const [detail, setDetail] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    fetch(`/api/ontology/entity-detail?entity_type=${encodeURIComponent(entity.entity_type)}`)
      .then(r => r.json())
      .then(d => setDetail(d))
      .catch(() => setDetail(null))
      .finally(() => setLoading(false))
  }, [entity.entity_type])

  const tables = entity.tables?.length ? entity.tables : detail?.tables || []
  const properties = detail?.properties || []
  const byTable = {}
  properties.forEach(p => {
    const t = p.table_name || 'unknown'
    if (!byTable[t]) byTable[t] = []
    byTable[t].push(p)
  })

  return (
    <div className="border-t border-slate-200 dark:border-dbx-navy-400/30 bg-dbx-oat/50 dark:bg-dbx-navy-600/50 p-4">
      {loading ? (
        <p className="text-sm text-slate-500">Loading details...</p>
      ) : (
        <>
          <h4 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-2">Source tables</h4>
          <div className="flex flex-wrap gap-2 mb-4">
            {tables.map(t => (
              <span key={t} className="px-2 py-1 rounded bg-slate-100 dark:bg-dbx-navy-500 font-mono text-xs text-slate-700 dark:text-slate-300">
                {t}
              </span>
            ))}
          </div>
          <h4 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-2">Columns by table</h4>
          <div className="space-y-3">
            {Object.entries(byTable).map(([table, cols]) => (
              <div key={table} className="rounded-lg border border-slate-200 dark:border-dbx-navy-400/20 overflow-hidden">
                <div className="px-3 py-1.5 bg-slate-100 dark:bg-dbx-navy-500 font-mono text-xs font-medium text-slate-700 dark:text-slate-300">
                  {table}
                </div>
                <table className="min-w-full text-xs">
                  <thead>
                    <tr className="border-b border-slate-200 dark:border-dbx-navy-400/20">
                      <th className="text-left px-3 py-1.5 font-medium text-slate-600 dark:text-slate-400">Column</th>
                      <th className="text-left px-3 py-1.5 font-medium text-slate-600 dark:text-slate-400">Role</th>
                      <th className="text-right px-3 py-1.5 font-medium text-slate-600 dark:text-slate-400">Confidence</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cols.map((c, i) => (
                      <tr key={i} className="border-b border-slate-100 dark:border-dbx-navy-500/30 last:border-0">
                        <td className="px-3 py-1.5 font-mono text-slate-700 dark:text-slate-300">{c.column_name}</td>
                        <td className="px-3 py-1.5">
                          <span className="px-1.5 py-0.5 rounded bg-slate-200 dark:bg-dbx-navy-500 text-slate-600 dark:text-slate-400">
                            {c.property_role || '-'}
                          </span>
                        </td>
                        <td className="px-3 py-1.5 text-right tabular-nums">{c.confidence != null ? `${(c.confidence * 100).toFixed(0)}%` : '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

export default function EntityBrowser() {
  const [entities, setEntities] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [expanded, setExpanded] = useState(null)
  const [filter, setFilter] = useState('')
  const cst = useCatalogSchemaTables()

  useEffect(() => {
    setLoading(true)
    setError(null)
    const params = new URLSearchParams()
    if (cst.catalog) params.set('catalog', cst.catalog)
    if (cst.schema) params.set('schema', cst.schema)
    const q = params.toString() ? `?${params}` : ''
    fetch(`/api/ontology/entities-summary${q}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(r.status)))
      .then(d => { setEntities(d.entities || []); setLoading(false) })
      .catch(e => { setError(e.message); setEntities([]); setLoading(false) })
  }, [cst.catalog, cst.schema])

  const filtered = filter
    ? entities.filter(e => e.entity_type?.toLowerCase().includes(filter.toLowerCase()))
    : entities

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-4">
        <h2 className="text-lg font-semibold text-slate-800 dark:text-slate-200">Entity Browser</h2>
        {(cst.catalogs?.length > 0 || cst.schemas?.length > 0) && (
          <div className="flex items-center gap-2">
            <select
              value={cst.catalog}
              onChange={e => cst.setCatalog(e.target.value)}
              className="text-sm rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 px-2 py-1"
            >
              <option value="">All catalogs</option>
              {cst.catalogs?.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
            <select
              value={cst.schema}
              onChange={e => cst.setSchema(e.target.value)}
              className="text-sm rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 px-2 py-1"
            >
              <option value="">All schemas</option>
              {cst.schemas?.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
        )}
        <input
          type="text"
          placeholder="Filter by entity type..."
          value={filter}
          onChange={e => setFilter(e.target.value)}
          className="text-sm rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 px-3 py-1.5 w-48"
        />
      </div>

      {error && <p className="text-sm text-red-600 dark:text-red-400">{error}</p>}
      {loading && <p className="text-sm text-slate-500">Loading entities...</p>}
      {!loading && !error && filtered.length === 0 && (
        <p className="text-sm text-slate-500">No entities found.</p>
      )}
      {!loading && !error && filtered.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {filtered.map(e => (
            <EntityCard
              key={e.entity_type}
              entity={e}
              expanded={expanded === e.entity_type}
              onToggle={() => setExpanded(prev => prev === e.entity_type ? null : e.entity_type)}
            />
          ))}
        </div>
      )}
    </div>
  )
}
