import { useState, useEffect, useMemo } from 'react'
import { useCatalogSchemaTables } from '../hooks/useCatalogSchemaTables'
import { cachedFetchObj, TTL } from '../apiCache'
import { PageHeader, StatCard } from './ui'

function RoleChips({ roles }) {
  const entries = Object.entries(roles || {}).filter(([, v]) => v > 0)
  if (entries.length === 0) return null
  return (
    <div className="flex flex-wrap gap-1.5">
      {entries.map(([role, cnt]) => (
        <span key={role} className="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-dbx-navy-500 text-[10px] text-slate-600 dark:text-slate-400">
          {cnt} {role}
        </span>
      ))}
    </div>
  )
}

function IssueChips({ entity }) {
  const chips = []
  const roles = entity.roles || {}
  if (!roles.identifier && !roles.primary_key) {
    chips.push({ label: 'No identifier', color: 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300' })
  }
  if ((entity.avg_confidence ?? 1) < 0.5) {
    chips.push({ label: 'Low confidence', color: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300' })
  }
  if ((entity.relationship_count ?? 0) === 0) {
    chips.push({ label: 'Unlinked', color: 'bg-slate-100 dark:bg-slate-700/40 text-slate-500 dark:text-slate-400' })
  }
  if (chips.length === 0) return null
  return (
    <div className="flex flex-wrap gap-1 mt-1.5">
      {chips.map(c => (
        <span key={c.label} className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${c.color}`}>
          {c.label}
        </span>
      ))}
    </div>
  )
}

function EntityCard({ entity, expanded, onToggle, onNavigate }) {
  const avg = entity.avg_confidence ?? 0
  const confColor = avg > 0.8 ? 'text-green-600 dark:text-green-400' : avg > 0.6 ? 'text-amber-600 dark:text-amber-400' : 'text-red-600 dark:text-red-400'
  const total = (entity.bundle_matches ?? 0) + (entity.heuristic_matches ?? 0)
  const bundlePct = total > 0 ? Math.round((entity.bundle_matches / total) * 100) : 0
  const borderColor = avg >= 0.8 ? 'border-l-emerald-400' : avg >= 0.6 ? 'border-l-amber-400' : 'border-l-red-400'

  return (
    <div
      className={`card border border-slate-200 dark:border-dbx-navy-400/30 border-l-4 ${borderColor} hover:border-dbx-lava/40 cursor-pointer transition-all`}
      onClick={onToggle}
    >
      <div className="p-4">
        <div className="flex items-center justify-between">
          <h3 className="font-semibold text-slate-800 dark:text-slate-200">{entity.entity_type}</h3>
          <span className={`text-sm font-medium tabular-nums ${confColor}`}>
            {(avg * 100).toFixed(0)}% avg
          </span>
        </div>
        <IssueChips entity={entity} />
        <div className="mt-2 flex items-center gap-4 text-xs text-slate-500 dark:text-slate-400">
          <span>{entity.table_count ?? 0} tables</span>
          <span>{entity.column_count ?? 0} columns</span>
          {(entity.relationship_count ?? 0) > 0 && <span>{entity.relationship_count} relationships</span>}
          <span className="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-dbx-navy-500 text-slate-600 dark:text-slate-300" title="Bundle match vs heuristic">
            {bundlePct}% bundle
          </span>
          {entity.source_ontology && (
            <span className="px-1.5 py-0.5 rounded bg-indigo-100 dark:bg-indigo-900/40 text-indigo-700 dark:text-indigo-300 font-medium" title="Source ontology standard">
              {entity.source_ontology}
            </span>
          )}
        </div>
        <div className="mt-2">
          <RoleChips roles={entity.roles} />
        </div>
      </div>
      {expanded && (
        <EntityExpanded entity={entity} onNavigate={onNavigate} />
      )}
    </div>
  )
}

function EntityExpanded({ entity, onNavigate }) {
  const [detail, setDetail] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    cachedFetchObj(`/api/ontology/entity-detail?entity_type=${encodeURIComponent(entity.entity_type)}`, {}, TTL.DASHBOARD)
      .then(({ data }) => { if (!cancelled) setDetail(data) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [entity.entity_type])

  const tables = entity.tables?.length ? entity.tables : detail?.tables || []
  const properties = detail?.properties || []
  const byTable = {}
  properties.forEach(p => {
    const t = p.table_name || 'unknown'
    if (!byTable[t]) byTable[t] = []
    byTable[t].push(p)
  })
  const hasLinked = properties.some(p => p.linked_entity_type)
  const hasDiscovery = properties.some(p => p.discovery_method)

  return (
    <div className="border-t border-slate-200 dark:border-dbx-navy-400/30 bg-dbx-oat/50 dark:bg-dbx-navy-600/50 p-4" onClick={e => e.stopPropagation()}>
      {loading ? (
        <p className="text-sm text-slate-500">Loading details...</p>
      ) : (
        <>
          {detail?.description && (
            <p className="text-sm text-slate-600 dark:text-slate-400 mb-3">{detail.description}</p>
          )}
          <div className="flex items-center gap-3 mb-3 text-xs">
            {detail?.source_ontology && (
              <span className="px-2 py-0.5 rounded-full bg-indigo-100 dark:bg-indigo-900/40 text-indigo-700 dark:text-indigo-300 font-medium">
                {detail.source_ontology}
              </span>
            )}
            {detail?.entity_uri && (
              <a href={detail.entity_uri} target="_blank" rel="noopener noreferrer"
                 className="text-dbx-sky hover:underline flex items-center gap-1" title={detail.entity_uri}>
                Ontology URI &#8599;
              </a>
            )}
            {onNavigate && (
              <button
                onClick={() => onNavigate('agent')}
                className="text-dbx-sky hover:underline flex items-center gap-1"
                title="View this entity in the knowledge graph"
              >
                View in Graph &#8599;
              </button>
            )}
          </div>
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
                      {hasLinked && <th className="text-left px-3 py-1.5 font-medium text-slate-600 dark:text-slate-400">Links to</th>}
                      {hasDiscovery && <th className="text-left px-3 py-1.5 font-medium text-slate-600 dark:text-slate-400">Source</th>}
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
                        {hasLinked && (
                          <td className="px-3 py-1.5 text-slate-600 dark:text-slate-400">
                            {c.linked_entity_type || '-'}
                          </td>
                        )}
                        {hasDiscovery && (
                          <td className="px-3 py-1.5">
                            {c.discovery_method ? (
                              <span className={`px-1.5 py-0.5 rounded text-[10px] ${
                                c.discovery_method.includes('bundle') ? 'bg-indigo-100 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-300' : 'bg-slate-100 dark:bg-dbx-navy-500 text-slate-500 dark:text-slate-400'
                              }`}>{c.discovery_method.includes('bundle') ? 'bundle' : 'heuristic'}</span>
                            ) : '-'}
                          </td>
                        )}
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

export default function EntityBrowser({ onNavigate }) {
  const [entities, setEntities] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [expanded, setExpanded] = useState(null)
  const [filter, setFilter] = useState('')
  const [sortBy, setSortBy] = useState('confidence')
  const cst = useCatalogSchemaTables()

  useEffect(() => {
    setLoading(true)
    setError(null)
    const params = new URLSearchParams()
    if (cst.catalog) params.set('catalog', cst.catalog)
    if (cst.schema) params.set('schema', cst.schema)
    const q = params.toString() ? `?${params}` : ''
    cachedFetchObj(`/api/ontology/entities-summary${q}`, {}, TTL.DASHBOARD)
      .then(({ data, error: err }) => {
        if (err) { setError(err); setEntities([]) }
        else setEntities(data?.entities || [])
      })
      .finally(() => setLoading(false))
  }, [cst.catalog, cst.schema])

  const filtered = useMemo(() => {
    const base = filter
      ? entities.filter(e => e.entity_type?.toLowerCase().includes(filter.toLowerCase()))
      : [...entities]
    return base.sort((a, b) => sortBy === 'confidence'
      ? (b.avg_confidence ?? 0) - (a.avg_confidence ?? 0)
      : (a.entity_type || '').localeCompare(b.entity_type || '')
    )
  }, [entities, filter, sortBy])

  const stats = useMemo(() => {
    if (!entities.length) return null
    const totalTables = entities.reduce((s, e) => s + (e.table_count ?? 0), 0)
    const totalCols = entities.reduce((s, e) => s + (e.column_count ?? 0), 0)
    const avgConf = totalCols > 0
      ? entities.reduce((s, e) => s + (e.avg_confidence ?? 0) * (e.column_count ?? 0), 0) / totalCols
      : 0
    const totalBundle = entities.reduce((s, e) => s + (e.bundle_matches ?? 0), 0)
    const totalAll = totalBundle + entities.reduce((s, e) => s + (e.heuristic_matches ?? 0), 0)
    const bundlePct = totalAll > 0 ? Math.round((totalBundle / totalAll) * 100) : 0
    const classifiedTables = new Set(entities.flatMap(e => e.tables || []))
    const allTables = cst.tables?.length ?? 0
    return { totalTables, avgConf, bundlePct, classifiedTables: classifiedTables.size, allTables }
  }, [entities, cst.tables])

  return (
    <div className="space-y-4">
      <PageHeader
        title="Entity Browser"
        subtitle="Ontology entities discovered across your catalog"
        actions={
          <div className="flex items-center gap-2">
            {(cst.catalogs?.length > 0 || cst.schemas?.length > 0) && (
              <>
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
              </>
            )}
            <input
              type="text"
              placeholder="Filter by entity type..."
              value={filter}
              onChange={e => setFilter(e.target.value)}
              className="text-sm rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 px-3 py-1.5 w-48"
            />
            <button
              onClick={() => setSortBy(s => s === 'confidence' ? 'alpha' : 'confidence')}
              className="text-xs px-2.5 py-1.5 rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-dbx-navy-500 transition-colors"
            >
              Sort: {sortBy === 'confidence' ? 'Confidence' : 'A-Z'}
            </button>
          </div>
        }
      />

      {!loading && !error && stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard label="Entity Types" value={entities.length} accentColor="border-l-dbx-teal" />
          <StatCard
            label="Tables Classified"
            value={stats.allTables > 0 ? `${stats.classifiedTables} / ${stats.allTables}` : stats.totalTables}
            sub={stats.allTables > 0 && stats.classifiedTables < stats.allTables
              ? `${stats.allTables - stats.classifiedTables} unclassified`
              : undefined}
            accentColor="border-l-dbx-sky"
            warn={stats.allTables > 0 && stats.classifiedTables < stats.allTables * 0.5}
          />
          <StatCard label="Avg Confidence" value={`${(stats.avgConf * 100).toFixed(0)}%`} accentColor="border-l-emerald-400" warn={stats.avgConf < 0.6} />
          <StatCard label="Bundle Coverage" value={`${stats.bundlePct}%`} sub="Bundle-matched vs heuristic" accentColor="border-l-indigo-400" />
        </div>
      )}

      {cst.error && (
        <div className="rounded-lg border border-red-200 dark:border-red-800/40 bg-red-50 dark:bg-red-900/20 px-4 py-3 text-sm text-red-700 dark:text-red-300">
          Could not load catalogs/schemas. Check that the SQL warehouse is running and the app service principal has USE permissions. <span className="font-mono text-red-500 dark:text-red-400">{cst.error}</span>
        </div>
      )}
      {error && <p className="text-sm text-red-600 dark:text-red-400">Could not load entities. Check permissions or try refreshing the page.</p>}
      {loading && <p className="text-sm text-slate-500">Loading entities...</p>}
      {!loading && !error && filtered.length === 0 && (
        <p className="text-sm text-slate-500">{filter ? 'No entities match the current filter.' : 'No entities found. Run the ontology pipeline first (Generate Metadata with an ontology bundle).'}</p>
      )}
      {!loading && !error && filtered.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {filtered.map(e => (
            <EntityCard
              key={e.entity_type}
              entity={e}
              expanded={expanded === e.entity_type}
              onToggle={() => setExpanded(prev => prev === e.entity_type ? null : e.entity_type)}
              onNavigate={onNavigate}
            />
          ))}
        </div>
      )}
    </div>
  )
}
