import { useState, useEffect } from 'react'
import { useLanguage } from '../contexts/LanguageContext'

function domainRangeStr(t, domain, range) {
  const d = Array.isArray(domain) ? domain.join(', ') : domain || t('ec.any')
  const r = Array.isArray(range) ? range.join(', ') : range || t('ec.any')
  return `${d} → ${r}`
}

function ValidationBadge({ t, valid, invalid }) {
  const total = valid + invalid
  if (total === 0) return <span className="text-slate-400 text-xs">-</span>
  if (invalid === 0) return <span className="px-2 py-0.5 rounded text-xs font-medium bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300">{t('ec.allValid')}</span>
  if (valid === 0) return <span className="px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300">{t('ec.allInvalid')}</span>
  const pct = (valid / total) * 100
  if (pct >= 80) return <span className="px-2 py-0.5 rounded text-xs font-medium bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300">{t('ec.someInvalid')}</span>
  return <span className="px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300">{t('ec.mostlyInvalid')}</span>
}

export default function EdgeCatalogViewer({ catalog, schema, bundleKey = 'general', onViewInGraph }) {
  const { t } = useLanguage()
  const [edges, setEdges] = useState([])
  const [categoryFilter, setCategoryFilter] = useState('')
  const [loading, setLoading] = useState(true)
  const [fetchError, setFetchError] = useState(null)
  const [selectedEdge, setSelectedEdge] = useState(null)

  useEffect(() => {
    const params = new URLSearchParams()
    if (bundleKey) params.set('bundle', bundleKey)
    if (catalog) params.set('catalog', catalog)
    if (schema) params.set('schema', schema)
    setFetchError(null)
    fetch(`/api/ontology/edge-catalog?${params}`)
      .then(r => { if (!r.ok) throw new Error(t('ec.loadFailed', { status: r.status })); return r.json() })
      .then(data => { setEdges(data.edges || []); setLoading(false) })
      .catch(e => { setFetchError(e.message); setLoading(false) })
  }, [bundleKey, catalog, schema])

  const filtered = categoryFilter ? edges.filter(e => e.category === categoryFilter) : edges
  const categories = [...new Set(edges.map(e => e.category).filter(Boolean))].sort()

  const handleRowClick = (edge) => {
    setSelectedEdge(prev => prev?.name === edge.name ? null : edge)
    if (onViewInGraph) onViewInGraph(edge.name)
  }

  return (
    <div className="p-4">
      <h3 className="text-lg font-semibold text-slate-800 dark:text-slate-200 mb-4">{t('ec.title')}</h3>
      <div className="flex flex-wrap gap-2 mb-4">
        <select
          value={categoryFilter}
          onChange={e => setCategoryFilter(e.target.value)}
          className="select-base text-sm w-36"
          aria-label={t('ec.filterByCategory')}
        >
          <option value="">{t('ec.allCategories')}</option>
          {categories.map(c => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
      </div>
      {fetchError && <p className="text-sm text-red-600 dark:text-red-400 mb-3">{fetchError}</p>}
      {loading ? (
        <p className="text-sm text-slate-500">{t('ec.loading')}</p>
      ) : (
        <>
          <div className="overflow-x-auto border border-slate-200 dark:border-dbx-navy-400/30 rounded-xl">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b-2 border-slate-200 dark:border-dbx-navy-400/30 text-left">
                  <th className="px-3 py-2.5 bg-dbx-oat dark:bg-dbx-navy-500 font-semibold text-slate-600 dark:text-slate-300 text-xs uppercase tracking-wider">{t('ec.colEdgeName')}</th>
                  <th className="px-3 py-2.5 bg-dbx-oat dark:bg-dbx-navy-500 font-semibold text-slate-600 dark:text-slate-300 text-xs uppercase tracking-wider">{t('ec.colInverse')}</th>
                  <th className="px-3 py-2.5 bg-dbx-oat dark:bg-dbx-navy-500 font-semibold text-slate-600 dark:text-slate-300 text-xs uppercase tracking-wider">{t('ec.colDomainRange')}</th>
                  <th className="px-3 py-2.5 bg-dbx-oat dark:bg-dbx-navy-500 font-semibold text-slate-600 dark:text-slate-300 text-xs uppercase tracking-wider">{t('ec.colCategory')}</th>
                  <th className="px-3 py-2.5 bg-dbx-oat dark:bg-dbx-navy-500 font-semibold text-slate-600 dark:text-slate-300 text-xs uppercase tracking-wider">{t('ec.colCount')}</th>
                  <th className="px-3 py-2.5 bg-dbx-oat dark:bg-dbx-navy-500 font-semibold text-slate-600 dark:text-slate-300 text-xs uppercase tracking-wider">{t('ec.colValidation')}</th>
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 && (
                  <tr><td colSpan={6} className="px-3 py-6 text-center text-sm text-slate-400">{t('ec.noEdges')}</td></tr>
                )}
                {filtered.map(e => (
                  <tr
                    key={e.name}
                    onClick={() => handleRowClick(e)}
                    className={`border-b border-slate-100 dark:border-dbx-navy-500/50 cursor-pointer transition-colors ${
                      selectedEdge?.name === e.name ? 'bg-dbx-oat-light dark:bg-dbx-navy-500/60' : 'hover:bg-slate-50 dark:hover:bg-dbx-navy-500/30'
                    }`}
                  >
                    <td className="px-3 py-2 font-medium text-slate-700 dark:text-slate-200">{e.name}</td>
                    <td className="px-3 py-2 text-slate-500 dark:text-slate-400">{e.inverse || '-'}</td>
                    <td className="px-3 py-2 text-slate-600 dark:text-slate-300">{domainRangeStr(t, e.domain, e.range)}</td>
                    <td className="px-3 py-2">
                      <span
                        className="inline-block px-2 py-0.5 rounded text-xs font-medium"
                        style={{
                          background: e.category === 'structural' ? '#e3f2fd' : e.category === 'business' ? '#e8f5e9' : e.category === 'lineage' ? '#fff3e0' : '#f3e5f5',
                          color: e.category === 'structural' ? '#1565c0' : e.category === 'business' ? '#2e7d32' : e.category === 'lineage' ? '#e65100' : '#7b1fa2',
                        }}
                      >
                        {e.category || '-'}
                      </span>
                    </td>
                    <td className="px-3 py-2 tabular-nums">{e.count ?? '-'}</td>
                    <td className="px-3 py-2"><ValidationBadge t={t} valid={e.valid ?? 0} invalid={e.invalid ?? 0} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {selectedEdge && (
            <div className="mt-4 p-4 bg-slate-50 dark:bg-dbx-navy-600 rounded-xl border border-slate-200 dark:border-dbx-navy-400/30">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <h4 className="font-semibold text-slate-800 dark:text-slate-200">{selectedEdge.name}</h4>
                  <p className="text-sm text-slate-600 dark:text-slate-400 mt-1">
                    {selectedEdge.inverse && t('ec.inverseLabel', { name: selectedEdge.inverse })}
                    {selectedEdge.symmetric && t('ec.symmetric')}
                  </p>
                  {(selectedEdge.domain || selectedEdge.range) && (
                    <p className="text-xs text-slate-500 mt-1">
                      {domainRangeStr(t, selectedEdge.domain, selectedEdge.range)}
                    </p>
                  )}
                </div>
                {onViewInGraph && (
                  <button
                    onClick={() => onViewInGraph(selectedEdge.name)}
                    className="px-3 py-1.5 text-sm font-medium bg-dbx-lava text-white rounded-lg hover:bg-red-700 transition-colors whitespace-nowrap"
                  >
                    {t('ec.viewInGraph')}
                  </button>
                )}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}
