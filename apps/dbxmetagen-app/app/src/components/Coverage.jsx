import React, { useState, useEffect } from 'react'
import { safeFetch, ErrorBanner } from '../App'
import { OntologyOverview } from './Ontology'
import { FKMapViz } from './ForeignKeyGeneration'

const TYPE_BADGE = {
  MANAGED: 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300',
  EXTERNAL: 'bg-cyan-100 text-cyan-700 dark:bg-cyan-900/40 dark:text-cyan-300',
  VIEW: 'bg-violet-100 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300',
  MATERIALIZED_VIEW: 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300',
  STREAMING_TABLE: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300',
  FOREIGN: 'bg-gray-100 text-gray-600 dark:bg-gray-800/40 dark:text-gray-300',
}

function MetadataBar({ label, value, total, color }) {
  const pct = total > 0 ? Math.round((value / total) * 100) : 0
  return (
    <div className="flex items-center gap-3">
      <span className="text-xs text-slate-600 dark:text-slate-400 font-medium w-24 text-right">{label}</span>
      <div className="flex-1 bg-dbx-oat dark:bg-dbx-navy-500 rounded-full h-2.5 relative overflow-hidden">
        <div className={`h-2.5 rounded-full ${color} transition-all duration-500`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-slate-500 dark:text-slate-400 w-20 tabular-nums">{value}/{total} ({pct}%)</span>
    </div>
  )
}

function CatalogCoverage() {
  const [summary, setSummary] = useState([])
  const [tables, setTables] = useState([])
  const [typeBreakdown, setTypeBreakdown] = useState([])
  const [metaSummary, setMetaSummary] = useState(null)
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(null)
  const [schemaMeta, setSchemaMeta] = useState(null)
  const [filter, setFilter] = useState('all')
  const [typeFilter, setTypeFilter] = useState('all')

  useEffect(() => {
    safeFetch('/api/coverage/summary').then(r => {
      setSummary(r.data)
      if (r.error) setError(r.error)
    })
    fetch('/api/coverage/type-breakdown')
      .then(r => r.ok ? r.json() : []).then(setTypeBreakdown).catch(() => {})
    fetch('/api/coverage/metadata-summary')
      .then(r => r.ok ? r.json() : null).then(setMetaSummary).catch(() => {})
  }, [])

  const drillDown = async (catalog, schema) => {
    setSelected({ catalog, schema })
    setSchemaMeta(null)
    const { data, error: e } = await safeFetch(`/api/coverage/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`)
    setTables(data)
    fetch(`/api/coverage/metadata-summary?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`)
      .then(r => r.ok ? r.json() : null).then(setSchemaMeta).catch(() => {})
    if (e) setError(e)
  }

  const totals = summary.reduce((acc, r) => ({
    total: acc.total + (parseInt(r.total_tables) || 0),
    profiled: acc.profiled + (parseInt(r.profiled_tables) || 0),
    unprofiled: acc.unprofiled + (parseInt(r.unprofiled_tables) || 0),
  }), { total: 0, profiled: 0, unprofiled: 0 })

  const filteredTables = tables.filter(t => {
    if (filter === 'profiled') return t.is_profiled === 'true' || t.is_profiled === true
    if (filter === 'unprofiled') return t.is_profiled === 'false' || t.is_profiled === false
    return true
  }).filter(t => {
    if (typeFilter === 'all') return true
    return t.table_type === typeFilter
  })

  const typeBadge = (tt) => {
    const cls = TYPE_BADGE[tt] || 'bg-gray-100 text-gray-600'
    return <span className={`px-2 py-0.5 rounded text-xs font-medium ${cls}`}>{tt}</span>
  }

  return (
    <div className="space-y-6">
      <ErrorBanner error={error} />

      {/* Summary cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="card p-5 text-center">
          <p className="section-title">Total Tables</p>
          <p className="text-3xl font-bold text-slate-800 dark:text-white mt-1">{totals.total}</p>
        </div>
        <div className="card border-l-4 border-l-dbx-green p-5 text-center">
          <p className="text-xs text-dbx-green-dark dark:text-dbx-green font-medium uppercase tracking-wider">Profiled</p>
          <p className="text-3xl font-bold text-dbx-green-dark dark:text-dbx-green mt-1">{totals.profiled}</p>
        </div>
        <div className="card border-l-4 border-l-dbx-amber p-5 text-center">
          <p className="text-xs text-dbx-amber-dark dark:text-dbx-amber font-medium uppercase tracking-wider">Unprofiled</p>
          <p className="text-3xl font-bold text-dbx-amber-dark dark:text-dbx-amber mt-1">{totals.unprofiled}</p>
        </div>
      </div>

      {/* Metadata Completeness */}
      {metaSummary && metaSummary.total > 0 && (
        <section className="card p-6">
          <h2 className="text-lg font-semibold text-slate-800 dark:text-slate-100 mb-4">Metadata Completeness</h2>
          <div className="space-y-2.5">
            <MetadataBar label="Profiled" value={totals.profiled} total={totals.total} color="bg-dbx-green" />
            <MetadataBar label="Comments" value={parseInt(metaSummary.with_comments) || 0} total={parseInt(metaSummary.total)} color="bg-blue-500" />
            <MetadataBar label="PII / PHI" value={parseInt(metaSummary.with_pii) || 0} total={parseInt(metaSummary.total)} color="bg-violet-500" />
            <MetadataBar label="Domain" value={parseInt(metaSummary.with_domain) || 0} total={parseInt(metaSummary.total)} color="bg-amber-500" />
            <MetadataBar label="Ontology" value={parseInt(metaSummary.with_ontology) || 0} total={parseInt(metaSummary.total)} color="bg-orange-500" />
            <MetadataBar label="FK Relations" value={parseInt(metaSummary.with_fk) || 0} total={parseInt(metaSummary.total)} color="bg-rose-500" />
          </div>
        </section>
      )}

      {/* Type breakdown */}
      {typeBreakdown.length > 0 && (
        <div className="flex flex-wrap gap-3">
          {typeBreakdown.map(tb => (
            <div key={tb.table_type}
              className="bg-dbx-oat-light dark:bg-dbx-navy-600 rounded-lg border border-slate-200 dark:border-dbx-navy-400/30 px-4 py-2.5 text-center shadow-sm min-w-[100px]">
              <span className={`px-2 py-0.5 rounded text-[10px] font-medium ${TYPE_BADGE[tb.table_type] || 'bg-gray-100 text-gray-600 dark:bg-gray-800/40 dark:text-gray-300'}`}>
                {tb.table_type}
              </span>
              <p className="text-xl font-bold text-slate-700 dark:text-slate-100 mt-1">{tb.count}</p>
            </div>
          ))}
        </div>
      )}

      {/* Schema breakdown */}
      <section className="card p-6">
        <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100 mb-4">Coverage by Schema</h2>
        {summary.length === 0
          ? <p className="text-sm text-slate-400">No table information available. Ensure the catalog is accessible.</p>
          : <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead><tr>
                  {['Catalog', 'Schema', 'Total', 'Profiled', 'Unprofiled', ''].map(h =>
                    <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat dark:bg-dbx-navy-500 font-semibold text-slate-600 dark:text-slate-300 border-b border-slate-200 dark:border-dbx-navy-400/30 text-xs uppercase tracking-wider">{h}</th>)}
                </tr></thead>
                <tbody>
                  {summary.map((r, i) => {
                    const pct = r.total_tables > 0 ? Math.round((r.profiled_tables / r.total_tables) * 100) : 0
                    return (
                      <tr key={i} className="border-b border-slate-100 dark:border-dbx-navy-400/20 hover:bg-orange-50/30 dark:hover:bg-dbx-navy-500/40 transition-colors cursor-pointer"
                          onClick={() => drillDown(r.table_catalog, r.table_schema)}>
                        <td className="px-3 py-2 text-slate-700 dark:text-slate-200 font-medium">{r.table_catalog}</td>
                        <td className="px-3 py-2 text-slate-700 dark:text-slate-300">{r.table_schema}</td>
                        <td className="px-3 py-2 text-slate-600 dark:text-slate-300">{r.total_tables}</td>
                        <td className="px-3 py-2 text-emerald-600 dark:text-emerald-400 font-semibold">{r.profiled_tables}</td>
                        <td className="px-3 py-2 text-amber-600 dark:text-amber-400 font-semibold">{r.unprofiled_tables}</td>
                        <td className="px-3 py-2">
                          <div className="w-24 bg-dbx-oat dark:bg-dbx-navy-500 rounded-full h-2">
                            <div className="bg-emerald-500 h-2 rounded-full" style={{ width: `${pct}%` }} />
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
        }
      </section>

      {/* Drill-down table list */}
      {selected && (
        <section className="card p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100">
              {selected.catalog}.{selected.schema}
              <span className="text-sm font-normal text-slate-400 ml-2">({filteredTables.length} tables)</span>
            </h2>
            <div className="flex gap-3">
              <div className="flex gap-1">
                {[['all','All'],['profiled','Profiled'],['unprofiled','Unprofiled']].map(([k,l]) => (
                  <button key={k} onClick={() => setFilter(k)}
                    className={`px-3 py-1.5 text-xs rounded-lg font-medium transition-all ${
                      filter === k ? 'bg-dbx-lava text-white' : 'bg-dbx-oat dark:bg-dbx-navy-500 text-slate-600 dark:text-slate-300 hover:bg-dbx-oat-dark dark:hover:bg-dbx-navy-400'}`}>{l}</button>
                ))}
              </div>
              <div className="flex gap-1">
                {[['all','All Types'], ...Object.keys(TYPE_BADGE).map(k => [k, k])].map(([k,l]) => {
                  const hasType = k === 'all' || tables.some(t => t.table_type === k)
                  if (!hasType) return null
                  return (
                    <button key={k} onClick={() => setTypeFilter(k)}
                      className={`px-2 py-1.5 text-[10px] rounded-lg font-medium transition-all ${
                        typeFilter === k ? 'bg-slate-700 dark:bg-dbx-teal text-white' : 'bg-dbx-oat dark:bg-dbx-navy-500 text-slate-600 dark:text-slate-300 hover:bg-dbx-oat-dark dark:hover:bg-dbx-navy-400'}`}>{l}</button>
                  )
                })}
              </div>
            </div>
          </div>
          {schemaMeta && schemaMeta.total > 0 && (
            <div className="mb-4 p-4 bg-white dark:bg-dbx-navy-500/50 rounded-lg border border-slate-100 dark:border-dbx-navy-400/30 space-y-2">
              <h3 className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider mb-2">Schema Metadata Completeness</h3>
              <MetadataBar label="Profiled" value={filteredTables.filter(t => t.is_profiled === 'true' || t.is_profiled === true).length} total={filteredTables.length} color="bg-emerald-500" />
              <MetadataBar label="Comments" value={parseInt(schemaMeta.with_comments) || 0} total={parseInt(schemaMeta.total)} color="bg-blue-500" />
              <MetadataBar label="PII / PHI" value={parseInt(schemaMeta.with_pii) || 0} total={parseInt(schemaMeta.total)} color="bg-violet-500" />
              <MetadataBar label="Domain" value={parseInt(schemaMeta.with_domain) || 0} total={parseInt(schemaMeta.total)} color="bg-amber-500" />
              <MetadataBar label="Ontology" value={parseInt(schemaMeta.with_ontology) || 0} total={parseInt(schemaMeta.total)} color="bg-orange-500" />
              <MetadataBar label="FK Relations" value={parseInt(schemaMeta.with_fk) || 0} total={parseInt(schemaMeta.total)} color="bg-rose-500" />
            </div>
          )}
          <div className="overflow-x-auto max-h-96">
            <table className="min-w-full text-sm">
              <thead><tr>
                {['Table', 'Type', 'Status'].map(h =>
                  <th key={h} className="text-left px-3 py-2.5 bg-dbx-oat dark:bg-dbx-navy-500 font-semibold text-slate-600 dark:text-slate-300 border-b border-slate-200 dark:border-dbx-navy-400/30 text-xs uppercase tracking-wider">{h}</th>)}
              </tr></thead>
              <tbody>
                {filteredTables.map((t, i) => (
                  <tr key={i} className="border-b border-slate-100 dark:border-dbx-navy-400/20 hover:bg-orange-50/30 dark:hover:bg-dbx-navy-500/40 transition-colors">
                    <td className="px-3 py-2 text-slate-700 dark:text-slate-200 font-mono text-xs">{t.table_name}</td>
                    <td className="px-3 py-2">{typeBadge(t.table_type)}</td>
                    <td className="px-3 py-2">
                      {(t.is_profiled === 'true' || t.is_profiled === true)
                        ? <span className="px-2 py-0.5 bg-emerald-50 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300 rounded text-xs font-medium">Profiled</span>
                        : <span className="px-2 py-0.5 bg-amber-50 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300 rounded text-xs font-medium">Unprofiled</span>
                      }
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  )
}

const COVERAGE_TABS = [
  { key: 'catalog', label: 'Catalog Coverage' },
  { key: 'ontology', label: 'Ontology Health' },
  { key: 'fk_map', label: 'FK Map' },
]

export default function Coverage() {
  const [tab, setTab] = useState('catalog')
  return (
    <div className="space-y-4">
      <div className="inline-flex bg-dbx-oat/60 dark:bg-dbx-navy-600 rounded-xl p-1 shadow-inner-soft">
        {COVERAGE_TABS.map(({ key, label }) => (
          <button key={key} onClick={() => setTab(key)}
            className={`px-3.5 py-1.5 text-sm rounded-lg transition-all duration-200 ${tab === key ? 'bg-white dark:bg-dbx-navy-500 shadow-sm font-semibold text-dbx-lava' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`}>{label}</button>
        ))}
      </div>
      {tab === 'catalog' && <CatalogCoverage />}
      {tab === 'ontology' && <OntologyOverview />}
      {tab === 'fk_map' && (
        <div className="space-y-4">
          <section className="card p-6">
            <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100 mb-4">Foreign Key Map</h2>
            <p className="text-xs text-slate-500 mb-3">Tables as nodes, FK predictions as edges. Clustered by domain similarity.</p>
            <FKMapViz />
          </section>
        </div>
      )}
    </div>
  )
}
