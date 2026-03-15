import React, { useState, useEffect } from 'react'
import { safeFetch, ErrorBanner } from '../App'
import { PageHeader, StatCard, SkeletonCards, EmptyState } from './ui'
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

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <StatCard label="Total Tables" value={totals.total} accentColor="border-l-dbx-sky" />
        <StatCard label="Profiled" value={totals.profiled} accentColor="border-l-dbx-green" sub={totals.total > 0 ? `${Math.round(totals.profiled / totals.total * 100)}%` : undefined} />
        <StatCard label="Unprofiled" value={totals.unprofiled} accentColor="border-l-dbx-amber" sub={totals.total > 0 ? `${Math.round(totals.unprofiled / totals.total * 100)}%` : undefined} />
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
              className="bg-dbx-oat-light dark:bg-dbx-navy-650 rounded-lg border border-slate-200 dark:border-dbx-navy-400/25 px-4 py-2.5 text-center shadow-sm min-w-[100px]">
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
          ? <EmptyState title="No table information available" description="Ensure the catalog is accessible and metadata generation has been run" />
          : <div className="overflow-x-auto surface-nested">
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

const OVERVIEW_CARDS = [
  { key: 'profiled', label: 'Data Profiles', color: 'border-l-emerald-500', bg: 'from-emerald-50 to-emerald-100/30 dark:from-emerald-900/20 dark:to-emerald-900/10', tab: 'data' },
  { key: 'with_comments', label: 'Descriptions', color: 'border-l-blue-500', bg: 'from-blue-50 to-blue-100/30 dark:from-blue-900/20 dark:to-blue-900/10', tab: 'data' },
  { key: 'with_ontology', label: 'Ontology Entities', color: 'border-l-orange-500', bg: 'from-orange-50 to-orange-100/30 dark:from-orange-900/20 dark:to-orange-900/10', tab: 'ontology' },
  { key: 'fk_count', label: 'FK Relationships', color: 'border-l-rose-500', bg: 'from-rose-50 to-rose-100/30 dark:from-rose-900/20 dark:to-rose-900/10', tab: 'graph' },
  { key: 'metric_views', label: 'Metric Views', color: 'border-l-amber-500', bg: 'from-amber-50 to-amber-100/30 dark:from-amber-900/20 dark:to-amber-900/10', tab: 'data' },
  { key: 'vs_documents', label: 'VS Documents', color: 'border-l-violet-500', bg: 'from-violet-50 to-violet-100/30 dark:from-violet-900/20 dark:to-violet-900/10', tab: 'data' },
]

const DETAIL_TABS = [
  { key: 'data', label: 'Data & Descriptions' },
  { key: 'ontology', label: 'Ontology & Entities' },
  { key: 'graph', label: 'Relationships & Graph' },
]

function CoverageCard({ label, value, total, color, bg, onClick, sub }) {
  const pct = total > 0 ? Math.round((value / total) * 100) : null
  return (
    <button onClick={onClick}
      className={`text-left rounded-xl p-4 border-l-4 ${color} border border-slate-200/60 dark:border-dbx-navy-400/25 bg-gradient-to-br ${bg} shadow-card hover:shadow-card-hover transition-all duration-200 hover:scale-[1.02]`}>
      <p className="text-xs font-medium uppercase tracking-wider text-slate-500 dark:text-slate-400 mb-1">{label}</p>
      <div className="flex items-baseline gap-2">
        <span className="text-2xl font-bold text-dbx-navy dark:text-white">{value ?? '--'}</span>
        {total != null && total > 0 && <span className="text-xs text-slate-400 dark:text-slate-500">/ {total}</span>}
      </div>
      {pct != null && (
        <div className="mt-2 h-1.5 bg-slate-200/60 dark:bg-dbx-navy-500 rounded-full overflow-hidden">
          <div className={`h-1.5 rounded-full ${color.replace('border-l-', 'bg-')} transition-all duration-500`} style={{ width: `${pct}%` }} />
        </div>
      )}
      {sub && <p className="text-[10px] text-slate-400 dark:text-slate-500 mt-1.5">{sub}</p>}
    </button>
  )
}

export default function Coverage() {
  const [holistic, setHolistic] = useState(null)
  const [tab, setTab] = useState('data')

  useEffect(() => {
    fetch('/api/coverage/holistic').then(r => r.ok ? r.json() : null).then(setHolistic).catch(() => {})
  }, [])

  const h = holistic || {}
  const total = h.total_tables || 0

  return (
    <div className="space-y-5">
      <PageHeader title="Metadata Coverage" subtitle="Holistic view of all metadata types across your catalog" />

      {/* Overview Cards */}
      {holistic ? (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          {OVERVIEW_CARDS.map(c => {
            const val = h[c.key] ?? 0
            const showTotal = ['profiled', 'with_comments', 'with_ontology'].includes(c.key) ? total : null
            const sub = c.key === 'with_ontology' && h.avg_confidence != null
              ? `avg confidence: ${(h.avg_confidence * 100).toFixed(0)}%`
              : c.key === 'vs_documents' && h.vs_by_type && Object.keys(h.vs_by_type).length > 0
              ? Object.entries(h.vs_by_type).map(([k, v]) => `${k}: ${v}`).join(', ')
              : c.key === 'metric_views' && h.metric_view_statuses && Object.keys(h.metric_view_statuses).length > 0
              ? Object.entries(h.metric_view_statuses).map(([k, v]) => `${k}: ${v}`).join(', ')
              : undefined
            return (
              <CoverageCard key={c.key} label={c.label} value={val} total={showTotal}
                color={c.color} bg={c.bg} sub={sub}
                onClick={() => setTab(c.tab)} />
            )
          })}
        </div>
      ) : (
        <SkeletonCards count={6} />
      )}

      {/* Detail Tabs */}
      <div className="inline-flex bg-dbx-oat/60 dark:bg-dbx-navy-600 rounded-xl p-1 shadow-inner-soft">
        {DETAIL_TABS.map(({ key, label }) => (
          <button key={key} onClick={() => setTab(key)}
            className={`px-3.5 py-1.5 text-sm rounded-lg transition-all duration-200 ${tab === key ? 'bg-white dark:bg-dbx-navy-500 shadow-sm font-semibold text-dbx-lava' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`}>{label}</button>
        ))}
      </div>

      {tab === 'data' && <CatalogCoverage />}
      {tab === 'ontology' && <OntologyOverview />}
      {tab === 'graph' && (
        <div className="space-y-4">
          <section className="card p-6">
            <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100 mb-2">Relationships & Graph</h2>
            <p className="text-xs text-slate-500 dark:text-slate-400 mb-4">Tables as nodes, FK predictions as edges. Clustered by domain similarity.</p>
            <FKMapViz />
          </section>
        </div>
      )}
    </div>
  )
}
