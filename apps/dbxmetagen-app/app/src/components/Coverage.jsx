import React, { useState, useEffect } from 'react'
import { ErrorBanner } from '../App'
import { cachedFetch, cachedFetchObj, TTL } from '../apiCache'
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

function CatalogCoverage({ catalog }) {
  const [summary, setSummary] = useState([])
  const [typeBreakdown, setTypeBreakdown] = useState([])
  const [metaSummary, setMetaSummary] = useState(null)
  const [reviewSummary, setReviewSummary] = useState(null)
  const [error, setError] = useState(null)

  useEffect(() => {
    const catParam = catalog ? `?catalog=${encodeURIComponent(catalog)}` : ''
    setReviewSummary(null)
    cachedFetch(`/api/coverage/summary${catParam}`, {}, TTL.DASHBOARD).then(r => {
      setSummary(r.data)
      if (r.error) setError(r.error)
    })
    fetch(`/api/coverage/type-breakdown${catParam}`)
      .then(r => r.ok ? r.json() : []).then(setTypeBreakdown).catch(() => {})
    fetch(`/api/coverage/metadata-summary${catParam}`)
      .then(r => r.ok ? r.json() : null).then(setMetaSummary).catch(() => {})
    fetch(`/api/coverage/review-summary${catParam}`)
      .then(r => r.ok ? r.json() : []).then(setReviewSummary).catch(() => {})
  }, [catalog])

  const totals = summary.reduce((acc, r) => ({
    total: acc.total + (parseInt(r.total_tables) || 0),
    profiled: acc.profiled + (parseInt(r.profiled_tables) || 0),
    unprofiled: acc.unprofiled + (parseInt(r.unprofiled_tables) || 0),
  }), { total: 0, profiled: 0, unprofiled: 0 })

  return (
    <div className="space-y-6">
      <ErrorBanner error={error} />

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <StatCard label="Total Tables" value={totals.total} accentColor="border-l-dbx-sky" />
        <StatCard label="Profiled" value={totals.profiled} accentColor="border-l-dbx-green" sub={totals.total > 0 ? `${Math.round(totals.profiled / totals.total * 100)}%` : undefined} />
        <StatCard label="Unprofiled" value={totals.unprofiled} accentColor="border-l-dbx-amber" sub={totals.total > 0 ? `${Math.round(totals.unprofiled / totals.total * 100)}%` : undefined} />
      </div>

      {reviewSummary && reviewSummary.length > 0 && (() => {
        const counts = { unreviewed: 0, in_review: 0, approved: 0 }
        reviewSummary.forEach(r => { counts[r.status] = (counts[r.status] || 0) + parseInt(r.cnt || 0) })
        const total = Object.values(counts).reduce((a, b) => a + b, 0)
        return (
          <section className="card border-l-4 border-l-indigo-500 p-4">
            <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200 mb-2">Governance Review Status</h3>
            <div className="flex gap-3">
              <span className="inline-flex items-center gap-1.5 text-xs"><span className="w-2 h-2 rounded-full bg-slate-400" />{counts.unreviewed} unreviewed</span>
              <span className="inline-flex items-center gap-1.5 text-xs"><span className="w-2 h-2 rounded-full bg-yellow-500" />{counts.in_review} in review</span>
              <span className="inline-flex items-center gap-1.5 text-xs"><span className="w-2 h-2 rounded-full bg-green-500" />{counts.approved} approved</span>
            </div>
            {total > 0 && (
              <div className="mt-2 flex h-2 rounded-full overflow-hidden bg-slate-200 dark:bg-dbx-navy-500">
                {counts.approved > 0 && <div className="bg-green-500" style={{ width: `${(counts.approved / total * 100).toFixed(1)}%` }} />}
                {counts.in_review > 0 && <div className="bg-yellow-500" style={{ width: `${(counts.in_review / total * 100).toFixed(1)}%` }} />}
                {counts.unreviewed > 0 && <div className="bg-slate-400" style={{ width: `${(counts.unreviewed / total * 100).toFixed(1)}%` }} />}
              </div>
            )}
          </section>
        )
      })()}

      {/* Discovery & Metadata Completeness */}
      {(totals.total > 0 || (metaSummary && metaSummary.total > 0)) && (
        <section className="card p-6">
          <h2 className="text-lg font-semibold text-slate-800 dark:text-slate-100 mb-1">Discovery Coverage</h2>
          <p className="text-xs text-slate-400 dark:text-slate-500 mb-3">Profiled tables out of all tables in the catalog</p>
          <div className="space-y-2.5 mb-5">
            <MetadataBar label="Profiled" value={totals.profiled} total={totals.total} color="bg-dbx-green" />
          </div>
          {metaSummary && metaSummary.total > 0 && (<>
            <h2 className="text-lg font-semibold text-slate-800 dark:text-slate-100 mb-1">Metadata Completeness</h2>
            <p className="text-xs text-slate-400 dark:text-slate-500 mb-3">Enrichment rates among profiled tables ({parseInt(metaSummary.total)} tables in KB)</p>
            <div className="space-y-2.5">
              <MetadataBar label="Comments" value={parseInt(metaSummary.with_comments) || 0} total={parseInt(metaSummary.total)} color="bg-blue-500" />
              <MetadataBar label="PII / PHI" value={parseInt(metaSummary.with_pii) || 0} total={parseInt(metaSummary.total)} color="bg-violet-500" />
              <MetadataBar label="Domain" value={parseInt(metaSummary.with_domain) || 0} total={parseInt(metaSummary.total)} color="bg-amber-500" />
              <MetadataBar label="Ontology" value={parseInt(metaSummary.with_ontology) || 0} total={parseInt(metaSummary.total)} color="bg-orange-500" />
              <MetadataBar label="Tables with FK" value={parseInt(metaSummary.with_fk) || 0} total={parseInt(metaSummary.total)} color="bg-rose-500" />
            </div>
          </>)}
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

    </div>
  )
}

const OVERVIEW_CARDS = [
  { key: 'profiled', label: 'Data Profiles', color: 'border-l-emerald-500', bg: 'from-emerald-50 to-emerald-100/30 dark:from-emerald-900/20 dark:to-emerald-900/10', tab: 'data' },
  { key: 'with_comments', label: 'Descriptions', color: 'border-l-blue-500', bg: 'from-blue-50 to-blue-100/30 dark:from-blue-900/20 dark:to-blue-900/10', tab: 'data' },
  { key: 'with_ontology', label: 'Ontology Coverage', color: 'border-l-orange-500', bg: 'from-orange-50 to-orange-100/30 dark:from-orange-900/20 dark:to-orange-900/10', tab: 'ontology' },
  { key: 'fk_count', label: 'FK Predictions', color: 'border-l-rose-500', bg: 'from-rose-50 to-rose-100/30 dark:from-rose-900/20 dark:to-rose-900/10', tab: 'graph' },
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
  const [catalogs, setCatalogs] = useState([])
  const [selectedCatalog, setSelectedCatalog] = useState('')

  useEffect(() => {
    fetch('/api/catalogs').then(r => r.ok ? r.json() : []).then(setCatalogs).catch(() => {})
    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data: cfg }) => {
      if (cfg?.catalog_name) setSelectedCatalog(cfg.catalog_name)
    })
  }, [])

  useEffect(() => {
    if (!selectedCatalog) return
    const catParam = `?catalog=${encodeURIComponent(selectedCatalog)}`
    fetch(`/api/coverage/holistic${catParam}`).then(r => r.ok ? r.json() : null).then(setHolistic).catch(() => {})
  }, [selectedCatalog])

  const h = holistic || {}
  const total = h.total_tables || 0

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <PageHeader title="Metadata Coverage" subtitle="Holistic view of all metadata types across your catalog" />
        {catalogs.length > 0 && (
          <select value={selectedCatalog} onChange={e => { setSelectedCatalog(e.target.value); setHolistic(null) }}
            className="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-slate-700 dark:text-slate-200">
            {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        )}
      </div>

      {/* Overview Cards */}
      {holistic ? (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          {OVERVIEW_CARDS.map(c => {
            const val = h[c.key] ?? 0
            const showTotal = ['profiled', 'with_comments'].includes(c.key) ? total
              : c.key === 'with_ontology' ? total : null
            const sub = c.key === 'with_ontology'
              ? `${h.entity_type_count || 0} types${h.avg_confidence != null ? `, avg conf: ${(h.avg_confidence * 100).toFixed(0)}%` : ''}`
              : c.key === 'fk_count'
              ? `confidence >= 50%${h.with_fk ? `, ${h.with_fk} tables involved` : ''}`
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

      {/* Detail Tabs — only 'data' shown; ontology/graph hidden but code preserved */}
      {(() => {
        const VISIBLE_TABS = new Set(['data'])
        const visible = DETAIL_TABS.filter(t => VISIBLE_TABS.has(t.key))
        return visible.length > 1 ? (
          <div className="inline-flex bg-dbx-oat/60 dark:bg-dbx-navy-600 rounded-xl p-1 shadow-inner-soft">
            {visible.map(({ key, label }) => (
              <button key={key} onClick={() => setTab(key)}
                className={`px-3.5 py-1.5 text-sm rounded-lg transition-all duration-200 ${tab === key ? 'bg-white dark:bg-dbx-navy-500 shadow-sm font-semibold text-dbx-lava' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}`}>{label}</button>
            ))}
          </div>
        ) : null
      })()}

      {tab === 'data' && <CatalogCoverage catalog={selectedCatalog} />}
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
