import React, { useState, useEffect } from 'react'
import { cachedFetchObj, TTL } from '../apiCache'
import { PageHeader, SkeletonCards } from './ui'

function CompletionBar({ label, value, total, color, sub }) {
  const pct = total > 0 ? Math.round((value / total) * 100) : 0
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-slate-700 dark:text-slate-200">{label}</span>
        <span className="text-sm tabular-nums text-slate-500 dark:text-slate-400">{value} / {total} <span className="text-xs">({pct}%)</span></span>
      </div>
      <div className="h-2.5 bg-slate-200/60 dark:bg-dbx-navy-500 rounded-full overflow-hidden">
        <div className={`h-2.5 rounded-full ${color} transition-all duration-500`} style={{ width: `${pct}%` }} />
      </div>
      {sub && <p className="text-[11px] text-slate-400 dark:text-slate-500">{sub}</p>}
    </div>
  )
}

/**
 * Self-contained catalog-coverage summary. Fetches its own data.
 * Embeddable anywhere (e.g. as a header on Review & Apply). If no `catalog`
 * prop is supplied it resolves the default catalog from /api/config.
 */
export function CoverageSummary({ catalog }) {
  const [holistic, setHolistic] = useState(null)
  const [reviewSummary, setReviewSummary] = useState(null)
  const [resolvedCatalog, setResolvedCatalog] = useState(catalog || '')

  useEffect(() => {
    if (catalog) { setResolvedCatalog(catalog); return }
    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data: cfg }) => {
      if (cfg?.catalog_name) setResolvedCatalog(cfg.catalog_name)
    })
  }, [catalog])

  useEffect(() => {
    if (!resolvedCatalog) return
    const catParam = `?catalog=${encodeURIComponent(resolvedCatalog)}`
    setHolistic(null)
    setReviewSummary(null)
    fetch(`/api/coverage/holistic${catParam}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`${r.status}`)))
      .then(setHolistic)
      .catch(() => setHolistic({ _error: true }))
    fetch(`/api/coverage/review-summary${catParam}`)
      .then(r => r.ok ? r.json() : []).then(setReviewSummary).catch(() => {})
  }, [resolvedCatalog])

  const h = holistic || {}
  const total = h.total_tables || 0
  const profiled = h.profiled || 0
  const profiledPct = total > 0 ? Math.round((profiled / total) * 100) : 0

  const reviewCounts = { unreviewed: 0, in_review: 0, approved: 0 }
  if (reviewSummary) reviewSummary.forEach(r => { reviewCounts[r.status] = (reviewCounts[r.status] || 0) + parseInt(r.cnt || 0) })
  const reviewTotal = Object.values(reviewCounts).reduce((a, b) => a + b, 0)

  const advancedStats = [
    h.entity_type_count > 0 && `${h.entity_type_count} ontology entity types`,
    h.fk_count > 0 && `${h.fk_count} FK predictions`,
    h.metric_views > 0 && `${h.metric_views} metric views`,
    h.vs_documents > 0 && `${h.vs_documents} search index documents`,
  ].filter(Boolean)

  if (holistic?._error || (holistic && total === 0)) return null
  if (!holistic) return <SkeletonCards count={1} />

  return (
    <div className="space-y-4">
      {/* Headline stat */}
      <div className="flex items-baseline gap-3">
        <span className="text-2xl font-bold text-dbx-navy dark:text-white tabular-nums">{profiled}</span>
        <span className="text-sm text-slate-400 dark:text-slate-500">of {total} tables profiled</span>
        <span className="ml-auto text-lg font-semibold tabular-nums text-slate-500 dark:text-slate-400">{profiledPct}%</span>
      </div>
      <div className="h-2.5 bg-slate-200/60 dark:bg-dbx-navy-500 rounded-full overflow-hidden">
        <div className="h-2.5 rounded-full bg-emerald-500 transition-all duration-500" style={{ width: `${profiledPct}%` }} />
      </div>

      {profiled > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-3 pt-1">
          <CompletionBar label="Descriptions" value={h.with_comments || 0} total={profiled} color="bg-blue-500" />
          <CompletionBar label="Sensitivity (PII / PHI / PCI)" value={h.with_pii || 0} total={profiled} color="bg-violet-500" />
          <CompletionBar label="Domain Classification" value={h.with_domain || 0} total={profiled} color="bg-amber-500" />
          <CompletionBar label="Ontology Entities" value={h.with_ontology || 0} total={profiled} color="bg-orange-500"
            sub={h.entity_type_count > 0 ? `${h.entity_type_count} distinct entity types discovered` : undefined} />
          <CompletionBar label="Foreign Key Predictions" value={h.with_fk || 0} total={profiled} color="bg-rose-500"
            sub={h.fk_count > 0 ? `${h.fk_count} predicted relationships` : undefined} />
        </div>
      )}

      {reviewTotal > 0 && (
        <div className="pt-1">
          <div className="flex gap-4 mb-2">
            <span className="inline-flex items-center gap-1.5 text-xs"><span className="w-2 h-2 rounded-full bg-green-500" />{reviewCounts.approved} approved</span>
            <span className="inline-flex items-center gap-1.5 text-xs"><span className="w-2 h-2 rounded-full bg-yellow-500" />{reviewCounts.in_review} in review</span>
            <span className="inline-flex items-center gap-1.5 text-xs"><span className="w-2 h-2 rounded-full bg-slate-400" />{reviewCounts.unreviewed} unreviewed</span>
          </div>
          <div className="flex h-2 rounded-full overflow-hidden bg-slate-200 dark:bg-dbx-navy-500">
            {reviewCounts.approved > 0 && <div className="bg-green-500" style={{ width: `${(reviewCounts.approved / reviewTotal * 100).toFixed(1)}%` }} />}
            {reviewCounts.in_review > 0 && <div className="bg-yellow-500" style={{ width: `${(reviewCounts.in_review / reviewTotal * 100).toFixed(1)}%` }} />}
            {reviewCounts.unreviewed > 0 && <div className="bg-slate-400" style={{ width: `${(reviewCounts.unreviewed / reviewTotal * 100).toFixed(1)}%` }} />}
          </div>
        </div>
      )}

      {advancedStats.length > 0 && (
        <div className="flex flex-wrap gap-x-6 gap-y-1 text-xs text-slate-500 dark:text-slate-400 pt-1">
          {advancedStats.map((s, i) => <span key={i}>{s}</span>)}
          {h.metric_view_statuses && Object.keys(h.metric_view_statuses).length > 0 && (
            <span>Metric view status: {Object.entries(h.metric_view_statuses).map(([k, v]) => `${v} ${k}`).join(', ')}</span>
          )}
        </div>
      )}
    </div>
  )
}

/**
 * Collapsible coverage panel for embedding at the top of Review & Apply.
 */
export function CoveragePanel({ catalog }) {
  return (
    <details className="card mb-4">
      <summary className="px-5 py-3 cursor-pointer text-sm font-semibold text-slate-700 dark:text-slate-200 select-none">
        Catalog coverage &amp; review status
      </summary>
      <div className="px-5 pb-5">
        <CoverageSummary catalog={catalog} />
      </div>
    </details>
  )
}

// Full-page wrapper retained for backward compatibility / direct linking.
export default function Coverage() {
  const [catalogs, setCatalogs] = useState([])
  const [selectedCatalog, setSelectedCatalog] = useState('')

  useEffect(() => {
    fetch('/api/catalogs').then(r => r.ok ? r.json() : []).then(setCatalogs).catch(() => {})
    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data: cfg }) => {
      if (cfg?.catalog_name) setSelectedCatalog(cfg.catalog_name)
    })
  }, [])

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <PageHeader title="Metadata Coverage" subtitle="How complete is your catalog's metadata?" />
        {catalogs.length > 0 && (
          <select value={selectedCatalog} onChange={e => setSelectedCatalog(e.target.value)}
            className="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-slate-700 dark:text-slate-200">
            {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        )}
      </div>
      {!selectedCatalog ? (
        <div className="card p-8 text-center">
          <p className="text-sm text-slate-500 dark:text-slate-400">Select a catalog above to view coverage.</p>
        </div>
      ) : (
        <div className="card p-6">
          <CoverageSummary catalog={selectedCatalog} />
        </div>
      )}
    </div>
  )
}
