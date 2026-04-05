import React, { useState, useEffect } from 'react'
import { ErrorBanner } from '../App'
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

export default function Coverage() {
  const [holistic, setHolistic] = useState(null)
  const [reviewSummary, setReviewSummary] = useState(null)
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
    setHolistic(null)
    setReviewSummary(null)
    fetch(`/api/coverage/holistic${catParam}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`${r.status}`)))
      .then(setHolistic)
      .catch(() => setHolistic({ _error: true }))
    fetch(`/api/coverage/review-summary${catParam}`)
      .then(r => r.ok ? r.json() : []).then(setReviewSummary).catch(() => {})
  }, [selectedCatalog])

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

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <PageHeader title="Metadata Coverage" subtitle="How complete is your catalog's metadata?" />
        {catalogs.length > 0 && (
          <select value={selectedCatalog} onChange={e => { setSelectedCatalog(e.target.value) }}
            className="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-dbx-navy-400 bg-white dark:bg-dbx-navy-600 text-slate-700 dark:text-slate-200">
            {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        )}
      </div>

      {!selectedCatalog ? (
        <div className="card p-8 text-center">
          <p className="text-sm text-slate-500 dark:text-slate-400">Select a catalog above to view coverage.</p>
        </div>
      ) : holistic?._error ? (
        <div className="card p-8 text-center space-y-2">
          <p className="text-sm text-slate-500 dark:text-slate-400">Could not load coverage data.</p>
          <button onClick={() => { setHolistic(null); setSelectedCatalog(c => c) }}
            className="text-xs px-3 py-1.5 rounded-md bg-dbx-lava text-white hover:bg-dbx-lava/90">Retry</button>
        </div>
      ) : !holistic ? (
        <SkeletonCards count={3} />
      ) : total === 0 ? (
        <div className="card p-8 text-center">
          <p className="text-sm text-slate-500 dark:text-slate-400">No tables found in this catalog. Run metadata generation first.</p>
        </div>
      ) : (
        <>
          {/* Headline stat */}
          <div className="card p-6">
            <div className="flex items-baseline gap-3 mb-3">
              <span className="text-3xl font-bold text-dbx-navy dark:text-white tabular-nums">{profiled}</span>
              <span className="text-lg text-slate-400 dark:text-slate-500">of {total} tables profiled</span>
              <span className="ml-auto text-2xl font-semibold tabular-nums text-slate-500 dark:text-slate-400">{profiledPct}%</span>
            </div>
            <div className="h-3 bg-slate-200/60 dark:bg-dbx-navy-500 rounded-full overflow-hidden">
              <div className="h-3 rounded-full bg-emerald-500 transition-all duration-500" style={{ width: `${profiledPct}%` }} />
            </div>
          </div>

          {/* Metadata completeness */}
          {profiled > 0 && (
            <div className="card p-6 space-y-4">
              <div>
                <h2 className="text-base font-semibold text-slate-800 dark:text-slate-100">Metadata Completeness</h2>
                <p className="text-xs text-slate-400 dark:text-slate-500 mt-0.5">Of {profiled} profiled tables, how many have each metadata type?</p>
              </div>
              <CompletionBar label="Descriptions" value={h.with_comments || 0} total={profiled} color="bg-blue-500" />
              <CompletionBar label="Sensitivity (PII / PHI / PCI)" value={h.with_pii || 0} total={profiled} color="bg-violet-500" />
              <CompletionBar label="Domain Classification" value={h.with_domain || 0} total={profiled} color="bg-amber-500" />
              <CompletionBar label="Ontology Entities" value={h.with_ontology || 0} total={profiled} color="bg-orange-500"
                sub={h.entity_type_count > 0 ? `${h.entity_type_count} distinct entity types discovered` : undefined} />
              <CompletionBar label="Foreign Key Predictions" value={h.with_fk || 0} total={profiled} color="bg-rose-500"
                sub={h.fk_count > 0 ? `${h.fk_count} predicted relationships` : undefined} />
            </div>
          )}

          {/* Governance review status */}
          {reviewTotal > 0 && (
            <div className="card p-5">
              <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200 mb-2.5">Review Status</h3>
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

          {/* Advanced metrics */}
          {advancedStats.length > 0 && (
            <details className="card">
              <summary className="px-5 py-3 cursor-pointer text-sm font-medium text-slate-600 dark:text-slate-300 select-none">
                Advanced Metrics
              </summary>
              <div className="px-5 pb-4 flex flex-wrap gap-x-6 gap-y-1 text-xs text-slate-500 dark:text-slate-400">
                {advancedStats.map((s, i) => <span key={i}>{s}</span>)}
                {h.metric_view_statuses && Object.keys(h.metric_view_statuses).length > 0 && (
                  <span>Metric view status: {Object.entries(h.metric_view_statuses).map(([k, v]) => `${v} ${k}`).join(', ')}</span>
                )}
              </div>
            </details>
          )}
        </>
      )}
    </div>
  )
}
