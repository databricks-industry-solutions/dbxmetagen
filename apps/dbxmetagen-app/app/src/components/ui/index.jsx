import React, { useState } from 'react'

/* ── Icons (inline SVG to avoid dependencies) ─────────────────────── */
const ChevronDown = ({ className = 'w-4 h-4' }) => (
  <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
)
const ChevronUp = ({ className = 'w-4 h-4' }) => (
  <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" /></svg>
)
const SortIcon = ({ dir }) => (
  <span className="inline-flex flex-col ml-1 -space-y-1 text-[8px] leading-none">
    <span className={dir === 'asc' ? 'text-dbx-teal' : 'text-slate-300 dark:text-slate-600'}>&#9650;</span>
    <span className={dir === 'desc' ? 'text-dbx-teal' : 'text-slate-300 dark:text-slate-600'}>&#9660;</span>
  </span>
)
const EmptyIcon = ({ className = 'w-10 h-10' }) => (
  <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
  </svg>
)

/* ── PageHeader ───────────────────────────────────────────────────── */
export function PageHeader({ title, subtitle, badge, actions }) {
  return (
    <div className="flex items-start justify-between mb-6">
      <div>
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-bold tracking-tight text-slate-800 dark:text-white">{title}</h1>
          {badge && <span className="badge bg-dbx-teal/10 text-dbx-teal text-xs">{badge}</span>}
        </div>
        {subtitle && <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">{subtitle}</p>}
      </div>
      {actions && <div className="flex items-center gap-2 flex-shrink-0">{actions}</div>}
    </div>
  )
}

/* ── Skeleton primitives ──────────────────────────────────────────── */
export function Skeleton({ className = 'h-4 w-full' }) {
  return <div className={`animate-pulse bg-slate-200 dark:bg-dbx-navy-500/40 rounded ${className}`} />
}

export function SkeletonTable({ rows = 5, cols = 4 }) {
  return (
    <div className="surface-nested p-1 overflow-hidden">
      <div className="flex gap-2 px-3 py-2.5">
        {Array.from({ length: cols }).map((_, i) => <Skeleton key={i} className="h-3 flex-1 rounded" />)}
      </div>
      {Array.from({ length: rows }).map((_, r) => (
        <div key={r} className="flex gap-2 px-3 py-2.5 border-t border-slate-100 dark:border-dbx-navy-400/10">
          {Array.from({ length: cols }).map((_, c) => <Skeleton key={c} className="h-3 flex-1 rounded" />)}
        </div>
      ))}
    </div>
  )
}

export function SkeletonCards({ count = 3 }) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="card p-5 space-y-3">
          <Skeleton className="h-3 w-20" />
          <Skeleton className="h-7 w-16" />
          <Skeleton className="h-2 w-24" />
        </div>
      ))}
    </div>
  )
}

/* ── EmptyState ───────────────────────────────────────────────────── */
export function EmptyState({ icon, title, description, action }) {
  return (
    <div className="flex flex-col items-center justify-center py-12 text-center">
      <div className="text-slate-300 dark:text-slate-600 mb-3">
        {icon || <EmptyIcon className="w-10 h-10" />}
      </div>
      <p className="text-sm font-semibold text-slate-500 dark:text-slate-400">{title}</p>
      {description && <p className="text-xs text-slate-400 dark:text-slate-500 mt-1 max-w-sm">{description}</p>}
      {action && (
        <button onClick={action.onClick} className="btn-primary btn-sm mt-4">{action.label}</button>
      )}
    </div>
  )
}

/* ── StatCard ─────────────────────────────────────────────────────── */
export function StatCard({ label, value, sub, icon, accentColor = 'border-l-dbx-lava', onClick, warn }) {
  return (
    <div onClick={onClick}
      className={`card border-l-4 ${accentColor} p-5 bg-gradient-to-br from-white to-slate-50 dark:from-dbx-navy-650 dark:to-dbx-navy-700 ${onClick ? 'cursor-pointer hover:shadow-card-hover hover:-translate-y-0.5 transition-all duration-200' : ''}`}>
      <div className="flex items-center gap-2">
        {icon && <span className="text-slate-400 dark:text-slate-500">{icon}</span>}
        <p className="text-xs font-medium uppercase tracking-wider text-slate-500 dark:text-slate-400">{label}</p>
      </div>
      <p className={`text-2xl font-bold mt-1 ${warn ? 'text-dbx-amber' : 'text-slate-800 dark:text-white'}`}>{value ?? '--'}</p>
      {sub && <p className="text-xs text-slate-400 dark:text-slate-500 mt-1 truncate">{sub}</p>}
    </div>
  )
}

/* ── DataTable ────────────────────────────────────────────────────── */
export function DataTable({ columns, data, sortable = false, onRowClick, emptyMessage, zebra = true, maxRows = 200 }) {
  const [sortKey, setSortKey] = useState(null)
  const [sortDir, setSortDir] = useState('asc')

  if (!data || data.length === 0) {
    return <EmptyState title={emptyMessage || 'No data available'} />
  }

  const cols = columns || Object.keys(data[0]).map(k => ({ key: k, label: k }))

  const handleSort = (key) => {
    if (!sortable) return
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  const sorted = sortKey
    ? [...data].sort((a, b) => {
        const av = a[sortKey], bv = b[sortKey]
        if (av == null) return 1
        if (bv == null) return -1
        const cmp = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv))
        return sortDir === 'asc' ? cmp : -cmp
      })
    : data

  return (
    <div className="surface-nested overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr>
            {cols.map(c => (
              <th key={c.key}
                onClick={() => (sortable || c.sortable) && handleSort(c.key)}
                className={`text-left px-3 py-2.5 bg-slate-50/80 dark:bg-dbx-navy-600/40 backdrop-blur-sm font-semibold text-slate-500 dark:text-slate-400 border-b border-slate-100 dark:border-dbx-navy-400/20 text-xs uppercase tracking-wider sticky top-0 z-10 ${(sortable || c.sortable) ? 'cursor-pointer select-none hover:text-dbx-teal' : ''} ${c.className || ''}`}>
                {c.label}{(sortable || c.sortable) && <SortIcon dir={sortKey === c.key ? sortDir : null} />}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.slice(0, maxRows).map((row, i) => (
            <tr key={i}
              onClick={() => onRowClick?.(row, i)}
              className={`border-b border-slate-100/80 dark:border-dbx-navy-400/10 hover:bg-dbx-teal-light/20 dark:hover:bg-dbx-navy-500/30 transition-colors ${onRowClick ? 'cursor-pointer' : ''} ${zebra && i % 2 === 1 ? 'bg-slate-50/50 dark:bg-dbx-navy-700/30' : ''}`}>
              {cols.map(c => (
                <td key={c.key} className={`px-3 py-2 max-w-xs truncate text-slate-600 dark:text-slate-300 ${c.className || ''}`} title={String(row[c.key] ?? '')}>
                  {c.render ? c.render(row[c.key], row) : String(row[c.key] ?? '')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ── Section (collapsible) ────────────────────────────────────────── */
export function Section({ title, defaultOpen = true, badge, children }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="card overflow-hidden">
      <button onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-5 py-3.5 hover:bg-slate-50/50 dark:hover:bg-dbx-navy-600/30 transition-colors">
        <div className="flex items-center gap-2">
          <span className="heading-section">{title}</span>
          {badge && <span className="badge bg-dbx-teal/10 text-dbx-teal text-xs">{badge}</span>}
        </div>
        {open ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
      </button>
      <div className={`transition-all duration-200 ease-in-out ${open ? 'max-h-[5000px] opacity-100' : 'max-h-0 opacity-0 overflow-hidden'}`}>
        <div className="px-5 pb-5 pt-1">{children}</div>
      </div>
    </div>
  )
}

/* ── AccentBar ────────────────────────────────────────────────────── */
export function AccentBar({ color = 'bg-dbx-teal' }) {
  return <div className={`h-0.5 ${color}`} />
}
