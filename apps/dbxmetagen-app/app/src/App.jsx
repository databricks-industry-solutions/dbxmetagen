import React, { useState, useEffect, useRef } from 'react'

// Global fetch interceptor: detect expired Databricks Apps OAuth sessions.
// When the session cookie expires, the proxy redirects to a login page --
// fetch follows the redirect and returns 200 + text/html instead of JSON.
const _originalFetch = window.fetch
window.fetch = async (...args) => {
  const res = await _originalFetch(...args)
  const ct = res.headers.get('content-type') || ''
  if (res.status === 401) {
    window.dispatchEvent(new Event('session-expired'))
  } else if (res.status === 403 && !ct.includes('application/json')) {
    window.dispatchEvent(new Event('session-expired'))
  } else if (res.ok) {
    const url = typeof args[0] === 'string' ? args[0] : args[0]?.url || ''
    if (url.startsWith('/api/') && ct.includes('text/html')) {
      window.dispatchEvent(new Event('session-expired'))
    }
  }
  return res
}

import { cachedFetchObj, TTL } from './apiCache'
import { Joyride, STATUS } from 'react-joyride'
import AgentChat from './components/AgentChat'
import BatchJobs from './components/BatchJobs'
import MetadataReview from './components/MetadataReview'
import Coverage from './components/Coverage'
import EntityBrowser from './components/EntityBrowser'
import SemanticLayer from './components/SemanticLayer'
import GenieBuilder from './components/GenieBuilder'
import AnalystChat from './components/AnalystChat'
import CustomerContext from './components/CustomerContext'
import OntologyBuilder from './components/OntologyBuilder'
import GettingStarted from './components/GettingStarted'
const COMPONENTS = {
  guide: GettingStarted,
  agent: AgentChat, coverage: Coverage, jobs: BatchJobs, metadata: MetadataReview,
  entities: EntityBrowser, semantic: SemanticLayer, genie: GenieBuilder,
  analyst: AnalystChat, context: CustomerContext, ontologyBuilder: OntologyBuilder,
}

const NAV_CAT_COLORS = {
  Design: 'text-dbx-lava',
  Review: 'text-dbx-sky dark:text-dbx-sky',
  Explore: 'text-dbx-violet dark:text-dbx-violet',
}

const TAB_ACCENT = {
  guide: 'border-t-dbx-lava',
  jobs: 'border-t-dbx-lava', semantic: 'border-t-dbx-lava', genie: 'border-t-dbx-lava', context: 'border-t-dbx-lava', ontologyBuilder: 'border-t-dbx-lava',
  metadata: 'border-t-dbx-sky', coverage: 'border-t-dbx-sky', entities: 'border-t-dbx-sky',
  agent: 'border-t-dbx-teal',
}

const NAV_STRUCTURE = [
  {
    category: 'Design',
    items: [
      { id: 'jobs',     label: 'Generate Metadata', desc: 'Descriptions, sensitivity, domains, ontology, FK predictions, and metrics' },
      { id: 'semantic', label: 'Define Metrics', desc: 'Define metric views and KPIs' },
      { id: 'genie',   label: 'Build Genie Space', desc: 'Build natural-language SQL spaces' },
      { id: 'ontologyBuilder', label: 'Build Ontology', desc: 'Visual entity, relationship, and property editor' },
    ],
  },
  {
    category: 'Review',
    items: [
      { id: 'metadata', label: 'Review & Apply', desc: 'Review, edit, and apply AI-generated metadata' },
      { id: 'coverage', label: 'Coverage', desc: 'Catalog health and metadata completeness' },
    ],
  },
  {
    category: 'Explore',
    items: [
      { id: 'agent',      label: 'Agent',           desc: 'Chat, graph explorer, and semantic search' },
      { id: 'entities', label: 'Entity Browser', desc: 'Entity-first navigation with conformance view' },
      // { id: 'analyst',    label: 'SQL Analyst Comparison', desc: 'Demonstrate semantic layer value with side-by-side agents' },
    ],
  },
]

export async function safeFetch(url, options) {
  try {
    const res = await fetch(url, options)
    if (!res.ok) {
      const body = await res.text().catch(() => '')
      let msg = `Error ${res.status}`
      try { const j = JSON.parse(body); if (j.detail) msg = j.detail } catch {}
      return { data: null, error: msg }
    }
    const data = await res.json()
    return { data, error: null }
  } catch (e) { return { data: null, error: e.message } }
}

export async function safeFetchObj(url, options) {
  try {
    const res = await fetch(url, options)
    if (!res.ok) {
      const body = await res.text().catch(() => '')
      let msg = `Error ${res.status}`
      try { const j = JSON.parse(body); if (j.detail) msg = j.detail } catch {}
      return { data: null, error: msg }
    }
    return { data: await res.json(), error: null }
  } catch (e) { return { data: null, error: e.message } }
}

export function PrereqBanner({ show, message, actionLabel, onAction }) {
  if (!show) return null
  return (
    <div className="rounded-xl border border-amber-200 dark:border-amber-700/40 bg-amber-50/80 dark:bg-amber-900/15 px-4 py-3 text-sm flex items-center gap-3 animate-slide-up mb-4">
      <svg className="w-4 h-4 text-amber-500 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      <span className="text-slate-600 dark:text-slate-300">{message}</span>
      {actionLabel && onAction && (
        <button onClick={onAction} className="shrink-0 text-xs font-semibold text-dbx-lava hover:underline">{actionLabel}</button>
      )}
    </div>
  )
}

export function ErrorBanner({ error, onDismiss }) {
  if (!error) return null
  return (
    <div className="card border-l-4 border-l-red-500 px-4 py-3 text-sm animate-slide-up flex justify-between items-start">
      <div>
        <span className="font-medium text-red-600 dark:text-red-400">Error:</span>{' '}
        <span className="text-slate-600 dark:text-slate-300">{error}</span>
      </div>
      {onDismiss && <button onClick={onDismiss} className="text-slate-400 hover:text-slate-600 ml-2">&times;</button>}
    </div>
  )
}

const TOUR_STEPS = [
  {
    target: '[data-tour="auth-badge"]',
    content: 'Check your connection status here. Green = catalog and schema access confirmed. Amber = OBO token issue. Red = missing permissions. Click to see details.',
    disableBeacon: true,
  },
  {
    target: '[data-tour="stepper"]',
    content: 'This workflow bar tracks your progress. Green checks appear as each step gets data. Click any step to jump there.',
  },
  {
    target: '[data-tour="nav-design"]',
    content: 'Start here. Run Core Metadata first (descriptions + sensitivity + domains), then Advanced Metadata to build the knowledge graph. Start with a few tables to verify results before scaling up.',
  },
  {
    target: '[data-tour="nav-review"]',
    content: 'Review every AI-generated result before applying. Pay special attention to PI/PHI classifications (compliance risk) and FK predictions (structural impact).',
  },
  {
    target: '[data-tour="nav-explore"]',
    content: 'Once metadata is generated and reviewed, explore it here. The agent, entity browser, and graph are powered by the same Delta tables you can also query directly.',
  },
  {
    target: '[data-tour="header-guide"]',
    content: 'Come back to the Getting Started page any time from here.',
  },
]

const PIPELINE_STEPS = [
  { id: 'jobs', label: 'Generate Metadata', short: 'Generate',
    desc: 'Run core (descriptions, sensitivity, domain) and advanced (ontology, FK, graph) jobs',
    countKey: 'profiled', totalKey: 'total_tables', unit: 'tables described' },
  { id: 'metadata', label: 'Review & Apply', short: 'Review',
    desc: 'Inspect AI-generated metadata, edit, and apply to Unity Catalog',
    countKey: 'with_comments', totalKey: 'total_tables', unit: 'with comments' },
  { id: 'semantic', label: 'Define Metrics', short: 'Metrics',
    desc: 'Create reusable KPI definitions as metric views',
    countKey: 'metric_views', unit: 'metric views' },
  { id: 'genie', label: 'Build Genie Space', short: 'Genie',
    desc: 'Create natural-language SQL spaces for end users',
    countKey: null, unit: null },
  { id: 'agent', label: 'Explore', short: 'Explore',
    desc: 'Chat with the metadata agent, graph explorer, and semantic search',
    countKey: 'vs_documents', unit: 'indexed docs' },
]

function PipelineStepper({ activeTab, onSelect, stats }) {
  const [dismissed, setDismissed] = useState(() => {
    try { return localStorage.getItem('dbxmetagen_stepperDismissed') === 'true' } catch { return false }
  })

  const dismiss = () => {
    setDismissed(true)
    try { localStorage.setItem('dbxmetagen_stepperDismissed', 'true') } catch {}
  }
  const show = () => {
    setDismissed(false)
    try { localStorage.removeItem('dbxmetagen_stepperDismissed') } catch {}
  }

  if (dismissed) {
    return (
      <button onClick={show} className="text-xs text-slate-400 dark:text-slate-500 hover:text-dbx-lava dark:hover:text-dbx-lava transition-colors flex items-center gap-1 mb-2">
        <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" /></svg>
        Show workflow guide
      </button>
    )
  }

  const activeIdx = PIPELINE_STEPS.findIndex(s => s.id === activeTab)

  return (
    <div className="mb-4 animate-slide-up" data-tour="stepper">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">Workflow</span>
        <button onClick={dismiss} className="text-xs text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 transition-colors">Hide</button>
      </div>
      <div className="flex items-stretch gap-0">
        {PIPELINE_STEPS.map((step, i) => {
          const count = step.countKey && stats ? (stats[step.countKey] || 0) : null
          const total = step.totalKey && stats ? (stats[step.totalKey] || 0) : null
          const isActive = step.id === activeTab
          const isDone = count !== null && count > 0
          const isReachable = i === 0 || (stats && (stats.profiled || 0) > 0)

          let ringColor = 'border-slate-300 dark:border-slate-600 text-slate-400 dark:text-slate-500'
          let bgColor = 'bg-white dark:bg-dbx-navy-600'
          if (isActive) {
            ringColor = 'border-dbx-lava text-dbx-lava'
            bgColor = 'bg-red-50/50 dark:bg-dbx-lava/10'
          } else if (isDone) {
            ringColor = 'border-emerald-400 text-emerald-500'
            bgColor = 'bg-emerald-50/50 dark:bg-emerald-900/10'
          }

          return (
            <React.Fragment key={step.id}>
              {i > 0 && (
                <div className="flex items-center px-0.5 shrink-0">
                  <div className={`w-6 h-0.5 ${isDone || isActive ? 'bg-emerald-300 dark:bg-emerald-600' : 'bg-slate-200 dark:bg-slate-700'}`} />
                </div>
              )}
              <button
                onClick={() => onSelect(step.id)}
                title={step.desc}
                className={`flex-1 min-w-0 rounded-xl border-2 ${ringColor} ${bgColor} px-3 py-2.5 text-left transition-all hover:shadow-sm group ${
                  !isReachable && !isActive ? 'opacity-50' : ''
                }`}
              >
                <div className="flex items-center gap-2 mb-0.5">
                  <span className={`flex items-center justify-center w-5 h-5 rounded-full text-[10px] font-bold shrink-0 ${
                    isActive ? 'bg-dbx-lava text-white'
                    : isDone ? 'bg-emerald-500 text-white'
                    : 'bg-slate-200 dark:bg-slate-700 text-slate-500 dark:text-slate-400'
                  }`}>
                    {isDone && !isActive ? (
                      <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>
                    ) : (i + 1)}
                  </span>
                  <span className={`text-xs font-semibold truncate ${isActive ? 'text-dbx-lava' : 'text-slate-700 dark:text-slate-200'}`}>
                    {step.short}
                  </span>
                </div>
                {count !== null && (
                  <p className="text-[10px] text-slate-500 dark:text-slate-400 mt-0.5 truncate pl-7">
                    {total !== null ? `${count}/${total}` : count} {step.unit}
                  </p>
                )}
                {count === null && step.unit === null && stats && (stats.profiled || 0) > 0 && (
                  <p className="text-[10px] text-slate-500 dark:text-slate-400 mt-0.5 truncate pl-7">Ready</p>
                )}
              </button>
            </React.Fragment>
          )
        })}
      </div>
    </div>
  )
}

function NavDropdown({ cat, activeTab, onSelect, dataTour }) {
  const [open, setOpen] = useState(false)
  const ref = useRef(null)
  const activeItem = cat.items.find(i => i.id === activeTab)
  const catColor = NAV_CAT_COLORS[cat.category] || 'text-slate-500'

  useEffect(() => {
    if (!open) return
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  return (
    <div ref={ref} className="relative" data-tour={dataTour}>
      <button onClick={() => setOpen(!open)}
        className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 whitespace-nowrap ${
          activeItem
            ? 'bg-white dark:bg-dbx-navy-500 text-dbx-lava shadow-sm'
            : 'text-slate-600 dark:text-slate-400 hover:text-dbx-navy dark:hover:text-slate-200 hover:bg-white/60 dark:hover:bg-dbx-navy-500/50'
        }`}>
        <span className={`text-[10px] font-semibold uppercase tracking-wider ${catColor}`}>{cat.category}</span>
        {cat.beta && <span className="ml-1 px-1 py-0.5 text-[8px] font-bold uppercase tracking-wider bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300 rounded">Beta</span>}
        {activeItem && <span className="text-slate-400 dark:text-slate-500 mx-0.5">/</span>}
        {activeItem && <span>{activeItem.label}{activeItem.beta && <span className="ml-1 px-1 py-0.5 text-[8px] font-bold uppercase tracking-wider bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300 rounded">Beta</span>}</span>}
        <svg className={`w-3 h-3 transition-transform duration-200 ${open ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {open && (
        <div className="absolute top-full left-0 mt-1 bg-white dark:bg-dbx-navy-600 rounded-xl shadow-elevated border border-slate-200/80 dark:border-dbx-navy-400/30 py-1.5 min-w-[220px] z-50 animate-slide-up">
          {cat.items.map(item => (
            <button key={item.id}
              onClick={() => { onSelect(item.id); setOpen(false) }}
              className={`w-full text-left px-4 py-2.5 transition-colors ${
                activeTab === item.id
                  ? 'bg-dbx-oat-light dark:bg-dbx-navy-500/60 text-dbx-lava'
                  : 'text-slate-700 dark:text-slate-300 hover:bg-dbx-oat-light/60 dark:hover:bg-dbx-navy-500/40'
              }`}>
              <span className="text-sm font-medium block">{item.label}{item.beta && <span className="ml-1.5 px-1 py-0.5 text-[8px] font-bold uppercase tracking-wider bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300 rounded align-middle">Beta</span>}</span>
              <span className="text-xs text-slate-400 dark:text-slate-500">{item.desc}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

class TabErrorBoundary extends React.Component {
  state = { error: null }
  static getDerivedStateFromError(error) { return { error } }
  componentDidCatch(err, info) {
    console.error('TabErrorBoundary caught:', err, info)
    if (err?.message?.includes('dynamically imported module') || err?.message?.includes('Loading chunk')) {
      window.dispatchEvent(new Event('session-expired'))
    }
  }
  render() {
    if (this.state.error) {
      return (
        <div className="card border-l-4 border-l-red-500 p-6 m-4">
          <h3 className="text-lg font-semibold text-red-600 dark:text-red-400 mb-2">Something went wrong</h3>
          <p className="text-sm text-slate-600 dark:text-slate-300 mb-4">
            {this.state.error?.message || 'An unexpected error occurred.'}
            {' '}This may be a permissions issue -- check that the app service principal has access to Unity Catalog.
          </p>
          <button onClick={() => this.setState({ error: null })}
            className="btn-primary btn-sm">Try again</button>
        </div>
      )
    }
    return this.props.children
  }
}

const VALID_TABS = new Set(Object.keys(COMPONENTS))
const readHash = () => { const h = window.location.hash.replace('#', ''); return VALID_TABS.has(h) ? h : 'jobs' }

function AuthStatusBadge() {
  const [auth, setAuth] = useState(null)
  const [expanded, setExpanded] = useState(false)
  useEffect(() => {
    fetch('/api/auth/check').then(r => r.ok ? r.json() : null).then(setAuth).catch(() => {})
  }, [])
  if (!auth) return null
  const ok = auth.has_catalog_access && auth.has_schema_access
  const hasWarning = auth.obo_enabled && !auth.obo_token_received
  const color = hasWarning ? 'bg-amber-500/20 text-amber-300 border-amber-400/30'
    : ok ? 'bg-emerald-500/15 text-emerald-300 border-emerald-400/30'
    : 'bg-red-500/20 text-red-300 border-red-400/30'
  const label = auth.user_identity
    ? auth.user_identity.split('@')[0]
    : (auth.obo_enabled ? 'SP fallback' : 'Service principal')
  return (
    <div className="relative" data-tour="auth-badge">
      <button onClick={() => setExpanded(e => !e)}
        className={`flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-[11px] font-medium border ${color} transition-all hover:brightness-125`}
        title={`Running as: ${auth.running_as}`}>
        <span className={`w-1.5 h-1.5 rounded-full ${hasWarning ? 'bg-amber-400' : ok ? 'bg-emerald-400' : 'bg-red-400'}`} />
        {label}
      </button>
      {expanded && (
        <div className="absolute right-0 top-full mt-2 w-80 bg-white dark:bg-dbx-navy-600 rounded-xl shadow-elevated border border-slate-200 dark:border-dbx-navy-400/30 p-4 text-xs z-50 space-y-2 animate-slide-up">
          <div className="flex justify-between"><span className="text-slate-500 dark:text-slate-400">Running as</span><span className="font-medium text-slate-700 dark:text-slate-200">{auth.running_as}</span></div>
          {auth.user_identity && <div className="flex justify-between"><span className="text-slate-500 dark:text-slate-400">User</span><span className="font-medium text-slate-700 dark:text-slate-200">{auth.user_identity}</span></div>}
          <div className="flex justify-between"><span className="text-slate-500 dark:text-slate-400">OBO enabled</span><span className={auth.obo_enabled ? 'text-emerald-600 dark:text-emerald-400' : 'text-slate-500'}>{auth.obo_enabled ? 'Yes' : 'No'}</span></div>
          {auth.obo_enabled && <div className="flex justify-between"><span className="text-slate-500 dark:text-slate-400">OBO token received</span><span className={auth.obo_token_received ? 'text-emerald-600 dark:text-emerald-400' : 'text-amber-600 dark:text-amber-400'}>{auth.obo_token_received ? 'Yes' : 'No'}</span></div>}
          <div className="flex justify-between"><span className="text-slate-500 dark:text-slate-400">Catalog access</span><span className={auth.has_catalog_access ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}>{auth.has_catalog_access ? 'OK' : 'Failed'}</span></div>
          <div className="flex justify-between"><span className="text-slate-500 dark:text-slate-400">Schema access</span><span className={auth.has_schema_access ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}>{auth.has_schema_access ? 'OK' : 'Failed'}</span></div>
          {auth.identity_error && <p className="text-amber-600 dark:text-amber-400 leading-relaxed">{auth.identity_error}</p>}
          {auth.catalog_error && <p className="text-red-600 dark:text-red-400 leading-relaxed">Catalog: {auth.catalog_error}</p>}
          {auth.schema_error && <p className="text-red-600 dark:text-red-400 leading-relaxed">Schema: {auth.schema_error}</p>}
        </div>
      )}
    </div>
  )
}

export default function App() {
  const [activeTab, setActiveTab] = useState(() => {
    const h = readHash()
    if (h === 'jobs') {
      try { if (!localStorage.getItem('dbxmetagen_tourSeen')) return 'guide' } catch {}
    }
    return h
  })
  const [visitedTabs, setVisitedTabs] = useState(new Set([activeTab]))
  const [runTour, setRunTour] = useState(false)
  const [sessionExpired, setSessionExpired] = useState(false)
  const [pipelineStats, setPipelineStats] = useState(null)
  const [appMeta, setAppMeta] = useState({ displayName: '', version: '' })
  const [dark, setDark] = useState(() => {
    if (typeof window !== 'undefined') {
      const saved = localStorage.getItem('dbxmetagen-dark')
      if (saved !== null) return saved === 'true'
      return window.matchMedia('(prefers-color-scheme: dark)').matches
    }
    return false
  })

  useEffect(() => {
    setVisitedTabs(prev => {
      if (prev.has(activeTab)) return prev
      const next = new Set(prev)
      next.add(activeTab)
      return next
    })
    if (window.location.hash !== `#${activeTab}`) window.history.pushState(null, '', `#${activeTab}`)
  }, [activeTab])

  useEffect(() => {
    const onHash = () => { const t = readHash(); if (t !== activeTab) setActiveTab(t) }
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [activeTab])

  useEffect(() => {
    if (activeTab !== 'guide') {
      try { localStorage.setItem('dbxmetagen_tourSeen', 'true') } catch {}
    }
  }, [activeTab])

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('dbxmetagen-dark', String(dark))
  }, [dark])

  useEffect(() => {
    cachedFetchObj('/api/config', {}, TTL.CONFIG).then(({ data }) => {
      if (data) setAppMeta({ displayName: data.app_display_name || '', version: data.version || '' })
    })
  }, [])

  useEffect(() => {
    cachedFetchObj('/api/coverage/holistic', {}, TTL.DASHBOARD)
      .then(({ data }) => { if (data) setPipelineStats(data) })
  }, [activeTab])

  useEffect(() => {
    const h = () => setSessionExpired(true)
    window.addEventListener('session-expired', h)
    return () => window.removeEventListener('session-expired', h)
  }, [])

  return (
    <div className="min-h-screen bg-dbx-oat-light dark:bg-dbx-navy transition-colors">
      {/* Header */}
      <header className="bg-gradient-to-r from-dbx-navy via-dbx-navy-700 to-dbx-navy-600 px-6 py-4 shadow-md">
        <div className="flex items-center justify-between max-w-[90rem] mx-auto">
          <div className="flex items-center gap-3">
            <div className="relative">
              <img src="/logo.png" alt="dbxmetagen" className="h-9 w-9" />
              <div className="absolute -bottom-0.5 left-0 right-0 h-0.5 bg-dbx-lava rounded-full" />
            </div>
            <div>
              <h1 className="text-xl font-bold text-white tracking-tight">
                dbxmetagen{appMeta.displayName && <><span className="text-dbx-oat/50 font-normal mx-1.5">:</span><span className="text-dbx-lava font-semibold">{appMeta.displayName}</span></>}
                {appMeta.version && <span className="ml-2 text-xs font-normal text-dbx-oat/40">v{appMeta.version}</span>}
              </h1>
              <p className="text-dbx-oat/60 text-xs mt-0.5">Automated metadata, knowledge graph, and semantic layer for Unity Catalog</p>
            </div>
          </div>
          <div className="flex items-center gap-1.5">
            <AuthStatusBadge />
            <button onClick={() => setActiveTab('guide')} data-tour="header-guide"
              className="w-8 h-8 flex items-center justify-center rounded-lg bg-white/10 backdrop-blur-sm border border-white/10 text-dbx-oat/80 hover:text-white hover:bg-white/20 transition-all text-sm font-bold"
              title="Getting Started" aria-label="Getting Started">?</button>
            <button onClick={() => setDark(d => !d)}
              className="w-8 h-8 flex items-center justify-center rounded-lg bg-white/10 backdrop-blur-sm border border-white/10 text-dbx-oat/80 hover:text-white hover:bg-white/20 transition-all"
              title={dark ? 'Switch to light mode' : 'Switch to dark mode'}
              aria-label="Toggle dark mode">
              {dark ? (
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                    d="M12 3v1m0 16v1m8.66-13.66l-.71.71M4.05 19.95l-.71.71M21 12h-1M4 12H3m16.95 7.95l-.71-.71M4.05 4.05l-.71-.71M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
                </svg>
              ) : (
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                    d="M20.354 15.354A9 9 0 018.646 3.646 9.005 9.005 0 0012 21a9.005 9.005 0 008.354-5.646z" />
                </svg>
              )}
            </button>
          </div>
        </div>
      </header>

      <Joyride
        steps={TOUR_STEPS}
        run={runTour}
        continuous
        showSkipButton
        showProgress
        callback={({ status }) => {
          if ([STATUS.FINISHED, STATUS.SKIPPED].includes(status)) setRunTour(false)
        }}
        styles={{
          options: { primaryColor: '#FF3621', zIndex: 10000 },
          tooltip: { borderRadius: 12, fontSize: 14 },
        }}
      />

      {/* Navigation */}
      <nav className="bg-white/80 dark:bg-dbx-navy-700/90 backdrop-blur-sm border-b border-dbx-oat-dark/50 dark:border-dbx-navy-400/20 shadow-nav sticky top-0 z-40">
        <div className="flex items-center gap-2 px-6 py-2 max-w-[90rem] mx-auto">
          <button
            onClick={() => setActiveTab('guide')}
            className={`px-3.5 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 whitespace-nowrap ${
              activeTab === 'guide'
                ? 'bg-white dark:bg-dbx-navy-500 text-dbx-lava shadow-sm'
                : 'text-slate-600 dark:text-slate-400 hover:text-dbx-navy dark:hover:text-slate-200 hover:bg-white/60 dark:hover:bg-dbx-navy-500/50'
            }`}>
            Guide
          </button>
          <div className="w-px h-6 bg-slate-200 dark:bg-dbx-navy-400/40 mx-0.5 flex-shrink-0" />
          {NAV_STRUCTURE.map((cat, ci) => (
            <React.Fragment key={cat.category}>
              {ci > 0 && <div className="w-px h-6 bg-slate-200 dark:bg-dbx-navy-400/40 mx-0.5 flex-shrink-0" />}
              <NavDropdown cat={cat} activeTab={activeTab} onSelect={setActiveTab} dataTour={`nav-${cat.category.toLowerCase()}`} />
            </React.Fragment>
          ))}
        </div>
      </nav>

      <main className="p-6 max-w-[90rem] mx-auto">
        <PipelineStepper activeTab={activeTab} onSelect={setActiveTab} stats={pipelineStats} />
        {Object.entries(COMPONENTS).map(([tabId, Comp]) => {
          if (!visitedTabs.has(tabId)) return null
          const isActive = tabId === activeTab
          return (
            <div key={tabId}
              className={`border-t-2 ${TAB_ACCENT[tabId] || 'border-t-transparent'} pt-4 ${isActive ? 'animate-slide-up' : ''}`}
              style={{ display: isActive ? 'block' : 'none' }}>
              <TabErrorBoundary>
                <Comp onNavigate={setActiveTab} pipelineStats={pipelineStats}
                  {...(tabId === 'guide' ? { onStartTour: () => setRunTour(true) } : {})} />
              </TabErrorBoundary>
            </div>
          )
        })}
      </main>
      {sessionExpired && (
        <div className="fixed inset-0 z-[9999] bg-black/50 flex items-center justify-center backdrop-blur-sm">
          <div className="card p-8 max-w-md text-center shadow-xl">
            <h2 className="text-lg font-semibold text-slate-800 dark:text-slate-100 mb-2">Session Expired</h2>
            <p className="text-sm text-slate-600 dark:text-slate-300 mb-4">
              Your authentication session has expired. Reload to re-authenticate.
            </p>
            <button onClick={() => window.location.reload()} className="btn-primary">Reload</button>
          </div>
        </div>
      )}
    </div>
  )
}
