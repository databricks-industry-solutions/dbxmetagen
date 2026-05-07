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

import AgentChat from './components/AgentChat'
import BatchJobs from './components/BatchJobs'
import MetadataReview from './components/MetadataReview'
import Coverage from './components/Coverage'
import EntityBrowser from './components/EntityBrowser'
import SemanticLayer from './components/SemanticLayer'
import GenieBuilder from './components/GenieBuilder'
import AnalystChat from './components/AnalystChat'
import CustomerContext from './components/CustomerContext'
const COMPONENTS = {
  agent: AgentChat, coverage: Coverage, jobs: BatchJobs, metadata: MetadataReview,
  entities: EntityBrowser, semantic: SemanticLayer, genie: GenieBuilder,
  analyst: AnalystChat, context: CustomerContext,
}

const NAV_CAT_COLORS = {
  Design: 'text-dbx-lava',
  Review: 'text-dbx-sky dark:text-dbx-sky',
  Explore: 'text-dbx-violet dark:text-dbx-violet',
}

const TAB_ACCENT = {
  jobs: 'border-t-dbx-lava', semantic: 'border-t-dbx-lava', genie: 'border-t-dbx-lava', context: 'border-t-dbx-lava',
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

const SLIDES = [
  {
    title: 'What Is a Semantic Layer?',
    body: [
      'A shared vocabulary that sits between raw data and the people who use it -- business-friendly names, descriptions, relationships, and pre-defined metrics.',
      'Anyone can find, trust, and use data without writing complex SQL.',
    ],
  },
  {
    title: 'The Databricks Semantic Layer',
    body: [
      'Unity Catalog provides the building blocks:',
      'Comments -- descriptions on every table and column\nTags -- classify columns (PII, domain, sensitivity)\nForeign keys -- declared relationships for auto-joins\nMetric views -- reusable KPI definitions\nVector Search index -- semantic retrieval for agents and RAG\nLakebase -- graph store for GraphRAG and entity traversal\nUC tables -- persisted semantic layer (ontology, profiling, FK predictions)',
    ],
  },
  {
    title: 'What dbxmetagen Does',
    body: [
      'Point it at a catalog and schema. It will:',
      '1. Generate descriptions for tables and columns\n2. Classify sensitive data (PII, PHI, domains)\n3. Predict foreign key relationships\n4. Map tables to business entity types\n5. Define metric views from questions\n6. Build a hierarchical vector index\n7. Build a graph implementation for GraphRAG and entity traversal\n8. Build Genie Spaces and agents on top',
    ],
  },
  {
    title: 'Workflow',
    body: [
      'Design -- Generate Metadata (core jobs, advanced analytics, metric assets) and Define Metrics.',
      'Review & Apply -- Edit and apply descriptions, tags, entity types, and foreign keys. Check coverage and ontology health.',
      'Explore -- Chat with the metadata agent, graph explorer, and semantic search.',
    ],
  },
  {
    title: 'Getting Started',
    body: [
      'Tip: start with a small set of tables to see results quickly, then expand.',
      '1. Design > Generate Metadata -- run core and advanced jobs on your target tables\n2. Review > Review & Apply -- inspect, edit, and apply the generated metadata\n3. Review > Coverage -- check completeness and health across your catalog\n4. Design > Define Metrics -- create reusable KPI definitions\n5. Design > Build Genie Space -- enable natural-language SQL for end users',
    ],
  },
]

function InfoSlides({ open, onClose }) {
  const [slide, setSlide] = useState(0)
  const ref = useRef(null)
  useEffect(() => { if (open) setSlide(0) }, [open])
  useEffect(() => {
    if (!open) return
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) onClose() }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open, onClose])
  if (!open) return null
  const s = SLIDES[slide]
  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-[100] flex items-center justify-center p-6 animate-fade-in">
      <div ref={ref} className="bg-white dark:bg-dbx-navy-600 rounded-2xl shadow-elevated max-w-xl w-full overflow-hidden animate-slide-up">
        <div className="flex items-center justify-between px-8 pt-6 pb-3">
          <span className="section-title">{slide + 1} / {SLIDES.length}</span>
          <button onClick={onClose} className="text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 transition-colors p-1 rounded-lg hover:bg-slate-100 dark:hover:bg-dbx-navy-500" aria-label="Close">
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
          </button>
        </div>
        <div className="px-8 pb-3">
          <h2 className="text-xl font-bold text-slate-800 dark:text-slate-100">{s.title}</h2>
        </div>
        <div className="px-8 pb-8 space-y-4 max-h-[55vh] overflow-y-auto scrollbar-thin">
          {s.body.map((p, i) => (
            <p key={i} className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed whitespace-pre-line">{p}</p>
          ))}
        </div>
        <div className="flex items-center justify-between px-8 pb-6">
          <button onClick={() => setSlide(Math.max(0, slide - 1))} disabled={slide === 0}
            className="btn-ghost btn-sm disabled:opacity-30">
            Previous
          </button>
          <div className="flex gap-2">
            {SLIDES.map((_, i) => (
              <button key={i} onClick={() => setSlide(i)}
                className={`w-2 h-2 rounded-full transition-all duration-200 ${i === slide ? 'bg-dbx-lava scale-125' : 'bg-slate-300 dark:bg-dbx-navy-400 hover:bg-slate-400'}`} />
            ))}
          </div>
          {slide < SLIDES.length - 1 ? (
            <button onClick={() => setSlide(slide + 1)} className="btn-primary btn-sm">Next</button>
          ) : (
            <button onClick={onClose} className="btn-primary btn-sm">Done</button>
          )}
        </div>
      </div>
    </div>
  )
}

function NavDropdown({ cat, activeTab, onSelect }) {
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
    <div ref={ref} className="relative">
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
    <div className="relative">
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
  const [activeTab, setActiveTab] = useState(readHash)
  const [visitedTabs, setVisitedTabs] = useState(new Set([readHash()]))
  const [showInfo, setShowInfo] = useState(false)
  const [sessionExpired, setSessionExpired] = useState(false)
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
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('dbxmetagen-dark', String(dark))
  }, [dark])

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
              <h1 className="text-xl font-bold text-white tracking-tight">dbxmetagen</h1>
              <p className="text-dbx-oat/60 text-xs mt-0.5">Automated metadata, knowledge graph, and semantic layer for Unity Catalog</p>
            </div>
          </div>
          <div className="flex items-center gap-1.5">
            <AuthStatusBadge />
            <button onClick={() => setShowInfo(true)}
              className="w-8 h-8 flex items-center justify-center rounded-lg bg-white/10 backdrop-blur-sm border border-white/10 text-dbx-oat/80 hover:text-white hover:bg-white/20 transition-all text-sm font-bold"
              title="What is dbxmetagen?" aria-label="Show help">?</button>
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

      <InfoSlides open={showInfo} onClose={() => setShowInfo(false)} />

      {/* Navigation */}
      <nav className="bg-white/80 dark:bg-dbx-navy-700/90 backdrop-blur-sm border-b border-dbx-oat-dark/50 dark:border-dbx-navy-400/20 shadow-nav sticky top-0 z-40">
        <div className="flex items-center gap-2 px-6 py-2 max-w-[90rem] mx-auto">
          {NAV_STRUCTURE.map((cat, ci) => (
            <React.Fragment key={cat.category}>
              {ci > 0 && <div className="w-px h-6 bg-slate-200 dark:bg-dbx-navy-400/40 mx-0.5 flex-shrink-0" />}
              <NavDropdown cat={cat} activeTab={activeTab} onSelect={setActiveTab} />
            </React.Fragment>
          ))}
        </div>
      </nav>

      <main className="p-6 max-w-[90rem] mx-auto">
        <details className="mb-4 group">
          <summary className="inline-flex items-center gap-1.5 text-sm font-medium text-slate-500 dark:text-slate-400 cursor-pointer select-none hover:text-dbx-lava dark:hover:text-dbx-lava transition-colors">
            <svg className="w-3.5 h-3.5 transition-transform group-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
            Not sure where to start?
          </summary>
          <ol className="mt-3 ml-1 space-y-2 text-sm text-slate-600 dark:text-slate-300 list-decimal list-inside bg-white/60 dark:bg-dbx-navy-600/50 rounded-xl p-5 border border-slate-200/80 dark:border-dbx-navy-400/20 animate-slide-up">
            <li>Choose an <strong>ontology</strong> or <strong>domain bundle</strong> on the Generate Metadata page.</li>
            <li>Run <strong>Generate Core Metadata</strong> on a few tables. Optionally check "Apply to tables" to write descriptions and tags directly.</li>
            <li>Run <strong>Generate Advanced Metadata</strong> -- incremental mode will process only the tables you just generated.</li>
            <li>Head to <strong>Review</strong> to inspect results, then either update the knowledge base tables or apply metadata to your tables.</li>
            <li>Go to <strong>Define Metrics</strong> and generate metric views for your key tables.</li>
            <li>That's it -- you have a semantic layer! Keep adding tables and it grows from here.</li>
          </ol>
        </details>
        {Object.entries(COMPONENTS).map(([tabId, Comp]) => {
          if (!visitedTabs.has(tabId)) return null
          const isActive = tabId === activeTab
          return (
            <div key={tabId}
              className={`border-t-2 ${TAB_ACCENT[tabId] || 'border-t-transparent'} pt-4 ${isActive ? 'animate-slide-up' : ''}`}
              style={{ display: isActive ? 'block' : 'none' }}>
              <TabErrorBoundary>
                <Comp onNavigate={setActiveTab} />
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
