import React, { useState, useEffect, useRef } from 'react'
import AgentChat from './components/AgentChat'
import BatchJobs from './components/BatchJobs'
import MetadataReview from './components/MetadataReview'
import Coverage from './components/Coverage'
import EntityBrowser from './components/EntityBrowser'
import SemanticLayer from './components/SemanticLayer'
import GenieBuilder from './components/GenieBuilder'
import AnalystChat from './components/AnalystChat'
const COMPONENTS = {
  agent: AgentChat, coverage: Coverage, jobs: BatchJobs, metadata: MetadataReview,
  entities: EntityBrowser, semantic: SemanticLayer, genie: GenieBuilder,
  analyst: AnalystChat,
}

const NAV_CAT_COLORS = {
  Design: 'text-dbx-lava',
  Review: 'text-dbx-sky dark:text-dbx-sky',
  Explore: 'text-dbx-violet dark:text-dbx-violet',
}

const TAB_ACCENT = {
  jobs: 'border-t-dbx-lava', semantic: 'border-t-dbx-lava', genie: 'border-t-dbx-lava',
  metadata: 'border-t-dbx-sky', coverage: 'border-t-dbx-sky', entities: 'border-t-dbx-sky',
  agent: 'border-t-dbx-teal',
}

const NAV_STRUCTURE = [
  {
    category: 'Design',
    items: [
      { id: 'jobs',     label: 'Generate Semantic Layer', desc: 'Core metadata, advanced analytics (ontology, FKs, graph), metric views' },
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
      'Design -- Generate Semantic Layer (core jobs, advanced analytics, metric assets) and Define Metrics.',
      'Review & Apply -- Edit and apply descriptions, tags, entity types, and foreign keys. Check coverage and ontology health.',
      'Explore -- Chat with the metadata agent, graph explorer, and semantic search.',
    ],
  },
  {
    title: 'Getting Started',
    body: [
      '1. Design > Generate Semantic Layer -- run core and advanced jobs on your target tables',
      '2. Review > Review & Apply -- edit and apply results',
      '3. Review > Coverage -- check completeness and health',
      '4. Design > Define Metrics',
      '5. Design > Build Genie Space',
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

export default function App() {
  const [activeTab, setActiveTab] = useState('jobs')
  const [visitedTabs, setVisitedTabs] = useState(new Set(['jobs']))
  const [showInfo, setShowInfo] = useState(false)
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
  }, [activeTab])

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('dbxmetagen-dark', String(dark))
  }, [dark])

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
              <p className="text-dbx-oat/60 text-xs mt-0.5">Metadata generation, knowledge graph, and semantic layer</p>
            </div>
          </div>
          <div className="flex items-center gap-1.5">
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
        {Object.entries(COMPONENTS).map(([tabId, Comp]) => {
          if (!visitedTabs.has(tabId)) return null
          const isActive = tabId === activeTab
          return (
            <div key={tabId}
              className={`border-t-2 ${TAB_ACCENT[tabId] || 'border-t-transparent'} pt-4 ${isActive ? 'animate-slide-up' : ''}`}
              style={{ display: isActive ? 'block' : 'none' }}>
              <Comp onNavigate={setActiveTab} />
            </div>
          )
        })}
      </main>
    </div>
  )
}
