import React, { useState, useEffect, useRef } from 'react'
import AgentChat from './components/AgentChat'
import BatchJobs from './components/BatchJobs'
import MetadataReview from './components/MetadataReview'
import Ontology from './components/Ontology'
import Analytics from './components/Analytics'
import Coverage from './components/Coverage'
import SemanticLayer from './components/SemanticLayer'
import GenieBuilder from './components/GenieBuilder'
import ForeignKeyGeneration from './components/ForeignKeyGeneration'
import VectorSearch from './components/VectorSearch'

const COMPONENTS = {
  agent: AgentChat, coverage: Coverage, jobs: BatchJobs, metadata: MetadataReview,
  ontology: Ontology, analytics: Analytics, fk: ForeignKeyGeneration,
  semantic: SemanticLayer, genie: GenieBuilder, vector: VectorSearch,
}

const NAV_STRUCTURE = [
  {
    category: 'Design',
    description: 'Build the semantic layer',
    defaultTab: 'jobs',
    items: [
      { id: 'jobs',     label: 'Generate Metadata', desc: 'Run metadata generation and analysis jobs' },
      { id: 'semantic', label: 'Define Metrics',     desc: 'Define metric views and KPIs' },
      { id: 'genie',    label: 'Build Genie Space',  desc: 'Build natural-language SQL spaces' },
    ],
  },
  {
    category: 'Review',
    description: 'Validate and correct what was built',
    defaultTab: 'metadata',
    items: [
      { id: 'metadata', label: 'Descriptions',  desc: 'Review and correct AI-generated descriptions' },
      { id: 'coverage', label: 'Coverage',       desc: 'Catalog health, profiling status, what\'s missing' },
      { id: 'ontology', label: 'Ontology',       desc: 'Entity types, confidence scores, validation' },
      { id: 'fk',       label: 'Foreign Keys',   desc: 'Review FK predictions, visualize map, apply DDL' },
    ],
  },
  {
    category: 'Explore',
    description: 'Discover patterns and ask questions',
    defaultTab: 'analytics',
    items: [
      { id: 'analytics', label: 'Knowledge Graph', desc: 'Graph explorer, clusters, and similarity' },
      { id: 'agent',     label: 'Agent',            desc: 'Chat with the metadata agent' },
      { id: 'vector',    label: 'Search',           desc: 'Semantic search over metadata' },
    ],
  },
]

const TAB_TO_CATEGORY = {}
const TAB_LABEL = {}
NAV_STRUCTURE.forEach(cat => {
  cat.items.forEach(item => {
    TAB_TO_CATEGORY[item.id] = cat.category
    TAB_LABEL[item.id] = item.label
  })
})

export async function safeFetch(url, options) {
  try {
    const res = await fetch(url, options)
    if (!res.ok) {
      const body = await res.text().catch(() => '')
      let msg = `Error ${res.status}`
      try { const j = JSON.parse(body); if (j.detail) msg = j.detail } catch {}
      return { data: [], error: msg }
    }
    const data = await res.json()
    return { data: Array.isArray(data) ? data : [], error: null }
  } catch (e) { return { data: [], error: e.message } }
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

export function ErrorBanner({ error }) {
  if (!error) return null
  return (
    <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 text-red-700 dark:text-red-300 rounded-lg px-4 py-3 text-sm">
      <span className="font-medium">API Error:</span> {error}
    </div>
  )
}

const SLIDES = [
  {
    title: 'What Is a Semantic Layer?',
    body: [
      'A semantic layer sits between raw data and the people who need it. Instead of requiring everyone to know which database table holds what, or how to join ten tables together, the semantic layer provides a shared vocabulary -- business-friendly names, descriptions, relationships, and pre-defined metrics -- so that anyone can find, trust, and use data without writing complex SQL.',
      'Business users: find data using terms you already know. Data engineers: define once, reuse everywhere. Governance teams: one place to document, classify, and control access to every asset.',
    ],
  },
  {
    title: 'Why Does It Matter?',
    body: [
      'Without a semantic layer, organizations hit predictable problems: different teams calculate "revenue" differently, analysts spend hours hunting for the right table, and nobody is sure which columns contain sensitive data.',
      'A semantic layer solves this by providing a single source of truth for definitions, relationships, and quality -- turning a data warehouse from a maze into a well-labeled library.',
    ],
  },
  {
    title: 'The Databricks Semantic Layer',
    body: [
      'Databricks provides the building blocks for a modern semantic layer through Unity Catalog:',
      'Table and column comments -- descriptions that follow every asset across notebooks, SQL, and dashboards. Tags -- classify columns as PII, geographic, domain-specific, or anything else. Foreign key constraints -- declared relationships that tools and query engines can use to auto-join tables. Metric views -- reusable KPI definitions that any tool can query consistently. Genie Spaces -- natural-language SQL interfaces where business users ask questions in plain English.',
    ],
  },
  {
    title: 'How dbxmetagen Builds Your Semantic Layer',
    body: [
      'dbxmetagen uses AI to automate the most time-consuming parts of building a semantic layer. Point it at a catalog and schema, and it will:',
      '1. Generate descriptions -- AI-written comments for every table and column, reviewed before applying.\n2. Classify data -- detect PII, PHI, geographic columns, and domain assignments.\n3. Predict foreign keys -- discover relationships using embedding similarity, rules, and AI validation.\n4. Build an ontology -- map tables and columns to business entity types with confidence scores.\n5. Define metrics -- generate Unity Catalog metric views from natural-language questions.\n6. Create Genie Spaces -- assemble everything into a Databricks Genie room for business users.',
    ],
  },
  {
    title: 'The Workflow: Design, Review, Explore',
    body: [
      'Design -- Run metadata generation jobs, define metric views, and build Genie Spaces. This is where you shape the semantic layer.',
      'Review -- Check and correct what was built: validate AI-generated descriptions, inspect catalog coverage, review ontology entity types, and approve foreign key predictions before applying them. This is where humans stay in the loop.',
      'Explore -- Discover patterns in the knowledge graph, chat with the metadata agent, or run semantic searches. This is where everyone -- engineers, analysts, business users -- gets answers.',
    ],
  },
  {
    title: 'Getting Started',
    body: [
      '1. Go to Design > Generate Metadata and run metadata generation on your target tables.',
      '2. Review results in Review > Descriptions -- edit any descriptions before applying.',
      '3. Check catalog completeness in Review > Coverage and validate entity types in Review > Ontology.',
      '4. Review predicted foreign keys in Review > Foreign Keys -- approve and apply DDL.',
      '5. Define metric views in Design > Define Metrics and create a Genie Space in Design > Build Genie Space.',
      '6. Explore the knowledge graph in Explore > Knowledge Graph, or ask questions via Explore > Agent.',
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
    <div className="fixed inset-0 bg-black/40 z-[100] flex items-center justify-center p-4">
      <div ref={ref} className="bg-white dark:bg-slate-800 rounded-2xl shadow-2xl max-w-lg w-full overflow-hidden">
        <div className="flex items-center justify-between px-6 pt-5 pb-2">
          <span className="text-xs font-medium text-stone-400 dark:text-stone-500">{slide + 1} / {SLIDES.length}</span>
          <button onClick={onClose} className="text-stone-400 hover:text-stone-600 dark:hover:text-stone-300 transition-colors">
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
          </button>
        </div>
        <div className="px-6 pb-2">
          <h2 className="text-lg font-bold text-stone-800 dark:text-stone-100">{s.title}</h2>
        </div>
        <div className="px-6 pb-6 space-y-3 max-h-[50vh] overflow-y-auto">
          {s.body.map((p, i) => (
            <p key={i} className="text-sm text-stone-600 dark:text-stone-300 leading-relaxed whitespace-pre-line">{p}</p>
          ))}
        </div>
        <div className="flex items-center justify-between px-6 pb-5">
          <button onClick={() => setSlide(Math.max(0, slide - 1))} disabled={slide === 0}
            className="px-3 py-1.5 text-sm font-medium rounded-lg transition-colors disabled:opacity-30 disabled:cursor-default text-stone-600 dark:text-stone-300 hover:bg-stone-100 dark:hover:bg-slate-700">
            Previous
          </button>
          <div className="flex gap-1.5">
            {SLIDES.map((_, i) => (
              <button key={i} onClick={() => setSlide(i)}
                className={`w-2 h-2 rounded-full transition-colors ${i === slide ? 'bg-dbx-lava' : 'bg-stone-300 dark:bg-slate-600'}`} />
            ))}
          </div>
          {slide < SLIDES.length - 1 ? (
            <button onClick={() => setSlide(slide + 1)}
              className="px-3 py-1.5 text-sm font-medium rounded-lg bg-dbx-lava text-white hover:bg-dbx-lava/90 transition-colors">
              Next
            </button>
          ) : (
            <button onClick={onClose}
              className="px-3 py-1.5 text-sm font-medium rounded-lg bg-dbx-lava text-white hover:bg-dbx-lava/90 transition-colors">
              Done
            </button>
          )}
        </div>
      </div>
    </div>
  )
}

function CategoryDropdown({ cat, activeTab, setActiveTab, openCategory, setOpenCategory }) {
  const ref = useRef(null)
  const isOpen = openCategory === cat.category
  const activeItem = cat.items.find(i => i.id === activeTab)
  const isActive = !!activeItem

  useEffect(() => {
    if (!isOpen) return
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpenCategory(null) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [isOpen, setOpenCategory])

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpenCategory(isOpen ? null : cat.category)}
        className={`flex items-center gap-1.5 px-4 py-3 text-sm font-semibold border-b-2 transition-colors whitespace-nowrap ${
          isActive
            ? 'border-dbx-lava text-dbx-lava dark:text-dbx-lava'
            : 'border-transparent text-stone-600 dark:text-stone-400 hover:text-dbx-navy dark:hover:text-dbx-oat'
        }`}
      >
        <span>{cat.category}</span>
        {isActive && <span className="font-normal text-stone-500 dark:text-stone-400">/ {activeItem.label}</span>}
        <svg className={`w-3.5 h-3.5 transition-transform ${isOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {isOpen && (
        <div className="absolute top-full left-0 mt-1 w-72 bg-white dark:bg-slate-800 border border-stone-200 dark:border-slate-600 rounded-lg shadow-lg z-50 py-1">
          {cat.items.map(item => (
            <button
              key={item.id}
              onClick={() => { setActiveTab(item.id); setOpenCategory(null) }}
              className={`w-full text-left px-4 py-2.5 transition-colors ${
                activeTab === item.id
                  ? 'bg-dbx-lava/10 dark:bg-dbx-lava/20'
                  : 'hover:bg-stone-100 dark:hover:bg-slate-700'
              }`}
            >
              <div className={`text-sm font-medium ${activeTab === item.id ? 'text-dbx-lava' : 'text-stone-800 dark:text-stone-200'}`}>
                {item.label}
              </div>
              <div className="text-xs text-stone-500 dark:text-stone-400 mt-0.5">{item.desc}</div>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

export default function App() {
  const [activeTab, setActiveTab] = useState('agent')
  const [openCategory, setOpenCategory] = useState(null)
  const [showInfo, setShowInfo] = useState(false)
  const [dark, setDark] = useState(() => {
    if (typeof window !== 'undefined') {
      const saved = localStorage.getItem('dbxmetagen-dark')
      if (saved !== null) return saved === 'true'
      return window.matchMedia('(prefers-color-scheme: dark)').matches
    }
    return false
  })
  const ActiveComponent = COMPONENTS[activeTab]
  const activeDesc = NAV_STRUCTURE.flatMap(c => c.items).find(i => i.id === activeTab)?.desc

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('dbxmetagen-dark', String(dark))
  }, [dark])

  return (
    <div className="min-h-screen bg-dbx-oat-light dark:bg-dbx-navy transition-colors">
      <header className="bg-dbx-navy px-6 py-4 shadow-md flex items-center justify-between">
        <div className="flex items-center gap-3">
          <img src="/logo.png" alt="dbxmetagen" className="h-9 w-9" />
          <div>
            <h1 className="text-xl font-bold text-white tracking-tight">dbxmetagen</h1>
            <p className="text-dbx-oat text-sm mt-0.5">Metadata generation, knowledge graph, and semantic layer</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
        <button
          onClick={() => setShowInfo(true)}
          className="w-8 h-8 flex items-center justify-center rounded-full border border-dbx-oat/40 text-dbx-oat hover:text-white hover:bg-white/10 transition-colors text-sm font-bold"
          title="What is dbxmetagen?"
        >?</button>
        <button
          onClick={() => setDark(d => !d)}
          className="p-2 rounded-lg text-dbx-oat hover:text-white hover:bg-white/10 transition-colors"
          title={dark ? 'Switch to light mode' : 'Switch to dark mode'}
        >
          {dark ? (
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M12 3v1m0 16v1m8.66-13.66l-.71.71M4.05 19.95l-.71.71M21 12h-1M4 12H3m16.95 7.95l-.71-.71M4.05 4.05l-.71-.71M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
            </svg>
          ) : (
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M20.354 15.354A9 9 0 018.646 3.646 9.005 9.005 0 0012 21a9.005 9.005 0 008.354-5.646z" />
            </svg>
          )}
        </button>
        </div>
      </header>
      <InfoSlides open={showInfo} onClose={() => setShowInfo(false)} />

      <nav className="bg-dbx-oat dark:bg-[#152E35] border-b border-stone-300 dark:border-slate-700 px-6 shadow-sm">
        <div className="flex space-x-2">
          {NAV_STRUCTURE.map(cat => (
            <CategoryDropdown
              key={cat.category}
              cat={cat}
              activeTab={activeTab}
              setActiveTab={setActiveTab}
              openCategory={openCategory}
              setOpenCategory={setOpenCategory}
            />
          ))}
        </div>
      </nav>

      {activeDesc && (
        <div className="bg-dbx-oat-light/80 dark:bg-dbx-navy/50 border-b border-stone-200 dark:border-slate-700 px-6 py-2">
          <p className="text-xs text-stone-500 dark:text-stone-400 max-w-7xl mx-auto">{activeDesc}</p>
        </div>
      )}

      <main className="p-6 max-w-7xl mx-auto">
        {ActiveComponent && <ActiveComponent />}
      </main>
    </div>
  )
}
