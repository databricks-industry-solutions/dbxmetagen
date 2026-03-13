import React, { useState, useEffect, useRef } from 'react'
import AgentChat from './components/AgentChat'
import BatchJobs from './components/BatchJobs'
import MetadataReview from './components/MetadataReview'
import Coverage from './components/Coverage'
import SemanticLayer from './components/SemanticLayer'
import GenieBuilder from './components/GenieBuilder'
import VectorSearch from './components/VectorSearch'

const COMPONENTS = {
  agent: AgentChat, coverage: Coverage, jobs: BatchJobs, metadata: MetadataReview,
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
      { id: 'metadata', label: 'Review & Apply', desc: 'Review, edit, and apply AI-generated metadata' },
      { id: 'coverage', label: 'Coverage',       desc: 'Catalog health, ontology overview, graph explorers' },
    ],
  },
  {
    category: 'Explore',
    description: 'Discover patterns and ask questions',
    defaultTab: 'agent',
    items: [
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
      'A shared vocabulary that sits between raw data and the people who use it -- business-friendly names, descriptions, relationships, and pre-defined metrics.',
      'Anyone can find, trust, and use data without writing complex SQL.',
    ],
  },
  {
    title: 'The Databricks Semantic Layer',
    body: [
      'Unity Catalog provides the building blocks:',
      'Comments -- descriptions on every table and column\nTags -- classify columns (PII, domain, sensitivity)\nForeign keys -- declared relationships for auto-joins\nMetric views -- reusable KPI definitions\nGenie Spaces -- natural-language SQL for business users',
    ],
  },
  {
    title: 'What dbxmetagen Does',
    body: [
      'Point it at a catalog and schema. It will:',
      '1. Generate descriptions for tables and columns\n2. Classify sensitive data (PII, PHI, domains)\n3. Predict foreign key relationships\n4. Map tables to business entity types\n5. Define metric views from questions\n6. Assemble Genie Spaces',
    ],
  },
  {
    title: 'Workflow',
    body: [
      'Design -- Generate metadata, define metrics, build Genie Spaces.',
      'Review & Apply -- Edit and apply descriptions, tags, entity types, and foreign keys. Check coverage and ontology health.',
      'Explore -- Chat with the metadata agent or run semantic searches.',
    ],
  },
  {
    title: 'Getting Started',
    body: [
      '1. Design > Generate Metadata -- run on your target tables',
      '2. Review > Review & Apply -- edit and apply results',
      '3. Review > Coverage -- check completeness and health',
      '4. Design > Define Metrics / Build Genie Space',
      '5. Explore > Agent or Search',
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
    <div className="fixed inset-0 bg-black/40 z-[100] flex items-center justify-center p-6">
      <div ref={ref} className="bg-white dark:bg-slate-800 rounded-2xl shadow-2xl max-w-xl w-full overflow-hidden">
        <div className="flex items-center justify-between px-8 pt-6 pb-3">
          <span className="text-xs font-medium text-stone-400 dark:text-stone-500">{slide + 1} / {SLIDES.length}</span>
          <button onClick={onClose} className="text-stone-400 hover:text-stone-600 dark:hover:text-stone-300 transition-colors">
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
          </button>
        </div>
        <div className="px-8 pb-3">
          <h2 className="text-lg font-bold text-stone-800 dark:text-stone-100">{s.title}</h2>
        </div>
        <div className="px-8 pb-8 space-y-4 max-h-[55vh] overflow-y-auto">
          {s.body.map((p, i) => (
            <p key={i} className="text-sm text-stone-600 dark:text-stone-300 leading-relaxed whitespace-pre-line">{p}</p>
          ))}
        </div>
        <div className="flex items-center justify-between px-8 pb-6">
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
