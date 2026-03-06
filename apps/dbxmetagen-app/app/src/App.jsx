import React, { useState, useEffect } from 'react'
import BatchJobs from './components/BatchJobs'
import MetadataReview from './components/MetadataReview'
import Ontology from './components/Ontology'
import Analytics from './components/Analytics'
import Coverage from './components/Coverage'
import SemanticLayer from './components/SemanticLayer'
import GenieBuilder from './components/GenieBuilder'

const TABS = [
  { id: 'coverage', label: 'Coverage' },
  { id: 'jobs', label: 'Batch Jobs' },
  { id: 'metadata', label: 'Metadata Review' },
  { id: 'ontology', label: 'Ontology' },
  { id: 'analytics', label: 'Graph Explorer' },
  { id: 'semantic', label: 'Semantic Layer' },
  { id: 'genie', label: 'Genie Builder' },
]

const COMPONENTS = {
  coverage: Coverage, jobs: BatchJobs, metadata: MetadataReview,
  ontology: Ontology, analytics: Analytics, semantic: SemanticLayer,
  genie: GenieBuilder,
}

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

export default function App() {
  const [activeTab, setActiveTab] = useState('coverage')
  const [dark, setDark] = useState(() => {
    if (typeof window !== 'undefined') {
      const saved = localStorage.getItem('dbxmetagen-dark')
      if (saved !== null) return saved === 'true'
      return window.matchMedia('(prefers-color-scheme: dark)').matches
    }
    return false
  })
  const ActiveComponent = COMPONENTS[activeTab]

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('dbxmetagen-dark', String(dark))
  }, [dark])

  return (
    <div className="min-h-screen bg-slate-50 dark:bg-slate-900 transition-colors">
      <header className="bg-gradient-to-r from-indigo-700 to-purple-700 dark:from-indigo-900 dark:to-purple-900 px-6 py-5 shadow-md flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-white tracking-tight">dbxmetagen</h1>
          <p className="text-indigo-200 text-sm mt-0.5">Metadata generation, knowledge graph, and semantic layer</p>
        </div>
        <button
          onClick={() => setDark(d => !d)}
          className="p-2 rounded-lg text-indigo-200 hover:text-white hover:bg-white/10 transition-colors"
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
      </header>

      <nav className="bg-white dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700 px-6 shadow-sm">
        <div className="flex space-x-1 overflow-x-auto">
          {TABS.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
                activeTab === tab.id
                  ? 'border-indigo-600 text-indigo-700 dark:text-indigo-400 bg-indigo-50/50 dark:bg-indigo-900/20'
                  : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-700/50'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </nav>

      <main className="p-6 max-w-7xl mx-auto">
        {ActiveComponent && <ActiveComponent />}
      </main>
    </div>
  )
}
