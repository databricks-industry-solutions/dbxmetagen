import React, { useState } from 'react'
import BatchJobs from './components/BatchJobs'
import MetadataReview from './components/MetadataReview'
import DataProfiling from './components/DataProfiling'
import Ontology from './components/Ontology'
import Analytics from './components/Analytics'
import Coverage from './components/Coverage'

const TABS = [
  { id: 'analytics', label: 'Graph Explorer' },
  { id: 'coverage', label: 'Coverage' },
  { id: 'jobs', label: 'Batch Jobs' },
  { id: 'metadata', label: 'Metadata Review' },
  { id: 'profiling', label: 'Data Profiling' },
  { id: 'ontology', label: 'Ontology' },
]

const COMPONENTS = { analytics: Analytics, coverage: Coverage, jobs: BatchJobs, metadata: MetadataReview, profiling: DataProfiling, ontology: Ontology }

// Safe fetch: returns { data: [], error: null } on success, { data: [], error: string } on failure
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

// Safe fetch that returns { data: object|null, error: string|null }
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

// Reusable error banner
export function ErrorBanner({ error }) {
  if (!error) return null
  return (
    <div className="bg-red-50 border border-red-200 text-red-700 rounded-lg px-4 py-3 text-sm">
      <span className="font-medium">API Error:</span> {error}
    </div>
  )
}

export default function App() {
  const [activeTab, setActiveTab] = useState('analytics')
  const ActiveComponent = COMPONENTS[activeTab]

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="bg-gradient-to-r from-indigo-700 to-purple-700 px-6 py-5 shadow-md">
        <h1 className="text-xl font-bold text-white tracking-tight">dbxmetagen</h1>
        <p className="text-indigo-200 text-sm mt-0.5">Metadata generation, profiling, and knowledge graph analytics</p>
      </header>

      <nav className="bg-white border-b border-slate-200 px-6 shadow-sm">
        <div className="flex space-x-1">
          {TABS.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                activeTab === tab.id
                  ? 'border-indigo-600 text-indigo-700 bg-indigo-50/50'
                  : 'border-transparent text-slate-500 hover:text-slate-700 hover:bg-slate-50'
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
