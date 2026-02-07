import React, { useState } from 'react'
import BatchJobs from './components/BatchJobs'
import MetadataReview from './components/MetadataReview'
import DataProfiling from './components/DataProfiling'
import Ontology from './components/Ontology'
import Analytics from './components/Analytics'

const TABS = [
  { id: 'jobs', label: 'Batch Jobs', component: BatchJobs },
  { id: 'metadata', label: 'Metadata Review', component: MetadataReview },
  { id: 'profiling', label: 'Data Profiling', component: DataProfiling },
  { id: 'ontology', label: 'Ontology', component: Ontology },
  { id: 'analytics', label: 'Analytics & Graph', component: Analytics },
]

export default function App() {
  const [activeTab, setActiveTab] = useState('jobs')
  const ActiveComponent = TABS.find(t => t.id === activeTab)?.component

  return (
    <div className="min-h-screen">
      <header className="bg-white border-b border-gray-200 px-6 py-4">
        <h1 className="text-xl font-semibold text-gray-900">dbxmetagen</h1>
        <p className="text-sm text-gray-500 mt-1">Metadata generation, profiling, and knowledge graph analytics</p>
      </header>

      <nav className="bg-white border-b border-gray-200 px-6">
        <div className="flex space-x-6">
          {TABS.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`py-3 text-sm font-medium border-b-2 transition-colors ${
                activeTab === tab.id
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
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
