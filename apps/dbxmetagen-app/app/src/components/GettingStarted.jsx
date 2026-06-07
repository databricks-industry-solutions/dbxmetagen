import React from 'react'
import { PageHeader } from './ui'

const ArrowDown = () => (
  <div className="flex justify-center py-2">
    <svg className="w-5 h-5 text-slate-300 dark:text-slate-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 14l-7 7m0 0l-7-7m7 7V3" />
    </svg>
  </div>
)

function FoundationCard({ step, title, what, why, tip, onClick, linkLabel }) {
  return (
    <div className="card p-6 flex flex-col h-full">
      <div className="flex items-center gap-3 mb-3">
        <span className="flex items-center justify-center w-7 h-7 rounded-full bg-dbx-lava text-white text-xs font-bold shrink-0">{step}</span>
        <h3 className="text-base font-bold text-slate-800 dark:text-white">{title}</h3>
      </div>
      <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed mb-2">
        <span className="font-semibold text-slate-700 dark:text-slate-200">What it does: </span>{what}
      </p>
      <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed mb-3">
        <span className="font-semibold text-slate-700 dark:text-slate-200">Why it matters: </span>{why}
      </p>
      {tip && (
        <p className="text-xs text-slate-500 dark:text-slate-400 italic mb-4">{tip}</p>
      )}
      <div className="mt-auto pt-2">
        <button onClick={onClick} className="btn-primary btn-sm">{linkLabel}</button>
      </div>
    </div>
  )
}

function OutcomeCard({ title, description, onClick, linkLabel }) {
  return (
    <div className="card p-5 flex flex-col h-full">
      <h4 className="text-sm font-bold text-slate-800 dark:text-white mb-2">{title}</h4>
      <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed flex-1">{description}</p>
      <div className="mt-3">
        <button onClick={onClick} className="text-xs font-semibold text-dbx-lava hover:underline">{linkLabel}</button>
      </div>
    </div>
  )
}

export default function GettingStarted({ onNavigate, onStartTour }) {
  return (
    <div className="space-y-6 max-w-4xl mx-auto">
      <PageHeader
        title="Getting Started"
        subtitle="A guide to building your semantic layer with dbxmetagen"
        actions={
          onStartTour && (
            <button onClick={onStartTour} className="btn-ghost btn-sm flex items-center gap-1.5">
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
              </svg>
              Take the Interactive Tour
            </button>
          )
        }
      />

      {/* Section 1: The Problem */}
      <div className="card p-6 border-l-4 border-l-dbx-lava bg-gradient-to-br from-white to-slate-50/80 dark:from-dbx-navy-650 dark:to-dbx-navy-700">
        <p className="text-sm text-slate-700 dark:text-slate-200 leading-relaxed">
          You have hundreds of tables in Unity Catalog, but can your analysts find the ones with patient data?
          Do they know which columns contain PII? How <code className="text-xs bg-slate-100 dark:bg-dbx-navy-500 px-1 py-0.5 rounded">dim_patient</code> connects
          to <code className="text-xs bg-slate-100 dark:bg-dbx-navy-500 px-1 py-0.5 rounded">fact_encounter</code>?
          Without context, every person who touches your data writes their own interpretation — and gets a different answer.
        </p>
        <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed mt-3">
          dbxmetagen generates that context automatically: plain-English descriptions, sensitivity labels, business domain tags,
          and a map of how your tables relate. Then you review it, apply what you trust, and build on top.
        </p>
      </div>

      {/* Section 2: Build the Foundation */}
      <div>
        <h2 className="heading-section mb-3">Build the Foundation</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <FoundationCard
            step={1}
            title="Core Metadata"
            what="Uses AI to write descriptions for every table and column. Classifies sensitive data (PII, PHI, PCI). Tags each table with its business domain."
            why="This is the minimum useful output. After this step, anyone browsing your catalog can understand what a table contains and whether it has sensitive data — without reading the schema."
            tip="Start with 5–10 tables to see results in minutes, then expand."
            onClick={() => onNavigate('jobs')}
            linkLabel="Generate Core Metadata"
          />
          <FoundationCard
            step={2}
            title="Knowledge Graph"
            what="Maps your tables to business concepts (Patient, Account, Transaction) using industry ontologies. Discovers foreign key relationships. Computes column similarity. Builds a searchable index."
            why="The knowledge graph answers structural questions — 'which tables represent patients?', 'how do these 50 tables connect?' — whether you use agents or just browse it directly."
            onClick={() => onNavigate('jobs')}
            linkLabel="Generate Advanced Metadata"
          />
        </div>
      </div>

      <ArrowDown />

      {/* Section 3: What Gets Built */}
      <div className="card p-6">
        <h2 className="heading-section mb-4">What Gets Built</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-emerald-600 dark:text-emerald-400 mb-2">
              Applied to Unity Catalog
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400 mb-3 italic">After you review and approve</p>
            <ul className="space-y-1.5 text-sm text-slate-600 dark:text-slate-300">
              <li className="flex items-start gap-2">
                <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
                Table and column descriptions
              </li>
              <li className="flex items-start gap-2">
                <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
                Sensitivity tags (PII, PHI, PCI)
              </li>
              <li className="flex items-start gap-2">
                <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
                Domain classification tags
              </li>
              <li className="flex items-start gap-2">
                <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
                Foreign key constraints
              </li>
              <li className="flex items-start gap-2">
                <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
                Metric view definitions
              </li>
            </ul>
          </div>
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-dbx-sky dark:text-dbx-sky mb-2">
              Stored in dbxmetagen Tables
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400 mb-3 italic">Powers downstream features</p>
            <ul className="space-y-1.5 text-sm text-slate-600 dark:text-slate-300">
              <li className="flex items-start gap-2">
                <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                Knowledge graph (entities, relationships, similarity)
              </li>
              <li className="flex items-start gap-2">
                <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                Entity-to-table mappings (ontology)
              </li>
              <li className="flex items-start gap-2">
                <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                Column profiles and data quality scores
              </li>
              <li className="flex items-start gap-2">
                <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                Searchable vector index
              </li>
              <li className="flex items-start gap-2">
                <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                Community summaries
              </li>
            </ul>
          </div>
        </div>
        <p className="text-xs text-slate-500 dark:text-slate-400 mt-4 pt-3 border-t border-slate-100 dark:border-dbx-navy-400/20">
          Nothing touches your production catalog until you explicitly apply it. dbxmetagen generates into its own tables first.
        </p>
      </div>

      <ArrowDown />

      {/* Section 4: Review and Apply */}
      <div className="card p-6 border-l-4 border-l-dbx-sky">
        <div className="flex items-center gap-3 mb-3">
          <span className="flex items-center justify-center w-7 h-7 rounded-full bg-dbx-sky text-white text-xs font-bold shrink-0">3</span>
          <h3 className="text-base font-bold text-slate-800 dark:text-white">Review and Apply</h3>
        </div>
        <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed mb-2">
          <span className="font-semibold text-slate-700 dark:text-slate-200">What it does: </span>
          Shows you everything the AI generated. Edit any description, reclassify any column, reject any suggestion.
          When you're satisfied, apply to Unity Catalog. Check coverage stats to see completeness across your catalog.
        </p>
        <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed mb-4">
          <span className="font-semibold text-slate-700 dark:text-slate-200">Why it matters: </span>
          Nothing touches your production catalog without your approval. You review, then decide what gets applied.
        </p>
        <div className="flex gap-2">
          <button onClick={() => onNavigate('metadata')} className="btn-primary btn-sm">Review Metadata</button>
          <button onClick={() => onNavigate('coverage')} className="btn-ghost btn-sm">View Coverage</button>
        </div>
      </div>

      <ArrowDown />

      {/* Section 5: Use the Semantic Layer */}
      <div>
        <h2 className="heading-section mb-1">Use the Semantic Layer</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 mb-4">
          With a reviewed semantic layer in place, you can use it in several ways. All of these are optional — pick what fits your needs.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <OutcomeCard
            title="Explore Your Data"
            description="Browse the knowledge graph to see how tables relate, which business entities they map to, and where sensitive data lives. Navigate by entity ('show me all Patient tables') instead of hunting through schema names."
            onClick={() => onNavigate('entities')}
            linkLabel="Open Entity Browser"
          />
          <OutcomeCard
            title="Ask Questions"
            description="Chat with a metadata-aware agent. Ask 'what tables contain patient demographics?' or 'how does the claims schema connect to encounters?' The agent uses the knowledge graph and vector index to find answers."
            onClick={() => onNavigate('agent')}
            linkLabel="Open Agent"
          />
          <OutcomeCard
            title="Define Metrics"
            description="Create reusable KPI definitions from business questions. These become governed Unity Catalog metric views that any tool can query consistently."
            onClick={() => onNavigate('semantic')}
            linkLabel="Define Metric Views"
          />
          <OutcomeCard
            title="Build a Genie Space"
            description="Assemble a natural-language SQL interface so end users can ask business questions in plain English. Genie uses your descriptions, relationships, and metric views to write accurate SQL."
            onClick={() => onNavigate('genie')}
            linkLabel="Build Genie Space"
          />
        </div>
      </div>
    </div>
  )
}
