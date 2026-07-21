import React from 'react'

/**
 * Two-door landing. The app's primary entry point: the user picks one of two
 * outcomes rather than being dropped into a config form. Secondary destinations
 * (Review, Coverage, Explore) stay one click away for returning/power users.
 *
 * This does not change any underlying flow — each door routes into the existing
 * tabs. Door 2 (semantic layer) runs the fixed sequence core metadata → full
 * analytics pipeline → metric views; that ordering is enforced inside its flow.
 */

function DoorCard({ accent, eyebrow, title, description, bullets, cta, onClick }) {
  return (
    <button
      onClick={onClick}
      className="card p-6 flex flex-col h-full text-left group hover:shadow-elevated transition-all duration-200 border-t-2 border-t-transparent hover:-translate-y-0.5"
      style={{ borderTopColor: accent }}
    >
      <span className="text-[11px] font-semibold uppercase tracking-wider" style={{ color: accent }}>{eyebrow}</span>
      <h3 className="text-lg font-bold text-slate-800 dark:text-white mt-1.5 mb-2">{title}</h3>
      <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed">{description}</p>
      <ul className="mt-3 space-y-1.5 flex-1">
        {bullets.map((b, i) => (
          <li key={i} className="flex items-start gap-2 text-sm text-slate-600 dark:text-slate-300">
            <svg className="w-4 h-4 mt-0.5 shrink-0" style={{ color: accent }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
            <span>{b}</span>
          </li>
        ))}
      </ul>
      <span
        className="mt-5 inline-flex items-center gap-1.5 self-start px-4 py-2 rounded-lg text-sm font-semibold text-white transition-transform group-hover:gap-2.5"
        style={{ backgroundColor: accent }}
      >
        {cta}
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
        </svg>
      </span>
    </button>
  )
}

const SECONDARY = [
  { id: 'metadata', label: 'Review & apply', desc: 'Edit and apply generated metadata' },
  { id: 'agent', label: 'Explore / Agent', desc: 'Ask questions, browse the graph' },
  { id: 'genie', label: 'Build Genie space', desc: 'Natural-language SQL over your data' },
]

export default function Home({ onNavigate }) {
  return (
    <div className="max-w-4xl mx-auto py-6">
      <div className="text-center mb-8">
        <h2 className="text-2xl font-bold text-slate-800 dark:text-white">What do you want to do?</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 mt-1.5">
          Pick a starting point. Once you're in, everything else is a click away.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
        <DoorCard
          accent="#FF3621"
          eyebrow="Start here"
          title="Generate basic metadata"
          description="AI-generated descriptions, sensitivity classification, and business domain tags — reviewed, then applied to your Unity Catalog tables."
          bullets={[
            'Table & column descriptions',
            'PII / PHI / PCI sensitivity labels',
            'Business domain tags',
            'Review before anything touches your catalog',
          ]}
          cta="Generate metadata"
          onClick={() => onNavigate('jobs')}
        />
        <DoorCard
          accent="#1B7EAB"
          eyebrow="Full semantic layer"
          title="Generate semantic layer"
          description="Build metric views, a vector index, and a knowledge graph for AI/BI and Genie. Generates and reviews the underlying metadata for you first."
          bullets={[
            'Metric views (KPIs your tools can query)',
            'Vector index for semantic search',
            'Knowledge graph of how tables relate',
            'Metadata generated as the foundation',
          ]}
          cta="Build semantic layer"
          onClick={() => onNavigate('semantic')}
        />
      </div>

      <div className="mt-8 border-t border-slate-200 dark:border-dbx-navy-400/30 pt-5">
        <p className="text-xs font-semibold uppercase tracking-wider text-slate-400 dark:text-slate-500 mb-3">
          Already have metadata?
        </p>
        <div className="flex flex-wrap gap-2.5">
          {SECONDARY.map(s => (
            <button
              key={s.id}
              onClick={() => onNavigate(s.id)}
              className="flex items-center gap-2 px-3.5 py-2 rounded-lg border border-slate-200 dark:border-dbx-navy-400/30 text-sm text-slate-600 dark:text-slate-300 hover:border-dbx-lava/40 hover:text-dbx-lava dark:hover:text-dbx-lava transition-colors"
              title={s.desc}
            >
              <span className="font-medium">{s.label}</span>
              <span className="text-xs text-slate-400 dark:text-slate-500 hidden sm:inline">· {s.desc}</span>
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}
