import React from 'react'
import { useViewMode } from '../hooks/useViewMode'
import { useLanguage } from '../contexts/LanguageContext'

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
  { id: 'metadata', label: 'home.secondary.reviewApply', desc: 'home.secondary.reviewApply.desc' },
  { id: 'agent', label: 'home.secondary.explore', desc: 'home.secondary.explore.desc' },
  { id: 'genie', label: 'home.secondary.genie', desc: 'home.secondary.genie.desc' },
]

// Mirror of App.jsx SIMPLIFIED_TABS: destinations that still exist in simplified view.
// Home must not surface doors/links that route to advanced-only tabs (they would bounce
// back to Generate), so both the semantic-layer door and the secondary links are gated.
const SIMPLIFIED_DESTS = new Set(['home', 'jobs', 'metadata', 'guide', 'context'])

export default function Home({ onNavigate }) {
  const { simplified } = useViewMode()
  const secondary = simplified ? SECONDARY.filter(s => SIMPLIFIED_DESTS.has(s.id)) : SECONDARY
  const { t } = useLanguage()
  return (
    <div className="max-w-4xl mx-auto py-6">
      <div className="text-center mb-8">
        <h2 className="text-2xl font-bold text-slate-800 dark:text-white">{t('home.title')}</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 mt-1.5">
          {t('home.subtitle')}
        </p>
      </div>

      <div className={`grid grid-cols-1 gap-5 ${simplified ? 'max-w-md mx-auto' : 'md:grid-cols-2'}`}>
        <DoorCard
          accent="#FF3621"
          eyebrow={t('home.door1.eyebrow')}
          title={t('home.door1.title')}
          description={t('home.door1.desc')}
          bullets={[
            t('home.door1.b1'),
            t('home.door1.b2'),
            t('home.door1.b3'),
            t('home.door1.b4'),
          ]}
          cta={t('home.door1.cta')}
          onClick={() => onNavigate('jobs')}
        />
        {/* Semantic layer is an advanced-only surface (hidden tab in simplified view),
            so the door is hidden there too — otherwise it dead-ends back to Generate. */}
        {!simplified && (
          <DoorCard
            accent="#1B7EAB"
            eyebrow={t('home.door2.eyebrow')}
            title={t('home.door2.title')}
            description={t('home.door2.desc')}
            bullets={[
              t('home.door2.b1'),
              t('home.door2.b2'),
              t('home.door2.b3'),
              t('home.door2.b4'),
            ]}
            cta={t('home.door2.cta')}
            onClick={() => onNavigate('semantic')}
          />
        )}
      </div>

      <div className="mt-8 border-t border-slate-200 dark:border-dbx-navy-400/30 pt-5">
        <p className="text-xs font-semibold uppercase tracking-wider text-slate-400 dark:text-slate-500 mb-3">
          {t('home.secondary.title')}
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-2.5">
          {secondary.map(s => (
            <button
              key={s.id}
              onClick={() => onNavigate(s.id)}
              className="flex items-center gap-2 min-w-0 px-3.5 py-2 rounded-lg border border-slate-200 dark:border-dbx-navy-400/30 text-sm text-slate-600 dark:text-slate-300 hover:border-dbx-lava/40 hover:text-dbx-lava dark:hover:text-dbx-lava transition-colors"
              title={t(s.desc)}
            >
              <span className="font-medium whitespace-nowrap">{t(s.label)}</span>
              <span className="text-xs text-slate-400 dark:text-slate-500 hidden lg:inline truncate">· {t(s.desc)}</span>
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}
