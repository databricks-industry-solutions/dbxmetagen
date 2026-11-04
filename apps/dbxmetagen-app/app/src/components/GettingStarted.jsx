import React from 'react'
import { PageHeader } from './ui'
import { useLanguage } from '../contexts/LanguageContext'

const ArrowDown = () => (
  <div className="flex justify-center py-2">
    <svg className="w-5 h-5 text-slate-300 dark:text-slate-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 14l-7 7m0 0l-7-7m7 7V3" />
    </svg>
  </div>
)

function FoundationCard({ step, title, what, why, tip, onClick, linkLabel }) {
  const { t } = useLanguage()
  return (
    <div className="card p-6 flex flex-col h-full">
      <div className="flex items-center gap-3 mb-3">
        <span className="flex items-center justify-center w-7 h-7 rounded-full bg-dbx-lava text-white text-xs font-bold shrink-0">{step}</span>
        <h3 className="text-base font-bold text-slate-800 dark:text-white">{title}</h3>
      </div>
      <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed mb-2">
        <span className="font-semibold text-slate-700 dark:text-slate-200">{t('gs.whatItDoes')}</span>{what}
      </p>
      <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed mb-3">
        <span className="font-semibold text-slate-700 dark:text-slate-200">{t('gs.whyItMatters')}</span>{why}
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
  const { t } = useLanguage()
  return (
    <div className="space-y-6 max-w-4xl mx-auto">
      <PageHeader
        title={t('gs.title')}
        subtitle={t('gs.subtitle')}
        actions={
          onStartTour && (
            <button onClick={onStartTour} className="btn-ghost btn-sm flex items-center gap-1.5">
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
              </svg>
              {t('gs.takeTour')}
            </button>
          )
        }
      />

      {/* Section 1: The Problem */}
      <div className="card p-6 border-l-4 border-l-dbx-lava bg-gradient-to-br from-white to-slate-50/80 dark:from-dbx-navy-650 dark:to-dbx-navy-700">
        <p className="text-sm text-slate-700 dark:text-slate-200 leading-relaxed">
          {t('gs.problem.p1')}
        </p>
        <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed mt-3">
          {t('gs.problem.p2')}
        </p>
      </div>

      {/* Section 2: Build the Foundation */}
      <div>
        <h2 className="heading-section mb-3">{t('gs.sec.foundation')}</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <FoundationCard
            step={1}
            title={t('gs.card.coreTitle')}
            what={t('gs.card.coreWhat')}
            why={t('gs.card.coreWhy')}
            tip={t('gs.card.coreTip')}
            onClick={() => onNavigate('jobs')}
            linkLabel={t('gs.card.coreCta')}
          />
          <FoundationCard
            step={2}
            title={t('gs.card.advancedTitle')}
            what={t('gs.card.advancedWhat')}
            why={t('gs.card.advancedWhy')}
            onClick={() => onNavigate('jobs')}
            linkLabel={t('gs.card.advancedCta')}
          />
        </div>
      </div>

      <ArrowDown />

      {/* Review callout */}
      <div className="card p-5 border-l-4 border-l-amber-400 bg-amber-50/50 dark:bg-amber-900/10">
        <h3 className="text-sm font-bold text-amber-800 dark:text-amber-300 mb-2">{t('gs.reviewMattersTitle')}</h3>
        <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed">
          {t('gs.reviewMattersBody')}
        </p>
      </div>

      <ArrowDown />

      {/* Section 3: What Gets Built */}
      <div className="card p-6">
        <h2 className="heading-section mb-4">{t('gs.sec.whatGetsBuilt')}</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-emerald-600 dark:text-emerald-400 mb-2">
              {t('gs.applied.title')}
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400 mb-3 italic">{t('gs.applied.subtitle')}</p>
            <ul className="space-y-1.5 text-sm text-slate-600 dark:text-slate-300">
              <li className="flex items-start gap-2">
                <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
                {t('gs.applied.item1')}
              </li>
              <li className="flex items-start gap-2">
                <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
                {t('gs.applied.item2')}
              </li>
              <li className="flex items-start gap-2">
                <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
                {t('gs.applied.item3')}
              </li>
              <li className="flex items-start gap-2">
                <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
                {t('gs.applied.item4')}
              </li>
              <li className="flex items-start gap-2">
                <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
                {t('gs.applied.item5')}
              </li>
            </ul>
          </div>
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-dbx-sky dark:text-dbx-sky mb-2">
              {t('gs.stored.title')}
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400 mb-3 italic">{t('gs.stored.subtitle')}</p>
            <ul className="space-y-2.5 text-sm text-slate-600 dark:text-slate-300">
              <li>
                <div className="flex items-start gap-2">
                  <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                  <span className="font-medium text-slate-700 dark:text-slate-200">{t('gs.stored.graphLabel')}</span>
                </div>
                <p className="text-xs text-slate-500 dark:text-slate-400 ml-5 mt-0.5">
                  {t('gs.stored.graphDesc')}
                </p>
              </li>
              <li>
                <div className="flex items-start gap-2">
                  <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                  <span className="font-medium text-slate-700 dark:text-slate-200">{t('gs.stored.vectorLabel')}</span>
                </div>
                <p className="text-xs text-slate-500 dark:text-slate-400 ml-5 mt-0.5">
                  {t('gs.stored.vectorDesc')}
                </p>
              </li>
              <li>
                <div className="flex items-start gap-2">
                  <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                  {t('gs.stored.item3')}
                </div>
              </li>
              <li>
                <div className="flex items-start gap-2">
                  <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                  {t('gs.stored.item4')}
                </div>
              </li>
              <li>
                <div className="flex items-start gap-2">
                  <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                  {t('gs.stored.item5')}
                </div>
              </li>
            </ul>
          </div>
        </div>
        <p className="text-xs text-slate-500 dark:text-slate-400 mt-4 pt-3 border-t border-slate-100 dark:border-dbx-navy-400/20">
          {t('gs.nothingTouches')}
        </p>
        <div className="mt-4 pt-3 border-t border-slate-100 dark:border-dbx-navy-400/20 space-y-2">
          <p className="text-xs text-slate-500 dark:text-slate-400">
            {t('gs.keepingFresh')}
          </p>
          <p className="text-xs text-slate-500 dark:text-slate-400">
            {t('gs.afterReviewingFks')}
          </p>
        </div>
        <div className="mt-4 pt-3 border-t border-slate-100 dark:border-dbx-navy-400/20">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400 mb-2">{t('gs.whatToLookFor')}</h3>
          <ul className="space-y-1 text-xs text-slate-500 dark:text-slate-400">
            <li>{t('gs.lookFor.ontology')}</li>
            <li>{t('gs.lookFor.pi')}</li>
            <li>{t('gs.lookFor.fk')}</li>
            <li>{t('gs.lookFor.comments')}</li>
          </ul>
        </div>
      </div>

      <ArrowDown />

      {/* Section 4: Review and Apply */}
      <div className="card p-6 border-l-4 border-l-dbx-sky">
        <div className="flex items-center gap-3 mb-3">
          <span className="flex items-center justify-center w-7 h-7 rounded-full bg-dbx-sky text-white text-xs font-bold shrink-0">3</span>
          <h3 className="text-base font-bold text-slate-800 dark:text-white">{t('gs.reviewApplyTitle')}</h3>
        </div>
        <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed mb-2">
          <span className="font-semibold text-slate-700 dark:text-slate-200">{t('gs.whatItDoes')}</span>
          {t('gs.reviewApplyWhat')}
        </p>
        <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed mb-4">
          <span className="font-semibold text-slate-700 dark:text-slate-200">{t('gs.whyItMatters')}</span>
          {t('gs.reviewApplyWhy')}
        </p>
        <div className="flex gap-2">
          <button onClick={() => onNavigate('metadata')} className="btn-primary btn-sm">{t('gs.reviewMetadata')}</button>
          <button onClick={() => onNavigate('metadata')} className="btn-ghost btn-sm">{t('gs.viewCoverage')}</button>
        </div>
      </div>

      <ArrowDown />

      {/* Section 5: Use Your Metadata */}
      <div>
        <h2 className="heading-section mb-1">{t('gs.sec.useMetadata')}</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 mb-4">
          {t('gs.useMetadataIntro')}
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <OutcomeCard
            title={t('gs.outcome.exploreTitle')}
            description={t('gs.outcome.exploreDesc')}
            onClick={() => onNavigate('entities')}
            linkLabel={t('gs.outcome.exploreCta')}
          />
          <OutcomeCard
            title={t('gs.outcome.askTitle')}
            description={t('gs.outcome.askDesc')}
            onClick={() => onNavigate('agent')}
            linkLabel={t('gs.outcome.askCta')}
          />
          <OutcomeCard
            title={t('gs.outcome.metricsTitle')}
            description={t('gs.outcome.metricsDesc')}
            onClick={() => onNavigate('semantic')}
            linkLabel={t('gs.outcome.metricsCta')}
          />
          <OutcomeCard
            title={t('gs.outcome.genieTitle')}
            description={t('gs.outcome.genieDesc')}
            onClick={() => onNavigate('genie')}
            linkLabel={t('gs.outcome.genieCta')}
          />
          <OutcomeCard
            title={t('gs.outcome.queryTitle')}
            description={t('gs.outcome.queryDesc')}
            onClick={() => onNavigate('metadata')}
            linkLabel={t('gs.outcome.queryCta')}
          />
        </div>
      </div>

      {/* Agent breakdown */}
      <div className="card p-6">
        <h2 className="heading-section mb-3">{t('gs.sec.agents')}</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 mb-4">{t('gs.agent.intro')}</p>
        <div className="space-y-3">
          <div className="p-4 rounded-lg border border-violet-200 dark:border-violet-800/40 bg-violet-50/50 dark:bg-violet-900/10">
            <h3 className="text-sm font-bold text-violet-700 dark:text-violet-300 mb-1">{t('gs.agent.metadata')}</h3>
            <p className="text-xs text-slate-600 dark:text-slate-300 leading-relaxed mb-2">
              {t('gs.agent.metadataDesc')}
            </p>
            <p className="text-[11px] text-slate-500 dark:text-slate-400">
              <span className="font-semibold">{t('gs.agent.toolsLabel')}</span> search_metadata, execute_metadata_sql, query_graph_nodes, get_node_details, find_similar_nodes, traverse_graph, expand_vs_hits, get_table_summary, get_data_quality, profile_key_columns, execute_data_sql, execute_graph_sql
            </p>
          </div>

          <div className="p-4 rounded-lg border border-amber-200 dark:border-amber-800/40 bg-amber-50/50 dark:bg-amber-900/10">
            <h3 className="text-sm font-bold text-amber-700 dark:text-amber-300 mb-1">{t('gs.agent.metricView')}</h3>
            <span className="text-[10px] font-medium text-amber-600 dark:text-amber-400 bg-amber-100 dark:bg-amber-900/40 px-1.5 py-0.5 rounded mb-2 inline-block">{t('common.underDevelopment')}</span>
            <p className="text-xs text-slate-600 dark:text-slate-300 leading-relaxed mb-2">
              {t('gs.agent.metricViewDesc')}
            </p>
            <p className="text-[11px] text-slate-500 dark:text-slate-400">
              <span className="font-semibold">{t('gs.agent.toolsLabel')}</span> search_metric_views, list_metric_views, describe_metric_view, metric_view_query, metric_view_dimension_query, get_related_views, get_view_lineage, explore_table_metrics, get_semantic_graph_context
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="p-3 rounded-lg border border-orange-200 dark:border-orange-800/40 bg-orange-50/30 dark:bg-orange-900/10">
              <div className="flex items-center gap-2 mb-1">
                <h4 className="text-xs font-bold text-orange-700 dark:text-orange-300">{t('gs.agent.governance')}</h4>
                <span className="text-[10px] font-medium text-orange-600 dark:text-orange-400 bg-orange-100 dark:bg-orange-900/40 px-1.5 py-0.5 rounded">{t('common.underDevelopment')}</span>
              </div>
              <p className="text-[11px] text-slate-600 dark:text-slate-300 leading-relaxed">
                {t('gs.agent.governanceDesc')}
              </p>
            </div>
            <div className="p-3 rounded-lg border border-rose-200 dark:border-rose-800/40 bg-rose-50/30 dark:bg-rose-900/10">
              <div className="flex items-center gap-2 mb-1">
                <h4 className="text-xs font-bold text-rose-700 dark:text-rose-300">{t('gs.agent.impact')}</h4>
                <span className="text-[10px] font-medium text-rose-600 dark:text-rose-400 bg-rose-100 dark:bg-rose-900/40 px-1.5 py-0.5 rounded">{t('common.underDevelopment')}</span>
              </div>
              <p className="text-[11px] text-slate-600 dark:text-slate-300 leading-relaxed">
                {t('gs.agent.impactDesc')}
              </p>
            </div>
          </div>

          <p className="text-[11px] text-slate-500 dark:text-slate-400 italic">
            {t('gs.agent.directAccessNote')}
          </p>
        </div>
      </div>

      {/* Cleanup tips */}
      <div className="card p-5 bg-slate-50/50 dark:bg-dbx-navy-700/30">
        <h3 className="text-sm font-bold text-slate-700 dark:text-slate-200 mb-2">{t('gs.cleanupTitle')}</h3>
        <p className="text-xs text-slate-500 dark:text-slate-400 leading-relaxed">
          {t('gs.cleanupBody')}
        </p>
      </div>
    </div>
  )
}
