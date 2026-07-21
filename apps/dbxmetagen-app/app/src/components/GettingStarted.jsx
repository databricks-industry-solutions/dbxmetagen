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
            title="Advanced Metadata (Knowledge Graph)"
            what="Maps your tables to business concepts (Patient, Account, Transaction) using industry ontologies. Discovers foreign key relationships. Computes column similarity. Builds a searchable index."
            why="The knowledge graph answers structural questions — 'which tables represent patients?', 'how do these 50 tables connect?' — whether you use agents or just browse it directly."
            onClick={() => onNavigate('jobs')}
            linkLabel="Generate Advanced Metadata"
          />
        </div>
      </div>

      <ArrowDown />

      {/* Review callout */}
      <div className="card p-5 border-l-4 border-l-amber-400 bg-amber-50/50 dark:bg-amber-900/10">
        <h3 className="text-sm font-bold text-amber-800 dark:text-amber-300 mb-2">Review matters at every step</h3>
        <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed">
          AI-generated metadata is a starting point, not a final answer. Sensitivity classifications need verification for compliance.
          Ontology mappings below 0.6 confidence should be checked. FK predictions need approval before they become constraints.
          The value of dbxmetagen comes from the combination of AI speed and human judgment.
        </p>
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
            <ul className="space-y-2.5 text-sm text-slate-600 dark:text-slate-300">
              <li>
                <div className="flex items-start gap-2">
                  <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                  <span className="font-medium text-slate-700 dark:text-slate-200">Knowledge graph</span>
                </div>
                <p className="text-xs text-slate-500 dark:text-slate-400 ml-5 mt-0.5">
                  <code className="text-xs bg-slate-100 dark:bg-dbx-navy-500 px-1 py-0.5 rounded">graph_nodes</code> — one row per table, column, or entity with metadata properties.
                  {' '}<code className="text-xs bg-slate-100 dark:bg-dbx-navy-500 px-1 py-0.5 rounded">graph_edges</code> — relationships between nodes (FK references, ontology links, similarity, schema containment). Each edge has a <code className="text-xs bg-slate-100 dark:bg-dbx-navy-500 px-1 py-0.5 rounded">source_system</code> indicating which module created it.
                  Optionally synced to Lakebase for low-latency graph traversal by agents.
                </p>
              </li>
              <li>
                <div className="flex items-start gap-2">
                  <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                  <span className="font-medium text-slate-700 dark:text-slate-200">Vector index</span>
                </div>
                <p className="text-xs text-slate-500 dark:text-slate-400 ml-5 mt-0.5">
                  A Databricks Vector Search index over metadata documents (table/column descriptions, ontology chunks, metric view definitions).
                  Powers semantic search in the agent and the Search tab. Updated by the full pipeline, the "Sync Vector Index" button, or the weekly scheduled job (paused by default).
                </p>
              </li>
              <li>
                <div className="flex items-start gap-2">
                  <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                  Entity-to-table mappings (ontology)
                </div>
              </li>
              <li>
                <div className="flex items-start gap-2">
                  <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                  Column profiles and data quality scores
                </div>
              </li>
              <li>
                <div className="flex items-start gap-2">
                  <span className="text-dbx-sky mt-0.5 shrink-0">&#9679;</span>
                  Community summaries
                </div>
              </li>
            </ul>
          </div>
        </div>
        <p className="text-xs text-slate-500 dark:text-slate-400 mt-4 pt-3 border-t border-slate-100 dark:border-dbx-navy-400/20">
          Nothing touches your production catalog until you explicitly apply it. dbxmetagen generates into its own tables first.
        </p>
        <div className="mt-4 pt-3 border-t border-slate-100 dark:border-dbx-navy-400/20 space-y-2">
          <p className="text-xs text-slate-500 dark:text-slate-400">
            <span className="font-semibold">Keeping things fresh:</span> A weekly vector index sync job is deployed but <span className="font-semibold">paused by default</span>.
            Unpause it in your Databricks Workflows UI to keep the search index fresh automatically as metadata changes.
            Without it, the index only updates when you run the full analytics pipeline or click "Sync Vector Index" in the Advanced Metadata tab.
          </p>
          <p className="text-xs text-slate-500 dark:text-slate-400">
            <span className="font-semibold">After reviewing foreign keys:</span> Use the "Sync Knowledge Graph" and "Sync Vector Index" buttons
            in the Advanced Metadata tab to propagate your approve/reject decisions without re-running the full pipeline.
          </p>
        </div>
        <div className="mt-4 pt-3 border-t border-slate-100 dark:border-dbx-navy-400/20">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400 mb-2">What to look for when reviewing</h3>
          <ul className="space-y-1 text-xs text-slate-500 dark:text-slate-400">
            <li><span className="font-medium text-slate-600 dark:text-slate-300">Ontology entities</span> — check confidence scores; below 0.6 means the mapping is uncertain</li>
            <li><span className="font-medium text-slate-600 dark:text-slate-300">PI / PHI / PCI</span> — verify all classifications; false negatives have compliance implications</li>
            <li><span className="font-medium text-slate-600 dark:text-slate-300">FK predictions</span> — approve high-confidence ones, review medium, reject false positives. Do this before generating metric views, since their joins are built from approved foreign keys</li>
            <li><span className="font-medium text-slate-600 dark:text-slate-300">Comments</span> — spot-check 10-20% for accuracy, especially domain-specific tables</li>
          </ul>
        </div>
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
          <button onClick={() => onNavigate('metadata')} className="btn-ghost btn-sm">View Coverage</button>
        </div>
      </div>

      <ArrowDown />

      {/* Section 5: Use Your Metadata */}
      <div>
        <h2 className="heading-section mb-1">Use Your Metadata</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 mb-4">
          The outputs are standard Delta tables and Vector Search indexes — consumable from any tool, not just this app.
          Genie spaces and the agent are built-in use cases, but you can also query the knowledge graph directly from notebooks, dashboards, or your own applications.
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
            description="Chat with a metadata-aware agent. Ask 'what tables contain patient demographics?' or 'how does the claims schema connect to encounters?' The agent uses the knowledge graph and vector index to find answers. See the agent breakdown below for details."
            onClick={() => onNavigate('agent')}
            linkLabel="Open Agent"
          />
          <OutcomeCard
            title="Define Metrics"
            description="Turn business questions into governed Unity Catalog metric views any tool can query consistently. For the richest output, select fact tables together with their dimensions so the generator can join them — review and approve FK predictions first, since joins come from foreign keys. Already-aggregated gold/data-mart tables instead produce simple single-table views with no joins."
            onClick={() => onNavigate('semantic')}
            linkLabel="Define Metric Views"
          />
          <OutcomeCard
            title="Build a Genie Space"
            description="Assemble a natural-language SQL interface so end users can ask business questions in plain English. Genie uses your descriptions, relationships, and metric views to write accurate SQL."
            onClick={() => onNavigate('genie')}
            linkLabel="Build Genie Space"
          />
          <OutcomeCard
            title="Query Directly"
            description="All outputs live in your metadata schema as Delta tables. Run SQL against graph_nodes, graph_edges, ontology_entities, or column_knowledge_base from any notebook, dashboard, or downstream pipeline."
            onClick={() => onNavigate('metadata')}
            linkLabel="Review & Coverage"
          />
        </div>
      </div>

      {/* Agent breakdown */}
      <div className="card p-6">
        <h2 className="heading-section mb-3">Agents</h2>
        <p className="text-sm text-slate-500 dark:text-slate-400 mb-4">
          The Explore tab has multiple agents, each with different data access and purpose.
        </p>
        <div className="space-y-3">
          <div className="p-4 rounded-lg border border-violet-200 dark:border-violet-800/40 bg-violet-50/50 dark:bg-violet-900/10">
            <h3 className="text-sm font-bold text-violet-700 dark:text-violet-300 mb-1">Metadata Agent (GraphRAG)</h3>
            <p className="text-xs text-slate-600 dark:text-slate-300 leading-relaxed mb-2">
              The primary agent. Combines vector search, multi-hop graph traversal (via Lakebase or Delta), and SQL queries across all knowledge base and graph tables.
              Intent-classifies your question first (discovery, query, relationship, governance), then selects the right tools.
            </p>
            <p className="text-[11px] text-slate-500 dark:text-slate-400">
              <span className="font-semibold">Tools:</span> search_metadata, execute_metadata_sql, query_graph_nodes, get_node_details, find_similar_nodes, traverse_graph, expand_vs_hits, get_table_summary, get_data_quality, profile_key_columns, execute_data_sql, execute_graph_sql
            </p>
          </div>

          <div className="p-4 rounded-lg border border-amber-200 dark:border-amber-800/40 bg-amber-50/50 dark:bg-amber-900/10">
            <h3 className="text-sm font-bold text-amber-700 dark:text-amber-300 mb-1">Metric View Agent</h3>
            <span className="text-[10px] font-medium text-amber-600 dark:text-amber-400 bg-amber-100 dark:bg-amber-900/40 px-1.5 py-0.5 rounded mb-2 inline-block">Under Development</span>
            <p className="text-xs text-slate-600 dark:text-slate-300 leading-relaxed mb-2">
              Answers business questions by querying deployed Unity Catalog metric views. Searches for relevant metrics, executes measure and dimension queries, and interprets KPI data. Only sees metric views -- not the full knowledge graph.
            </p>
            <p className="text-[11px] text-slate-500 dark:text-slate-400">
              <span className="font-semibold">Tools:</span> search_metric_views, list_metric_views, describe_metric_view, metric_view_query, metric_view_dimension_query, get_related_views, get_view_lineage, explore_table_metrics, get_semantic_graph_context
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="p-3 rounded-lg border border-orange-200 dark:border-orange-800/40 bg-orange-50/30 dark:bg-orange-900/10">
              <h4 className="text-xs font-bold text-orange-700 dark:text-orange-300 mb-1">Governance Agent</h4>
              <p className="text-[11px] text-slate-600 dark:text-slate-300 leading-relaxed">
                Audits PII/PHI/PCI classification coverage, finds gaps in masking, traces sensitive data lineage, and identifies re-identification risk paths through entity relationships.
              </p>
            </div>
            <div className="p-3 rounded-lg border border-rose-200 dark:border-rose-800/40 bg-rose-50/30 dark:bg-rose-900/10">
              <h4 className="text-xs font-bold text-rose-700 dark:text-rose-300 mb-1">Impact Analysis Agent</h4>
              <p className="text-[11px] text-slate-600 dark:text-slate-300 leading-relaxed">
                What-if analysis for schema changes. Shows downstream dependencies, affected metric views, entity impact, and column importance before you drop, rename, or retype a column or table.
              </p>
            </div>
          </div>

          <p className="text-[11px] text-slate-500 dark:text-slate-400 italic">
            The Graph Explorer and Search tabs provide direct access to the knowledge graph and vector index without going through an agent.
          </p>
        </div>
      </div>

      {/* Cleanup tips */}
      <div className="card p-5 bg-slate-50/50 dark:bg-dbx-navy-700/30">
        <h3 className="text-sm font-bold text-slate-700 dark:text-slate-200 mb-2">Re-running and cleanup</h3>
        <p className="text-xs text-slate-500 dark:text-slate-400 leading-relaxed">
          Re-running the pipeline is safe — results merge via deterministic IDs. If you see too many similarity edges
          or stale data from a previous ontology bundle, check <span className="font-medium">Sweep stale artifacts</span> on the
          next full (non-incremental) pipeline run — it refreshes stale entities, edges, and docs together for the tables in scope.
          To sweep just one artifact type after review, the Advanced Metadata tab's <span className="font-medium">Sync Knowledge Graph</span> button
          sweeps edges and <span className="font-medium">Sync Vector Index</span> sweeps docs. For targeted cleanup, query <code className="text-xs bg-slate-100 dark:bg-dbx-navy-500 px-1 py-0.5 rounded">graph_edges</code> grouped
          by <code className="text-xs bg-slate-100 dark:bg-dbx-navy-500 px-1 py-0.5 rounded">source_system</code> to diagnose which module is producing excess edges.
        </p>
      </div>
    </div>
  )
}
