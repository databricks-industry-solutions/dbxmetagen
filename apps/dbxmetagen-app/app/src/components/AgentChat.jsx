import React, { useState, useEffect, useRef, lazy, Suspense } from 'react'
import { ErrorBanner } from '../App'
import simpleMarkdown from '../utils/simpleMarkdown'
import ChatInputBar from './ChatInputBar'
import GovernanceExplorer from './GovernanceExplorer'
import ImpactAnalysis from './ImpactAnalysis'

const GraphSubgraph = lazy(() => import('./GraphSubgraph'))

const TOOL_DESCRIPTIONS = {
  search_metadata: 'Semantic search over indexed metadata documents',
  execute_metadata_sql: 'SQL query against knowledge base tables',
  execute_baseline_sql: 'SQL query (baseline: 3 KB tables only)',
  get_table_summary: 'Full summary of a specific table',
  get_data_quality: 'Profiling stats and quality scores',
  query_graph_nodes: 'Query the knowledge graph nodes',
  get_node_details: 'Get details for a specific graph node',
  find_similar_nodes: 'Find similar nodes via graph embeddings',
  traverse_graph: 'Traverse graph relationships',
  expand_vs_hits: 'Bridge VS results into graph (1-hop expansion)',
}

const INTENT_LABELS = {
  discovery: 'Discovery -- semantic search + table summary',
  query: 'Query -- SQL aggregation + table lookup',
  relationship: 'Relationship -- FK + graph traversal',
  governance: 'Governance -- PII/PHI + quality analysis',
  general: 'General -- full tool suite',
}

const MODE_CONFIG = {
  quick: { label: 'Quick Query', color: 'bg-blue-600', ring: 'ring-blue-600', lightBg: 'bg-blue-50 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300', desc: 'Fast ReAct with VS + graph', valueColor: 'text-blue-600' },
  graphrag: { label: 'GraphRAG Analysis', color: 'bg-violet-600', ring: 'ring-violet-600', lightBg: 'bg-violet-50 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300', desc: 'Multi-agent with full semantic layer', valueColor: 'text-violet-600' },
  baseline: { label: 'Baseline Analysis', color: 'bg-slate-600', ring: 'ring-slate-600', lightBg: 'bg-slate-50 text-slate-700 dark:bg-slate-800/40 dark:text-slate-300', desc: 'Multi-agent with KB tables only', valueColor: 'text-slate-600' },
  governance: { label: 'Governance', color: 'bg-orange-600', ring: 'ring-orange-600', lightBg: 'bg-orange-50 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300', desc: 'Sensitivity, compliance & protection audit', valueColor: 'text-orange-600', embedded: true },
  impact: { label: 'Impact Analysis', color: 'bg-rose-600', ring: 'ring-rose-600', lightBg: 'bg-rose-50 text-rose-700 dark:bg-rose-900/40 dark:text-rose-300', desc: 'What-if analysis for schema changes', valueColor: 'text-rose-600', embedded: true },
}

// Only these modes are shown in the UI; the rest are hidden but retained in MODE_CONFIG.
// Hidden modes: baseline, governance, impact
const VISIBLE_MODES = new Set(['quick', 'graphrag'])

const STAGE_LABELS = {
  starting: 'Starting...',
  routing: 'Classifying intent...',
  searching: 'Searching metadata...',
  planning: 'Planning analysis...',
  retrieving: 'Gathering evidence...',
  analyzing: 'Interpreting results...',
  responding: 'Preparing response...',
  clarifying: 'Clarifying question...',
}

const MODE_QUESTIONS = {
  quick: [
    { label: 'How do orders connect to accounts?', query: 'How do the order, product, and account tables relate to each other? Show the join keys and relationship paths.' },
    { label: 'Sensitive data + connections', query: 'Which tables contain sensitive customer information (PII/PHI) and how are those tables connected to each other?' },
    { label: 'Find "revenue" tables', query: 'Find all tables related to "revenue" and explain how they connect to the order pipeline.' },
    { label: 'Domain map + cross-domain links', query: 'What business domain does each table belong to and which domains share the most cross-domain relationships?' },
  ],
  graphrag: [
    { label: 'How do orders connect to accounts?', query: 'How do the order, product, and account tables relate to each other? Show the join keys and relationship paths.' },
    { label: 'Sensitive data + connections', query: 'Which tables contain sensitive customer information (PII/PHI) and how are those tables connected to each other?' },
    { label: 'Find "revenue" tables', query: 'Find all tables related to "revenue" and explain how they connect to the order pipeline.' },
    { label: 'Domain map + cross-domain links', query: 'What business domain does each table belong to and which domains share the most cross-domain relationships?' },
  ],
  baseline: [
    { label: 'How do orders connect to accounts?', query: 'How do the order, product, and account tables relate to each other? Show the join keys and relationship paths.' },
    { label: 'Sensitive data + connections', query: 'Which tables contain sensitive customer information (PII/PHI) and how are those tables connected to each other?' },
    { label: 'Find "revenue" tables', query: 'Find all tables related to "revenue" and explain how they connect to the order pipeline.' },
    { label: 'Domain map + cross-domain links', query: 'What business domain does each table belong to and which domains share the most cross-domain relationships?' },
  ],
}

function StatCard({ label, value, sub, gradient, onClick }) {
  return (
    <div onClick={onClick}
      className={`rounded-xl p-4 border shadow-card bg-gradient-to-br ${gradient} ${onClick ? 'cursor-pointer hover:shadow-card-hover transition-all duration-200 hover:scale-[1.02]' : ''}`}>
      <p className="text-xs font-medium uppercase tracking-wider text-slate-500 dark:text-slate-400 mb-1">{label}</p>
      <p className="text-2xl font-bold text-dbx-navy dark:text-white">{value ?? '--'}</p>
      {sub && <p className="text-xs text-slate-400 mt-1 truncate">{sub}</p>}
    </div>
  )
}

function AgentReasoning({ intent, toolCalls }) {
  const [open, setOpen] = useState(false)
  if (!intent && (!toolCalls || toolCalls.length === 0)) return null
  return (
    <div className="mt-2.5 pt-2.5 border-t border-slate-200/50 dark:border-dbx-navy-400/30">
      <button onClick={() => setOpen(!open)}
        className="text-xs text-slate-400 hover:text-dbx-navy dark:hover:text-slate-200 transition-colors flex items-center gap-1.5">
        <svg className={`w-3 h-3 transition-transform duration-200 ${open ? 'rotate-90' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        {open ? 'Hide' : 'Show'} reasoning
        {toolCalls?.length > 0 && <span className="badge bg-slate-100 dark:bg-dbx-navy-500 text-slate-500 dark:text-slate-400">{toolCalls.length} tools</span>}
      </button>
      {open && (
        <div className="mt-2 px-3 py-2.5 bg-dbx-oat-light dark:bg-dbx-navy-500/50 border border-dbx-oat-dark/30 dark:border-dbx-navy-400/30 rounded-lg text-xs space-y-2 animate-slide-up">
          {intent && (
            <div>
              <span className="text-slate-400">Intent:</span>{' '}
              <span className="font-mono font-medium text-dbx-navy dark:text-slate-200">{intent}</span>
              <span className="text-slate-400 ml-2">{INTENT_LABELS[intent] || ''}</span>
            </div>
          )}
          {toolCalls?.length > 0 && (
            <div className="space-y-1">
              <span className="text-slate-400">Tools used:</span>
              {toolCalls.map((t, i) => (
                <div key={i} className="flex items-baseline gap-2 ml-3" title={TOOL_DESCRIPTIONS[t] || ''}>
                  <span className="font-mono font-medium text-emerald-600 dark:text-emerald-400">{t}</span>
                  <span className="text-slate-400">{TOOL_DESCRIPTIONS[t] || ''}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function MessageBubble({ msg, onRetry }) {
  if (msg.role === 'user') {
    return (
      <div className="flex justify-end mb-4">
        <div className="bg-gradient-to-br from-dbx-violet-dark to-dbx-violet text-white rounded-2xl px-4 py-3 max-w-[80%] text-sm whitespace-pre-wrap shadow-card">
          {msg.content}
        </div>
      </div>
    )
  }
  if (msg.role === 'error') {
    return (
      <div className="flex justify-start mb-4 animate-slide-up">
        <div className="card border-l-4 border-l-red-400 px-4 py-3 max-w-[85%] text-sm">
          <p className="text-red-600 dark:text-red-400">{msg.content}</p>
          {onRetry && (
            <button onClick={onRetry} className="mt-2 text-xs text-red-500 hover:text-red-700 dark:hover:text-red-300 font-medium">
              Retry this question
            </button>
          )}
        </div>
      </div>
    )
  }
  const cfg = MODE_CONFIG[msg.mode] || MODE_CONFIG.quick
  const elapsed = msg.elapsed_ms
  return (
    <div className="flex justify-start mb-4 animate-slide-up">
      <div className="card px-4 py-3 max-w-[85%] text-sm overflow-hidden">
        <div className="flex items-center gap-2 mb-1">
          {msg.mode && (
            <span className={`badge text-xs ${cfg.lightBg}`}>
              {cfg.label}
            </span>
          )}
          {elapsed != null && (
            <span className="text-[10px] text-slate-400 dark:text-slate-500 tabular-nums">
              {elapsed >= 1000 ? `${(elapsed / 1000).toFixed(1)}s` : `${elapsed}ms`}
            </span>
          )}
        </div>
        <div className="prose prose-sm max-w-none whitespace-pre-wrap break-words text-slate-700 dark:text-slate-300"
          dangerouslySetInnerHTML={{ __html: simpleMarkdown(msg.content) }} />
        {msg.graph_data && Object.keys(msg.graph_data.nodes || {}).length > 0 && (
          <Suspense fallback={<div className="h-48 flex items-center justify-center text-xs text-slate-400">Loading graph...</div>}>
            <div className="mt-3 border border-slate-200 dark:border-slate-600 rounded-xl overflow-hidden">
              <div className="px-3 py-1.5 bg-slate-50 dark:bg-dbx-navy-600 flex items-center gap-2">
                <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">Graph Context</span>
                <span className="text-[10px] text-slate-400">{Object.keys(msg.graph_data.nodes).length} nodes, {(msg.graph_data.edges || []).length} edges</span>
              </div>
              <GraphSubgraph
                nodes={msg.graph_data.nodes}
                edges={msg.graph_data.edges}
                startNode={msg.graph_data.start_node}
                height={300}
              />
            </div>
          </Suspense>
        )}
        <AgentReasoning intent={msg.intent} toolCalls={msg.tool_calls} />
      </div>
    </div>
  )
}

function DomainItem({ label, count, onClick }) {
  return (
    <button onClick={onClick}
      className="flex items-center justify-between w-full px-3 py-2 rounded-lg hover:bg-dbx-oat dark:hover:bg-dbx-navy-500/50 transition-all text-left group">
      <span className="text-xs font-medium text-slate-700 dark:text-slate-300 truncate group-hover:text-dbx-navy dark:group-hover:text-white">{label}</span>
      <span className="text-xs font-bold text-dbx-navy dark:text-dbx-teal ml-2 flex-shrink-0">{count}</span>
    </button>
  )
}

function RetrievalTechniques() {
  const [open, setOpen] = useState(false)
  return (
    <div className="mb-3">
      <button onClick={() => setOpen(!open)}
        className="text-xs text-slate-400 hover:text-dbx-navy dark:hover:text-slate-200 transition-colors flex items-center gap-1.5">
        <svg className={`w-3 h-3 transition-transform duration-200 ${open ? 'rotate-90' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        Retrieval Techniques
      </button>
      {open && (
        <div className="mt-2 card p-4 text-xs text-slate-600 dark:text-slate-300 space-y-3 animate-slide-up">
          <div>
            <p className="font-semibold text-blue-600 dark:text-blue-400 mb-0.5">Quick Query</p>
            <p>Intent classification selects tool subset. Vector search (VS) finds semantically similar metadata documents. Graph tools do 1-hop expansion from VS hits via Lakebase PG (falls back to UC Delta).</p>
          </div>
          <div>
            <p className="font-semibold text-violet-600 dark:text-violet-400 mb-0.5">GraphRAG Analysis</p>
            <p>Multi-agent pipeline: Supervisor routes to Planner, Retrieval, Analyst, and Response subagents. Planner generates a numbered data-gathering plan using knowledge of the graph schema (node types, edge types, join expressions). Retrieval executes VS semantic search, graph traversal (multi-hop BFS with edge_type filtering), and SQL queries. node_id bridges VS hits to graph nodes for hybrid search. Analyst synthesizes evidence into findings with source citations.</p>
          </div>
          <div>
            <p className="font-semibold text-slate-700 dark:text-slate-300 mb-0.5">Baseline Analysis</p>
            <p>Same multi-agent pipeline as GraphRAG but restricted to three tables: table_knowledge_base, column_knowledge_base, schema_knowledge_base. No vector search, no graph traversal, no ontology, no FK predictions. Demonstrates the value added by the semantic layer.</p>
          </div>
          <div>
            <p className="font-semibold text-slate-700 dark:text-slate-300 mb-0.5">Fallback Strategy</p>
            <p>All graph queries try Lakebase PG first (sub-100ms), then fall back to UC Delta tables. VS queries use the Databricks Vector Search endpoint.</p>
          </div>
        </div>
      )}
    </div>
  )
}

function ToolsPanel() {
  const [open, setOpen] = useState(false)
  return (
    <div className="card p-4">
      <button onClick={() => setOpen(!open)}
        className="flex items-center justify-between w-full text-left">
        <h3 className="section-title">Available Tools</h3>
        <svg className={`w-3.5 h-3.5 text-slate-400 transition-transform duration-200 ${open ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {open && (
        <div className="mt-3 space-y-1.5 animate-slide-up">
          {Object.entries(TOOL_DESCRIPTIONS).map(([name, desc]) => (
            <div key={name} className="flex items-baseline gap-2 px-2 py-1.5 rounded-lg bg-dbx-oat-light/50 dark:bg-dbx-navy-500/30">
              <span className="font-mono text-xs font-medium text-emerald-600 dark:text-emerald-400 flex-shrink-0">{name}</span>
              <span className="text-xs text-slate-500 dark:text-slate-400">{desc}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function AgentChat() {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [stage, setStage] = useState(null)
  const [error, setError] = useState(null)
  const [stats, setStats] = useState(null)
  const [domainStats, setDomainStats] = useState(null)
  const [suggestions, setSuggestions] = useState([])
  const [mode, setMode] = useState('quick')
  const chatEndRef = useRef(null)

  useEffect(() => {
    fetch('/api/agent/stats').then(r => r.ok ? r.json() : null).then(setStats).catch(() => {})
    fetch('/api/agent/suggestions').then(r => r.ok ? r.json() : []).then(setSuggestions).catch(() => {})
    fetch('/api/agent/domain-stats').then(r => r.ok ? r.json() : null).then(setDomainStats).catch(() => {})
  }, [])

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  const sendDeepAnalysis = async (text, useMode) => {
    setStage('starting')
    try {
      const history = messages.filter(m => m.role !== 'error').map(m => ({ role: m.role, content: m.content }))
      const submitRes = await fetch('/api/agent/deep/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: text, history, mode: useMode }),
      })
      if (!submitRes.ok) {
        const data = await submitRes.json().catch(() => ({}))
        setMessages(prev => [...prev, { role: 'error', content: data.detail || 'Submit failed', _query: text, _mode: useMode }])
        return
      }
      const { task_id, error: submitErr } = await submitRes.json()
      if (submitErr || !task_id) {
        setMessages(prev => [...prev, { role: 'error', content: submitErr || 'No task ID returned', _query: text, _mode: useMode }])
        return
      }
      for (let i = 0; i < 150; i++) {
        await new Promise(r => setTimeout(r, 2000))
        const pollRes = await fetch(`/api/agent/deep/task/${task_id}`)
        if (!pollRes.ok) continue
        const task = await pollRes.json()
        if (task.status === 'done') {
          setMessages(prev => [...prev, {
            role: 'assistant',
            content: task.answer || task.response || '',
            tool_calls: task.tool_calls || [],
            intent: null,
            mode: task.mode || useMode,
            elapsed_ms: task.elapsed_ms,
            graph_data: task.graph_data || null,
          }])
          return
        }
        if (task.status === 'error') {
          setMessages(prev => [...prev, { role: 'error', content: task.error || 'Analysis failed', _query: text, _mode: useMode }])
          return
        }
        if (task.stage) setStage(task.stage)
      }
      setMessages(prev => [...prev, { role: 'error', content: 'Analysis timed out after 5 minutes', _query: text, _mode: useMode }])
    } catch (e) {
      setMessages(prev => [...prev, { role: 'error', content: e.message, _query: text, _mode: useMode }])
    }
  }

  const sendQuick = async (text, useMode) => {
    setStage('routing')
    const history = messages.filter(m => m.role !== 'error').map(m => ({ role: m.role, content: m.content }))
    try {
      setStage('searching')
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: text, history, mode: useMode }),
      })
      setStage('responding')
      const data = await res.json()
      if (!res.ok) {
        setMessages(prev => [...prev, { role: 'error', content: data.detail || 'Agent request failed', _query: text, _mode: useMode }])
      } else {
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: data.answer,
          tool_calls: data.tool_calls || [],
          intent: data.intent,
          mode: data.mode || useMode,
          elapsed_ms: data.elapsed_ms,
          graph_data: data.graph_data || null,
        }])
      }
    } catch (e) {
      setMessages(prev => [...prev, { role: 'error', content: e.message, _query: text, _mode: useMode }])
    }
  }

  const send = async (text, retryMode) => {
    if (!text.trim()) return
    const useMode = retryMode || mode
    const userMsg = { role: 'user', content: text.trim() }
    setMessages(prev => [...prev, userMsg])
    setInput('')
    setLoading(true)
    setError(null)

    if (useMode === 'graphrag' || useMode === 'baseline') {
      await sendDeepAnalysis(text.trim(), useMode)
    } else {
      await sendQuick(text.trim(), useMode)
    }
    setLoading(false)
    setStage(null)
  }

  const clearChat = () => { setMessages([]); setError(null) }
  const injectQuery = (q) => send(q)
  const cfg = MODE_CONFIG[mode] || MODE_CONFIG.quick

  return (
    <div className="min-h-[calc(100vh-12rem)]">
      {/* Mode selector -- segmented control */}
      <div className="flex flex-wrap bg-dbx-oat dark:bg-dbx-navy-650 rounded-xl p-1 mb-3 shadow-inner-soft">
        {Object.entries(MODE_CONFIG).filter(([key]) => VISIBLE_MODES.has(key)).map(([key, c]) => (
          <button key={key} onClick={() => { setMode(key); setMessages([]); setError(null) }}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${
              mode === key
                ? `${c.color} text-white shadow-sm`
                : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'
            }`}
            title={c.desc}>{c.label}</button>
        ))}
      </div>

      {mode === 'governance' ? <GovernanceExplorer embedded /> :
       mode === 'impact' ? <ImpactAnalysis embedded /> : (
      <div className="flex gap-5">
      <ErrorBanner error={error} />

      {/* Left: Chat */}
      <div className="flex-1 flex flex-col min-w-0" style={{ flexBasis: '65%' }}>
        <RetrievalTechniques />

        {/* Chat area */}
        <div className="flex-1 min-h-0 card p-5 overflow-y-auto mb-3 scrollbar-thin bg-gradient-to-b from-white to-dbx-oat-light/50 dark:from-dbx-navy-650 dark:to-dbx-navy/50">
          {messages.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full text-center py-12">
              <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-dbx-navy to-dbx-navy-500 flex items-center justify-center mb-4 shadow-card">
                <svg className="w-7 h-7 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                </svg>
              </div>
              <h2 className="text-lg font-bold text-slate-800 dark:text-slate-100 mb-1">{cfg.label}</h2>
              <p className="text-sm text-slate-500 dark:text-slate-400 mb-6 max-w-md">{cfg.desc}</p>
              <div className="flex flex-wrap gap-2.5 justify-center max-w-lg">
                {(MODE_QUESTIONS[mode] || MODE_QUESTIONS.quick).map((s, i) => (
                  <button key={i} onClick={() => send(s.query)}
                    className="group flex items-center gap-2 px-4 py-2 card-interactive text-xs text-slate-600 dark:text-slate-300 hover:text-dbx-navy dark:hover:text-white">
                    <span className="w-1.5 h-1.5 rounded-full bg-dbx-lava/60 group-hover:bg-dbx-lava transition-colors flex-shrink-0" />
                    {s.label}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <>
              {messages.map((m, i) => (
                <MessageBubble key={i} msg={m}
                  onRetry={m.role === 'error' ? () => send(m._query, m._mode) : null} />
              ))}
              {loading && (
                <div className="flex justify-start mb-4 animate-slide-up">
                  <div className="card px-4 py-3 text-sm text-slate-400 flex items-center gap-2.5">
                    <span className="inline-block w-2 h-2 bg-dbx-lava rounded-full animate-pulse" />
                    {STAGE_LABELS[stage] || 'Thinking...'}
                  </div>
                </div>
              )}
              <div ref={chatEndRef} />
            </>
          )}
        </div>

        {/* Input bar */}
        <ChatInputBar value={input} onChange={setInput} onSubmit={() => send(input)}
          loading={loading}
          placeholder={mode === 'quick' ? 'Ask about your data catalog...' : `Ask for ${cfg.label.toLowerCase()}...`}
          buttonLabel="Send" buttonColor={cfg.color}
          onClear={messages.length > 0 ? clearChat : undefined} />
      </div>

      {/* Right: Stats Panel */}
      <div className="w-80 xl:w-96 flex-shrink-0 overflow-y-auto space-y-4 scrollbar-thin max-h-[calc(100vh-12rem)]">
        {stats && (
          <div className="grid grid-cols-2 gap-3">
            <StatCard label="Tables" value={stats.tables_profiled}
              gradient="from-blue-50 to-blue-100/50 border-blue-200/50 dark:from-blue-900/20 dark:to-blue-900/10 dark:border-blue-800/30"
              onClick={() => injectQuery('What tables exist in my catalog?')} />
            <StatCard label="Entity Types" value={stats.entity_types}
              gradient="from-violet-50 to-violet-100/50 border-violet-200/50 dark:from-violet-900/20 dark:to-violet-900/10 dark:border-violet-800/30"
              onClick={() => injectQuery('What entity types were discovered?')} />
            <StatCard label="FK Relations" value={stats.fk_predictions}
              gradient="from-emerald-50 to-emerald-100/50 border-emerald-200/50 dark:from-emerald-900/20 dark:to-emerald-900/10 dark:border-emerald-800/30"
              onClick={() => injectQuery('Show me foreign key relationships')} />
            <StatCard label="Metric Views" value={stats.metric_views}
              gradient="from-amber-50 to-amber-100/50 border-amber-200/50 dark:from-amber-900/20 dark:to-amber-900/10 dark:border-amber-800/30"
              onClick={() => injectQuery('What metric views are available?')} />
            <StatCard label="VS Documents" value={stats.vs_documents}
              gradient="from-rose-50 to-rose-100/50 border-rose-200/50 dark:from-rose-900/20 dark:to-rose-900/10 dark:border-rose-800/30"
              sub={stats.vs_source === 'index'
                ? `${stats.vs_documents} (from VS index)`
                : stats.vs_by_type ? Object.entries(stats.vs_by_type).map(([k,v]) => `${k}: ${v}`).join(', ') : ''} />
            <StatCard label="Quality" value={stats.avg_quality ?? '--'}
              gradient="from-teal-50 to-teal-100/50 border-teal-200/50 dark:from-teal-900/20 dark:to-teal-900/10 dark:border-teal-800/30"
              onClick={() => injectQuery('Show me the data quality summary')} />
          </div>
        )}

        {domainStats?.tables_by_domain?.length > 0 && (
          <div className="card p-4">
            <h3 className="section-title mb-3">Tables by Domain</h3>
            <div className="space-y-0.5 max-h-44 overflow-y-auto scrollbar-thin">
              {domainStats.tables_by_domain.map((d, i) => (
                <DomainItem key={i} label={d.domain || 'unknown'} count={d.count}
                  onClick={() => injectQuery(`Describe the ${d.domain} domain tables`)} />
              ))}
            </div>
          </div>
        )}

        {domainStats?.entities_by_type?.length > 0 && (
          <div className="card p-4">
            <h3 className="section-title mb-3">Entity Types</h3>
            <div className="space-y-0.5 max-h-44 overflow-y-auto scrollbar-thin">
              {domainStats.entities_by_type.map((e, i) => (
                <DomainItem key={i} label={e.type || 'unknown'} count={e.count}
                  onClick={() => injectQuery(`Tell me about the ${e.type} entity type`)} />
              ))}
            </div>
          </div>
        )}

        {domainStats?.fk_by_domain?.length > 0 && (
          <div className="card p-4">
            <h3 className="section-title mb-3">FK Relations by Domain</h3>
            <div className="space-y-0.5 max-h-36 overflow-y-auto scrollbar-thin">
              {domainStats.fk_by_domain.map((d, i) => (
                <DomainItem key={i} label={d.domain || 'unknown'} count={d.count}
                  onClick={() => injectQuery(`Show foreign key relationships in the ${d.domain} domain`)} />
              ))}
            </div>
          </div>
        )}

        <ToolsPanel />
      </div>
      </div>
      )}
    </div>
  )
}
