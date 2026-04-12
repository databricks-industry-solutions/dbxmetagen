import React, { useState, useEffect, useRef, lazy, Suspense } from 'react'
import { ErrorBanner } from '../App'
import simpleMarkdown from '../utils/simpleMarkdown'
import ChatInputBar from './ChatInputBar'
import GovernanceExplorer from './GovernanceExplorer'
import ImpactAnalysis from './ImpactAnalysis'
import ChartRenderer from './ChartRenderer'

const GraphSubgraph = lazy(() => import('./GraphSubgraph'))
const GraphExplorer = lazy(() => import('./GraphExplorer'))
const VectorSearch = lazy(() => import('./VectorSearch'))

const SUB_TABS = [
  { id: 'chat',   label: 'Chat' },
  { id: 'graph',  label: 'Graph Explorer' },
  { id: 'search', label: 'Search' },
]

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
  quick: { label: 'Quick Query', color: 'bg-blue-600', ring: 'ring-blue-600', lightBg: 'bg-blue-50 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300', desc: 'Fast query using search and graph traversal', valueColor: 'text-blue-600' },
  graphrag: { label: 'dbxmetagen Agent', color: 'bg-violet-600', ring: 'ring-violet-600', lightBg: 'bg-violet-50 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300', desc: 'Deep analysis with knowledge graph, vector search, and SQL', valueColor: 'text-violet-600' },
  baseline: { label: 'Baseline Analysis', color: 'bg-slate-600', ring: 'ring-slate-600', lightBg: 'bg-slate-50 text-slate-700 dark:bg-slate-800/40 dark:text-slate-300', desc: 'Multi-agent analysis using knowledge base tables', valueColor: 'text-slate-600' },
  governance: { label: 'Governance', color: 'bg-orange-600', ring: 'ring-orange-600', lightBg: 'bg-orange-50 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300', desc: 'Sensitivity, compliance & protection audit', valueColor: 'text-orange-600', embedded: true },
  impact: { label: 'Impact Analysis', color: 'bg-rose-600', ring: 'ring-rose-600', lightBg: 'bg-rose-50 text-rose-700 dark:bg-rose-900/40 dark:text-rose-300', desc: 'What-if analysis for schema changes', valueColor: 'text-rose-600', embedded: true },
}

/**
 * Strip leading JSON blocks (intent classification, retrieval plan) from the
 * LLM answer. These are internal agent state that belongs in the reasoning
 * panel, not the main answer body.
 */
function stripLeadingJson(text) {
  if (!text) return text
  let s = text.trimStart()
  // Repeatedly strip JSON objects at the start of the text
  while (s.startsWith('{')) {
    let depth = 0, i = 0
    for (; i < s.length; i++) {
      if (s[i] === '{') depth++
      else if (s[i] === '}') { depth--; if (depth === 0) { i++; break } }
    }
    if (depth !== 0) break
    s = s.slice(i).trimStart()
  }
  return s
}

// Only these modes are shown in the UI; the rest are hidden but retained in MODE_CONFIG.
// Hidden modes: baseline, governance, impact
const VISIBLE_MODES = new Set(['graphrag'])

const STAGE_LABELS = {
  starting: 'Starting...',
  routing: 'Classifying intent...',
  classifying_intent: 'Classifying intent...',
  searching: 'Searching metadata...',
  planning: 'Planning analysis...',
  retrieving: 'Gathering evidence...',
  gathering: 'Gathering evidence...',
  assembling: 'Assembling evidence...',
  analyzing: 'Generating analysis...',
  responding: 'Preparing response...',
  clarifying: 'Clarifying question...',
}

const MODE_QUESTIONS = {
  quick: [
    { label: 'Which table is the join hub?', query: 'Which table has the most foreign key relationships? Show its join keys and the tables it connects to.' },
    { label: 'Highest PII concentration', query: 'Which table has the most columns classified as PII or PHI? Show those columns and what other tables it connects to.' },
    { label: 'Catalog at a glance', query: 'Summarize the data catalog: total tables, top 3 domains by table count, and the densest relationship cluster.' },
    { label: 'Most cross-linked domain', query: 'Which business domain has the most relationships to other domains? Show the key cross-domain table connections.' },
  ],
  graphrag: [
    { label: 'Which table is the join hub?', query: 'Which table has the most foreign key relationships? Show its join keys and the tables it connects to.' },
    { label: 'Highest PII concentration', query: 'Which table has the most columns classified as PII or PHI? Show those columns and what other tables it connects to.' },
    { label: 'Sensitive columns missing docs', query: 'Are there any PII or PHI columns that lack descriptions or have high null rates? Show the top concerns.' },
    { label: 'Most mapped entity type', query: 'Which ontology entity type maps to the most tables? Show its key attributes and source tables.' },
    { label: 'Worst data quality table', query: 'Which table has the highest null rate or lowest completeness score? Show its most problematic columns.' },
    { label: 'Most cross-linked domain', query: 'Which business domain has the most relationships to other domains? Show the key cross-domain table connections.' },
    { label: 'Widest table by columns', query: 'Which table has the most columns? How many are classified as measures vs dimensions vs identifiers?' },
    { label: 'Catalog at a glance', query: 'Summarize the data catalog: total tables, top 3 domains by table count, and the densest relationship cluster.' },
    { label: 'Profile the most connected entity', query: 'Which entity type has the most relationships to other entities? List its columns classified as measures, dimensions, and identifiers.' },
    { label: 'Strongest cross-domain entity link', query: 'Find the pair of entity types from different business domains with the strongest relationship by confidence. Show the connecting edges and the key columns involved.' },
  ],
  baseline: [
    { label: 'Which table is the join hub?', query: 'Which table has the most foreign key relationships? Show its join keys and the tables it connects to.' },
    { label: 'Highest PII concentration', query: 'Which table has the most columns classified as PII or PHI? Show those columns and what other tables it connects to.' },
    { label: 'Catalog at a glance', query: 'Summarize the data catalog: total tables, top 3 domains by table count, and the densest relationship cluster.' },
    { label: 'Most cross-linked domain', query: 'Which business domain has the most relationships to other domains? Show the key cross-domain table connections.' },
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

function MessageBubble({ msg, onRetry, chart, plotLoading, onGeneratePlot }) {
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
  const isError = msg.content?.startsWith('I encountered an error') || msg.content?.startsWith('Deep analysis error')
  const showPlotButton = !isError && !chart && !plotLoading
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
          dangerouslySetInnerHTML={{ __html: simpleMarkdown(stripLeadingJson(msg.content)) }} />
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

        {chart && <ChartRenderer spec={chart} />}
        {chart?.no_data && (
          <p className="mt-2 text-xs text-slate-500 italic">{chart.reason}</p>
        )}

        {plotLoading && (
          <div className="mt-3 flex items-center gap-2 text-xs text-slate-400 dark:text-slate-500">
            <div className="w-3 h-3 border-2 border-emerald-500 dark:border-emerald-400 border-t-transparent rounded-full animate-spin" />
            Generating chart...
          </div>
        )}

        {showPlotButton && onGeneratePlot && (
          <button onClick={onGeneratePlot}
            className="mt-3 px-3 py-1.5 text-xs bg-slate-100 dark:bg-dbx-navy-500/50 text-slate-600 dark:text-slate-300 border border-slate-300 dark:border-dbx-navy-400/40 rounded-lg hover:bg-emerald-50 dark:hover:bg-emerald-500/20 hover:text-emerald-700 dark:hover:text-emerald-300 hover:border-emerald-300 dark:hover:border-emerald-500/30 transition-all flex items-center gap-1.5"
            title="Use an AI agent to generate a chart from this response">
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
            </svg>
            Generate Plot
          </button>
        )}

        {msg.tool_calls && msg.tool_calls.length > 0 && (
          <div className="mt-3 space-y-1.5">
            <div className="flex flex-wrap gap-1.5">
              {msg.tool_calls.map((tool, i) => (
                <span key={i} className="relative group text-xs px-2 py-1 bg-emerald-100 dark:bg-emerald-500/20 text-emerald-700 dark:text-emerald-300 rounded-md cursor-help">
                  {tool}
                  <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 px-2.5 py-1.5 bg-slate-800 text-slate-200 text-[11px] rounded-lg shadow-lg border border-slate-700/50 whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50">
                    {TOOL_DESCRIPTIONS[tool] || tool}
                  </span>
                </span>
              ))}
            </div>
            <div className="text-[11px] text-slate-500 leading-relaxed">
              {msg.tool_calls.map((tool, i) => (
                <span key={i}>{i > 0 && ' | '}<span className="text-slate-400 dark:text-slate-400">{tool}</span>: {TOOL_DESCRIPTIONS[tool] || 'Unknown tool'}</span>
              ))}
            </div>
          </div>
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
            <p className="font-semibold text-violet-600 dark:text-violet-400 mb-0.5">dbxmetagen Agent</p>
            <p>Two-phase analysis: Phase 1 gathers evidence deterministically via vector search, knowledge graph traversal, SQL on metadata tables, and structured data retrieval -- all in parallel with timeouts. Phase 2 passes the collected evidence to a single LLM synthesis call that produces findings with source citations.</p>
          </div>
          <div>
            <p className="font-semibold text-slate-700 dark:text-slate-300 mb-0.5">Data Sources</p>
            <p>Vector search finds semantically similar metadata. Graph traversal does multi-hop BFS with edge-type filtering via Lakebase PG (falls back to UC Delta). SQL queries cover knowledge bases, ontology entities, FK predictions, profiling results, and metric views.</p>
          </div>
          <div>
            <p className="font-semibold text-slate-700 dark:text-slate-300 mb-0.5">Grounding</p>
            <p>All findings must cite evidence sources. If gathered evidence is insufficient, the agent says so rather than guessing.</p>
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
  const [subTab, setSubTab] = useState('chat')
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [sessionId] = useState(() => crypto.randomUUID())
  const [stage, setStage] = useState(null)
  const [deepSteps, setDeepSteps] = useState([])
  const [deepMessage, setDeepMessage] = useState('')
  const [deepElapsedMs, setDeepElapsedMs] = useState(0)
  const [elapsedSec, setElapsedSec] = useState(0)
  const [streamedAnswer, setStreamedAnswer] = useState('')
  const [error, setError] = useState(null)
  const [stats, setStats] = useState(null)
  const [domainStats, setDomainStats] = useState(null)
  const [suggestions, setSuggestions] = useState([])
  const [mode, setMode] = useState('graphrag')
  const [chartData, setChartData] = useState({})
  const [plotLoading, setPlotLoading] = useState({})
  const chatEndRef = useRef(null)

  useEffect(() => {
    fetch('/api/agent/stats').then(r => r.ok ? r.json() : null).then(setStats).catch(() => {})
    fetch('/api/agent/suggestions').then(r => r.ok ? r.json() : []).then(setSuggestions).catch(() => {})
    fetch('/api/agent/domain-stats').then(r => r.ok ? r.json() : null).then(setDomainStats).catch(() => {})
  }, [])

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  useEffect(() => {
    if (!loading) { setElapsedSec(0); return }
    const t = setInterval(() => setElapsedSec(s => s + 1), 1000)
    return () => clearInterval(t)
  }, [loading])

  const sendDeepAnalysis = async (text, useMode) => {
    setStage('starting')
    setDeepSteps([])
    setDeepMessage('')
    setDeepElapsedMs(0)
    setStreamedAnswer('')

    const history = messages.filter(m => m.role !== 'error').map(m => ({ role: m.role, content: m.content }))

    // Try SSE stream first, fall back to polling on failure
    try {
      const res = await fetch('/api/agent/deep/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: text, history, mode: useMode, session_id: sessionId }),
      })
      if (!res.ok || !res.body) throw new Error('SSE not available')

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      let completed = false

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const parts = buffer.split('\n\n')
        buffer = parts.pop() || ''

        for (const part of parts) {
          if (!part.trim()) continue
          const lines = part.split('\n')
          let eventType = '', eventData = ''
          for (const line of lines) {
            if (line.startsWith('event: ')) eventType = line.slice(7)
            else if (line.startsWith('data: ')) eventData = line.slice(6)
          }
          if (!eventType || !eventData) continue

          try {
            const data = JSON.parse(eventData)
            if (eventType === 'stage') {
              setStage(data.stage)
              if (data.message) setDeepMessage(data.message)
            } else if (eventType === 'progress') {
              if (data.message) setDeepMessage(data.message)
              setDeepSteps(prev => [...prev, { stage: data.stage, message: data.message, ts: Date.now() / 1000 }])
            } else if (eventType === 'token') {
              setStreamedAnswer(prev => prev + (data.content || ''))
            } else if (eventType === 'done') {
              completed = true
              setMessages(prev => [...prev, {
                role: 'assistant',
                content: data.answer || '',
                tool_calls: data.tool_calls || [],
                intent: data.intent || null,
                mode: data.mode || useMode,
                graph_data: data.graph_data || null,
                trace_id: data.trace_id || null,
              }])
            } else if (eventType === 'error') {
              completed = true
              setMessages(prev => [...prev, { role: 'error', content: data.message || 'Analysis failed', _query: text, _mode: useMode }])
            }
          } catch { /* skip malformed event */ }
        }
      }
      if (!completed) {
        setMessages(prev => [...prev, { role: 'error', content: 'Stream ended without completion', _query: text, _mode: useMode }])
      }
      return
    } catch (sseErr) {
      // SSE failed, fall back to polling
      console.warn('SSE stream failed, falling back to polling:', sseErr.message)
    }

    // Fallback: submit/poll (legacy path)
    try {
      const res = await fetch('/api/agent/deep/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: text, history, mode: useMode, session_id: sessionId }),
      })
      if (!res.ok) {
        const data = await res.json().catch(() => ({}))
        setMessages(prev => [...prev, { role: 'error', content: data.detail || 'Submit failed', _query: text, _mode: useMode }])
        return
      }
      const { task_id, error: submitErr } = await res.json()
      if (submitErr || !task_id) {
        setMessages(prev => [...prev, { role: 'error', content: submitErr || 'No task ID returned', _query: text, _mode: useMode }])
        return
      }
      for (let i = 0; i < 300; i++) {
        await new Promise(r => setTimeout(r, 2000))
        const pollRes = await fetch(`/api/agent/deep/task/${task_id}`)
        if (!pollRes.ok) continue
        const task = await pollRes.json()
        if (task.status === 'done') {
          setMessages(prev => [...prev, {
            role: 'assistant', content: task.answer || '', tool_calls: task.tool_calls || [],
            intent: null, mode: task.mode || useMode, elapsed_ms: task.elapsed_ms,
            graph_data: task.graph_data || null,
          }])
          return
        }
        if (task.status === 'error') {
          setMessages(prev => [...prev, { role: 'error', content: task.error || 'Analysis failed', _query: text, _mode: useMode }])
          return
        }
        if (task.stage) setStage(task.stage)
        if (task.message) setDeepMessage(task.message)
        if (task.steps) setDeepSteps(task.steps)
        if (task.elapsed_ms != null) setDeepElapsedMs(task.elapsed_ms)
      }
      setMessages(prev => [...prev, { role: 'error', content: 'Analysis timed out after 10 minutes.', _query: text, _mode: useMode }])
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
        body: JSON.stringify({ message: text, history, mode: useMode, session_id: sessionId }),
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
    setDeepSteps([])
    setDeepMessage('')
    setDeepElapsedMs(0)
    setStreamedAnswer('')
  }

  const generatePlot = async (idx) => {
    const msg = messages[idx]
    if (!msg || msg.role === 'user') return
    setPlotLoading(prev => ({ ...prev, [idx]: true }))
    try {
      const history = messages.slice(0, idx + 1).map(m => ({ role: m.role, content: m.content }))
      const res = await fetch('/api/agent/plot', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ content: msg.content, history }),
      })
      const spec = await res.json()
      setChartData(prev => ({ ...prev, [idx]: spec }))
    } catch (err) {
      setChartData(prev => ({ ...prev, [idx]: { no_data: true, reason: err.message } }))
    } finally {
      setPlotLoading(prev => ({ ...prev, [idx]: false }))
    }
  }

  const clearChat = () => { setMessages([]); setError(null); setChartData({}); setPlotLoading({}) }
  const injectQuery = (q) => send(q)
  const cfg = MODE_CONFIG[mode] || MODE_CONFIG.quick

  return (
    <div className="min-h-[calc(100vh-12rem)]">
      <p className="text-xs text-slate-500 dark:text-slate-400 mb-3 max-w-3xl leading-relaxed">
        Graph Explorer and Search expect prior pipeline outputs (metadata jobs, graph, vector index). Chat is most useful after core metadata exists for your tables.
      </p>
      {/* Sub-tab bar: Chat | Graph Explorer | Search */}
      <div className="flex items-center gap-1 mb-3 border-b border-slate-200 dark:border-dbx-navy-400/40">
        {SUB_TABS.map(t => (
          <button key={t.id} onClick={() => setSubTab(t.id)}
            className={`px-4 py-2 text-sm font-medium transition-colors border-b-2 -mb-px ${
              subTab === t.id
                ? 'border-dbx-teal text-dbx-teal dark:text-teal-300'
                : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'
            }`}>{t.label}</button>
        ))}
      </div>

      {subTab === 'graph' ? (
        <Suspense fallback={<div className="text-sm text-slate-400 p-8">Loading Graph Explorer...</div>}>
          <GraphExplorer />
        </Suspense>
      ) : subTab === 'search' ? (
        <Suspense fallback={<div className="text-sm text-slate-400 p-8">Loading Search...</div>}>
          <VectorSearch />
        </Suspense>
      ) : (<>
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
              {messages.map((m, i) => {
                const chart = chartData[i] && !chartData[i].no_data ? chartData[i] : (chartData[i]?.no_data ? chartData[i] : null)
                return (
                  <MessageBubble key={i} msg={m}
                    onRetry={m.role === 'error' ? () => send(m._query, m._mode) : null}
                    chart={chart}
                    plotLoading={!!plotLoading[i]}
                    onGeneratePlot={() => generatePlot(i)} />
                )
              })}
              {loading && (
                <div className="flex justify-start mb-4 animate-slide-up">
                  <div className={`card px-4 py-3 text-sm text-slate-500 dark:text-slate-400 w-full ${streamedAnswer ? 'max-w-2xl' : 'max-w-md'}`}>
                    <div className="flex items-center gap-2.5 mb-1">
                      <span className="inline-block w-2 h-2 bg-dbx-lava rounded-full animate-pulse flex-shrink-0" />
                      <span className="font-medium text-slate-700 dark:text-slate-200">
                        {streamedAnswer ? 'Generating analysis...' : (deepMessage || STAGE_LABELS[stage] || 'Thinking...')}
                      </span>
                      {elapsedSec > 0 && (
                        <span className="ml-auto text-xs tabular-nums text-slate-400">
                          {Math.floor(elapsedSec / 60)}:{String(elapsedSec % 60).padStart(2, '0')}
                        </span>
                      )}
                    </div>
                    {streamedAnswer ? (
                      <div className="mt-2 prose prose-sm max-w-none whitespace-pre-wrap break-words text-slate-700 dark:text-slate-300"
                        dangerouslySetInnerHTML={{ __html: simpleMarkdown(stripLeadingJson(streamedAnswer)) }} />
                    ) : deepSteps.length > 0 ? (
                      <details className="mt-2">
                        <summary className="cursor-pointer text-xs text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 select-none">
                          {deepSteps.length} step{deepSteps.length !== 1 ? 's' : ''} completed
                        </summary>
                        <div className="mt-1.5 max-h-40 overflow-y-auto scrollbar-thin space-y-0.5 pl-1">
                          {deepSteps.map((s, i) => (
                            <div key={i} className="flex items-start gap-2 text-xs text-slate-400">
                              <span className="w-4 text-right tabular-nums flex-shrink-0">{i + 1}.</span>
                              <span className="truncate">{s.message || STAGE_LABELS[s.stage] || s.stage}</span>
                            </div>
                          ))}
                        </div>
                      </details>
                    ) : null}
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
      </>)}
    </div>
  )
}
