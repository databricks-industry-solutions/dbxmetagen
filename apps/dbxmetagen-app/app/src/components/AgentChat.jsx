import React, { useState, useEffect, useRef } from 'react'
import { ErrorBanner } from '../App'

const TOOL_DESCRIPTIONS = {
  search_metadata: 'Semantic search over indexed metadata documents',
  execute_metadata_sql: 'SQL query against knowledge base tables',
  get_table_summary: 'Full summary of a specific table',
  get_data_quality: 'Profiling stats and quality scores',
  query_graph_nodes: 'Query the knowledge graph nodes',
  get_node_details: 'Get details for a specific graph node',
  find_similar_nodes: 'Find similar nodes via graph embeddings',
  traverse_graph: 'Traverse graph relationships',
}

const INTENT_LABELS = {
  discovery: 'Discovery -- semantic search + table summary',
  query: 'Query -- SQL aggregation + table lookup',
  relationship: 'Relationship -- FK + graph traversal',
  governance: 'Governance -- PII/PHI + quality analysis',
  general: 'General -- full tool suite',
}

const STAGE_LABELS = {
  starting: 'Starting...',
  routing: 'Classifying intent...',
  searching: 'Searching metadata...',
  analyzing: 'Analyzing results...',
  responding: 'Preparing response...',
}

function StatCard({ label, value, sub, gradient, onClick }) {
  return (
    <div onClick={onClick}
      className={`rounded-xl p-3.5 border bg-gradient-to-br ${gradient} ${onClick ? 'cursor-pointer hover:scale-[1.02] transition-transform' : ''}`}>
      <p className="text-[10px] font-medium uppercase tracking-wider text-gray-500 mb-0.5">{label}</p>
      <p className="text-xl font-bold text-dbx-lava">{value ?? '--'}</p>
      {sub && <p className="text-[10px] text-gray-400 mt-0.5 truncate">{sub}</p>}
    </div>
  )
}

function AgentReasoning({ intent, toolCalls }) {
  const [open, setOpen] = useState(false)
  if (!intent && (!toolCalls || toolCalls.length === 0)) return null
  return (
    <div className="mt-2">
      <button onClick={() => setOpen(!open)}
        className="text-[11px] text-gray-400 hover:text-dbx-navy transition-colors flex items-center gap-1">
        <span className="font-mono">{open ? '\u25B4' : '\u25BE'}</span>
        {open ? 'Hide' : 'Show'} agent reasoning
        {toolCalls?.length > 0 && <span className="text-gray-300 ml-1">({toolCalls.length} tools)</span>}
      </button>
      {open && (
        <div className="mt-1.5 px-3 py-2 bg-dbx-oat border rounded-lg text-xs space-y-1.5">
          {intent && (
            <div>
              <span className="text-gray-400">Intent:</span>{' '}
              <span className="font-mono text-dbx-navy">{intent}</span>
              <span className="text-gray-400 ml-2">{INTENT_LABELS[intent] || ''}</span>
            </div>
          )}
          {toolCalls?.length > 0 && (
            <div className="space-y-0.5">
              <span className="text-gray-400">Tools used:</span>
              {toolCalls.map((t, i) => (
                <div key={i} className="flex items-baseline gap-2 ml-3">
                  <span className="font-mono text-emerald-700">{t}</span>
                  <span className="text-gray-400">{TOOL_DESCRIPTIONS[t] || ''}</span>
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
      <div className="flex justify-end mb-3">
        <div className="bg-dbx-navy text-white rounded-2xl rounded-br-sm px-4 py-2.5 max-w-[80%] text-sm whitespace-pre-wrap">
          {msg.content}
        </div>
      </div>
    )
  }
  if (msg.role === 'error') {
    return (
      <div className="flex justify-start mb-3">
        <div className="bg-red-50 border border-red-200 rounded-2xl rounded-bl-sm px-4 py-2.5 max-w-[85%] text-sm">
          <p className="text-red-700">{msg.content}</p>
          {onRetry && (
            <button onClick={onRetry} className="mt-2 text-xs text-red-600 hover:text-red-800 underline">
              Retry this question
            </button>
          )}
        </div>
      </div>
    )
  }
  return (
    <div className="flex justify-start mb-3">
      <div className="bg-dbx-oat-light border rounded-2xl rounded-bl-sm px-4 py-2.5 max-w-[85%] text-sm overflow-hidden">
        {msg.mode && (
          <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-medium mb-1.5 ${msg.mode === 'deep' ? 'bg-violet-100 text-violet-700' : 'bg-blue-100 text-blue-700'}`}>
            {msg.mode === 'deep' ? 'Deep Analysis' : 'Quick Query'}
          </span>
        )}
        <div className="prose prose-sm max-w-none whitespace-pre-wrap break-words"
          dangerouslySetInnerHTML={{ __html: simpleMarkdown(msg.content) }} />
        <AgentReasoning intent={msg.intent} toolCalls={msg.tool_calls} />
      </div>
    </div>
  )
}

function simpleMarkdown(text) {
  if (!text) return ''
  return text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/`([^`]+)`/g, '<code class="bg-dbx-oat px-1 rounded text-xs">$1</code>')
    .replace(/^### (.+)$/gm, '<h3 class="text-base font-semibold mt-3 mb-1">$1</h3>')
    .replace(/^## (.+)$/gm, '<h2 class="text-lg font-semibold mt-3 mb-1">$1</h2>')
    .replace(/^- (.+)$/gm, '<li class="ml-4 list-disc">$1</li>')
    .replace(/\n\n/g, '<br/><br/>')
    .replace(/\n/g, '<br/>')
}

function DomainItem({ label, count, onClick }) {
  return (
    <button onClick={onClick}
      className="flex items-center justify-between w-full px-2.5 py-1.5 rounded hover:bg-dbx-navy/5 transition-colors text-left">
      <span className="text-xs text-gray-700 truncate">{label}</span>
      <span className="text-xs font-semibold text-dbx-navy ml-2 flex-shrink-0">{count}</span>
    </button>
  )
}

const GRAPH_QUESTIONS = [
  "What tables contain PII data?",
  "Which tables are most similar to each other?",
  "Show me the lineage for this catalog",
  "What columns might be foreign keys?",
]

const AGENT_TYPES = [
  { key: 'metadata', label: 'Metadata Intelligence', desc: 'SQL, vector search, and knowledge base analysis' },
  { key: 'graph', label: 'Graph Explorer', desc: 'Traverse knowledge graph relationships' },
]

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
  const [agentType, setAgentType] = useState('metadata')
  const chatEndRef = useRef(null)

  useEffect(() => {
    fetch('/api/agent/stats').then(r => r.ok ? r.json() : null).then(setStats).catch(() => {})
    fetch('/api/agent/suggestions').then(r => r.ok ? r.json() : []).then(setSuggestions).catch(() => {})
    fetch('/api/agent/domain-stats').then(r => r.ok ? r.json() : null).then(setDomainStats).catch(() => {})
  }, [])

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  const send = async (text, retryMode) => {
    if (!text.trim()) return
    const useMode = retryMode || mode
    const userMsg = { role: 'user', content: text.trim() }
    setMessages(prev => [...prev, userMsg])
    setInput('')
    setLoading(true)
    setError(null)

    if (agentType === 'graph') {
      setStage('searching')
      try {
        const res = await fetch('/api/graph/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question: text.trim(), max_hops: 3 }),
        })
        setStage('responding')
        const data = await res.json()
        if (!res.ok) {
          setMessages(prev => [...prev, { role: 'error', content: data.detail || 'Graph query failed', _query: text.trim(), _mode: useMode }])
        } else {
          setMessages(prev => [...prev, {
            role: 'assistant',
            content: data.answer,
            tool_calls: [],
            intent: 'graph',
            mode: 'graph',
          }])
        }
      } catch (e) {
        setMessages(prev => [...prev, { role: 'error', content: e.message, _query: text.trim(), _mode: useMode }])
      }
    } else {
      setStage('routing')
      const history = messages.filter(m => m.role !== 'error').map(m => ({ role: m.role, content: m.content }))
      try {
        setStage('searching')
        const res = await fetch('/api/agent/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message: text.trim(), history, mode: useMode }),
        })
        setStage('responding')
        const data = await res.json()
        if (!res.ok) {
          setMessages(prev => [...prev, { role: 'error', content: data.detail || 'Agent request failed', _query: text.trim(), _mode: useMode }])
        } else {
          setMessages(prev => [...prev, {
            role: 'assistant',
            content: data.answer,
            tool_calls: data.tool_calls || [],
            intent: data.intent,
            mode: data.mode || useMode,
          }])
        }
      } catch (e) {
        setMessages(prev => [...prev, { role: 'error', content: e.message, _query: text.trim(), _mode: useMode }])
      }
    }
    setLoading(false)
    setStage(null)
  }

  const handleKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      send(input)
    }
  }

  const clearChat = () => {
    setMessages([])
    setError(null)
  }

  const injectQuery = (q) => send(q)

  return (
    <div className="flex gap-4" style={{ minHeight: 'calc(100vh - 200px)' }}>
      <ErrorBanner error={error} />

      {/* Left: Chat */}
      <div className="flex-1 flex flex-col min-w-0" style={{ flexBasis: '65%' }}>
        {/* Agent type selector */}
        <div className="flex gap-2 mb-3">
          {AGENT_TYPES.map(a => (
            <button key={a.key} onClick={() => { setAgentType(a.key); setMessages([]); setError(null) }}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${agentType === a.key ? 'bg-dbx-navy text-white shadow-sm' : 'bg-dbx-oat text-slate-600 hover:bg-dbx-oat-dark'}`}
              title={a.desc}>{a.label}</button>
          ))}
        </div>
        {/* Chat area */}
        <div className="flex-1 bg-dbx-oat-light/40 rounded-xl border p-4 overflow-y-auto mb-3" style={{ maxHeight: '65vh' }}>
          {messages.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full text-center py-10">
              <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-dbx-navy to-dbx-navy/70 flex items-center justify-center mb-3">
                <svg className="w-6 h-6 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                </svg>
              </div>
              <h2 className="text-lg font-semibold text-dbx-navy mb-1">
                {agentType === 'graph' ? 'Graph Explorer Agent' : 'Metadata Intelligence Agent'}
              </h2>
              <p className="text-xs text-gray-500 mb-5 max-w-md">
                {agentType === 'graph'
                  ? 'Traverse knowledge graph relationships to discover patterns in your data catalog.'
                  : 'Ask about tables, columns, relationships, entity types, data quality, and metric views.'}
              </p>
              <div className="flex flex-wrap gap-2 justify-center max-w-lg">
                {agentType === 'graph'
                  ? GRAPH_QUESTIONS.map((q, i) => (
                    <button key={i} onClick={() => send(q)}
                      className="px-3 py-1.5 bg-purple-50 text-purple-700 border border-purple-200 rounded-full text-xs hover:bg-purple-100 transition-colors">
                      {q}
                    </button>
                  ))
                  : suggestions.map((s, i) => (
                    <button key={i} onClick={() => send(s.query)}
                      className="px-3 py-1.5 bg-dbx-oat-light border border-gray-200 rounded-full text-xs text-gray-600 hover:bg-dbx-navy/5 hover:border-dbx-navy/30 transition-colors">
                      {s.label}
                    </button>
                  ))
                }
              </div>
            </div>
          ) : (
            <>
              {messages.map((m, i) => (
                <MessageBubble key={i} msg={m}
                  onRetry={m.role === 'error' ? () => send(m._query, m._mode) : null} />
              ))}
              {loading && (
                <div className="flex justify-start mb-3">
                  <div className="bg-dbx-oat-light border rounded-2xl rounded-bl-sm px-4 py-3 text-sm text-gray-400 flex items-center gap-2">
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
        <div className="flex gap-2 items-center">
          {/* Mode toggle */}
          <div className="flex rounded-lg border overflow-hidden flex-shrink-0">
            <button onClick={() => setMode('quick')}
              className={`px-3 py-2 text-xs font-medium transition-colors ${mode === 'quick' ? 'bg-dbx-navy text-white' : 'bg-dbx-oat-light text-gray-500 hover:bg-dbx-oat'}`}>
              Quick
            </button>
            <button onClick={() => setMode('deep')}
              className={`px-3 py-2 text-xs font-medium transition-colors ${mode === 'deep' ? 'bg-violet-600 text-white' : 'bg-dbx-oat-light text-gray-500 hover:bg-dbx-oat'}`}>
              Deep
            </button>
          </div>
          <input
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKey}
            placeholder={mode === 'deep' ? 'Ask for thorough analysis...' : 'Ask about your data catalog...'}
            disabled={loading}
            className="flex-1 border rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-dbx-navy/30 disabled:opacity-50"
          />
          <button onClick={() => send(input)} disabled={loading || !input.trim()}
            className={`px-4 py-2.5 text-white rounded-lg text-sm font-medium disabled:opacity-50 transition-colors ${mode === 'deep' ? 'bg-violet-600 hover:bg-violet-700' : 'bg-dbx-navy hover:bg-dbx-navy/90'}`}>
            Send
          </button>
          {messages.length > 0 && (
            <button onClick={clearChat} title="Clear chat"
              className="px-2.5 py-2.5 text-gray-400 hover:text-gray-600 rounded-lg hover:bg-dbx-oat-dark transition-colors flex-shrink-0">
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
            </button>
          )}
        </div>
      </div>

      {/* Right: Stats Panel */}
      <div className="flex-shrink-0 overflow-y-auto space-y-3" style={{ width: '320px', maxHeight: 'calc(100vh - 200px)' }}>
        {/* Stat tiles */}
        {stats && (
          <div className="grid grid-cols-2 gap-2.5">
            <StatCard label="Tables" value={stats.tables_profiled}
              gradient="from-blue-500/15 to-blue-600/5 border-blue-200"
              onClick={() => injectQuery('What tables exist in my catalog?')} />
            <StatCard label="Entity Types" value={stats.entity_types}
              gradient="from-violet-500/15 to-violet-600/5 border-violet-200"
              onClick={() => injectQuery('What entity types were discovered?')} />
            <StatCard label="FK Relations" value={stats.fk_predictions}
              gradient="from-emerald-500/15 to-emerald-600/5 border-emerald-200"
              onClick={() => injectQuery('Show me foreign key relationships')} />
            <StatCard label="Metric Views" value={stats.metric_views}
              gradient="from-amber-500/15 to-amber-600/5 border-amber-200"
              onClick={() => injectQuery('What metric views are available?')} />
            <StatCard label="VS Documents" value={stats.vs_documents}
              gradient="from-rose-500/15 to-rose-600/5 border-rose-200"
              sub={stats.vs_by_type ? Object.entries(stats.vs_by_type).map(([k,v]) => `${k}: ${v}`).join(', ') : ''} />
            <StatCard label="Quality" value={stats.avg_quality ?? '--'}
              gradient="from-teal-500/15 to-teal-600/5 border-teal-200"
              onClick={() => injectQuery('Show me the data quality summary')} />
          </div>
        )}

        {/* Domain breakdown */}
        {domainStats?.tables_by_domain?.length > 0 && (
          <div className="bg-dbx-oat-light rounded-xl border p-3">
            <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Tables by Domain</h3>
            <div className="space-y-0.5 max-h-40 overflow-y-auto">
              {domainStats.tables_by_domain.map((d, i) => (
                <DomainItem key={i} label={d.domain || 'unknown'} count={d.count}
                  onClick={() => injectQuery(`Describe the ${d.domain} domain tables`)} />
              ))}
            </div>
          </div>
        )}

        {/* Entity types */}
        {domainStats?.entities_by_type?.length > 0 && (
          <div className="bg-dbx-oat-light rounded-xl border p-3">
            <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Entity Types</h3>
            <div className="space-y-0.5 max-h-40 overflow-y-auto">
              {domainStats.entities_by_type.map((e, i) => (
                <DomainItem key={i} label={e.type || 'unknown'} count={e.count}
                  onClick={() => injectQuery(`Tell me about the ${e.type} entity type`)} />
              ))}
            </div>
          </div>
        )}

        {/* FK by domain */}
        {domainStats?.fk_by_domain?.length > 0 && (
          <div className="bg-dbx-oat-light rounded-xl border p-3">
            <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">FK Relations by Domain</h3>
            <div className="space-y-0.5 max-h-32 overflow-y-auto">
              {domainStats.fk_by_domain.map((d, i) => (
                <DomainItem key={i} label={d.domain || 'unknown'} count={d.count}
                  onClick={() => injectQuery(`Show foreign key relationships in the ${d.domain} domain`)} />
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
