import React, { useState, useCallback, useRef } from 'react'
import { ErrorBanner } from '../App'
import simpleMarkdown from '../utils/simpleMarkdown'
import ChatInputBar from './ChatInputBar'
import SuggestedQuestions from './SuggestedQuestions'
import ChartRenderer from './ChartRenderer'

const MODE_CONFIG = {
  compare: { label: 'Compare (Blind vs Enriched)', color: 'bg-amber-600', desc: 'Run both side-by-side to see the value of the semantic layer' },
  enriched: { label: 'Enriched Only', color: 'bg-emerald-600', desc: 'Full semantic layer: KB, ontology, FK, profiling, metrics' },
  blind: { label: 'Blind Only', color: 'bg-slate-600', desc: 'Schema introspection only: DESCRIBE, SHOW, sample rows' },
}

const STAGE_LABELS = {
  starting: 'Starting both agents...',
  blind_running: 'Blind agent working...',
  enriched_running: 'Enriched agent working...',
  blind_done: 'Blind done, waiting for enriched...',
  enriched_done: 'Enriched done, waiting for blind...',
  comparing: 'Generating comparison analysis...',
}

const QUESTION_GROUPS = [
  {
    label: 'Semantic Disambiguation',
    desc: 'Business terms that don\'t match column names -- column comments, ontology roles, and metric definitions bridge the gap (enriched advantage)',
    questions: [
      'How has medication spending per care episode trended over the past year?',
      'What is the workforce-to-patient ratio across our care facilities, and which facilities are understaffed?',
      'Calculate the operational efficiency score by hospital using length of stay, cost per encounter, and ED throughput',
      'Show departments where emergency wait times exceed 30 minutes and staffing ratio is below average',
    ],
  },
  {
    label: 'Governance & Data Quality',
    desc: 'Sensitivity flags, documentation gaps, and profiling stats -- requires classification metadata and column-level context (enriched advantage)',
    questions: [
      'Which columns contain sensitive personal data but lack documentation or have high incompleteness?',
      'What are the PII/PHI exposure risks across the data catalog, and which tables need remediation?',
      'Show me the data quality score for encounter-related tables: null rates, completeness, and uniqueness',
    ],
  },
  {
    label: 'Multi-Table Aggregation',
    desc: 'Complex joins and rollups with explicit table references -- tests both agents\' ability to discover and combine data',
    questions: [
      'What are the top 10 accounts by total order value, broken down by product name?',
      'Show quarterly fund flow trends for the top 5 funds by total AUM, including their average return',
      'Show revenue concentration risk: which customers drive 80% of bookings?',
    ],
  },
]

function DataTable({ columns, rows }) {
  if (!columns?.length || !rows?.length) return null
  return (
    <div className="mb-3">
      <p className="text-xs font-semibold text-slate-500 mb-1">Result Data ({rows.length} rows):</p>
      <div className="max-h-64 overflow-auto rounded-lg border dark:border-slate-700">
        <table className="w-full text-xs">
          <thead className="bg-slate-100 dark:bg-slate-800 sticky top-0">
            <tr>
              {columns.map(c => (
                <th key={c} className="px-2 py-1.5 text-left font-semibold text-slate-600 dark:text-slate-300 whitespace-nowrap">{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i} className={i % 2 ? 'bg-slate-50 dark:bg-slate-800/30' : ''}>
                {columns.map(c => (
                  <td key={c} className="px-2 py-1 text-slate-600 dark:text-slate-400 whitespace-nowrap max-w-xs truncate">{row[c] ?? ''}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function ResultPanel({ title, color, result, loading, stage, chart, plotLoading, onGeneratePlot }) {
  if (loading) return (
    <div className="flex-1 min-w-0 border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50">
      <h3 className={`text-sm font-bold ${color} mb-3`}>{title}</h3>
      <div className="flex items-center gap-2 text-xs text-slate-500 animate-pulse">
        <div className="w-2 h-2 rounded-full bg-amber-400 animate-bounce" />
        {stage || 'Running...'}
      </div>
    </div>
  )
  if (!result) return (
    <div className="flex-1 min-w-0 border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50 opacity-50">
      <h3 className={`text-sm font-bold ${color} mb-3`}>{title}</h3>
      <p className="text-xs text-slate-400">Waiting for query...</p>
    </div>
  )
  const r = result.error ? null : result
  return (
    <div className="flex-1 min-w-0 border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50 overflow-hidden">
      <div className="flex items-center justify-between mb-3">
        <h3 className={`text-sm font-bold ${color}`}>{title}</h3>
        <div className="flex items-center gap-3">
          <TokenBadge usage={r?.token_usage} />
          {r?.elapsed_ms && <span className="text-xs text-slate-400">{(r.elapsed_ms / 1000).toFixed(1)}s</span>}
        </div>
      </div>
      {result.error ? (
        <div className="text-xs text-red-500 bg-red-50 dark:bg-red-900/20 rounded p-2">{result.error}</div>
      ) : (
        <>
          {r?.sql && (
            <div className="mb-3">
              <p className="text-xs font-semibold text-slate-500 mb-1">Generated SQL:</p>
              <pre className="bg-slate-100 dark:bg-slate-800 rounded-lg p-3 text-xs overflow-x-auto whitespace-pre-wrap">{r.sql}</pre>
            </div>
          )}
          <DataTable columns={r?.result_columns} rows={r?.result_rows} />
          {r?.answer && (
            <div className="text-sm text-slate-700 dark:text-slate-300 prose prose-sm max-w-none overflow-x-auto break-words"
              dangerouslySetInnerHTML={{ __html: simpleMarkdown(r.answer) }} />
          )}
          {chart && !chart.no_data && <ChartRenderer spec={chart} />}
          {chart?.no_data && (
            <p className="mt-2 text-xs text-slate-500 italic">{chart.reason}</p>
          )}
          {plotLoading && (
            <div className="mt-3 flex items-center gap-2 text-xs text-slate-400">
              <div className="w-3 h-3 border-2 border-teal-400 border-t-transparent rounded-full animate-spin" />
              Generating chart...
            </div>
          )}
          {r?.answer && !chart && !plotLoading && onGeneratePlot && (
            <button onClick={onGeneratePlot}
              className="mt-3 px-3 py-1.5 text-xs bg-slate-100 dark:bg-slate-700/50 text-slate-600 dark:text-slate-300 border border-slate-200 dark:border-slate-600/40 rounded-lg hover:bg-teal-50 dark:hover:bg-teal-500/20 hover:text-teal-700 dark:hover:text-teal-300 hover:border-teal-300 dark:hover:border-teal-500/30 transition-all flex items-center gap-1.5">
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
              Generate Plot
            </button>
          )}
          {r?.tool_calls?.length > 0 && (
            <div className="mt-3 pt-2 border-t">
              <p className="text-xs font-semibold text-slate-400 mb-1">Tools used ({r.tool_calls.length}):</p>
              <div className="flex flex-wrap gap-1">
                {r.tool_calls.map((t, i) => (
                  <span key={i} className="text-xs px-2 py-0.5 rounded-full bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300">{t}</span>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

function TokenBadge({ usage }) {
  if (!usage || !usage.total_tokens) return null
  return (
    <span className="text-xs text-slate-400" title={`In: ${(usage.input_tokens || 0).toLocaleString()} / Out: ${(usage.output_tokens || 0).toLocaleString()}`}>
      {(usage.total_tokens).toLocaleString()} tokens
    </span>
  )
}

export default function AnalystChat() {
  const [mode, setMode] = useState('compare')
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [stage, setStage] = useState('')
  const [compareResult, setCompareResult] = useState(null)
  const [singleResult, setSingleResult] = useState(null)
  const [history, setHistory] = useState([])
  const [chartData, setChartData] = useState({})
  const [plotLoading, setPlotLoading] = useState({})
  const abortRef = useRef(null)

  const generatePlot = useCallback(async (key, content, sql) => {
    setPlotLoading(prev => ({ ...prev, [key]: true }))
    try {
      const res = await fetch('/api/analyst/plot', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ content, sql }),
      })
      const spec = await res.json()
      setChartData(prev => ({ ...prev, [key]: spec }))
    } catch (err) {
      setChartData(prev => ({ ...prev, [key]: { no_data: true, reason: err.message } }))
    } finally {
      setPlotLoading(prev => ({ ...prev, [key]: false }))
    }
  }, [])

  async function handleSubmit(questionOverride) {
    const q = questionOverride || input
    if (!q.trim() || loading) return
    if (abortRef.current) abortRef.current.abort()
    const ctrl = new AbortController()
    abortRef.current = ctrl

    setError(null)
    setLoading(true)
    setStage('Starting...')
    setCompareResult(null)
    setSingleResult(null)
    setChartData({})
    setPlotLoading({})
    setInput('')
    setHistory(prev => [...prev, { role: 'user', content: q }])

    const body = mode === 'compare'
      ? { question: q, mode: 'compare' }
      : { question: q, mode, history }

    try {
      const res = await fetch('/api/analyst/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: ctrl.signal,
      })

      if (!res.ok) {
        const detail = await res.text()
        throw new Error(detail || `HTTP ${res.status}`)
      }

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const parts = buffer.split('\n\n')
        buffer = parts.pop()

        for (const part of parts) {
          const line = part.trim()
          if (!line.startsWith('data: ')) continue
          try {
            const evt = JSON.parse(line.slice(6))
            if (evt.event === 'stage') {
              setStage(STAGE_LABELS[evt.stage] || evt.stage || '')
            } else if (evt.event === 'partial' && mode === 'compare') {
              const modeKey = evt.mode
              setCompareResult(prev => ({ ...prev, [modeKey]: evt.result || { error: evt.error } }))
              setStage(STAGE_LABELS[`${modeKey}_done`] || `${modeKey} done...`)
            } else if (evt.event === 'done') {
              if (mode === 'compare') {
                setCompareResult(evt.result)
              } else {
                setSingleResult(evt.result)
                if (evt.result?.answer) setHistory(prev => [...prev, { role: 'assistant', content: evt.result.answer }])
              }
              setLoading(false)
            } else if (evt.event === 'error') {
              setError(evt.error || 'Unknown error')
              setLoading(false)
            }
          } catch (_) {}
        }
      }
      setLoading(false)
    } catch (e) {
      if (e.name !== 'AbortError') {
        setError(e.message)
        setLoading(false)
      }
    }
  }

  function clearChat() {
    if (abortRef.current) abortRef.current.abort()
    setHistory([]); setCompareResult(null); setSingleResult(null)
    setChartData({}); setPlotLoading({}); setError(null); setInput('')
    setLoading(false); setStage('')
  }

  const showCompare = mode === 'compare' && (compareResult || loading)
  const showSingle = mode !== 'compare' && singleResult

  return (
    <div className="max-w-7xl mx-auto p-4 space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-dbx-navy dark:text-white">SQL Analyst Agent Comparison</h2>
          <p className="text-xs text-slate-500 mt-0.5">Demonstrating the realizable value of a semantic metadata layer</p>
        </div>
        <div className="flex items-center gap-2">
          <div className="flex gap-1 bg-dbx-oat dark:bg-dbx-navy-500 rounded-lg p-1">
            {Object.entries(MODE_CONFIG).map(([k, v]) => (
              <button key={k} onClick={() => setMode(k)}
                className={`px-3 py-1.5 rounded-md text-xs font-medium transition-all ${mode === k ? `${v.color} text-white shadow` : 'text-slate-600 dark:text-slate-400 hover:bg-white/50'}`}>
                {v.label}
              </button>
            ))}
          </div>
          {(history.length > 0 || compareResult || singleResult) && (
            <button onClick={clearChat}
              className="px-3 py-1.5 rounded-md text-xs font-medium text-slate-500 hover:text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 transition-all border border-slate-200 dark:border-slate-600">
              Clear Chat
            </button>
          )}
        </div>
      </div>

      <ErrorBanner error={error} onDismiss={() => setError(null)} />

      <p className="text-xs text-slate-400">{MODE_CONFIG[mode].desc}</p>

      {!loading && !compareResult && !singleResult && (
        <div className="space-y-4">
          {QUESTION_GROUPS.map((group, gi) => (
            <div key={gi}>
              <div className="mb-2">
                <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300">{group.label}</h3>
                <p className="text-xs text-slate-400">{group.desc}</p>
              </div>
              <SuggestedQuestions questions={group.questions} onSelect={q => handleSubmit(q)} />
            </div>
          ))}
        </div>
      )}

      <ChatInputBar value={input} onChange={setInput} onSubmit={() => handleSubmit()}
        loading={loading} placeholder="Ask a question about your data..." />

      {loading && (
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <div className="w-2 h-2 rounded-full bg-amber-400 animate-bounce" />
          {stage}
        </div>
      )}

      {showCompare && (
        <div className="space-y-4">
          {compareResult?.comparison_analysis && (
            <div className="border rounded-xl p-4 bg-emerald-50/50 dark:bg-emerald-900/10">
              <h3 className="text-sm font-bold text-emerald-700 dark:text-emerald-400 mb-2">Semantic Layer Value Analysis</h3>
              <div className="text-sm text-slate-700 dark:text-slate-300 prose prose-sm max-w-none mb-3"
                dangerouslySetInnerHTML={{ __html: simpleMarkdown(compareResult.comparison_analysis) }} />
              <div className="flex flex-wrap gap-x-6 gap-y-1 text-xs text-slate-500 pt-2 border-t border-emerald-200/50 dark:border-emerald-800/30">
                <span>Time: Blind {compareResult.blind?.elapsed_ms ? `${(compareResult.blind.elapsed_ms/1000).toFixed(1)}s` : '?'} / Enriched {compareResult.enriched?.elapsed_ms ? `${(compareResult.enriched.elapsed_ms/1000).toFixed(1)}s` : '?'}</span>
                <span>Steps: Blind {compareResult.blind?.steps || '?'} / Enriched {compareResult.enriched?.steps || '?'}</span>
                <span>Tokens: Blind {compareResult.blind?.token_usage?.total_tokens?.toLocaleString() || '?'} / Enriched {compareResult.enriched?.token_usage?.total_tokens?.toLocaleString() || '?'}</span>
                <span>Semantic tools: {(compareResult.enriched?.tool_calls || []).filter(t =>
                  ['find_relevant_tables', 'get_join_path', 'get_column_context', 'get_metric_definitions', 'get_value_distribution'].includes(t)
                ).length}</span>
              </div>
            </div>
          )}
          <div className="flex gap-4">
            <ResultPanel title="Blind Mode (Schema Only)" color="text-slate-600"
              result={compareResult?.blind} loading={loading && !compareResult?.blind} stage={stage}
              chart={chartData.blind} plotLoading={plotLoading.blind}
              onGeneratePlot={compareResult?.blind?.answer ? () => generatePlot('blind', compareResult.blind.answer, compareResult.blind.sql) : undefined} />
            <ResultPanel title="Enriched Mode (Semantic Layer)" color="text-emerald-600"
              result={compareResult?.enriched} loading={loading && !compareResult?.enriched} stage={stage}
              chart={chartData.enriched} plotLoading={plotLoading.enriched}
              onGeneratePlot={compareResult?.enriched?.answer ? () => generatePlot('enriched', compareResult.enriched.answer, compareResult.enriched.sql) : undefined} />
          </div>
        </div>
      )}

      {showSingle && (
        <div className="space-y-3">
          <ResultPanel title={`${mode === 'enriched' ? 'Enriched' : 'Blind'} Result`}
            color={mode === 'enriched' ? 'text-emerald-600' : 'text-slate-600'}
            result={singleResult}
            chart={chartData.single} plotLoading={plotLoading.single}
            onGeneratePlot={() => generatePlot('single', singleResult?.answer, singleResult?.sql)} />
          {singleResult?.token_usage?.total_tokens > 0 && (
            <div className="text-xs text-slate-400 pl-1">
              Tokens: {singleResult.token_usage.total_tokens.toLocaleString()} (in: {(singleResult.token_usage.input_tokens || 0).toLocaleString()}, out: {(singleResult.token_usage.output_tokens || 0).toLocaleString()})
            </div>
          )}
        </div>
      )}
    </div>
  )
}
