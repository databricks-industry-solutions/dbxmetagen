import React, { useState, useCallback, useRef } from 'react'
import { ErrorBanner } from '../App'
import simpleMarkdown from '../utils/simpleMarkdown'
import ChatInputBar from './ChatInputBar'
import SuggestedQuestions from './SuggestedQuestions'
import ChartRenderer from './ChartRenderer'
import { useLanguage } from '../contexts/LanguageContext'

// Display label/desc resolved via t() at render; keys are code values matched against the API.
const MODE_CONFIG = {
  compare: { labelKey: 'anc.mode.compare.label', color: 'bg-amber-600', descKey: 'anc.mode.compare.desc' },
  enriched: { labelKey: 'anc.mode.enriched.label', color: 'bg-emerald-600', descKey: 'anc.mode.enriched.desc' },
  blind: { labelKey: 'anc.mode.blind.label', color: 'bg-slate-600', descKey: 'anc.mode.blind.desc' },
}

// Stage labels resolved via t(`anc.stage.<stage>`) at render.

// Question strings are agent INPUT (sent to backend) -- kept English. Only label/desc are UI.
const QUESTION_GROUPS = [
  {
    labelKey: 'anc.group.semantic.label',
    descKey: 'anc.group.semantic.desc',
    questions: [
      'How has medication spending per care episode trended over the past year?',
      'What is the workforce-to-patient ratio across our care facilities, and which facilities are understaffed?',
      'Calculate the operational efficiency score by hospital using length of stay, cost per encounter, and ED throughput',
      'Show departments where emergency wait times exceed 30 minutes and staffing ratio is below average',
    ],
  },
  {
    labelKey: 'anc.group.governance.label',
    descKey: 'anc.group.governance.desc',
    questions: [
      'Which columns contain sensitive personal data but lack documentation or have high incompleteness?',
      'What are the PII/PHI exposure risks across the data catalog, and which tables need remediation?',
      'Show me the data quality score for encounter-related tables: null rates, completeness, and uniqueness',
    ],
  },
  {
    labelKey: 'anc.group.aggregation.label',
    descKey: 'anc.group.aggregation.desc',
    questions: [
      'What are the top 10 accounts by total order value, broken down by product name?',
      'Show quarterly fund flow trends for the top 5 funds by total AUM, including their average return',
      'Show revenue concentration risk: which customers drive 80% of bookings?',
    ],
  },
]

function DataTable({ columns, rows }) {
  const { t } = useLanguage()
  if (!columns?.length || !rows?.length) return null
  return (
    <div className="mb-3">
      <p className="text-xs font-semibold text-slate-500 mb-1">{t('anc.resultData', { count: rows.length })}</p>
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
  // Aliased to `tr` -- tool_calls.map uses `t` as the item param below.
  const { t: tr } = useLanguage()
  if (loading) return (
    <div className="flex-1 min-w-0 border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50">
      <h3 className={`text-sm font-bold ${color} mb-3`}>{title}</h3>
      <div className="flex items-center gap-2 text-xs text-slate-500 animate-pulse">
        <div className="w-2 h-2 rounded-full bg-amber-400 animate-bounce" />
        {stage || tr('anc.running')}
      </div>
    </div>
  )
  if (!result) return (
    <div className="flex-1 min-w-0 border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50 opacity-50">
      <h3 className={`text-sm font-bold ${color} mb-3`}>{title}</h3>
      <p className="text-xs text-slate-400">{tr('anc.waitingForQuery')}</p>
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
              <p className="text-xs font-semibold text-slate-500 mb-1">{tr('anc.generatedSql')}</p>
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
              {tr('anc.generatingChart')}
            </div>
          )}
          {r?.answer && !chart && !plotLoading && onGeneratePlot && (
            <button onClick={onGeneratePlot}
              className="mt-3 px-3 py-1.5 text-xs bg-slate-100 dark:bg-slate-700/50 text-slate-600 dark:text-slate-300 border border-slate-200 dark:border-slate-600/40 rounded-lg hover:bg-teal-50 dark:hover:bg-teal-500/20 hover:text-teal-700 dark:hover:text-teal-300 hover:border-teal-300 dark:hover:border-teal-500/30 transition-all flex items-center gap-1.5">
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
              {tr('anc.generatePlot')}
            </button>
          )}
          {r?.tool_calls?.length > 0 && (
            <div className="mt-3 pt-2 border-t">
              <p className="text-xs font-semibold text-slate-400 mb-1">{tr('anc.toolsUsed', { count: r.tool_calls.length })}</p>
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
  const { t } = useLanguage()
  if (!usage || !usage.total_tokens) return null
  return (
    <span className="text-xs text-slate-400" title={t('anc.tokenTooltip', { input: (usage.input_tokens || 0).toLocaleString(), output: (usage.output_tokens || 0).toLocaleString() })}>
      {t('anc.tokensLabel', { count: (usage.total_tokens).toLocaleString() })}
    </span>
  )
}

export default function AnalystChat() {
  // Aliased to `tr` -- line ~352 uses `t` as a `.filter(t => ...)` param.
  const { t: tr } = useLanguage()
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
    setStage(tr('anc.stage.starting'))
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
              const sk = `anc.stage.${evt.stage}`
              const sl = tr(sk)
              setStage(sl === sk ? (evt.stage || '') : sl)
            } else if (evt.event === 'partial' && mode === 'compare') {
              const modeKey = evt.mode
              setCompareResult(prev => ({ ...prev, [modeKey]: evt.result || { error: evt.error } }))
              const dk = `anc.stage.${modeKey}_done`
              const dl = tr(dk)
              setStage(dl === dk ? `${modeKey} done...` : dl)
            } else if (evt.event === 'done') {
              if (mode === 'compare') {
                setCompareResult(evt.result)
              } else {
                setSingleResult(evt.result)
                if (evt.result?.answer) setHistory(prev => [...prev, { role: 'assistant', content: evt.result.answer }])
              }
              setLoading(false)
            } else if (evt.event === 'error') {
              setError(evt.error || tr('anc.unknownError'))
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
          <h2 className="text-xl font-bold text-dbx-navy dark:text-white">{tr('anc.pageTitle')}</h2>
          <p className="text-xs text-slate-500 mt-0.5">{tr('anc.pageSubtitle')}</p>
        </div>
        <div className="flex items-center gap-2">
          <div className="flex gap-1 bg-dbx-oat dark:bg-dbx-navy-500 rounded-lg p-1">
            {Object.entries(MODE_CONFIG).map(([k, v]) => (
              <button key={k} onClick={() => setMode(k)}
                className={`px-3 py-1.5 rounded-md text-xs font-medium transition-all ${mode === k ? `${v.color} text-white shadow` : 'text-slate-600 dark:text-slate-400 hover:bg-white/50'}`}>
                {tr(v.labelKey)}
              </button>
            ))}
          </div>
          {(history.length > 0 || compareResult || singleResult) && (
            <button onClick={clearChat}
              className="px-3 py-1.5 rounded-md text-xs font-medium text-slate-500 hover:text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 transition-all border border-slate-200 dark:border-slate-600">
              {tr('anc.clearChat')}
            </button>
          )}
        </div>
      </div>

      <ErrorBanner error={error} onDismiss={() => setError(null)} />

      <p className="text-xs text-slate-400">{tr(MODE_CONFIG[mode].descKey)}</p>

      {!loading && !compareResult && !singleResult && (
        <div className="space-y-4">
          {QUESTION_GROUPS.map((group, gi) => (
            <div key={gi}>
              <div className="mb-2">
                <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300">{tr(group.labelKey)}</h3>
                <p className="text-xs text-slate-400">{tr(group.descKey)}</p>
              </div>
              <SuggestedQuestions questions={group.questions} onSelect={q => handleSubmit(q)} />
            </div>
          ))}
        </div>
      )}

      <ChatInputBar value={input} onChange={setInput} onSubmit={() => handleSubmit()}
        loading={loading} placeholder={tr('anc.inputPlaceholder')} />

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
              <h3 className="text-sm font-bold text-emerald-700 dark:text-emerald-400 mb-2">{tr('anc.valueAnalysisTitle')}</h3>
              <div className="text-sm text-slate-700 dark:text-slate-300 prose prose-sm max-w-none mb-3"
                dangerouslySetInnerHTML={{ __html: simpleMarkdown(compareResult.comparison_analysis) }} />
              <div className="flex flex-wrap gap-x-6 gap-y-1 text-xs text-slate-500 pt-2 border-t border-emerald-200/50 dark:border-emerald-800/30">
                <span>{tr('anc.metricTime', { blind: compareResult.blind?.elapsed_ms ? `${(compareResult.blind.elapsed_ms/1000).toFixed(1)}s` : '?', enriched: compareResult.enriched?.elapsed_ms ? `${(compareResult.enriched.elapsed_ms/1000).toFixed(1)}s` : '?' })}</span>
                <span>{tr('anc.metricSteps', { blind: compareResult.blind?.steps || '?', enriched: compareResult.enriched?.steps || '?' })}</span>
                <span>{tr('anc.metricTokens', { blind: compareResult.blind?.token_usage?.total_tokens?.toLocaleString() || '?', enriched: compareResult.enriched?.token_usage?.total_tokens?.toLocaleString() || '?' })}</span>
                <span>{tr('anc.metricSemanticTools', { count: (compareResult.enriched?.tool_calls || []).filter(t =>
                  ['find_relevant_tables', 'get_join_path', 'get_column_context', 'get_metric_definitions', 'get_value_distribution'].includes(t)
                ).length })}</span>
              </div>
            </div>
          )}
          <div className="flex gap-4">
            <ResultPanel title={tr('anc.panelBlindTitle')} color="text-slate-600"
              result={compareResult?.blind} loading={loading && !compareResult?.blind} stage={stage}
              chart={chartData.blind} plotLoading={plotLoading.blind}
              onGeneratePlot={compareResult?.blind?.answer ? () => generatePlot('blind', compareResult.blind.answer, compareResult.blind.sql) : undefined} />
            <ResultPanel title={tr('anc.panelEnrichedTitle')} color="text-emerald-600"
              result={compareResult?.enriched} loading={loading && !compareResult?.enriched} stage={stage}
              chart={chartData.enriched} plotLoading={plotLoading.enriched}
              onGeneratePlot={compareResult?.enriched?.answer ? () => generatePlot('enriched', compareResult.enriched.answer, compareResult.enriched.sql) : undefined} />
          </div>
        </div>
      )}

      {showSingle && (
        <div className="space-y-3">
          <ResultPanel title={tr(mode === 'enriched' ? 'anc.panelSingleEnriched' : 'anc.panelSingleBlind')}
            color={mode === 'enriched' ? 'text-emerald-600' : 'text-slate-600'}
            result={singleResult}
            chart={chartData.single} plotLoading={plotLoading.single}
            onGeneratePlot={() => generatePlot('single', singleResult?.answer, singleResult?.sql)} />
          {singleResult?.token_usage?.total_tokens > 0 && (
            <div className="text-xs text-slate-400 pl-1">
              {tr('anc.singleTokens', { total: singleResult.token_usage.total_tokens.toLocaleString(), input: (singleResult.token_usage.input_tokens || 0).toLocaleString(), output: (singleResult.token_usage.output_tokens || 0).toLocaleString() })}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
