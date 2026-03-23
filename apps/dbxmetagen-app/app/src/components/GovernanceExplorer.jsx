import React, { useState, useEffect, useRef } from 'react'
import { ErrorBanner } from '../App'
import ChatInputBar from './ChatInputBar'
import ChatMessageBubble from './ChatMessageBubble'
import SuggestedQuestions from './SuggestedQuestions'

const TYPE_COLORS = {
  phi: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
  pii: 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400',
  pci: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400',
}

function Badge({ type }) {
  return (
    <span className={`text-xs font-bold px-2 py-0.5 rounded-full uppercase ${TYPE_COLORS[type] || 'bg-slate-100 text-slate-600'}`}>
      {type}
    </span>
  )
}

function SummaryPanel({ data, loading }) {
  if (loading) return <div className="animate-pulse h-32 bg-slate-100 dark:bg-slate-800 rounded-xl" />
  if (!data?.length) return <p className="text-xs text-slate-400">No classifications found.</p>
  const grouped = {}
  data.forEach(r => {
    const key = r.schema || r.catalog || 'unknown'
    if (!grouped[key]) grouped[key] = {}
    grouped[key][r.classification_type] = (grouped[key][r.classification_type] || 0) + parseInt(r.column_count || 0)
  })
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead><tr className="border-b">
          <th className="text-left py-1.5 px-2 font-medium text-slate-500">Schema</th>
          <th className="text-center py-1.5 px-2 font-medium text-red-500">PHI</th>
          <th className="text-center py-1.5 px-2 font-medium text-orange-500">PII</th>
          <th className="text-center py-1.5 px-2 font-medium text-yellow-600">PCI</th>
        </tr></thead>
        <tbody>{Object.entries(grouped).map(([schema, counts]) => (
          <tr key={schema} className="border-b border-slate-100 dark:border-slate-700">
            <td className="py-1.5 px-2 font-medium text-slate-700 dark:text-slate-300">{schema}</td>
            <td className="text-center py-1.5 px-2">{counts.phi ? <span className="font-bold text-red-600">{counts.phi}</span> : <span className="text-slate-300">0</span>}</td>
            <td className="text-center py-1.5 px-2">{counts.pii ? <span className="font-bold text-orange-600">{counts.pii}</span> : <span className="text-slate-300">0</span>}</td>
            <td className="text-center py-1.5 px-2">{counts.pci ? <span className="font-bold text-yellow-600">{counts.pci}</span> : <span className="text-slate-300">0</span>}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  )
}

function GapsPanel({ data, loading }) {
  if (loading) return <div className="animate-pulse h-32 bg-slate-100 dark:bg-slate-800 rounded-xl" />
  if (!data?.length) return <p className="text-xs text-emerald-600">No classification gaps detected.</p>
  return (
    <div className="overflow-x-auto max-h-64 overflow-y-auto">
      <table className="w-full text-xs">
        <thead><tr className="border-b sticky top-0 bg-white dark:bg-dbx-navy-600">
          <th className="text-left py-1.5 px-2 font-medium text-slate-500">Table</th>
          <th className="text-left py-1.5 px-2 font-medium text-slate-500">Column</th>
          <th className="text-left py-1.5 px-2 font-medium text-slate-500">Pattern</th>
          <th className="text-right py-1.5 px-2 font-medium text-slate-500">Distinct</th>
          <th className="text-right py-1.5 px-2 font-medium text-slate-500">Null %</th>
        </tr></thead>
        <tbody>{data.map((r, i) => (
          <tr key={i} className="border-b border-slate-100 dark:border-slate-700">
            <td className="py-1.5 px-2 text-slate-700 dark:text-slate-300 truncate max-w-[150px]" title={r.table_name}>{r.table_name?.split('.').pop()}</td>
            <td className="py-1.5 px-2 font-mono text-slate-600 dark:text-slate-400">{r.column_name}</td>
            <td className="py-1.5 px-2"><span className="px-1.5 py-0.5 rounded bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400 text-xs">{r.pattern_detected}</span></td>
            <td className="text-right py-1.5 px-2 text-slate-600">{r.distinct_count}</td>
            <td className="text-right py-1.5 px-2 text-slate-600">{r.null_rate ? `${(parseFloat(r.null_rate)*100).toFixed(1)}%` : '-'}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  )
}

function MaskingPanel({ data, loading }) {
  if (loading) return <div className="animate-pulse h-32 bg-slate-100 dark:bg-slate-800 rounded-xl" />
  if (!data?.length) return <p className="text-xs text-emerald-600">All classified columns have masking policies.</p>
  const shown = data.slice(0, 50)
  return (
    <>
      <div className="overflow-x-auto max-h-48 overflow-y-auto">
        <table className="w-full text-xs">
          <thead><tr className="border-b sticky top-0 bg-white dark:bg-dbx-navy-600">
            <th className="text-left py-1.5 px-2 font-medium text-slate-500">Table</th>
            <th className="text-left py-1.5 px-2 font-medium text-slate-500">Column</th>
            <th className="text-left py-1.5 px-2 font-medium text-slate-500">Classification</th>
          </tr></thead>
          <tbody>{shown.map((r, i) => (
            <tr key={i} className="border-b border-slate-100 dark:border-slate-700">
              <td className="py-1.5 px-2 text-slate-700 dark:text-slate-300 truncate max-w-[150px]">{r.table_name?.split('.').pop()}</td>
              <td className="py-1.5 px-2 font-mono text-slate-600 dark:text-slate-400">{r.column_name}</td>
              <td className="py-1.5 px-2"><Badge type={r.classification_type} /></td>
            </tr>
          ))}</tbody>
        </table>
      </div>
      {data.length > 50 && (
        <p className="text-xs text-slate-400 mt-2">Showing 50 of {data.length} unmasked columns. Check Unity Catalog for the full list.</p>
      )}
    </>
  )
}

export default function GovernanceExplorer({ embedded }) {
  const [summary, setSummary] = useState(null)
  const [gaps, setGaps] = useState(null)
  const [masking, setMasking] = useState(null)
  const [loading, setLoading] = useState({ summary: true, gaps: true, masking: true })
  const [panelError, setPanelError] = useState({ summary: null, gaps: null, masking: null })
  const [error, setError] = useState(null)
  const [messages, setMessages] = useState([])
  const [chatInput, setChatInput] = useState('')
  const [chatLoading, setChatLoading] = useState(false)
  const chatEndRef = useRef(null)

  function loadPanel(endpoint, key, setter, extract) {
    setPanelError(p => ({ ...p, [key]: null }))
    setLoading(l => ({ ...l, [key]: true }))
    fetch(endpoint).then(r => r.json())
      .then(d => { setter(extract(d)); setLoading(l => ({ ...l, [key]: false })) })
      .catch(e => { setPanelError(p => ({ ...p, [key]: e.message })); setLoading(l => ({ ...l, [key]: false })) })
  }

  useEffect(() => {
    loadPanel('/api/governance/summary', 'summary', setSummary, d => d.summary || [])
    loadPanel('/api/governance/gaps', 'gaps', setGaps, d => d.gaps || [])
    loadPanel('/api/governance/masking', 'masking', setMasking, d => d.masking_audit || [])
  }, [])

  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [messages, chatLoading])

  async function handleChat(override) {
    const q = override || chatInput
    if (!q.trim() || chatLoading) return
    setChatInput('')
    setMessages(prev => [...prev, { role: 'user', content: q }])
    setChatLoading(true)
    try {
      const res = await fetch('/api/governance/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: q, history: messages }),
      })
      const data = await res.json()
      setMessages(prev => [...prev, { role: 'assistant', content: data.answer, tool_calls: data.tool_calls }])
    } catch (e) {
      setMessages(prev => [...prev, { role: 'error', content: e.message }])
    }
    setChatLoading(false)
  }

  return (
    <div className={embedded ? 'space-y-4' : 'max-w-7xl mx-auto p-4 space-y-4'}>
      {!embedded && (
        <div>
          <h2 className="text-xl font-bold text-dbx-navy dark:text-white">Governance & Compliance</h2>
          <p className="text-xs text-slate-500 mt-0.5">Sensitivity heatmap, classification gaps, protection audit, and governance Q&A</p>
        </div>
      )}

      <ErrorBanner error={error} onDismiss={() => setError(null)} />

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-bold text-slate-700 dark:text-slate-200">Sensitivity Heatmap</h3>
            <button onClick={() => handleChat('What are the most sensitive schemas and why?')} className="text-[10px] text-blue-500 hover:underline">Ask about this</button>
          </div>
          {panelError.summary
            ? <p className="text-xs text-red-500">Could not load summary. <button onClick={() => loadPanel('/api/governance/summary', 'summary', setSummary, d => d.summary || [])} className="underline">Retry</button></p>
            : <SummaryPanel data={summary} loading={loading.summary} />}
        </div>
        <div className="border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-bold text-amber-700 dark:text-amber-400">Classification Gaps</h3>
            <div className="flex items-center gap-2">
              {gaps && <span className="text-xs text-amber-500 font-bold">{gaps.length} found</span>}
              <button onClick={() => handleChat('Which classification gaps should I prioritize?')} className="text-[10px] text-blue-500 hover:underline">Ask about this</button>
            </div>
          </div>
          {panelError.gaps
            ? <p className="text-xs text-red-500">Could not load gaps. <button onClick={() => loadPanel('/api/governance/gaps', 'gaps', setGaps, d => d.gaps || [])} className="underline">Retry</button></p>
            : <GapsPanel data={gaps} loading={loading.gaps} />}
        </div>
        <div className="border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-bold text-red-700 dark:text-red-400">Unmasked Sensitive Columns</h3>
            <div className="flex items-center gap-2">
              {masking && <span className="text-xs text-red-500 font-bold">{masking.length} found</span>}
              <button onClick={() => handleChat('Which unmasked columns are highest risk?')} className="text-[10px] text-blue-500 hover:underline">Ask about this</button>
            </div>
          </div>
          {panelError.masking
            ? <p className="text-xs text-red-500">Could not load masking audit. <button onClick={() => loadPanel('/api/governance/masking', 'masking', setMasking, d => d.masking_audit || [])} className="underline">Retry</button></p>
            : <MaskingPanel data={masking} loading={loading.masking} />}
        </div>
      </div>

      <div className="border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50">
        <h3 className="text-sm font-bold text-slate-700 dark:text-slate-200 mb-3">Governance Chat</h3>
        <div className="space-y-3 max-h-80 overflow-y-auto mb-3">
          {messages.length === 0 && (
            <SuggestedQuestions
              questions={['Where is all our PHI data?', 'Which PII columns lack masking?', 'Show re-identification risks for Patient entities', 'Generate a HIPAA data inventory']}
              onSelect={handleChat} />
          )}
          {messages.map((msg, i) => (
            <ChatMessageBubble key={i} msg={msg} />
          ))}
          {chatLoading && <div className="text-xs text-slate-400 animate-pulse">Analyzing governance data...</div>}
          <div ref={chatEndRef} />
        </div>
        <ChatInputBar value={chatInput} onChange={setChatInput} onSubmit={() => handleChat()}
          loading={chatLoading} placeholder="Ask a governance question..." />
      </div>
    </div>
  )
}
