import React, { useState, useEffect, useRef, useCallback } from 'react'
import { ErrorBanner } from '../App'
import simpleMarkdown from '../utils/simpleMarkdown'
import ChatInputBar from './ChatInputBar'
import ChatMessageBubble from './ChatMessageBubble'
import SuggestedQuestions from './SuggestedQuestions'
import useTaskPolling from '../hooks/useTaskPolling'

const SAMPLE_QUESTIONS = [
  'What if I drop the patient_id column from the encounters table?',
  'What happens if I rename the orders table?',
  'Impact of changing the diagnosis_code data type from string to int',
  'What if I deprecate the billing_summary table?',
]

export default function ImpactAnalysis({ embedded }) {
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [stage, setStage] = useState('')
  const [report, setReport] = useState(null)
  const [chatMessages, setChatMessages] = useState([])
  const [chatInput, setChatInput] = useState('')
  const [chatLoading, setChatLoading] = useState(false)
  const chatEndRef = useRef(null)

  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [chatMessages])

  const poller = useTaskPolling({
    onDone: useCallback(task => { setReport(task.result); setLoading(false) }, []),
    onError: useCallback(err => { setError(err); setLoading(false) }, []),
    onStage: useCallback(s => setStage(s), []),
    interval: 2500,
  })

  async function handleAnalyze(questionOverride) {
    const q = questionOverride || input
    if (!q.trim() || loading) return
    setError(null)
    setLoading(true)
    setStage('Starting impact analysis...')
    setReport(null)
    setChatMessages([])
    setInput('')

    try {
      const res = await fetch('/api/impact/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: q }),
      })
      const { task_id } = await res.json()
      poller.start(`/api/impact/task/${task_id}`)
    } catch (e) {
      setError(e.message)
      setLoading(false)
    }
  }

  async function handleChat() {
    if (!chatInput.trim() || chatLoading) return
    const q = chatInput
    setChatInput('')
    setChatMessages(prev => [...prev, { role: 'user', content: q }])
    setChatLoading(true)
    try {
      const res = await fetch('/api/impact/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: q,
          history: [...chatMessages, { role: 'user', content: q }],
        }),
      })
      const data = await res.json()
      setChatMessages(prev => [...prev, { role: 'assistant', content: data.answer }])
    } catch (e) {
      setChatMessages(prev => [...prev, { role: 'error', content: e.message }])
    }
    setChatLoading(false)
  }

  return (
    <div className={embedded ? 'space-y-4' : 'max-w-6xl mx-auto p-4 space-y-4'}>
      {!embedded && (
        <div>
          <h2 className="text-xl font-bold text-dbx-navy dark:text-white">Impact Analysis</h2>
          <p className="text-xs text-slate-500 mt-0.5">What-if analysis for schema changes using the full semantic layer</p>
        </div>
      )}

      <ErrorBanner error={error} onDismiss={() => setError(null)} />

      <ChatInputBar value={input} onChange={setInput} onSubmit={() => handleAnalyze()}
        loading={loading} placeholder="What if I change...?"
        buttonLabel={loading ? 'Analyzing...' : 'Analyze Impact'} />

      {!loading && !report && (
        <SuggestedQuestions questions={SAMPLE_QUESTIONS} onSelect={q => handleAnalyze(q)} />
      )}

      {loading && (
        <div className="border rounded-xl p-6 bg-white dark:bg-dbx-navy-600/50 text-center">
          <div className="w-8 h-8 border-2 border-dbx-teal border-t-transparent rounded-full animate-spin mx-auto mb-3" />
          <p className="text-sm text-slate-500">{stage}</p>
          <p className="text-xs text-slate-400 mt-1">The supervisor agent is coordinating identify, trace, and assess steps...</p>
        </div>
      )}

      {report && (
        <div className="space-y-4">
          {report.target && (
            <div className="border rounded-xl p-4 bg-slate-50 dark:bg-dbx-navy-600/50">
              <h3 className="text-sm font-bold text-slate-700 dark:text-slate-200 mb-1">Target Identified</h3>
              <p className="text-xs text-slate-600 dark:text-slate-400">{report.target}</p>
              <p className="text-xs text-slate-400 mt-1">Supervisor steps: {report.steps}</p>
            </div>
          )}

          <div className="border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50">
            <h3 className="text-sm font-bold text-dbx-navy dark:text-white mb-3">Impact Report</h3>
            <div className="text-sm text-slate-700 dark:text-slate-300 prose prose-sm max-w-none"
              dangerouslySetInnerHTML={{ __html: simpleMarkdown(report.answer) }} />
          </div>

          <div className="border rounded-xl p-4 bg-white dark:bg-dbx-navy-600/50">
            <h3 className="text-sm font-bold text-slate-700 dark:text-slate-200 mb-3">Follow-up Questions</h3>
            <div className="space-y-2 max-h-48 overflow-y-auto mb-3">
              {chatMessages.map((msg, i) => (
                <ChatMessageBubble key={i} msg={msg} size="xs" />
              ))}
              {chatLoading && <div className="text-xs text-slate-400 animate-pulse">Analyzing...</div>}
              <div ref={chatEndRef} />
            </div>
            <ChatInputBar value={chatInput} onChange={setChatInput} onSubmit={handleChat}
              loading={chatLoading} placeholder="Ask a follow-up about this impact..." />
          </div>
        </div>
      )}
    </div>
  )
}
