import React from 'react'
import { useLanguage } from '../contexts/LanguageContext'

export default function ChatInputBar({
  value, onChange, onSubmit, loading,
  placeholder,
  buttonLabel,
  buttonColor = 'bg-dbx-orange',
  onClear,
}) {
  const { t } = useLanguage()
  const ph = placeholder ?? t('chat.askPlaceholder')
  const label = buttonLabel ?? t('chat.ask')
  return (
    <div className="flex gap-2 items-center">
      <input
        value={value}
        onChange={e => onChange(e.target.value)}
        onKeyDown={e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); onSubmit() } }}
        placeholder={ph}
        disabled={loading}
        className="flex-1 px-4 py-2.5 rounded-xl border text-sm bg-white dark:bg-dbx-navy-600 focus:ring-2 focus:ring-dbx-teal outline-none"
      />
      <button onClick={onSubmit} disabled={loading || !value?.trim()}
        className={`px-5 py-2.5 rounded-xl ${buttonColor} hover:opacity-90 text-white text-sm font-medium disabled:opacity-50 transition-all`}>
        {loading ? t('chat.running') : label}
      </button>
      {onClear && (
        <button onClick={onClear} title={t('chat.clear')}
          className="btn-ghost p-2.5 rounded-lg flex-shrink-0">
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
          </svg>
        </button>
      )}
    </div>
  )
}
