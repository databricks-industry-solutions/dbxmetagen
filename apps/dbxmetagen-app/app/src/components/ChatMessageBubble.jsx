import React from 'react'
import simpleMarkdown from '../utils/simpleMarkdown'

/**
 * Shared chat message bubble. Handles user, assistant, and error roles.
 *
 * Props:
 *   msg        - { role, content, tool_calls?, mode?, elapsed_ms?, intent? }
 *   modeBadge  - optional { label, className } for mode indicator
 *   onRetry    - if provided, shows a retry button on error messages
 *   size       - 'sm' (default) or 'xs' for compact follow-up chats
 *   toolDescriptions - optional map of tool name -> description string
 */
export default function ChatMessageBubble({ msg, modeBadge, onRetry, size = 'sm', toolDescriptions }) {
  const textSize = size === 'xs' ? 'text-xs' : 'text-sm'

  if (msg.role === 'user') {
    return (
      <div className="flex justify-end mb-4">
        <div className={`bg-gradient-to-br from-dbx-violet-dark to-dbx-violet text-white rounded-2xl px-4 py-3 max-w-[80%] ${textSize} whitespace-pre-wrap shadow-card`}>
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

  return (
    <div className="flex justify-start mb-4 animate-slide-up">
      <div className={`max-w-[85%] rounded-xl px-4 py-3 ${textSize} bg-dbx-oat dark:bg-dbx-navy-500 text-slate-700 dark:text-slate-300`}>
        <div className="flex items-center gap-2 mb-1">
          {modeBadge && <span className={`badge text-xs ${modeBadge.className}`}>{modeBadge.label}</span>}
          {msg.elapsed_ms != null && (
            <span className="text-[10px] text-slate-400 dark:text-slate-500 tabular-nums">
              {msg.elapsed_ms >= 1000 ? `${(msg.elapsed_ms / 1000).toFixed(1)}s` : `${msg.elapsed_ms}ms`}
            </span>
          )}
        </div>
        <div className="prose prose-sm max-w-none whitespace-pre-wrap break-words"
          dangerouslySetInnerHTML={{ __html: simpleMarkdown(msg.content) }} />
        {msg.tool_calls?.length > 0 && (
          <div className="mt-1 pt-1 border-t border-slate-200/30 flex flex-wrap gap-1">
            {msg.tool_calls.map((t, j) => (
              <span key={j} className="text-xs opacity-60" title={toolDescriptions?.[t] || ''}>
                {t}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
