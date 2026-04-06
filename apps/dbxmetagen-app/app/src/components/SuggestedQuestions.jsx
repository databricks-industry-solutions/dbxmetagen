import React from 'react'

/**
 * Grid of clickable suggestion buttons.
 *
 * Props:
 *   questions - array of strings or { label, query } objects
 *   onSelect  - called with the query string when clicked
 *   columns   - grid columns (default 2)
 */
const GRID_COLS = { 1: 'grid-cols-1', 2: 'grid-cols-2', 3: 'grid-cols-3', 4: 'grid-cols-4' }

export default function SuggestedQuestions({ questions, onSelect, columns = 2 }) {
  if (!questions?.length) return null
  return (
    <div className={`grid gap-2 ${GRID_COLS[columns] || 'grid-cols-2'}`}>
      {questions.map((q, i) => {
        const label = typeof q === 'string' ? q : q.label
        const query = typeof q === 'string' ? q : q.query
        return (
          <button key={i} onClick={() => onSelect(query)}
            className="text-left text-xs p-3 rounded-lg border hover:border-dbx-teal hover:bg-dbx-oat/50 dark:hover:bg-dbx-navy-500/50 transition-all text-slate-600 dark:text-slate-400">
            {label}
          </button>
        )
      })}
    </div>
  )
}
