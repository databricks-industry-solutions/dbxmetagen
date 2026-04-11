/**
 * Lightweight markdown-to-HTML converter for agent responses.
 * Supports fenced code blocks, GFM pipe tables, bold, inline code,
 * headings, lists, blockquotes, and line breaks.
 */

function parseTable(block) {
  const lines = block.split('\n').filter(l => l.trim())
  if (lines.length < 2) return null
  const sep = lines[1]
  if (!/^\|?[\s-:|]+\|?$/.test(sep)) return null

  const parseRow = (line) =>
    line.replace(/^\|/, '').replace(/\|$/, '').split('|').map(c => c.trim())

  const headers = parseRow(lines[0])
  const rows = lines.slice(2).map(parseRow)

  const th = headers.map(h => `<th class="px-3 py-1.5 text-left text-xs font-semibold text-slate-600 dark:text-slate-300 border-b border-slate-300 dark:border-slate-600">${h}</th>`).join('')
  const tbody = rows.map(r => {
    const tds = r.map(c => `<td class="px-3 py-1.5 text-xs text-slate-700 dark:text-slate-300 border-b border-slate-200 dark:border-slate-700">${c}</td>`).join('')
    return `<tr class="hover:bg-slate-50 dark:hover:bg-slate-800/40">${tds}</tr>`
  }).join('')
  return `<div class="my-3 overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-700"><table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700"><thead class="bg-slate-50 dark:bg-slate-800/60"><tr>${th}</tr></thead><tbody>${tbody}</tbody></table></div>`
}

export default function simpleMarkdown(text) {
  if (!text) return ''

  let s = text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')

  // Fenced code blocks (must run before table detection)
  s = s.replace(/```(\w*)\n?([\s\S]*?)```/g, '<pre class="bg-slate-100 dark:bg-slate-800 rounded-lg p-3 my-2 overflow-x-auto text-xs"><code>$2</code></pre>')

  // GFM pipe tables: find consecutive lines starting with |
  s = s.replace(/((?:^|\n)\|.+\|[ \t]*\n\|[\s:|-]+\|[ \t]*\n(?:\|.+\|[ \t]*\n?)+)/g, (match) => {
    const html = parseTable(match.trim())
    return html || match
  })

  // Blockquotes
  s = s.replace(/^&gt; (.+)$/gm, '<blockquote class="border-l-3 border-slate-300 dark:border-slate-600 pl-3 my-2 text-slate-500 dark:text-slate-400 italic text-xs">$1</blockquote>')

  // Inline formatting
  s = s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
  s = s.replace(/`([^`]+)`/g, '<code class="bg-dbx-oat dark:bg-dbx-navy-500 px-1.5 py-0.5 rounded text-xs font-mono">$1</code>')

  // Headings
  s = s.replace(/^### (.+)$/gm, '<h3 class="text-base font-semibold mt-3 mb-1">$1</h3>')
  s = s.replace(/^## (.+)$/gm, '<h2 class="text-lg font-semibold mt-3 mb-1">$1</h2>')

  // Numbered lists
  s = s.replace(/^\d+\. (.+)$/gm, '<li class="ml-4 list-decimal">$1</li>')
  // Bullet lists
  s = s.replace(/^- (.+)$/gm, '<li class="ml-4 list-disc">$1</li>')

  // Horizontal rules (--- on its own line)
  s = s.replace(/^---$/gm, '<hr class="my-3 border-slate-200 dark:border-slate-700" />')

  // Newlines
  s = s.replace(/\n\n/g, '<br/><br/>')
  s = s.replace(/\n/g, '<br/>')

  return s
}
