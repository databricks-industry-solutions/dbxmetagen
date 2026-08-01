// Pure helpers for Genie space external-edit ("drift") detection.
//
// Extracted from GenieUpdater.jsx so they can be unit-tested without a React /
// bundler test harness (see genieDrift.test.mjs, runnable with plain `node`).
//
// The Genie API and build_serialized_space() regenerate volatile fields on every
// write -- random hex `id`s and a `--rt=...` relationship-type marker appended to
// join SQL predicates. Two logically identical serialized_space objects therefore
// never compare byte-equal. canonicalize() strips those volatile fields and sorts
// object keys so a genuine external edit can be told apart from mere re-serialization.

// Keys whose array VALUE is user-ordered and meaningful: a steward's manual
// reordering of these IS a real external edit, so we must NOT sort them away.
// Everything else stays order-independent (the Genie API's proto->JSON is not
// order-stable across GETs, so sorting prevents false "modified externally"
// conflicts -- the original reason canonicalize sorts arrays).
const ORDER_SENSITIVE_KEYS = new Set(['sample_questions', 'example_question_sqls'])

// Recursively normalize a serialized_space value so two serializations of the
// same logical space compare equal. Rules:
//   - object keys are sorted, and volatile `id` (per-write random hex) is dropped
//   - the internal `--rt=...--` join-relationship marker is stripped from strings
//   - array elements are canonicalized, then any that reduce to an empty string
//     (e.g. a standalone marker entry) are dropped; the array is sorted by
//     canonical content EXCEPT under an order-sensitive key, where original order
//     is preserved so a steward's reordering is detected as drift.
// `key` is the object key this value was reached under (undefined at the root).
export function canonicalize(value, key) {
  if (Array.isArray(value)) {
    const items = value
      .map(v => canonicalize(v))
      .filter(v => v !== '') // drop marker-only / emptied entries
    if (ORDER_SENSITIVE_KEYS.has(key)) {
      return items // preserve order: reordering here is a genuine edit
    }
    // Order-independent: sort by a stable serialization of each element.
    return items
      .map(v => [JSON.stringify(v), v])
      .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(pair => pair[1])
  }
  if (value && typeof value === 'object') {
    const out = {}
    for (const k of Object.keys(value).sort()) {
      if (k === 'id') continue // per-write random hex; never a meaningful diff
      out[k] = canonicalize(value[k], k)
    }
    return out
  }
  if (typeof value === 'string' && value.includes('--rt=')) {
    return value.replace(/--rt=[^-]*--/g, '').trim()
  }
  return value
}

// True when two serialized_space objects are logically identical, ignoring key
// order, regenerated ids, and the join SQL marker.
export function spacesEqual(a, b) {
  return JSON.stringify(canonicalize(a || {})) === JSON.stringify(canonicalize(b || {}))
}

// ---------------------------------------------------------------------------
// Structured per-section diff (replaces the raw-JSON "Show Diff")
// ---------------------------------------------------------------------------

function _descToStr(d) {
  if (!d) return ''
  if (Array.isArray(d)) return d.join(', ')
  return String(d)
}

// Normalize one sample-question entry (string | {question} | {text}) to a string.
function _sqText(q) {
  if (q == null) return ''
  if (typeof q === 'string') return q
  const v = Array.isArray(q.question) ? q.question[0] : (q.question ?? q.text)
  return v == null ? '' : String(v)
}

// Pull the comparable logical sections out of a serialized_space (API shape).
// Tolerant of the naming variants the Genie API emits (example_question_sqls
// vs example_sql; text_instructions[] vs text; sample_questions under config or
// top-level, as strings or objects).
function extractSections(ss) {
  ss = ss || {}
  const ds = ss.data_sources || {}
  const inst = ss.instructions || {}
  const snip = inst.sql_snippets || {}
  const cfg = ss.config || {}
  let text = ''
  const ti = inst.text_instructions
  if (Array.isArray(ti) && ti.length) {
    text = ti.map(t => (Array.isArray(t.content) ? t.content.join('\n') : String(t.content || ''))).join('\n\n')
  } else {
    text = inst.text || ''
  }
  const rawSq = cfg.sample_questions || ss.sample_questions || []
  return {
    description: _descToStr(ss.description),
    text,
    tables: ds.tables || [],
    metric_views: ds.metric_views || [],
    example_sql: inst.example_question_sqls || inst.example_sql || [],
    join_specs: inst.join_specs || [],
    measures: snip.measures || [],
    filters: snip.filters || [],
    expressions: snip.expressions || [],
    sample_questions: rawSq.map(_sqText),
  }
}

// Identity key for an item in a keyed collection (so we can tell add/remove from
// in-place change). Falls back to canonical content when no natural key exists.
function _itemKey(item, kind) {
  if (item == null || typeof item !== 'object') return JSON.stringify(canonicalize(item))
  switch (kind) {
    case 'tables':
    case 'metric_views':
      return item.identifier || JSON.stringify(canonicalize(item))
    case 'example_sql':
      return _descToStr(item.question) || JSON.stringify(canonicalize(item))
    case 'join_specs': {
      const l = item.left?.identifier || item.left || ''
      const r = item.right?.identifier || item.right || ''
      return `${l}::${r}` || JSON.stringify(canonicalize(item))
    }
    case 'measures':
    case 'expressions':
      return item.alias || item.display_name || JSON.stringify(canonicalize(item))
    case 'filters':
      return item.display_name || JSON.stringify(canonicalize(item))
    default:
      return JSON.stringify(canonicalize(item))
  }
}

// Diff two keyed collections → {added:[keys], removed:[keys], changed:[keys]}.
function _diffCollection(baseArr, liveArr, kind) {
  const baseMap = new Map()
  const liveMap = new Map()
  for (const it of baseArr || []) baseMap.set(_itemKey(it, kind), it)
  for (const it of liveArr || []) liveMap.set(_itemKey(it, kind), it)
  const added = []
  const removed = []
  const changed = []
  for (const k of liveMap.keys()) if (!baseMap.has(k)) added.push(k)
  for (const k of baseMap.keys()) {
    if (!liveMap.has(k)) { removed.push(k); continue }
    const a = JSON.stringify(canonicalize(baseMap.get(k)))
    const b = JSON.stringify(canonicalize(liveMap.get(k)))
    if (a !== b) changed.push(k)
  }
  return { added, removed, changed }
}

const _COLLECTION_LABELS = {
  tables: 'Tables',
  metric_views: 'Metric views',
  example_sql: 'Example SQL',
  join_specs: 'Joins',
  measures: 'Measures',
  filters: 'Filters',
  expressions: 'Expressions',
}

// Produce a readable, per-section summary of what changed going from `baseline`
// to `live` (i.e. what an external edit did). Returns an array of section
// entries; empty when the spaces are logically identical. Each entry is one of:
//   { section, type: 'scalar', before, after }
//   { section, type: 'collection', added, removed, changed }   (key-name arrays)
//   { section, type: 'ordered', added, removed, reordered }    (sample_questions)
export function summarizeSpaceDiff(baseline, live) {
  const b = extractSections(baseline)
  const l = extractSections(live)
  const out = []

  // Scalars
  for (const [key, label] of [['description', 'Description'], ['text', 'Text instructions']]) {
    if (b[key] !== l[key]) {
      out.push({ section: label, type: 'scalar', before: b[key], after: l[key] })
    }
  }

  // Keyed collections
  for (const kind of Object.keys(_COLLECTION_LABELS)) {
    const { added, removed, changed } = _diffCollection(b[kind], l[kind], kind)
    if (added.length || removed.length || changed.length) {
      out.push({ section: _COLLECTION_LABELS[kind], type: 'collection', added, removed, changed })
    }
  }

  // Sample questions: order matters.
  const bq = (b.sample_questions || []).map(String)
  const lq = (l.sample_questions || []).map(String)
  const bqSet = new Set(bq)
  const lqSet = new Set(lq)
  const qAdded = lq.filter(q => !bqSet.has(q))
  const qRemoved = bq.filter(q => !lqSet.has(q))
  const sameSet = qAdded.length === 0 && qRemoved.length === 0
  const reordered = sameSet && JSON.stringify(bq) !== JSON.stringify(lq)
  if (qAdded.length || qRemoved.length || reordered) {
    out.push({ section: 'Sample questions', type: 'ordered', added: qAdded, removed: qRemoved, reordered })
  }

  return out
}
