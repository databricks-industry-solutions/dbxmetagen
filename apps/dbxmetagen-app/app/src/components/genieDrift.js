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
