const _cache = new Map()

function _get(key, ttlMs) {
  const entry = _cache.get(key)
  if (!entry) return null
  if (Date.now() - entry.ts > ttlMs) { _cache.delete(key); return null }
  return entry.value
}

function _set(key, value) {
  _cache.set(key, { value, ts: Date.now() })
}

export async function cachedFetch(url, options = {}, ttlMs = 30_000) {
  const cached = _get(url, ttlMs)
  if (cached) return cached
  try {
    const res = await fetch(url, options)
    if (!res.ok) {
      const body = await res.text().catch(() => '')
      let msg = `Error ${res.status}`
      try { const j = JSON.parse(body); if (j.detail) msg = j.detail } catch {}
      return { data: [], error: msg }
    }
    const data = await res.json()
    const result = { data: Array.isArray(data) ? data : [], error: null }
    _set(url, result)
    return result
  } catch (e) { return { data: [], error: e.message } }
}

export async function cachedFetchObj(url, options = {}, ttlMs = 30_000) {
  const cached = _get(url, ttlMs)
  if (cached) return cached
  try {
    const res = await fetch(url, options)
    if (!res.ok) {
      const body = await res.text().catch(() => '')
      let msg = `Error ${res.status}`
      try { const j = JSON.parse(body); if (j.detail) msg = j.detail } catch {}
      return { data: null, error: msg }
    }
    const result = { data: await res.json(), error: null }
    _set(url, result)
    return result
  } catch (e) { return { data: null, error: e.message } }
}

export function invalidateCache(urlPattern) {
  if (!urlPattern) { _cache.clear(); return }
  for (const key of _cache.keys()) {
    if (key.includes(urlPattern)) _cache.delete(key)
  }
}

export const TTL = {
  DASHBOARD: 60_000,
  CONFIG: 30_000,
  POLLING: 5_000,
}
