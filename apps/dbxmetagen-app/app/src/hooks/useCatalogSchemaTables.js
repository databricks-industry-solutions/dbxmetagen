import { useState, useEffect, useMemo } from 'react'

async function safeArrayFetch(url) {
  const res = await fetch(url)
  if (!res.ok) {
    const body = await res.text().catch(() => '')
    let msg = `Error ${res.status}`
    try { const j = JSON.parse(body); if (j.detail) msg = j.detail } catch {}
    return { data: [], error: msg }
  }
  const data = await res.json()
  return { data: Array.isArray(data) ? data : [], error: null }
}

/**
 * Shared hook for the catalog -> schema -> tables cascade.
 * Fetches catalogs on mount, schemas when catalog changes, tables when schema changes.
 */
export function useCatalogSchemaTables(initialCatalog = '', initialSchema = '') {
  const [catalogs, setCatalogs] = useState([])
  const [schemas, setSchemas] = useState([])
  const [tables, setTables] = useState([])
  const [catalog, setCatalog] = useState(initialCatalog)
  const [schema, setSchema] = useState(initialSchema)
  const [filter, setFilter] = useState('')
  const [error, setError] = useState(null)

  useEffect(() => {
    safeArrayFetch('/api/catalogs')
      .then(({ data, error: err }) => { setCatalogs(data); if (err) setError(err) })
      .catch(() => setCatalogs([]))
  }, [])

  useEffect(() => {
    setSchemas([]); setTables([]); setError(null)
    if (!catalog) return
    safeArrayFetch(`/api/schemas?catalog=${encodeURIComponent(catalog)}`)
      .then(({ data, error: err }) => { setSchemas(data); if (err) setError(err) })
      .catch(() => setSchemas([]))
  }, [catalog])

  useEffect(() => {
    setTables([])
    if (!catalog || !schema) return
    safeArrayFetch(`/api/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`)
      .then(({ data, error: err }) => { setTables(data); if (err) setError(err) })
      .catch(() => setTables([]))
  }, [catalog, schema])

  const filtered = useMemo(() => {
    if (!filter) return tables
    const f = filter.toLowerCase()
    return tables.filter(t => t.toLowerCase().includes(f))
  }, [tables, filter])

  return { catalogs, schemas, tables, filtered, catalog, schema, filter, error, setCatalog, setSchema, setFilter }
}
