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
 *
 * @param {string} initialCatalog
 * @param {string} initialSchema
 * @param {{ kbOnly?: boolean }} opts - When kbOnly is true, only tables present in the
 *   knowledge base are returned. Tables not yet processed by core metadata are excluded.
 */
export function useCatalogSchemaTables(initialCatalog = '', initialSchema = '', opts = {}) {
  const { kbOnly = false } = opts
  const [catalogs, setCatalogs] = useState([])
  const [schemas, setSchemas] = useState([])
  const [tables, setTables] = useState([])
  const [allSchemaTableCount, setAllSchemaTableCount] = useState(0)
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
    setSchemas([]); setTables([]); setError(null); setAllSchemaTableCount(0)
    if (!catalog) return
    safeArrayFetch(`/api/schemas?catalog=${encodeURIComponent(catalog)}`)
      .then(({ data, error: err }) => { setSchemas(data); if (err) setError(err) })
      .catch(() => setSchemas([]))
  }, [catalog])

  useEffect(() => {
    setTables([]); setAllSchemaTableCount(0)
    if (!catalog || !schema) return
    const catEnc = encodeURIComponent(catalog)
    const schEnc = encodeURIComponent(schema)
    if (kbOnly) {
      Promise.all([
        safeArrayFetch(`/api/tables/kb?catalog=${catEnc}&schema=${schEnc}`),
        safeArrayFetch(`/api/tables?catalog=${catEnc}&schema=${schEnc}`),
      ]).then(([kb, all]) => {
        if (kb.error) setError(kb.error)
        setTables(kb.data)
        setAllSchemaTableCount(all.data.length)
      }).catch(() => setTables([]))
    } else {
      safeArrayFetch(`/api/tables?catalog=${catEnc}&schema=${schEnc}`)
        .then(({ data, error: err }) => { setTables(data); setAllSchemaTableCount(data.length); if (err) setError(err) })
        .catch(() => setTables([]))
    }
  }, [catalog, schema, kbOnly])

  const filtered = useMemo(() => {
    if (!filter) return tables
    const f = filter.toLowerCase()
    return tables.filter(t => t.toLowerCase().includes(f))
  }, [tables, filter])

  return { catalogs, schemas, tables, filtered, allSchemaTableCount, catalog, schema, filter, error, setCatalog, setSchema, setFilter }
}
