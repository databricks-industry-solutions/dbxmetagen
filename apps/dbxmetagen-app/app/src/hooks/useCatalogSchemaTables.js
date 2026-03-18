import { useState, useEffect, useMemo } from 'react'

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

  useEffect(() => {
    fetch('/api/catalogs').then(r => r.json()).then(setCatalogs).catch(() => {})
  }, [])

  useEffect(() => {
    setSchemas([]); setTables([])
    if (!catalog) return
    fetch(`/api/schemas?catalog=${encodeURIComponent(catalog)}`)
      .then(r => r.json()).then(setSchemas).catch(() => setSchemas([]))
  }, [catalog])

  useEffect(() => {
    setTables([])
    if (!catalog || !schema) return
    fetch(`/api/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`)
      .then(r => r.json()).then(setTables).catch(() => setTables([]))
  }, [catalog, schema])

  const filtered = useMemo(() => {
    if (!filter) return tables
    const f = filter.toLowerCase()
    return tables.filter(t => t.toLowerCase().includes(f))
  }, [tables, filter])

  return { catalogs, schemas, tables, filtered, catalog, schema, filter, setCatalog, setSchema, setFilter }
}
