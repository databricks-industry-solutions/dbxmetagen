import React, { useState, useEffect, useReducer, useRef, useMemo, useCallback } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { safeFetchObj, ErrorBanner } from '../App'
import { cachedFetch, invalidateCache, TTL } from '../apiCache'
import { PageHeader, EmptyState, SkeletonCards } from './ui'
import { useCatalogSchemaTables } from '../hooks/useCatalogSchemaTables'

const PALETTE = [
  '#FF3621', '#6366f1', '#10b981', '#f59e0b', '#3b82f6',
  '#ec4899', '#8b5cf6', '#14b8a6', '#f97316', '#0ea5e9',
]
const ROLE_OPTIONS = [
  'primary_key', 'business_key', 'object_property', 'measure', 'dimension',
  'temporal', 'geographic', 'label', 'audit', 'derived', 'composite_component',
]
const EDGE_CATEGORIES = ['structural', 'business', 'lineage', 'semantic']
const CARDINALITY_OPTIONS = ['one_to_one', 'one_to_many', 'many_to_one', 'many_to_many']

const EMPTY_STATE = {
  metadata: { name: '', version: '1.0', format_version: '2.0', industry: '', description: '' },
  entities: {},
  edge_catalog: {},
  property_roles: {},
  domains: [],
  domain_entity_affinity: {},
}

// ── Reducer ──────────────────────────────────────────────────────────

function builderReducer(state, action) {
  switch (action.type) {
    case 'LOAD_STATE':
      return { ...EMPTY_STATE, ...action.payload }
    case 'SET_METADATA':
      return { ...state, metadata: { ...state.metadata, ...action.payload } }

    case 'ADD_ENTITY': {
      const { name, ...rest } = action.payload
      if (state.entities[name]) return state
      return { ...state, entities: { ...state.entities, [name]: { description: '', keywords: [], typical_attributes: [], properties: {}, relationships: {}, ...rest } } }
    }
    case 'UPDATE_ENTITY': {
      const { name, updates } = action.payload
      if (!state.entities[name]) return state
      return { ...state, entities: { ...state.entities, [name]: { ...state.entities[name], ...updates } } }
    }
    case 'RENAME_ENTITY': {
      const { oldName, newName } = action.payload
      if (!state.entities[oldName] || state.entities[newName]) return state
      const { [oldName]: ent, ...rest } = state.entities
      return { ...state, entities: { ...rest, [newName]: ent } }
    }
    case 'REMOVE_ENTITY': {
      const { [action.payload]: _, ...rest } = state.entities
      const ec = { ...state.edge_catalog }
      for (const [k, v] of Object.entries(ec)) {
        if (v.domain === action.payload || v.range === action.payload) delete ec[k]
      }
      return { ...state, entities: rest, edge_catalog: ec }
    }

    case 'ADD_PROPERTY': {
      const { entityName, propName, prop } = action.payload
      const ent = state.entities[entityName]
      if (!ent) return state
      return { ...state, entities: { ...state.entities, [entityName]: { ...ent, properties: { ...ent.properties, [propName]: prop } } } }
    }
    case 'UPDATE_PROPERTY': {
      const { entityName, propName, updates } = action.payload
      const ent = state.entities[entityName]
      if (!ent || !ent.properties[propName]) return state
      return { ...state, entities: { ...state.entities, [entityName]: { ...ent, properties: { ...ent.properties, [propName]: { ...ent.properties[propName], ...updates } } } } }
    }
    case 'REMOVE_PROPERTY': {
      const { entityName, propName } = action.payload
      const ent = state.entities[entityName]
      if (!ent) return state
      const { [propName]: _, ...restProps } = ent.properties
      return { ...state, entities: { ...state.entities, [entityName]: { ...ent, properties: restProps } } }
    }

    case 'ADD_EDGE': {
      const { name, edge } = action.payload
      return { ...state, edge_catalog: { ...state.edge_catalog, [name]: edge } }
    }
    case 'UPDATE_EDGE': {
      const { name, updates } = action.payload
      if (!state.edge_catalog[name]) return state
      return { ...state, edge_catalog: { ...state.edge_catalog, [name]: { ...state.edge_catalog[name], ...updates } } }
    }
    case 'REMOVE_EDGE': {
      const { [action.payload]: _, ...rest } = state.edge_catalog
      return { ...state, edge_catalog: rest }
    }

    case 'ADD_DOMAIN':
      return { ...state, domains: [...state.domains, action.payload] }
    case 'UPDATE_DOMAIN': {
      const doms = [...state.domains]
      doms[action.payload.index] = { ...doms[action.payload.index], ...action.payload.updates }
      return { ...state, domains: doms }
    }
    case 'REMOVE_DOMAIN':
      return { ...state, domains: state.domains.filter((_, i) => i !== action.payload) }

    default:
      return state
  }
}

// ── Undo wrapper ─────────────────────────────────────────────────────

function useUndoReducer(reducer, init) {
  const [history, setHistory] = useState({ past: [], present: init, future: [] })

  const dispatch = useCallback((action) => {
    setHistory(h => {
      if (action.type === 'UNDO') {
        if (!h.past.length) return h
        return { past: h.past.slice(0, -1), present: h.past[h.past.length - 1], future: [h.present, ...h.future].slice(0, 50) }
      }
      if (action.type === 'REDO') {
        if (!h.future.length) return h
        return { past: [...h.past, h.present].slice(-50), present: h.future[0], future: h.future.slice(1) }
      }
      const next = reducer(h.present, action)
      if (next === h.present) return h
      return { past: [...h.past, h.present].slice(-50), present: next, future: [] }
    })
  }, [reducer])

  return [history.present, dispatch, { canUndo: history.past.length > 0, canRedo: history.future.length > 0 }]
}

// ── Tag Input ────────────────────────────────────────────────────────

function TagInput({ tags = [], onChange, placeholder = 'Type and press Enter' }) {
  const [value, setValue] = useState('')
  const add = () => {
    const v = value.trim()
    if (v && !tags.includes(v)) { onChange([...tags, v]); setValue('') }
  }
  return (
    <div className="flex flex-wrap gap-1.5 items-center">
      {tags.map(t => (
        <span key={t} className="inline-flex items-center gap-1 px-2 py-0.5 bg-slate-100 dark:bg-dbx-navy-500 text-xs rounded-md">
          {t}
          <button type="button" onClick={() => onChange(tags.filter(x => x !== t))} className="text-slate-400 hover:text-red-500">&times;</button>
        </span>
      ))}
      <input value={value} onChange={e => setValue(e.target.value)}
        onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); add() } }}
        placeholder={placeholder}
        className="input-base !py-1 !text-xs flex-1 min-w-[100px]" />
    </div>
  )
}

// ── Suggestion Card ──────────────────────────────────────────────────

function SuggestionCard({ item, type, onAccept, onDismiss }) {
  return (
    <div className="card p-3 flex items-start gap-3 animate-slide-up">
      <div className="flex-1 min-w-0">
        <p className="text-sm font-semibold text-slate-800 dark:text-slate-100">{item.name}</p>
        <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5 line-clamp-2">{item.description || item.reasoning}</p>
        {item.keywords?.length > 0 && (
          <div className="flex flex-wrap gap-1 mt-1.5">
            {item.keywords.slice(0, 5).map(k => <span key={k} className="px-1.5 py-0.5 bg-slate-100 dark:bg-dbx-navy-500 text-[10px] rounded">{k}</span>)}
          </div>
        )}
      </div>
      <div className="flex flex-col gap-1.5 flex-shrink-0">
        <button onClick={() => onAccept(item)} className="btn-primary !py-1 !px-2 !text-xs">Accept</button>
        <button onClick={() => onDismiss(item)} className="btn-ghost !py-1 !px-2 !text-xs">Dismiss</button>
      </div>
    </div>
  )
}

// ── Entity Form ──────────────────────────────────────────────────────

function EntityForm({ entity, entityName, allEntities, onUpdate, onDelete, onSuggestProps, suggestingProps }) {
  if (!entity) return null
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-bold text-slate-800 dark:text-slate-100">{entityName}</h3>
        <button onClick={onDelete} className="text-xs text-red-500 hover:text-red-700">Delete</button>
      </div>
      <label className="block">
        <span className="text-xs text-slate-500">Description</span>
        <textarea value={entity.description || ''} onChange={e => onUpdate({ description: e.target.value })}
          rows={2} className="textarea-base !text-xs mt-1" />
      </label>
      <label className="block">
        <span className="text-xs text-slate-500">URI (optional)</span>
        <input value={entity.uri || ''} onChange={e => onUpdate({ uri: e.target.value })}
          className="input-base !text-xs mt-1" placeholder="schema:Person" />
      </label>
      <div>
        <span className="text-xs text-slate-500">Keywords</span>
        <TagInput tags={entity.keywords || []} onChange={keywords => onUpdate({ keywords })} placeholder="Add keyword..." />
      </div>
      <div>
        <span className="text-xs text-slate-500">Typical Attributes</span>
        <TagInput tags={entity.typical_attributes || []} onChange={typical_attributes => onUpdate({ typical_attributes })} placeholder="Add attribute..." />
      </div>

      {/* Properties */}
      <div className="border-t border-slate-200 dark:border-dbx-navy-400/30 pt-3">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs font-semibold text-slate-600 dark:text-slate-300">Properties ({Object.keys(entity.properties || {}).length})</span>
          <button onClick={onSuggestProps} disabled={suggestingProps}
            className="btn-ghost !py-0.5 !px-2 !text-[10px]">{suggestingProps ? 'Suggesting...' : 'Suggest Properties'}</button>
        </div>
        {Object.entries(entity.properties || {}).map(([pName, prop]) => (
          <PropertyRow key={pName} name={pName} prop={prop} allEntities={allEntities}
            onUpdate={updates => onUpdate({ properties: { ...entity.properties, [pName]: { ...prop, ...updates } } })}
            onRemove={() => {
              const { [pName]: _, ...rest } = entity.properties
              onUpdate({ properties: rest })
            }} />
        ))}
        <AddPropertyRow onAdd={(name, prop) => onUpdate({ properties: { ...entity.properties, [name]: prop } })} allEntities={allEntities} />
      </div>
    </div>
  )
}

function PropertyRow({ name, prop, allEntities, onUpdate, onRemove }) {
  const [expanded, setExpanded] = useState(false)
  return (
    <div className="border border-slate-200 dark:border-dbx-navy-400/20 rounded-lg p-2 mb-1.5 text-xs">
      <div className="flex items-center justify-between">
        <button onClick={() => setExpanded(!expanded)} className="font-medium text-slate-700 dark:text-slate-200">{name}</button>
        <div className="flex items-center gap-2">
          <span className="text-[10px] px-1.5 py-0.5 bg-slate-100 dark:bg-dbx-navy-500 rounded">{prop.role}</span>
          <button onClick={onRemove} className="text-red-400 hover:text-red-600">&times;</button>
        </div>
      </div>
      {expanded && (
        <div className="mt-2 space-y-2 pt-2 border-t border-slate-100 dark:border-dbx-navy-500/30">
          <div className="grid grid-cols-2 gap-2">
            <label className="block">
              <span className="text-[10px] text-slate-500">Kind</span>
              <select value={prop.kind || 'data_property'} onChange={e => onUpdate({ kind: e.target.value })} className="select-base !text-xs mt-0.5">
                <option value="data_property">data_property</option>
                <option value="object_property">object_property</option>
              </select>
            </label>
            <label className="block">
              <span className="text-[10px] text-slate-500">Role</span>
              <select value={prop.role || 'dimension'} onChange={e => onUpdate({ role: e.target.value })} className="select-base !text-xs mt-0.5">
                {ROLE_OPTIONS.map(r => <option key={r} value={r}>{r}</option>)}
              </select>
            </label>
          </div>
          {prop.kind === 'object_property' && (
            <div className="grid grid-cols-2 gap-2">
              <label className="block">
                <span className="text-[10px] text-slate-500">Edge</span>
                <input value={prop.edge || ''} onChange={e => onUpdate({ edge: e.target.value })} className="input-base !text-xs mt-0.5" />
              </label>
              <label className="block">
                <span className="text-[10px] text-slate-500">Target Entity</span>
                <select value={prop.target_entity || ''} onChange={e => onUpdate({ target_entity: e.target.value })} className="select-base !text-xs mt-0.5">
                  <option value="">(none)</option>
                  {allEntities.map(e => <option key={e} value={e}>{e}</option>)}
                </select>
              </label>
            </div>
          )}
          <div>
            <span className="text-[10px] text-slate-500">Typical Attributes</span>
            <TagInput tags={prop.typical_attributes || []} onChange={typical_attributes => onUpdate({ typical_attributes })} placeholder="column name..." />
          </div>
        </div>
      )}
    </div>
  )
}

function AddPropertyRow({ onAdd, allEntities }) {
  const [open, setOpen] = useState(false)
  const [name, setName] = useState('')
  const [kind, setKind] = useState('data_property')
  const [role, setRole] = useState('dimension')

  if (!open) return <button onClick={() => setOpen(true)} className="text-xs text-dbx-teal hover:underline">+ Add property</button>

  return (
    <div className="border border-dashed border-slate-300 dark:border-dbx-navy-400/40 rounded-lg p-2 mt-1.5 space-y-2">
      <input value={name} onChange={e => setName(e.target.value)} placeholder="Property name" className="input-base !text-xs" />
      <div className="grid grid-cols-2 gap-2">
        <select value={kind} onChange={e => setKind(e.target.value)} className="select-base !text-xs">
          <option value="data_property">data_property</option>
          <option value="object_property">object_property</option>
        </select>
        <select value={role} onChange={e => setRole(e.target.value)} className="select-base !text-xs">
          {ROLE_OPTIONS.map(r => <option key={r} value={r}>{r}</option>)}
        </select>
      </div>
      <div className="flex gap-2">
        <button onClick={() => { if (name.trim()) { onAdd(name.trim(), { kind, role, typical_attributes: [] }); setName(''); setOpen(false) } }}
          className="btn-primary !py-1 !px-2 !text-xs">Add</button>
        <button onClick={() => setOpen(false)} className="btn-ghost !py-1 !px-2 !text-xs">Cancel</button>
      </div>
    </div>
  )
}

// ── Relationship Form ────────────────────────────────────────────────

function RelationshipForm({ edgeName, edge, entityNames, onUpdate, onDelete }) {
  if (!edge) return null
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-bold text-slate-800 dark:text-slate-100">{edgeName}</h3>
        <button onClick={onDelete} className="text-xs text-red-500 hover:text-red-700">Delete</button>
      </div>
      <div className="grid grid-cols-2 gap-2">
        <label className="block">
          <span className="text-xs text-slate-500">Source (Domain)</span>
          <select value={edge.domain || ''} onChange={e => onUpdate({ domain: e.target.value })} className="select-base !text-xs mt-1">
            <option value="">Any</option>
            {entityNames.map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        </label>
        <label className="block">
          <span className="text-xs text-slate-500">Target (Range)</span>
          <select value={edge.range || ''} onChange={e => onUpdate({ range: e.target.value })} className="select-base !text-xs mt-1">
            <option value="">Any</option>
            {entityNames.map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        </label>
      </div>
      <label className="block">
        <span className="text-xs text-slate-500">Inverse</span>
        <input value={edge.inverse || ''} onChange={e => onUpdate({ inverse: e.target.value })} className="input-base !text-xs mt-1" />
      </label>
      <div className="grid grid-cols-2 gap-2">
        <label className="block">
          <span className="text-xs text-slate-500">Category</span>
          <select value={edge.category || 'business'} onChange={e => onUpdate({ category: e.target.value })} className="select-base !text-xs mt-1">
            {EDGE_CATEGORIES.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </label>
        <label className="flex items-center gap-2 mt-4 text-xs text-slate-600 dark:text-slate-300 cursor-pointer">
          <input type="checkbox" checked={!!edge.symmetric} onChange={e => onUpdate({ symmetric: e.target.checked })} />
          Symmetric
        </label>
      </div>
    </div>
  )
}

// ── Add Entity Modal ─────────────────────────────────────────────────

function AddEntityModal({ open, onClose, onAdd }) {
  const [name, setName] = useState('')
  const [desc, setDesc] = useState('')
  if (!open) return null
  return (
    <div className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="bg-white dark:bg-dbx-navy-600 rounded-xl shadow-elevated p-6 w-full max-w-sm animate-slide-up" onClick={e => e.stopPropagation()}>
        <h3 className="text-sm font-bold text-slate-800 dark:text-slate-100 mb-3">New Entity Type</h3>
        <input value={name} onChange={e => setName(e.target.value)} placeholder="Entity name (e.g. Patient)" className="input-base !text-sm mb-2" autoFocus />
        <textarea value={desc} onChange={e => setDesc(e.target.value)} placeholder="Description" rows={2} className="textarea-base !text-xs mb-3" />
        <div className="flex gap-2 justify-end">
          <button onClick={onClose} className="btn-ghost btn-sm">Cancel</button>
          <button onClick={() => { if (name.trim()) { onAdd(name.trim(), desc); setName(''); setDesc(''); onClose() } }}
            className="btn-primary btn-sm">Add</button>
        </div>
      </div>
    </div>
  )
}

// ── Add Edge Modal ───────────────────────────────────────────────────

function AddEdgeModal({ open, onClose, onAdd, entityNames, source, target }) {
  const [name, setName] = useState('')
  const [inverse, setInverse] = useState('')
  const [category, setCategory] = useState('business')
  const [src, setSrc] = useState(source || '')
  const [tgt, setTgt] = useState(target || '')
  useEffect(() => { setSrc(source || ''); setTgt(target || '') }, [source, target])
  if (!open) return null
  return (
    <div className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="bg-white dark:bg-dbx-navy-600 rounded-xl shadow-elevated p-6 w-full max-w-sm animate-slide-up" onClick={e => e.stopPropagation()}>
        <h3 className="text-sm font-bold text-slate-800 dark:text-slate-100 mb-3">New Relationship</h3>
        <input value={name} onChange={e => setName(e.target.value)} placeholder="Relationship name (e.g. works_at)" className="input-base !text-sm mb-2" autoFocus />
        <input value={inverse} onChange={e => setInverse(e.target.value)} placeholder="Inverse (e.g. employs)" className="input-base !text-xs mb-2" />
        <div className="grid grid-cols-2 gap-2 mb-2">
          <select value={src} onChange={e => setSrc(e.target.value)} className="select-base !text-xs">
            <option value="">Source...</option>
            {entityNames.map(n => <option key={n} value={n}>{n}</option>)}
          </select>
          <select value={tgt} onChange={e => setTgt(e.target.value)} className="select-base !text-xs">
            <option value="">Target...</option>
            {entityNames.map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        </div>
        <select value={category} onChange={e => setCategory(e.target.value)} className="select-base !text-xs mb-3">
          {EDGE_CATEGORIES.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
        <div className="flex gap-2 justify-end">
          <button onClick={onClose} className="btn-ghost btn-sm">Cancel</button>
          <button onClick={() => {
            if (name.trim() && src && tgt) {
              onAdd(name.trim(), { inverse, symmetric: false, category, domain: src, range: tgt })
              setName(''); setInverse(''); onClose()
            }
          }} className="btn-primary btn-sm">Add</button>
        </div>
      </div>
    </div>
  )
}

// ── YAML Serializer ──────────────────────────────────────────────────

function stateToYaml(state) {
  const lines = []
  const w = (indent, text) => lines.push('  '.repeat(indent) + text)

  w(0, 'metadata:')
  for (const [k, v] of Object.entries(state.metadata || {})) {
    if (v) w(1, `${k}: '${v}'`)
  }

  w(0, 'ontology:')
  w(1, `version: '${state.metadata?.version || '1.0'}'`)
  w(1, `name: '${state.metadata?.name || 'Custom Ontology'}'`)
  if (state.metadata?.description) w(1, `description: '${state.metadata.description}'`)

  if (Object.keys(state.property_roles || {}).length) {
    w(1, 'property_roles:')
    for (const [rn, rv] of Object.entries(state.property_roles)) {
      w(2, `${rn}:`)
      if (rv.description) w(3, `description: ${rv.description}`)
      if (rv.maps_to_kind) w(3, `maps_to_kind: ${rv.maps_to_kind}`)
      if (rv.semantic_role) w(3, `semantic_role: ${rv.semantic_role}`)
    }
  }

  if (Object.keys(state.edge_catalog || {}).length) {
    w(1, 'edge_catalog:')
    for (const [en, ev] of Object.entries(state.edge_catalog)) {
      w(2, `${en}:`)
      if (ev.inverse) w(3, `inverse: ${ev.inverse}`)
      w(3, `symmetric: ${!!ev.symmetric}`)
      w(3, `category: ${ev.category || 'business'}`)
      if (ev.domain) w(3, `domain: ${ev.domain}`)
      if (ev.range) w(3, `range: ${ev.range}`)
    }
  }

  w(1, 'entities:')
  w(2, 'auto_discover: true')
  w(2, 'discovery_confidence_threshold: 0.4')
  w(2, 'definitions:')
  for (const [eName, eDef] of Object.entries(state.entities || {})) {
    w(3, `${eName}:`)
    if (eDef.description) w(4, `description: '${eDef.description.replace(/'/g, "''")}'`)
    if (eDef.uri) w(4, `uri: '${eDef.uri}'`)
    if (eDef.source_ontology) w(4, `source_ontology: '${eDef.source_ontology}'`)
    if (eDef.keywords?.length) {
      w(4, 'keywords:')
      eDef.keywords.forEach(k => w(5, `- ${k}`))
    }
    if (eDef.typical_attributes?.length) {
      w(4, 'typical_attributes:')
      eDef.typical_attributes.forEach(a => w(5, `- ${a}`))
    }
    if (Object.keys(eDef.properties || {}).length) {
      w(4, 'properties:')
      for (const [pn, pv] of Object.entries(eDef.properties)) {
        w(5, `${pn}:`)
        w(6, `kind: ${pv.kind || 'data_property'}`)
        w(6, `role: ${pv.role || 'dimension'}`)
        if (pv.edge) w(6, `edge: ${pv.edge}`)
        if (pv.target_entity) w(6, `target_entity: ${pv.target_entity}`)
        if (pv.typical_attributes?.length) {
          w(6, 'typical_attributes:')
          pv.typical_attributes.forEach(a => w(7, `- ${a}`))
        }
      }
    }
    if (Object.keys(eDef.relationships || {}).length) {
      w(4, 'relationships:')
      for (const [rn, rv] of Object.entries(eDef.relationships)) {
        w(5, `${rn}:`)
        if (rv.target) w(6, `target: ${rv.target}`)
        if (rv.cardinality) w(6, `cardinality: ${rv.cardinality}`)
      }
    }
  }

  if (state.domains?.length) {
    w(0, 'domains:')
    for (const d of state.domains) {
      w(1, `${(d.name || 'unnamed').toLowerCase().replace(/\s+/g, '_')}:`)
      w(2, `name: '${d.name}'`)
      if (d.description) w(2, `description: '${d.description.replace(/'/g, "''")}'`)
      if (d.keywords?.length) {
        w(2, 'keywords:')
        d.keywords.forEach(k => w(3, `- ${k}`))
      }
    }
  }

  return lines.join('\n')
}

// ── Main Component ───────────────────────────────────────────────────

export default function OntologyBuilder({ onNavigate }) {
  const [state, dispatch, { canUndo, canRedo }] = useUndoReducer(builderReducer, EMPTY_STATE)
  const [selectedNode, setSelectedNode] = useState(null)
  const [selectedEdge, setSelectedEdge] = useState(null)
  const [showAddEntity, setShowAddEntity] = useState(false)
  const [showAddEdge, setShowAddEdge] = useState(false)
  const [addEdgeSource, setAddEdgeSource] = useState('')
  const [addEdgeTarget, setAddEdgeTarget] = useState('')
  const [showYaml, setShowYaml] = useState(false)
  const [bundles, setBundles] = useState([])
  const [loadedBundle, setLoadedBundle] = useState('')
  const [error, setError] = useState(null)
  const [saving, setSaving] = useState(false)
  const [saveMsg, setSaveMsg] = useState('')

  // LLM suggestion state
  const [suggestions, setSuggestions] = useState([])
  const [suggesting, setSuggesting] = useState(false)
  const [relSuggestions, setRelSuggestions] = useState([])
  const [suggestingRels, setSuggestingRels] = useState(false)
  const [propSuggestions, setPropSuggestions] = useState([])
  const [suggestingProps, setSuggestingProps] = useState(false)

  // Table selection for LLM context
  const cst = useCatalogSchemaTables()
  const [selectedTables, setSelectedTables] = useState(new Set())
  const [domain, setDomain] = useState('')

  const graphRef = useRef()
  const entityNames = useMemo(() => Object.keys(state.entities), [state.entities])

  // Load bundles list
  useEffect(() => {
    cachedFetch('/api/ontology/bundles', {}, TTL.CONFIG)
      .then(({ data }) => setBundles(data || []))
  }, [])

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'z' && !e.shiftKey) { e.preventDefault(); dispatch({ type: 'UNDO' }) }
      if ((e.metaKey || e.ctrlKey) && (e.key === 'y' || (e.key === 'z' && e.shiftKey))) { e.preventDefault(); dispatch({ type: 'REDO' }) }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [dispatch])

  // Graph data for react-force-graph-2d
  const graphData = useMemo(() => {
    const nodes = entityNames.map((name, i) => ({
      id: name,
      label: name,
      color: PALETTE[i % PALETTE.length],
      val: 3 + Object.keys(state.entities[name]?.properties || {}).length * 0.5,
    }))
    const links = []
    const entitySet = new Set(entityNames)
    for (const [eName, eDef] of Object.entries(state.edge_catalog)) {
      if (eDef.domain && eDef.range && entitySet.has(eDef.domain) && entitySet.has(eDef.range)) {
        links.push({ source: eDef.domain, target: eDef.range, label: eName, id: eName })
      }
    }
    return { nodes, links }
  }, [state.entities, state.edge_catalog, entityNames])

  // Load bundle
  const loadBundle = useCallback(async (key) => {
    if (!key) { dispatch({ type: 'LOAD_STATE', payload: EMPTY_STATE }); setLoadedBundle(''); return }
    setError(null)
    const { data, error: err } = await safeFetchObj(`/api/ontology/builder/load/${encodeURIComponent(key)}`)
    if (err) { setError(err); return }
    if (data) {
      dispatch({ type: 'LOAD_STATE', payload: data })
      setLoadedBundle(key)
      setSelectedNode(null); setSelectedEdge(null)
    }
  }, [dispatch])

  // Save bundle
  const saveBundle = useCallback(async () => {
    if (!state.metadata.name) { setError('Bundle name is required'); return }
    setSaving(true); setError(null); setSaveMsg('')
    const { data, error: err } = await safeFetchObj('/api/ontology/builder/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(state),
    })
    setSaving(false)
    if (err) { setError(err); return }
    setSaveMsg(`Saved as ${data?.bundle_key || 'bundle'}`)
    invalidateCache('/api/ontology/bundles')
    cachedFetch('/api/ontology/bundles', {}, TTL.CONFIG).then(({ data }) => setBundles(data || []))
    setTimeout(() => setSaveMsg(''), 4000)
  }, [state])

  // Suggest entities
  const suggestEntities = useCallback(async () => {
    if (!selectedTables.size && !domain) { setError('Select tables or a domain first'); return }
    setSuggesting(true); setError(null)
    const { data, error: err } = await safeFetchObj('/api/ontology/builder/suggest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tables: [...selectedTables],
        domain,
        existing_entities: entityNames,
      }),
    })
    setSuggesting(false)
    if (err) { setError(err); return }
    setSuggestions(data?.suggestions || [])
  }, [selectedTables, domain, entityNames])

  // Suggest relationships
  const suggestRelationships = useCallback(async () => {
    if (entityNames.length < 2) { setError('Need at least 2 entities to suggest relationships'); return }
    setSuggestingRels(true); setError(null)
    const { data, error: err } = await safeFetchObj('/api/ontology/builder/suggest-relationships', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ entities: entityNames, tables: [...selectedTables], domain }),
    })
    setSuggestingRels(false)
    if (err) { setError(err); return }
    setRelSuggestions(data?.suggestions || [])
  }, [entityNames, selectedTables, domain])

  // Suggest properties for selected entity
  const suggestProperties = useCallback(async () => {
    if (!selectedNode || !state.entities[selectedNode]) return
    setSuggestingProps(true); setError(null)
    const ent = state.entities[selectedNode]
    const { data, error: err } = await safeFetchObj('/api/ontology/builder/suggest-properties', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        entity_name: selectedNode,
        entity_description: ent.description || '',
        tables: [...selectedTables],
        existing_properties: Object.keys(ent.properties || {}),
      }),
    })
    setSuggestingProps(false)
    if (err) { setError(err); return }
    setPropSuggestions(data?.suggestions || [])
  }, [selectedNode, state.entities, selectedTables])

  // Accept helpers
  const acceptEntity = useCallback((item) => {
    dispatch({ type: 'ADD_ENTITY', payload: { name: item.name, description: item.description, keywords: item.keywords || [], typical_attributes: item.typical_attributes || [] } })
    setSuggestions(prev => prev.filter(s => s.name !== item.name))
  }, [dispatch])

  const acceptRelationship = useCallback((item) => {
    dispatch({ type: 'ADD_EDGE', payload: { name: item.name, edge: { inverse: item.inverse || '', symmetric: !!item.symmetric, category: item.category || 'business', domain: item.source_entity, range: item.target_entity } } })
    setRelSuggestions(prev => prev.filter(s => s.name !== item.name))
  }, [dispatch])

  const acceptProperty = useCallback((item) => {
    if (!selectedNode) return
    dispatch({ type: 'ADD_PROPERTY', payload: { entityName: selectedNode, propName: item.name, prop: { kind: item.kind || 'data_property', role: item.role || 'dimension', typical_attributes: item.typical_attributes || [], edge: item.edge, target_entity: item.target_entity } } })
    setPropSuggestions(prev => prev.filter(s => s.name !== item.name))
  }, [dispatch, selectedNode])

  const nodeCanvasObject = useCallback((node, ctx, globalScale) => {
    const label = node.label
    const fontSize = Math.max(12 / globalScale, 3)
    ctx.font = `bold ${fontSize}px sans-serif`
    const w = ctx.measureText(label).width + fontSize * 1.5
    const h = fontSize * 2
    const isSelected = selectedNode === node.id

    ctx.beginPath()
    ctx.roundRect(node.x - w / 2, node.y - h / 2, w, h, 4 / globalScale)
    ctx.fillStyle = node.color
    ctx.fill()
    if (isSelected) {
      ctx.strokeStyle = '#fff'
      ctx.lineWidth = 2 / globalScale
      ctx.stroke()
    }

    ctx.fillStyle = '#fff'
    ctx.textAlign = 'center'
    ctx.textBaseline = 'middle'
    ctx.fillText(label, node.x, node.y)
  }, [selectedNode])

  const linkCanvasObject = useCallback((link, ctx, globalScale) => {
    const fontSize = Math.max(9 / globalScale, 2)
    ctx.font = `${fontSize}px sans-serif`
    const midX = (link.source.x + link.target.x) / 2
    const midY = (link.source.y + link.target.y) / 2
    ctx.fillStyle = selectedEdge === link.id ? '#FF3621' : '#94a3b8'
    ctx.textAlign = 'center'
    ctx.textBaseline = 'middle'
    ctx.fillText(link.label || '', midX, midY - fontSize)
  }, [selectedEdge])

  const yamlPreview = useMemo(() => stateToYaml(state), [state])

  const selectedEntity = selectedNode ? state.entities[selectedNode] : null
  const selectedEdgeObj = selectedEdge ? state.edge_catalog[selectedEdge] : null

  return (
    <div>
      <PageHeader title="Build Ontology" subtitle="Visual entity, relationship, and property editor with LLM suggestions"
        badge={loadedBundle || 'New'}
        actions={
          <div className="flex items-center gap-2">
            <button onClick={() => dispatch({ type: 'UNDO' })} disabled={!canUndo} className="btn-ghost btn-sm" title="Undo (Ctrl+Z)">Undo</button>
            <button onClick={() => dispatch({ type: 'REDO' })} disabled={!canRedo} className="btn-ghost btn-sm" title="Redo (Ctrl+Y)">Redo</button>
            <button onClick={() => setShowYaml(!showYaml)} className="btn-ghost btn-sm">{showYaml ? 'Hide YAML' : 'Show YAML'}</button>
            <button onClick={saveBundle} disabled={saving} className="btn-primary btn-sm">{saving ? 'Saving...' : 'Save Bundle'}</button>
          </div>
        } />

      <ErrorBanner error={error} onDismiss={() => setError(null)} />
      {saveMsg && <div className="card border-l-4 border-l-emerald-500 px-4 py-2 text-sm text-emerald-700 dark:text-emerald-300 mb-4 animate-slide-up">{saveMsg}</div>}

      {/* Setup Bar */}
      <div className="card p-4 mb-4 space-y-3">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          {/* Bundle loader */}
          <label className="block">
            <span className="text-xs text-slate-500 dark:text-slate-400">Load Bundle</span>
            <select value={loadedBundle} onChange={e => loadBundle(e.target.value)} className="select-base !text-sm mt-1">
              <option value="">Start blank</option>
              {bundles.map(b => <option key={b.key} value={b.key}>{b.name || b.key}</option>)}
            </select>
          </label>

          {/* Metadata */}
          <label className="block">
            <span className="text-xs text-slate-500 dark:text-slate-400">Bundle Name</span>
            <input value={state.metadata.name} onChange={e => dispatch({ type: 'SET_METADATA', payload: { name: e.target.value } })}
              className="input-base !text-sm mt-1" placeholder="My Custom Ontology" />
          </label>
          <label className="block">
            <span className="text-xs text-slate-500 dark:text-slate-400">Industry</span>
            <input value={state.metadata.industry} onChange={e => dispatch({ type: 'SET_METADATA', payload: { industry: e.target.value } })}
              className="input-base !text-sm mt-1" placeholder="e.g. healthcare, finance" />
          </label>
          <label className="block">
            <span className="text-xs text-slate-500 dark:text-slate-400">Stakeholder Domain</span>
            <input value={domain} onChange={e => setDomain(e.target.value)}
              className="input-base !text-sm mt-1" placeholder="e.g. clinical operations" />
          </label>
        </div>

        {/* Table context for suggestions */}
        <details className="text-xs">
          <summary className="cursor-pointer text-slate-500 dark:text-slate-400 font-medium">Table context for LLM suggestions</summary>
          <div className="mt-2 grid grid-cols-3 gap-2">
            <select value={cst.catalog} onChange={e => cst.setCatalog(e.target.value)} className="select-base !text-xs">
              <option value="">Catalog...</option>
              {cst.catalogs.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
            <select value={cst.schema} onChange={e => cst.setSchema(e.target.value)} className="select-base !text-xs">
              <option value="">Schema...</option>
              {cst.schemas.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            <input value={cst.filter} onChange={e => cst.setFilter(e.target.value)} placeholder="Filter tables..." className="input-base !text-xs" />
          </div>
          {cst.filtered.length > 0 && (
            <div className="mt-2 max-h-32 overflow-y-auto border border-slate-200 dark:border-dbx-navy-400/30 rounded-lg p-2 space-y-0.5">
              {cst.filtered.map(t => (
                <label key={t} className="flex items-center gap-2 cursor-pointer hover:bg-slate-50 dark:hover:bg-dbx-navy-500/30 px-1 py-0.5 rounded">
                  <input type="checkbox" checked={selectedTables.has(t)}
                    onChange={e => {
                      const next = new Set(selectedTables)
                      e.target.checked ? next.add(t) : next.delete(t)
                      setSelectedTables(next)
                    }} />
                  <span className="text-xs text-slate-600 dark:text-slate-300 truncate">{t}</span>
                </label>
              ))}
            </div>
          )}
          {selectedTables.size > 0 && <p className="text-[10px] text-slate-400 mt-1">{selectedTables.size} table(s) selected for LLM context</p>}
        </details>

        {/* Description */}
        <label className="block">
          <span className="text-xs text-slate-500 dark:text-slate-400">Description</span>
          <input value={state.metadata.description} onChange={e => dispatch({ type: 'SET_METADATA', payload: { description: e.target.value } })}
            className="input-base !text-xs mt-1" placeholder="A brief description of this ontology bundle" />
        </label>
      </div>

      {/* Suggestion Actions */}
      <div className="flex flex-wrap gap-2 mb-4">
        <button onClick={suggestEntities} disabled={suggesting} className="btn-primary btn-sm">
          {suggesting ? 'Suggesting...' : 'Suggest Entities'}
        </button>
        <button onClick={suggestRelationships} disabled={suggestingRels || entityNames.length < 2} className="btn-secondary btn-sm">
          {suggestingRels ? 'Suggesting...' : 'Suggest Relationships'}
        </button>
        <button onClick={() => setShowAddEntity(true)} className="btn-ghost btn-sm">+ Entity</button>
        <button onClick={() => { setAddEdgeSource(''); setAddEdgeTarget(''); setShowAddEdge(true) }} className="btn-ghost btn-sm">+ Relationship</button>
        <span className="text-xs text-slate-400 self-center ml-2">{entityNames.length} entities, {Object.keys(state.edge_catalog).length} relationships</span>
      </div>

      {/* Entity Suggestions */}
      {suggestions.length > 0 && (
        <div className="mb-4 space-y-2">
          <p className="text-xs font-semibold text-slate-600 dark:text-slate-300">Entity Suggestions</p>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
            {suggestions.map(s => (
              <SuggestionCard key={s.name} item={s} type="entity" onAccept={acceptEntity}
                onDismiss={() => setSuggestions(prev => prev.filter(x => x.name !== s.name))} />
            ))}
          </div>
        </div>
      )}

      {/* Relationship Suggestions */}
      {relSuggestions.length > 0 && (
        <div className="mb-4 space-y-2">
          <p className="text-xs font-semibold text-slate-600 dark:text-slate-300">Relationship Suggestions</p>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
            {relSuggestions.map(s => (
              <SuggestionCard key={s.name} item={{ ...s, description: `${s.source_entity} -> ${s.target_entity}` }} type="relationship" onAccept={acceptRelationship}
                onDismiss={() => setRelSuggestions(prev => prev.filter(x => x.name !== s.name))} />
            ))}
          </div>
        </div>
      )}

      {/* Main Layout: Canvas + Detail Panel */}
      <div className="flex gap-4">
        {/* Canvas */}
        <div className="flex-1 card overflow-hidden" style={{ minHeight: 500 }}>
          {entityNames.length === 0 ? (
            <EmptyState title="No entities yet" description="Add entities manually or use Suggest Entities to get started." />
          ) : (
            <ForceGraph2D
              ref={graphRef}
              graphData={graphData}
              nodeCanvasObject={nodeCanvasObject}
              nodePointerAreaPaint={(node, color, ctx) => {
                const fontSize = 12
                ctx.font = `bold ${fontSize}px sans-serif`
                const w = ctx.measureText(node.label).width + fontSize * 1.5
                const h = fontSize * 2
                ctx.fillStyle = color
                ctx.fillRect(node.x - w / 2, node.y - h / 2, w, h)
              }}
              linkCanvasObjectMode={() => 'after'}
              linkCanvasObject={linkCanvasObject}
              linkDirectionalArrowLength={6}
              linkDirectionalArrowRelPos={0.85}
              linkColor={() => '#cbd5e1'}
              onNodeClick={(node) => { setSelectedNode(node.id); setSelectedEdge(null) }}
              onLinkClick={(link) => { setSelectedEdge(link.id); setSelectedNode(null) }}
              onBackgroundClick={() => { setSelectedNode(null); setSelectedEdge(null) }}
              enableNodeDrag={true}
              cooldownTicks={60}
              width={undefined}
              height={480}
            />
          )}
        </div>

        {/* Detail Panel */}
        <div className="w-80 flex-shrink-0 space-y-4">
          <div className="card p-4 max-h-[600px] overflow-y-auto scrollbar-thin">
            {selectedEntity ? (
              <>
                <EntityForm
                  entity={selectedEntity}
                  entityName={selectedNode}
                  allEntities={entityNames}
                  onUpdate={(updates) => dispatch({ type: 'UPDATE_ENTITY', payload: { name: selectedNode, updates } })}
                  onDelete={() => { dispatch({ type: 'REMOVE_ENTITY', payload: selectedNode }); setSelectedNode(null) }}
                  onSuggestProps={suggestProperties}
                  suggestingProps={suggestingProps}
                />
                {/* Property Suggestions */}
                {propSuggestions.length > 0 && (
                  <div className="mt-3 border-t border-slate-200 dark:border-dbx-navy-400/30 pt-3 space-y-2">
                    <p className="text-xs font-semibold text-slate-600 dark:text-slate-300">Property Suggestions</p>
                    {propSuggestions.map(s => (
                      <SuggestionCard key={s.name} item={s} type="property" onAccept={acceptProperty}
                        onDismiss={() => setPropSuggestions(prev => prev.filter(x => x.name !== s.name))} />
                    ))}
                  </div>
                )}
              </>
            ) : selectedEdgeObj ? (
              <RelationshipForm
                edgeName={selectedEdge}
                edge={selectedEdgeObj}
                entityNames={entityNames}
                onUpdate={(updates) => dispatch({ type: 'UPDATE_EDGE', payload: { name: selectedEdge, updates } })}
                onDelete={() => { dispatch({ type: 'REMOVE_EDGE', payload: selectedEdge }); setSelectedEdge(null) }}
              />
            ) : (
              <div className="text-center py-8">
                <p className="text-sm text-slate-400 dark:text-slate-500">Select a node or edge to edit</p>
                <p className="text-xs text-slate-400 dark:text-slate-500 mt-1">Or use the buttons above to add entities and relationships</p>
              </div>
            )}
          </div>

          {/* Domain List */}
          <details className="card p-4">
            <summary className="text-xs font-semibold text-slate-600 dark:text-slate-300 cursor-pointer">Domains ({state.domains.length})</summary>
            <div className="mt-2 space-y-2">
              {state.domains.map((d, i) => (
                <div key={i} className="flex items-center gap-2 text-xs">
                  <input value={d.name} onChange={e => dispatch({ type: 'UPDATE_DOMAIN', payload: { index: i, updates: { name: e.target.value } } })}
                    className="input-base !text-xs flex-1" placeholder="Domain name" />
                  <button onClick={() => dispatch({ type: 'REMOVE_DOMAIN', payload: i })} className="text-red-400 hover:text-red-600">&times;</button>
                </div>
              ))}
              <button onClick={() => dispatch({ type: 'ADD_DOMAIN', payload: { name: '', description: '', keywords: [] } })}
                className="text-xs text-dbx-teal hover:underline">+ Add domain</button>
            </div>
          </details>
        </div>
      </div>

      {/* YAML Preview */}
      {showYaml && (
        <div className="card p-4 mt-4">
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-semibold text-slate-600 dark:text-slate-300">YAML Preview</span>
            <button onClick={() => {
              const blob = new Blob([yamlPreview], { type: 'text/yaml' })
              const url = URL.createObjectURL(blob)
              const a = document.createElement('a')
              a.href = url; a.download = `${state.metadata.name || 'ontology'}.yaml`; a.click()
              URL.revokeObjectURL(url)
            }} className="btn-ghost btn-sm">Download YAML</button>
          </div>
          <pre className="bg-slate-50 dark:bg-dbx-navy-700 p-4 rounded-lg text-xs font-mono overflow-x-auto max-h-96 overflow-y-auto scrollbar-thin text-slate-700 dark:text-slate-300 whitespace-pre">
            {yamlPreview}
          </pre>
        </div>
      )}

      {/* Modals */}
      <AddEntityModal open={showAddEntity} onClose={() => setShowAddEntity(false)}
        onAdd={(name, desc) => dispatch({ type: 'ADD_ENTITY', payload: { name, description: desc } })} />
      <AddEdgeModal open={showAddEdge} onClose={() => setShowAddEdge(false)} entityNames={entityNames}
        source={addEdgeSource} target={addEdgeTarget}
        onAdd={(name, edge) => dispatch({ type: 'ADD_EDGE', payload: { name, edge } })} />
    </div>
  )
}
