// Shared non-display constants for graph visualizations.
// Node-type → hex color. Used by the force-graph canvas (GraphSubgraph) and the
// Graph Explorer legend/filter chips so the two stay in lockstep. Display labels
// live in i18n (gsg.nodeType.*), not here.
export const NODE_COLORS = {
  table: '#3b82f6',
  column: '#10b981',
  schema: '#f59e0b',
  entity: '#8b5cf6',
}
