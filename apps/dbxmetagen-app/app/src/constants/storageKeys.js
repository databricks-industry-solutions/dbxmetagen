// Shared localStorage keys that form a cross-component contract (one component
// writes, another reads). Centralized so a typo can't silently break the handoff.
export const LS_ONTOLOGY_BUNDLE = 'dbxmetagen_ontologyBundle'
