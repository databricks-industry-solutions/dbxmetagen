// Join-clause helpers for the ERD designer (plain JS so they can be unit-tested
// with node, without a JSX build).

// Every "<qualified> = <qualified>" pair of an ON clause, in order, as
// [{src, dst}]. The column is the LAST dotted segment on each side, so this works
// whether a side is `alias.col`, `table.col`, or fully-qualified
// `catalog.schema.table.col`. A composite `a.x = b.y AND a.z = b.w` yields both
// pairs; a single-column clause yields one.
export function parseOnPairs(on) {
  const re = /([\w.`]+)\s*=\s*([\w.`]+)/g
  const lastSeg = s => (s.split('.').pop() || '').replace(/`/g, '').trim()
  const pairs = []
  let m
  while ((m = re.exec(on || '')) !== null) {
    const src = lastSeg(m[1])
    const dst = lastSeg(m[2])
    if (src && dst) pairs.push({ src, dst })
  }
  return pairs
}
