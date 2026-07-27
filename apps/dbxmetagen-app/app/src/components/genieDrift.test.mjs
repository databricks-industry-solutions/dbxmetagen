// Plain-node tests for Genie drift helpers. No test framework required.
//   node src/components/genieDrift.test.mjs
import assert from 'node:assert/strict'
import { canonicalize, spacesEqual } from './genieDrift.js'

let passed = 0
function test(name, fn) {
  fn()
  passed++
  console.log(`ok - ${name}`)
}

// A "tracked" space (as build_serialized_space would produce it): hex ids, the
// --rt marker on join SQL, one key order.
const tracked = {
  version: 2,
  data_sources: { tables: [{ identifier: 'c.s.orders' }, { identifier: 'c.s.customers' }] },
  instructions: {
    join_specs: [{
      id: 'aaaa1111bbbb2222',
      left: { identifier: 'c.s.orders', alias: 'orders' },
      right: { identifier: 'c.s.customers', alias: 'customers' },
      sql: ['`orders`.`customer_id` = `customers`.`id`', '--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--'],
    }],
  },
}

// The SAME space as the Genie API re-serializes it: different id, keys in a
// different order, marker still present. Logically identical.
const liveSameLogical = {
  data_sources: { tables: [{ identifier: 'c.s.orders' }, { identifier: 'c.s.customers' }] },
  version: 2,
  instructions: {
    join_specs: [{
      right: { alias: 'customers', identifier: 'c.s.customers' },
      left: { alias: 'orders', identifier: 'c.s.orders' },
      id: 'ffff9999eeee8888',
      sql: ['`orders`.`customer_id` = `customers`.`id`', '--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--'],
    }],
  },
}

test('logically identical spaces (diff ids + key order) compare equal -> no false drift', () => {
  assert.equal(spacesEqual(tracked, liveSameLogical), true)
})

test('canonicalize strips id and drops the --rt marker entry', () => {
  const c = canonicalize(tracked)
  const js = c.instructions.join_specs[0]
  assert.equal('id' in js, false)
  // marker entry is dropped entirely; only the real predicate survives
  assert.deepEqual(js.sql, ['`orders`.`customer_id` = `customers`.`id`'])
})

test('presence vs absence of the --rt marker does NOT count as drift', () => {
  const withMarker = { instructions: { join_specs: [{ sql: ['a.x = b.y', '--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--'] }] } }
  const withoutMarker = { instructions: { join_specs: [{ sql: ['a.x = b.y'] }] } }
  assert.equal(spacesEqual(withMarker, withoutMarker), true)
})

test('a genuine external edit (added table) compares unequal -> real drift', () => {
  const edited = JSON.parse(JSON.stringify(liveSameLogical))
  edited.data_sources.tables.push({ identifier: 'c.s.products' })
  assert.equal(spacesEqual(tracked, edited), false)
})

test('a genuine external edit (changed join predicate) compares unequal', () => {
  const edited = JSON.parse(JSON.stringify(liveSameLogical))
  edited.instructions.join_specs[0].sql[0] = '`orders`.`cust_id` = `customers`.`id`'
  assert.equal(spacesEqual(tracked, edited), false)
})

test('empty / null spaces are equal to each other and to {}', () => {
  assert.equal(spacesEqual(null, {}), true)
  assert.equal(spacesEqual(undefined, null), true)
})

test('array element ORDER is NOT significant (Genie GETs are not order-stable)', () => {
  // Same set of tables/joins in a different order is the same logical space.
  const reordered = { data_sources: { tables: [{ identifier: 'b' }, { identifier: 'a' }] } }
  const original = { data_sources: { tables: [{ identifier: 'a' }, { identifier: 'b' }] } }
  assert.equal(spacesEqual(reordered, original), true)
})

test('array MEMBERSHIP change (added element) is still a real diff', () => {
  const two = { data_sources: { tables: [{ identifier: 'a' }, { identifier: 'b' }] } }
  const three = { data_sources: { tables: [{ identifier: 'b' }, { identifier: 'a' }, { identifier: 'c' }] } }
  assert.equal(spacesEqual(two, three), false)
})

test('reordered join_specs (only ids/order differ) compare equal', () => {
  const a = { instructions: { join_specs: [
    { id: '1', left: { identifier: 'x' }, sql: ['x.a = y.b'] },
    { id: '2', left: { identifier: 'y' }, sql: ['y.c = z.d'] },
  ] } }
  const b = { instructions: { join_specs: [
    { id: '9', left: { identifier: 'y' }, sql: ['y.c = z.d'] },
    { id: '8', left: { identifier: 'x' }, sql: ['x.a = y.b'] },
  ] } }
  assert.equal(spacesEqual(a, b), true)
})

console.log(`\n${passed} passed`)
