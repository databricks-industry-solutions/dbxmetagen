// Plain-node tests for ERD join helpers. No test framework required.
//   node src/components/erdJoin.test.mjs
import assert from 'node:assert/strict'
import { parseOnPairs } from './erdJoin.js'

let passed = 0
function test(name, fn) {
  fn()
  passed++
  console.log(`ok - ${name}`)
}

test('single-column clause yields one pair', () => {
  assert.deepEqual(parseOnPairs('src.customer_id = dim_customer.id'), [{ src: 'customer_id', dst: 'id' }])
})

test('composite clause yields every pair in order', () => {
  assert.deepEqual(
    parseOnPairs('src.guid = user_mst.guid AND src.base_dt = user_mst.base_dt'),
    [{ src: 'guid', dst: 'guid' }, { src: 'base_dt', dst: 'base_dt' }],
  )
})

test('fully-qualified sides use the last segment (not the catalog)', () => {
  assert.deepEqual(
    parseOnPairs('cat.sch.orders.customer_id = cat.sch.customers.id'),
    [{ src: 'customer_id', dst: 'id' }],
  )
})

test('backticks are stripped', () => {
  assert.deepEqual(parseOnPairs('source.`order_id` = o.`id`'), [{ src: 'order_id', dst: 'id' }])
})

test('empty or unparseable input yields no pairs', () => {
  assert.deepEqual(parseOnPairs(''), [])
  assert.deepEqual(parseOnPairs(null), [])
  assert.deepEqual(parseOnPairs('guid AND base_dt'), [])
})

console.log(`# ${passed} passed`)
