import assert from 'node:assert/strict'
import { test } from 'node:test'
import { addDemoOrders, advanceDemoOrder, makeDemoData } from './demo.js'

const count = (data, status) => data.stats.items.find((item) => item.stat_key === `count_${status}`)?.stat_value || 0

test('delivery updates the order, counts, note, and timeline without changing the original data', () => {
  const original = makeDemoData()
  const order = original.orders.find((item) => item.status === 'SHIPPED')
  const updated = advanceDemoOrder(original, order.order_id, 'DELIVERED', '  At reception  ')
  const current = updated.orders.find((item) => item.order_id === order.order_id)
  assert.equal(current.status, 'DELIVERED')
  assert.deepEqual(current.allowed_statuses, [])
  assert.equal(count(updated, 'shipped'), count(original, 'shipped') - 1)
  assert.equal(count(updated, 'delivered'), count(original, 'delivered') + 1)
  assert.equal(updated.stats.total_orders, original.stats.total_orders)
  assert.deepEqual(updated.histories[order.order_id].slice(0, -1), original.histories[order.order_id])
  assert.equal(updated.histories[order.order_id].at(-1).note, 'At reception')
  assert.ok(new Date(current.updated_at) > new Date(order.updated_at))
  assert.equal(order.status, 'SHIPPED')
})

test('an order progresses through every delivery stage', () => {
  let data = makeDemoData()
  const id = data.orders.find((order) => order.status === 'CREATED').order_id
  for (const status of ['CONFIRMED', 'PROCESSING', 'SHIPPED', 'DELIVERED']) {
    data = advanceDemoOrder(data, id, status)
  }
  assert.deepEqual(data.histories[id].map((event) => event.status), ['CREATED', 'CONFIRMED', 'PROCESSING', 'SHIPPED', 'DELIVERED'])
  assert.equal(data.stats.items.reduce((sum, item) => sum + item.stat_value, 0), data.orders.length)
})

test('skipping stages and advancing completed or cancelled orders are rejected', () => {
  const data = makeDemoData()
  for (const status of ['CREATED', 'DELIVERED', 'CANCELLED', 'RETURN_IN_TRANSIT']) {
    const order = data.orders.find((item) => item.status === status)
    assert.throws(() => advanceDemoOrder(data, order.order_id, 'DELIVERED'), /transition is not available/)
  }
})

test('sample orders have unique IDs, creation events, and can be delivered manually', () => {
  const original = makeDemoData()
  let data = addDemoOrders(original)
  const added = data.orders.slice(0, 5)
  assert.equal(data.orders.length, original.orders.length + 5)
  assert.equal(data.stats.total_orders, original.stats.total_orders + 5)
  assert.equal(count(data, 'created'), count(original, 'created') + 5)
  assert.equal(new Set(data.orders.map((order) => order.order_id)).size, data.orders.length)
  for (const order of added) {
    assert.equal(order.status, 'CREATED')
    assert.deepEqual(order.allowed_statuses, ['CONFIRMED'])
    assert.ok(order.customer_id && order.seller_id && order.product_name)
    assert.ok(order.quantity > 0 && order.total_amount > 0)
    assert.equal(data.histories[order.order_id].length, 1)
    assert.equal(data.histories[order.order_id][0].status, 'CREATED')
  }
  for (const status of ['CONFIRMED', 'PROCESSING', 'SHIPPED', 'DELIVERED']) {
    data = advanceDemoOrder(data, added[0].order_id, status)
  }
  assert.equal(data.orders[0].status, 'DELIVERED')
  assert.equal(data.histories[added[0].order_id].length, 5)
  assert.equal(original.orders.length, 9)
  assert.deepEqual(data.histories[original.orders[0].order_id], original.histories[original.orders[0].order_id])
})

test('multiple sample batches remain distinct and excessive batch sizes are rejected', () => {
  let data = makeDemoData()
  data = addDemoOrders(addDemoOrders(data))
  assert.equal(data.orders.length, 19)
  assert.equal(new Set(data.orders.map((order) => order.order_id)).size, 19)
  for (const count of [0, 11, 1.5, '5']) assert.throws(() => addDemoOrders(data, count), /between 1 and 10/)
})
