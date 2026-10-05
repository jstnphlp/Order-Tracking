const records = [
  ['ORD-A3F2C1B0', 'CUST-1042', 'Wireless Headphones', 'SELLER-101', 1, 2490, 'SHIPPED'],
  ['ORD-7B8E2D41', 'CUST-2081', 'Everyday Backpack', 'SELLER-104', 1, 1850, 'DELIVERED'],
  ['ORD-9C4A6F12', 'CUST-1093', 'Mechanical Keyboard', 'SELLER-102', 1, 3290, 'PROCESSING'],
  ['ORD-2D5F8A60', 'CUST-3022', 'Canvas Sneakers', 'SELLER-103', 2, 2980, 'CONFIRMED'],
  ['ORD-5E1B7C93', 'CUST-1042', 'Ceramic Coffee Set', 'SELLER-105', 1, 1290, 'CREATED'],
  ['ORD-8F3D2A17', 'CUST-4017', 'Portable Speaker', 'SELLER-101', 1, 1990, 'RETURN_IN_TRANSIT'],
  ['ORD-4A6C9E25', 'CUST-2095', 'Linen Tote Bag', 'SELLER-104', 1, 690, 'DELIVERED'],
  ['ORD-1B7E3D84', 'CUST-3018', 'Desk Organizer', 'SELLER-105', 1, 890, 'CANCELLED'],
  ['ORD-6C2A8F39', 'CUST-5012', 'Running Shoes', 'SELLER-103', 1, 3490, 'REFUNDED'],
]

const transitions = { CREATED: 'CONFIRMED', CONFIRMED: 'PROCESSING', PROCESSING: 'SHIPPED', SHIPPED: 'DELIVERED' }
const actions = (status) => transitions[status] ? [transitions[status]] : []

function summarize(orders, updatedAt) {
  const counts = orders.reduce((result, order) => {
    const key = `count_${order.status.toLowerCase()}`
    result[key] = (result[key] || 0) + 1
    return result
  }, {})
  return {
    total_orders: orders.length,
    updated_at: updatedAt,
    items: Object.entries(counts).map(([stat_key, stat_value]) => ({ stat_key, stat_value })),
  }
}

export function makeDemoData() {
  const now = Date.now()
  const orders = records.map((record, index) => ({
    ...Object.fromEntries(['order_id', 'customer_id', 'product_name', 'seller_id', 'quantity', 'total_amount', 'status'].map((key, i) => [key, record[i]])),
    updated_at: new Date(now - index * 180000).toISOString(),
    allowed_statuses: actions(record[6]),
  }))
  return {
    orders,
    stats: summarize(orders, new Date(now).toISOString()),
    histories: Object.fromEntries(orders.map((order) => [order.order_id, demoHistory(order)])),
  }
}

export function advanceDemoOrder(data, orderId, status, note = '') {
  const order = data.orders.find((item) => item.order_id === orderId)
  if (!order || transitions[order.status] !== status) throw new Error('This delivery transition is not available.')
  const timestamp = new Date(Math.max(Date.now(), new Date(order.updated_at).getTime() + 1)).toISOString()
  const orders = data.orders.map((item) => item.order_id === orderId ? {
    ...item, status, updated_at: timestamp, allowed_statuses: actions(status),
  } : item)
  return {
    orders,
    stats: summarize(orders, timestamp),
    histories: {
      ...data.histories,
      [orderId]: [...data.histories[orderId], { status, event_time: timestamp, note: note.trim() || notes[status] }],
    },
  }
}

export function addDemoOrders(data, count = 5) {
  if (!Number.isInteger(count) || count < 1 || count > 10) throw new Error('Add between 1 and 10 sample orders.')
  const now = Date.now()
  const added = Array.from({ length: count }, (_, index) => {
    const record = records[index % 5]
    const quantity = index % 3 + 1
    return {
      order_id: `ORD-DEMO-${crypto.randomUUID().slice(0, 12).toUpperCase()}`,
      customer_id: `CUST-${crypto.randomUUID().slice(0, 8).toUpperCase()}`,
      product_name: record[2], seller_id: record[3], quantity,
      total_amount: Math.round(record[5] / record[4] * quantity * 100) / 100,
      status: 'CREATED', updated_at: new Date(now + index).toISOString(),
      allowed_statuses: ['CONFIRMED'],
    }
  })
  const orders = [...added, ...data.orders]
  return {
    orders,
    stats: summarize(orders, added.at(-1).updated_at),
    histories: {
      ...data.histories,
      ...Object.fromEntries(added.map((order) => [order.order_id, [{
        status: 'CREATED', event_time: order.updated_at, note: 'Sample order added from dashboard',
      }]])),
    },
  }
}

const delivery = ['CREATED', 'CONFIRMED', 'PROCESSING', 'SHIPPED', 'DELIVERED']
const returns = ['RETURN_REQUESTED', 'RETURN_APPROVED', 'RETURN_IN_TRANSIT', 'RETURN_RECEIVED', 'REFUNDED']
const notes = {
  CREATED: 'Order received. The journey begins.',
  CONFIRMED: 'Payment confirmed and order accepted.',
  PROCESSING: 'The seller is preparing your parcel.',
  SHIPPED: 'Parcel dispatched and on its way.',
  DELIVERED: 'Parcel delivered to the customer.',
  CANCELLED: 'Order cancelled before dispatch.',
  RETURN_REQUESTED: 'Customer requested a return.',
  RETURN_APPROVED: 'Return approved by the seller.',
  RETURN_IN_TRANSIT: 'Parcel is on its way back to the seller.',
  RETURN_RECEIVED: 'Seller received the returned parcel.',
  REFUNDED: 'Refund completed.',
}

export function demoHistory(order) {
  let statuses = delivery.slice(0, delivery.indexOf(order.status) + 1)
  if (order.status === 'CANCELLED') statuses = ['CREATED', 'CANCELLED']
  if (returns.includes(order.status)) statuses = [...delivery, ...returns.slice(0, returns.indexOf(order.status) + 1)]
  return statuses.map((status, index) => ({
    status,
    note: notes[status],
    event_time: new Date(new Date(order.updated_at).getTime() - (statuses.length - index - 1) * 3600000).toISOString(),
  }))
}
