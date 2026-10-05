async function requestJson(path, signal, options = {}) {
  const response = await fetch(`/api${path}`, {
    ...options,
    signal: AbortSignal.any([signal, AbortSignal.timeout(15000)]),
  })
  if (!response.ok) {
    let message = 'Could not reach the order service. Check that the API and Cassandra are running.'
    let details
    try {
      const body = await response.json()
      if (typeof body.detail === 'string') message = body.detail
      else if (typeof body.detail?.message === 'string') {
        message = body.detail.message
        details = body.detail
      }
    } catch { /* The proxy may return a plain-text error. */ }
    const error = new Error(message)
    error.details = details
    throw error
  }
  return response.json()
}

export function getJson(path, signal) {
  return requestJson(path, signal)
}

export function updateOrderStatus(order, status, note, signal) {
  return requestJson(`/orders/${encodeURIComponent(order.order_id)}/status`, signal, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      status,
      expected_status: order.status,
      expected_updated_at: order.updated_at,
      note,
    }),
  })
}

export function createSampleOrders(signal) {
  return requestJson('/orders/sample', signal, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ count: 5 }),
  })
}
