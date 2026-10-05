import { useEffect, useRef, useState } from 'react'
import Icon from './Icons.jsx'
import RouteIllustration from './RouteIllustration.jsx'
import { createSampleOrders, getJson, updateOrderStatus } from './api.js'
import { addDemoOrders, advanceDemoOrder, makeDemoData } from './demo.js'
import OrderControls from './OrderControls.jsx'

const STATUS_LABELS = {
  CREATED: 'Created', CONFIRMED: 'Confirmed', PROCESSING: 'Processing',
  SHIPPED: 'Shipped', DELIVERED: 'Delivered', CANCELLED: 'Cancelled',
  RETURN_REQUESTED: 'Return requested', RETURN_APPROVED: 'Return approved',
  RETURN_IN_TRANSIT: 'Return in transit', RETURN_RECEIVED: 'Return received',
  REFUNDED: 'Refunded', RETURN_REJECTED: 'Return rejected',
  RETURNING_TO_BUYER: 'Returning to buyer', RETURNED_TO_BUYER: 'Returned to buyer',
}
const label = (status) => STATUS_LABELS[status] || status?.replaceAll('_', ' ').toLowerCase() || 'Unknown'
const isReturn = (status) => status?.startsWith('RETURN') || status === 'REFUNDED'
const statusTone = (status) => status === 'DELIVERED' ? 'green' : status === 'SHIPPED' ? 'blue' : isReturn(status) ? 'purple' : status === 'CANCELLED' ? 'gray' : 'amber'
const time = (value, options = {}) => value ? new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', ...options }) : '—'
const date = (value) => value ? new Date(value).toLocaleDateString([], { month: 'short', day: 'numeric' }) : '—'
const money = (value) => value == null ? '—' : new Intl.NumberFormat('en-PH', { style: 'currency', currency: 'PHP', maximumFractionDigits: 2 }).format(value)
const EMPTY = { orders: [], stats: { total_orders: 0, items: [], updated_at: null } }

function StatusBadge({ status }) {
  return <span className={`status-badge ${statusTone(status)}`}><span />{label(status)}</span>
}

function StatCard({ title, value, icon, tone, subtitle }) {
  return <article className="stat-card"><div className="stat-top"><span>{title}</span><span className={`stat-icon ${tone}`}><Icon name={icon} size={19} /></span></div><strong>{value.toLocaleString()}</strong><span className="stat-subtitle">{subtitle}</span></article>
}

function exportOrders(orders) {
  const fields = ['order_id', 'customer_id', 'product_name', 'seller_id', 'quantity', 'total_amount', 'status', 'updated_at']
  const escape = (value) => {
    let text = String(value ?? '')
    if (/^[=+\-@\t\r]/.test(text)) text = `'${text}`
    return `"${text.replaceAll('"', '""')}"`
  }
  const csv = [fields.join(','), ...orders.map((order) => fields.map((field) => escape(order[field])).join(','))].join('\r\n')
  const url = URL.createObjectURL(new Blob(['\uFEFF', csv], { type: 'text/csv;charset=utf-8' }))
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `parcel-orders-${new Date().toISOString().slice(0, 10)}.csv`
  anchor.click()
  URL.revokeObjectURL(url)
}

export default function App() {
  const [mode, setMode] = useState('live')
  const [data, setData] = useState(EMPTY)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [lastSync, setLastSync] = useState(null)
  const [refresh, setRefresh] = useState(0)
  const [search, setSearch] = useState('')
  const [filter, setFilter] = useState('ALL')
  const [section, setSection] = useState('overview')
  const [selectedId, setSelectedId] = useState(null)
  const [history, setHistory] = useState([])
  const [historyLoading, setHistoryLoading] = useState(false)
  const [historyError, setHistoryError] = useState('')
  const [demo, setDemo] = useState(makeDemoData)
  const [actionBusy, setActionBusy] = useState(false)
  const [actions, setActions] = useState({})
  const [creation, setCreation] = useState(null)
  const [trackedIds, setTrackedIds] = useState([])
  const actionRequest = useRef(null)
  const ordersRef = useRef(null)
  const selected = data.orders.find((order) => order.order_id === selectedId)

  useEffect(() => () => actionRequest.current?.abort(), [])

  useEffect(() => {
    if (mode !== 'live') return
    setCreation((previous) => {
      if (!previous?.ids?.length || !['waiting', 'error'].includes(previous.phase)) return previous
      const loaded = previous.ids.filter((id) => data.orders.some((order) => order.order_id === id)).length
      if (loaded === previous.ids.length) return {
        ...previous, phase: 'done', message: `${loaded} sample orders are ready. Select one to start its journey.`,
      }
      if (previous.phase === 'error' || previous.loaded === loaded) return previous
      return { ...previous, loaded, message: `Waiting for new orders to appear (${loaded}/${previous.ids.length}).` }
    })
  }, [data.orders, mode])

  useEffect(() => {
    if (mode !== 'live') return
    setActions((previous) => {
      let changed = false
      const next = { ...previous }
      for (const [id, action] of Object.entries(previous)) {
        const current = data.orders.find((order) => order.order_id === id)
        if (action.phase !== 'waiting' || !current) continue
        const applied = current.status === action.status ||
          new Date(current.updated_at).getTime() >= new Date(action.timestamp).getTime()
        if (applied) {
          changed = true
          next[id] = { ...action, phase: 'done', message: `Order updated. Current status: ${label(current.status)}.` }
        }
      }
      return changed ? next : previous
    })
  }, [data.orders, mode])

  useEffect(() => {
    if (mode === 'demo') {
      setData(demo)
      setSelectedId((id) => demo.orders.some((order) => order.order_id === id) ? id : demo.orders[0].order_id)
      setLoading(false)
      setError('')
      return
    }
    let active = true
    let timer
    const controller = new AbortController()
    async function poll() {
      try {
        const query = new URLSearchParams({ limit: '100' })
        trackedIds.forEach((id) => query.append('tracked', id))
        const [orders, stats] = await Promise.all([
          getJson(`/orders?${query}`, controller.signal), getJson('/stats', controller.signal),
        ])
        if (!active) return
        setData({ orders: orders.items, stats })
        setSelectedId((id) => orders.items.some((order) => order.order_id === id) || trackedIds.includes(id) ? id : orders.items[0]?.order_id ?? null)
        setError('')
        setLastSync(new Date().toISOString())
      } catch (failure) {
        if (active) setError(failure.message || 'The order service is unavailable.')
      } finally {
        if (active) {
          setLoading(false)
          timer = setTimeout(poll, 5000)
        }
      }
    }
    poll()
    return () => { active = false; controller.abort(); clearTimeout(timer) }
  }, [mode, refresh, demo, trackedIds])

  useEffect(() => {
    setHistory([])
    setHistoryError('')
    if (!selectedId) { setHistoryLoading(false); return }
    if (mode === 'demo') {
      setHistory(demo.histories[selectedId] || [])
      setHistoryLoading(false)
      return
    }
    let active = true
    let timer
    const controller = new AbortController()
    setHistoryLoading(true)
    async function poll() {
      try {
        const result = await getJson(`/orders/${encodeURIComponent(selectedId)}/history`, controller.signal)
        if (active) { setHistory(result.items); setHistoryError('') }
      } catch (failure) {
        if (active) setHistoryError(failure.message || 'Unable to load history.')
      } finally {
        if (active) { setHistoryLoading(false); timer = setTimeout(poll, 5000) }
      }
    }
    poll()
    return () => { active = false; controller.abort(); clearTimeout(timer) }
  }, [selectedId, mode, refresh, demo])

  const counts = Object.fromEntries(data.stats.items.map((item) => [item.stat_key.replace(/^count_/, '').toUpperCase(), item.stat_value || 0]))
  const count = (...statuses) => statuses.reduce((sum, status) => sum + (counts[status] || 0), 0)
  const returns = Object.entries(counts).filter(([status]) => isReturn(status)).reduce((sum, [, value]) => sum + value, 0)
  const filtered = data.orders.filter((order) => {
    const query = search.trim().toLowerCase()
    const matches = [order.order_id, order.customer_id, order.product_name, order.seller_id].some((value) => value?.toLowerCase().includes(query))
    return matches && (filter === 'ALL' || (filter === 'RETURNS' ? isReturn(order.status) : order.status === filter))
  })
  const deliveryRate = data.stats.total_orders ? Math.round(count('DELIVERED') / data.stats.total_orders * 100) : 0
  const deliverySteps = ['CREATED', 'CONFIRMED', 'PROCESSING', 'SHIPPED', 'DELIVERED']
  const selectedStep = selected ? deliverySteps.indexOf(selected.status) : -1

  function changeMode(next) {
    if (mode === next || actionBusy) return
    setData(EMPTY)
    setSelectedId(null)
    setError('')
    setLastSync(null)
    setActions({})
    setCreation(null)
    setLoading(next === 'live')
    setMode(next)
  }

  async function addSamples() {
    if (actionRequest.current || creation?.phase === 'waiting') return
    setSearch('')
    setFilter('ALL')
    setSection('orders')
    if (mode === 'demo') {
      const updated = addDemoOrders(demo)
      setDemo(updated)
      setSelectedId(updated.orders[0].order_id)
      setCreation({ phase: 'done', message: '5 sample orders added. Select one to start its journey.' })
      ordersRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
      return
    }
    const controller = new AbortController()
    actionRequest.current = controller
    setActionBusy(true)
    setCreation({ phase: 'sending', message: 'Adding 5 sample orders…' })
    try {
      const result = await createSampleOrders(controller.signal)
      const ids = result.items.map((item) => item.order_id)
      setTrackedIds((previous) => [...new Set([...ids, ...previous])].slice(0, 50))
      setSelectedId(ids[0])
      setCreation({ phase: 'waiting', ids, loaded: 0, sentAt: Date.now(), message: 'Waiting for new orders to appear (0/5).' })
      setRefresh((value) => value + 1)
      ordersRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    } catch (failure) {
      if (!controller.signal.aborted) {
        const ids = failure.details?.submitted_order_ids || []
        if (ids.length) setTrackedIds((previous) => [...new Set([...ids, ...previous])].slice(0, 50))
        setCreation({ phase: 'error', ids, message: failure.message || 'Could not add sample orders.' })
        setRefresh((value) => value + 1)
      }
    } finally {
      actionRequest.current = null
      if (!controller.signal.aborted) setActionBusy(false)
    }
  }

  async function submitStatus(order, status, note) {
    if (actionRequest.current || ['sending', 'waiting'].includes(actions[order.order_id]?.phase)) return false
    if (mode === 'demo') {
      setDemo(advanceDemoOrder(demo, order.order_id, status, note))
      setActions((previous) => ({ ...previous, [order.order_id]: {
        phase: 'done', message: `Demo order updated to ${label(status)}.`,
      } }))
      return true
    }
    const controller = new AbortController()
    actionRequest.current = controller
    setActionBusy(true)
    setActions((previous) => ({ ...previous, [order.order_id]: { phase: 'sending', message: 'Sending status event…' } }))
    try {
      const result = await updateOrderStatus(order, status, note, controller.signal)
      setActions((previous) => ({ ...previous, [order.order_id]: {
        ...result, phase: 'waiting', sentAt: Date.now(), message: `Update sent. Waiting for ${label(status)} to appear.`,
      } }))
      setRefresh((value) => value + 1)
      return true
    } catch (failure) {
      if (!controller.signal.aborted) {
        setActions((previous) => ({ ...previous, [order.order_id]: {
          phase: 'error', message: failure.message || 'Could not send the update. Refresh before retrying.',
        } }))
        setRefresh((value) => value + 1)
      }
      return false
    } finally {
      actionRequest.current = null
      if (!controller.signal.aborted) setActionBusy(false)
    }
  }

  function navigate(next) {
    setSection(next)
    setFilter(next === 'shipments' ? 'SHIPPED' : next === 'returns' ? 'RETURNS' : 'ALL')
    setSearch('')
    if (next !== 'overview') ordersRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    else window.scrollTo({ top: 0, behavior: 'smooth' })
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a className="brand" href="#" onClick={(event) => { event.preventDefault(); navigate('overview') }}><span className="brand-mark"><Icon name="box" size={27} /></span>parcel<span className="brand-dot">.</span></a>
        <div className="workspace"><span className="workspace-icon">OT</span><div><strong>Order Tracking</strong><span>Operations workspace</span></div></div>
        <span className="nav-label">WORKSPACE</span>
        <nav aria-label="Main navigation">
          {[['overview', 'grid', 'Overview'], ['orders', 'box', 'All orders'], ['shipments', 'truck', 'In transit'], ['returns', 'return', 'Returns']].map(([id, icon, text]) => <button key={id} className={`nav-item ${section === id ? 'active' : ''}`} onClick={() => navigate(id)}><Icon name={icon} /><span>{text}</span>{id === 'orders' && <span className="nav-count">{data.stats.total_orders}</span>}</button>)}
        </nav>
        <div className="sidebar-bottom"><div className="sidebar-card"><span className="sidebar-card-icon"><Icon name="pin" size={25} /></span><strong>Every parcel.<br />A little closer.</strong><p>Follow the journey, from first mile to doorstep.</p><span className="mini-route"><i /><b /><i /></span></div><div className="operator"><span className="avatar">OP</span><div><strong>Operations team</strong><span>Order control center</span></div><span className="online-dot" /></div></div>
      </aside>

      <div className="main-shell">
        <header className="topbar"><div className="breadcrumb">Workspace <Icon name="chevron" size={13} /><strong>{section === 'overview' ? 'Overview' : section === 'orders' ? 'All orders' : section === 'shipments' ? 'In transit' : 'Returns'}</strong></div><div className="topbar-right"><div className="mode-switch" aria-label="Data source"><button className={mode === 'live' ? 'selected' : ''} aria-pressed={mode === 'live'} onClick={() => changeMode('live')}><span className={`source-dot ${error ? 'offline' : ''}`} />Live</button><button className={mode === 'demo' ? 'selected' : ''} aria-pressed={mode === 'demo'} onClick={() => changeMode('demo')}>Demo</button></div><span className="topbar-avatar">OP</span></div></header>

        <main>
          <section className="page-heading"><div><span className="eyebrow">THE BIG PICTURE</span><h1>A good day to deliver<span>.</span></h1><p>Your orders, their journeys. All moving in one place.</p></div><div className="heading-actions"><button className="button button-white refresh-button" onClick={() => setRefresh((value) => value + 1)} disabled={loading}><Icon name="refresh" size={16} /><span>Refresh</span></button><button className="button button-dark sample-button" onClick={addSamples} disabled={actionBusy || creation?.phase === 'waiting'}><Icon name="box" size={16} />{creation?.phase === 'sending' ? 'Adding orders…' : 'Add sample orders'}</button></div></section>

          {creation && <div className={`notice ${creation.phase === 'error' ? 'error-notice' : creation.phase === 'done' ? 'success-notice' : 'waiting-notice'}`} role={creation.phase === 'error' ? 'alert' : 'status'}><Icon name={creation.phase === 'done' ? 'checkCircle' : creation.phase === 'error' ? 'alert' : 'clock'} size={18} /><span>{creation.message}{creation.phase === 'waiting' && Date.now() - creation.sentAt > 30000 && ' This is taking longer than expected. Check that Spark is running.'}</span>{creation.phase === 'waiting' && <button onClick={() => setRefresh((value) => value + 1)}>Check for orders <Icon name="refresh" size={14} /></button>}{['done', 'error'].includes(creation.phase) && <button onClick={() => setCreation(null)} aria-label="Dismiss sample order message">Dismiss</button>}</div>}

          {mode === 'demo' && <div className="notice demo-notice"><Icon name="layers" size={18} /><span><strong>Demo workspace.</strong> You’re viewing sample orders. Switch to Live for Cassandra data.</span><button onClick={() => changeMode('live')}>Go live <Icon name="arrow" size={15} /></button></div>}
          {error && <div className="notice error-notice" role="alert"><Icon name="alert" size={19} /><span><strong>{lastSync ? 'Connection interrupted. Showing the last successful snapshot.' : 'Live data is unavailable.'}</strong> {error}</span><button onClick={() => changeMode('demo')}>View demo <Icon name="arrow" size={15} /></button></div>}

          <section className="hero"><div className="hero-copy"><span className="hero-label"><span /> FROM FIRST MILE TO LAST</span><h2>Small parcels.<br />Big journeys.</h2><p>Keep an eye on every handoff.<br />We’ll keep the whole picture in view.</p><button className="button button-dark" onClick={() => navigate('orders')}>Explore your orders <Icon name="arrow" size={17} /></button></div><RouteIllustration /><span className="hero-index">01 / THE JOURNEY</span></section>

          <div className="section-line"><h2>At a glance</h2><span><span className={`source-dot ${mode === 'demo' ? 'demo-dot' : error ? 'offline' : ''}`} />{mode === 'demo' ? 'Sample data' : loading ? 'Connecting…' : error ? 'Connection unavailable' : lastSync ? `Synced ${time(lastSync)} · refreshes every 5s` : 'Waiting for data'}</span></div>
          <section className="stats-grid" aria-label="Order statistics" aria-busy={loading}>
            <StatCard title="Total orders" value={data.stats.total_orders} icon="box" tone="neutral" subtitle="Across all current statuses" />
            <StatCard title="Getting ready" value={count('CREATED', 'CONFIRMED', 'PROCESSING')} icon="clock" tone="amber" subtitle="Created, confirmed & processing" />
            <StatCard title="On the road" value={count('SHIPPED')} icon="truck" tone="blue" subtitle="Dispatched to customers" />
            <StatCard title="Delivered" value={count('DELIVERED')} icon="checkCircle" tone="green" subtitle={`${deliveryRate}% of all current orders`} />
            <StatCard title="Returns & refunds" value={returns} icon="return" tone="purple" subtitle="All return stages & refunded" />
          </section>

          {!loading && !error && mode === 'live' && !data.stats.items.length && <div className="notice waiting-notice"><Icon name="clock" size={18} /><span>No statistics yet. Submit the Spark streaming job and send order events to populate this dashboard.</span></div>}

          <div className="operations-grid" ref={ordersRef}>
            <section className="orders-panel panel"><div className="panel-heading"><div><h2>Order activity <span className="number-pill">{filtered.length}</span></h2><p>{mode === 'demo' ? 'A few journeys from our sample workspace.' : 'Latest updates within the loaded order sample.'}</p></div><button className="button export-button" onClick={() => exportOrders(filtered)} disabled={!filtered.length}><Icon name="download" size={16} /><span>Export</span></button></div>
              <div className="table-toolbar"><label className="search-input"><Icon name="search" size={17} /><input aria-label="Search loaded orders" placeholder="Search order, product or customer…" value={search} onChange={(event) => setSearch(event.target.value)} />{search && <button className="clear-search" aria-label="Clear search" onClick={() => setSearch('')}>×</button>}</label><select aria-label="Filter orders by status" value={filter} onChange={(event) => setFilter(event.target.value)}><option value="ALL">All statuses</option><option value="RETURNS">Returns & refunds</option>{[...new Set([...Object.keys(STATUS_LABELS), ...data.orders.map((order) => order.status).filter(Boolean)])].map((status) => <option key={status} value={status}>{label(status)}</option>)}</select></div>
              <div className="table-scroll"><table><thead><tr><th>ORDER / PRODUCT</th><th>STATUS</th><th>AMOUNT</th><th>UPDATED</th><th><span className="sr-only">Details</span></th></tr></thead><tbody>{filtered.map((order) => <tr key={order.order_id} className={selectedId === order.order_id ? 'selected-row' : ''}><td><button className="order-link" aria-pressed={selectedId === order.order_id} onClick={() => setSelectedId(order.order_id)}><span className="order-box"><Icon name="box" size={18} /></span><span><strong>{order.order_id}</strong><small>{order.product_name || 'Unnamed product'}</small></span></button></td><td><StatusBadge status={order.status} /></td><td className="amount">{money(order.total_amount)}</td><td className="updated-cell">{time(order.updated_at)}<small>{date(order.updated_at)}</small></td><td><button className="row-arrow" aria-label={`Track ${order.order_id}`} onClick={() => setSelectedId(order.order_id)}><Icon name="arrowUp" size={17} /></button></td></tr>)}</tbody></table>
                {!filtered.length && <div className="empty-state" role="status"><span><Icon name={loading ? 'refresh' : error ? 'signal' : 'box'} size={28} /></span><strong>{loading ? 'Finding your parcels…' : error && !data.orders.length ? 'Waiting for a connection' : search || filter !== 'ALL' ? 'No matching orders' : 'Your next journey starts here'}</strong><p>{loading ? 'Connecting to the order service.' : error && !data.orders.length ? 'Connect the live service or explore the demo workspace.' : search || filter !== 'ALL' ? 'Try another search or choose a different status.' : 'New orders will appear as Spark processes events.'}</p></div>}
              </div><div className="table-footer"><span>Showing {filtered.length} of {data.orders.length} loaded orders</span><span>{mode === 'demo' ? 'Demo collection' : trackedIds.length ? '100-order sample + orders added this session' : 'Up to 100 orders · search filters this sample'}</span></div>
            </section>

            <aside className="tracking-panel panel" aria-label="Selected order details"><div className="panel-heading"><div><span className="eyebrow">A CLOSER LOOK</span><h2>Parcel journey</h2></div><span className="tracking-icon"><Icon name="pin" size={21} /></span></div>
              {selected ? <><div className="selected-order"><div><span className="tiny-label">TRACKING ID</span><h3>{selected.order_id}</h3></div><StatusBadge status={selected.status} /></div><div className="parcel-summary"><span className="parcel-large"><Icon name="box" size={30} /></span><div><strong>{selected.product_name || 'Unnamed product'}</strong><span>{selected.quantity ?? '—'} item{selected.quantity === 1 ? '' : 's'} <span className="middot">·</span> {money(selected.total_amount)}</span></div></div><dl className="order-meta"><div><dt>Customer</dt><dd>{selected.customer_id || '—'}</dd></div><div><dt>Seller</dt><dd>{selected.seller_id || '—'}</dd></div><div><dt>Last update</dt><dd>{date(selected.updated_at)}, {time(selected.updated_at)}</dd></div></dl>
                {selectedStep >= 0 && <div className="delivery-progress" aria-label={`${label(selected.status)}: stage ${selectedStep + 1} of 5`}><div>{deliverySteps.map((status, index) => <span key={status} className={index <= selectedStep ? 'complete' : ''} />)}</div><span>{selected.status === 'DELIVERED' ? 'A journey well delivered.' : `${selectedStep + 1} of 5 delivery stages reached`}</span></div>}
                <OrderControls key={`${mode}-${selected.order_id}`} order={selected} mode={mode} busy={actionBusy}
                  feedback={actions[selected.order_id]} onSubmit={submitStatus} onRefresh={() => setRefresh((value) => value + 1)} />
                <div className="journey-heading"><h3>Event timeline</h3><Icon name="clock" size={15} /></div>
                {historyError && <p className="history-error" role="alert">{historyError}{history.length > 0 && ' Showing previously loaded events.'}</p>}
                {historyLoading ? <p className="timeline-empty" role="status">Loading the journey…</p> : !history.length ? <p className="timeline-empty">{historyError ? 'Timeline unavailable.' : 'No events recorded yet.'}</p> : <ol className="timeline">{history.map((event, index) => <li key={`${event.event_time}-${event.status}`} className={index === history.length - 1 ? 'current' : ''}><span className="timeline-marker">{index === history.length - 1 ? <Icon name="pin" size={12} /> : <Icon name="check" size={11} />}</span><div><div className="timeline-title"><strong>{label(event.status)}</strong><span>{time(event.event_time)}</span></div><p>{event.note || 'Order status updated.'}</p><small>{date(event.event_time)}</small></div></li>)}</ol>}
                <div className="tracking-footnote"><Icon name="signal" size={14} /><span>{mode === 'demo' ? 'Illustrative event history' : 'Latest 100 events · refreshes every 5s'}</span></div></> : <div className="empty-state tracking-empty"><span><Icon name="pin" size={30} /></span><strong>Every order has a story.</strong><p>Select an order to follow its journey.</p></div>}
            </aside>
          </div>
          <footer className="page-footer"><span><Icon name="box" size={15} /> Made for the journey.</span><span>{mode === 'demo' ? 'Sample workspace' : `Statistics updated ${data.stats.updated_at ? `${date(data.stats.updated_at)}, ${time(data.stats.updated_at)}` : '—'}`}<span className="footer-dot">·</span>Parcel / Order Tracking</span></footer>
        </main>
      </div>
    </div>
  )
}
