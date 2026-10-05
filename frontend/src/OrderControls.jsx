import { useState } from 'react'
import Icon from './Icons.jsx'

const titles = {
  CONFIRMED: 'Confirm order',
  PROCESSING: 'Start processing',
  SHIPPED: 'Ship order',
  DELIVERED: 'Mark delivered',
}

export default function OrderControls({ order, mode, busy, feedback, onSubmit, onRefresh }) {
  const [note, setNote] = useState('')
  const next = order.allowed_statuses?.[0]
  const waiting = feedback?.phase === 'waiting' || feedback?.phase === 'sending'
  const delayed = waiting && feedback.sentAt && Date.now() - feedback.sentAt > 30000

  async function submit(event) {
    event.preventDefault()
    if (await onSubmit(order, next, note)) setNote('')
  }

  return (
    <section className="order-controls" aria-label="Order status controls">
      <div className="control-heading"><h3>Move this order forward</h3><Icon name="truck" size={16} /></div>
      {next ? (
        <form onSubmit={submit}>
          <label htmlFor={`note-${order.order_id}`}>Event note <span>(optional)</span></label>
          <textarea id={`note-${order.order_id}`} value={note} maxLength={500} rows={2}
            placeholder="Add a handoff or delivery note…" disabled={busy || waiting}
            onChange={(event) => setNote(event.target.value)} />
          <button className="button button-dark" type="submit" disabled={busy || waiting}>
            <Icon name={next === 'DELIVERED' ? 'checkCircle' : 'arrow'} size={16} />
            {waiting ? 'Waiting for update…' : busy ? 'Sending…' : titles[next]}
          </button>
        </form>
      ) : <p className="control-description">{order.status === 'DELIVERED' ? 'Delivery complete. This parcel has arrived.' : 'No delivery actions are available for this status.'}</p>}
      {feedback && <p className={`action-feedback ${feedback.phase}`} role={feedback.phase === 'error' ? 'alert' : 'status'}>{feedback.message}</p>}
      {delayed && <div className="pending-help"><p>The update is taking longer than expected. Check that Spark is running.</p><button type="button" onClick={onRefresh}>Check for update <Icon name="refresh" size={12} /></button></div>}
      <p className="control-description">{mode === 'demo' ? 'Updates sample orders for this session.' : 'Changes appear once the event has been processed.'}</p>
    </section>
  )
}
