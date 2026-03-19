import { useEffect, useMemo, useState } from 'react'
import { useDashboardData, type ClientRow, type StaffRow } from '../dashboard/DashboardDataProvider'
import './DashboardPage.css'

type DashboardTab = 'overview' | 'clients' | 'staff' | 'risk'
type FilterOption = 'All' | 'Not Started' | 'Critical' | 'At Risk' | 'On Track' | 'Ahead' | 'Uncontracted'
type SortColumn = 'name' | 'conTot' | 'recTot' | 'pending' | 'rate' | 'conInd' | 'recInd' | 'conBus' | 'recBus'

type EnrichedClient = ClientRow & {
  name: string
  id: string
  conInd: number
  conBus: number
  conTot: number
  recInd: number
  recBus: number
  recTot: number
  pending: number
  rate: number | null
  overContracted: boolean
}

type EnrichedStaff = StaffRow & {
  recTot: number
}

type RiskTone = {
  bg: string
  text: string
  bar: string
}

const filterOptions: FilterOption[] = ['All', 'Not Started', 'Critical', 'At Risk', 'On Track', 'Ahead', 'Uncontracted']

function fmt(n: number) {
  return new Intl.NumberFormat().format(n)
}

function pct(n: number | null) {
  if (n === null || Number.isNaN(n)) return '—'
  return `${(n * 100).toFixed(1)}%`
}

function formatDate(value: string) {
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return value
  return new Intl.DateTimeFormat(undefined, { month: 'long', day: '2-digit', year: 'numeric' }).format(parsed)
}

function riskLabel(client: EnrichedClient) {
  if (client.rate === null) return 'Uncontracted'
  if (client.rate === 0) return 'Not Started'
  if (client.rate < 0.15) return 'Critical'
  if (client.rate < 0.35) return 'At Risk'
  if (client.rate < 0.6) return 'On Track'
  return 'Ahead'
}

function riskTone(label: string): RiskTone {
  const map: Record<string, RiskTone> = {
    'Not Started': { bg: '#f5f5f4', text: '#44403c', bar: '#78716c' },
    Critical: { bg: '#f5f5f4', text: '#292524', bar: '#57534e' },
    'At Risk': { bg: '#fafaf9', text: '#44403c', bar: '#78716c' },
    'On Track': { bg: '#fafaf9', text: '#44403c', bar: '#57534e' },
    Ahead: { bg: '#fafaf9', text: '#292524', bar: '#44403c' },
    Uncontracted: { bg: '#f5f3ff', text: '#5b5568', bar: '#8b5cf6' },
  }
  return map[label] ?? { bg: '#f5f5f4', text: '#6b7280', bar: '#a8a29e' }
}

function averageRate(rows: EnrichedClient[]) {
  const rated = rows.filter((row) => row.rate !== null)
  if (rated.length === 0) return 0
  return rated.reduce((sum, row) => sum + (row.rate ?? 0), 0) / rated.length
}

function getSortValue(client: EnrichedClient, sortCol: SortColumn) {
  if (sortCol === 'name') return client.name.toLowerCase()
  return client[sortCol] ?? 0
}

export function DashboardPage() {
  const { snapshots, snapshotId, setSnapshotId, kpis, clients, staff, loading, error } = useDashboardData()
  const [tab, setTab] = useState<DashboardTab>('overview')
  const [sortCol, setSortCol] = useState<SortColumn>('rate')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [filter, setFilter] = useState<FilterOption>('All')
  const [search, setSearch] = useState('')
  const [selectedClientId, setSelectedClientId] = useState<string | null>(null)

  useEffect(() => {
    setSelectedClientId(null)
  }, [snapshotId])

  const enrichedClients = useMemo<EnrichedClient[]>(
    () =>
      clients.map((client) => {
        const rate = client.contracted_total > 0 ? client.receipt_rate ?? 0 : client.received_total > 0 ? null : 0

        return {
          ...client,
          name: client.client_name,
          id: client.client_id,
          conInd: client.contracted_ind,
          conBus: client.contracted_bus,
          conTot: client.contracted_total,
          recInd: client.received_ind,
          recBus: client.received_bus,
          recTot: client.received_total,
          pending: client.pending_total,
          rate,
          overContracted: client.received_total > client.contracted_total,
        }
      }),
    [clients],
  )

  const enrichedStaff = useMemo<EnrichedStaff[]>(
    () => staff.map((member) => ({ ...member, recTot: member.received_total })),
    [staff],
  )

  const sortedClients = useMemo(() => {
    const filtered = enrichedClients.filter((client) => {
      const matchesFilter = filter === 'All' || riskLabel(client) === filter
      const term = search.trim().toLowerCase()
      const matchesSearch = term.length === 0 || client.name.toLowerCase().includes(term) || client.id.toLowerCase().includes(term)
      return matchesFilter && matchesSearch
    })

    return [...filtered].sort((left, right) => {
      const a = getSortValue(left, sortCol)
      const b = getSortValue(right, sortCol)

      if (typeof a === 'string' && typeof b === 'string') {
        return sortDir === 'asc' ? a.localeCompare(b) : b.localeCompare(a)
      }

      const aValue = typeof a === 'number' ? a : -Infinity
      const bValue = typeof b === 'number' ? b : -Infinity
      return sortDir === 'asc' ? aValue - bValue : bValue - aValue
    })
  }, [enrichedClients, filter, search, sortCol, sortDir])

  const selectedClient = useMemo(
    () => (selectedClientId ? enrichedClients.find((client) => client.id === selectedClientId) ?? null : null),
    [enrichedClients, selectedClientId],
  )

  const topClients = useMemo(() => [...enrichedClients].sort((a, b) => b.conTot - a.conTot).slice(0, 10), [enrichedClients])
  const topPerformers = useMemo(
    () =>
      [...enrichedClients]
        .filter((client) => client.rate !== null && client.conTot >= 20)
        .sort((a, b) => (b.rate ?? 0) - (a.rate ?? 0))
        .slice(0, 5),
    [enrichedClients],
  )
  const zeroReceivedClients = useMemo(
    () => [...enrichedClients].filter((client) => client.rate === 0 && client.conTot > 0).sort((a, b) => b.conTot - a.conTot),
    [enrichedClients],
  )
  const criticalClients = useMemo(
    () => [...enrichedClients].filter((client) => client.rate !== null && client.rate > 0 && client.rate < 0.15).sort((a, b) => b.conTot - a.conTot),
    [enrichedClients],
  )
  const anomalyClients = useMemo(
    () => enrichedClients.filter((client) => client.overContracted || (client.rate === null && client.recTot > 0)),
    [enrichedClients],
  )
  const aheadClients = useMemo(
    () => [...enrichedClients].filter((client) => client.rate !== null && client.rate >= 0.6).sort((a, b) => (b.rate ?? 0) - (a.rate ?? 0)),
    [enrichedClients],
  )

  const distribution = useMemo(
    () => [
      { label: 'Ahead (≥60%)', clients: enrichedClients.filter((client) => client.rate !== null && client.rate >= 0.6), tone: 'Ahead' },
      { label: 'On Track (35–60%)', clients: enrichedClients.filter((client) => client.rate !== null && client.rate >= 0.35 && client.rate < 0.6), tone: 'On Track' },
      { label: 'At Risk (15–35%)', clients: enrichedClients.filter((client) => client.rate !== null && client.rate >= 0.15 && client.rate < 0.35), tone: 'At Risk' },
      { label: 'Critical (<15%)', clients: enrichedClients.filter((client) => client.rate !== null && client.rate > 0 && client.rate < 0.15), tone: 'Critical' },
      { label: 'Not Started (0%)', clients: enrichedClients.filter((client) => client.rate === 0), tone: 'Not Started' },
      { label: 'Uncontracted', clients: enrichedClients.filter((client) => client.rate === null && client.recTot > 0), tone: 'Uncontracted' },
    ],
    [enrichedClients],
  )

  function handleSort(column: SortColumn) {
    if (sortCol === column) {
      setSortDir((current) => (current === 'asc' ? 'desc' : 'asc'))
      return
    }

    setSortCol(column)
    setSortDir(column === 'name' ? 'asc' : 'desc')
  }

  if (!snapshotId && snapshots.length === 0) {
    return (
      <div className="dashboard-page">
        <div className="dashboard-empty-state">
          <h1>Dashboard</h1>
          <p>Upload a contracted vs actual workbook in Documents Processing to generate the first snapshot.</p>
        </div>
      </div>
    )
  }

  return (
    <div className="dashboard-page">
      <header className="dashboard-header">
        <div className="dashboard-title-wrap">
          <div className="dashboard-season-chip">2026 Tax Season</div>
          <h1>Tax Returns — Contracted vs Received</h1>
          <p>
            {kpis
              ? `As of ${formatDate(kpis.snapshot.as_of_date)} · ${kpis.active_clients} CPA clients · ${fmt(kpis.total_contracted)} total contracted`
              : 'Snapshot analytics for the active tax season'}
          </p>
        </div>

        <div className="dashboard-header-right">
          <div className="dashboard-segmented">
            {[
              ['overview', 'Overview'],
              ['clients', 'Client Table'],
              ['staff', 'Staff Workload'],
              ['risk', 'Risk Flags'],
            ].map(([id, label]) => (
              <button key={id} className={`dashboard-tab${tab === id ? ' active' : ''}`} onClick={() => setTab(id as DashboardTab)} type="button">
                {label}
              </button>
            ))}
          </div>

          <label className="dashboard-snapshot-picker">
            <span>Snapshot</span>
            <select value={snapshotId ?? ''} onChange={(e) => setSnapshotId(Number(e.target.value))}>
              {snapshots.map((snapshot) => (
                <option key={snapshot.id} value={snapshot.id}>
                  {snapshot.as_of_date} · {snapshot.source_filename}
                </option>
              ))}
            </select>
          </label>
        </div>
      </header>

      {error && <div className="dashboard-banner">{error}</div>}

      <section className="dashboard-kpi-grid">
        {[
          { label: 'Total Contracted', value: kpis ? fmt(kpis.total_contracted) : '—', sub: 'All clients combined', accent: '#1f2937' },
          { label: 'Received to Date', value: kpis ? fmt(kpis.total_received) : '—', sub: kpis ? `${pct(kpis.overall_receipt_rate)} receipt rate` : '', accent: '#1f2937' },
          { label: 'Still Outstanding', value: kpis ? fmt(kpis.total_pending) : '—', sub: 'Returns yet to arrive', accent: '#1f2937' },
          { label: 'Individual Returns', value: kpis ? fmt(kpis.total_received_ind) : '—', sub: kpis ? `of ${fmt(kpis.total_contracted_ind)} contracted` : '', accent: '#4b5563' },
          { label: 'Business Returns', value: kpis ? fmt(kpis.total_received_bus) : '—', sub: kpis ? `of ${fmt(kpis.total_contracted_bus)} contracted` : '', accent: '#4b5563' },
          { label: 'Not Yet Started', value: kpis ? fmt(kpis.zero_received_clients) : '—', sub: 'Clients at 0% receipt', accent: '#1f2937' },
        ].map((card) => (
          <article key={card.label} className="dashboard-kpi-card">
            <span>{card.label}</span>
            <strong style={{ color: card.accent }}>{card.value}</strong>
            <small>{card.sub}</small>
          </article>
        ))}
      </section>

      {tab === 'overview' && (
        <section className="dashboard-overview-grid">
          <div className="dashboard-column">
            <article className="dashboard-card">
              <div className="dashboard-card-header">
                <div>
                  <h2>Season Progress</h2>
                  <p>Overall receipt rate across all active clients in the selected snapshot.</p>
                </div>
              </div>

              <div className="dashboard-progress-hero">
                <div className="dashboard-progress-bar large">
                  <div className="dashboard-progress-fill" style={{ width: `${(kpis?.overall_receipt_rate ?? 0) * 100}%`, background: '#1f2937' }}>
                    <span>{pct(kpis?.overall_receipt_rate ?? 0)}</span>
                  </div>
                </div>
                <div className="dashboard-progress-meta">{kpis ? `${fmt(kpis.total_received)} / ${fmt(kpis.total_contracted)}` : '—'}</div>
              </div>

              <div className="dashboard-mini-grid">
                {[
                  { label: 'Individual Returns', received: kpis?.total_received_ind ?? 0, contracted: kpis?.total_contracted_ind ?? 0, color: '#374151' },
                  { label: 'Business Returns', received: kpis?.total_received_bus ?? 0, contracted: kpis?.total_contracted_bus ?? 0, color: '#6b7280' },
                ].map((group) => {
                  const ratio = group.contracted > 0 ? group.received / group.contracted : 0
                  return (
                    <div key={group.label} className="dashboard-mini-card">
                      <span>{group.label}</span>
                      <strong>{fmt(group.received)}</strong>
                      <small>of {fmt(group.contracted)} contracted</small>
                      <div className="dashboard-progress-bar">
                        <div className="dashboard-progress-fill" style={{ width: `${ratio * 100}%`, background: group.color }} />
                      </div>
                      <em>{pct(ratio)} received</em>
                    </div>
                  )
                })}
              </div>
            </article>

            <article className="dashboard-card">
              <div className="dashboard-card-header">
                <div>
                  <h2>Client Receipt Rate Distribution</h2>
                  <p>How clients are distributed across receipt rate buckets.</p>
                </div>
              </div>

              <div className="dashboard-stack">
                {distribution.map((bucket) => {
                  const tone = riskTone(bucket.tone)
                  const width = kpis && kpis.active_clients > 0 ? (bucket.clients.length / kpis.active_clients) * 100 : 0
                  return (
                    <div key={bucket.label} className="dashboard-distribution-row">
                      <div className="dashboard-distribution-labels">
                        <span>{bucket.label}</span>
                        <div>
                          <small>{bucket.clients.length} clients</small>
                          {bucket.clients.length > 0 && <strong style={{ background: tone.bg, color: tone.text }}>{Math.round(width)}%</strong>}
                        </div>
                      </div>
                      <div className="dashboard-progress-bar">
                        <div className="dashboard-progress-fill" style={{ width: `${width}%`, background: tone.bar }} />
                      </div>
                    </div>
                  )
                })}
              </div>
            </article>

            <article className="dashboard-card">
              <div className="dashboard-card-header">
                <div>
                  <h2>Top 10 Clients by Contracted Volume</h2>
                  <p>Largest clients by total contracted returns, with current receipt progress.</p>
                </div>
              </div>

              <div className="dashboard-stack">
                {topClients.map((client, index) => {
                  const tone = riskTone(riskLabel(client))
                  return (
                    <div key={client.id} className="dashboard-top-row">
                      <span className="dashboard-rank">{index + 1}</span>
                      <span className="dashboard-top-name">{client.name}</span>
                      <div className="dashboard-progress-bar">
                        <div className="dashboard-progress-fill" style={{ width: `${(client.rate ?? 0) * 100}%`, background: tone.bar }} />
                      </div>
                      <strong style={{ color: tone.text }}>{pct(client.rate)}</strong>
                      <small>{fmt(client.recTot)}/{fmt(client.conTot)}</small>
                    </div>
                  )
                })}
              </div>
            </article>
          </div>

          <div className="dashboard-sidebar-column">
            <article className="dashboard-card compact">
              <div className="dashboard-card-header">
                <div>
                  <h2>Top Performers</h2>
                </div>
              </div>
              <div className="dashboard-stack">
                {topPerformers.map((client) => (
                  <div key={client.id} className="dashboard-inline-stat">
                    <span>{client.name}</span>
                      <strong style={{ background: '#f5f5f4', color: '#292524' }}>{pct(client.rate)}</strong>
                  </div>
                ))}
              </div>
            </article>

            <article className="dashboard-card compact">
              <div className="dashboard-card-header">
                <div>
                  <h2>Zero Received</h2>
                  <p>{kpis ? `${kpis.zero_received_clients} clients with contracted returns but 0 received` : ''}</p>
                </div>
              </div>
              <div className="dashboard-stack">
                {zeroReceivedClients.length === 0 ? (
                  <div className="dashboard-empty-card">No zero-received clients in this snapshot.</div>
                ) : (
                  zeroReceivedClients.map((client) => (
                    <div key={client.id} className="dashboard-inline-stat">
                      <span>{client.name}</span>
                      <strong style={{ background: '#f5f5f4', color: '#44403c' }}>{fmt(client.conTot)} due</strong>
                    </div>
                  ))
                )}
              </div>
            </article>

            <article className="dashboard-card compact">
              <div className="dashboard-card-header">
                <div>
                  <h2>Season Snapshot</h2>
                </div>
              </div>
              <div className="dashboard-stack">
                {[
                  ['Active CPA clients', kpis ? fmt(kpis.active_clients) : '—'],
                  ['Contracted Individual', kpis ? fmt(kpis.total_contracted_ind) : '—'],
                  ['Contracted Business', kpis ? fmt(kpis.total_contracted_bus) : '—'],
                  ['Ind receipt rate', kpis ? pct(kpis.total_contracted_ind ? kpis.total_received_ind / kpis.total_contracted_ind : 0) : '—'],
                  ['Bus receipt rate', kpis ? pct(kpis.total_contracted_bus ? kpis.total_received_bus / kpis.total_contracted_bus : 0) : '—'],
                  ['Over-delivered clients', kpis ? fmt(kpis.over_delivered_clients) : '—'],
                  ['Staff returns received', kpis ? fmt(kpis.staff_total_received) : '—'],
                ].map(([label, value]) => (
                  <div key={label} className="dashboard-inline-stat plain">
                    <span>{label}</span>
                    <strong>{value}</strong>
                  </div>
                ))}
              </div>
            </article>
          </div>
        </section>
      )}

      {tab === 'clients' && (
        <section className="dashboard-tab-section">
          <div className="dashboard-client-toolbar">
            <div className="dashboard-filter-row">
              {filterOptions.map((option) => (
                <button key={option} className={`dashboard-filter${filter === option ? ' active' : ''}`} onClick={() => setFilter(option)} type="button">
                  {option}
                </button>
              ))}
            </div>
            <div className="dashboard-search-wrap">
              <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search client..." />
              <span>{sortedClients.length} clients</span>
            </div>
          </div>

          <div className="dashboard-client-grid">
            <article className="dashboard-card table-card">
              <div className="dashboard-table-wrap">
                <table className="dashboard-table">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th onClick={() => handleSort('name')}>Client{sortCol === 'name' ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}</th>
                      <th onClick={() => handleSort('conTot')}>Contracted{sortCol === 'conTot' ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}</th>
                      <th onClick={() => handleSort('recTot')}>Received{sortCol === 'recTot' ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}</th>
                      <th onClick={() => handleSort('pending')}>Pending{sortCol === 'pending' ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}</th>
                      <th onClick={() => handleSort('rate')}>Receipt %{sortCol === 'rate' ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}</th>
                      <th onClick={() => handleSort('conInd')}>Ind Con{sortCol === 'conInd' ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}</th>
                      <th onClick={() => handleSort('recInd')}>Ind Rec{sortCol === 'recInd' ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}</th>
                      <th onClick={() => handleSort('conBus')}>Bus Con{sortCol === 'conBus' ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}</th>
                      <th onClick={() => handleSort('recBus')}>Bus Rec{sortCol === 'recBus' ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedClients.map((client, index) => {
                      const label = riskLabel(client)
                      const tone = riskTone(label)
                      return (
                        <tr key={client.id} className={selectedClientId === client.id ? 'selected' : ''} onClick={() => setSelectedClientId((current) => (current === client.id ? null : client.id))}>
                          <td>{index + 1}</td>
                          <td>{client.name}</td>
                          <td>{fmt(client.conTot)}</td>
                      <td className="positive">{fmt(client.recTot)}</td>
                          <td>{fmt(client.pending)}</td>
                          <td>{pct(client.rate)}</td>
                          <td>{fmt(client.conInd)}</td>
                          <td>{fmt(client.recInd)}</td>
                          <td>{fmt(client.conBus)}</td>
                          <td>{fmt(client.recBus)}</td>
                          <td>
                            <span className="dashboard-pill" style={{ background: tone.bg, color: tone.text }}>
                              {label}
                            </span>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                  <tfoot>
                    <tr>
                      <td colSpan={2}>Totals</td>
                      <td>{kpis ? fmt(kpis.total_contracted) : '—'}</td>
                      <td>{kpis ? fmt(kpis.total_received) : '—'}</td>
                      <td>{kpis ? fmt(kpis.total_pending) : '—'}</td>
                      <td>{kpis ? pct(kpis.overall_receipt_rate) : '—'}</td>
                      <td>{kpis ? fmt(kpis.total_contracted_ind) : '—'}</td>
                      <td>{kpis ? fmt(kpis.total_received_ind) : '—'}</td>
                      <td>{kpis ? fmt(kpis.total_contracted_bus) : '—'}</td>
                      <td>{kpis ? fmt(kpis.total_received_bus) : '—'}</td>
                      <td />
                    </tr>
                  </tfoot>
                </table>
              </div>
            </article>

            <div className="dashboard-sidebar-column">
              {selectedClient ? (
                <article className="dashboard-card">
                  <div className="dashboard-card-header">
                    <div>
                      <h2>{selectedClient.name}</h2>
                      <p>{selectedClient.client_type}</p>
                    </div>
                    <button className="dashboard-dismiss" onClick={() => setSelectedClientId(null)} type="button">
                      ×
                    </button>
                  </div>

                  <div className="dashboard-pill-row">
                    <span className="dashboard-pill identifier">{selectedClient.id}</span>
                    <span className="dashboard-pill" style={{ background: riskTone(riskLabel(selectedClient)).bg, color: riskTone(riskLabel(selectedClient)).text }}>
                      {riskLabel(selectedClient)}
                    </span>
                  </div>

                  <div className="dashboard-progress-stack">
                    <div className="dashboard-inline-stat plain">
                      <span>Overall receipt</span>
                      <strong>{pct(selectedClient.rate)}</strong>
                    </div>
                    <div className="dashboard-progress-bar">
                      <div className="dashboard-progress-fill" style={{ width: `${(selectedClient.rate ?? 0) * 100}%`, background: riskTone(riskLabel(selectedClient)).bar }} />
                    </div>
                  </div>

                  <div className="dashboard-stack">
                    {[
                      ['Contracted Total', fmt(selectedClient.conTot)],
                      ['Received Total', fmt(selectedClient.recTot)],
                      ['Outstanding', fmt(selectedClient.pending)],
                      ['Con Individual', fmt(selectedClient.conInd)],
                      ['Rec Individual', fmt(selectedClient.recInd)],
                      ['Con Business', fmt(selectedClient.conBus)],
                      ['Rec Business', fmt(selectedClient.recBus)],
                      ['Over-delivered', selectedClient.overContracted ? 'Yes' : 'No'],
                    ].map(([label, value]) => (
                      <div key={label} className="dashboard-inline-stat plain">
                        <span>{label}</span>
                        <strong>{value}</strong>
                      </div>
                    ))}
                  </div>

                  <div className="dashboard-progress-stack">
                    {[
                      ['Individual', selectedClient.recInd, selectedClient.conInd, '#374151'],
                      ['Business', selectedClient.recBus, selectedClient.conBus, '#6b7280'],
                    ].map(([label, received, contracted, color]) => {
                      const ratio = typeof received === 'number' && typeof contracted === 'number' && contracted > 0 ? received / contracted : 0
                      return (
                        <div key={label}>
                          <div className="dashboard-inline-stat plain">
                            <span>{label}</span>
                            <strong style={{ color: color as string }}>
                              {fmt(received as number)} / {fmt(contracted as number)}
                            </strong>
                          </div>
                          <div className="dashboard-progress-bar">
                            <div className="dashboard-progress-fill" style={{ width: `${ratio * 100}%`, background: color as string }} />
                          </div>
                        </div>
                      )
                    })}
                  </div>
                </article>
              ) : (
                <article className="dashboard-card empty-side-card">
                  <div className="dashboard-empty-card">Click any client row for a detailed breakdown.</div>
                </article>
              )}

              <article className="dashboard-card compact">
                <div className="dashboard-card-header">
                  <div>
                    <h2>Filtered Summary</h2>
                  </div>
                </div>
                <div className="dashboard-stack">
                  {[
                    ['Showing', `${sortedClients.length} clients`],
                    ['Contracted', fmt(sortedClients.reduce((sum, client) => sum + client.conTot, 0))],
                    ['Received', fmt(sortedClients.reduce((sum, client) => sum + client.recTot, 0))],
                    ['Avg receipt rate', pct(averageRate(sortedClients))],
                  ].map(([label, value]) => (
                    <div key={label} className="dashboard-inline-stat plain">
                      <span>{label}</span>
                      <strong>{value}</strong>
                    </div>
                  ))}
                </div>
              </article>
            </div>
          </div>
        </section>
      )}

      {tab === 'staff' && (
        <section className="dashboard-two-column">
          <div className="dashboard-column">
            <article className="dashboard-card">
              <div className="dashboard-card-header">
                <div>
                  <h2>Staff Returns Received</h2>
                  <p>Dedicated FTE and prep staff workload for the selected snapshot.</p>
                </div>
              </div>

              {enrichedStaff.length === 0 ? (
                <div className="dashboard-empty-card">No staff section was found in this workbook.</div>
              ) : (
                <div className="dashboard-stack">
                  {enrichedStaff.map((member) => {
                    const maxValue = Math.max(...enrichedStaff.map((row) => row.recTot), 1)
                    const indPct = member.recTot > 0 ? (member.received_ind / member.recTot) * 100 : 0
                    const busPct = member.recTot > 0 ? (member.received_bus / member.recTot) * 100 : 0
                    return (
                      <div key={member.staff_id} className="dashboard-staff-row">
                        <div className="dashboard-staff-meta">
                          <div className="dashboard-staff-badge">{member.staff_id}</div>
                          <div>
                            <div className="dashboard-staff-name">{member.name}</div>
                            <div className="dashboard-staff-subtitle">
                              <span className="dashboard-pill identifier">{member.staff_type}</span>
                              <span>
                                {fmt(member.received_ind)} Ind · {fmt(member.received_bus)} Bus
                              </span>
                            </div>
                          </div>
                          <strong>{fmt(member.recTot)}</strong>
                        </div>

                        <div className="dashboard-staff-bar">
                          <div className="dashboard-staff-bar-inner" style={{ width: `${(member.recTot / maxValue) * 100}%` }}>
                            <div style={{ width: `${indPct}%`, background: '#374151' }} />
                            <div style={{ width: `${busPct}%`, background: '#9ca3af' }} />
                          </div>
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
            </article>
          </div>

          <div className="dashboard-sidebar-column">
            <article className="dashboard-card compact">
              <div className="dashboard-card-header">
                <div>
                  <h2>Staff Totals</h2>
                </div>
              </div>
              <div className="dashboard-stack">
                {[
                  ['Total Received', fmt(enrichedStaff.reduce((sum, member) => sum + member.recTot, 0))],
                  ['Total Individual', fmt(enrichedStaff.reduce((sum, member) => sum + member.received_ind, 0))],
                  ['Total Business', fmt(enrichedStaff.reduce((sum, member) => sum + member.received_bus, 0))],
                  ['Staff Count', fmt(enrichedStaff.length)],
                  ['Avg per Staff', enrichedStaff.length > 0 ? fmt(Math.round(enrichedStaff.reduce((sum, member) => sum + member.recTot, 0) / enrichedStaff.length)) : '0'],
                ].map(([label, value]) => (
                  <div key={label} className="dashboard-inline-stat plain">
                    <span>{label}</span>
                    <strong>{value}</strong>
                  </div>
                ))}
              </div>
            </article>

            <article className="dashboard-card compact">
              <div className="dashboard-card-header">
                <div>
                  <h2>Business vs Individual Split</h2>
                </div>
              </div>
              {(() => {
                const totalInd = enrichedStaff.reduce((sum, member) => sum + member.received_ind, 0)
                const totalBus = enrichedStaff.reduce((sum, member) => sum + member.received_bus, 0)
                const total = totalInd + totalBus
                return (
                  <div className="dashboard-progress-stack">
                    <div className="dashboard-split-bar">
                      <div style={{ width: `${total > 0 ? (totalInd / total) * 100 : 0}%`, background: '#374151' }} />
                      <div style={{ width: `${total > 0 ? (totalBus / total) * 100 : 0}%`, background: '#9ca3af' }} />
                    </div>
                    {[
                      ['Individual', totalInd, '#374151'],
                      ['Business', totalBus, '#6b7280'],
                    ].map(([label, value, color]) => (
                      <div key={label} className="dashboard-inline-stat plain">
                        <span>{label}</span>
                        <strong style={{ color: color as string }}>
                          {fmt(value as number)} {total > 0 ? `(${pct((value as number) / total)})` : ''}
                        </strong>
                      </div>
                    ))}
                  </div>
                )
              })()}
            </article>
          </div>
        </section>
      )}

      {tab === 'risk' && (
        <section className="dashboard-risk-grid">
          <article className="dashboard-card risk-card red">
            <div className="dashboard-card-header">
              <div>
                <h2>Zero Received — Highest Volume at Risk</h2>
                <p>Clients with contracted returns but none received yet.</p>
              </div>
            </div>
            <div className="dashboard-stack">
              {zeroReceivedClients.map((client) => (
                <div key={client.id} className="dashboard-flag-row">
                  <div>
                    <strong>{client.name}</strong>
                    <span>
                      {fmt(client.conInd)} Ind · {fmt(client.conBus)} Bus
                    </span>
                  </div>
                  <span className="dashboard-pill" style={{ background: '#f5f5f4', color: '#44403c' }}>
                    {fmt(client.conTot)} due
                  </span>
                </div>
              ))}
            </div>
          </article>

          <article className="dashboard-card risk-card amber">
            <div className="dashboard-card-header">
              <div>
                <h2>Critical — Under 15% Receipt Rate</h2>
                <p>Low receipt rate. Follow-up recommended.</p>
              </div>
            </div>
            <div className="dashboard-stack">
              {criticalClients.map((client) => (
                <div key={client.id} className="dashboard-flag-row">
                  <div>
                    <strong>{client.name}</strong>
                    <span>
                      {fmt(client.recTot)} received of {fmt(client.conTot)}
                    </span>
                  </div>
                  <span className="dashboard-pill" style={{ background: '#f5f5f4', color: '#292524' }}>
                    {pct(client.rate)}
                  </span>
                </div>
              ))}
            </div>
          </article>

          <article className="dashboard-card risk-card violet">
            <div className="dashboard-card-header">
              <div>
                <h2>Over-Delivered / Uncontracted</h2>
                <p>Clients sending more returns than contracted or with no contract.</p>
              </div>
            </div>
            <div className="dashboard-stack">
              {anomalyClients.length === 0 ? (
                <div className="dashboard-empty-card">No anomalies in the selected snapshot.</div>
              ) : (
                anomalyClients.map((client) => (
                  <div key={client.id} className="dashboard-flag-row">
                    <div>
                      <strong>
                        {client.name} <span>({client.id})</span>
                      </strong>
                      <span>
                        Contracted: {fmt(client.conTot)} · Received: {fmt(client.recTot)}
                      </span>
                    </div>
                    <span className="dashboard-pill" style={{ background: '#f5f3ff', color: '#5b5568' }}>
                      {client.rate === null ? 'No contract' : `+${fmt(client.recTot - client.conTot)}`}
                    </span>
                  </div>
                ))
              )}
            </div>
          </article>

          <article className="dashboard-card risk-card green">
            <div className="dashboard-card-header">
              <div>
                <h2>Ahead of Schedule (≥60%)</h2>
                <p>Clients who have delivered 60% or more of contracted returns.</p>
              </div>
            </div>
            <div className="dashboard-stack">
              {aheadClients.map((client) => (
                <div key={client.id} className="dashboard-flag-row">
                  <div>
                    <strong>{client.name}</strong>
                    <span>
                      {fmt(client.recTot)} of {fmt(client.conTot)} received
                    </span>
                  </div>
                  <span className="dashboard-pill" style={{ background: '#f5f5f4', color: '#292524' }}>
                    {pct(client.rate)}
                  </span>
                </div>
              ))}
            </div>
          </article>
        </section>
      )}

      {loading && <div className="dashboard-loading">Refreshing snapshot metrics...</div>}
    </div>
  )
}
