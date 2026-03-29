import type { ClientRow, Kpis, Snapshot, StaffRow } from './DashboardDataProvider'

type EnrichedClient = ClientRow & {
  name: string
  id: string
  conTot: number
  recTot: number
  pending: number
  rate: number | null
}

type EnrichedStaff = StaffRow & {
  recTot: number
}

type DashboardAssistantContext = {
  snapshotId: number | undefined
  snapshots: Snapshot[]
  kpis: Kpis | null
  clients: ClientRow[]
  staff: StaffRow[]
}

export type AssistantAnswer = {
  title: string
  summary?: string
  cards?: Array<{
    title: string
    value: string
    meta?: string | null
  }>
  bullets?: string[]
}

function fmt(n: number) {
  return new Intl.NumberFormat().format(n)
}

function pct(n: number | null) {
  if (n === null || Number.isNaN(n)) return '—'
  return `${(n * 100).toFixed(1)}%`
}

function normalize(value: string) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

function formatDate(value: string) {
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return value
  return new Intl.DateTimeFormat(undefined, { month: 'short', day: '2-digit', year: 'numeric' }).format(parsed)
}

function enrichClients(clients: ClientRow[]): EnrichedClient[] {
  return clients.map((client) => ({
    ...client,
    name: client.client_name,
    id: client.client_id,
    conTot: client.contracted_total,
    recTot: client.received_total,
    pending: client.pending_total,
    rate: client.contracted_total > 0 ? client.receipt_rate ?? 0 : client.received_total > 0 ? null : 0,
  }))
}

function enrichStaff(staff: StaffRow[]): EnrichedStaff[] {
  return staff.map((member) => ({
    ...member,
    recTot: member.received_total,
  }))
}

function riskLabel(rate: number | null) {
  if (rate === null) return 'Uncontracted'
  if (rate === 0) return 'Not Started'
  if (rate < 0.15) return 'Critical'
  if (rate < 0.35) return 'At Risk'
  if (rate < 0.6) return 'On Track'
  return 'Ahead'
}

function findMentionedClients(question: string, clients: EnrichedClient[]) {
  const normalizedQuestion = normalize(question)
  const rawQuestion = question.toLowerCase()
  const matches: Array<{ client: EnrichedClient; score: number }> = []

  for (const client of clients) {
    const normalizedName = normalize(client.name)
    const normalizedId = normalize(client.id)
    let score = 0

    if (normalizedName.length >= 4 && normalizedQuestion.includes(normalizedName)) {
      score = Math.max(score, normalizedName.length + 10)
    }

    if (normalizedId.length >= 2 && rawQuestion.match(new RegExp(`\\b${normalizedId}\\b`, 'i'))) {
      score = Math.max(score, normalizedId.length + 3)
    }

    if (score > 0) matches.push({ client, score })
  }

  const unique = new Map<string, { client: EnrichedClient; score: number }>()
  for (const match of matches.sort((left, right) => right.score - left.score)) {
    if (!unique.has(match.client.id)) unique.set(match.client.id, match)
  }

  return [...unique.values()].map((entry) => entry.client)
}

function findMentionedStaff(question: string, staff: EnrichedStaff[]) {
  const normalizedQuestion = normalize(question)
  return staff.filter((member) => {
    const normalizedName = normalize(member.name)
    const normalizedId = normalize(member.staff_id)
    return normalizedQuestion.includes(normalizedName) || normalizedQuestion.includes(normalizedId)
  })
}

function buildClientSummary(client: EnrichedClient): AssistantAnswer {
  return {
    title: `${client.name} (${client.id})`,
    summary: `${riskLabel(client.rate)} client in the current snapshot.`,
    cards: [
      { title: 'Received', value: fmt(client.recTot), meta: `of ${fmt(client.conTot)} contracted` },
      { title: 'Pending', value: fmt(client.pending), meta: 'still outstanding' },
      { title: 'Receipt Rate', value: pct(client.rate), meta: 'current completion' },
    ],
    bullets: [
      `Individual: ${fmt(client.received_ind)} received of ${fmt(client.contracted_ind)} contracted.`,
      `Business: ${fmt(client.received_bus)} received of ${fmt(client.contracted_bus)} contracted.`,
    ],
  }
}

function buildComparison(first: EnrichedClient, second: EnrichedClient): AssistantAnswer {
  const receivedLeader =
    first.recTot === second.recTot
      ? 'Both clients are tied on received returns.'
      : first.recTot > second.recTot
        ? `${first.name} has received ${fmt(first.recTot - second.recTot)} more returns.`
        : `${second.name} has received ${fmt(second.recTot - first.recTot)} more returns.`

  const rateLeader =
    first.rate === second.rate
      ? 'Both clients have the same receipt rate.'
      : (first.rate ?? -1) > (second.rate ?? -1)
        ? `${first.name} is ahead on receipt rate at ${pct(first.rate)} versus ${pct(second.rate)}.`
        : `${second.name} is ahead on receipt rate at ${pct(second.rate)} versus ${pct(first.rate)}.`

  return {
    title: `${first.name} vs ${second.name}`,
    summary: 'Current snapshot comparison.',
    cards: [
      {
        title: first.name,
        value: `${fmt(first.recTot)} / ${fmt(first.conTot)}`,
        meta: `${fmt(first.pending)} pending · ${pct(first.rate)} receipt rate`,
      },
      {
        title: second.name,
        value: `${fmt(second.recTot)} / ${fmt(second.conTot)}`,
        meta: `${fmt(second.pending)} pending · ${pct(second.rate)} receipt rate`,
      },
    ],
    bullets: [receivedLeader, rateLeader],
  }
}

function buildSnapshotSummary(kpis: Kpis): AssistantAnswer {
  return {
    title: `Snapshot Summary · ${formatDate(kpis.snapshot.as_of_date)}`,
    summary: `${fmt(kpis.active_clients)} active clients in the current workbook.`,
    cards: [
      { title: 'Contracted', value: fmt(kpis.total_contracted), meta: 'total returns' },
      { title: 'Received', value: fmt(kpis.total_received), meta: `${pct(kpis.overall_receipt_rate)} overall rate` },
      { title: 'Pending', value: fmt(kpis.total_pending), meta: 'still outstanding' },
    ],
    bullets: [
      `Individual received: ${fmt(kpis.total_received_ind)} of ${fmt(kpis.total_contracted_ind)}.`,
      `Business received: ${fmt(kpis.total_received_bus)} of ${fmt(kpis.total_contracted_bus)}.`,
      `${fmt(kpis.zero_received_clients)} clients have zero received returns.`,
    ],
  }
}

function buildListAnswer(title: string, summary: string, rows: string[]): AssistantAnswer {
  return {
    title,
    summary,
    bullets: rows,
  }
}

function topClientsByMetric(clients: EnrichedClient[], metric: 'conTot' | 'recTot' | 'pending' | 'rate', limit: number) {
  return [...clients]
    .filter((client) => (metric === 'rate' ? client.rate !== null : true))
    .sort((left, right) => {
      const a = metric === 'rate' ? left.rate ?? -1 : left[metric]
      const b = metric === 'rate' ? right.rate ?? -1 : right[metric]
      return b - a
    })
    .slice(0, limit)
    .map((client, index) => {
      const value = metric === 'rate' ? pct(client.rate) : fmt(client[metric])
      return `${index + 1}. ${client.name} (${value})`
    })
}

function zeroReceivedSummary(clients: EnrichedClient[]) {
  return clients
    .filter((client) => client.conTot > 0 && client.recTot === 0)
    .sort((left, right) => right.conTot - left.conTot)
    .slice(0, 5)
    .map((client) => `${client.name} (${fmt(client.conTot)} contracted)`)
}

function criticalSummary(clients: EnrichedClient[]) {
  return clients
    .filter((client) => client.rate !== null && client.rate > 0 && client.rate < 0.15)
    .sort((left, right) => right.conTot - left.conTot)
    .slice(0, 5)
    .map((client) => `${client.name} (${pct(client.rate)})`)
}

function staffSummary(staff: EnrichedStaff[]) {
  return [...staff]
    .sort((left, right) => right.recTot - left.recTot)
    .slice(0, 5)
    .map((member) => `${member.name} (${fmt(member.recTot)})`)
}

export function buildWelcomeAnswer(): AssistantAnswer {
  return {
    title: 'Tax Assistant',
    summary: 'Ask about the current snapshot, client comparisons, risk flags, top clients, or staff workload.',
    bullets: ['Try: "compare TaxCo and Jennifer Wu"', 'Try: "who is critical"', 'Try: "top clients by contracted volume"'],
  }
}

export function answerDashboardQuestion(question: string, context: DashboardAssistantContext): AssistantAnswer {
  if (!context.kpis || context.clients.length === 0) {
    return {
      title: 'No Snapshot Loaded',
      summary: 'Upload a workbook and open a snapshot first.',
    }
  }

  const normalizedQuestion = normalize(question)
  const clients = enrichClients(context.clients)
  const staff = enrichStaff(context.staff)
  const mentionedClients = findMentionedClients(question, clients)
  const mentionedStaff = findMentionedStaff(question, staff)

  if ((normalizedQuestion.includes('compare') || normalizedQuestion.includes(' vs ') || normalizedQuestion.includes(' versus ')) && mentionedClients.length >= 2) {
    return buildComparison(mentionedClients[0], mentionedClients[1])
  }

  if (mentionedClients.length === 1) {
    return buildClientSummary(mentionedClients[0])
  }

  if (mentionedStaff.length === 1) {
    const member = mentionedStaff[0]
    return {
      title: `${member.name} (${member.staff_id})`,
      summary: `${member.staff_type} in the current staff workload section.`,
      cards: [
        { title: 'Total Received', value: fmt(member.recTot), meta: 'returns handled' },
        { title: 'Individual', value: fmt(member.received_ind), meta: 'individual returns' },
        { title: 'Business', value: fmt(member.received_bus), meta: 'business returns' },
      ],
    }
  }

  if (normalizedQuestion.includes('summary') || normalizedQuestion.includes('overview') || normalizedQuestion.includes('how are we doing') || normalizedQuestion.includes('snapshot')) {
    return buildSnapshotSummary(context.kpis)
  }

  if ((normalizedQuestion.includes('top') || normalizedQuestion.includes('highest') || normalizedQuestion.includes('largest')) && normalizedQuestion.includes('contract')) {
    return buildListAnswer('Top Contracted Clients', 'Clients with the highest contracted return volume.', topClientsByMetric(clients, 'conTot', 5))
  }

  if ((normalizedQuestion.includes('top') || normalizedQuestion.includes('highest')) && normalizedQuestion.includes('received')) {
    return buildListAnswer('Top Received Clients', 'Clients with the highest received return count.', topClientsByMetric(clients, 'recTot', 5))
  }

  if ((normalizedQuestion.includes('top') || normalizedQuestion.includes('highest')) && (normalizedQuestion.includes('pending') || normalizedQuestion.includes('outstanding'))) {
    return buildListAnswer('Top Pending Clients', 'Clients with the largest outstanding workload.', topClientsByMetric(clients, 'pending', 5))
  }

  if ((normalizedQuestion.includes('top') || normalizedQuestion.includes('highest')) && (normalizedQuestion.includes('rate') || normalizedQuestion.includes('ahead'))) {
    return buildListAnswer('Top Receipt Rates', 'Clients currently furthest along by receipt rate.', topClientsByMetric(clients, 'rate', 5))
  }

  if (normalizedQuestion.includes('zero received') || normalizedQuestion.includes('not started') || normalizedQuestion.includes('zero')) {
    const rows = zeroReceivedSummary(clients)
    return buildListAnswer('Zero Received Clients', rows.length > 0 ? 'Clients with contracted returns but no received returns yet.' : 'No clients have zero received returns in the current snapshot.', rows)
  }

  if (normalizedQuestion.includes('critical') || normalizedQuestion.includes('at risk') || normalizedQuestion.includes('risk')) {
    const rows = criticalSummary(clients)
    return buildListAnswer('Critical Clients', rows.length > 0 ? 'Clients under 15% receipt rate.' : 'There are no critical clients under 15% receipt rate in the current snapshot.', rows)
  }

  if (normalizedQuestion.includes('staff') || normalizedQuestion.includes('workload')) {
    const rows = staffSummary(staff)
    return buildListAnswer('Staff Workload', rows.length > 0 ? 'Top staff by received returns.' : 'No staff workload section is available in the current snapshot.', rows)
  }

  return {
    title: 'Supported Questions',
    summary: 'I can answer current-snapshot questions about clients, risk, and workload.',
    bullets: ['Compare named clients', 'Show top contracted/received/pending clients', 'List zero received or critical clients', 'Summarize staff workload'],
  }
}
