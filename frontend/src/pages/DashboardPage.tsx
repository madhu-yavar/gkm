import { useEffect, useMemo, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { useApi, type AnalysisSetDashboardViewResponse, type DashboardProposalResponse, type DashboardTabSpec, type WorkbookSchemaSheetSpec } from '../api'
import { useDashboardData, type ClientRow, type StaffRow } from '../dashboard/DashboardDataProvider'
import { TrendingUp, TrendingDown, AlertTriangle, BarChart3, Search, FileDown, X, ArrowLeft, ArrowRight, RotateCcw, ChevronDown, ChevronRight } from 'lucide-react'

type DashboardTab = string
type FilterOption = 'All' | 'Not Started' | 'Critical' | 'At Risk' | 'On Track' | 'Ahead' | 'Uncontracted'
type SortColumn = 'name' | 'conTot' | 'recTot' | 'pending' | 'rate' | 'conInd' | 'recInd' | 'conBus' | 'recBus'

type EnrichedClient = ClientRow & {
  name: string; id: string; conInd: number; conBus: number; conTot: number
  recInd: number; recBus: number; recTot: number; pending: number; rate: number | null; overContracted: boolean
}
type EnrichedStaff = StaffRow & { recTot: number }
type DashboardLayoutPreferences = {
  hidden_cards: string[]
  card_orders: Record<string, string[]>
}
type DashboardCardItem = {
  key: string
  label: string
  content: React.ReactNode
}
type AdaptiveRuntimeItem = {
  label: string
  value?: number
  share?: number
  meta?: string
  start?: number
  end?: number
}
type AdaptiveRuntimeWidget = {
  key: string
  tab: string
  title: string
  description?: string
  chart_type: string
  value_format?: 'number' | 'percent'
  options?: string[]
  selected_option?: string
  option_items?: Record<string, AdaptiveRuntimeItem[]>
  option_rows?: Record<string, Array<Record<string, string>>>
  items?: AdaptiveRuntimeItem[]
  rows?: Array<Record<string, string>>
  insight?: string
}
type AdaptiveRuntimeDashboard = {
  domain: string
  primary_entity: string
  primary_measure: string
  chart_preferences?: string[]
  kpis?: Array<{ key: string; label: string; value: string; meta?: string }>
  widgets?: AdaptiveRuntimeWidget[]
  supporting_notes?: string[]
}
type ProposalStatusTone = 'success' | 'warning'

function inferFocusTabFromRequest(
  config: { tabs?: DashboardTabSpec[] | null } | null | undefined,
  requestText: string,
  fallbackTab?: string | null,
) {
  const tabs = config?.tabs ?? []
  if (fallbackTab && tabs.some((item) => item.key === fallbackTab)) return fallbackTab
  const lowered = requestText.toLowerCase()
  if (tabs.some((item) => item.key === 'trends') && ['month', 'trend', 'forecast', 'period', 'odr'].some((token) => lowered.includes(token))) {
    return 'trends'
  }
  if (tabs.some((item) => item.key === 'comparison') && ['compare', 'comparison', 'versus', 'vs'].some((token) => lowered.includes(token))) {
    return 'comparison'
  }
  if (tabs.some((item) => item.key === 'quality') && ['quality', 'duplicate', 'outlier', 'missing'].some((token) => lowered.includes(token))) {
    return 'quality'
  }
  if (tabs.some((item) => item.key === 'analysis') && ['pool', 'tower', 'client', 'owner', 'staff', 'segment', 'product'].some((token) => lowered.includes(token))) {
    return 'analysis'
  }
  return tabs[0]?.key ?? 'overview'
}

function requestedChartPreferences(text: string) {
  const lowered = text.toLowerCase()
  const matches = ['scatter', 'pie', 'gantt', 'line', 'bar', 'table'].filter((item) => lowered.includes(item))
  return Array.from(new Set(matches))
}

function proposalChartPreferences(config: Record<string, unknown> | null | undefined) {
  const spec = config?.adaptive_dashboard_spec
  const fromSpec = spec && typeof spec === 'object' && Array.isArray((spec as Record<string, unknown>).chart_preferences)
    ? ((spec as Record<string, unknown>).chart_preferences as unknown[])
    : []
  const fromConfig = Array.isArray(config?.chart_preferences) ? (config.chart_preferences as unknown[]) : []
  return Array.from(
    new Set(
      [...fromSpec, ...fromConfig]
        .map((item) => (typeof item === 'string' ? item.trim().toLowerCase() : ''))
        .filter(Boolean),
    ),
  )
}

function proposalTabKeys(config: { tabs?: DashboardTabSpec[] | null } | null | undefined) {
  return new Set((config?.tabs ?? []).map((item) => item.key))
}

function proposalSectionKeys(config: { tabs?: DashboardTabSpec[] | null } | null | undefined) {
  return new Set((config?.tabs ?? []).flatMap((tab) => tab.sections.map((section) => `${tab.key}:${section.key}`)))
}

function summarizeProposalRefresh(
  previous: DashboardProposalResponse | null,
  next: DashboardProposalResponse,
  appliedInput: string,
): { tone: ProposalStatusTone; message: string } {
  const requestedCharts = requestedChartPreferences(appliedInput)
  const nextCharts = proposalChartPreferences(next.proposal)
  const acceptedCharts = requestedCharts.filter((item) => nextCharts.includes(item))
  const missingCharts = requestedCharts.filter((item) => !nextCharts.includes(item))
  const previousTabSet = proposalTabKeys(previous?.proposal)
  const nextTabSet = proposalTabKeys(next.proposal)
  const previousSectionSet = proposalSectionKeys(previous?.proposal)
  const nextSectionSet = proposalSectionKeys(next.proposal)
  const addedTabs = Array.from(nextTabSet).filter((item) => !previousTabSet.has(item))
  const addedSections = Array.from(nextSectionSet).filter((item) => !previousSectionSet.has(item))
  const highlights: string[] = []

  if (addedTabs.length) highlights.push(`new tabs: ${addedTabs.join(', ')}`)
  if (addedSections.length) highlights.push(`new widgets/sections: ${addedSections.length}`)
  if (acceptedCharts.length) highlights.push(`accepted chart types: ${acceptedCharts.join(', ')}`)

  if (highlights.length) {
    return { tone: 'success', message: `Proposal updated with ${highlights.join(' · ')}.` }
  }
  if (appliedInput.trim()) {
    const chartMessage = missingCharts.length ? ` Requested chart types not yet reflected: ${missingCharts.join(', ')}.` : ''
    return {
      tone: 'warning',
      message: `Proposal refreshed, but no structural widget change was detected.${chartMessage} Refine the request or review the current blueprint before approving.`,
    }
  }
  return {
    tone: 'success',
    message: 'Proposal refreshed. Review the latest blueprint details, then approve when ready.',
  }
}

function isSemanticBlob(value: unknown) {
  if (typeof value !== 'string') return false
  const text = value.trim()
  return (text.startsWith('{') && text.endsWith('}')) || (text.startsWith('[') && text.endsWith(']'))
}

function semanticDetailsFromConfig(config: { semantic_summary?: string | null; semantic_details?: Record<string, unknown> | null } | null | undefined) {
  const details = config?.semantic_details
  return details && typeof details === 'object' && !Array.isArray(details) ? details : null
}

function semanticObjectNames(value: unknown, keys: string[], limit = 6) {
  if (!Array.isArray(value)) return []
  return value
    .map((entry) => {
      if (!entry || typeof entry !== 'object') return null
      const record = entry as Record<string, unknown>
      for (const key of keys) {
        const raw = record[key]
        if (typeof raw === 'string' && raw.trim()) return raw.trim()
      }
      return null
    })
    .filter((item): item is string => Boolean(item))
    .slice(0, limit)
}

function semanticSummaryText(config: { semantic_summary?: string | null; semantic_details?: Record<string, unknown> | null } | null | undefined) {
  const details = semanticDetailsFromConfig(config)
  const description = typeof details?.description === 'string' ? details.description.trim() : ''
  const businessDomain = typeof details?.business_domain === 'string' ? details.business_domain.trim() : typeof details?.dominant_domain === 'string' ? details.dominant_domain.trim() : ''
  const summary = typeof config?.semantic_summary === 'string' ? config.semantic_summary.trim() : ''
  if (description && businessDomain) return `${businessDomain}: ${description}`
  if (description) return description
  if (summary && !isSemanticBlob(summary)) return summary
  return 'Semantic interpretation was captured for this workbook and normalized for adaptive dashboard generation.'
}

function ProposalSection({
  title,
  sectionKey,
  expanded,
  onToggle,
  children,
}: {
  title: string
  sectionKey: string
  expanded: boolean
  onToggle: (sectionKey: string) => void
  children: React.ReactNode
}) {
  return (
    <section className="rounded-2xl border border-border bg-card overflow-hidden">
      <button
        className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left"
        onClick={() => onToggle(sectionKey)}
        type="button"
      >
        <div className="text-sm font-bold text-foreground">{title}</div>
        {expanded ? <ChevronDown className="h-4 w-4 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 text-muted-foreground" />}
      </button>
      {expanded ? <div className="border-t border-border px-4 py-4">{children}</div> : null}
    </section>
  )
}

const filterOptions: FilterOption[] = ['All', 'Not Started', 'Critical', 'At Risk', 'On Track', 'Ahead', 'Uncontracted']
const defaultTabs: DashboardTabSpec[] = [
  { key: 'overview', label: 'Overview', description: 'Portfolio KPIs and distribution.', sections: [] },
  { key: 'clients', label: 'Client Table', description: 'Detailed client-level delivery view.', sections: [] },
  { key: 'staff', label: 'Staff Workload', description: 'Team throughput and concentration.', sections: [] },
  { key: 'risk', label: 'Risk Flags', description: 'Operational exceptions and follow-up priorities.', sections: [] },
]

function fmt(n: number) { return new Intl.NumberFormat().format(n) }
function pct(n: number | null) { if (n === null || Number.isNaN(n)) return '—'; return `${(n * 100).toFixed(1)}%` }
function formatDate(value: string) { const p = new Date(value); if (Number.isNaN(p.getTime())) return value; return new Intl.DateTimeFormat(undefined, { month: 'long', day: '2-digit', year: 'numeric' }).format(p) }

function riskLabel(client: EnrichedClient) {
  if (client.rate === null) return 'Uncontracted'
  if (client.rate === 0) return 'Not Started'
  if (client.rate < 0.15) return 'Critical'
  if (client.rate < 0.35) return 'At Risk'
  if (client.rate < 0.6) return 'On Track'
  return 'Ahead'
}

function riskColor(label: string) {
  const map: Record<string, { bg: string; text: string; bar: string }> = {
    'Not Started': { bg: 'hsl(var(--muted))', text: 'hsl(var(--foreground))', bar: 'hsl(var(--muted-foreground))' },
    Critical: { bg: 'hsl(0 84% 60% / 0.1)', text: 'hsl(0 84% 45%)', bar: 'hsl(0 84% 50%)' },
    'At Risk': { bg: 'hsl(38 92% 50% / 0.1)', text: 'hsl(38 80% 40%)', bar: 'hsl(38 92% 50%)' },
    'On Track': { bg: 'hsl(152 60% 38% / 0.1)', text: 'hsl(152 60% 30%)', bar: 'hsl(152 60% 38%)' },
    Ahead: { bg: 'hsl(217 91% 60% / 0.1)', text: 'hsl(217 91% 45%)', bar: 'hsl(217 91% 55%)' },
    Uncontracted: { bg: 'hsl(262 83% 58% / 0.08)', text: 'hsl(262 60% 45%)', bar: 'hsl(262 83% 58%)' },
  }
  return map[label] ?? { bg: 'hsl(var(--muted))', text: 'hsl(var(--muted-foreground))', bar: 'hsl(var(--muted-foreground))' }
}

function getSortValue(c: EnrichedClient, col: SortColumn) { if (col === 'name') return c.name.toLowerCase(); return c[col] ?? 0 }

function normalizeLayoutPreferences(value: DashboardLayoutPreferences | null | undefined): DashboardLayoutPreferences {
  return {
    hidden_cards: Array.isArray(value?.hidden_cards) ? value!.hidden_cards.filter((item) => typeof item === 'string' && item.trim()) : [],
    card_orders: value?.card_orders && typeof value.card_orders === 'object' ? value.card_orders : {},
  }
}

function orderDashboardCards(zoneKey: string, items: DashboardCardItem[], prefs: DashboardLayoutPreferences): DashboardCardItem[] {
  const hidden = new Set(prefs.hidden_cards)
  const order = prefs.card_orders[zoneKey] ?? []
  const rank = new Map(order.map((key, index) => [key, index]))
  return items
    .filter((item) => !hidden.has(item.key))
    .sort((left, right) => {
      const leftRank = rank.has(left.key) ? rank.get(left.key)! : Number.MAX_SAFE_INTEGER
      const rightRank = rank.has(right.key) ? rank.get(right.key)! : Number.MAX_SAFE_INTEGER
      if (leftRank !== rightRank) return leftRank - rightRank
      return items.findIndex((item) => item.key === left.key) - items.findIndex((item) => item.key === right.key)
    })
}

function DashboardManagedCard({
  item,
  zoneKey,
  index,
  total,
  onHide,
  onMove,
}: {
  item: DashboardCardItem
  zoneKey: string
  index: number
  total: number
  onHide: (cardKey: string) => void
  onMove: (zoneKey: string, cardKey: string, direction: 'left' | 'right') => void
}) {
  return (
    <div className="relative">
      <div className="absolute right-3 top-3 z-[1] flex items-center gap-1 rounded-lg border border-border/70 bg-card/95 px-1.5 py-1 shadow-soft">
        <button
          className="grid h-6 w-6 place-items-center rounded-md text-muted-foreground transition hover:bg-accent hover:text-foreground disabled:opacity-35"
          disabled={index === 0}
          onClick={() => onMove(zoneKey, item.key, 'left')}
          type="button"
        >
          <ArrowLeft className="h-3.5 w-3.5" />
        </button>
        <button
          className="grid h-6 w-6 place-items-center rounded-md text-muted-foreground transition hover:bg-accent hover:text-foreground disabled:opacity-35"
          disabled={index === total - 1}
          onClick={() => onMove(zoneKey, item.key, 'right')}
          type="button"
        >
          <ArrowRight className="h-3.5 w-3.5" />
        </button>
        <button
          className="grid h-6 w-6 place-items-center rounded-md text-muted-foreground transition hover:bg-destructive/10 hover:text-destructive"
          onClick={() => onHide(item.key)}
          type="button"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
      {item.content}
    </div>
  )
}

function formatAdaptiveValue(value: number, valueFormat: 'number' | 'percent' = 'number') {
  if (valueFormat === 'percent') return `${value.toFixed(2)}%`
  return fmt(Math.round(value))
}

function AdaptiveBarChartWithFormat({ items, valueFormat }: { items: AdaptiveRuntimeItem[]; valueFormat?: 'number' | 'percent' }) {
  const maxValue = Math.max(...items.map((item) => item.value ?? 0), 1)
  return (
    <div className="flex flex-col gap-3">
      {items.map((item) => (
        <div key={item.label}>
          <div className="mb-1 flex items-center justify-between gap-3 text-sm">
            <span className="truncate text-foreground">{item.label}</span>
            <span className="shrink-0 text-muted-foreground">{formatAdaptiveValue(item.value ?? 0, valueFormat)}</span>
          </div>
          <div className="h-2 overflow-hidden rounded-full bg-secondary">
            <div className="h-full rounded-full bg-primary/70" style={{ width: `${((item.value ?? 0) / maxValue) * 100}%` }} />
          </div>
        </div>
      ))}
    </div>
  )
}

function AdaptivePieChart({ items }: { items: AdaptiveRuntimeItem[] }) {
  const palette = ['#7c3aed', '#a855f7', '#c084fc', '#8b5cf6', '#6d28d9', '#5b21b6']
  const total = items.reduce((sum, item) => sum + (item.value ?? 0), 0) || 1
  let cursor = 0
  const stops = items.map((item, index) => {
    const start = cursor
    cursor += ((item.value ?? 0) / total) * 100
    return `${palette[index % palette.length]} ${start}% ${cursor}%`
  })
  return (
    <div className="grid grid-cols-[160px_1fr] items-center gap-4 max-sm:grid-cols-1">
      <div
        className="mx-auto h-40 w-40 rounded-full border border-border/60"
        style={{ background: `conic-gradient(${stops.join(', ')})` }}
      />
      <div className="flex flex-col gap-2">
        {items.map((item, index) => (
          <div key={item.label} className="flex items-center justify-between gap-3 text-sm">
            <div className="flex items-center gap-2">
              <span className="inline-block h-3 w-3 rounded-full" style={{ backgroundColor: palette[index % palette.length] }} />
              <span className="text-foreground">{item.label}</span>
            </div>
            <span className="text-muted-foreground">{((item.value ?? 0) / total * 100).toFixed(1)}%</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function AdaptiveLineChart({ items }: { items: AdaptiveRuntimeItem[] }) {
  const values = items.map((item) => item.value ?? 0)
  const maxValue = Math.max(...values, 1)
  const minValue = Math.min(...values, 0)
  const range = Math.max(maxValue - minValue, 1)
  const points = items.map((item, index) => {
    const x = items.length === 1 ? 160 : (index / Math.max(items.length - 1, 1)) * 320
    const y = 120 - (((item.value ?? 0) - minValue) / range) * 100
    return `${x},${y}`
  })
  return (
    <div className="space-y-3">
      <svg viewBox="0 0 320 140" className="h-40 w-full overflow-visible">
        <polyline fill="none" stroke="rgba(124,58,237,0.75)" strokeWidth="3" points={points.join(' ')} />
        {items.map((item, index) => {
          const x = items.length === 1 ? 160 : (index / Math.max(items.length - 1, 1)) * 320
          const y = 120 - (((item.value ?? 0) - minValue) / range) * 100
          return <circle key={`${item.label}-${index}`} cx={x} cy={y} r="4" fill="#7c3aed" />
        })}
      </svg>
      <div className="grid grid-cols-4 gap-2 text-xs text-muted-foreground max-sm:grid-cols-2">
        {items.map((item) => (
          <div key={item.label} className="rounded-xl border border-border bg-secondary/20 px-3 py-2">
            <div className="font-semibold text-foreground">{item.label}</div>
            <div>{fmt(Math.round(item.value ?? 0))}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

function AdaptiveScatterChart({ items }: { items: AdaptiveRuntimeItem[] }) {
  const values = items.map((item) => item.value ?? 0)
  const maxValue = Math.max(...values, 1)
  const minValue = Math.min(...values, 0)
  const range = Math.max(maxValue - minValue, 1)
  return (
    <div className="space-y-3">
      <svg viewBox="0 0 320 140" className="h-40 w-full overflow-visible">
        <line x1="24" y1="120" x2="312" y2="120" stroke="rgba(100,116,139,0.45)" strokeWidth="1.5" />
        <line x1="24" y1="12" x2="24" y2="120" stroke="rgba(100,116,139,0.45)" strokeWidth="1.5" />
        {items.map((item, index) => {
          const x = items.length === 1 ? 168 : 36 + (index / Math.max(items.length - 1, 1)) * 260
          const y = 120 - (((item.value ?? 0) - minValue) / range) * 96
          return (
            <g key={`${item.label}-${index}`}>
              <circle cx={x} cy={y} r="5" fill="#7c3aed" fillOpacity="0.85" />
              <text x={x} y={134} textAnchor="middle" fontSize="10" fill="rgba(71,85,105,0.95)">
                {item.label}
              </text>
            </g>
          )
        })}
      </svg>
      <div className="grid grid-cols-4 gap-2 text-xs text-muted-foreground max-sm:grid-cols-2">
        {items.map((item) => (
          <div key={item.label} className="rounded-xl border border-border bg-secondary/20 px-3 py-2">
            <div className="font-semibold text-foreground">{item.label}</div>
            <div>{fmt(Math.round(item.value ?? 0))}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

function AdaptiveGanttChart({ items }: { items: AdaptiveRuntimeItem[] }) {
  const maxEnd = Math.max(...items.map((item) => item.end ?? 0), 1)
  return (
    <div className="flex flex-col gap-3">
      {items.map((item) => {
        const start = item.start ?? 0
        const end = item.end ?? start + 1
        return (
          <div key={item.label} className="grid grid-cols-[140px_1fr] items-center gap-3 max-sm:grid-cols-1">
            <div className="text-sm font-medium text-foreground">{item.label}</div>
            <div className="relative h-8 rounded-xl bg-secondary/50">
              <div
                className="absolute top-1.5 h-5 rounded-lg bg-primary/70"
                style={{
                  left: `${(start / maxEnd) * 100}%`,
                  width: `${((end - start) / maxEnd) * 100}%`,
                }}
              />
            </div>
          </div>
        )
      })}
    </div>
  )
}

function AdaptiveTable({ rows }: { rows: Array<Record<string, string>> }) {
  const columns = rows.length ? Object.keys(rows[0]) : []
  return (
    <div className="overflow-auto rounded-2xl border border-border">
      <table className="w-full border-collapse">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column} className="border-b border-border bg-secondary px-3 py-2 text-left text-[0.68rem] font-bold uppercase tracking-widest text-muted-foreground">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={`row-${index}`} className="odd:bg-card even:bg-secondary/10">
              {columns.map((column) => (
                <td key={`${index}-${column}`} className="border-b border-border/60 px-3 py-2 text-sm text-foreground">
                  {row[column]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function AdaptiveWidgetCard({ widget }: { widget: AdaptiveRuntimeWidget }) {
  const optionLabels = widget.options ?? Object.keys(widget.option_items ?? {})
  const [selectedOption, setSelectedOption] = useState<string>(widget.selected_option ?? optionLabels[0] ?? '')
  useEffect(() => {
    setSelectedOption(widget.selected_option ?? optionLabels[0] ?? '')
  }, [widget.selected_option, widget.key, optionLabels])
  const items = selectedOption && widget.option_items?.[selectedOption]
    ? widget.option_items[selectedOption]
    : (widget.items ?? [])
  const rows = selectedOption && widget.option_rows?.[selectedOption]
    ? widget.option_rows[selectedOption]
    : (widget.rows ?? [])
  return (
    <article className="rounded-2xl border border-border bg-card p-4 shadow-soft">
      <h2 className="text-base font-bold text-foreground">{widget.title}</h2>
      {widget.description ? <p className="mt-1 text-sm text-muted-foreground">{widget.description}</p> : null}
      {optionLabels.length > 1 ? (
        <div className="mt-3 flex items-center gap-2">
          <label className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Month</label>
          <select
            className="rounded-xl border border-border bg-card px-3 py-2 text-sm text-foreground"
            onChange={(event) => setSelectedOption(event.target.value)}
            value={selectedOption}
          >
            {optionLabels.map((item) => (
              <option key={item} value={item}>{item}</option>
            ))}
          </select>
        </div>
      ) : null}
      <div className="mt-4">
        {widget.chart_type === 'pie' && items.length > 0 ? <AdaptivePieChart items={items} /> : null}
        {widget.chart_type === 'line' && items.length > 0 ? <AdaptiveLineChart items={items} /> : null}
        {widget.chart_type === 'scatter' && items.length > 0 ? <AdaptiveScatterChart items={items} /> : null}
        {widget.chart_type === 'gantt' && items.length > 0 ? <AdaptiveGanttChart items={items} /> : null}
        {widget.chart_type === 'table' && rows.length > 0 ? <AdaptiveTable rows={rows} /> : null}
        {widget.chart_type === 'bar' && items.length > 0 ? <AdaptiveBarChartWithFormat items={items} valueFormat={widget.value_format} /> : null}
        {!['pie', 'line', 'scatter', 'gantt', 'table', 'bar'].includes(widget.chart_type) && items.length > 0 ? <AdaptiveBarChartWithFormat items={items} valueFormat={widget.value_format} /> : null}
        {widget.chart_type !== 'table' && items.length === 0 && rows.length === 0 ? <p className="text-sm text-muted-foreground">No chart-ready data was available for this widget.</p> : null}
        {widget.chart_type === 'table' && rows.length === 0 && items.length > 0 ? <AdaptiveBarChartWithFormat items={items} valueFormat={widget.value_format} /> : null}
      </div>
      {widget.insight ? <p className="mt-4 rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">{widget.insight}</p> : null}
    </article>
  )
}

export function DashboardPage() {
  const api = useApi()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const { snapshots, snapshotId, setSnapshotId, kpis, clients, staff, blueprint, runtime, loading, error, refreshBlueprint, refreshDashboard } = useDashboardData()
  const analysisSetId = Number(searchParams.get('analysis_set_id') || '')
  const snapshotParamId = Number(searchParams.get('snapshot_id') || '')
  const [tab, setTab] = useState<DashboardTab>('overview')
  const [sortCol, setSortCol] = useState<SortColumn>('rate')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [filter, setFilter] = useState<FilterOption>('All')
  const [search, setSearch] = useState('')
  const [selectedClientId, setSelectedClientId] = useState<string | null>(null)
  const [reportError, setReportError] = useState<string | null>(null)
  const [downloadingOverall, setDownloadingOverall] = useState(false)
  const [downloadingAnalytics, setDownloadingAnalytics] = useState(false)
  const [downloadingClientId, setDownloadingClientId] = useState<string | null>(null)
  const [proposal, setProposal] = useState<DashboardProposalResponse | null>(null)
  const [proposalError, setProposalError] = useState<string | null>(null)
  const [proposalRefreshLoading, setProposalRefreshLoading] = useState(false)
  const [proposalApproveLoading, setProposalApproveLoading] = useState(false)
  const [showProposalModal, setShowProposalModal] = useState(false)
  const [proposalSnapshotId, setProposalSnapshotId] = useState<number | null>(null)
  const [proposalGuidance, setProposalGuidance] = useState('')
  const [proposalQuestionAnswers, setProposalQuestionAnswers] = useState<Record<string, string>>({})
  const [lastAppliedProposalInput, setLastAppliedProposalInput] = useState('')
  const [proposalStatusNote, setProposalStatusNote] = useState<string | null>(null)
  const [proposalStatusTone, setProposalStatusTone] = useState<ProposalStatusTone>('success')
  const [dashboardStatusNote, setDashboardStatusNote] = useState<string | null>(null)
  const [dashboardStatusTone, setDashboardStatusTone] = useState<ProposalStatusTone>('success')
  const [proposalSectionState, setProposalSectionState] = useState<Record<string, boolean>>({
    rationale: false,
    guidance: true,
    refinement: false,
    semantics: false,
    edaPlan: false,
    edaEvidence: false,
    layout: false,
  })
  const [layoutPreferences, setLayoutPreferences] = useState<DashboardLayoutPreferences>({ hidden_cards: [], card_orders: {} })
  const [combinedView, setCombinedView] = useState<AnalysisSetDashboardViewResponse | null>(null)
  const [combinedLoading, setCombinedLoading] = useState(false)
  const [combinedError, setCombinedError] = useState<string | null>(null)

  function draftProposalFromBlueprint() {
    if (!blueprint || !activeDashboardSnapshotId) return null
    return {
      id: -activeDashboardSnapshotId,
      snapshot_id: activeDashboardSnapshotId,
      status: 'approved',
      match_mode: 'approved_blueprint',
      confidence_score: 1,
      title: blueprint.config.title || blueprint.name,
      summary: blueprint.config.semantic_summary || blueprint.description,
      rationale: 'Opened from the currently approved blueprint. Refresh the proposal if you want the agent to rethink the widgets from your latest guidance.',
      schema_signature: blueprint.schema_signature,
      workbook_type: blueprint.workbook_type,
      matched_blueprint_id: blueprint.id,
      approved_blueprint_id: blueprint.id,
      proposal: blueprint.config,
    } as DashboardProposalResponse
  }

  function openProposalModalFromCurrentState() {
    if (snapshotContextMismatch) {
      setProposalError('The dashboard is still showing a different snapshot than the one requested. Wait for the correct file to load before refining widgets.')
      return false
    }
    const seeded = proposal ?? draftProposalFromBlueprint()
    if (!seeded) return false
    setProposal(seeded)
    setProposalSnapshotId(activeDashboardSnapshotId ?? null)
    setShowProposalModal(true)
    setProposalError(null)
    return true
  }

  function goToSnapshot(nextSnapshotId: number) {
    if (!Number.isFinite(nextSnapshotId) || nextSnapshotId <= 0) return
    setSnapshotId(nextSnapshotId)
    navigate(`/dashboard?snapshot_id=${nextSnapshotId}`)
  }

  useEffect(() => {
    if (!Number.isFinite(analysisSetId) || analysisSetId <= 0) {
      setCombinedView(null)
      setCombinedError(null)
      return
    }
    setCombinedLoading(true)
    setCombinedError(null)
    void (async () => {
      try {
        const view = await api.getAnalysisSetDashboardView(analysisSetId)
        setCombinedView(view)
      } catch {
        setCombinedError('Could not load the combined dashboard view.')
      } finally {
        setCombinedLoading(false)
      }
    })()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [analysisSetId])

  useEffect(() => {
    if (combinedView?.dashboard_config?.tabs?.length) {
      setTab(combinedView.dashboard_config.tabs[0].key)
    }
  }, [combinedView?.dashboard_config?.tabs])

  useEffect(() => {
    if (!Number.isFinite(snapshotParamId) || snapshotParamId <= 0) return
    if (snapshotId === snapshotParamId) return
    if (snapshots.length > 0 && !snapshots.some((item) => item.id === snapshotParamId)) return
    setSnapshotId(snapshotParamId)
  }, [snapshotParamId, snapshotId, snapshots, setSnapshotId])

  useEffect(() => { setSelectedClientId(null) }, [snapshotId])
  const tabs = blueprint?.config.tabs?.length ? blueprint.config.tabs : defaultTabs
  const schemaFields: WorkbookSchemaSheetSpec[] = proposal?.proposal.schema_fields ?? blueprint?.config.schema_fields ?? []
  const dashboardFamily = blueprint?.config.dashboard_family ?? proposal?.proposal.dashboard_family ?? 'variance_dashboard'
  const dashboardTitle = blueprint?.config.title ?? proposal?.proposal.title ?? 'Tax Returns — Contracted vs Received'
  const dashboardSubtitle = blueprint?.config.subtitle ?? proposal?.proposal.subtitle ?? 'Snapshot analytics for the active tax season'
  const statusRuntime = (runtime?.payload ?? null) as {
    total_returns?: number
    completed_returns?: number
    open_returns?: number
    awaiting_answers?: number
    under_review?: number
    in_process?: number
    ready_for_preparation?: number
    status_counts?: Array<{ label: string; count: number }>
    return_type_counts?: Array<{ label: string; count: number }>
    client_type_counts?: Array<{ label: string; count: number }>
    open_queue?: Array<{ tax_payer_name: string; return_code: string; return_type: string; return_status: string; client_type: string; age_days: number | null; cpa_notes?: string; gkm_notes?: string }>
    stale_items?: Array<{ tax_payer_name: string; return_status: string; age_days: number | null }>
    note_rows?: Array<{ tax_payer_name: string; return_code: string; cpa_notes?: string; gkm_notes?: string }>
  } | null
  const productRuntime = (runtime?.payload ?? null) as {
    total_products?: number
    product_type_count?: number
    uom_count?: number
    category_count?: number
    product_type_counts?: Array<{ label: string; count: number }>
    uom_counts?: Array<{ label: string; count: number }>
    category_counts?: Array<{ label: string; count: number }>
    catalog_rows?: Array<{ product_id: string; description: string; product_type: string; base_uom: string; category: string; hsn_code: string }>
    quality_gaps?: Array<{ product_id: string; description: string; missing_fields: string[] }>
  } | null
  const genericRuntime = (runtime?.payload ?? null) as {
    mode?: string
    total_sheets?: number
    tabular_sheet_count?: number
    reference_sheet_count?: number
    total_rows?: number
    numeric_measure_count?: number
    comparison_group_count?: number
    text_reference_items?: string[]
    comparison_groups?: Array<{
      group_label: string
      leading_series: string
      leading_total: number
      rate_basis?: string
      matched_pool_count?: number
      unmatched_tc_pool_count?: number
      unmatched_bc_pool_count?: number
      lowest_rate_segments?: Array<{ label: string; ratio: number; bad_count: number; total_count: number }>
      highest_rate_segments?: Array<{ label: string; ratio: number; bad_count: number; total_count: number }>
      series_totals: Array<{ series: string; sheet_name: string; grand_total: number }>
    }>
    sheet_summaries?: Array<{
      sheet_name: string
      sheet_kind: string
      row_count: number
      column_count: number
      headers: string[]
      dimension_header?: string | null
      measure_count?: number | null
      grand_total?: number | null
      measure_totals?: Array<{ label: string; total: number }>
      top_segments?: Array<{ label: string; total: number }>
      text_items?: string[]
    }>
    adaptive_dashboard?: AdaptiveRuntimeDashboard
  } | null
  const activeDashboardSnapshotId = runtime?.snapshot_id ?? kpis?.snapshot.id ?? snapshotId
  const activeDashboardSnapshotRecord = snapshots.find((item) => item.id === activeDashboardSnapshotId)
  const activeDashboardSnapshotLabel = kpis?.snapshot
    ? `${kpis.snapshot.as_of_date} · ${kpis.snapshot.source_filename}`
    : activeDashboardSnapshotRecord
      ? `${activeDashboardSnapshotRecord.as_of_date} · ${activeDashboardSnapshotRecord.source_filename}`
      : null
  const snapshotContextMismatch = Number.isFinite(snapshotParamId)
    && snapshotParamId > 0
    && activeDashboardSnapshotId !== undefined
    && activeDashboardSnapshotId !== snapshotParamId

  useEffect(() => {
    if (!tabs.some((item) => item.key === tab)) setTab(tabs[0]?.key ?? 'overview')
  }, [tab, tabs])

  useEffect(() => {
    if (!activeDashboardSnapshotId) {
      setProposal(null)
      setProposalSnapshotId(null)
      setShowProposalModal(false)
      setProposalGuidance('')
      setProposalQuestionAnswers({})
      setLastAppliedProposalInput('')
      setProposalStatusNote(null)
      setProposalStatusTone('success')
      setDashboardStatusNote(null)
      setDashboardStatusTone('success')
      return
    }
    if (proposalSnapshotId !== activeDashboardSnapshotId) {
      setProposal(null)
      setProposalSnapshotId(activeDashboardSnapshotId)
      setShowProposalModal(false)
      setProposalGuidance('')
      setProposalQuestionAnswers({})
      setLastAppliedProposalInput('')
      setProposalStatusNote(null)
      setProposalStatusTone('success')
      setDashboardStatusNote(null)
      setDashboardStatusTone('success')
      setProposalError(null)
    }
  }, [activeDashboardSnapshotId, proposalSnapshotId])

  useEffect(() => {
    setProposalQuestionAnswers({})
    setLastAppliedProposalInput('')
    setProposalStatusNote(null)
    setProposalStatusTone('success')
  }, [proposal?.id])

  useEffect(() => {
    setLayoutPreferences(normalizeLayoutPreferences(blueprint?.config.dashboard_preferences))
  }, [blueprint?.id, blueprint?.config.dashboard_preferences])

  function buildProposalInput(guidance: string, answers: Record<string, string>) {
    const cleanedGuidance = guidance.trim()
    const answeredQuestions = Object.entries(answers)
      .map(([question, answer]) => [question.trim(), answer.trim()] as const)
      .filter(([, answer]) => Boolean(answer))
    if (!answeredQuestions.length) return cleanedGuidance
    const answersBlock = answeredQuestions
      .map(([question, answer]) => `Question: ${question}\nAnswer: ${answer}`)
      .join('\n\n')
    return cleanedGuidance ? `${cleanedGuidance}\n\nBusiness question answers:\n${answersBlock}` : `Business question answers:\n${answersBlock}`
  }

  async function openProposalReview(userGuidance?: string) {
    const targetSnapshotId = activeDashboardSnapshotId ?? snapshotId
    if (!targetSnapshotId) return
    if (snapshotContextMismatch) {
      setProposalError('The dashboard is still syncing to another snapshot. Wait until the correct file is loaded before generating a proposal.')
      return
    }
    setProposalRefreshLoading(true)
    setProposalError(null)
    setProposalStatusNote(null)
    setProposalStatusTone('success')
    setDashboardStatusNote(null)
    const proposalInput = buildProposalInput(userGuidance ?? proposalGuidance, proposalQuestionAnswers)
    const previousProposal = proposal
    try {
      const nextProposal = await api.proposeDashboardBlueprint(
        targetSnapshotId,
        proposalInput,
      )
      setProposal(nextProposal)
      setProposalSnapshotId(targetSnapshotId)
      setLastAppliedProposalInput(proposalInput)
      setShowProposalModal(true)
      setProposalSectionState((current) => ({ ...current, refinement: true }))
      const backendResult = nextProposal.refinement_result
      if (backendResult && backendResult.status !== 'not_requested') {
        setProposalStatusTone(backendResult.status === 'rejected' ? 'warning' : 'success')
        setProposalStatusNote(backendResult.message)
      } else {
        const summary = summarizeProposalRefresh(previousProposal, nextProposal, proposalInput)
        setProposalStatusTone(summary.tone)
        setProposalStatusNote(summary.message)
      }
    } catch {
      setProposalError('Could not generate a dashboard proposal for this snapshot.')
    } finally {
      setProposalRefreshLoading(false)
    }
  }

  async function approveCurrentProposal() {
    if (!proposal) return
    if (snapshotContextMismatch || (activeDashboardSnapshotId && proposal.snapshot_id !== activeDashboardSnapshotId)) {
      setProposalError('This proposal does not match the dashboard snapshot currently loaded. Reopen the correct file before approval.')
      return
    }
    setProposalApproveLoading(true)
    setProposalError(null)
    setProposalStatusNote(null)
    setProposalStatusTone('success')
    setDashboardStatusNote(null)
    setDashboardStatusTone('success')
    try {
      const hasQuestionAnswers = Object.values(proposalQuestionAnswers).some((value) => value.trim())
      const currentProposalInput = buildProposalInput(proposalGuidance, proposalQuestionAnswers)
      const needsFreshProposal = proposal.id <= 0 || currentProposalInput !== lastAppliedProposalInput || (!lastAppliedProposalInput && (hasQuestionAnswers || proposalGuidance.trim()))
      const proposalToApprove = needsFreshProposal
        ? await api.proposeDashboardBlueprint(
            proposal.snapshot_id,
            currentProposalInput,
          )
        : proposal
      if (proposalToApprove.refinement_result?.status === 'rejected') {
        setProposal(proposalToApprove)
        setProposalStatusTone('warning')
        setProposalStatusNote(proposalToApprove.refinement_result.message)
        setProposalError('The latest widget request was rejected. Update the request or clear it before approval.')
        return
      }
      setProposal(proposalToApprove)
      setLastAppliedProposalInput(currentProposalInput)
      await api.approveDashboardProposal(proposalToApprove.id)
      const focusTab = inferFocusTabFromRequest(
        proposalToApprove.proposal,
        currentProposalInput,
        proposalToApprove.refinement_result?.diff.added_tabs[0] ?? null,
      )
      try {
        await refreshDashboard(proposalToApprove.snapshot_id)
        setTab(focusTab)
        setDashboardStatusTone('success')
        setDashboardStatusNote(`Blueprint approved. Review the updated widget under ${focusTab[0].toUpperCase()}${focusTab.slice(1)}.`)
      } catch {
        setProposalStatusTone('warning')
        setProposalStatusNote('Blueprint approved, but the dashboard refresh did not complete immediately. Reopen the dashboard or refresh the page.')
        setDashboardStatusTone('warning')
        setDashboardStatusNote('Blueprint approved, but the dashboard did not refresh immediately. Refresh the page and review the updated tab.')
      }
      setProposal(null)
      setShowProposalModal(false)
    } catch {
      setProposalError('Could not approve this dashboard proposal.')
    } finally {
      setProposalApproveLoading(false)
    }
  }

  async function persistLayoutPreferences(nextPreferences: DashboardLayoutPreferences) {
    setLayoutPreferences(nextPreferences)
    if (!activeDashboardSnapshotId || !blueprint) return
    try {
      await api.updateDashboardBlueprintPreferences(nextPreferences, activeDashboardSnapshotId)
      await refreshBlueprint(activeDashboardSnapshotId)
    } catch {
      setProposalError('Could not save dashboard layout changes.')
    }
  }

  function hideDashboardCard(cardKey: string) {
    if (layoutPreferences.hidden_cards.includes(cardKey)) return
    void persistLayoutPreferences({
      ...layoutPreferences,
      hidden_cards: [...layoutPreferences.hidden_cards, cardKey],
    })
  }

  function moveDashboardCard(zoneKey: string, cardKey: string, direction: 'left' | 'right', items: DashboardCardItem[]) {
    const ordered = orderDashboardCards(zoneKey, items, layoutPreferences)
    const index = ordered.findIndex((item) => item.key === cardKey)
    if (index < 0) return
    const swapIndex = direction === 'left' ? index - 1 : index + 1
    if (swapIndex < 0 || swapIndex >= ordered.length) return
    const nextOrdered = [...ordered]
    const [moved] = nextOrdered.splice(index, 1)
    nextOrdered.splice(swapIndex, 0, moved)
    void persistLayoutPreferences({
      ...layoutPreferences,
      card_orders: {
        ...layoutPreferences.card_orders,
        [zoneKey]: nextOrdered.map((item) => item.key),
      },
    })
  }

  function resetDashboardLayout() {
    void persistLayoutPreferences({ hidden_cards: [], card_orders: {} })
  }

  const enrichedClients = useMemo<EnrichedClient[]>(() =>
    clients.map((c) => ({
      ...c, name: c.client_name, id: c.client_id,
      conInd: c.contracted_ind, conBus: c.contracted_bus, conTot: c.contracted_total,
      recInd: c.received_ind, recBus: c.received_bus, recTot: c.received_total,
      pending: c.pending_total,
      rate: c.contracted_total > 0 ? c.receipt_rate ?? 0 : c.received_total > 0 ? null : 0,
      overContracted: c.received_total > c.contracted_total,
    })), [clients])

  const enrichedStaff = useMemo<EnrichedStaff[]>(() => staff.map((m) => ({ ...m, recTot: m.received_total })), [staff])
  const genericSheetCount = schemaFields.length
  const genericSectionCount = schemaFields.reduce((sum, sheet) => sum + sheet.sections.length, 0)
  const genericFieldCount = schemaFields.reduce((sum, sheet) => sum + sheet.sections.reduce((inner, section) => inner + section.fields.length, 0), 0)
  const genericBusinessQuestions = (proposal?.proposal.business_questions ?? blueprint?.config.business_questions ?? []).length
  const genericAmbiguities = (proposal?.proposal.ambiguities ?? blueprint?.config.ambiguities ?? []).length
  const activeGenericSheet = useMemo(() => {
    const tabIndex = tabs.findIndex((item) => item.key === tab)
    return tabIndex >= 0 ? schemaFields[tabIndex] ?? null : null
  }, [schemaFields, tab, tabs])
  const activeGenericRuntime = useMemo(() => {
    const activeSheetName = activeGenericSheet?.sheet_name
    if (!activeSheetName) return null
    return genericRuntime?.sheet_summaries?.find((item) => item.sheet_name === activeSheetName) ?? null
  }, [activeGenericSheet, genericRuntime])
  const activeGenericComparison = useMemo(() => {
    const sheetName = activeGenericSheet?.sheet_name
    if (!sheetName) return null
    return genericRuntime?.comparison_groups?.find((group) =>
      group.series_totals.some((item) => item.sheet_name === sheetName),
    ) ?? null
  }, [activeGenericSheet, genericRuntime])
  const adaptiveGenericDashboard = genericRuntime?.adaptive_dashboard ?? null
  const adaptiveWidgets = adaptiveGenericDashboard?.widgets ?? []
  const activeAdaptiveWidgets = adaptiveWidgets.filter((item) => item.tab === tab)
  const currentDraftProposalInput = buildProposalInput(proposalGuidance, proposalQuestionAnswers)
  const proposalApproveBlocked = proposal?.refinement_result?.status === 'rejected' && currentDraftProposalInput === lastAppliedProposalInput

  const sortedClients = useMemo(() => {
    const filtered = enrichedClients.filter((c) => {
      const mf = filter === 'All' || riskLabel(c) === filter
      const t = search.trim().toLowerCase()
      const ms = t.length === 0 || c.name.toLowerCase().includes(t) || c.id.toLowerCase().includes(t)
      return mf && ms
    })
    return [...filtered].sort((a, b) => {
      const av = getSortValue(a, sortCol); const bv = getSortValue(b, sortCol)
      if (typeof av === 'string' && typeof bv === 'string') return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      const an = typeof av === 'number' ? av : -Infinity; const bn = typeof bv === 'number' ? bv : -Infinity
      return sortDir === 'asc' ? an - bn : bn - an
    })
  }, [enrichedClients, filter, search, sortCol, sortDir])

  const selectedClient = useMemo(() => selectedClientId ? enrichedClients.find((c) => c.id === selectedClientId) ?? null : null, [enrichedClients, selectedClientId])
  const topClients = useMemo(() => [...enrichedClients].sort((a, b) => b.conTot - a.conTot).slice(0, 10), [enrichedClients])
  const zeroReceivedClients = useMemo(() => enrichedClients.filter((c) => c.rate === 0 && c.conTot > 0).sort((a, b) => b.conTot - a.conTot), [enrichedClients])
  const criticalClients = useMemo(() => enrichedClients.filter((c) => c.rate !== null && c.rate > 0 && c.rate < 0.15).sort((a, b) => b.conTot - a.conTot), [enrichedClients])
  const anomalyClients = useMemo(() => enrichedClients.filter((c) => c.overContracted || (c.rate === null && c.recTot > 0)), [enrichedClients])
  const aheadClients = useMemo(() => enrichedClients.filter((c) => c.rate !== null && c.rate >= 0.6).sort((a, b) => (b.rate ?? 0) - (a.rate ?? 0)), [enrichedClients])

  const combinedDashboardConfig = combinedView?.dashboard_config ?? null
  const combinedRuntime = (combinedView?.runtime_payload ?? null) as {
    adaptive_dashboard?: AdaptiveRuntimeDashboard
  } | null
  const combinedAdaptiveDashboard = combinedRuntime?.adaptive_dashboard ?? null
  const combinedTabs = combinedDashboardConfig?.tabs?.length ? combinedDashboardConfig.tabs : defaultTabs
  const combinedWidgets = combinedAdaptiveDashboard?.widgets ?? []
  const activeCombinedWidgets = combinedWidgets.filter((item) => item.tab === tab)

  function toggleProposalSection(sectionKey: string) {
    setProposalSectionState((current) => ({ ...current, [sectionKey]: !current[sectionKey] }))
  }

  const distribution = useMemo(() => [
    { label: 'Ahead (≥60%)', clients: enrichedClients.filter((c) => c.rate !== null && c.rate >= 0.6), tone: 'Ahead' },
    { label: 'On Track (35–60%)', clients: enrichedClients.filter((c) => c.rate !== null && c.rate >= 0.35 && c.rate < 0.6), tone: 'On Track' },
    { label: 'At Risk (15–35%)', clients: enrichedClients.filter((c) => c.rate !== null && c.rate >= 0.15 && c.rate < 0.35), tone: 'At Risk' },
    { label: 'Critical (<15%)', clients: enrichedClients.filter((c) => c.rate !== null && c.rate > 0 && c.rate < 0.15), tone: 'Critical' },
    { label: 'Not Started (0%)', clients: enrichedClients.filter((c) => c.rate === 0), tone: 'Not Started' },
    { label: 'Uncontracted', clients: enrichedClients.filter((c) => c.rate === null && c.recTot > 0), tone: 'Uncontracted' },
  ], [enrichedClients])

  function handleSort(col: SortColumn) { if (sortCol === col) { setSortDir((d) => d === 'asc' ? 'desc' : 'asc') } else { setSortCol(col); setSortDir(col === 'name' ? 'asc' : 'desc') } }

  if (!snapshotId && snapshots.length === 0) {
    return (
      <div className="flex flex-col gap-4">
        <div className="bg-card border border-border rounded-2xl p-8 text-center animate-fade-in">
          <BarChart3 className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
          <h1 className="text-xl font-bold text-foreground mb-2">Dashboard</h1>
          <p className="text-muted-foreground">Upload a contracted vs actual workbook in Documents Processing to generate the first snapshot.</p>
        </div>
      </div>
    )
  }

  if (Number.isFinite(analysisSetId) && analysisSetId > 0) {
    if (combinedLoading) {
      return (
        <div className="bg-card border border-border rounded-2xl p-8 text-center animate-fade-in">
          <BarChart3 className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
          <h1 className="text-xl font-bold text-foreground mb-2">Combined Dashboard</h1>
          <p className="text-muted-foreground">Loading the merged dashboard built from the confirmed multi-document analysis set.</p>
        </div>
      )
    }
    if (combinedError || !combinedView || !combinedDashboardConfig || !combinedAdaptiveDashboard) {
      return (
        <div className="bg-card border border-border rounded-2xl p-8 text-center animate-fade-in">
          <AlertTriangle className="w-12 h-12 text-warning mx-auto mb-4" />
          <h1 className="text-xl font-bold text-foreground mb-2">Combined Dashboard</h1>
          <p className="text-muted-foreground">{combinedError ?? 'The combined dashboard is not ready yet.'}</p>
        </div>
      )
    }
    return (
      <div className="flex flex-col gap-4 animate-fade-in">
        <header
          className="rounded-2xl p-5 text-primary-foreground shadow-elevated relative overflow-hidden"
          style={{ background: 'linear-gradient(135deg, hsl(262, 38%, 18%) 0%, hsl(268, 44%, 16%) 42%, hsl(274, 52%, 20%) 100%)' }}
        >
          <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,hsla(270,70%,65%,0.22),transparent_28%),radial-gradient(circle_at_bottom_left,hsla(262,83%,58%,0.18),transparent_26%)] pointer-events-none" />
          <div className="relative flex items-start justify-between gap-5 flex-wrap">
            <div>
              <span className="inline-flex items-center px-3 py-1 rounded-full border border-primary-foreground/12 bg-primary-foreground/10 text-primary-foreground text-xs font-bold mb-3">
                Combined Analysis
              </span>
              <h1 className="text-2xl lg:text-3xl font-black leading-tight tracking-tight">{combinedDashboardConfig.title}</h1>
              <p className="mt-1.5 text-primary-foreground/70 text-sm">
                {combinedDashboardConfig.subtitle}
              </p>
            </div>
            <div className="flex flex-col items-end gap-3">
              <div className="flex items-center gap-2 flex-wrap justify-end">
                <button
                  onClick={async () => {
                    setReportError(null)
                    try {
                      setDownloadingOverall(true)
                      await api.downloadCombinedExecutiveReport(analysisSetId)
                    } catch {
                      setReportError('Combined executive report download failed.')
                    } finally {
                      setDownloadingOverall(false)
                    }
                  }}
                  className="inline-flex items-center gap-2 rounded-xl border border-primary-foreground/15 bg-primary-foreground/10 px-3.5 py-2 text-sm font-semibold text-primary-foreground transition hover:bg-primary-foreground/15"
                  type="button"
                >
                  <FileDown className="w-4 h-4" />
                  {downloadingOverall ? 'Preparing PDF…' : 'Combined Executive PDF'}
                </button>
                <button
                  onClick={async () => {
                    setReportError(null)
                    try {
                      setDownloadingAnalytics(true)
                      await api.downloadCombinedAnalyticsReport(analysisSetId)
                    } catch {
                      setReportError('Combined analytics report download failed.')
                    } finally {
                      setDownloadingAnalytics(false)
                    }
                  }}
                  className="inline-flex items-center gap-2 rounded-xl border border-primary-foreground/15 bg-primary-foreground/10 px-3.5 py-2 text-sm font-semibold text-primary-foreground transition hover:bg-primary-foreground/15"
                  type="button"
                >
                  <FileDown className="w-4 h-4" />
                  {downloadingAnalytics ? 'Preparing PDF…' : 'Combined Analytics PDF'}
                </button>
                <div className="inline-flex gap-1.5 p-1 rounded-xl bg-primary-foreground/10 backdrop-blur-sm border border-primary-foreground/10">
                  {combinedTabs.map((item) => (
                    <button
                      key={item.key}
                      onClick={() => setTab(item.key)}
                      className={`px-3.5 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${tab === item.key ? 'text-foreground shadow-card' : 'text-primary-foreground/75 hover:text-primary-foreground hover:bg-primary-foreground/5'}`}
                      style={tab === item.key ? { background: 'linear-gradient(180deg, hsla(0, 0%, 100%, 0.98), hsla(262, 60%, 96%, 0.92))' } : undefined}
                    >
                      {item.label}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </header>

        {reportError ? <div className="bg-destructive/10 border border-destructive/30 text-destructive px-4 py-3 rounded-xl text-sm font-semibold">{reportError}</div> : null}

        <section className="grid grid-cols-4 max-lg:grid-cols-2 max-md:grid-cols-1 gap-3">
          {(combinedAdaptiveDashboard.kpis ?? []).map((item, index) => (
            <article key={item.key} className="bg-card border border-border rounded-2xl p-4 shadow-soft">
              <span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">{item.label}</span>
              <strong className={`block text-2xl font-black ${index % 4 === 0 ? 'text-primary' : index % 4 === 1 ? 'text-success' : index % 4 === 2 ? 'text-info' : 'text-warning'}`}>{item.value}</strong>
              <small className="text-muted-foreground text-sm">{item.meta ?? 'Combined metric'}</small>
            </article>
          ))}
        </section>

        <section className="grid gap-4">
          {activeCombinedWidgets.length > 0 ? (
            activeCombinedWidgets.map((widget) => <AdaptiveWidgetCard key={widget.key} widget={widget} />)
          ) : (
            <article className="rounded-2xl border border-border bg-card p-6 shadow-soft">
              <h2 className="text-base font-bold text-foreground">No Widgets For This Tab Yet</h2>
              <p className="mt-2 text-sm text-muted-foreground">The combined analysis bundle is ready, but no widgets were generated for this tab.</p>
            </article>
          )}
        </section>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-4 animate-fade-in">
      {/* Header */}
      <header
        className="rounded-2xl p-5 text-primary-foreground shadow-elevated relative overflow-hidden"
        style={{ background: 'linear-gradient(135deg, hsl(262, 38%, 18%) 0%, hsl(268, 44%, 16%) 42%, hsl(274, 52%, 20%) 100%)' }}
      >
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,hsla(270,70%,65%,0.22),transparent_28%),radial-gradient(circle_at_bottom_left,hsla(262,83%,58%,0.18),transparent_26%)] pointer-events-none" />
        <div className="relative flex items-start justify-between gap-5 flex-wrap">
          <div>
            <span className="inline-flex items-center px-3 py-1 rounded-full border border-primary-foreground/12 bg-primary-foreground/10 text-primary-foreground text-xs font-bold mb-3">
              {dashboardFamily === 'status_pipeline_dashboard'
                ? 'Operations Workflow'
                : dashboardFamily === 'product_catalog_dashboard'
                  ? 'Product Master'
                  : dashboardFamily === 'generic_review_dashboard'
                    ? 'Adaptive Workbook'
                    : '2026 Tax Season'}
            </span>
            <h1 className="text-2xl lg:text-3xl font-black leading-tight tracking-tight">{dashboardTitle}</h1>
            <p className="mt-1.5 text-primary-foreground/70 text-sm">
              {dashboardFamily === 'status_pipeline_dashboard'
                ? `${dashboardSubtitle}${activeDashboardSnapshotId ? ` · Snapshot ${activeDashboardSnapshotId}` : ''}`
                : dashboardFamily === 'product_catalog_dashboard'
                ? `${dashboardSubtitle}${activeDashboardSnapshotId ? ` · Snapshot ${activeDashboardSnapshotId}` : ''}`
                : dashboardFamily === 'generic_review_dashboard'
                ? `${dashboardSubtitle}${activeDashboardSnapshotId ? ` · Snapshot ${activeDashboardSnapshotId}` : ''}`
                : kpis ? `As of ${formatDate(kpis.snapshot.as_of_date)} · ${kpis.active_clients} CPA clients · ${fmt(kpis.total_contracted)} total contracted` : dashboardSubtitle}
            </p>
          </div>
          <div className="flex flex-col items-end gap-3">
            <div className="flex items-center gap-2 flex-wrap justify-end">
              {(layoutPreferences.hidden_cards.length > 0 || Object.keys(layoutPreferences.card_orders).length > 0) && (
                <button
                  onClick={resetDashboardLayout}
                  className="inline-flex items-center gap-2 rounded-xl border border-primary-foreground/15 bg-primary-foreground/10 px-3.5 py-2 text-sm font-semibold text-primary-foreground transition hover:bg-primary-foreground/15"
                  type="button"
                >
                  <RotateCcw className="w-4 h-4" />
                  Reset Layout
                </button>
              )}
              <button
                onClick={() => {
                  if (!openProposalModalFromCurrentState()) {
                    void openProposalReview()
                  }
                }}
                className="inline-flex items-center gap-2 rounded-xl border border-primary-foreground/15 bg-primary-foreground/10 px-3.5 py-2 text-sm font-semibold text-primary-foreground transition hover:bg-primary-foreground/15"
                disabled={proposalRefreshLoading || proposalApproveLoading || snapshotContextMismatch}
                type="button"
              >
                {blueprint ? 'Review Blueprint' : loading || proposalRefreshLoading ? 'Generating Proposal…' : 'Review Proposal'}
              </button>
              <button
                onClick={() => {
                  if (!openProposalModalFromCurrentState()) {
                    void openProposalReview(proposalGuidance)
                  } else {
                    setProposalSectionState((current) => ({ ...current, guidance: true }))
                  }
                }}
                className="inline-flex items-center gap-2 rounded-xl border border-primary-foreground/15 bg-primary-foreground/10 px-3.5 py-2 text-sm font-semibold text-primary-foreground transition hover:bg-primary-foreground/15"
                disabled={proposalRefreshLoading || proposalApproveLoading || snapshotContextMismatch}
                type="button"
              >
                Refine Widgets
              </button>
              <button
                onClick={async () => {
                  if (!activeDashboardSnapshotId) return
                  setReportError(null)
                  try {
                    setDownloadingOverall(true)
                    await api.downloadOverallReport(activeDashboardSnapshotId)
                  } catch {
                    setReportError('Executive report download failed. Ensure the backend is running and the snapshot still exists.')
                  } finally {
                    setDownloadingOverall(false)
                  }
                }}
                className="inline-flex items-center gap-2 rounded-xl border border-primary-foreground/15 bg-primary-foreground/10 px-3.5 py-2 text-sm font-semibold text-primary-foreground transition hover:bg-primary-foreground/15"
                type="button"
              >
                <FileDown className="w-4 h-4" />
                {downloadingOverall ? 'Preparing PDF…' : 'Executive PDF'}
              </button>
              <button
                onClick={async () => {
                  if (!activeDashboardSnapshotId) return
                  setReportError(null)
                  try {
                    setDownloadingAnalytics(true)
                    await api.downloadAnalyticsReport(activeDashboardSnapshotId)
                  } catch {
                    setReportError('Analytics report download failed. Ensure the backend is running and the snapshot still exists.')
                  } finally {
                    setDownloadingAnalytics(false)
                  }
                }}
                className="inline-flex items-center gap-2 rounded-xl border border-primary-foreground/15 bg-primary-foreground/10 px-3.5 py-2 text-sm font-semibold text-primary-foreground transition hover:bg-primary-foreground/15"
                type="button"
              >
                <FileDown className="w-4 h-4" />
                {downloadingAnalytics ? 'Preparing PDF…' : 'Analytics PDF'}
              </button>
              <div className="inline-flex gap-1.5 p-1 rounded-xl bg-primary-foreground/10 backdrop-blur-sm border border-primary-foreground/10">
              {tabs.map((item) => (
                <button key={item.key} onClick={() => setTab(item.key)}
                  className={`px-3.5 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${tab === item.key ? 'text-foreground shadow-card' : 'text-primary-foreground/75 hover:text-primary-foreground hover:bg-primary-foreground/5'}`}
                  style={tab === item.key ? { background: 'linear-gradient(180deg, hsla(0, 0%, 100%, 0.98), hsla(262, 60%, 96%, 0.92))' } : undefined}>
                  {item.label}
                </button>
              ))}
            </div>
            </div>
            <label className="flex items-center gap-2.5 text-primary-foreground/70 text-sm">
              <span>Snapshot</span>
              <select value={activeDashboardSnapshotId ?? ''} onChange={(e) => goToSnapshot(Number(e.target.value))}
                className="min-w-[260px] px-3 py-2 rounded-lg bg-primary-foreground/10 border border-primary-foreground/15 text-primary-foreground text-sm backdrop-blur-sm">
                {snapshots.map((s) => <option key={s.id} value={s.id} className="text-foreground bg-card">{s.as_of_date} · {s.source_filename}</option>)}
              </select>
            </label>
          </div>
        </div>
      </header>

      {(error || reportError || proposalError || snapshotContextMismatch) && (
        <div className="bg-destructive/10 border border-destructive/30 text-destructive px-4 py-3 rounded-xl text-sm font-semibold">
          {snapshotContextMismatch
            ? `Snapshot mismatch detected. URL snapshot is ${snapshotParamId}, but the loaded dashboard is ${activeDashboardSnapshotId}. Wait for the correct file to load or reopen the intended snapshot.`
            : error ?? reportError ?? proposalError}
        </div>
      )}
      {dashboardStatusNote ? (
        <div
          className={`px-4 py-3 rounded-xl text-sm font-semibold ${
            dashboardStatusTone === 'warning'
              ? 'bg-warning/10 border border-warning/30 text-warning'
              : 'bg-success/10 border border-success/30 text-success'
          }`}
        >
          {dashboardStatusNote}
        </div>
      ) : null}

      {/* KPI Grid */}
      {dashboardFamily === 'variance_dashboard' ? (
        <>
      {(() => {
        const zoneKey = 'variance.kpi.primary'
        const items: DashboardCardItem[] = [
          {
            key: 'variance.kpi.total_contracted',
            label: 'Total Contracted',
            content: (
              <article className="bg-card border border-border rounded-2xl p-4 shadow-soft hover:shadow-card transition-shadow duration-300 animate-fade-in">
                <div className="flex items-start justify-between">
                  <span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Total Contracted</span>
                  <span className="text-primary opacity-60"><BarChart3 className="w-5 h-5" /></span>
                </div>
                <strong className="block text-2xl font-black text-primary mb-1">{kpis ? fmt(kpis.total_contracted) : '—'}</strong>
                <small className="text-muted-foreground text-sm">All clients combined</small>
              </article>
            ),
          },
          {
            key: 'variance.kpi.received_to_date',
            label: 'Received to Date',
            content: (
              <article className="bg-card border border-border rounded-2xl p-4 shadow-soft hover:shadow-card transition-shadow duration-300 animate-fade-in">
                <div className="flex items-start justify-between">
                  <span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Received to Date</span>
                  <span className="text-success opacity-60"><TrendingUp className="w-5 h-5" /></span>
                </div>
                <strong className="block text-2xl font-black text-success mb-1">{kpis ? fmt(kpis.total_received) : '—'}</strong>
                <small className="text-muted-foreground text-sm">{kpis ? `${pct(kpis.overall_receipt_rate)} receipt rate` : ''}</small>
              </article>
            ),
          },
          {
            key: 'variance.kpi.outstanding',
            label: 'Still Outstanding',
            content: (
              <article className="bg-card border border-border rounded-2xl p-4 shadow-soft hover:shadow-card transition-shadow duration-300 animate-fade-in">
                <div className="flex items-start justify-between">
                  <span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Still Outstanding</span>
                  <span className="text-warning opacity-60"><TrendingDown className="w-5 h-5" /></span>
                </div>
                <strong className="block text-2xl font-black text-warning mb-1">{kpis ? fmt(kpis.total_pending) : '—'}</strong>
                <small className="text-muted-foreground text-sm">Returns yet to arrive</small>
              </article>
            ),
          },
        ]
        const ordered = orderDashboardCards(zoneKey, items, layoutPreferences)
        return (
          <section className="grid grid-cols-3 max-md:grid-cols-1 gap-3">
            {ordered.map((item, index) => (
              <DashboardManagedCard
                key={item.key}
                item={item}
                zoneKey={zoneKey}
                index={index}
                total={ordered.length}
                onHide={hideDashboardCard}
                onMove={(nextZone, cardKey, direction) => moveDashboardCard(nextZone, cardKey, direction, items)}
              />
            ))}
          </section>
        )
      })()}

      {/* Additional KPIs */}
      {kpis && (
        (() => {
          const zoneKey = 'variance.kpi.secondary'
          const items: DashboardCardItem[] = [
            {
              key: 'variance.kpi.individual_returns',
              label: 'Individual Returns',
              content: (
                <article className="bg-card border border-border rounded-xl p-3.5 shadow-soft">
                  <span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Individual Returns</span>
                  <strong className="block text-xl font-black text-info">{fmt(kpis.total_received_ind)}</strong>
                  <small className="text-muted-foreground text-xs">of {fmt(kpis.total_contracted_ind)} contracted</small>
                </article>
              ),
            },
            {
              key: 'variance.kpi.business_returns',
              label: 'Business Returns',
              content: (
                <article className="bg-card border border-border rounded-xl p-3.5 shadow-soft">
                  <span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Business Returns</span>
                  <strong className="block text-xl font-black text-primary">{fmt(kpis.total_received_bus)}</strong>
                  <small className="text-muted-foreground text-xs">of {fmt(kpis.total_contracted_bus)} contracted</small>
                </article>
              ),
            },
            {
              key: 'variance.kpi.not_started',
              label: 'Not Yet Started',
              content: (
                <article className="bg-card border border-border rounded-xl p-3.5 shadow-soft">
                  <span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Not Yet Started</span>
                  <strong className="block text-xl font-black text-destructive">{String(kpis.zero_received_clients)}</strong>
                  <small className="text-muted-foreground text-xs">Clients at 0% receipt</small>
                </article>
              ),
            },
            {
              key: 'variance.kpi.over_contracted',
              label: 'Over Contracted',
              content: (
                <article className="bg-card border border-border rounded-xl p-3.5 shadow-soft">
                  <span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Over Contracted</span>
                  <strong className="block text-xl font-black text-warning">{String(kpis.over_delivered_clients)}</strong>
                  <small className="text-muted-foreground text-xs">Received &gt; contracted</small>
                </article>
              ),
            },
          ]
          const ordered = orderDashboardCards(zoneKey, items, layoutPreferences)
          return (
            <section className="grid grid-cols-4 max-lg:grid-cols-2 max-md:grid-cols-1 gap-3">
              {ordered.map((item, index) => (
                <DashboardManagedCard
                  key={item.key}
                  item={item}
                  zoneKey={zoneKey}
                  index={index}
                  total={ordered.length}
                  onHide={hideDashboardCard}
                  onMove={(nextZone, cardKey, direction) => moveDashboardCard(nextZone, cardKey, direction, items)}
                />
              ))}
            </section>
          )
        })()
      )}
        </>
      ) : dashboardFamily === 'status_pipeline_dashboard' ? (
        (() => {
          const zoneKey = 'status.kpi'
          const items: DashboardCardItem[] = [
            { key: 'status.kpi.total_returns', label: 'Total Returns', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Total Returns</span><strong className="block text-2xl font-black text-primary">{fmt(statusRuntime?.total_returns ?? 0)}</strong><small className="text-muted-foreground text-sm">Rows in the active workbook</small></article> },
            { key: 'status.kpi.completed', label: 'Completed', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Completed</span><strong className="block text-2xl font-black text-success">{fmt(statusRuntime?.completed_returns ?? 0)}</strong><small className="text-muted-foreground text-sm">{statusRuntime?.total_returns ? ((statusRuntime.completed_returns ?? 0) / statusRuntime.total_returns * 100).toFixed(0) : 0}% closure</small></article> },
            { key: 'status.kpi.open_queue', label: 'Open Queue', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Open Queue</span><strong className="block text-2xl font-black text-warning">{fmt(statusRuntime?.open_returns ?? 0)}</strong><small className="text-muted-foreground text-sm">Still in workflow</small></article> },
            { key: 'status.kpi.awaiting_answers', label: 'Awaiting Answers', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Awaiting Answers</span><strong className="block text-2xl font-black text-destructive">{fmt(statusRuntime?.awaiting_answers ?? 0)}</strong><small className="text-muted-foreground text-sm">Need client follow-up</small></article> },
            { key: 'status.kpi.under_review', label: 'Under Review', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Under Review</span><strong className="block text-2xl font-black text-info">{fmt(statusRuntime?.under_review ?? 0)}</strong><small className="text-muted-foreground text-sm">Active review stage</small></article> },
            { key: 'status.kpi.in_process', label: 'In Process', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">In Process</span><strong className="block text-2xl font-black text-primary">{fmt(statusRuntime?.in_process ?? 0)}</strong><small className="text-muted-foreground text-sm">Currently being worked</small></article> },
          ]
          const ordered = orderDashboardCards(zoneKey, items, layoutPreferences)
          return <section className="grid grid-cols-3 max-md:grid-cols-1 gap-3">{ordered.map((item, index) => <DashboardManagedCard key={item.key} item={item} zoneKey={zoneKey} index={index} total={ordered.length} onHide={hideDashboardCard} onMove={(nextZone, cardKey, direction) => moveDashboardCard(nextZone, cardKey, direction, items)} />)}</section>
        })()
      ) : (
        (() => {
          const zoneKey = dashboardFamily === 'generic_review_dashboard' ? 'generic.kpi' : 'product.kpi'
          const items: DashboardCardItem[] = dashboardFamily === 'generic_review_dashboard'
            ? (
              adaptiveGenericDashboard?.kpis?.length
                ? adaptiveGenericDashboard.kpis.map((item, index) => ({
                    key: `generic.kpi.${item.key}`,
                    label: item.label,
                    content: (
                      <article className="bg-card border border-border rounded-2xl p-4 shadow-soft">
                        <span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">{item.label}</span>
                        <strong className={`block text-2xl font-black ${index % 4 === 0 ? 'text-primary' : index % 4 === 1 ? 'text-success' : index % 4 === 2 ? 'text-info' : 'text-warning'}`}>{item.value}</strong>
                        <small className="text-muted-foreground text-sm">{item.meta ?? 'Adaptive metric derived from the approved dashboard intent.'}</small>
                      </article>
                    ),
                  }))
                : [
                    { key: 'generic.kpi.data_sheets', label: 'Data Sheets', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Data Sheets</span><strong className="block text-2xl font-black text-primary">{String(genericRuntime?.tabular_sheet_count ?? genericSheetCount)}</strong><small className="text-muted-foreground text-sm">Sheets with numeric distributions ready for analysis</small></article> },
                    { key: 'generic.kpi.rows_analyzed', label: 'Rows Analyzed', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Rows Analyzed</span><strong className="block text-2xl font-black text-success">{fmt(genericRuntime?.total_rows ?? genericSectionCount)}</strong><small className="text-muted-foreground text-sm">Visible source rows across the preserved workbook</small></article> },
                    { key: 'generic.kpi.numeric_measures', label: 'Numeric Measures', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Numeric Measures</span><strong className="block text-2xl font-black text-info">{String(genericRuntime?.numeric_measure_count ?? genericFieldCount)}</strong><small className="text-muted-foreground text-sm">Numeric series surfaced for generic dashboard runtime</small></article> },
                  ]
            )
            : [
              { key: 'product.kpi.products', label: 'Products', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Products</span><strong className="block text-2xl font-black text-primary">{fmt(productRuntime?.total_products ?? 0)}</strong><small className="text-muted-foreground text-sm">Rows in the active catalog</small></article> },
              { key: 'product.kpi.types', label: 'Product Types', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Product Types</span><strong className="block text-2xl font-black text-success">{fmt(productRuntime?.product_type_count ?? 0)}</strong><small className="text-muted-foreground text-sm">Distinct product-type labels</small></article> },
              { key: 'product.kpi.uom', label: 'Units of Measure', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Units of Measure</span><strong className="block text-2xl font-black text-info">{fmt(productRuntime?.uom_count ?? 0)}</strong><small className="text-muted-foreground text-sm">Distinct base UoM values</small></article> },
              { key: 'product.kpi.categories', label: 'Categories', content: <article className="bg-card border border-border rounded-2xl p-4 shadow-soft"><span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">Categories</span><strong className="block text-2xl font-black text-warning">{fmt(productRuntime?.category_count ?? 0)}</strong><small className="text-muted-foreground text-sm">Distinct product-category codes</small></article> },
            ]
          const ordered = orderDashboardCards(zoneKey, items, layoutPreferences)
          return <section className="grid grid-cols-4 max-lg:grid-cols-2 max-md:grid-cols-1 gap-3">{ordered.map((item, index) => <DashboardManagedCard key={item.key} item={item} zoneKey={zoneKey} index={index} total={ordered.length} onHide={hideDashboardCard} onMove={(nextZone, cardKey, direction) => moveDashboardCard(nextZone, cardKey, direction, items)} />)}</section>
        })()
      )}

      {/* Tab Content */}
      {dashboardFamily === 'status_pipeline_dashboard' && tab === 'overview' && (
        <div className="grid grid-cols-[1.2fr_0.8fr] max-lg:grid-cols-1 gap-3">
          <div className="flex flex-col gap-3">
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
              <h2 className="text-base font-bold text-foreground mb-1">Pipeline Health</h2>
              <p className="text-muted-foreground text-sm mb-4">Status distribution across the uploaded return workflow.</p>
              <div className="grid grid-cols-2 gap-3 max-sm:grid-cols-1">
                {(statusRuntime?.status_counts ?? []).map((item) => (
                  <div key={item.label} className="rounded-xl border border-border bg-secondary/30 p-3">
                    <div className="text-xs font-bold tracking-widest uppercase text-muted-foreground">{item.label}</div>
                    <div className="mt-2 text-2xl font-black text-foreground">{fmt(item.count)}</div>
                  </div>
                ))}
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3 max-md:grid-cols-1">
              <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
                <h2 className="text-base font-bold text-foreground mb-1">Status Distribution</h2>
                <p className="text-muted-foreground text-sm mb-3">Share of returns by current stage.</p>
                <div className="flex flex-col gap-3">
                  {(statusRuntime?.status_counts ?? []).map((item) => {
                    const total = statusRuntime?.total_returns ?? 1
                    const width = total > 0 ? (item.count / total) * 100 : 0
                    return (
                      <div key={item.label}>
                        <div className="flex items-center justify-between text-sm mb-1">
                          <span className="text-foreground font-medium">{item.label}</span>
                          <span className="text-muted-foreground">{fmt(item.count)}</span>
                        </div>
                        <div className="h-2 rounded-full bg-secondary overflow-hidden">
                          <div className="h-full rounded-full bg-primary/60" style={{ width: `${width}%` }} />
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>

              <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
                <h2 className="text-base font-bold text-foreground mb-1">Return Type Mix</h2>
                <p className="text-muted-foreground text-sm mb-3">How the queue is split by return type.</p>
                <div className="flex flex-col gap-3">
                  {(statusRuntime?.return_type_counts ?? []).map((item) => (
                    <div key={item.label} className="flex items-center justify-between rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm">
                      <span className="text-foreground font-medium">{item.label}</span>
                      <span className="text-muted-foreground font-semibold">{fmt(item.count)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>

          <div className="flex flex-col gap-3">
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
              <h2 className="text-base font-bold text-foreground mb-1">Stale Items</h2>
              <p className="text-muted-foreground text-sm mb-3">Open returns that have been sitting in the queue.</p>
              {(statusRuntime?.stale_items?.length ?? 0) === 0 ? (
                <p className="text-muted-foreground text-sm text-center py-6">No stale items detected from the available dates.</p>
              ) : (
                <div className="flex flex-col gap-2">
                  {statusRuntime?.stale_items?.slice(0, 8).map((row) => (
                    <div key={`${row.tax_payer_name}-${row.return_status}-${row.age_days}`} className="rounded-xl border border-border bg-secondary/20 px-3 py-2">
                      <div className="text-sm font-semibold text-foreground">{row.tax_payer_name}</div>
                      <div className="mt-1 text-xs text-muted-foreground">{row.return_status} · {row.age_days ?? '—'} days open</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {dashboardFamily === 'status_pipeline_dashboard' && tab === 'pipeline' && (
        <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
          <h2 className="text-base font-bold text-foreground mb-1">Pipeline Board</h2>
          <p className="text-muted-foreground text-sm mb-4">Operational stage cards for the current uploaded workbook.</p>
          <div className="grid grid-cols-3 gap-3 max-lg:grid-cols-2 max-md:grid-cols-1">
            {(statusRuntime?.status_counts ?? []).map((item) => (
              <article key={item.label} className="rounded-2xl border border-border bg-secondary/20 p-4">
                <div className="text-xs font-bold tracking-widest uppercase text-muted-foreground">{item.label}</div>
                <div className="mt-2 text-3xl font-black text-foreground">{fmt(item.count)}</div>
                <div className="mt-3 h-2 rounded-full bg-white/70 overflow-hidden">
                  <div
                    className="h-full rounded-full bg-primary/60"
                    style={{ width: `${((item.count || 0) / Math.max(...(statusRuntime?.status_counts ?? [{ count: 1 }]).map((row) => row.count), 1)) * 100}%` }}
                  />
                </div>
              </article>
            ))}
          </div>
        </div>
      )}

      {dashboardFamily === 'status_pipeline_dashboard' && tab === 'queue' && (
        <div className="grid grid-cols-[1fr_320px] max-lg:grid-cols-1 gap-3">
          <div className="bg-card border border-border rounded-2xl overflow-hidden shadow-soft">
            <div className="px-4 py-4 border-b border-border">
              <h2 className="text-base font-bold text-foreground">Open Return Queue</h2>
              <p className="text-muted-foreground text-sm mt-1">Returns that are still active in the workflow.</p>
            </div>
            <div className="overflow-auto">
              <table className="w-full border-collapse">
                <thead>
                  <tr>
                    {['Tax Payer', 'Return Code', 'Status', 'Type', 'Age'].map((heading) => (
                      <th key={heading} className="px-3 py-2.5 bg-secondary text-muted-foreground text-left text-[0.66rem] font-bold tracking-widest uppercase border-b border-border">{heading}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(statusRuntime?.open_queue ?? []).map((row) => (
                    <tr key={`${row.return_code}-${row.tax_payer_name}`} className="hover:bg-accent/30 transition">
                      <td className="px-3 py-2.5 text-sm font-semibold text-foreground border-b border-border/50">{row.tax_payer_name}</td>
                      <td className="px-3 py-2.5 text-sm text-muted-foreground border-b border-border/50">{row.return_code}</td>
                      <td className="px-3 py-2.5 text-sm text-muted-foreground border-b border-border/50">{row.return_status}</td>
                      <td className="px-3 py-2.5 text-sm text-muted-foreground border-b border-border/50">{row.return_type}</td>
                      <td className="px-3 py-2.5 text-sm text-muted-foreground border-b border-border/50">{row.age_days ?? '—'}d</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
            <h2 className="text-base font-bold text-foreground mb-3">Status Mix</h2>
            <div className="flex flex-col gap-2">
              {(statusRuntime?.status_counts ?? []).map((item) => (
                <div key={item.label} className="flex items-center justify-between rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm">
                  <span className="text-foreground">{item.label}</span>
                  <span className="font-semibold text-muted-foreground">{fmt(item.count)}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {dashboardFamily === 'status_pipeline_dashboard' && tab === 'notes' && (
        <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
          <h2 className="text-base font-bold text-foreground mb-1">Operational Notes</h2>
          <p className="text-muted-foreground text-sm mb-4">Rows carrying notes or follow-up context from the workbook.</p>
          {(statusRuntime?.note_rows?.length ?? 0) === 0 ? (
            <p className="text-muted-foreground text-sm text-center py-8">No note-driven follow-up items in this snapshot.</p>
          ) : (
            <div className="grid gap-3">
              {statusRuntime?.note_rows?.map((row) => (
                <article key={`${row.return_code}-${row.tax_payer_name}`} className="rounded-2xl border border-border bg-secondary/20 p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <div className="text-sm font-bold text-foreground">{row.tax_payer_name}</div>
                      <div className="text-xs text-muted-foreground mt-1">{row.return_code}</div>
                    </div>
                  </div>
                  {row.cpa_notes && row.cpa_notes !== '_x000D_' && <p className="mt-3 text-sm text-foreground"><span className="font-semibold">CPA:</span> {row.cpa_notes}</p>}
                  {row.gkm_notes && row.gkm_notes !== '_x000D_' && <p className="mt-2 text-sm text-foreground"><span className="font-semibold">GKM:</span> {row.gkm_notes}</p>}
                </article>
              ))}
            </div>
          )}
        </div>
      )}

      {dashboardFamily === 'product_catalog_dashboard' && tab === 'overview' && (
        <div className="grid grid-cols-[1fr_340px] max-lg:grid-cols-1 gap-3">
          <div className="flex flex-col gap-3">
            <div className="grid grid-cols-2 gap-3 max-md:grid-cols-1">
              <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
                <h2 className="text-base font-bold text-foreground mb-1">Product Type Mix</h2>
                <p className="text-muted-foreground text-sm mb-3">Distribution of products by type.</p>
                <div className="flex flex-col gap-3">
                  {(productRuntime?.product_type_counts ?? []).map((item) => (
                    <div key={item.label} className="flex items-center justify-between rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm">
                      <span className="text-foreground">{item.label}</span>
                      <span className="font-semibold text-muted-foreground">{fmt(item.count)}</span>
                    </div>
                  ))}
                </div>
              </div>
              <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
                <h2 className="text-base font-bold text-foreground mb-1">Base UoM Mix</h2>
                <p className="text-muted-foreground text-sm mb-3">Units of measure represented in the catalog.</p>
                <div className="flex flex-col gap-3">
                  {(productRuntime?.uom_counts ?? []).map((item) => (
                    <div key={item.label} className="flex items-center justify-between rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm">
                      <span className="text-foreground">{item.label}</span>
                      <span className="font-semibold text-muted-foreground">{fmt(item.count)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
          <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
            <h2 className="text-base font-bold text-foreground mb-1">Top Categories</h2>
            <p className="text-muted-foreground text-sm mb-3">Largest category buckets by product count.</p>
            <div className="flex flex-col gap-2">
              {(productRuntime?.category_counts ?? []).slice(0, 10).map((item) => (
                <div key={item.label} className="flex items-center justify-between rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm">
                  <span className="text-foreground">{item.label}</span>
                  <span className="font-semibold text-muted-foreground">{fmt(item.count)}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {dashboardFamily === 'product_catalog_dashboard' && tab === 'catalog' && (
        <div className="bg-card border border-border rounded-2xl overflow-hidden shadow-soft">
          <div className="px-4 py-4 border-b border-border">
            <h2 className="text-base font-bold text-foreground">Product Catalog</h2>
            <p className="text-muted-foreground text-sm mt-1">Source rows from the uploaded product master workbook.</p>
          </div>
          <div className="overflow-auto">
            <table className="w-full border-collapse">
              <thead>
                <tr>
                  {['Product ID', 'Description', 'Type', 'Base UoM', 'Category', 'HSN'].map((heading) => (
                    <th key={heading} className="px-3 py-2.5 bg-secondary text-muted-foreground text-left text-[0.66rem] font-bold tracking-widest uppercase border-b border-border">{heading}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(productRuntime?.catalog_rows ?? []).map((row) => (
                  <tr key={`${row.product_id}-${row.description}`} className="hover:bg-accent/30 transition">
                    <td className="px-3 py-2.5 text-sm font-semibold text-foreground border-b border-border/50">{row.product_id}</td>
                    <td className="px-3 py-2.5 text-sm text-muted-foreground border-b border-border/50">{row.description}</td>
                    <td className="px-3 py-2.5 text-sm text-muted-foreground border-b border-border/50">{row.product_type}</td>
                    <td className="px-3 py-2.5 text-sm text-muted-foreground border-b border-border/50">{row.base_uom}</td>
                    <td className="px-3 py-2.5 text-sm text-muted-foreground border-b border-border/50">{row.category}</td>
                    <td className="px-3 py-2.5 text-sm text-muted-foreground border-b border-border/50">{row.hsn_code}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {dashboardFamily === 'product_catalog_dashboard' && tab === 'categories' && (
        <div className="grid grid-cols-[1fr_320px] max-lg:grid-cols-1 gap-3">
          <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
            <h2 className="text-base font-bold text-foreground mb-1">Category Distribution</h2>
            <p className="text-muted-foreground text-sm mb-3">Product count by category code.</p>
            <div className="flex flex-col gap-3">
              {(productRuntime?.category_counts ?? []).map((item) => (
                <div key={item.label}>
                  <div className="flex items-center justify-between text-sm mb-1">
                    <span className="text-foreground">{item.label}</span>
                    <span className="text-muted-foreground">{fmt(item.count)}</span>
                  </div>
                  <div className="h-2 rounded-full bg-secondary overflow-hidden">
                    <div className="h-full rounded-full bg-primary/60" style={{ width: `${((item.count || 0) / Math.max(...(productRuntime?.category_counts ?? [{ count: 1 }]).map((row) => row.count), 1)) * 100}%` }} />
                  </div>
                </div>
              ))}
            </div>
          </div>
          <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
            <h2 className="text-base font-bold text-foreground mb-3">Catalog Signals</h2>
            <div className="flex flex-col gap-2 text-sm">
              <div className="flex items-center justify-between rounded-xl border border-border bg-secondary/20 px-3 py-2">
                <span className="text-foreground">Distinct product types</span>
                <span className="font-semibold text-muted-foreground">{fmt(productRuntime?.product_type_count ?? 0)}</span>
              </div>
              <div className="flex items-center justify-between rounded-xl border border-border bg-secondary/20 px-3 py-2">
                <span className="text-foreground">Distinct units</span>
                <span className="font-semibold text-muted-foreground">{fmt(productRuntime?.uom_count ?? 0)}</span>
              </div>
              <div className="flex items-center justify-between rounded-xl border border-border bg-secondary/20 px-3 py-2">
                <span className="text-foreground">Distinct categories</span>
                <span className="font-semibold text-muted-foreground">{fmt(productRuntime?.category_count ?? 0)}</span>
              </div>
            </div>
          </div>
        </div>
      )}

      {dashboardFamily === 'product_catalog_dashboard' && tab === 'quality' && (
        <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
          <h2 className="text-base font-bold text-foreground mb-1">Data Quality</h2>
          <p className="text-muted-foreground text-sm mb-4">Rows missing key catalog attributes.</p>
          {(productRuntime?.quality_gaps?.length ?? 0) === 0 ? (
            <p className="text-muted-foreground text-sm text-center py-8">No major missing-field issues detected in the sampled catalog attributes.</p>
          ) : (
            <div className="grid gap-3">
              {(productRuntime?.quality_gaps ?? []).map((row) => (
                <article key={`${row.product_id}-${row.description}`} className="rounded-2xl border border-border bg-secondary/20 p-4">
                  <div className="text-sm font-bold text-foreground">{row.product_id}</div>
                  <div className="text-sm text-muted-foreground mt-1">{row.description}</div>
                  <div className="mt-2 text-xs text-muted-foreground">Missing: {row.missing_fields.join(', ')}</div>
                </article>
              ))}
            </div>
          )}
        </div>
      )}

      {dashboardFamily === 'variance_dashboard' && tab === 'overview' && (
        <div className="grid grid-cols-[1fr_340px] max-lg:grid-cols-1 gap-3">
          <div className="flex flex-col gap-3">
            {/* Overall Progress */}
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
              <h2 className="text-base font-bold text-foreground mb-1">Overall Progress</h2>
              <p className="text-muted-foreground text-sm mb-3">Receipt rate across all clients</p>
              <div className="flex items-center gap-3 mb-3">
                <div className="flex-1 h-4 rounded-full bg-secondary overflow-hidden">
                  <div className="h-full rounded-full transition-all duration-500" style={{ width: `${(kpis?.overall_receipt_rate ?? 0) * 100}%`, background: 'linear-gradient(90deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }} />
                </div>
                <span className="text-sm font-bold text-foreground whitespace-nowrap">{pct(kpis?.overall_receipt_rate ?? 0)}</span>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div className="p-3 rounded-xl bg-secondary/50 border border-border/50">
                  <span className="block text-xs text-muted-foreground mb-1">Individual</span>
                  <strong className="text-lg font-black text-foreground">{kpis ? fmt(kpis.total_received_ind) : '—'}</strong>
                  <small className="block text-xs text-muted-foreground">of {kpis ? fmt(kpis.total_contracted_ind) : '—'}</small>
                </div>
                <div className="p-3 rounded-xl bg-secondary/50 border border-border/50">
                  <span className="block text-xs text-muted-foreground mb-1">Business</span>
                  <strong className="text-lg font-black text-foreground">{kpis ? fmt(kpis.total_received_bus) : '—'}</strong>
                  <small className="block text-xs text-muted-foreground">of {kpis ? fmt(kpis.total_contracted_bus) : '—'}</small>
                </div>
              </div>
            </div>

            {/* Distribution */}
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
              <h2 className="text-base font-bold text-foreground mb-1">Client Distribution</h2>
              <p className="text-muted-foreground text-sm mb-4">By receipt rate band</p>
              <div className="flex flex-col gap-3">
                {distribution.map((d) => {
                  const rc = riskColor(d.tone)
                  const total = enrichedClients.length || 1
                  return (
                    <div key={d.label}>
                      <div className="flex justify-between items-center gap-3 mb-1">
                        <span className="text-sm text-foreground font-medium">{d.label}</span>
                        <div className="flex items-center gap-2">
                          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[0.66rem] font-bold" style={{ background: rc.bg, color: rc.text }}>{d.clients.length}</span>
                          <small className="text-muted-foreground text-xs">{((d.clients.length / total) * 100).toFixed(0)}%</small>
                        </div>
                      </div>
                      <div className="h-2 rounded-full bg-secondary overflow-hidden">
                        <div className="h-full rounded-full transition-all duration-500" style={{ width: `${(d.clients.length / total) * 100}%`, background: rc.bar }} />
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>

            {/* Top Clients */}
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
              <h2 className="text-base font-bold text-foreground mb-1">Top Clients by Volume</h2>
              <p className="text-muted-foreground text-sm mb-3">Highest contracted returns</p>
              <div className="flex flex-col gap-2">
                {topClients.map((c, i) => (
                  <div key={c.id} className="grid grid-cols-[24px_1fr_auto_56px_90px] gap-2 items-center text-sm">
                    <span className="text-muted-foreground/50 text-xs">{i + 1}</span>
                    <span className="font-semibold text-foreground truncate">{c.name}</span>
                    <div className="flex-1 h-1.5 rounded-full bg-secondary overflow-hidden min-w-[60px]">
                      <div className="h-full rounded-full bg-primary/60" style={{ width: `${(c.recTot / (c.conTot || 1)) * 100}%` }} />
                    </div>
                    <span className="text-xs text-muted-foreground text-right">{fmt(c.conTot)}</span>
                    <span className="text-xs font-bold text-right" style={{ color: riskColor(riskLabel(c)).text }}>{pct(c.rate)}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Sidebar */}
          <div className="flex flex-col gap-3">
            {/* Zero Received */}
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft" style={{ background: 'linear-gradient(180deg, hsl(0 84% 60% / 0.03), hsl(var(--card)))' }}>
              <div className="flex items-start gap-2 mb-3">
                <AlertTriangle className="w-5 h-5 text-destructive shrink-0 mt-0.5" />
                <div>
                  <h2 className="text-base font-bold text-foreground">Zero Received — Highest Volume at Risk</h2>
                  <p className="text-muted-foreground text-xs mt-0.5">Clients with contracted returns but none received yet.</p>
                </div>
              </div>
              {zeroReceivedClients.length === 0 ? (
                <p className="text-muted-foreground text-sm text-center py-4">None — all clients have some returns.</p>
              ) : (
                <div className="flex flex-col gap-2">
                  {zeroReceivedClients.slice(0, 8).map((c) => (
                    <div key={c.id} className="flex justify-between items-center py-1.5 border-b border-border/50 last:border-0">
                      <div>
                        <div className="font-semibold text-sm text-foreground">{c.name}</div>
                        <div className="text-xs text-muted-foreground">{fmt(c.conInd)} Ind · {fmt(c.conBus)} Bus</div>
                      </div>
                      <span className="text-xs font-bold px-2 py-0.5 rounded-full bg-destructive/10 text-destructive">{fmt(c.conTot)} due</span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Critical */}
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft" style={{ background: 'linear-gradient(180deg, hsl(38 92% 50% / 0.03), hsl(var(--card)))' }}>
              <h2 className="text-base font-bold text-foreground mb-1">Critical — Under 15% Receipt Rate</h2>
              <p className="text-muted-foreground text-xs mb-3">Low receipt rate. Follow-up recommended.</p>
              {criticalClients.length === 0 ? (
                <p className="text-muted-foreground text-sm text-center py-4">No critical clients.</p>
              ) : (
                <div className="flex flex-col gap-2">
                  {criticalClients.slice(0, 5).map((c) => (
                    <div key={c.id} className="flex justify-between items-center py-1.5 border-b border-border/50 last:border-0">
                      <div>
                        <div className="font-semibold text-sm text-foreground">{c.name}</div>
                        <div className="text-xs text-muted-foreground">{fmt(c.recTot)} received of {fmt(c.conTot)}</div>
                      </div>
                      <span className="text-xs font-bold px-2 py-0.5 rounded-full bg-warning/10 text-warning">{pct(c.rate)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Ahead */}
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft" style={{ background: 'linear-gradient(180deg, hsl(152 60% 38% / 0.03), hsl(var(--card)))' }}>
              <h2 className="text-base font-bold text-foreground mb-1">Ahead — Over 60% Receipt</h2>
              <p className="text-muted-foreground text-xs mb-3">Strong performers in the current snapshot.</p>
              {aheadClients.length === 0 ? (
                <p className="text-muted-foreground text-sm text-center py-4">None yet.</p>
              ) : (
                <div className="flex flex-col gap-2">
                  {aheadClients.slice(0, 5).map((c) => (
                    <div key={c.id} className="flex justify-between items-center py-1.5 border-b border-border/50 last:border-0">
                      <div>
                        <div className="font-semibold text-sm text-foreground">{c.name}</div>
                        <div className="text-xs text-muted-foreground">{fmt(c.recTot)} of {fmt(c.conTot)}</div>
                      </div>
                      <span className="text-xs font-bold px-2 py-0.5 rounded-full bg-success/10 text-success">{pct(c.rate)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Client Table Tab */}
      {dashboardFamily === 'variance_dashboard' && tab === 'clients' && (
        <div className="flex flex-col gap-3">
          <div className="flex items-center justify-between gap-4 flex-wrap">
            <div className="flex items-center gap-2 flex-wrap">
              {filterOptions.map((f) => (
                <button key={f} onClick={() => setFilter(f)}
                  className={`px-3 py-1.5 rounded-full text-xs font-medium border transition ${filter === f ? 'bg-primary/10 text-primary border-primary/30' : 'bg-card text-muted-foreground border-border hover:bg-accent'}`}>
                  {f}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-3">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search clients..."
                  className="w-52 pl-9 pr-3 py-2 rounded-lg border border-border bg-card text-foreground text-sm focus:outline-none focus:border-primary/40 transition" />
              </div>
              <span className="text-muted-foreground text-sm font-semibold">{sortedClients.length} clients</span>
            </div>
          </div>

          <div className="grid grid-cols-[1fr_340px] max-lg:grid-cols-1 gap-3">
            <div className="bg-card border border-border rounded-2xl overflow-hidden shadow-soft">
              <div className="overflow-auto">
                <table className="w-full border-collapse">
                  <thead>
                    <tr>
                      {[
                        { key: 'name' as SortColumn, label: 'Client' },
                        { key: 'conInd' as SortColumn, label: 'Con. Ind' },
                        { key: 'recInd' as SortColumn, label: 'Rec. Ind' },
                        { key: 'conBus' as SortColumn, label: 'Con. Bus' },
                        { key: 'recBus' as SortColumn, label: 'Rec. Bus' },
                        { key: 'conTot' as SortColumn, label: 'Contracted' },
                        { key: 'recTot' as SortColumn, label: 'Received' },
                        { key: 'pending' as SortColumn, label: 'Pending' },
                        { key: 'rate' as SortColumn, label: 'Rate' },
                      ].map((col) => (
                        <th key={col.key} onClick={() => handleSort(col.key)}
                          className="sticky top-0 bg-secondary text-muted-foreground px-3 py-2.5 text-left text-[0.66rem] font-bold tracking-widest uppercase border-b border-border whitespace-nowrap cursor-pointer hover:text-foreground transition">
                          {col.label} {sortCol === col.key ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sortedClients.map((c) => {
                      const rl = riskLabel(c); const rc = riskColor(rl)
                      return (
                        <tr key={c.id} onClick={() => setSelectedClientId(c.id)}
                          className={`cursor-pointer hover:bg-accent/50 transition ${selectedClientId === c.id ? 'bg-accent' : ''}`}>
                          <td className="px-3 py-2 text-sm font-semibold text-foreground border-b border-border/50 whitespace-nowrap">{c.name}</td>
                          <td className="px-3 py-2 text-sm text-muted-foreground border-b border-border/50">{fmt(c.conInd)}</td>
                          <td className="px-3 py-2 text-sm text-muted-foreground border-b border-border/50">{fmt(c.recInd)}</td>
                          <td className="px-3 py-2 text-sm text-muted-foreground border-b border-border/50">{fmt(c.conBus)}</td>
                          <td className="px-3 py-2 text-sm text-muted-foreground border-b border-border/50">{fmt(c.recBus)}</td>
                          <td className="px-3 py-2 text-sm font-bold text-foreground border-b border-border/50">{fmt(c.conTot)}</td>
                          <td className="px-3 py-2 text-sm font-bold text-foreground border-b border-border/50">{fmt(c.recTot)}</td>
                          <td className="px-3 py-2 text-sm text-muted-foreground border-b border-border/50">{fmt(c.pending)}</td>
                          <td className="px-3 py-2 border-b border-border/50">
                            <span className="inline-flex px-2 py-0.5 rounded-full text-[0.66rem] font-bold" style={{ background: rc.bg, color: rc.text }}>{pct(c.rate)}</span>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Selected Client Detail */}
            {selectedClient && (
              <div className="bg-card border border-border rounded-2xl p-4 shadow-soft animate-fade-in">
                <div className="flex justify-between items-start gap-3 mb-3">
                  <div>
                    <h3 className="text-lg font-bold text-foreground">{selectedClient.name}</h3>
                    <span className="inline-flex px-2 py-0.5 rounded-full text-xs font-bold bg-secondary text-muted-foreground mt-1">{selectedClient.id}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={async () => {
                        if (!activeDashboardSnapshotId) return
                        setReportError(null)
                        try {
                          setDownloadingClientId(selectedClient.id)
                          await api.downloadClientReport(selectedClient.id, activeDashboardSnapshotId)
                        } catch {
                          setReportError('Client report download failed. Ensure the backend is running and the client exists in the selected snapshot.')
                        } finally {
                          setDownloadingClientId(null)
                        }
                      }}
                      className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-card px-2.5 py-1.5 text-xs font-semibold text-muted-foreground transition hover:text-foreground hover:bg-accent"
                      type="button"
                    >
                      <FileDown className="w-3.5 h-3.5" />
                      {downloadingClientId === selectedClient.id ? 'Preparing…' : 'Client PDF'}
                    </button>
                    <button onClick={() => setSelectedClientId(null)} className="w-7 h-7 rounded-full bg-secondary text-muted-foreground grid place-items-center hover:text-foreground transition">×</button>
                  </div>
                </div>
                <span className="inline-flex px-2.5 py-0.5 rounded-full text-xs font-bold mb-3" style={{ ...(() => { const rc = riskColor(riskLabel(selectedClient)); return { background: rc.bg, color: rc.text } })() }}>{riskLabel(selectedClient)}</span>
                <div className="flex flex-col gap-3">
                  {[
                    { label: 'Contracted', value: fmt(selectedClient.conTot) },
                    { label: 'Received', value: fmt(selectedClient.recTot) },
                    { label: 'Pending', value: fmt(selectedClient.pending) },
                    { label: 'Receipt Rate', value: pct(selectedClient.rate) },
                    { label: 'Ind. Contracted', value: fmt(selectedClient.conInd) },
                    { label: 'Ind. Received', value: fmt(selectedClient.recInd) },
                    { label: 'Bus. Contracted', value: fmt(selectedClient.conBus) },
                    { label: 'Bus. Received', value: fmt(selectedClient.recBus) },
                  ].map((s) => (
                    <div key={s.label} className="flex justify-between items-center py-1.5 border-b border-border/50 last:border-0 text-sm">
                      <span className="text-muted-foreground">{s.label}</span>
                      <strong className="text-foreground">{s.value}</strong>
                    </div>
                  ))}
                </div>
                <div className="mt-4">
                  <div className="text-xs text-muted-foreground mb-1">Progress</div>
                  <div className="h-3 rounded-full bg-secondary overflow-hidden">
                    <div className="h-full rounded-full transition-all duration-500" style={{ width: `${Math.min((selectedClient.rate ?? 0) * 100, 100)}%`, background: riskColor(riskLabel(selectedClient)).bar }} />
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Staff Tab */}
      {dashboardFamily === 'variance_dashboard' && tab === 'staff' && (
        <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
          <h2 className="text-base font-bold text-foreground mb-1">Staff Workload</h2>
          <p className="text-muted-foreground text-sm mb-4">Returns received per team member</p>
          {enrichedStaff.length === 0 ? (
            <p className="text-muted-foreground text-sm text-center py-8">No staff data in this snapshot.</p>
          ) : (
            <div className="flex flex-col gap-3">
              {[...enrichedStaff].sort((a, b) => b.recTot - a.recTot).map((m) => {
                const maxRec = Math.max(...enrichedStaff.map((s) => s.recTot), 1)
                return (
                  <div key={m.staff_id} className="py-3 border-b border-border/50 last:border-0">
                    <div className="grid grid-cols-[44px_1fr_auto] gap-3 items-center">
                      <div className="grid place-items-center w-10 h-10 rounded-xl border border-border bg-secondary text-muted-foreground font-extrabold text-xs">{m.name.split(' ').map((w) => w[0]).join('').slice(0, 2)}</div>
                      <div>
                        <div className="font-bold text-foreground text-sm">{m.name}</div>
                        <div className="flex gap-2 mt-0.5 text-muted-foreground text-xs">
                          <span>{m.staff_type}</span>
                          <span>·</span>
                          <span>{fmt(m.received_ind)} Ind</span>
                          <span>·</span>
                          <span>{fmt(m.received_bus)} Bus</span>
                        </div>
                      </div>
                      <strong className="text-foreground">{fmt(m.recTot)}</strong>
                    </div>
                    <div className="mt-2 h-2 rounded-full bg-secondary overflow-hidden">
                      <div className="h-full rounded-full bg-primary/50 transition-all duration-500" style={{ width: `${(m.recTot / maxRec) * 100}%` }} />
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      )}

      {/* Risk Tab */}
      {dashboardFamily === 'variance_dashboard' && tab === 'risk' && (
        <div className="grid grid-cols-[1fr_340px] max-lg:grid-cols-1 gap-3">
          <div className="flex flex-col gap-3">
            {/* Zero Received */}
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
              <div className="flex items-center gap-2 mb-3">
                <AlertTriangle className="w-5 h-5 text-destructive" />
                <h2 className="text-base font-bold text-foreground">Zero Received ({zeroReceivedClients.length})</h2>
              </div>
              {zeroReceivedClients.map((c) => (
                <div key={c.id} className="flex justify-between items-center py-2 border-b border-border/50 last:border-0 text-sm">
                  <div>
                    <span className="font-semibold text-foreground">{c.name}</span>
                    <span className="text-muted-foreground ml-2 text-xs">{fmt(c.conInd)} Ind · {fmt(c.conBus)} Bus</span>
                  </div>
                  <span className="text-xs font-bold px-2 py-0.5 rounded-full bg-destructive/10 text-destructive">{fmt(c.conTot)} due</span>
                </div>
              ))}
            </div>

            {/* Critical */}
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
              <h2 className="text-base font-bold text-foreground mb-3">Critical ({criticalClients.length})</h2>
              {criticalClients.map((c) => (
                <div key={c.id} className="flex justify-between items-center py-2 border-b border-border/50 last:border-0 text-sm">
                  <span className="font-semibold text-foreground">{c.name}</span>
                  <span className="text-xs font-bold px-2 py-0.5 rounded-full bg-warning/10 text-warning">{pct(c.rate)}</span>
                </div>
              ))}
            </div>

            {/* Anomalies */}
            <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
              <h2 className="text-base font-bold text-foreground mb-3">Anomalies ({anomalyClients.length})</h2>
              {anomalyClients.length === 0 ? (
                <p className="text-muted-foreground text-sm text-center py-4">No anomalies detected.</p>
              ) : anomalyClients.map((c) => (
                <div key={c.id} className="flex justify-between items-center py-2 border-b border-border/50 last:border-0 text-sm">
                  <span className="font-semibold text-foreground">{c.name}</span>
                  <span className="text-xs font-bold px-2 py-0.5 rounded-full bg-accent text-accent-foreground">{c.overContracted ? 'Over-delivered' : 'Uncontracted'}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Sidebar summary */}
          <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
            <h2 className="text-base font-bold text-foreground mb-3">Risk Summary</h2>
            {distribution.map((d) => {
              const rc = riskColor(d.tone)
              return (
                <div key={d.label} className="flex justify-between items-center py-2 border-b border-border/50 last:border-0 text-sm">
                  <span className="text-muted-foreground">{d.label}</span>
                  <span className="font-bold px-2 py-0.5 rounded-full text-xs" style={{ background: rc.bg, color: rc.text }}>{d.clients.length}</span>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {dashboardFamily === 'generic_review_dashboard' && (
        <div className="grid gap-4">
          {adaptiveGenericDashboard ? (
            <div className="grid gap-3">
              <div className="rounded-2xl border border-border bg-card p-5 shadow-soft">
                <h2 className="text-base font-bold text-foreground">{tabs.find((item) => item.key === tab)?.label ?? 'Adaptive Dashboard'}</h2>
                <p className="mt-1 text-sm text-muted-foreground">{tabs.find((item) => item.key === tab)?.description ?? blueprint?.description ?? 'Adaptive dashboard generated from semantic interpretation and EDA.'}</p>
                <div className="mt-4 grid gap-3 md:grid-cols-2">
                  <div className="rounded-2xl border border-border bg-secondary/20 p-4 text-sm text-muted-foreground">
                    {semanticSummaryText(proposal?.proposal ?? blueprint?.config)}
                  </div>
                  <div className="rounded-2xl border border-border bg-secondary/20 p-4 text-sm text-muted-foreground">
                    Domain: <span className="font-semibold text-foreground">{adaptiveGenericDashboard.domain}</span><br />
                    Focus: <span className="font-semibold text-foreground">{adaptiveGenericDashboard.primary_entity}</span> by <span className="font-semibold text-foreground">{adaptiveGenericDashboard.primary_measure}</span><br />
                    Charts: <span className="font-semibold text-foreground">{(adaptiveGenericDashboard.chart_preferences ?? []).length ? (adaptiveGenericDashboard.chart_preferences ?? []).join(', ') : 'adaptive defaults'}</span>
                  </div>
                </div>
              </div>
              {activeAdaptiveWidgets.length === 0 ? (
                <div className="rounded-2xl border border-border bg-card p-5 text-sm text-muted-foreground shadow-soft">
                  No adaptive widgets were generated for this tab yet.
                </div>
              ) : (
                <div className="grid gap-3 lg:grid-cols-2">
                  {(() => {
                    const zoneKey = `generic.adaptive.${tab}`
                    const items: DashboardCardItem[] = activeAdaptiveWidgets.map((widget) => ({
                      key: `generic.adaptive.${widget.key}`,
                      label: widget.title,
                      content: <AdaptiveWidgetCard widget={widget} />,
                    }))
                    const ordered = orderDashboardCards(zoneKey, items, layoutPreferences)
                    return ordered.map((item, index) => (
                      <DashboardManagedCard
                        key={item.key}
                        item={item}
                        zoneKey={zoneKey}
                        index={index}
                        total={ordered.length}
                        onHide={hideDashboardCard}
                        onMove={(nextZone, cardKey, direction) => moveDashboardCard(nextZone, cardKey, direction, items)}
                      />
                    ))
                  })()}
                </div>
              )}
              {(adaptiveGenericDashboard.supporting_notes ?? []).length ? (
                <div className="rounded-2xl border border-border bg-card p-4 shadow-soft">
                  <h2 className="text-base font-bold text-foreground mb-3">Analytical Notes</h2>
                  <div className="grid gap-2">
                    {(adaptiveGenericDashboard.supporting_notes ?? []).map((item) => (
                      <div key={item} className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">
                        {item}
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          ) : (
            <div className="grid grid-cols-[1fr_340px] max-lg:grid-cols-1 gap-3">
              {(() => {
              const mainZone = 'generic.main'
              const sideZone = 'generic.sidebar'
              const mainItems: DashboardCardItem[] = [
                {
                  key: 'generic.main.active_sheet',
                  label: 'Active Sheet',
                  content: (
                    <div className="bg-card border border-border rounded-2xl p-5 shadow-soft">
                      <h2 className="text-base font-bold text-foreground mb-1">{tabs.find((item) => item.key === tab)?.label ?? 'Proposed View'}</h2>
                      <p className="text-muted-foreground text-sm mb-4">{tabs.find((item) => item.key === tab)?.description ?? 'Schema-driven dashboard view.'}</p>
                      {(proposal?.proposal.semantic_summary ?? blueprint?.config.semantic_summary) && (
                        <div className="rounded-2xl border border-border bg-secondary/20 p-4 mb-4 text-sm text-muted-foreground">
                          {semanticSummaryText(proposal?.proposal ?? blueprint?.config)}
                        </div>
                      )}
                      {activeGenericSheet ? (
                        <section className="rounded-2xl border border-border bg-secondary/20 p-4">
                          <div className="mb-3">
                            <div className="text-sm font-bold text-foreground">{activeGenericSheet.sheet_name}</div>
                            <div className="text-xs text-muted-foreground">
                              {activeGenericRuntime?.sheet_kind === 'distribution'
                                ? 'Active-sheet distribution runtime extracted from preserved workbook rows.'
                                : activeGenericRuntime?.sheet_kind === 'text_reference'
                                  ? 'Reference sheet text extracted for dashboard context.'
                                  : 'Detected sections and source fields for the active sheet.'}
                            </div>
                          </div>
                          {activeGenericRuntime?.sheet_kind === 'distribution' ? (
                            <div className="grid gap-4">
                              <section className="grid grid-cols-3 max-md:grid-cols-1 gap-3">
                                {[
                                  { label: 'Rows', value: fmt(activeGenericRuntime.row_count ?? 0), sub: 'Visible records in this sheet', color: 'text-primary' },
                                  { label: 'Measures', value: String(activeGenericRuntime.measure_count ?? 0), sub: 'Numeric columns used for distribution analysis', color: 'text-success' },
                                  { label: 'Sheet Total', value: fmt(Math.round(activeGenericRuntime.grand_total ?? 0)), sub: 'Combined total across visible numeric measures', color: 'text-info' },
                                ].map((card) => (
                                  <article key={card.label} className="rounded-xl border border-border bg-card p-3">
                                    <span className="block text-muted-foreground text-[0.68rem] font-bold tracking-widest uppercase mb-2">{card.label}</span>
                                    <strong className={`block text-xl font-black ${card.color}`}>{card.value}</strong>
                                    <small className="text-muted-foreground text-xs">{card.sub}</small>
                                  </article>
                                ))}
                              </section>
                              <div className="grid grid-cols-2 max-lg:grid-cols-1 gap-3">
                                <div className="rounded-xl border border-border bg-card p-3">
                                  <div className="text-sm font-semibold text-foreground">Largest Measure Totals</div>
                                  <div className="mt-3 flex flex-col gap-3">
                                    {(activeGenericRuntime.measure_totals ?? []).slice(0, 6).map((item) => {
                                      const maxValue = Math.max(...(activeGenericRuntime.measure_totals ?? [{ total: 1 }]).map((row) => row.total), 1)
                                      return (
                                        <div key={item.label}>
                                          <div className="flex items-center justify-between text-sm mb-1">
                                            <span className="text-foreground">{item.label}</span>
                                            <span className="text-muted-foreground">{fmt(Math.round(item.total))}</span>
                                          </div>
                                          <div className="h-2 rounded-full bg-secondary overflow-hidden">
                                            <div className="h-full rounded-full bg-primary/60" style={{ width: `${(item.total / maxValue) * 100}%` }} />
                                          </div>
                                        </div>
                                      )
                                    })}
                                  </div>
                                </div>
                                <div className="rounded-xl border border-border bg-card p-3">
                                  <div className="text-sm font-semibold text-foreground">Top {activeGenericRuntime.dimension_header ?? 'Segments'}</div>
                                  <div className="mt-3 flex flex-col gap-3">
                                    {(activeGenericRuntime.top_segments ?? []).slice(0, 6).map((item) => {
                                      const maxValue = Math.max(...(activeGenericRuntime.top_segments ?? [{ total: 1 }]).map((row) => row.total), 1)
                                      return (
                                        <div key={item.label}>
                                          <div className="flex items-center justify-between text-sm mb-1 gap-3">
                                            <span className="text-foreground truncate">{item.label}</span>
                                            <span className="text-muted-foreground shrink-0">{fmt(Math.round(item.total))}</span>
                                          </div>
                                          <div className="h-2 rounded-full bg-secondary overflow-hidden">
                                            <div className="h-full rounded-full bg-success/70" style={{ width: `${(item.total / maxValue) * 100}%` }} />
                                          </div>
                                        </div>
                                      )
                                    })}
                                  </div>
                                </div>
                              </div>
                            </div>
                          ) : activeGenericRuntime?.sheet_kind === 'text_reference' ? (
                            <div className="rounded-xl border border-border bg-card p-3">
                              <div className="text-sm font-semibold text-foreground">Reference Notes</div>
                              <div className="mt-3 grid gap-2">
                                {(activeGenericRuntime.text_items ?? []).length === 0 ? (
                                  <div className="text-sm text-muted-foreground">No reference text was captured for this sheet.</div>
                                ) : (
                                  (activeGenericRuntime.text_items ?? []).map((item) => (
                                    <div key={item} className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">
                                      {item}
                                    </div>
                                  ))
                                )}
                              </div>
                            </div>
                          ) : null}
                          <div className="flex flex-col gap-3">
                            {activeGenericSheet.sections.map((section) => (
                              <div key={`${activeGenericSheet.sheet_name}-${section.section_key}`} className="rounded-xl border border-border bg-card p-3">
                                <div className="text-sm font-semibold text-foreground">{section.section_label}</div>
                                <div className="mt-2 flex flex-wrap gap-2">
                                  {section.fields.map((field) => (
                                    <span key={`${section.section_key}-${field.column}-${field.header_label}`} className="inline-flex rounded-full border border-border bg-secondary px-2.5 py-1 text-xs text-muted-foreground">
                                      {field.header_label}
                                    </span>
                                  ))}
                                </div>
                              </div>
                            ))}
                          </div>
                        </section>
                      ) : (
                        <div className="rounded-2xl border border-border bg-secondary/20 p-4 text-sm text-muted-foreground">
                          No active sheet mapping was found for this generic dashboard tab.
                        </div>
                      )}
                    </div>
                  ),
                },
              ]
              const sideItems: DashboardCardItem[] = [
                {
                  key: 'generic.sidebar.comparison',
                  label: 'Cross-Sheet Comparison',
                  content: (
                    <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
                  <h2 className="text-base font-bold text-foreground mb-3">Cross-Sheet Comparison</h2>
                  {activeGenericComparison ? (
                    <div className="flex flex-col gap-3">
                      <div className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">
                        {activeGenericComparison.group_label} comparison. Leading series: {activeGenericComparison.leading_series} with {fmt(Math.round(activeGenericComparison.leading_total))}.
                      </div>
                      {activeGenericComparison.rate_basis ? (
                        <div className="rounded-xl border border-border bg-card px-3 py-2 text-sm text-muted-foreground">
                          Comparison basis: matched pools only. Matched pools {fmt(activeGenericComparison.matched_pool_count ?? 0)}, TC-only pools {fmt(activeGenericComparison.unmatched_tc_pool_count ?? 0)}, BC-only pools {fmt(activeGenericComparison.unmatched_bc_pool_count ?? 0)}.
                        </div>
                      ) : null}
                      {activeGenericComparison.series_totals.map((item) => {
                        const maxValue = Math.max(...activeGenericComparison.series_totals.map((row) => row.grand_total), 1)
                        return (
                          <div key={item.sheet_name}>
                            <div className="flex items-center justify-between text-sm mb-1">
                              <span className="text-foreground">{item.series}</span>
                              <span className="text-muted-foreground">{fmt(Math.round(item.grand_total))}</span>
                            </div>
                            <div className="h-2 rounded-full bg-secondary overflow-hidden">
                              <div className="h-full rounded-full bg-warning/70" style={{ width: `${(item.grand_total / maxValue) * 100}%` }} />
                            </div>
                          </div>
                        )
                      })}
                      {activeGenericComparison.rate_basis && (activeGenericComparison.lowest_rate_segments?.length || activeGenericComparison.highest_rate_segments?.length) ? (
                        <div className="grid grid-cols-2 max-md:grid-cols-1 gap-3">
                          <div className="rounded-xl border border-border bg-card p-3">
                            <div className="text-sm font-semibold text-foreground">Lowest {activeGenericComparison.rate_basis} Pools</div>
                            <div className="mt-3 flex flex-col gap-2">
                              {(activeGenericComparison.lowest_rate_segments ?? []).map((item) => (
                                <div key={`low-${item.label}`} className="flex items-center justify-between text-sm gap-3">
                                  <div>
                                    <div className="text-foreground">{item.label}</div>
                                    <div className="text-xs text-muted-foreground">BC {fmt(Math.round(item.bad_count))} / TC {fmt(Math.round(item.total_count))}</div>
                                  </div>
                                  <span className="text-muted-foreground">{(item.ratio * 100).toFixed(2)}%</span>
                                </div>
                              ))}
                            </div>
                          </div>
                          <div className="rounded-xl border border-border bg-card p-3">
                            <div className="text-sm font-semibold text-foreground">Highest {activeGenericComparison.rate_basis} Pools</div>
                            <div className="mt-3 flex flex-col gap-2">
                              {(activeGenericComparison.highest_rate_segments ?? []).map((item) => (
                                <div key={`high-${item.label}`} className="flex items-center justify-between text-sm gap-3">
                                  <div>
                                    <div className="text-foreground">{item.label}</div>
                                    <div className="text-xs text-muted-foreground">BC {fmt(Math.round(item.bad_count))} / TC {fmt(Math.round(item.total_count))}</div>
                                  </div>
                                  <span className="text-muted-foreground">{(item.ratio * 100).toFixed(2)}%</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        </div>
                      ) : null}
                    </div>
                  ) : (
                    <p className="text-muted-foreground text-sm">No paired comparison group was inferred for the active sheet yet.</p>
                  )}
                    </div>
                  ),
                },
                {
                  key: 'generic.sidebar.coverage',
                  label: 'Coverage',
                  content: (
                    <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
                      <h2 className="text-base font-bold text-foreground mb-3">Coverage</h2>
                      <div className="flex flex-col gap-2">
                        <div className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">
                          {fmt(genericRuntime?.tabular_sheet_count ?? genericSheetCount)} data sheets and {fmt(genericRuntime?.comparison_group_count ?? 0)} paired period groups are currently available for analysis.
                        </div>
                        <div className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">
                          {fmt(genericBusinessQuestions + genericAmbiguities)} open modeling questions remain. These should inform blueprint review, not be treated as live business insights.
                        </div>
                      </div>
                    </div>
                  ),
                },
                {
                  key: 'generic.sidebar.reference',
                  label: 'Reference Notes',
                  content: (
                    <div className="bg-card border border-border rounded-2xl p-4 shadow-soft">
                      <h2 className="text-base font-bold text-foreground mb-3">Reference Notes</h2>
                      {(genericRuntime?.text_reference_items ?? []).length === 0 ? (
                        <p className="text-muted-foreground text-sm">No reference notes were extracted from supporting sheets.</p>
                      ) : (
                        <div className="flex flex-col gap-2">
                          {(genericRuntime?.text_reference_items ?? []).slice(0, 6).map((item) => (
                            <div key={item} className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">
                              {item}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  ),
                },
              ]
              const orderedMain = orderDashboardCards(mainZone, mainItems, layoutPreferences)
              const orderedSide = orderDashboardCards(sideZone, sideItems, layoutPreferences)
              return (
                <>
                  <div className="flex flex-col gap-3">
                    {orderedMain.map((item, index) => (
                      <DashboardManagedCard key={item.key} item={item} zoneKey={mainZone} index={index} total={orderedMain.length} onHide={hideDashboardCard} onMove={(nextZone, cardKey, direction) => moveDashboardCard(nextZone, cardKey, direction, mainItems)} />
                    ))}
                  </div>
                  <div className="flex flex-col gap-3">
                    {orderedSide.map((item, index) => (
                      <DashboardManagedCard key={item.key} item={item} zoneKey={sideZone} index={index} total={orderedSide.length} onHide={hideDashboardCard} onMove={(nextZone, cardKey, direction) => moveDashboardCard(nextZone, cardKey, direction, sideItems)} />
                    ))}
                  </div>
                </>
              )
            })()}
            </div>
          )}
        </div>
      )}

      {showProposalModal && proposal && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-foreground/15 backdrop-blur-sm p-4">
          <div className="w-full max-w-5xl max-h-[88vh] overflow-hidden rounded-3xl border border-border bg-card shadow-[0_28px_80px_rgba(15,23,42,0.16)]">
            <div className="flex items-start justify-between gap-4 border-b border-border px-6 py-5">
              <div>
                <h2 className="text-lg font-black text-foreground">{proposal.title}</h2>
                <p className="mt-1 text-sm text-muted-foreground">{proposal.summary}</p>
                <div className="mt-2 flex flex-wrap gap-2 text-xs">
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Snapshot: {proposal.snapshot_id}</span>
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">{activeDashboardSnapshotLabel ?? 'Snapshot loading...'}</span>
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Schema: {proposal.schema_signature.slice(0, 12)}</span>
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Type: {proposal.workbook_type}</span>
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Confidence: {(proposal.confidence_score * 100).toFixed(0)}%</span>
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Mode: {proposal.match_mode}</span>
                </div>
              </div>
              <button
                className="grid h-9 w-9 place-items-center rounded-full bg-secondary text-muted-foreground transition hover:text-foreground disabled:opacity-40"
                disabled={proposalRefreshLoading || proposalApproveLoading}
                onClick={() => setShowProposalModal(false)}
                type="button"
              >
                ×
              </button>
            </div>

            <div className="max-h-[62vh] overflow-auto px-6 py-5">
              <div className="grid gap-4">
                {proposalStatusNote ? (
                  <div
                    className={`rounded-2xl px-4 py-3 text-sm font-semibold ${
                      proposalStatusTone === 'warning'
                        ? 'border border-warning/30 bg-warning/10 text-warning'
                        : 'border border-success/30 bg-success/10 text-success'
                    }`}
                  >
                    {proposalStatusNote}
                  </div>
                ) : null}
                {snapshotContextMismatch ? (
                  <div className="rounded-2xl border border-warning/30 bg-warning/10 px-4 py-3 text-sm font-semibold text-warning">
                    The proposal modal is blocked because the dashboard is still loaded for snapshot {activeDashboardSnapshotId}, while the URL points to snapshot {snapshotParamId}.
                  </div>
                ) : null}
                <ProposalSection title="Start Here: Add your widget ideas" sectionKey="guidance" expanded={proposalSectionState.guidance} onToggle={toggleProposalSection}>
                  <div className="rounded-2xl border border-primary/15 bg-[linear-gradient(180deg,hsla(262,100%,99%,0.94),hsla(268,80%,96%,0.9))] p-4">
                    <p className="text-sm font-semibold text-foreground">Use this box before approval.</p>
                    <p className="mt-1 text-sm text-muted-foreground">Mention the business goal, preferred cuts, KPIs, widget types, and any exact charts or drilldowns you want the generated dashboard to include.</p>
                  </div>
                  <textarea
                    className="mt-3 min-h-[132px] w-full rounded-2xl border border-primary/15 bg-card px-4 py-3 text-sm text-foreground outline-none transition focus:border-primary/40 focus:bg-card"
                    onChange={(event) => setProposalGuidance(event.target.value)}
                    placeholder="Example: Focus on top towers by dues, penalty concentration, quarter movement, and owner exposure. Prefer pie charts for rankings and a gantt-like timeline for quarter coverage."
                    value={proposalGuidance}
                  />
                  <div className="mt-3 flex flex-wrap gap-2 text-xs text-muted-foreground">
                    <span className="inline-flex rounded-full border border-border bg-card px-2.5 py-1">Name the KPI</span>
                    <span className="inline-flex rounded-full border border-border bg-card px-2.5 py-1">Specify chart type</span>
                    <span className="inline-flex rounded-full border border-border bg-card px-2.5 py-1">Ask for ranking or trend</span>
                  </div>
                </ProposalSection>

                {proposal.refinement_result && proposal.refinement_result.status !== 'not_requested' ? (
                  <ProposalSection title="Refinement result" sectionKey="refinement" expanded={proposalSectionState.refinement} onToggle={toggleProposalSection}>
                    <div className={`rounded-2xl border px-4 py-3 ${proposal.refinement_result.status === 'rejected' ? 'border-warning/30 bg-warning/10' : 'border-success/30 bg-success/10'}`}>
                      <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Status</div>
                      <div className="mt-1 text-sm font-semibold text-foreground">{proposal.refinement_result.status.replace('_', ' ')}</div>
                      <div className="mt-2 text-sm text-muted-foreground">{proposal.refinement_result.message}</div>
                    </div>
                    {proposal.refinement_result.accepted_requests.length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Accepted</div>
                        <div className="mt-2 grid gap-2">
                          {proposal.refinement_result.accepted_requests.map((item) => (
                            <div key={item} className="rounded-xl border border-success/20 bg-success/5 px-3 py-2 text-sm text-foreground">{item}</div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {proposal.refinement_result.unsupported_requests.length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Unsupported or missing</div>
                        <div className="mt-2 grid gap-2">
                          {proposal.refinement_result.unsupported_requests.map((item) => (
                            <div key={item} className="rounded-xl border border-warning/20 bg-warning/5 px-3 py-2 text-sm text-foreground">{item}</div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {proposal.refinement_result.warnings.length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Warnings</div>
                        <div className="mt-2 grid gap-2">
                          {proposal.refinement_result.warnings.map((item) => (
                            <div key={item} className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">{item}</div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <div className="rounded-xl border border-border bg-secondary/20 p-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Diff summary</div>
                        <div className="mt-2 text-sm text-muted-foreground">
                          Added tabs: {proposal.refinement_result.diff.added_tabs.length ? proposal.refinement_result.diff.added_tabs.join(', ') : 'none'}
                        </div>
                        <div className="mt-1 text-sm text-muted-foreground">
                          Added widgets/sections: {proposal.refinement_result.diff.added_section_count}
                        </div>
                        <div className="mt-1 text-sm text-muted-foreground">
                          Title changed: {proposal.refinement_result.diff.changed_title ? 'yes' : 'no'}
                        </div>
                        <div className="mt-1 text-sm text-muted-foreground">
                          Summary changed: {proposal.refinement_result.diff.changed_summary ? 'yes' : 'no'}
                        </div>
                      </div>
                      <div className="rounded-xl border border-border bg-secondary/20 p-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Chart request outcome</div>
                        <div className="mt-2 text-sm text-muted-foreground">
                          Accepted chart types: {proposal.refinement_result.diff.accepted_chart_types.length ? proposal.refinement_result.diff.accepted_chart_types.join(', ') : 'none'}
                        </div>
                        <div className="mt-1 text-sm text-muted-foreground">
                          Missing chart types: {proposal.refinement_result.diff.missing_chart_types.length ? proposal.refinement_result.diff.missing_chart_types.join(', ') : 'none'}
                        </div>
                      </div>
                    </div>
                  </ProposalSection>
                ) : null}

                <ProposalSection title="Why this dashboard was proposed" sectionKey="rationale" expanded={proposalSectionState.rationale} onToggle={toggleProposalSection}>
                  <p className="text-sm text-muted-foreground">{proposal.rationale}</p>
                </ProposalSection>

                {(proposal.proposal.semantic_summary || proposal.proposal.semantic_details || (proposal.proposal.ambiguities?.length ?? 0) > 0 || (proposal.proposal.business_questions?.length ?? 0) > 0) && (
                  <ProposalSection title="Semantic interpretation" sectionKey="semantics" expanded={proposalSectionState.semantics} onToggle={toggleProposalSection}>
                    <div className="grid gap-3 md:grid-cols-2">
                      <div className="rounded-xl border border-border bg-secondary/20 p-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Summary</div>
                        <p className="mt-2 text-sm text-muted-foreground">{semanticSummaryText(proposal.proposal)}</p>
                      </div>
                      <div className="rounded-xl border border-border bg-secondary/20 p-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Business domain</div>
                        <div className="mt-2 text-sm font-semibold text-foreground">
                          {String(semanticDetailsFromConfig(proposal.proposal)?.business_domain ?? semanticDetailsFromConfig(proposal.proposal)?.dominant_domain ?? proposal.proposal.dashboard_family ?? 'Adaptive workbook')}
                        </div>
                      </div>
                    </div>
                    {semanticObjectNames(semanticDetailsFromConfig(proposal.proposal)?.entities, ['entity_name', 'name']).length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Entities</div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {semanticObjectNames(semanticDetailsFromConfig(proposal.proposal)?.entities, ['entity_name', 'name']).map((item) => (
                            <span key={item} className="inline-flex rounded-full border border-border bg-card px-2.5 py-1 text-xs text-muted-foreground">{item}</span>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {semanticObjectNames(semanticDetailsFromConfig(proposal.proposal)?.dimensions, ['dimension_name', 'name']).length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Dimensions</div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {semanticObjectNames(semanticDetailsFromConfig(proposal.proposal)?.dimensions, ['dimension_name', 'name']).map((item) => (
                            <span key={item} className="inline-flex rounded-full border border-border bg-card px-2.5 py-1 text-xs text-muted-foreground">{item}</span>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {semanticObjectNames(semanticDetailsFromConfig(proposal.proposal)?.measures, ['measure_name', 'name']).length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Measures</div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {semanticObjectNames(semanticDetailsFromConfig(proposal.proposal)?.measures, ['measure_name', 'name']).map((item) => (
                            <span key={item} className="inline-flex rounded-full border border-border bg-card px-2.5 py-1 text-xs text-muted-foreground">{item}</span>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {proposal.proposal.ambiguities?.length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Open ambiguities</div>
                        <div className="mt-2 grid gap-2">
                          {proposal.proposal.ambiguities.map((item) => (
                            <div key={item} className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">{item}</div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {proposal.proposal.business_questions?.length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Business questions</div>
                        <div className="mt-2 grid gap-3">
                          {proposal.proposal.business_questions.map((item) => (
                            <div key={item} className="rounded-xl border border-border bg-secondary/20 px-3 py-3">
                              <div className="text-sm text-muted-foreground">{item}</div>
                              <textarea
                                className="mt-3 min-h-[88px] w-full rounded-xl border border-border bg-card px-3 py-2 text-sm text-foreground outline-none transition focus:border-primary/40"
                                onChange={(event) =>
                                  setProposalQuestionAnswers((current) => ({ ...current, [item]: event.target.value }))
                                }
                                placeholder="Answer this business question here. Your answer will be used when you apply widget ideas or approve the proposal."
                                value={proposalQuestionAnswers[item] ?? ''}
                              />
                            </div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                  </ProposalSection>
                )}

                {(proposal.proposal.eda_plan ?? proposal.proposal.investigation_plan)?.length ? (
                  <ProposalSection title="EDA plan" sectionKey="edaPlan" expanded={proposalSectionState.edaPlan} onToggle={toggleProposalSection}>
                    {(proposal.proposal.orchestrator_workflow ?? proposal.proposal.proposal_workflow) ? (
                      <div className="mb-3 text-[0.68rem] font-bold uppercase tracking-widest text-muted-foreground">
                        {proposal.proposal.orchestrator_workflow ?? proposal.proposal.proposal_workflow}
                      </div>
                    ) : null}
                    <div className="grid gap-3">
                      {(proposal.proposal.eda_plan ?? proposal.proposal.investigation_plan ?? []).map((step) => (
                        <div key={step.key} className="rounded-xl border border-border bg-secondary/20 p-3">
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-sm font-semibold text-foreground">{step.title}</div>
                            <span className="text-[0.68rem] font-bold uppercase tracking-widest text-muted-foreground">{step.tool}</span>
                          </div>
                          <div className="mt-1 text-sm text-muted-foreground">{step.objective}</div>
                          <div className="mt-2 text-xs text-muted-foreground">{step.rationale}</div>
                        </div>
                      ))}
                    </div>
                  </ProposalSection>
                ) : null}

                {(proposal.proposal.eda_evidence ?? proposal.proposal.investigation_evidence)?.length ? (
                  <ProposalSection title="EDA evidence" sectionKey="edaEvidence" expanded={proposalSectionState.edaEvidence} onToggle={toggleProposalSection}>
                    <div className="grid gap-3">
                      {(proposal.proposal.eda_evidence ?? proposal.proposal.investigation_evidence ?? []).map((item) => (
                        <div key={`${item.key}-${item.title}`} className="rounded-xl border border-border bg-secondary/20 p-3">
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-sm font-semibold text-foreground">{item.title}</div>
                            <span className="text-[0.68rem] font-bold uppercase tracking-widest text-muted-foreground">
                              {item.tool}{item.confidence_score !== undefined && item.confidence_score !== null ? ` · ${(item.confidence_score * 100).toFixed(0)}%` : ''}
                            </span>
                          </div>
                          <div className="mt-1 text-sm text-muted-foreground">{item.detail}</div>
                          {item.supporting_metrics?.length ? (
                            <div className="mt-2 flex flex-wrap gap-2">
                              {item.supporting_metrics.map((metric) => (
                                <span key={metric} className="inline-flex rounded-full border border-border bg-card px-2.5 py-1 text-xs text-muted-foreground">{metric}</span>
                              ))}
                            </div>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  </ProposalSection>
                ) : null}

                <ProposalSection title="Layout and widgets" sectionKey="layout" expanded={proposalSectionState.layout} onToggle={toggleProposalSection}>
                  {proposal.proposal.customization_prompts.length > 0 ? (
                    <div className="mb-4">
                      <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Suggested business customization questions</div>
                      <div className="mt-2 grid gap-2">
                        {proposal.proposal.customization_prompts.map((item) => (
                          <div key={item} className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">{item}</div>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  <div className="grid gap-4">
                    {proposal.proposal.tabs.map((item) => (
                      <section key={item.key} className="rounded-2xl border border-border bg-secondary/10 overflow-hidden">
                        <div className="border-b border-border px-4 py-3">
                          <div className="text-sm font-bold text-foreground">{item.label}</div>
                          <div className="text-xs text-muted-foreground mt-1">{item.description}</div>
                        </div>
                        <div className="px-4 py-4 grid gap-3">
                          {item.sections.map((section) => (
                            <div key={section.key} className="rounded-xl border border-border bg-card p-3">
                              <div className="text-sm font-semibold text-foreground">{section.label}</div>
                              <div className="text-xs text-muted-foreground mt-1">{section.description}</div>
                              <div className="mt-2 text-[0.72rem] font-bold uppercase tracking-widest text-muted-foreground">{section.renderer}</div>
                            </div>
                          ))}
                        </div>
                      </section>
                    ))}
                  </div>
                </ProposalSection>
              </div>
            </div>

            <div className="sticky bottom-0 flex items-center justify-between gap-3 border-t border-border bg-card px-6 py-4">
                <div className="text-sm text-muted-foreground">
                {snapshotContextMismatch
                  ? 'Approval is blocked because the dashboard is loaded for a different snapshot than the proposal request.'
                  : currentDraftProposalInput !== lastAppliedProposalInput
                  ? 'Apply the current widget changes before approval, or approve directly to regenerate the proposal with the latest request.'
                  : proposalApproveBlocked
                  ? 'Approval is blocked because the latest widget refinement request was rejected. Update or clear the request first.'
                  : 'Approving this will save the blueprint for future workbooks with the same schema signature.'}
              </div>
              <div className="flex items-center gap-2">
                <button
                  className="rounded-xl border border-border bg-card px-4 py-2.5 text-sm font-semibold text-foreground transition hover:bg-accent"
                  onClick={() => void openProposalReview()}
                  disabled={proposalRefreshLoading || proposalApproveLoading || snapshotContextMismatch}
                  type="button"
                >
                  {proposalRefreshLoading ? 'Applying…' : proposalGuidance.trim() ? 'Apply Widget Ideas' : 'Refresh Proposal'}
                </button>
                <button
                  className="rounded-xl px-4 py-2.5 text-sm font-semibold text-primary-foreground transition disabled:opacity-40"
                  disabled={proposalRefreshLoading || proposalApproveLoading || proposalApproveBlocked || snapshotContextMismatch}
                  onClick={() => void approveCurrentProposal()}
                  style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}
                  type="button"
                >
                  {proposalApproveLoading ? 'Approving…' : 'Approve Blueprint'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
