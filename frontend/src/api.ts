import { useAuth } from './auth/AuthProvider'
import { loadGeminiSettings } from './geminiSettings'

export const API_BASE = import.meta.env.VITE_API_BASE ?? 'http://localhost:8000'
const TOKEN_STORAGE_KEY = 'gkm_token'

export type TokenResponse = { access_token: string; token_type: 'bearer' }
export type PiiType = 'name' | 'email' | 'phone' | 'address' | 'identifier' | 'custom'
export type PiiFieldSelection = {
  sheet_name: string
  section_key: string
  header_label: string
  pii_type: PiiType
}
export type WorkbookPreviewResponse = {
  upload_token: string
  workbook_family: string
  family_label: string
  family_mode: string
  sheets: Array<{
    sheet_name: string
    sections: Array<{
      section_key: string
      section_label: string
      header_row: number
      headers: Array<{
        column: string
        header_label: string
        sample_value: string | null
        suggested_pii_type: PiiType | null
      }>
    }>
  }>
}
export type DocumentProcessingJobResponse = {
  id: number
  upload_token: string
  workbook_family: string
  status: string
  stage: string
  progress_percent: number
  message: string
  error_detail: string | null
  snapshot_id: number | null
}
export type DashboardSectionSpec = {
  key: string
  label: string
  description: string
  renderer: string
  slot?: string | null
  widget_type?: string | null
  bindings?: Record<string, unknown> | null
}
export type DashboardTabSpec = {
  key: string
  label: string
  description: string
  sections: DashboardSectionSpec[]
}
export type WorkbookSchemaFieldSpec = {
  column: string
  header_label: string
  normalized_header: string
  sample_value: string | null
  suggested_pii_type: string | null
}
export type WorkbookSchemaSectionSpec = {
  section_key: string
  section_label: string
  header_row: number
  fields: WorkbookSchemaFieldSpec[]
}
export type WorkbookSchemaSheetSpec = {
  sheet_name: string
  sections: WorkbookSchemaSectionSpec[]
}
export type DashboardBlueprintConfig = {
  dashboard_family: string
  layout_template: string
  title: string
  subtitle: string
  tabs: DashboardTabSpec[]
  kpi_cards: Array<Record<string, unknown>>
  schema_fields: WorkbookSchemaSheetSpec[]
  customization_prompts: string[]
  semantic_summary?: string | null
  semantic_details?: Record<string, unknown> | null
  business_questions?: string[]
  ambiguities?: string[]
  semantic_confidence?: number | null
  eda_plan?: Array<{
    key: string
    title: string
    objective: string
    tool: string
    rationale: string
    priority?: number | null
  }>
  eda_evidence?: Array<{
    key: string
    tool: string
    title: string
    detail: string
    confidence_score?: number | null
    supporting_metrics?: string[]
  }>
  eda_workflow?: string | null
  orchestrator_workflow?: string | null
  investigation_plan?: Array<{
    key: string
    title: string
    objective: string
    tool: string
    rationale: string
    priority?: number | null
  }>
  investigation_evidence?: Array<{
    key: string
    tool: string
    title: string
    detail: string
    confidence_score?: number | null
    supporting_metrics?: string[]
  }>
  proposal_workflow?: string | null
  dashboard_preferences?: {
    hidden_cards: string[]
    card_orders: Record<string, string[]>
  }
}
export type DashboardBlueprintResponse = {
  id: number
  blueprint_key: string
  name: string
  description: string
  schema_signature: string
  workbook_type: string
  status: string
  config: DashboardBlueprintConfig
}
export type DashboardRefinementResult = {
  status: 'not_requested' | 'fulfilled' | 'partially_fulfilled' | 'rejected'
  message: string
  accepted_requests: string[]
  unsupported_requests: string[]
  warnings: string[]
  diff: {
    added_tabs: string[]
    added_section_count: number
    added_section_labels: string[]
    accepted_chart_types: string[]
    missing_chart_types: string[]
    changed_title: boolean
    changed_summary: boolean
  }
}
export type DashboardProposalResponse = {
  id: number
  snapshot_id: number
  status: string
  match_mode: string
  confidence_score: number
  title: string
  summary: string
  rationale: string
  schema_signature: string
  workbook_type: string
  matched_blueprint_id: number | null
  approved_blueprint_id: number | null
  proposal: DashboardBlueprintConfig
  refinement_result?: DashboardRefinementResult | null
}
export type DashboardRuntimeResponse = {
  snapshot_id: number
  workbook_type: string
  payload: Record<string, unknown> | null
}
export type DashboardKpis = {
  snapshot: { id: number; as_of_date: string; source_filename: string }
  total_contracted: number
  total_received: number
  total_pending: number
  total_contracted_ind: number
  total_contracted_bus: number
  total_received_ind: number
  total_received_bus: number
  overall_receipt_rate: number
  active_clients: number
  zero_received_clients: number
  over_delivered_clients: number
  staff_total_received: number
}
export type DashboardClientRow = {
  client_name: string
  client_id: string
  client_type: string
  contracted_ind: number
  contracted_bus: number
  contracted_total: number
  received_ind: number
  received_bus: number
  received_total: number
  pending_ind: number
  pending_bus: number
  pending_total: number
  receipt_rate: number | null
}
export type DashboardStaffRow = {
  name: string
  staff_id: string
  staff_type: string
  received_ind: number
  received_bus: number
  received_total: number
}
export type DashboardAssistantAnswer = {
  title: string
  summary?: string
  cards: Array<{
    title: string
    value: string
    meta?: string | null
  }>
  bullets: string[]
}
export type AnalysisSetMemberResponse = {
  snapshot_id: number
  source_filename: string
  as_of_date: string | null
  workbook_type: string | null
  member_order: number
  role_label: string | null
}
export type AnalysisSetProposalResponse = {
  id: number
  status: string
  name: string
  summary: string
  intent?: string | null
  relationship_type: string
  confidence_score: number
  comparability: string
  rationale: string
  suggested_join_keys: string[]
  suggested_period_order: AnalysisSetMemberResponse[]
  conflicts: string[]
  dashboard_hypothesis: string[]
  members: AnalysisSetMemberResponse[]
}
export type AnalysisSetDashboardViewResponse = {
  analysis_set_id: number
  name: string
  summary: string
  relationship_type: string
  confidence_score: number
  dashboard_config: DashboardBlueprintConfig
  runtime_payload: Record<string, unknown> | null
  generated_at?: string | null
}

async function http<T>(path: string, opts: RequestInit = {}, token?: string | null): Promise<T> {
  const headers = new Headers(opts.headers)
  if (!headers.has('Content-Type') && !(opts.body instanceof FormData)) headers.set('Content-Type', 'application/json')
  if (token) headers.set('Authorization', `Bearer ${token}`)

  // Optional: Gemini key/model for server-side processing (stored locally in browser).
  const g = loadGeminiSettings()
  if (g.enabled && g.apiKey) {
    headers.set('X-Gemini-Api-Key', g.apiKey)
    headers.set('X-Gemini-Model', g.model)
  }

  const res = await fetch(`${API_BASE}${path}`, { ...opts, headers })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    if (res.status === 401 && token) {
      localStorage.removeItem(TOKEN_STORAGE_KEY)
      if (typeof window !== 'undefined' && window.location.pathname !== '/login') window.location.assign('/login')
      throw new Error('Your session is no longer valid. Please sign in again.')
    }
    throw new Error(text || `HTTP ${res.status}`)
  }
  if (res.status === 204) return undefined as T
  const ct = res.headers.get('content-type') || ''
  if (!ct.includes('application/json')) return (await res.text()) as unknown as T
  return (await res.json()) as T
}

async function downloadFile(path: string, filename: string, token?: string | null): Promise<void> {
  const headers = new Headers()
  if (token) headers.set('Authorization', `Bearer ${token}`)
  const g = loadGeminiSettings()
  if (g.enabled && g.apiKey) {
    headers.set('X-Gemini-Api-Key', g.apiKey)
    headers.set('X-Gemini-Model', g.model)
  }

  const res = await fetch(`${API_BASE}${path}`, { headers })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    if (res.status === 401 && token) {
      localStorage.removeItem(TOKEN_STORAGE_KEY)
      if (typeof window !== 'undefined' && window.location.pathname !== '/login') window.location.assign('/login')
      throw new Error('Your session is no longer valid. Please sign in again.')
    }
    throw new Error(text || `HTTP ${res.status}`)
  }

  const blob = await res.blob()
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  document.body.appendChild(link)
  link.click()
  link.remove()
  URL.revokeObjectURL(url)
}

export function useApi() {
  const { token } = useAuth()
  return {
    login: (email: string, password: string) =>
      http<TokenResponse>('/auth/login', { method: 'POST', body: JSON.stringify({ email, password }) }),
    listSnapshots: () => http<Array<{ id: number; as_of_date: string; source_filename: string }>>('/snapshots', {}, token),
    deleteSnapshot: (snapshotId: number) => http<void>(`/snapshots/${snapshotId}`, { method: 'DELETE' }, token),
    getKpis: (snapshotId?: number) =>
      http<DashboardKpis>(`/dashboard/kpis${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`, {}, token),
    listClients: (snapshotId?: number) =>
      http<DashboardClientRow[]>(`/dashboard/clients${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`, {}, token),
    listStaff: (snapshotId?: number) =>
      http<DashboardStaffRow[]>(`/dashboard/staff${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`, {}, token),
    getDashboardBlueprint: (snapshotId?: number) =>
      http<DashboardBlueprintResponse | null>(`/dashboard/blueprint${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`, {}, token),
    proposeDashboardBlueprint: (snapshotId?: number, userGuidance?: string) =>
      http<DashboardProposalResponse>(
        `/dashboard/proposals${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`,
        { method: 'POST', body: JSON.stringify({ user_guidance: userGuidance?.trim() || null }) },
        token,
      ),
    approveDashboardProposal: (proposalId: number) =>
      http<DashboardBlueprintResponse>(`/dashboard/proposals/${proposalId}/approve`, { method: 'POST' }, token),
    updateDashboardBlueprintPreferences: (
      preferences: { hidden_cards: string[]; card_orders: Record<string, string[]> },
      snapshotId?: number,
    ) =>
      http<DashboardBlueprintResponse>(
        `/dashboard/blueprint/preferences${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`,
        { method: 'PATCH', body: JSON.stringify(preferences) },
        token,
      ),
    getDashboardRuntime: (snapshotId?: number) =>
      http<DashboardRuntimeResponse>(`/dashboard/runtime${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`, {}, token),
    askDashboardQuestion: (question: string, snapshotId?: number) =>
      http<DashboardAssistantAnswer>('/dashboard/chat', {
        method: 'POST',
        body: JSON.stringify({ question, snapshot_id: snapshotId }),
      }, token),
    previewWorkbook: async (file: File) => {
      const fd = new FormData()
      fd.append('file', file)
      return http<WorkbookPreviewResponse>(
        '/documents/excel/contracted-vs-actual/preview',
        { method: 'POST', body: fd },
        token,
      )
    },
    startDocumentProcessing: (uploadToken: string, piiFields: PiiFieldSelection[], dashboardGuidance?: string) =>
      http<DocumentProcessingJobResponse>(
        '/documents/process',
        {
          method: 'POST',
          body: JSON.stringify({
            upload_token: uploadToken,
            pii_fields: piiFields,
            dashboard_guidance: dashboardGuidance?.trim() || null,
          }),
        },
        token,
      ),
    getDocumentProcessingJob: (jobId: number) =>
      http<DocumentProcessingJobResponse>(`/documents/jobs/${jobId}`, {}, token),
    downloadOverallReport: (snapshotId?: number) =>
      downloadFile(
        `/reports/summary.pdf${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`,
        `gkm-summary-snapshot-${snapshotId ?? 'latest'}.pdf`,
        token,
      ),
    downloadAnalyticsReport: (snapshotId?: number) =>
      downloadFile(
        `/reports/analytics-summary.pdf${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`,
        `gkm-analytics-summary-snapshot-${snapshotId ?? 'latest'}.pdf`,
        token,
      ),
    proposeAnalysisSet: (snapshotIds: number[], intent?: string, title?: string) =>
      http<AnalysisSetProposalResponse>(
        '/analysis-sets/proposals',
        {
          method: 'POST',
          body: JSON.stringify({
            snapshot_ids: snapshotIds,
            intent: intent?.trim() || null,
            title: title?.trim() || null,
          }),
        },
        token,
      ),
    listAnalysisSets: () => http<AnalysisSetProposalResponse[]>('/analysis-sets', {}, token),
    getAnalysisSet: (analysisSetId: number) => http<AnalysisSetProposalResponse>(`/analysis-sets/${analysisSetId}`, {}, token),
    confirmAnalysisSet: (
      analysisSetId: number,
      payload: {
        title?: string | null
        intent?: string | null
        relationship_type?: string | null
        join_keys?: string[]
        member_labels?: Record<number, string>
      },
    ) =>
      http<AnalysisSetProposalResponse>(
        `/analysis-sets/${analysisSetId}/confirm`,
        {
          method: 'POST',
          body: JSON.stringify({
            title: payload.title?.trim() || null,
            intent: payload.intent?.trim() || null,
            relationship_type: payload.relationship_type?.trim() || null,
            join_keys: payload.join_keys ?? [],
            member_labels: payload.member_labels ?? {},
          }),
        },
        token,
      ),
    getAnalysisSetDashboardView: (analysisSetId: number) =>
      http<AnalysisSetDashboardViewResponse>(`/analysis-sets/${analysisSetId}/dashboard-view`, {}, token),
    downloadCombinedExecutiveReport: (analysisSetId: number) =>
      downloadFile(
        `/analysis-sets/${analysisSetId}/summary.pdf`,
        `gkm-combined-summary-${analysisSetId}.pdf`,
        token,
      ),
    downloadCombinedAnalyticsReport: (analysisSetId: number) =>
      downloadFile(
        `/analysis-sets/${analysisSetId}/analytics-summary.pdf`,
        `gkm-combined-analytics-${analysisSetId}.pdf`,
        token,
      ),
    downloadClientReport: (clientId: string, snapshotId?: number) =>
      downloadFile(
        `/reports/clients/${encodeURIComponent(clientId)}/summary.pdf${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`,
        `gkm-client-${clientId}-snapshot-${snapshotId ?? 'latest'}.pdf`,
        token,
      ),
  }
}
