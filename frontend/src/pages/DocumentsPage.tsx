import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import type { AnalysisSetProposalResponse, DashboardProposalResponse, DocumentProcessingJobResponse, PiiFieldSelection, PiiType, WorkbookPreviewResponse } from '../api'
import { useApi } from '../api'
import { useDashboardData } from '../dashboard/DashboardDataProvider'
import { Upload, Eye, Trash2, FileSpreadsheet, CheckCircle2, FileDown, ChevronDown, ChevronRight, LoaderCircle } from 'lucide-react'

type DocRow = {
  id: string; fileName: string; format: string; type: string
  status: 'Preparing preview' | 'Ready for review' | 'Processing' | 'Processed' | 'Rejected'; subtitle: string; snapshotId?: number
}
type ProposalStatusTone = 'success' | 'warning'

function pillStyle(status: DocRow['status']) {
  const map: Record<DocRow['status'], { bg: string; text: string; dot: string }> = {
    'Preparing preview': { bg: 'hsl(217 91% 60% / 0.1)', text: 'hsl(217 91% 45%)', dot: 'hsl(217 91% 55%)' },
    'Ready for review': { bg: 'hsl(262 83% 58% / 0.1)', text: 'hsl(262 60% 45%)', dot: 'hsl(262 83% 58%)' },
    Processing: { bg: 'hsl(217 91% 60% / 0.1)', text: 'hsl(217 91% 45%)', dot: 'hsl(217 91% 55%)' },
    Processed: { bg: 'hsl(152 60% 38% / 0.1)', text: 'hsl(152 60% 30%)', dot: 'hsl(152 60% 38%)' },
    Rejected: { bg: 'hsl(0 84% 60% / 0.1)', text: 'hsl(0 84% 45%)', dot: 'hsl(0 84% 50%)' },
  }
  return map[status]
}

function formatDate(value: string) {
  const p = new Date(value)
  if (Number.isNaN(p.getTime())) return value
  return new Intl.DateTimeFormat(undefined, { month: 'short', day: '2-digit', year: 'numeric' }).format(p)
}

const piiTypeOptions: Array<{ value: PiiType; label: string }> = [
  { value: 'name', label: 'Name' },
  { value: 'identifier', label: 'Identifier' },
  { value: 'email', label: 'Email' },
  { value: 'phone', label: 'Phone' },
  { value: 'address', label: 'Address' },
  { value: 'custom', label: 'Custom' },
]

function fieldKey(sheetName: string, sectionKey: string, headerLabel: string) {
  return `${sheetName}::${sectionKey}::${headerLabel}`
}

function extractErrorMessage(error: unknown, fallback: string) {
  if (error instanceof Error && error.message.trim()) return error.message
  if (typeof error === 'string' && error.trim()) return error
  return fallback
}

function titleCaseWords(value: string) {
  return value
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function isSemanticBlob(value: unknown) {
  if (typeof value !== 'string') return false
  const text = value.trim()
  return (text.startsWith('{') && text.endsWith('}')) || (text.startsWith('[') && text.endsWith(']'))
}

function semanticDetailsFromConfig(config: DashboardProposalResponse['proposal'] | null | undefined) {
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

function semanticSummaryText(config: DashboardProposalResponse['proposal'] | null | undefined) {
  const details = semanticDetailsFromConfig(config)
  const description = typeof details?.description === 'string' ? details.description.trim() : ''
  const businessDomain = typeof details?.business_domain === 'string' ? details.business_domain.trim() : ''
  const summary = typeof config?.semantic_summary === 'string' ? config.semantic_summary.trim() : ''
  if (description && businessDomain) return `${businessDomain}: ${description}`
  if (description) return description
  if (summary && !isSemanticBlob(summary)) return summary
  return 'Semantic interpretation was captured for this workbook and has been normalized for dashboard planning.'
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

function proposalTabKeys(config: DashboardProposalResponse['proposal'] | null | undefined) {
  return new Set((config?.tabs ?? []).map((item) => item.key))
}

function proposalSectionKeys(config: DashboardProposalResponse['proposal'] | null | undefined) {
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

export function DocumentsPage() {
  const api = useApi()
  const nav = useNavigate()
  const { snapshots, refreshSnapshots, refreshDashboard, setSnapshotId } = useDashboardData()
  const [file, setFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [msg, setMsg] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [lastProcessedSnapshotId, setLastProcessedSnapshotId] = useState<number | null>(null)
  const [downloadingSnapshotReport, setDownloadingSnapshotReport] = useState<string | null>(null)
  const [preview, setPreview] = useState<WorkbookPreviewResponse | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [showPiiModal, setShowPiiModal] = useState(false)
  const [showDashboardIntentModal, setShowDashboardIntentModal] = useState(false)
  const [piiReviewed, setPiiReviewed] = useState(false)
  const [piiSelections, setPiiSelections] = useState<Record<string, PiiType>>({})
  const [dashboardProposal, setDashboardProposal] = useState<DashboardProposalResponse | null>(null)
  const [showDashboardProposalModal, setShowDashboardProposalModal] = useState(false)
  const [proposalRefreshLoading, setProposalRefreshLoading] = useState(false)
  const [proposalApproveLoading, setProposalApproveLoading] = useState(false)
  const [proposalGuidance, setProposalGuidance] = useState('')
  const [proposalQuestionAnswers, setProposalQuestionAnswers] = useState<Record<string, string>>({})
  const [lastAppliedProposalInput, setLastAppliedProposalInput] = useState('')
  const [proposalStatusNote, setProposalStatusNote] = useState<string | null>(null)
  const [proposalStatusTone, setProposalStatusTone] = useState<ProposalStatusTone>('success')
  const [proposalSectionState, setProposalSectionState] = useState<Record<string, boolean>>({
    rationale: false,
    guidance: true,
    refinement: false,
    semantics: false,
    edaPlan: false,
    edaEvidence: false,
    layout: false,
  })
  const [dashboardIntent, setDashboardIntent] = useState('')
  const [processingJob, setProcessingJob] = useState<DocumentProcessingJobResponse | null>(null)
  const [selectedSnapshotIds, setSelectedSnapshotIds] = useState<number[]>([])
  const [showAnalysisSetModal, setShowAnalysisSetModal] = useState(false)
  const [analysisSetDraft, setAnalysisSetDraft] = useState<AnalysisSetProposalResponse | null>(null)
  const [analysisSetLoading, setAnalysisSetLoading] = useState(false)
  const [analysisSetConfirming, setAnalysisSetConfirming] = useState(false)
  const [analysisSetTitle, setAnalysisSetTitle] = useState('')
  const [analysisSetIntent, setAnalysisSetIntent] = useState('')
  const [analysisSetRelationshipType, setAnalysisSetRelationshipType] = useState('time_series')
  const [analysisSetJoinKeysText, setAnalysisSetJoinKeysText] = useState('')
  const [analysisSetMemberLabels, setAnalysisSetMemberLabels] = useState<Record<number, string>>({})
  const [analysisSets, setAnalysisSets] = useState<AnalysisSetProposalResponse[]>([])
  const [lastConfirmedAnalysisSet, setLastConfirmedAnalysisSet] = useState<AnalysisSetProposalResponse | null>(null)
  const [downloadingAnalysisSetReport, setDownloadingAnalysisSetReport] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const processingActive = processingJob !== null && ['queued', 'running'].includes(processingJob.status)
  const workflowStep = previewLoading
    ? 'Preparing Preview'
    : processingActive
      ? 'Processing Snapshot'
      : file && preview && !piiReviewed
        ? 'PII Review Ready'
        : file && preview && piiReviewed
          ? 'Ready to Process'
          : 'Idle'
  const workflowMessage = previewLoading
    ? 'Scanning workbook structure, reading headers, and sampling a few rows for PII suggestions.'
    : processingActive
      ? (processingJob?.message ?? 'Processing workbook in the background.')
      : file && preview && !piiReviewed
        ? 'Review the suggested PII fields before the snapshot job starts.'
        : file && preview && piiReviewed
          ? (dashboardIntent.trim()
            ? 'PII review is complete and SME dashboard context is captured. Start processing when ready.'
            : 'PII review is complete. You can optionally add dashboard context before starting processing.')
          : 'Choose a workbook to begin schema profiling.'

  const rows = useMemo<DocRow[]>(() => {
    const out: DocRow[] = []
    if (file) {
      const selectedStatus: DocRow['status'] =
        previewLoading ? 'Preparing preview'
        : processingJob && (processingJob.status === 'queued' || processingJob.status === 'running') ? 'Processing'
        : processingJob?.status === 'failed' ? 'Rejected'
        : 'Ready for review'
      const selectedSubtitle =
        previewLoading ? 'Reading headers and preparing PII review...'
        : processingJob?.status === 'queued' || processingJob?.status === 'running' ? processingJob.message
        : processingJob?.status === 'failed' ? (processingJob.error_detail || processingJob.message)
        : preview && !piiReviewed ? 'PII review is ready.'
        : `${(file.size / 1024).toFixed(1)} KB`
      out.push({ id: 'selected', fileName: file.name, format: file.name.toLowerCase().endsWith('.xlsx') ? 'XLSX' : '—', type: preview?.family_label ?? 'Workbook',
        status: selectedStatus,
        subtitle: selectedSubtitle })
    }
    for (const s of snapshots) {
      out.push({ id: String(s.id), fileName: s.source_filename, format: s.source_filename.toLowerCase().endsWith('.xlsx') ? 'XLSX' : '—', type: 'Snapshot',
        status: 'Processed', subtitle: `As of ${formatDate(s.as_of_date)}`, snapshotId: s.id })
    }
    return out
  }, [file, snapshots, preview, previewLoading, processingJob, piiReviewed])

  const snapshotLabelById = useMemo(() => {
    const labels = new Map<number, string>()
    for (const item of snapshots) {
      labels.set(item.id, `${item.source_filename} (${formatDate(item.as_of_date)})`)
    }
    return labels
  }, [snapshots])

  const lastProcessedSnapshotLabel = lastProcessedSnapshotId
    ? (snapshotLabelById.get(lastProcessedSnapshotId) ?? `the latest processed workbook`)
    : null
  useEffect(() => {
    if (!processingJob || !['queued', 'running'].includes(processingJob.status)) return
    const timer = window.setInterval(() => {
      void (async () => {
        try {
          const nextJob = await api.getDocumentProcessingJob(processingJob.id)
          setProcessingJob(nextJob)
          if (nextJob.status === 'succeeded' && nextJob.snapshot_id) {
            setUploading(false)
            setLastProcessedSnapshotId(nextJob.snapshot_id)
            setSnapshotId(nextJob.snapshot_id)
            setMsg(`Snapshot created for ${file?.name ?? 'the workbook'}. The dashboard proposal is being generated now.`)
            setProposalRefreshLoading(true)
            setProposalStatusNote(null)
            setProposalStatusTone('success')
            try {
              const proposal = await api.proposeDashboardBlueprint(nextJob.snapshot_id, dashboardIntent)
              setDashboardProposal(proposal)
              setLastAppliedProposalInput(dashboardIntent.trim())
              setProposalStatusNote('Proposal generated. Review the tabs and widgets, then approve when ready.')
              setShowDashboardProposalModal(true)
              setProposalGuidance(dashboardIntent)
              setMsg(`Processing complete for ${file?.name ?? 'the workbook'}. Review and refine the generated dashboard widgets before opening the dashboard.`)
            } catch {
              setErr('Snapshot processed, but the dashboard proposal could not be generated.')
            } finally {
              setProposalRefreshLoading(false)
            }
            setFile(null)
            setPreview(null)
            setPiiSelections({})
            setPiiReviewed(false)
            await refreshSnapshots()
            await refreshDashboard(nextJob.snapshot_id)
          } else if (nextJob.status === 'failed') {
            setUploading(false)
            setErr(nextJob.error_detail || nextJob.message || 'Processing failed.')
          }
        } catch {
          setUploading(false)
          setErr('Could not refresh document processing status.')
        }
      })()
    }, 1200)
    return () => window.clearInterval(timer)
  }, [api, processingJob, refreshDashboard, refreshSnapshots])

  useEffect(() => {
    setSelectedSnapshotIds((current) => current.filter((id) => snapshots.some((snapshot) => snapshot.id === id)))
  }, [snapshots])

  useEffect(() => {
    setProposalQuestionAnswers({})
    setLastAppliedProposalInput('')
    setProposalStatusNote(null)
    setProposalStatusTone('success')
  }, [dashboardProposal?.id])

  useEffect(() => {
    void (async () => {
      try {
        const rows = await api.listAnalysisSets()
        setAnalysisSets(rows)
      } catch {
        setAnalysisSets([])
      }
    })()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  function triggerProcessFlow() {
    if (!file) return
    if (!preview) {
      setErr('Workbook preview is not ready yet. Wait for the PII review step to load.')
      return
    }
    if (!piiReviewed) {
      setShowPiiModal(true)
      return
    }
    void handleProcess()
  }

  function savePiiReview(options?: { openDashboardIntent?: boolean }) {
    setShowPiiModal(false)
    setPiiReviewed(true)
    setMsg(`PII review saved for ${file?.name ?? 'the workbook'}. ${dashboardIntent.trim() ? 'SME dashboard context is already present.' : 'You can add dashboard intent before processing if needed.'}`)
    if (options?.openDashboardIntent) {
      setShowDashboardIntentModal(true)
    }
  }

  async function handleProcess() {
    if (!file || !preview || !piiReviewed) return
    setUploading(true); setMsg(null); setErr(null)
    try {
      const selectedFields: PiiFieldSelection[] = []
      for (const [key, piiType] of Object.entries(piiSelections)) {
        const [sheet_name, section_key, header_label] = key.split('::')
        if (sheet_name && section_key && header_label) selectedFields.push({ sheet_name, section_key, header_label, pii_type: piiType })
      }
      const job = await api.startDocumentProcessing(preview.upload_token, selectedFields, dashboardIntent)
      setProcessingJob(job)
    } catch (error) {
      setUploading(false)
      const message = extractErrorMessage(error, 'Process failed. Ensure the backend is running and the workbook matches the expected layout.')
      if (message.includes('already processed')) {
        await refreshSnapshots()
        setFile(null)
        setPreview(null)
        setPiiSelections({})
        setPiiReviewed(false)
        setDashboardIntent('')
        setShowDashboardIntentModal(false)
        setMsg('This upload was already processed. The snapshot list has been refreshed; use the processed row below instead of starting the job again.')
        return
      }
      if (message.includes('already being processed') || message.includes('already active')) {
        setMsg('Processing for this upload is already running. Wait for the current job to complete instead of starting it again.')
        return
      }
      setErr(message)
    }
  }

  function toggleProposalSection(sectionKey: string) {
    setProposalSectionState((current) => ({ ...current, [sectionKey]: !current[sectionKey] }))
  }

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

  const currentDraftProposalInput = buildProposalInput(proposalGuidance, proposalQuestionAnswers)
  const proposalApproveBlocked = dashboardProposal?.refinement_result?.status === 'rejected' && currentDraftProposalInput === lastAppliedProposalInput

  function toggleSnapshotSelection(snapshotId: number) {
    setSelectedSnapshotIds((current) =>
      current.includes(snapshotId)
        ? current.filter((id) => id !== snapshotId)
        : [...current, snapshotId],
    )
  }

  async function openCombineAnalysisModal() {
    if (selectedSnapshotIds.length < 2) {
      setErr('Select at least two processed documents to create a combined analysis.')
      return
    }
    setErr(null)
    setMsg(`Assessing similarity across ${selectedSnapshotIds.length} processed documents...`)
    setAnalysisSetLoading(true)
    try {
      const draft = await api.proposeAnalysisSet(selectedSnapshotIds)
      setAnalysisSetDraft(draft)
      setAnalysisSetTitle(draft.name)
      setAnalysisSetIntent(draft.intent ?? '')
      setAnalysisSetRelationshipType(draft.relationship_type)
      setAnalysisSetJoinKeysText(draft.suggested_join_keys.join(', '))
      setAnalysisSetMemberLabels(
        Object.fromEntries(draft.members.map((member) => [member.snapshot_id, member.role_label ?? `Document ${member.member_order}`])),
      )
      setShowAnalysisSetModal(true)
      setMsg(`Review the combined-analysis proposal for ${selectedSnapshotIds.length} selected documents.`)
    } catch (error) {
      setErr(extractErrorMessage(error, 'Could not generate the combined-analysis proposal.'))
    } finally {
      setAnalysisSetLoading(false)
    }
  }

  async function confirmCombinedAnalysis() {
    if (!analysisSetDraft) return
    setErr(null)
    setAnalysisSetConfirming(true)
    try {
      const confirmed = await api.confirmAnalysisSet(analysisSetDraft.id, {
        title: analysisSetTitle,
        intent: analysisSetIntent,
        relationship_type: analysisSetRelationshipType,
        join_keys: analysisSetJoinKeysText.split(',').map((item) => item.trim()).filter(Boolean),
        member_labels: analysisSetMemberLabels,
      })
      setAnalysisSetDraft(confirmed)
      setShowAnalysisSetModal(false)
      setSelectedSnapshotIds([])
      setLastConfirmedAnalysisSet(confirmed)
      setAnalysisSets((current) => [confirmed, ...current.filter((item) => item.id !== confirmed.id)])
      setMsg(`Combined analysis set "${confirmed.name}" was created. The combined dashboard and summaries are now ready.`)
    } catch (error) {
      setErr(extractErrorMessage(error, 'Could not confirm the combined-analysis proposal.'))
    } finally {
      setAnalysisSetConfirming(false)
    }
  }

  return (
    <div className="flex flex-col gap-5 animate-fade-in">
      <header className="flex items-start justify-between gap-5 flex-wrap">
        <div>
          <h1 className="text-2xl font-black text-foreground tracking-tight">Documents</h1>
          <p className="mt-1 text-sm text-muted-foreground">Upload a workbook and turn it into a dashboard snapshot.</p>
        </div>
      </header>

      <section className="rounded-2xl border border-border bg-card px-5 py-5 shadow-soft">
        <div className="flex flex-col gap-4">
          <div className="flex items-center justify-between gap-4 flex-wrap">
            <div>
              <h2 className="text-base font-bold text-foreground">Upload workbook</h2>
              <p className="mt-1 text-sm text-muted-foreground">Accepted format: `.xlsx` workbook. The app profiles the schema first, then routes it into the right dashboard family.</p>
            </div>
            <div className="text-sm text-muted-foreground">
              {snapshots.length} snapshot{snapshots.length === 1 ? '' : 's'}
            </div>
          </div>

            <div className="rounded-2xl border border-dashed border-border bg-secondary/35 p-5">
              <div className="flex items-center justify-between gap-4 flex-wrap">
              <div className="min-w-0">
                <div className="text-sm font-semibold text-foreground">
                  {file ? file.name : 'No workbook selected'}
                </div>
                <div className="mt-1 text-sm text-muted-foreground">
                  {file ? `${(file.size / 1024).toFixed(1)} KB` : 'Choose a file to create a new snapshot.'}
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-2 text-xs">
                  <span className="inline-flex rounded-full border border-border bg-card px-2.5 py-1 font-semibold text-foreground">
                    {workflowStep}
                  </span>
                  {processingActive && processingJob ? (
                    <span className="inline-flex rounded-full border border-primary/20 bg-primary/10 px-2.5 py-1 font-semibold text-primary">
                      {processingJob.progress_percent}% complete
                    </span>
                  ) : null}
                </div>
                <div className="mt-2 max-w-2xl text-xs text-muted-foreground">
                  {workflowMessage}
                </div>
              </div>

              <div className="flex items-center gap-2 flex-wrap">
                <button
                  onClick={() => fileInputRef.current?.click()}
                  className="inline-flex items-center gap-2 rounded-xl border border-border bg-card px-4 py-2.5 text-sm font-semibold text-foreground transition hover:bg-accent"
                  type="button"
                >
                  <Upload className="w-4 h-4" />
                  {file ? 'Change File' : 'Choose File'}
                </button>
                <button
                  onClick={triggerProcessFlow}
                  disabled={!file || uploading || previewLoading || !preview || processingActive}
                  className="inline-flex items-center gap-2 rounded-xl px-4 py-2.5 text-sm font-semibold text-primary-foreground transition disabled:opacity-40"
                  style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}
                  type="button"
                >
                  {previewLoading ? 'Preparing Preview…' : uploading ? 'Processing…' : !preview ? 'Select File' : !piiReviewed ? 'Review PII' : 'Process Snapshot'}
                </button>
              </div>
              </div>

              {file && preview && piiReviewed && !processingActive ? (
                <div className="mt-4 rounded-2xl border border-primary/15 bg-primary/5 px-4 py-4">
                  <div className="flex items-start justify-between gap-4 flex-wrap">
                    <div>
                      <div className="text-sm font-bold text-foreground">PII review complete</div>
                      <div className="mt-1 text-sm text-muted-foreground">
                        <span className="font-semibold text-foreground">{file.name}</span> is ready for processing.
                        {dashboardIntent.trim()
                          ? ' SME dashboard context has been captured and will be used in the first proposal.'
                          : ' You can process now or add optional SME dashboard context first.'}
                      </div>
                    </div>
                    <div className="flex items-center gap-2 flex-wrap">
                      <button
                        onClick={() => setShowDashboardIntentModal(true)}
                        className="inline-flex items-center gap-2 rounded-xl border border-border bg-card px-4 py-2.5 text-sm font-semibold text-foreground transition hover:bg-accent"
                        type="button"
                      >
                        {dashboardIntent.trim() ? 'Edit Dashboard Intent' : 'Add Dashboard Intent'}
                      </button>
                    </div>
                  </div>
                </div>
              ) : null}

            <input
              ref={fileInputRef}
              type="file"
              accept=".xlsx"
              className="hidden"
              onChange={(e) => {
                const nextFile = e.target.files?.[0] ?? null
                setFile(nextFile)
                setMsg(null)
                setErr(null)
                setPiiReviewed(false)
                setPreview(null)
                setPiiSelections({})
                setProcessingJob(null)
                setDashboardProposal(null)
                setShowDashboardProposalModal(false)
                setProposalGuidance('')
                setDashboardIntent('')
                setShowDashboardIntentModal(false)
                if (!nextFile) return
                setPreviewLoading(true)
                void (async () => {
                  try {
                    const nextPreview = await api.previewWorkbook(nextFile)
                    setPreview(nextPreview)
                    const suggested: Record<string, PiiType> = {}
                    for (const sheet of nextPreview.sheets) {
                      for (const section of sheet.sections) {
                        for (const header of section.headers) {
                          if (header.suggested_pii_type) {
                            suggested[fieldKey(sheet.sheet_name, section.section_key, header.header_label)] = header.suggested_pii_type
                          }
                        }
                      }
                    }
                    setPiiSelections(suggested)
                    setShowPiiModal(true)
                  } catch (error) {
                    setErr(extractErrorMessage(error, 'Could not preview workbook headers for PII review.'))
                  } finally {
                    setPreviewLoading(false)
                  }
                })()
              }}
            />
          </div>
        </div>
      </section>

      {msg && (
        <div className="bg-success/10 border border-success/30 text-success px-4 py-3 rounded-2xl text-sm font-semibold flex items-center justify-between gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <CheckCircle2 className="w-4 h-4" />
            <span>{msg}</span>
          </div>
          {lastProcessedSnapshotId && (
            <div className="flex items-center gap-2 flex-wrap">
              {proposalRefreshLoading ? (
                <div className="inline-flex items-center gap-2 rounded-full border border-success/25 bg-success/5 px-3 py-1.5 text-xs font-bold text-success">
                  <LoaderCircle className="h-3.5 w-3.5 animate-spin" />
                  Generating dashboard proposal…
                </div>
              ) : (
                <>
                  <button
                    className="inline-flex items-center gap-2 rounded-xl border border-border bg-card px-3 py-2 text-xs font-bold text-foreground transition hover:bg-accent"
                    disabled={proposalApproveLoading || !dashboardProposal}
                    onClick={() => {
                      if (!(proposalApproveLoading) && dashboardProposal) setShowDashboardProposalModal(true)
                    }}
                    type="button"
                  >
                    Review / Refine Widgets
                  </button>
                  <button
                    className="inline-flex items-center gap-2 rounded-xl border border-border bg-card px-3 py-2 text-xs font-bold text-foreground transition hover:bg-accent"
                    disabled={proposalApproveLoading || downloadingSnapshotReport !== null}
                    onClick={async () => {
                      if (proposalApproveLoading) return
                      setErr(null)
                      try {
                        setMsg(`Preparing executive PDF for ${lastProcessedSnapshotLabel ?? 'the latest processed workbook'}...`)
                        setDownloadingSnapshotReport(`${lastProcessedSnapshotId}:executive`)
                        await api.downloadOverallReport(lastProcessedSnapshotId)
                        setMsg(`Executive PDF download started for ${lastProcessedSnapshotLabel ?? 'the latest processed workbook'}.`)
                      } catch {
                        setErr('Executive report download failed. Ensure the backend is running and the snapshot still exists.')
                      } finally {
                        setDownloadingSnapshotReport(null)
                      }
                    }}
                    type="button"
                  >
                    <FileDown className="w-4 h-4" />
                    {downloadingSnapshotReport === `${lastProcessedSnapshotId}:executive` ? 'Preparing PDF…' : 'Executive PDF'}
                  </button>
                  <button
                    className="inline-flex items-center gap-2 rounded-xl border border-border bg-card px-3 py-2 text-xs font-bold text-foreground transition hover:bg-accent"
                    disabled={proposalApproveLoading || downloadingSnapshotReport !== null}
                    onClick={async () => {
                      if (proposalApproveLoading) return
                      setErr(null)
                      try {
                        setMsg(`Preparing analytics PDF for ${lastProcessedSnapshotLabel ?? 'the latest processed workbook'}...`)
                        setDownloadingSnapshotReport(`${lastProcessedSnapshotId}:analytics`)
                        await api.downloadAnalyticsReport(lastProcessedSnapshotId)
                        setMsg(`Analytics PDF download started for ${lastProcessedSnapshotLabel ?? 'the latest processed workbook'}.`)
                      } catch {
                        setErr('Analytics report download failed. Ensure the backend is running and the snapshot still exists.')
                      } finally {
                        setDownloadingSnapshotReport(null)
                      }
                    }}
                    type="button"
                  >
                    <FileDown className="w-4 h-4" />
                    {downloadingSnapshotReport === `${lastProcessedSnapshotId}:analytics` ? 'Preparing PDF…' : 'Analytics PDF'}
                  </button>
                </>
              )}
            </div>
          )}
        </div>
      )}
      {lastConfirmedAnalysisSet ? (
        <div className="bg-primary/5 border border-primary/20 text-foreground px-4 py-4 rounded-2xl text-sm">
          <div className="flex items-start justify-between gap-4 flex-wrap">
            <div>
              <div className="text-sm font-bold text-foreground">Combined analysis ready: {lastConfirmedAnalysisSet.name}</div>
              <div className="mt-1 text-sm text-muted-foreground">Open the merged dashboard or download the combined executive and analytics summaries generated from the confirmed merge logic.</div>
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <button
                className="inline-flex items-center gap-2 rounded-xl border border-border bg-card px-3 py-2 text-xs font-bold text-foreground transition hover:bg-accent"
                onClick={() => nav(`/dashboard?analysis_set_id=${lastConfirmedAnalysisSet.id}`)}
                type="button"
              >
                Open Combined Dashboard
              </button>
              <button
                className="inline-flex items-center gap-2 rounded-xl border border-border bg-card px-3 py-2 text-xs font-bold text-foreground transition hover:bg-accent"
                disabled={downloadingAnalysisSetReport !== null}
                onClick={async () => {
                  try {
                    setDownloadingAnalysisSetReport(`${lastConfirmedAnalysisSet.id}:executive`)
                    await api.downloadCombinedExecutiveReport(lastConfirmedAnalysisSet.id)
                  } catch {
                    setErr('Combined executive report download failed.')
                  } finally {
                    setDownloadingAnalysisSetReport(null)
                  }
                }}
                type="button"
              >
                <FileDown className="w-4 h-4" />
                {downloadingAnalysisSetReport === `${lastConfirmedAnalysisSet.id}:executive` ? 'Preparing…' : 'Combined Executive PDF'}
              </button>
              <button
                className="inline-flex items-center gap-2 rounded-xl border border-border bg-card px-3 py-2 text-xs font-bold text-foreground transition hover:bg-accent"
                disabled={downloadingAnalysisSetReport !== null}
                onClick={async () => {
                  try {
                    setDownloadingAnalysisSetReport(`${lastConfirmedAnalysisSet.id}:analytics`)
                    await api.downloadCombinedAnalyticsReport(lastConfirmedAnalysisSet.id)
                  } catch {
                    setErr('Combined analytics report download failed.')
                  } finally {
                    setDownloadingAnalysisSetReport(null)
                  }
                }}
                type="button"
              >
                <FileDown className="w-4 h-4" />
                {downloadingAnalysisSetReport === `${lastConfirmedAnalysisSet.id}:analytics` ? 'Preparing…' : 'Combined Analytics PDF'}
              </button>
            </div>
          </div>
        </div>
      ) : null}
      {err && <div className="bg-destructive/10 border border-destructive/30 text-destructive px-4 py-3 rounded-2xl text-sm font-semibold">{err}</div>}

      <section className="bg-card border border-border rounded-2xl shadow-soft overflow-hidden">
        <div className="flex items-end justify-between gap-4 p-5 border-b border-border">
          <div>
            <h2 className="text-lg font-bold text-foreground">Snapshots</h2>
            <p className="text-muted-foreground text-sm mt-0.5">Processed workbooks and generated dashboard snapshots.</p>
          </div>
          <div className="flex items-center gap-3 flex-wrap">
            {selectedSnapshotIds.length >= 2 ? (
              <button
                className="inline-flex items-center gap-2 rounded-xl px-4 py-2.5 text-sm font-semibold text-primary-foreground transition disabled:opacity-40"
                disabled={analysisSetLoading}
                onClick={() => void openCombineAnalysisModal()}
                style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}
                type="button"
              >
                {analysisSetLoading ? 'Analyzing Similarity…' : `Combine Analysis (${selectedSnapshotIds.length})`}
              </button>
            ) : null}
            <span className="text-muted-foreground font-bold text-sm">{rows.length} items</span>
          </div>
        </div>
        {selectedSnapshotIds.length >= 2 ? (
          <div className="border-b border-border bg-primary/5 px-5 py-3">
            <div className="flex items-start justify-between gap-3 flex-wrap">
              <div>
                <div className="text-sm font-semibold text-foreground">{selectedSnapshotIds.length} documents selected for combined analysis</div>
                <div className="mt-1 text-xs text-muted-foreground">The similarity agent will compare semantics, shared headers, likely join keys, and period ordering before you confirm the merge logic.</div>
              </div>
              <button
                className="rounded-xl border border-border bg-card px-3 py-2 text-xs font-semibold text-foreground transition hover:bg-accent"
                onClick={() => setSelectedSnapshotIds([])}
                type="button"
              >
                Clear Selection
              </button>
            </div>
          </div>
        ) : null}
        <div className="overflow-auto">
          <table className="w-full border-collapse">
            <thead>
              <tr>
                {['Select', 'File name', 'Format', 'Type', 'Status', 'Actions'].map((h) => (
                  <th key={h} className="px-4 py-3 bg-secondary text-muted-foreground text-[0.68rem] tracking-widest uppercase text-left border-b border-border">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr><td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">No documents yet. Upload a workbook to get started.</td></tr>
              ) : rows.map((row) => {
                const pill = pillStyle(row.status)
                return (
                  <tr key={row.id} className="hover:bg-accent/30 transition">
                    <td className="px-4 py-4 border-b border-border/50 align-top">
                      {row.snapshotId ? (
                        <input
                          checked={selectedSnapshotIds.includes(row.snapshotId)}
                          className="mt-1 h-4 w-4 accent-primary"
                          onChange={() => toggleSnapshotSelection(row.snapshotId!)}
                          type="checkbox"
                        />
                      ) : (
                        <span className="text-xs text-muted-foreground">—</span>
                      )}
                    </td>
                    <td className="px-4 py-4 border-b border-border/50">
                      <div className="flex items-center gap-3">
                        <div className="grid place-items-center w-10 h-10 rounded-xl bg-warning/10 text-warning"><FileSpreadsheet className="w-5 h-5" /></div>
                        <div>
                          <div className="font-bold text-foreground text-sm">{row.fileName}</div>
                          <div className="text-xs text-muted-foreground mt-0.5">{row.subtitle}</div>
                        </div>
                      </div>
                    </td>
                    <td className="px-4 py-4 text-sm text-muted-foreground border-b border-border/50">{row.format}</td>
                    <td className="px-4 py-4 text-sm text-muted-foreground border-b border-border/50">{row.type}</td>
                    <td className="px-4 py-4 border-b border-border/50">
                      <span className="inline-flex items-center gap-2 px-3 py-1 rounded-full text-xs font-bold border" style={{ background: pill.bg, color: pill.text, borderColor: 'transparent' }}>
                        <span className="w-2 h-2 rounded-full" style={{ background: pill.dot }} />
                        {row.status}
                      </span>
                    </td>
                    <td className="px-4 py-4 border-b border-border/50">
                      <div className="flex items-center gap-2">
                        {row.id === 'selected' && (
                          <span className="px-3 py-1.5 rounded-lg bg-primary/10 text-primary font-bold text-xs">
                            {previewLoading ? 'Preparing Preview…' : processingActive && processingJob ? `${processingJob.progress_percent}% complete` : proposalRefreshLoading ? 'Generating Proposal…' : !preview ? 'Waiting for Preview' : !piiReviewed ? 'PII Review Pending' : 'Ready to Process'}
                          </span>
                        )}
                        <button
                          onClick={() => {
                            if (!row.snapshotId) return
                            setSnapshotId(row.snapshotId)
                            nav(`/dashboard?snapshot_id=${row.snapshotId}`)
                          }}
                          disabled={!row.snapshotId}
                          className="grid place-items-center w-9 h-9 rounded-lg bg-card border border-border text-muted-foreground hover:text-foreground disabled:opacity-40 transition">
                          <Eye className="w-4 h-4" />
                        </button>
                        {row.snapshotId && (
                          <button
                            onClick={async () => {
                              if (proposalRefreshLoading || proposalApproveLoading) return
                              setMsg(`Preparing executive PDF for ${row.fileName}...`)
                              setErr(null)
                              try {
                                setDownloadingSnapshotReport(`${row.snapshotId!}:executive`)
                                await api.downloadOverallReport(row.snapshotId!)
                                setMsg(`Executive PDF download started for ${row.fileName}.`)
                              } catch {
                                setErr('Executive report download failed. Ensure the backend is running and the snapshot still exists.')
                              } finally {
                                setDownloadingSnapshotReport(null)
                              }
                            }}
                            className="inline-flex items-center gap-2 rounded-lg bg-card border border-border px-3 py-2 text-xs font-bold text-muted-foreground hover:text-foreground transition"
                            disabled={proposalRefreshLoading || proposalApproveLoading || downloadingSnapshotReport !== null}
                            title="Download executive PDF"
                            type="button"
                          >
                            <FileDown className="w-4 h-4" />
                            {downloadingSnapshotReport === `${row.snapshotId}:executive` ? 'Preparing…' : 'Executive PDF'}
                          </button>
                        )}
                        {row.snapshotId && (
                          <button
                            onClick={async () => {
                              if (proposalRefreshLoading || proposalApproveLoading) return
                              setMsg(`Preparing analytics PDF for ${row.fileName}...`)
                              setErr(null)
                              try {
                                setDownloadingSnapshotReport(`${row.snapshotId!}:analytics`)
                                await api.downloadAnalyticsReport(row.snapshotId!)
                                setMsg(`Analytics PDF download started for ${row.fileName}.`)
                              } catch {
                                setErr('Analytics report download failed. Ensure the backend is running and the snapshot still exists.')
                              } finally {
                                setDownloadingSnapshotReport(null)
                              }
                            }}
                            className="inline-flex items-center gap-2 rounded-lg bg-card border border-border px-3 py-2 text-xs font-bold text-muted-foreground hover:text-foreground transition"
                            disabled={proposalRefreshLoading || proposalApproveLoading || downloadingSnapshotReport !== null}
                            title="Download analytics PDF"
                            type="button"
                          >
                            <FileDown className="w-4 h-4" />
                            {downloadingSnapshotReport === `${row.snapshotId}:analytics` ? 'Preparing…' : 'Analytics PDF'}
                          </button>
                        )}
                        {row.snapshotId && (
                          <button onClick={async () => { await api.deleteSnapshot(row.snapshotId!); await refreshSnapshots() }}
                            className="grid place-items-center w-9 h-9 rounded-lg bg-card border border-border text-destructive hover:bg-destructive/10 transition">
                            <Trash2 className="w-4 h-4" />
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </section>

      <section className="bg-card border border-border rounded-2xl shadow-soft overflow-hidden">
        <div className="flex items-end justify-between gap-4 p-5 border-b border-border">
          <div>
            <h2 className="text-lg font-bold text-foreground">Combined Analyses</h2>
            <p className="text-muted-foreground text-sm mt-0.5">Persisted multi-document analysis sets confirmed through the HITL merge-review flow.</p>
          </div>
          <span className="text-muted-foreground font-bold text-sm">{analysisSets.length} items</span>
        </div>
        <div className="overflow-auto">
          <table className="w-full border-collapse">
            <thead>
              <tr>
                {['Name', 'Relationship', 'Status', 'Members', 'Actions'].map((h) => (
                  <th key={h} className="px-4 py-3 bg-secondary text-muted-foreground text-[0.68rem] tracking-widest uppercase text-left border-b border-border">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {analysisSets.length === 0 ? (
                <tr><td colSpan={5} className="px-4 py-8 text-center text-muted-foreground">No combined analyses yet. Select at least two processed snapshots and use Combine Analysis.</td></tr>
              ) : analysisSets.map((item) => (
                <tr key={item.id} className="hover:bg-accent/30 transition">
                  <td className="px-4 py-4 border-b border-border/50">
                    <div className="font-bold text-foreground text-sm">{item.name}</div>
                    <div className="mt-0.5 text-xs text-muted-foreground">{item.summary}</div>
                  </td>
                  <td className="px-4 py-4 border-b border-border/50 text-sm text-muted-foreground">{titleCaseWords(item.relationship_type)}</td>
                  <td className="px-4 py-4 border-b border-border/50">
                    <span className="inline-flex rounded-full bg-secondary px-3 py-1 text-xs font-bold text-muted-foreground">{titleCaseWords(item.status)}</span>
                  </td>
                  <td className="px-4 py-4 border-b border-border/50 text-sm text-muted-foreground">{item.members.length}</td>
                  <td className="px-4 py-4 border-b border-border/50">
                    <div className="flex items-center gap-2 flex-wrap">
                      <button
                        className="inline-flex items-center gap-2 rounded-lg bg-card border border-border px-3 py-2 text-xs font-bold text-muted-foreground hover:text-foreground transition"
                        onClick={() => nav(`/dashboard?analysis_set_id=${item.id}`)}
                        type="button"
                      >
                        <Eye className="w-4 h-4" />
                        Dashboard
                      </button>
                      <button
                        className="inline-flex items-center gap-2 rounded-lg bg-card border border-border px-3 py-2 text-xs font-bold text-muted-foreground hover:text-foreground transition"
                        disabled={downloadingAnalysisSetReport !== null}
                        onClick={async () => {
                          try {
                            setDownloadingAnalysisSetReport(`${item.id}:executive`)
                            await api.downloadCombinedExecutiveReport(item.id)
                          } catch {
                            setErr('Combined executive report download failed.')
                          } finally {
                            setDownloadingAnalysisSetReport(null)
                          }
                        }}
                        type="button"
                      >
                        <FileDown className="w-4 h-4" />
                        {downloadingAnalysisSetReport === `${item.id}:executive` ? 'Preparing…' : 'Executive PDF'}
                      </button>
                      <button
                        className="inline-flex items-center gap-2 rounded-lg bg-card border border-border px-3 py-2 text-xs font-bold text-muted-foreground hover:text-foreground transition"
                        disabled={downloadingAnalysisSetReport !== null}
                        onClick={async () => {
                          try {
                            setDownloadingAnalysisSetReport(`${item.id}:analytics`)
                            await api.downloadCombinedAnalyticsReport(item.id)
                          } catch {
                            setErr('Combined analytics report download failed.')
                          } finally {
                            setDownloadingAnalysisSetReport(null)
                          }
                        }}
                        type="button"
                      >
                        <FileDown className="w-4 h-4" />
                        {downloadingAnalysisSetReport === `${item.id}:analytics` ? 'Preparing…' : 'Analytics PDF'}
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {showPiiModal && preview && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-foreground/15 backdrop-blur-sm p-4">
          <div className="w-full max-w-5xl max-h-[88vh] overflow-hidden rounded-3xl border border-border bg-card shadow-[0_28px_80px_rgba(15,23,42,0.16)]">
            <div className="flex items-start justify-between gap-4 border-b border-border px-6 py-5">
              <div>
                <h2 className="text-lg font-black text-foreground">Review PII Before Processing</h2>
                <p className="mt-1 text-sm text-muted-foreground">Select the fields that contain PII. Those values will be masked before any LLM call and restored afterward in the dashboard and reports.</p>
              </div>
              <button
                className="grid h-9 w-9 place-items-center rounded-full bg-secondary text-muted-foreground transition hover:text-foreground"
                onClick={() => setShowPiiModal(false)}
                type="button"
              >
                ×
              </button>
            </div>

            <div className="max-h-[62vh] overflow-auto px-6 py-5">
              <div className="flex flex-col gap-6">
                {preview.sheets.map((sheet) => (
                  <section key={sheet.sheet_name} className="rounded-2xl border border-border bg-secondary/20 p-4">
                    <div className="mb-4">
                      <h3 className="text-sm font-bold text-foreground">{sheet.sheet_name}</h3>
                      <p className="mt-1 text-xs text-muted-foreground">Review fields section by section and mark any PII columns that should be masked before LLM processing.</p>
                    </div>
                    <div className="flex flex-col gap-4">
                      {sheet.sections.map((section) => (
                        <div key={`${sheet.sheet_name}-${section.section_key}`} className="rounded-2xl border border-border bg-card overflow-hidden">
                          <div className="flex items-center justify-between gap-4 border-b border-border px-4 py-3">
                            <div>
                              <div className="text-sm font-bold text-foreground">{section.section_label}</div>
                              <div className="text-xs text-muted-foreground">Header row {section.header_row}</div>
                            </div>
                            <div className="text-xs text-muted-foreground">{section.headers.length} fields</div>
                          </div>
                          <div className="overflow-auto">
                            <table className="w-full border-collapse">
                              <thead>
                                <tr>
                                  {['Mask', 'Column', 'Field', 'Sample value', 'PII type'].map((heading) => (
                                    <th key={heading} className="px-4 py-3 text-left text-[0.68rem] font-bold uppercase tracking-widest text-muted-foreground bg-secondary border-b border-border">
                                      {heading}
                                    </th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {section.headers.map((header) => {
                                  const key = fieldKey(sheet.sheet_name, section.section_key, header.header_label)
                                  const checked = Boolean(piiSelections[key])
                                  return (
                                    <tr key={key} className="border-b border-border/50 last:border-0">
                                      <td className="px-4 py-3 align-top">
                                        <input
                                          checked={checked}
                                          className="mt-1 h-4 w-4 accent-primary"
                                          onChange={(event) => {
                                            setPiiSelections((current) => {
                                              const next = { ...current }
                                              if (event.target.checked) {
                                                next[key] = header.suggested_pii_type ?? 'custom'
                                              } else {
                                                delete next[key]
                                              }
                                              return next
                                            })
                                          }}
                                          type="checkbox"
                                        />
                                      </td>
                                      <td className="px-4 py-3 text-sm text-muted-foreground align-top">{header.column}</td>
                                      <td className="px-4 py-3 align-top">
                                        <div className="text-sm font-semibold text-foreground">{header.header_label}</div>
                                        {header.suggested_pii_type && <div className="mt-1 text-xs text-primary">Suggested: {header.suggested_pii_type}</div>}
                                      </td>
                                      <td className="px-4 py-3 text-sm text-muted-foreground align-top">{header.sample_value || '—'}</td>
                                      <td className="px-4 py-3 align-top">
                                        <select
                                          className="rounded-lg border border-border bg-card px-3 py-2 text-sm text-foreground disabled:opacity-50"
                                          disabled={!checked}
                                          onChange={(event) => {
                                            const nextValue = event.target.value as PiiType
                                            setPiiSelections((current) => ({ ...current, [key]: nextValue }))
                                          }}
                                          value={piiSelections[key] ?? (header.suggested_pii_type ?? 'custom')}
                                        >
                                          {piiTypeOptions.map((option) => (
                                            <option key={option.value} value={option.value}>{option.label}</option>
                                          ))}
                                        </select>
                                      </td>
                                    </tr>
                                  )
                                })}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      ))}
                    </div>
                  </section>
                ))}
              </div>
            </div>

            <div className="flex items-center justify-between gap-3 border-t border-border px-6 py-4">
              <div className="text-sm text-muted-foreground">
                {Object.keys(piiSelections).length} field{Object.keys(piiSelections).length === 1 ? '' : 's'} marked as PII
              </div>
              <div className="flex items-center gap-2">
                <button
                  className="rounded-xl border border-border bg-card px-4 py-2.5 text-sm font-semibold text-foreground transition hover:bg-accent"
                  onClick={() => savePiiReview()}
                  type="button"
                >
                  Save Selection
                </button>
                <button
                  className="rounded-xl px-4 py-2.5 text-sm font-semibold text-primary-foreground transition"
                  onClick={() => savePiiReview({ openDashboardIntent: true })}
                  style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}
                  type="button"
                >
                  Save + Add Intent
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {showDashboardIntentModal && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-foreground/15 backdrop-blur-sm p-4">
          <div className="w-full max-w-3xl rounded-3xl border border-border bg-card shadow-[0_28px_80px_rgba(15,23,42,0.16)]">
            <div className="flex items-start justify-between gap-4 border-b border-border px-6 py-5">
              <div>
                <h2 className="text-lg font-black text-foreground">Dashboard Intent</h2>
                <p className="mt-1 text-sm text-muted-foreground">This is an optional pre-processing SME note. It is used once during the first semantic interpretation and proposal generation. You can still refine the proposal later with additional comments.</p>
              </div>
              <button
                className="grid h-9 w-9 place-items-center rounded-full bg-secondary text-muted-foreground transition hover:text-foreground"
                onClick={() => setShowDashboardIntentModal(false)}
                type="button"
              >
                ×
              </button>
            </div>
            <div className="px-6 py-5">
              <textarea
                className="min-h-[140px] w-full rounded-2xl border border-border bg-secondary/20 px-4 py-4 text-sm text-foreground outline-none transition focus:border-primary/40 focus:bg-card"
                onChange={(event) => setDashboardIntent(event.target.value)}
                placeholder="Example: This workbook is for ODR portfolio monitoring. Show trend by quarter, compare TC vs BC, highlight top deteriorating pools, and use the Questions sheet to drive the dashboard."
                value={dashboardIntent}
              />
            </div>
            <div className="flex items-center justify-between gap-3 border-t border-border px-6 py-4">
              <div className="text-sm text-muted-foreground">
                This note will be used in the first semantic interpretation and dashboard proposal only.
              </div>
              <div className="flex items-center gap-2">
                <button
                  className="rounded-xl border border-border bg-card px-4 py-2.5 text-sm font-semibold text-foreground transition hover:bg-accent"
                  onClick={() => {
                    setShowDashboardIntentModal(false)
                    void handleProcess()
                  }}
                  type="button"
                >
                  Skip For Now
                </button>
                <button
                  className="rounded-xl px-4 py-2.5 text-sm font-semibold text-primary-foreground transition"
                  onClick={() => {
                    setShowDashboardIntentModal(false)
                    void handleProcess()
                  }}
                  style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}
                  type="button"
                >
                  Generate Dashboard
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {showDashboardProposalModal && dashboardProposal && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-foreground/15 backdrop-blur-sm p-4">
          <div className="w-full max-w-5xl max-h-[88vh] overflow-hidden rounded-3xl border border-border bg-card shadow-[0_28px_80px_rgba(15,23,42,0.16)]">
            <div className="flex items-start justify-between gap-4 border-b border-border px-6 py-5">
              <div>
                <h2 className="text-lg font-black text-foreground">{dashboardProposal.proposal.title}</h2>
                <p className="mt-1 text-sm text-muted-foreground">{dashboardProposal.summary}</p>
                <div className="mt-2 flex flex-wrap gap-2 text-xs">
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Family: {dashboardProposal.proposal.dashboard_family}</span>
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Layout: {dashboardProposal.proposal.layout_template}</span>
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Confidence: {(dashboardProposal.confidence_score * 100).toFixed(0)}%</span>
                </div>
              </div>
              <button
                className="grid h-9 w-9 place-items-center rounded-full bg-secondary text-muted-foreground transition hover:text-foreground disabled:opacity-40"
                disabled={proposalRefreshLoading || proposalApproveLoading}
                onClick={() => setShowDashboardProposalModal(false)}
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
                <ProposalSection
                  title="Start Here: Add your widget ideas"
                  sectionKey="guidance"
                  expanded={proposalSectionState.guidance}
                  onToggle={toggleProposalSection}
                >
                  <div className="rounded-2xl border border-primary/15 bg-[linear-gradient(180deg,hsla(262,100%,99%,0.94),hsla(268,80%,96%,0.9))] p-4">
                    <p className="text-sm font-semibold text-foreground">Use this box before approval.</p>
                    <p className="mt-1 text-sm text-muted-foreground">Shape the generated widgets here. Mention the business question, preferred dimensions, KPIs, and exact chart types you want to see.</p>
                  </div>
                  <textarea
                    className="mt-3 min-h-[132px] w-full rounded-2xl border border-primary/15 bg-card px-4 py-3 text-sm text-foreground outline-none transition focus:border-primary/40 focus:bg-card"
                    onChange={(event) => setProposalGuidance(event.target.value)}
                    placeholder="Example: This workbook should become a collections dashboard. Focus on top 5 towers by dues, penalty exposure, quarter movement, and owner concentration."
                    value={proposalGuidance}
                  />
                  <div className="mt-3 flex flex-wrap gap-2 text-xs text-muted-foreground">
                    <span className="inline-flex rounded-full border border-border bg-card px-2.5 py-1">Name the KPI</span>
                    <span className="inline-flex rounded-full border border-border bg-card px-2.5 py-1">Specify chart type</span>
                    <span className="inline-flex rounded-full border border-border bg-card px-2.5 py-1">Ask for ranking or trend</span>
                  </div>
                </ProposalSection>

                {dashboardProposal.refinement_result && dashboardProposal.refinement_result.status !== 'not_requested' ? (
                  <ProposalSection
                    title="Refinement result"
                    sectionKey="refinement"
                    expanded={proposalSectionState.refinement}
                    onToggle={toggleProposalSection}
                  >
                    <div className={`rounded-2xl border px-4 py-3 ${dashboardProposal.refinement_result.status === 'rejected' ? 'border-warning/30 bg-warning/10' : 'border-success/30 bg-success/10'}`}>
                      <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Status</div>
                      <div className="mt-1 text-sm font-semibold text-foreground">{dashboardProposal.refinement_result.status.replace('_', ' ')}</div>
                      <div className="mt-2 text-sm text-muted-foreground">{dashboardProposal.refinement_result.message}</div>
                    </div>
                    {dashboardProposal.refinement_result.accepted_requests.length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Accepted</div>
                        <div className="mt-2 grid gap-2">
                          {dashboardProposal.refinement_result.accepted_requests.map((item) => (
                            <div key={item} className="rounded-xl border border-success/20 bg-success/5 px-3 py-2 text-sm text-foreground">{item}</div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {dashboardProposal.refinement_result.unsupported_requests.length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Unsupported or missing</div>
                        <div className="mt-2 grid gap-2">
                          {dashboardProposal.refinement_result.unsupported_requests.map((item) => (
                            <div key={item} className="rounded-xl border border-warning/20 bg-warning/5 px-3 py-2 text-sm text-foreground">{item}</div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {dashboardProposal.refinement_result.warnings.length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Warnings</div>
                        <div className="mt-2 grid gap-2">
                          {dashboardProposal.refinement_result.warnings.map((item) => (
                            <div key={item} className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">{item}</div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <div className="rounded-xl border border-border bg-secondary/20 p-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Diff summary</div>
                        <div className="mt-2 text-sm text-muted-foreground">
                          Added tabs: {dashboardProposal.refinement_result.diff.added_tabs.length ? dashboardProposal.refinement_result.diff.added_tabs.join(', ') : 'none'}
                        </div>
                        <div className="mt-1 text-sm text-muted-foreground">
                          Added widgets/sections: {dashboardProposal.refinement_result.diff.added_section_count}
                        </div>
                        <div className="mt-1 text-sm text-muted-foreground">
                          Title changed: {dashboardProposal.refinement_result.diff.changed_title ? 'yes' : 'no'}
                        </div>
                        <div className="mt-1 text-sm text-muted-foreground">
                          Summary changed: {dashboardProposal.refinement_result.diff.changed_summary ? 'yes' : 'no'}
                        </div>
                      </div>
                      <div className="rounded-xl border border-border bg-secondary/20 p-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Chart request outcome</div>
                        <div className="mt-2 text-sm text-muted-foreground">
                          Accepted chart types: {dashboardProposal.refinement_result.diff.accepted_chart_types.length ? dashboardProposal.refinement_result.diff.accepted_chart_types.join(', ') : 'none'}
                        </div>
                        <div className="mt-1 text-sm text-muted-foreground">
                          Missing chart types: {dashboardProposal.refinement_result.diff.missing_chart_types.length ? dashboardProposal.refinement_result.diff.missing_chart_types.join(', ') : 'none'}
                        </div>
                      </div>
                    </div>
                  </ProposalSection>
                ) : null}

                <ProposalSection
                  title="Proposal rationale"
                  sectionKey="rationale"
                  expanded={proposalSectionState.rationale}
                  onToggle={toggleProposalSection}
                >
                  <p className="text-sm text-muted-foreground">{dashboardProposal.rationale}</p>
                </ProposalSection>

                {(dashboardProposal.proposal.semantic_summary || dashboardProposal.proposal.semantic_details || (dashboardProposal.proposal.ambiguities?.length ?? 0) > 0 || (dashboardProposal.proposal.business_questions?.length ?? 0) > 0) && (
                  <ProposalSection
                    title="Semantic interpretation"
                    sectionKey="semantics"
                    expanded={proposalSectionState.semantics}
                    onToggle={toggleProposalSection}
                  >
                    <div className="grid gap-3 md:grid-cols-2">
                      <div className="rounded-xl border border-border bg-secondary/20 p-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Summary</div>
                        <p className="mt-2 text-sm text-muted-foreground">{semanticSummaryText(dashboardProposal.proposal)}</p>
                      </div>
                      <div className="rounded-xl border border-border bg-secondary/20 p-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Business domain</div>
                        <div className="mt-2 text-sm font-semibold text-foreground">
                          {String(semanticDetailsFromConfig(dashboardProposal.proposal)?.business_domain ?? semanticDetailsFromConfig(dashboardProposal.proposal)?.dominant_domain ?? dashboardProposal.proposal.dashboard_family ?? 'Adaptive workbook')}
                        </div>
                      </div>
                    </div>
                    {semanticObjectNames(semanticDetailsFromConfig(dashboardProposal.proposal)?.entities, ['entity_name', 'name']).length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Entities</div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {semanticObjectNames(semanticDetailsFromConfig(dashboardProposal.proposal)?.entities, ['entity_name', 'name']).map((item) => (
                            <span key={item} className="inline-flex rounded-full border border-border bg-card px-2.5 py-1 text-xs text-muted-foreground">{item}</span>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {semanticObjectNames(semanticDetailsFromConfig(dashboardProposal.proposal)?.dimensions, ['dimension_name', 'name']).length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Dimensions</div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {semanticObjectNames(semanticDetailsFromConfig(dashboardProposal.proposal)?.dimensions, ['dimension_name', 'name']).map((item) => (
                            <span key={item} className="inline-flex rounded-full border border-border bg-card px-2.5 py-1 text-xs text-muted-foreground">{item}</span>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {semanticObjectNames(semanticDetailsFromConfig(dashboardProposal.proposal)?.measures, ['measure_name', 'name']).length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Measures</div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {semanticObjectNames(semanticDetailsFromConfig(dashboardProposal.proposal)?.measures, ['measure_name', 'name']).map((item) => (
                            <span key={item} className="inline-flex rounded-full border border-border bg-card px-2.5 py-1 text-xs text-muted-foreground">{item}</span>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {dashboardProposal.proposal.ambiguities?.length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Open ambiguities</div>
                        <div className="mt-2 grid gap-2">
                          {dashboardProposal.proposal.ambiguities.map((item) => (
                            <div key={item} className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">{item}</div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {dashboardProposal.proposal.business_questions?.length ? (
                      <div className="mt-3">
                        <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Business questions</div>
                        <div className="mt-2 grid gap-3">
                          {dashboardProposal.proposal.business_questions.map((item) => (
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

                {(dashboardProposal.proposal.eda_plan ?? dashboardProposal.proposal.investigation_plan)?.length ? (
                  <ProposalSection
                    title="EDA plan"
                    sectionKey="edaPlan"
                    expanded={proposalSectionState.edaPlan}
                    onToggle={toggleProposalSection}
                  >
                    {(dashboardProposal.proposal.orchestrator_workflow ?? dashboardProposal.proposal.proposal_workflow) ? (
                      <div className="mb-3 text-[0.68rem] font-bold uppercase tracking-widest text-muted-foreground">
                        {dashboardProposal.proposal.orchestrator_workflow ?? dashboardProposal.proposal.proposal_workflow}
                      </div>
                    ) : null}
                    <div className="grid gap-3">
                      {(dashboardProposal.proposal.eda_plan ?? dashboardProposal.proposal.investigation_plan ?? []).map((step) => (
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

                {(dashboardProposal.proposal.eda_evidence ?? dashboardProposal.proposal.investigation_evidence)?.length ? (
                  <ProposalSection
                    title="EDA evidence"
                    sectionKey="edaEvidence"
                    expanded={proposalSectionState.edaEvidence}
                    onToggle={toggleProposalSection}
                  >
                    <div className="grid gap-3">
                      {(dashboardProposal.proposal.eda_evidence ?? dashboardProposal.proposal.investigation_evidence ?? []).map((item) => (
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

                <ProposalSection
                  title="Layout and widgets"
                  sectionKey="layout"
                  expanded={proposalSectionState.layout}
                  onToggle={toggleProposalSection}
                >
                  {dashboardProposal.proposal.customization_prompts.length > 0 ? (
                    <div className="mb-4">
                      <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Suggested business customization questions</div>
                      <div className="mt-2 grid gap-2">
                        {dashboardProposal.proposal.customization_prompts.map((item) => (
                          <div key={item} className="rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-muted-foreground">{item}</div>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  <div className="grid gap-4">
                    {dashboardProposal.proposal.tabs.map((tab) => (
                      <section key={tab.key} className="rounded-2xl border border-border bg-secondary/10 overflow-hidden">
                        <div className="border-b border-border px-4 py-3">
                          <div className="text-sm font-bold text-foreground">{tab.label}</div>
                          <div className="text-xs text-muted-foreground mt-1">{tab.description}</div>
                        </div>
                        <div className="px-4 py-4 grid gap-3">
                          {tab.sections.map((section) => (
                            <div key={section.key} className="rounded-xl border border-border bg-card p-3">
                              <div className="flex items-center justify-between gap-3">
                                <div className="text-sm font-semibold text-foreground">{section.label}</div>
                                <span className="text-[0.68rem] font-bold uppercase tracking-widest text-muted-foreground">{section.slot ?? section.renderer}</span>
                              </div>
                              <div className="text-xs text-muted-foreground mt-1">{section.description}</div>
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
                {currentDraftProposalInput !== lastAppliedProposalInput
                  ? 'Apply the current widget changes before approval, or approve directly to regenerate the proposal with the latest request.'
                  : proposalApproveBlocked
                  ? 'Approval is blocked because the latest widget refinement request was rejected. Update or clear the request first.'
                  : 'Approving this will generate the dashboard view and save the blueprint for matching uploads.'}
              </div>
              <div className="flex items-center gap-2">
                <button
                  className="rounded-xl border border-border bg-card px-4 py-2.5 text-sm font-semibold text-foreground transition hover:bg-accent"
                  onClick={async () => {
                    setProposalRefreshLoading(true)
                    setErr(null)
                    setProposalStatusNote(null)
                    setProposalStatusTone('success')
                    const proposalInput = buildProposalInput(proposalGuidance, proposalQuestionAnswers)
                    const previousProposal = dashboardProposal
                    try {
                      const refreshed = await api.proposeDashboardBlueprint(
                        dashboardProposal.snapshot_id,
                        proposalInput,
                      )
                      setDashboardProposal(refreshed)
                      setLastAppliedProposalInput(proposalInput)
                      const backendResult = refreshed.refinement_result
                      if (backendResult && backendResult.status !== 'not_requested') {
                        setProposalStatusTone(backendResult.status === 'rejected' ? 'warning' : 'success')
                        setProposalStatusNote(backendResult.message)
                      } else {
                        const summary = summarizeProposalRefresh(previousProposal, refreshed, proposalInput)
                        setProposalStatusTone(summary.tone)
                        setProposalStatusNote(summary.message)
                      }
                    } catch {
                      setErr('Could not refine the dashboard proposal.')
                    } finally {
                      setProposalRefreshLoading(false)
                    }
                  }}
                  disabled={proposalRefreshLoading || proposalApproveLoading}
                  type="button"
                >
                  {proposalRefreshLoading ? 'Applying…' : proposalGuidance.trim() ? 'Apply Widget Ideas' : 'Refresh Proposal'}
                </button>
                <button
                  className="rounded-xl px-4 py-2.5 text-sm font-semibold text-primary-foreground transition disabled:opacity-40"
                  disabled={proposalRefreshLoading || proposalApproveLoading || proposalApproveBlocked}
                  onClick={async () => {
                    if (!dashboardProposal) return
                    setProposalApproveLoading(true)
                    setErr(null)
                    setProposalStatusNote(null)
                    setProposalStatusTone('success')
                    try {
                      const hasQuestionAnswers = Object.values(proposalQuestionAnswers).some((value) => value.trim())
                      const currentProposalInput = buildProposalInput(proposalGuidance, proposalQuestionAnswers)
                      const needsFreshProposal = dashboardProposal.id <= 0 || currentProposalInput !== lastAppliedProposalInput || (!lastAppliedProposalInput && (hasQuestionAnswers || proposalGuidance.trim()))
                      const proposalToApprove = needsFreshProposal
                        ? await api.proposeDashboardBlueprint(
                            dashboardProposal.snapshot_id,
                            currentProposalInput,
                          )
                        : dashboardProposal
                      if (proposalToApprove.refinement_result?.status === 'rejected') {
                        setDashboardProposal(proposalToApprove)
                        setProposalStatusTone('warning')
                        setProposalStatusNote(proposalToApprove.refinement_result.message)
                        setErr('The latest widget request was rejected. Update the request or clear it before approval.')
                        return
                      }
                      setDashboardProposal(proposalToApprove)
                      setLastAppliedProposalInput(currentProposalInput)
                      await api.approveDashboardProposal(proposalToApprove.id)
                      setSnapshotId(dashboardProposal.snapshot_id)
                      try {
                        await refreshDashboard(dashboardProposal.snapshot_id)
                      } catch {
                        setProposalStatusTone('warning')
                        setProposalStatusNote('Blueprint approved, but the dashboard refresh did not complete immediately. Open the dashboard or refresh the page.')
                      }
                      setShowDashboardProposalModal(false)
                      nav(`/dashboard?snapshot_id=${dashboardProposal.snapshot_id}`)
                    } catch {
                      setErr('Could not approve the dashboard proposal.')
                    } finally {
                      setProposalApproveLoading(false)
                    }
                  }}
                  style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}
                  type="button"
                >
                  {proposalApproveLoading ? 'Approving…' : 'Approve and Generate'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {showAnalysisSetModal && analysisSetDraft ? (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-foreground/15 backdrop-blur-sm p-4">
          <div className="w-full max-w-5xl max-h-[88vh] overflow-hidden rounded-3xl border border-border bg-card shadow-[0_28px_80px_rgba(15,23,42,0.16)]">
            <div className="flex items-start justify-between gap-4 border-b border-border px-6 py-5">
              <div>
                <h2 className="text-lg font-black text-foreground">Combined Analysis Review</h2>
                <p className="mt-1 text-sm text-muted-foreground">Confirm how these processed documents should be aligned before the merged analytics and dashboard generation stage begins.</p>
                <div className="mt-2 flex flex-wrap gap-2 text-xs">
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Relationship: {titleCaseWords(analysisSetDraft.relationship_type)}</span>
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Comparability: {titleCaseWords(analysisSetDraft.comparability)}</span>
                  <span className="inline-flex rounded-full bg-secondary px-2.5 py-1 text-muted-foreground">Confidence: {(analysisSetDraft.confidence_score * 100).toFixed(0)}%</span>
                </div>
              </div>
              <button
                className="grid h-9 w-9 place-items-center rounded-full bg-secondary text-muted-foreground transition hover:text-foreground"
                onClick={() => setShowAnalysisSetModal(false)}
                type="button"
              >
                ×
              </button>
            </div>

            <div className="max-h-[62vh] overflow-auto px-6 py-5">
              <div className="grid gap-5">
                <section className="rounded-2xl border border-primary/15 bg-[linear-gradient(180deg,hsla(262,100%,99%,0.94),hsla(268,80%,96%,0.9))] p-4">
                  <div className="text-sm font-semibold text-foreground">Start here: confirm the merge intent</div>
                  <p className="mt-1 text-sm text-muted-foreground">This is the HITL checkpoint. The agent suggests similarity, join keys, and ordering; you confirm or edit them before merged analysis is persisted.</p>
                  <div className="mt-4 grid gap-4 md:grid-cols-2">
                    <div>
                      <label className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Analysis set title</label>
                      <input
                        className="mt-2 w-full rounded-2xl border border-border bg-card px-4 py-3 text-sm text-foreground outline-none transition focus:border-primary/40"
                        onChange={(event) => setAnalysisSetTitle(event.target.value)}
                        value={analysisSetTitle}
                      />
                    </div>
                    <div>
                      <label className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Relationship type</label>
                      <select
                        className="mt-2 w-full rounded-2xl border border-border bg-card px-4 py-3 text-sm text-foreground outline-none transition focus:border-primary/40"
                        onChange={(event) => setAnalysisSetRelationshipType(event.target.value)}
                        value={analysisSetRelationshipType}
                      >
                        <option value="time_series">Time Series</option>
                        <option value="scenario_comparison">Scenario Comparison</option>
                        <option value="portfolio_comparison">Portfolio Comparison</option>
                        <option value="semantic_comparison">Semantic Comparison</option>
                      </select>
                    </div>
                  </div>
                  <div className="mt-4">
                    <label className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Combined-analysis intent</label>
                    <textarea
                      className="mt-2 min-h-[110px] w-full rounded-2xl border border-border bg-card px-4 py-3 text-sm text-foreground outline-none transition focus:border-primary/40"
                      onChange={(event) => setAnalysisSetIntent(event.target.value)}
                      placeholder="Example: Compare these two processed portfolios over time, align them by client, and create merged trend, delta, and concentration analytics."
                      value={analysisSetIntent}
                    />
                  </div>
                  <div className="mt-4">
                    <label className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Confirmed join keys</label>
                    <input
                      className="mt-2 w-full rounded-2xl border border-border bg-card px-4 py-3 text-sm text-foreground outline-none transition focus:border-primary/40"
                      onChange={(event) => setAnalysisSetJoinKeysText(event.target.value)}
                      placeholder="Client Name, Client ID, Tower"
                      value={analysisSetJoinKeysText}
                    />
                    <div className="mt-1 text-xs text-muted-foreground">Comma-separated. Remove or edit anything that is not a safe merge key.</div>
                  </div>
                </section>

                <section className="rounded-2xl border border-border bg-secondary/15 p-4">
                  <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Similarity rationale</div>
                  <p className="mt-2 text-sm text-muted-foreground">{analysisSetDraft.rationale}</p>
                  {analysisSetDraft.dashboard_hypothesis.length ? (
                    <div className="mt-4">
                      <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Dashboard hypothesis</div>
                      <div className="mt-2 grid gap-2">
                        {analysisSetDraft.dashboard_hypothesis.map((item) => (
                          <div key={item} className="rounded-xl border border-border bg-card px-3 py-2 text-sm text-muted-foreground">{item}</div>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  {analysisSetDraft.conflicts.length ? (
                    <div className="mt-4">
                      <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Conflicts to review</div>
                      <div className="mt-2 grid gap-2">
                        {analysisSetDraft.conflicts.map((item) => (
                          <div key={item} className="rounded-xl border border-warning/20 bg-warning/5 px-3 py-2 text-sm text-muted-foreground">{item}</div>
                        ))}
                      </div>
                    </div>
                  ) : null}
                </section>

                <section className="rounded-2xl border border-border bg-secondary/15 p-4">
                  <div className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Selected documents and ordering</div>
                  <div className="mt-3 grid gap-3">
                    {analysisSetDraft.members.map((member) => (
                      <div key={member.snapshot_id} className="grid gap-3 rounded-xl border border-border bg-card p-3 md:grid-cols-[minmax(0,1fr)_220px]">
                        <div>
                          <div className="text-sm font-semibold text-foreground">{member.source_filename}</div>
                          <div className="mt-1 text-xs text-muted-foreground">
                            {member.as_of_date ? formatDate(member.as_of_date) : 'No date'} · {member.workbook_type ?? 'Unknown family'}
                          </div>
                        </div>
                        <div>
                          <label className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Member label</label>
                          <input
                            className="mt-2 w-full rounded-xl border border-border bg-secondary/20 px-3 py-2 text-sm text-foreground outline-none transition focus:border-primary/40"
                            onChange={(event) =>
                              setAnalysisSetMemberLabels((current) => ({ ...current, [member.snapshot_id]: event.target.value }))
                            }
                            value={analysisSetMemberLabels[member.snapshot_id] ?? ''}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                </section>
              </div>
            </div>

            <div className="sticky bottom-0 flex items-center justify-between gap-3 border-t border-border bg-card px-6 py-4">
              <div className="text-sm text-muted-foreground">
                Confirming this stores the join logic and comparison intent for the next combined analytics and dashboard generation stage.
              </div>
              <div className="flex items-center gap-2">
                <button
                  className="rounded-xl border border-border bg-card px-4 py-2.5 text-sm font-semibold text-foreground transition hover:bg-accent"
                  onClick={() => setShowAnalysisSetModal(false)}
                  type="button"
                >
                  Close
                </button>
                <button
                  className="rounded-xl px-4 py-2.5 text-sm font-semibold text-primary-foreground transition disabled:opacity-40"
                  disabled={analysisSetConfirming}
                  onClick={() => void confirmCombinedAnalysis()}
                  style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}
                  type="button"
                >
                  {analysisSetConfirming ? 'Confirming…' : 'Confirm Combined Analysis'}
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}
