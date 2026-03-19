import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useApi } from '../api'
import { useDashboardData } from '../dashboard/DashboardDataProvider'
import { loadGeminiSettings } from '../geminiSettings'
import './DocumentsPage.css'

type DocRow = {
  id: string
  fileName: string
  format: string
  type: string
  status: 'Uploading' | 'To be reviewed' | 'Processed' | 'Rejected'
  subtitle: string
  snapshotId?: number
}

function pillStyle(status: DocRow['status']) {
  const map: Record<DocRow['status'], { bg: string; text: string; border: string; dot: string }> = {
    Uploading: { bg: '#eff6ff', text: '#1d4ed8', border: '#bfdbfe', dot: '#3b82f6' },
    'To be reviewed': { bg: '#f3e8ff', text: '#6d28d9', border: '#ddd6fe', dot: '#8b5cf6' },
    Processed: { bg: '#ecfdf5', text: '#166534', border: '#bbf7d0', dot: '#22c55e' },
    Rejected: { bg: '#fef2f2', text: '#b91c1c', border: '#fecaca', dot: '#ef4444' },
  }
  return map[status]
}

function IconUpload() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path d="M12 3v10m0-10-4 4m4-4 4 4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M4 14v4a3 3 0 0 0 3 3h10a3 3 0 0 0 3-3v-4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function IconEye() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path d="M2.75 12S6.25 5.75 12 5.75 21.25 12 21.25 12 17.75 18.25 12 18.25 2.75 12 2.75 12Z" stroke="currentColor" strokeWidth="1.7" />
      <circle cx="12" cy="12" r="2.75" stroke="currentColor" strokeWidth="1.7" />
    </svg>
  )
}

function IconTrash() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M5 7h14m-9 0V5a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1v2m1 0-1 14a2 2 0 0 1-2 2H9a2 2 0 0 1-2-2L6 7"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path d="M10 11v7m4-7v7" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
    </svg>
  )
}

function IconSpreadsheet() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path d="M7 3.75A2.25 2.25 0 0 0 4.75 6v12A2.25 2.25 0 0 0 7 20.25h10A2.25 2.25 0 0 0 19.25 18V8.8a2.25 2.25 0 0 0-.66-1.59l-2.8-2.8a2.25 2.25 0 0 0-1.59-.66H7Z" fill="#f97316" opacity="0.2" />
      <path d="M9 10.25h6M9 13.25h6M9 16.25h4m1-12v4h4" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
      <path d="M7 3.75A2.25 2.25 0 0 0 4.75 6v12A2.25 2.25 0 0 0 7 20.25h10A2.25 2.25 0 0 0 19.25 18V8.8a2.25 2.25 0 0 0-.66-1.59l-2.8-2.8a2.25 2.25 0 0 0-1.59-.66H7Z" stroke="currentColor" strokeWidth="1.7" />
    </svg>
  )
}

function formatDate(value: string) {
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return value
  return new Intl.DateTimeFormat(undefined, { month: 'short', day: '2-digit', year: 'numeric' }).format(parsed)
}

export function DocumentsPage() {
  const api = useApi()
  const nav = useNavigate()
  const { snapshots, refreshSnapshots, refreshDashboard } = useDashboardData()
  const [file, setFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [msg, setMsg] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [useGemini, setUseGemini] = useState(false)
  const [hasGeminiKey, setHasGeminiKey] = useState(false)
  const fileInputRef = useRef<HTMLInputElement | null>(null)

  useEffect(() => {
    const settings = loadGeminiSettings()
    setUseGemini(Boolean(settings.enabled && settings.apiKey))
    setHasGeminiKey(Boolean(settings.apiKey))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const rows = useMemo<DocRow[]>(() => {
    const out: DocRow[] = []
    if (file) {
      out.push({
        id: 'selected',
        fileName: file.name,
        format: file.name.toLowerCase().endsWith('.xlsx') ? 'XLSX' : '—',
        type: 'Contracted vs Actual',
        status: uploading ? 'Uploading' : 'To be reviewed',
        subtitle: uploading ? 'Uploading workbook into the parser' : `${(file.size / 1024).toFixed(1)} KB`,
      })
    }

    for (const snapshot of snapshots) {
      out.push({
        id: String(snapshot.id),
        fileName: snapshot.source_filename,
        format: snapshot.source_filename.toLowerCase().endsWith('.xlsx') ? 'XLSX' : '—',
        type: 'Snapshot',
        status: 'Processed',
        subtitle: `As of ${formatDate(snapshot.as_of_date)}`,
        snapshotId: snapshot.id,
      })
    }

    return out
  }, [file, snapshots, uploading])

  const latestSnapshot = snapshots[0]
  const summaryCards = [
    { label: 'Processed snapshots', value: String(snapshots.length).padStart(2, '0'), tone: 'purple' },
    { label: 'Latest snapshot', value: latestSnapshot ? formatDate(latestSnapshot.as_of_date) : 'None', tone: 'green' },
    { label: 'Gemini assist', value: useGemini ? 'Enabled' : hasGeminiKey ? 'Ready' : 'Off', tone: 'amber' },
  ] as const

  async function handleProcess() {
    if (!file) return
    setUploading(true)
    setMsg(null)
    setErr(null)
    try {
      const res = await api.uploadContractedVsActualExcel(file, { useGemini })
      setMsg(`Processed snapshot ${res.snapshot_id}. Dashboard updated for ${formatDate(res.as_of_date)}.`)
      setFile(null)
      await refreshSnapshots()
      await refreshDashboard(res.snapshot_id)
    } catch {
      setErr('Process failed. Ensure the backend is running and the workbook matches the expected layout.')
    } finally {
      setUploading(false)
    }
  }

  return (
    <div className="documents-page">
      <header className="documents-header">
        <div>
          <h1>Documents Processing</h1>
          <p>Central repository for all processed tax-season workbooks and generated snapshots.</p>
        </div>
        <div className="documents-header-actions">
          <label className="documents-gemini-toggle">
            <input type="checkbox" checked={useGemini} onChange={(e) => setUseGemini(e.target.checked)} disabled={!hasGeminiKey} />
            <span>Use Gemini validation</span>
            {!hasGeminiKey && (
              <button className="documents-inline-link" onClick={() => nav('/settings')} type="button">
                Set key
              </button>
            )}
          </label>

          <button className="documents-upload-button" onClick={() => fileInputRef.current?.click()} type="button">
            <IconUpload />
            Upload Document
            <input
              ref={fileInputRef}
              type="file"
              accept=".xlsx"
              className="documents-hidden-input"
              onChange={(e) => {
                const nextFile = e.target.files?.[0] ?? null
                setFile(nextFile)
                setMsg(null)
                setErr(null)
              }}
            />
          </button>
        </div>
      </header>

      <section className="documents-summary-grid">
        {summaryCards.map((card) => (
          <article key={card.label} className={`documents-summary-card ${card.tone}`}>
            <span>{card.label}</span>
            <strong>{card.value}</strong>
          </article>
        ))}
      </section>

      {msg && <div className="documents-feedback success">{msg}</div>}
      {err && <div className="documents-feedback error">{err}</div>}

      <section className="documents-panel">
        <div className="documents-panel-header">
          <div>
            <h2>Central repository for processed documents</h2>
            <p>Upload a workbook, review its state, then process it into a dashboard snapshot.</p>
          </div>
          <span>{rows.length} items</span>
        </div>

        <div className="documents-table-wrap">
          <table className="documents-table">
            <thead>
              <tr>
                <th className="documents-checkbox-col">
                  <input type="checkbox" disabled />
                </th>
                {['File name', 'Format', 'Type', 'Status', 'Actions'].map((heading) => (
                  <th key={heading}>{heading}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => {
                const pill = pillStyle(row.status)
                return (
                  <tr key={row.id}>
                    <td className="documents-checkbox-col">
                      <input type="checkbox" />
                    </td>
                    <td>
                      <div className="documents-file-cell">
                        <div className="documents-file-icon">
                          <IconSpreadsheet />
                        </div>
                        <div>
                          <div className="documents-file-name">{row.fileName}</div>
                          <div className="documents-file-subtitle">{row.subtitle}</div>
                        </div>
                      </div>
                    </td>
                    <td>{row.format || '—'}</td>
                    <td>{row.type}</td>
                    <td>
                      <span className="documents-status-pill" style={{ background: pill.bg, color: pill.text, borderColor: pill.border }}>
                        <span className="documents-status-dot" style={{ background: pill.dot }} />
                        {row.status}
                      </span>
                    </td>
                    <td>
                      <div className="documents-actions">
                        <button className="documents-icon-button" onClick={() => nav(row.snapshotId ? '/dashboard' : '/documents')} type="button" aria-label="View" disabled={!row.snapshotId}>
                          <IconEye />
                        </button>
                        <button className="documents-process-button" disabled={row.id !== 'selected' || uploading} onClick={() => void handleProcess()} type="button">
                          {uploading && row.id === 'selected' ? 'Processing…' : 'Process'}
                        </button>
                        <button
                          className="documents-icon-button danger"
                          title="Delete"
                          onClick={() => {
                            setMsg(null)
                            setErr(null)
                            void (async () => {
                              if (row.id === 'selected') {
                                setFile(null)
                                return
                              }
                              const ok = window.confirm(`Delete snapshot "${row.fileName}"?`)
                              if (!ok) return
                              await api.deleteSnapshot(Number(row.id))
                              await refreshSnapshots()
                            })().catch(() => {
                              setErr('Delete failed.')
                            })
                          }}
                          type="button"
                        >
                          <IconTrash />
                        </button>
                      </div>
                    </td>
                  </tr>
                )
              })}

              {rows.length === 0 && (
                <tr>
                  <td className="documents-empty" colSpan={6}>
                    No documents yet. Upload the contracted vs actual workbook to create your first snapshot.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}
