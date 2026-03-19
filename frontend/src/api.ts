import { useAuth } from './auth/AuthProvider'
import { loadGeminiSettings } from './geminiSettings'

export const API_BASE = import.meta.env.VITE_API_BASE ?? 'http://localhost:8000'

export type TokenResponse = { access_token: string; token_type: 'bearer' }

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
    throw new Error(text || `HTTP ${res.status}`)
  }
  if (res.status === 204) return undefined as T
  const ct = res.headers.get('content-type') || ''
  if (!ct.includes('application/json')) return (await res.text()) as unknown as T
  return (await res.json()) as T
}

export function useApi() {
  const { token } = useAuth()
  return {
    login: (email: string, password: string) =>
      http<TokenResponse>('/auth/login', { method: 'POST', body: JSON.stringify({ email, password }) }),
    listSnapshots: () => http<Array<{ id: number; as_of_date: string; source_filename: string }>>('/snapshots', {}, token),
    deleteSnapshot: (snapshotId: number) => http<void>(`/snapshots/${snapshotId}`, { method: 'DELETE' }, token),
    getKpis: (snapshotId?: number) =>
      http<any>(`/dashboard/kpis${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`, {}, token),
    listClients: (snapshotId?: number) =>
      http<any>(`/dashboard/clients${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`, {}, token),
    listStaff: (snapshotId?: number) =>
      http<any>(`/dashboard/staff${snapshotId ? `?snapshot_id=${snapshotId}` : ''}`, {}, token),
    uploadContractedVsActualExcel: async (file: File, opts?: { useGemini?: boolean }) => {
      const fd = new FormData()
      fd.append('file', file)
      return http<{ snapshot_id: number; as_of_date: string; gemini_used?: boolean }>(
        `/documents/excel/contracted-vs-actual${opts?.useGemini ? '?use_gemini=true' : ''}`,
        { method: 'POST', body: fd },
        token,
      )
    },
  }
}

