import { useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '../auth/AuthProvider'
import { useApi } from '../api'

export function LoginPage() {
  const api = useApi()
  const { setToken } = useAuth()
  const nav = useNavigate()
  const [email, setEmail] = useState('admin@example.com')
  const [password, setPassword] = useState('admin1234')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const canSubmit = useMemo(() => email.trim().length > 0 && password.length > 0 && !loading, [email, password, loading])

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 1fr', height: '100%' }}>
      <div
        style={{
          background: 'radial-gradient(1200px 600px at 20% 20%, #7c3aed 0%, #111827 65%, #0b1020 100%)',
          color: 'white',
          padding: 44,
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'flex-end',
        }}
      >
        <div style={{ fontWeight: 900, fontSize: 22, opacity: 0.9, marginBottom: 18 }}>yavar</div>
        <div style={{ fontSize: 40, fontWeight: 900, lineHeight: 1.05, maxWidth: 520 }}>
          Power Your Workflow with Agentic AI
        </div>
        <div style={{ marginTop: 10, opacity: 0.85, maxWidth: 520 }}>
          GKM platform helps ingest documents, redact sensitive fields, and generate dynamic analytics for tax outsourcing
          engagements.
        </div>
      </div>

      <div style={{ background: 'white', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 28 }}>
        <div style={{ width: 420, maxWidth: '100%' }}>
          <div style={{ fontSize: 28, fontWeight: 900, marginBottom: 4 }}>Welcome</div>
          <div style={{ fontSize: 12, color: 'var(--muted)', marginBottom: 22 }}>Please enter login details below</div>

          <label style={{ display: 'block', fontSize: 12, fontWeight: 700, marginBottom: 6, color: 'var(--muted)' }}>
            Email
          </label>
          <input
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder="you@company.com"
            style={{
              width: '100%',
              borderRadius: 8,
              border: '1px solid var(--border)',
              padding: '10px 12px',
              outline: 'none',
              marginBottom: 14,
            }}
          />

          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
            <label style={{ display: 'block', fontSize: 12, fontWeight: 700, marginBottom: 6, color: 'var(--muted)' }}>
              Password
            </label>
            <span style={{ fontSize: 12, color: 'var(--brand)', fontWeight: 700 }}>Forgot Password?</span>
          </div>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="Enter your password"
            style={{
              width: '100%',
              borderRadius: 8,
              border: '1px solid var(--border)',
              padding: '10px 12px',
              outline: 'none',
              marginBottom: 12,
            }}
          />

          {error && (
            <div style={{ background: '#fef2f2', border: '1px solid #fecaca', color: '#b91c1c', padding: 10, borderRadius: 8, marginBottom: 12, fontSize: 12 }}>
              {error}
            </div>
          )}

          <button
            disabled={!canSubmit}
            onClick={async () => {
              setError(null)
              setLoading(true)
              try {
                const res = await api.login(email, password)
                setToken(res.access_token)
                nav('/dashboard')
              } catch (e: any) {
                setError('Login failed. Check credentials and backend connectivity.')
              } finally {
                setLoading(false)
              }
            }}
            style={{
              width: '100%',
              borderRadius: 10,
              border: '1px solid transparent',
              background: '#b794f4',
              color: 'white',
              padding: '11px 12px',
              fontWeight: 800,
              cursor: canSubmit ? 'pointer' : 'not-allowed',
              opacity: canSubmit ? 1 : 0.6,
            }}
          >
            {loading ? 'Logging in…' : 'Login'}
          </button>

          <div style={{ marginTop: 12, fontSize: 11, color: 'var(--muted)' }}>
            Default seed user comes from backend `.env` (see `backend/.env.example`).
          </div>
        </div>
      </div>
    </div>
  )
}

