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
    <div className="grid grid-cols-[1.2fr_1fr] h-full max-md:grid-cols-1">
      {/* Left — Brand Panel */}
      <div className="gradient-header flex flex-col justify-end p-10 text-primary-foreground max-md:hidden"
        style={{ background: 'radial-gradient(1200px 600px at 20% 20%, hsl(var(--primary)) 0%, hsl(220,30%,12%) 65%, hsl(230,25%,8%) 100%)' }}>
        <div className="font-black text-xl opacity-90 mb-5 tracking-wide">yavar</div>
        <h1 className="text-[2.5rem] font-black leading-[1.05] max-w-[520px]">
          Power Your Workflow with Agentic AI
        </h1>
        <p className="mt-3 opacity-80 max-w-[520px] text-base leading-relaxed">
          GKM platform helps ingest documents, redact sensitive fields, and generate dynamic analytics for tax outsourcing engagements.
        </p>
      </div>

      {/* Right — Login Form */}
      <div className="bg-card flex items-center justify-center p-7">
        <div className="w-[420px] max-w-full animate-fade-in">
          <h2 className="text-3xl font-black text-foreground mb-1">Welcome</h2>
          <p className="text-sm text-muted-foreground mb-6">Please enter login details below</p>

          <label className="block text-xs font-bold text-muted-foreground mb-1.5">Email</label>
          <input value={email} onChange={(e) => setEmail(e.target.value)} placeholder="you@company.com"
            className="w-full rounded-lg border border-border px-3 py-2.5 text-foreground bg-card outline-none focus:border-primary/50 transition mb-4" />

          <div className="flex justify-between items-baseline mb-1.5">
            <label className="text-xs font-bold text-muted-foreground">Password</label>
            <span className="text-xs text-primary font-bold cursor-pointer hover:underline">Forgot Password?</span>
          </div>
          <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} placeholder="Enter your password"
            className="w-full rounded-lg border border-border px-3 py-2.5 text-foreground bg-card outline-none focus:border-primary/50 transition mb-3" />

          {error && (
            <div className="bg-destructive/10 border border-destructive/30 text-destructive px-3 py-2.5 rounded-lg mb-3 text-xs font-semibold animate-fade-in">
              {error}
            </div>
          )}

          <button disabled={!canSubmit}
            onClick={async () => {
              setError(null); setLoading(true)
              try { const res = await api.login(email, password); setToken(res.access_token); nav('/dashboard') }
              catch { setError('Login failed. Check credentials and backend connectivity.') }
              finally { setLoading(false) }
            }}
            className="w-full rounded-xl py-3 font-extrabold text-primary-foreground transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed hover:opacity-90 hover:shadow-elevated"
            style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}>
            {loading ? 'Logging in…' : 'Login'}
          </button>

          <p className="mt-3 text-[0.7rem] text-muted-foreground">
            Default seed user comes from backend <code className="bg-secondary px-1 py-0.5 rounded text-xs">.env</code>
          </p>
        </div>
      </div>
    </div>
  )
}
