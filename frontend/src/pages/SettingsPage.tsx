import { useMemo, useState } from 'react'
import type { GeminiModel } from '../geminiSettings'
import { loadGeminiSettings, saveGeminiSettings } from '../geminiSettings'
import { Settings2, Key, Cpu, CheckCircle2, Sparkles } from 'lucide-react'

export function SettingsPage() {
  const initial = useMemo(() => loadGeminiSettings(), [])
  const [enabled, setEnabled] = useState(initial.enabled)
  const [apiKey, setApiKey] = useState(initial.apiKey)
  const [model, setModel] = useState<GeminiModel>(initial.model)
  const [saved, setSaved] = useState<string | null>(null)

  return (
    <div className="max-w-[980px] mx-auto animate-fade-in">
      {/* Page Header */}
      <div className="flex items-center gap-3 mb-6">
        <div className="grid place-items-center w-10 h-10 rounded-xl bg-primary/10 text-primary">
          <Settings2 className="w-5 h-5" />
        </div>
        <div>
          <h1 className="text-xl font-black text-foreground">Settings</h1>
          <p className="text-xs text-muted-foreground">Configure processing providers and defaults.</p>
        </div>
      </div>

      {/* Gemini Settings Card */}
      <div className="bg-card border border-border rounded-2xl p-6 shadow-soft">
        <div className="flex justify-between gap-4 items-start flex-wrap">
          <div className="flex items-start gap-3">
            <div className="grid place-items-center w-10 h-10 rounded-xl bg-accent text-accent-foreground shrink-0 mt-0.5">
              <Sparkles className="w-5 h-5" />
            </div>
            <div>
              <h2 className="text-base font-black text-foreground">Gemini (optional)</h2>
              <p className="text-xs text-muted-foreground mt-1 max-w-md">
                Your API key is stored <strong>only in your browser</strong> (localStorage) and sent to the backend via request headers when enabled.
              </p>
            </div>
          </div>
          <label className="inline-flex items-center gap-2 text-xs font-bold text-muted-foreground cursor-pointer select-none">
            <input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} className="accent-primary w-4 h-4" />
            Enable Gemini processing
          </label>
        </div>

        <div className="grid grid-cols-[1fr_240px] max-md:grid-cols-1 gap-4 mt-5">
          <div>
            <label className="flex items-center gap-1.5 text-xs font-bold text-muted-foreground mb-2">
              <Key className="w-3.5 h-3.5" /> Gemini API Key
            </label>
            <input value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="AIza..."
              type="password"
              className="w-full rounded-xl border border-border px-3.5 py-2.5 bg-card text-foreground outline-none focus:border-primary/50 transition text-sm" />
          </div>
          <div>
            <label className="flex items-center gap-1.5 text-xs font-bold text-muted-foreground mb-2">
              <Cpu className="w-3.5 h-3.5" /> Model
            </label>
            <select value={model} onChange={(e) => setModel(e.target.value as GeminiModel)}
              className="w-full rounded-xl border border-border px-3.5 py-2.5 bg-card text-foreground font-bold outline-none focus:border-primary/50 transition text-sm">
              <option value="gemini-2.5-flash">Gemini 2.5 Flash</option>
              <option value="gemini-2.5-pro">Gemini 2.5 Pro</option>
            </select>
          </div>
        </div>

        {saved && (
          <div className="mt-4 flex items-center gap-2 bg-success/10 border border-success/30 text-success px-4 py-2.5 rounded-xl text-xs font-semibold animate-fade-in">
            <CheckCircle2 className="w-4 h-4" /> {saved}
          </div>
        )}

        <div className="mt-5 flex gap-3">
          <button onClick={() => {
            saveGeminiSettings({ enabled, apiKey, model })
            setSaved('Saved. New uploads can use Gemini if you enable it on the Documents Processing screen.')
            setTimeout(() => setSaved(null), 3500)
          }}
            className="px-5 py-2.5 rounded-xl font-extrabold text-primary-foreground transition hover:opacity-90"
            style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}>
            Save
          </button>
          <button onClick={() => {
            setEnabled(false); setApiKey(''); setModel('gemini-2.5-flash')
            saveGeminiSettings({ enabled: false, apiKey: '', model: 'gemini-2.5-flash' })
            setSaved('Cleared.')
            setTimeout(() => setSaved(null), 2500)
          }}
            className="px-5 py-2.5 rounded-xl font-extrabold border border-border bg-card text-muted-foreground hover:text-foreground hover:bg-accent transition">
            Clear
          </button>
        </div>
      </div>
    </div>
  )
}
