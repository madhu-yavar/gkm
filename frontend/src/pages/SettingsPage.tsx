import { useMemo, useState } from 'react'
import type { GeminiModel } from '../geminiSettings'
import { loadGeminiSettings, saveGeminiSettings } from '../geminiSettings'

export function SettingsPage() {
  const initial = useMemo(() => loadGeminiSettings(), [])
  const [enabled, setEnabled] = useState(initial.enabled)
  const [apiKey, setApiKey] = useState(initial.apiKey)
  const [model, setModel] = useState<GeminiModel>(initial.model)
  const [saved, setSaved] = useState<string | null>(null)

  return (
    <div style={{ maxWidth: 980, margin: '0 auto' }}>
      <div style={{ fontSize: 20, fontWeight: 900, marginBottom: 6 }}>Settings</div>
      <div style={{ fontSize: 12, color: 'var(--muted)' }}>Configure processing providers and defaults.</div>

      <div style={{ marginTop: 14, background: 'white', border: '1px solid var(--border)', borderRadius: 14, padding: 16 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'baseline', flexWrap: 'wrap' }}>
          <div>
            <div style={{ fontSize: 14, fontWeight: 900 }}>Gemini (optional)</div>
            <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 4 }}>
              Your API key is stored <b>only in your browser</b> (localStorage) and sent to the backend via request headers when enabled.
            </div>
          </div>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 12, fontWeight: 800, color: 'var(--muted)' }}>
            <input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
            Enable Gemini processing
          </label>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 240px', gap: 12, marginTop: 14 }}>
          <div>
            <div style={{ fontSize: 12, fontWeight: 800, color: 'var(--muted)', marginBottom: 6 }}>Gemini API Key</div>
            <input
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              placeholder="AIza..."
              type="password"
              style={{ width: '100%', borderRadius: 10, border: '1px solid var(--border)', padding: '10px 12px', outline: 'none' }}
            />
          </div>
          <div>
            <div style={{ fontSize: 12, fontWeight: 800, color: 'var(--muted)', marginBottom: 6 }}>Model</div>
            <select
              value={model}
              onChange={(e) => setModel(e.target.value as GeminiModel)}
              style={{ width: '100%', borderRadius: 10, border: '1px solid var(--border)', padding: '10px 12px', outline: 'none', background: 'white', fontWeight: 800 }}
            >
              <option value="gemini-2.5-flash">Gemini 2.5 Flash</option>
              <option value="gemini-2.5-pro">Gemini 2.5 Pro</option>
              <option value="gemini-3">Gemini 3</option>
            </select>
          </div>
        </div>

        {saved && (
          <div style={{ marginTop: 12, fontSize: 12, background: '#ecfdf5', border: '1px solid #bbf7d0', color: '#166534', padding: 10, borderRadius: 10 }}>
            {saved}
          </div>
        )}

        <div style={{ marginTop: 14, display: 'flex', gap: 10 }}>
          <button
            onClick={() => {
              saveGeminiSettings({ enabled, apiKey, model })
              setSaved('Saved. New uploads can use Gemini if you enable it on the Documents Processing screen.')
              setTimeout(() => setSaved(null), 3500)
            }}
            style={{
              borderRadius: 10,
              border: '1px solid transparent',
              background: 'var(--brand)',
              color: 'white',
              padding: '10px 14px',
              fontWeight: 900,
              cursor: 'pointer',
            }}
          >
            Save
          </button>
          <button
            onClick={() => {
              setEnabled(false)
              setApiKey('')
              setModel('gemini-2.5-flash')
              saveGeminiSettings({ enabled: false, apiKey: '', model: 'gemini-2.5-flash' })
              setSaved('Cleared.')
              setTimeout(() => setSaved(null), 2500)
            }}
            style={{
              borderRadius: 10,
              border: '1px solid var(--border)',
              background: 'white',
              color: 'var(--muted)',
              padding: '10px 14px',
              fontWeight: 900,
              cursor: 'pointer',
            }}
          >
            Clear
          </button>
        </div>
      </div>
    </div>
  )
}

