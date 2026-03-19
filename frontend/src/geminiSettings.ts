export type GeminiModel = 'gemini-2.5-flash' | 'gemini-2.5-pro' | 'gemini-3'

export type GeminiSettings = {
  apiKey: string
  model: GeminiModel
  enabled: boolean
}

const KEY = 'gkm_gemini_settings'

export function loadGeminiSettings(): GeminiSettings {
  const raw = localStorage.getItem(KEY)
  if (!raw) return { apiKey: '', model: 'gemini-2.5-flash', enabled: false }
  try {
    const parsed = JSON.parse(raw) as Partial<GeminiSettings>
    return {
      apiKey: parsed.apiKey ?? '',
      model: (parsed.model as GeminiModel) ?? 'gemini-2.5-flash',
      enabled: parsed.enabled ?? false,
    }
  } catch {
    return { apiKey: '', model: 'gemini-2.5-flash', enabled: false }
  }
}

export function saveGeminiSettings(next: GeminiSettings) {
  localStorage.setItem(KEY, JSON.stringify(next))
}

