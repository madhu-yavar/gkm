export type GeminiModel = 'gemini-2.5-flash' | 'gemini-2.5-pro'

export type GeminiSettings = {
  apiKey: string
  model: GeminiModel
  enabled: boolean
}

const KEY = 'gkm_gemini_settings'

function sanitizeGeminiModel(model: string | undefined): GeminiModel {
  return model === 'gemini-2.5-pro' ? 'gemini-2.5-pro' : 'gemini-2.5-flash'
}

export function loadGeminiSettings(): GeminiSettings {
  const raw = localStorage.getItem(KEY)
  if (!raw) return { apiKey: '', model: 'gemini-2.5-flash', enabled: false }
  try {
    const parsed = JSON.parse(raw) as Partial<GeminiSettings>
    return {
      apiKey: parsed.apiKey ?? '',
      model: sanitizeGeminiModel(parsed.model),
      enabled: parsed.enabled ?? false,
    }
  } catch {
    return { apiKey: '', model: 'gemini-2.5-flash', enabled: false }
  }
}

export function saveGeminiSettings(next: GeminiSettings) {
  localStorage.setItem(KEY, JSON.stringify(next))
}
