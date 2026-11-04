// Lightweight i18n: static dictionaries + t(key) lookup with English fallback.
// No runtime LLM, no i18next. Korean dict is generated build-time (LLM + glossary)
// and reviewed by hand.
import { en } from './strings.en'
import { ko } from './strings.ko'

const DICTS = { en, ko }

export const SUPPORTED_LANGS = ['en', 'ko']

export function detectDefaultLang() {
  if (typeof window === 'undefined') return 'en'
  try {
    const saved = localStorage.getItem('dbxmetagen-language')
    if (saved && SUPPORTED_LANGS.includes(saved)) return saved
  } catch {}
  const nav = (typeof navigator !== 'undefined' && (navigator.language || navigator.userLanguage)) || 'en'
  return nav.toLowerCase().startsWith('ko') ? 'ko' : 'en'
}

// Translate: look up key in the active language, fall back to English, then to
// the key itself (so a missing key is visible but never crashes the UI).
// Supports {var} interpolation via the optional params object.
export function translate(lang, key, params) {
  const dict = DICTS[lang] || en
  let s = dict[key]
  if (s == null) s = en[key]
  if (s == null) s = key
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      s = s.replace(new RegExp('\\{' + k + '\\}', 'g'), String(v))
    }
  }
  return s
}
