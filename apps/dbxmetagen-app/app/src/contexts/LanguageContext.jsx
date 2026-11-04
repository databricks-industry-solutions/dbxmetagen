import React, { createContext, useContext, useState, useEffect, useCallback } from 'react'
import { detectDefaultLang, translate, SUPPORTED_LANGS } from '../i18n'

const LanguageContext = createContext({
  language: 'en',
  setLanguage: () => {},
  t: (key) => key,
})

export function LanguageProvider({ children }) {
  const [language, setLanguageState] = useState(detectDefaultLang)

  useEffect(() => {
    try { localStorage.setItem('dbxmetagen-language', language) } catch {}
    if (typeof document !== 'undefined') document.documentElement.lang = language
  }, [language])

  const setLanguage = useCallback((lang) => {
    if (SUPPORTED_LANGS.includes(lang)) setLanguageState(lang)
  }, [])

  const t = useCallback((key, params) => translate(language, key, params), [language])

  return (
    <LanguageContext.Provider value={{ language, setLanguage, t }}>
      {children}
    </LanguageContext.Provider>
  )
}

export function useLanguage() {
  return useContext(LanguageContext)
}
