import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import { JobRunnerProvider } from './hooks/useJobRunner'
import { LanguageProvider } from './contexts/LanguageContext'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <LanguageProvider>
      <JobRunnerProvider>
        <App />
      </JobRunnerProvider>
    </LanguageProvider>
  </React.StrictMode>
)
