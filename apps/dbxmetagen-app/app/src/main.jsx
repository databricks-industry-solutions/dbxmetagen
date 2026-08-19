import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import { JobRunnerProvider } from './hooks/useJobRunner'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <JobRunnerProvider>
      <App />
    </JobRunnerProvider>
  </React.StrictMode>
)
