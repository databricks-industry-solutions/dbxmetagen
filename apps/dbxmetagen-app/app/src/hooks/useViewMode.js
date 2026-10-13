import { createContext, useContext } from 'react'

// App view mode (#246). 'simplified' hides advanced tools/controls for non-technical
// stewards; 'advanced' is the full app. The default comes from the backend
// (DEFAULT_VIEW_MODE / variables.yml default_view_mode); each user can override it from
// the Settings widget, persisted per-browser. It is a display preference, not access
// control. App.jsx owns the state and provides this context; components read `simplified`.
export const ViewModeContext = createContext({
  viewMode: 'advanced',
  simplified: false,
  setViewMode: () => {},
})

export function useViewMode() {
  return useContext(ViewModeContext)
}
