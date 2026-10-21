import { createContext, useContext } from 'react'

// Certified-asset apply policy (#247). Governs how applying metadata treats UC-certified
// assets (certification_status=certified). The value comes from the backend
// (CERTIFIED_APPLY_POLICY / variables.yml certified_apply_policy) via /api/config and is
// deployment-wide (not a per-user preference):
//   'warn'  (default) -- apply but surface/warn; a per-run "skip certified" checkbox opts
//                        into skipping instead.
//   'block'           -- never apply to certified; HARD, no override (checkbox hidden, apply
//                        disabled on certified rows). Enforced server-side + in the jobs.
//   'allow'           -- no gate.
// App.jsx owns the state and provides this context; components read `policy`.
export const CertifiedApplyPolicyContext = createContext({ policy: 'warn' })

export function useCertifiedApplyPolicy() {
  return useContext(CertifiedApplyPolicyContext)
}
