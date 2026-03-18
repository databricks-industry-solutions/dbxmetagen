import { useRef, useEffect, useCallback } from 'react'

/**
 * Poll a task endpoint until done/error or component unmounts.
 *
 * @param {Object} opts
 * @param {Function} opts.onDone   - called with task payload on success
 * @param {Function} opts.onError  - called with error string on failure
 * @param {Function} [opts.onStage] - called with stage string during progress
 * @param {number}   [opts.interval=2000]
 * @returns {{ start: (pollUrl: string) => void, stop: () => void }}
 */
export default function useTaskPolling({ onDone, onError, onStage, interval = 2000 }) {
  const timerRef = useRef(null)

  const stop = useCallback(() => {
    if (timerRef.current) {
      clearInterval(timerRef.current)
      timerRef.current = null
    }
  }, [])

  useEffect(() => stop, [stop])

  const start = useCallback((pollUrl) => {
    stop()
    timerRef.current = setInterval(async () => {
      try {
        const res = await fetch(pollUrl)
        if (!res.ok) return
        const task = await res.json()
        if (task.stage && onStage) onStage(task.stage)
        if (task.status === 'done') {
          stop()
          onDone(task)
        } else if (task.status === 'error') {
          stop()
          onError(task.error || 'Task failed')
        }
      } catch { /* retry on next tick */ }
    }, interval)
  }, [stop, onDone, onError, onStage, interval])

  return { start, stop }
}
