// Date/time display helper.
//
// WHY: the backend stores timestamps in UTC and serializes them WITHOUT a timezone
// marker (e.g. "2026-10-05 06:44:31.478091" or "2026-10-05T06:44:31"). A bare
// `new Date(s)` on such a string is parsed as *browser-local* time, so any browser
// outside UTC renders it off by its UTC offset. These helpers parse the value as
// UTC explicitly and then format it in the browser's own timezone and locale.

// Parse a backend timestamp (UTC, possibly without a tz marker) into a Date.
function parseUtc(value) {
  if (!value) return null;
  let s = String(value).trim();
  // Already has a tz marker (Z or +hh:mm) -> trust it.
  const hasTz = /[zZ]$|[+-]\d{2}:?\d{2}$/.test(s);
  if (!hasTz) {
    // Normalize "YYYY-MM-DD HH:MM:SS" -> ISO and mark as UTC.
    s = s.replace(' ', 'T') + 'Z';
  }
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

// Date + time in the browser's timezone and locale.
export function formatDateTime(value) {
  const d = parseUtc(value);
  if (!d) return '';
  return d.toLocaleString();
}

// Date only in the browser's timezone and locale.
export function formatDate(value) {
  const d = parseUtc(value);
  if (!d) return '';
  return d.toLocaleDateString();
}
