// The key lives in sessionStorage of this tab only and is sent in a header,
// never in a URL (URLs end up in logs, history and Referer).
function getKey() {
  try { return sessionStorage.getItem('apiKey') || ''; } catch { return ''; }
}
function setKey(k) {
  try { sessionStorage.setItem('apiKey', k); } catch { /* private mode */ }
}
function api(path, opts = {}) {
  return fetch(path, {
    ...opts,
    cache: 'no-store',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + getKey(), ...(opts.headers || {}) },
  });
}
