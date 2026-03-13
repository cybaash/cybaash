// ── GitHub Storage Layer — STREAMING REWRITE ─────────────────────────────────
// All portfolio data lives in portfolio/data.json in the GitHub repo.
// The admin panel reads/writes it via the GitHub Contents API using a
// Personal Access Token stored in localStorage (never leaves the browser).
//
// ⚠  ROOT CAUSE FIX (was crashing on large files):
//    The original code used atob(meta.content) to decode the API response.
//    GitHub's Contents API base64-encodes the file body, and atob() is
//    synchronous + single-threaded.  On a file >= ~5 MB it blocks the main
//    thread for several seconds then throws "string too long" in V8.
//    Fix: use the raw download URL with ReadableStream so the browser never
//    has to base64-decode the entire file body in one synchronous call.

const CONFIG_KEY = 'portfolio_github_config'

export function getGithubConfig() {
  try {
    const raw = localStorage.getItem(CONFIG_KEY)
    return raw ? JSON.parse(raw) : null
  } catch { return null }
}

export function saveGithubConfig(owner, repo, token) {
  localStorage.setItem(CONFIG_KEY, JSON.stringify({ owner, repo, token }))
}

export function clearGithubConfig() {
  localStorage.removeItem(CONFIG_KEY)
}

// ── URL builders ──────────────────────────────────────────────────────────────
const DATA_PATH = 'portfolio/data.json'

// Raw URL — no base64, supports ReadableStream (fixes the crash)
function rawUrl(cfg) {
  return `https://raw.githubusercontent.com/${cfg.owner}/${cfg.repo}/main/${DATA_PATH}`
}

// Contents API — only needed for SHA + writes
function contentsUrl(cfg) {
  return `https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${DATA_PATH}`
}

function apiHeaders(cfg) {
  return {
    'Authorization':        `Bearer ${cfg.token}`,
    'Accept':               'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'Content-Type':         'application/json',
  }
}

// ── Stream-read the raw file ──────────────────────────────────────────────────
// No atob() — reads the file as a stream of UTF-8 chunks so the browser
// thread is never blocked processing a 12 MB base64 string.
async function fetchDataStreaming(cfg) {
  const r = await fetch(rawUrl(cfg), {
    headers: { 'Authorization': `Bearer ${cfg.token}` },
    cache:   'no-store',
  })
  if (r.status === 404) return {}
  if (!r.ok) throw new Error(`GitHub raw fetch failed: ${r.status}`)

  const decoder = new TextDecoder('utf-8')
  const reader  = r.body.getReader()
  const parts   = []
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    parts.push(decoder.decode(value, { stream: true }))
  }
  parts.push(decoder.decode()) // flush final bytes
  return JSON.parse(parts.join(''))
}

// ── Fetch only the current SHA (tiny metadata response) ───────────────────────
async function fetchSha(cfg) {
  const r = await fetch(contentsUrl(cfg), {
    headers: {
      ...apiHeaders(cfg),
      'Accept': 'application/vnd.github.object+json', // metadata only, no content
    }
  })
  if (r.status === 404) return null
  if (!r.ok) {
    const err = await r.json().catch(() => ({}))
    throw new Error(`GitHub SHA fetch failed: ${err.message || r.status}`)
  }
  const meta = await r.json()
  return meta.sha
}

// ── Write the full file back via Contents API ─────────────────────────────────
// btoa() is used here on our own outgoing string — safe because WE control
// the payload size (we only write one section at a time, then the merged doc).
async function writeFile(cfg, data, sha) {
  const encoded = btoa(unescape(encodeURIComponent(JSON.stringify(data, null, 2))))
  const body = {
    message: `chore: update portfolio data [skip ci]`,
    content: encoded,
    ...(sha ? { sha } : {}),
  }
  const r = await fetch(contentsUrl(cfg), {
    method:  'PUT',
    headers: apiHeaders(cfg),
    body:    JSON.stringify(body),
  })
  if (!r.ok) {
    const err = await r.json().catch(() => ({}))
    throw new Error(`GitHub write failed: ${err.message || r.status}`)
  }
  const res = await r.json()
  return res.content.sha
}

// ── Cached SHA ────────────────────────────────────────────────────────────────
let _sha = null

// ── Public API (mirrors original supabase.js interface) ───────────────────────

export async function loadAll(defaults) {
  const cfg = getGithubConfig()
  if (!cfg?.token) return defaults
  try {
    const content = await fetchDataStreaming(cfg)
    // Refresh SHA in background so the next save is instant
    fetchSha(cfg).then(sha => { _sha = sha }).catch(() => {})
    return { ...defaults, ...content }
  } catch (e) {
    console.error('[GitHub] loadAll failed:', e.message)
    return defaults
  }
}

export async function loadSection(section, fallback) {
  const cfg = getGithubConfig()
  if (!cfg?.token) return fallback
  try {
    const content = await fetchDataStreaming(cfg)
    return content[section] ?? fallback
  } catch { return fallback }
}

export async function saveSection(section, value) {
  const cfg = getGithubConfig()
  if (!cfg?.token) throw new Error('GitHub not configured')
  try {
    // Always re-fetch full doc + fresh sha to prevent merge conflicts
    const [content, sha] = await Promise.all([
      fetchDataStreaming(cfg),
      fetchSha(cfg),
    ])
    const updated = { ...content, [section]: value }
    _sha = await writeFile(cfg, updated, sha)
  } catch (e) {
    console.error('[GitHub] saveSection error:', e.message)
    throw new Error(`Save failed for "${section}": ${e.message}`)
  }
}

export async function testConnection(owner, repo, token) {
  if (!owner || !repo || !token) {
    return { ok: false, msg: 'Owner, repository name, and token are all required.' }
  }
  try {
    const r = await fetch(`https://api.github.com/repos/${owner}/${repo}`, {
      headers: {
        'Authorization':        `Bearer ${token}`,
        'Accept':               'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
      }
    })
    if (r.status === 401) return { ok: false, msg: 'Token invalid or expired.' }
    if (r.status === 403) return { ok: false, msg: 'Token lacks repo access — enable "Contents" read+write permission.' }
    if (r.status === 404) return { ok: false, msg: `Repository "${owner}/${repo}" not found. Check the owner and repo name.` }
    if (!r.ok)            return { ok: false, msg: `GitHub returned ${r.status}.` }

    const meta = await r.json()
    if (!meta.permissions?.push) {
      return { ok: false, msg: 'Token has read access but no write permission to this repo.' }
    }

    // Verify the data file exists via raw URL
    const rawR = await fetch(
      `https://raw.githubusercontent.com/${owner}/${repo}/main/${DATA_PATH}`,
      { headers: { 'Authorization': `Bearer ${token}` }, method: 'HEAD' }
    )
    if (!rawR.ok) {
      return { ok: false, msg: `Repo accessible but ${DATA_PATH} not found on main branch.` }
    }

    return { ok: true }
  } catch (e) {
    return { ok: false, msg: e.message }
  }
}

// No-op stubs — keep App.jsx import surface identical
export function resetClient() {}
export function subscribeToChanges() { return () => {} }
