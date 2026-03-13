// ── GitHub Storage Layer v2 ───────────────────────────────────────────────────
// Reads data.json via the GitHub Contents API using the
// application/vnd.github.raw+json Accept header — this returns the file body
// directly (no base64 wrapping) AND supports CORS from browser pages.
//
// raw.githubusercontent.com does NOT support CORS with Authorization headers
// from browsers — any fetch() to it with a token throws "Failed to fetch"
// (CORS preflight blocked). This version avoids that entirely.
//
// Writes still use the standard Contents API (PUT) which requires the SHA.

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

function contentsUrl(cfg) {
  return `https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${DATA_PATH}`
}

// Standard JSON headers (for writes + SHA fetches)
function apiHeaders(cfg) {
  return {
    'Authorization':        `Bearer ${cfg.token}`,
    'Accept':               'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'Content-Type':         'application/json',
  }
}

// Raw-body headers — returns file content directly, no base64, CORS-safe
function rawHeaders(cfg) {
  return {
    'Authorization':        `Bearer ${cfg.token}`,
    'Accept':               'application/vnd.github.raw+json',
    'X-GitHub-Api-Version': '2022-11-28',
  }
}

// ── Read: Contents API with raw media type ────────────────────────────────────
// Using Accept: application/vnd.github.raw+json tells GitHub to return the
// file body as plain text in the response body instead of a JSON envelope
// with a base64-encoded content field. This:
//   ✓ Works with CORS from browser pages (unlike raw.githubusercontent.com)
//   ✓ Avoids the atob() single-call crash on large files
//   ✓ Streams via ReadableStream + TextDecoder
async function fetchDataStreaming(cfg) {
  const r = await fetch(contentsUrl(cfg), {
    headers: rawHeaders(cfg),
    cache:   'no-store',
  })

  if (r.status === 404) return {}
  if (!r.ok) {
    const msg = await r.text().catch(() => r.status)
    throw new Error(`GitHub read failed: ${msg}`)
  }

  // Stream in 64 KB chunks — never blocks the main thread on large files
  const decoder = new TextDecoder('utf-8')
  const reader  = r.body.getReader()
  const parts   = []

  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    parts.push(decoder.decode(value, { stream: true }))
  }
  parts.push(decoder.decode()) // flush

  return JSON.parse(parts.join(''))
}

// ── Fetch only the SHA (standard JSON envelope, tiny response) ────────────────
async function fetchSha(cfg) {
  const r = await fetch(contentsUrl(cfg), {
    headers: apiHeaders(cfg),
  })
  if (r.status === 404) return null
  if (!r.ok) {
    const err = await r.json().catch(() => ({}))
    throw new Error(`GitHub SHA fetch failed: ${err.message || r.status}`)
  }
  const meta = await r.json()
  return meta.sha
}

// ── Write: PUT the full file back ─────────────────────────────────────────────
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

// ── Public API ────────────────────────────────────────────────────────────────

export async function loadAll(defaults) {
  const cfg = getGithubConfig()
  if (!cfg?.token) return defaults
  try {
    const content = await fetchDataStreaming(cfg)
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
    // Step 1: verify repo access + write permission
    const repoR = await fetch(`https://api.github.com/repos/${owner}/${repo}`, {
      headers: {
        'Authorization':        `Bearer ${token}`,
        'Accept':               'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
      }
    })
    if (repoR.status === 401) return { ok: false, msg: 'Token invalid or expired.' }
    if (repoR.status === 403) return { ok: false, msg: 'Token lacks repo access — enable "Contents" read+write permission.' }
    if (repoR.status === 404) return { ok: false, msg: `Repository "${owner}/${repo}" not found. Check the owner and repo name.` }
    if (!repoR.ok)            return { ok: false, msg: `GitHub returned ${repoR.status}.` }

    const meta = await repoR.json()
    if (!meta.permissions?.push) {
      return { ok: false, msg: 'Token has read access but no write permission to this repo.' }
    }

    // Step 2: verify data.json is reachable via Contents API (CORS-safe)
    const fileR = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/contents/${DATA_PATH}`,
      {
        method: 'HEAD',
        headers: {
          'Authorization':        `Bearer ${token}`,
          'Accept':               'application/vnd.github+json',
          'X-GitHub-Api-Version': '2022-11-28',
        }
      }
    )
    if (fileR.status === 404) {
      return { ok: false, msg: `Repo accessible but ${DATA_PATH} not found. Check the file exists on the main branch.` }
    }
    if (!fileR.ok) {
      return { ok: false, msg: `Could not access ${DATA_PATH}: HTTP ${fileR.status}` }
    }

    return { ok: true }
  } catch (e) {
    // Network-level error (CORS, offline, DNS)
    return { ok: false, msg: `Network error: ${e.message}. Check your token and repo name.` }
  }
}

// No-op stubs — keep App.jsx import surface identical
export function resetClient() {}
export function subscribeToChanges() { return () => {} }
