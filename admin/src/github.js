// ── GitHub Storage Layer v3 — split-file edition ──────────────────────────────
//
// data.json is now split into 5 smaller files to avoid GitHub's 1 MB base64
// encoding limit and to keep browser memory usage manageable:
//
//   portfolio/data_main.json        — about, contact, experience, skills, projects, flags
//   portfolio/data_creds_1.json     — credentials[0..83]
//   portfolio/data_creds_2.json     — credentials[84..167]
//   portfolio/data_creds_3.json     — credentials[168..251]
//   portfolio/data_creds_4.json     — credentials[252..335]
//   portfolio/data_creds_5.json     — credentials[336..419]  (overflow buffer)
//
// Public API is identical to v2 (loadAll, saveSection, testConnection, etc.)
// so App.jsx requires zero changes.
//
// Reads  → all 5 files in parallel via Contents API + raw media type (CORS-safe)
// Writes → only the affected file is re-uploaded (fast, minimal API calls)
//
// ─────────────────────────────────────────────────────────────────────────────

const CONFIG_KEY = 'portfolio_github_config'

export function getGithubConfig() {
  try {
    // sessionStorage: token is cleared when the tab/browser closes, limiting exposure window
    const raw = sessionStorage.getItem(CONFIG_KEY)
    return raw ? JSON.parse(raw) : null
  } catch { return null }
}
export function saveGithubConfig(owner, repo, token) {
  sessionStorage.setItem(CONFIG_KEY, JSON.stringify({ owner, repo, token }))
}
export function clearGithubConfig() {
  sessionStorage.removeItem(CONFIG_KEY)
}

// ── File map ─────────────────────────────────────────────────────────────────
// Which logical sections live in which physical file.
const MAIN_SECTIONS = ['about', 'contact', 'experience', 'skills', 'projects', 'flags']
const CRED_FILES    = [
  'portfolio/data_creds_1.json',
  'portfolio/data_creds_2.json',
  'portfolio/data_creds_3.json',
  'portfolio/data_creds_4.json',
  'portfolio/data_creds_5.json',  // added: capacity now 5 × 84 = 420
]
const MAIN_FILE     = 'portfolio/data_main.json'
const CRED_CHUNK    = 84   // max credentials per file (5 files × 84 = 420 total capacity)

// ── URL builders ─────────────────────────────────────────────────────────────
function contentsUrl(cfg, path) {
  return `https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${path}`
}
function apiHeaders(cfg) {
  return {
    'Authorization':        `Bearer ${cfg.token}`,
    'Accept':               'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'Content-Type':         'application/json',
  }
}
function rawHeaders(cfg) {
  return {
    'Authorization':        `Bearer ${cfg.token}`,
    'Accept':               'application/vnd.github.raw+json',
    'X-GitHub-Api-Version': '2022-11-28',
  }
}

// ── Stream-read a single file ─────────────────────────────────────────────────
async function fetchFileStreaming(cfg, path) {
  const r = await fetch(contentsUrl(cfg, path), {
    headers: rawHeaders(cfg),
    cache:   'no-store',
  })
  if (r.status === 404) return null
  if (!r.ok) {
    const msg = await r.text().catch(() => r.status)
    throw new Error(`GitHub read failed (${path}): ${msg}`)
  }
  const decoder = new TextDecoder('utf-8')
  const reader  = r.body.getReader()
  const parts   = []
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    parts.push(decoder.decode(value, { stream: true }))
  }
  parts.push(decoder.decode())
  return JSON.parse(parts.join(''))
}

// ── Fetch SHA for a single file ───────────────────────────────────────────────
async function fetchSha(cfg, path) {
  const r = await fetch(contentsUrl(cfg, path), { headers: apiHeaders(cfg) })
  if (r.status === 404) return null
  if (!r.ok) {
    const err = await r.json().catch(() => ({}))
    throw new Error(`GitHub SHA fetch failed (${path}): ${err.message || r.status}`)
  }
  const meta = await r.json()
  return meta.sha
}

// ── Write a single file ───────────────────────────────────────────────────────
// Safe UTF-8 → base64 encoder.
// Encodes in 8 KB chunks to avoid call-stack overflow on large credential files (3+ MB).
// Replaces the deprecated btoa(unescape(encodeURIComponent(…))) pattern which could
// silently corrupt non-ASCII characters (Arabic names, accented chars, emoji).
function safeBase64Encode(str) {
  const bytes = new TextEncoder().encode(str)
  let binary = ''
  const CHUNK = 8192
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK))
  }
  return btoa(binary)
}

async function writeFile(cfg, path, data, sha, _retries = 2) {
  const encoded = safeBase64Encode(JSON.stringify(data, null, 2))
  const body = {
    message: `chore: update ${path.split('/').pop().replace('.json','')} — ${new Date().toISOString()}`,
    content: encoded,
    ...(sha ? { sha } : {}),
  }
  const r = await fetch(contentsUrl(cfg, path), {
    method:  'PUT',
    headers: apiHeaders(cfg),
    body:    JSON.stringify(body),
  })
  // 409 Conflict = SHA mismatch (file was updated between our read and write).
  // Re-fetch the latest SHA and retry up to _retries times.
  if (r.status === 409 && _retries > 0) {
    console.warn(`[GitHub] 409 conflict on ${path} — re-fetching SHA and retrying (${_retries} left)`)
    const freshSha = await fetchSha(cfg, path)
    return writeFile(cfg, path, data, freshSha, _retries - 1)
  }
  if (!r.ok) {
    const err = await r.json().catch(() => ({}))
    throw new Error(`GitHub write failed (${path}): ${err.message || r.status}`)
  }
  const res = await r.json()
  return res.content.sha
}

// ── Split credentials into chunks ─────────────────────────────────────────────
function splitCredentials(allCreds) {
  const maxCapacity = CRED_FILES.length * CRED_CHUNK   // currently 5 × 84 = 420
  if (allCreds.length > maxCapacity) {
    throw new Error(
      `Credential count (${allCreds.length}) exceeds storage capacity (${maxCapacity}). ` +
      `Add portfolio/data_creds_6.json and update CRED_FILES in github.js to expand capacity.`
    )
  }
  const chunks = []
  for (let i = 0; i < CRED_FILES.length; i++) {
    chunks.push(allCreds.slice(i * CRED_CHUNK, (i + 1) * CRED_CHUNK))
  }
  return chunks
}

// ── Load all data (parallel) ──────────────────────────────────────────────────
export async function loadAll(defaults) {
  const cfg = getGithubConfig()
  if (!cfg?.token) return defaults

  try {
    // Fetch all 5 files simultaneously
    const [mainData, ...credDatas] = await Promise.all([
      fetchFileStreaming(cfg, MAIN_FILE),
      ...CRED_FILES.map(f => fetchFileStreaming(cfg, f).catch(() => null)),
    ])

    // Merge credentials from all chunk files
    const allCreds = []
    credDatas.forEach(cd => {
      if (cd && Array.isArray(cd.credentials)) allCreds.push(...cd.credentials)
    })

    return {
      ...defaults,
      ...(mainData || {}),
      credentials: allCreds.length ? allCreds : (defaults.credentials || []),
    }
  } catch (e) {
    console.error('[GitHub] loadAll failed:', e.message)
    return defaults
  }
}

// ── Save a single section ─────────────────────────────────────────────────────
// • For any main section (about/contact/skills// ─  …experience/projects/flags): read+update data_main.json only
// • For credentials: read all credential files, merge, re-split, rewrite all 5

export async function saveSection(section, value) {
  const cfg = getGithubConfig()
  if (!cfg?.token) throw new Error('GitHub not configured')

  try {
    if (MAIN_SECTIONS.includes(section)) {
      // ── Fast path: only touch data_main.json ──────────────────────────────
      const [mainData, sha] = await Promise.all([
        fetchFileStreaming(cfg, MAIN_FILE),
        fetchSha(cfg, MAIN_FILE),
      ])
      const updated = { ...(mainData || {}), [section]: value }
      await writeFile(cfg, MAIN_FILE, updated, sha)

    } else if (section === 'credentials') {
      // ── Credential path: re-split and write all 5 credential files ────────
      const chunks = splitCredentials(value)
      // Fetch all SHAs in parallel (needed for update)
      const shas = await Promise.all(CRED_FILES.map(f => fetchSha(cfg, f)))
      // Write all 5 files in parallel
      await Promise.all(CRED_FILES.map((f, i) =>
        writeFile(cfg, f, { credentials: chunks[i] || [] }, shas[i])
      ))

    } else {
      throw new Error(`Unknown section: "${section}"`)
    }
  } catch (e) {
    console.error('[GitHub] saveSection error:', e.message)
    throw new Error(`Save failed for "${section}": ${e.message}`)
  }
}

export async function loadSection(section, fallback) {
  const cfg = getGithubConfig()
  if (!cfg?.token) return fallback
  try {
    if (MAIN_SECTIONS.includes(section)) {
      const data = await fetchFileStreaming(cfg, MAIN_FILE)
      return data?.[section] ?? fallback
    } else if (section === 'credentials') {
      const datas = await Promise.all(CRED_FILES.map(f => fetchFileStreaming(cfg, f).catch(() => null)))
      const all = []
      datas.forEach(d => { if (d && Array.isArray(d.credentials)) all.push(...d.credentials) })
      return all.length ? all : fallback
    }
    return fallback
  } catch { return fallback }
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

    // Step 2: verify data_main.json is reachable
    const fileR = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/contents/${MAIN_FILE}`,
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
      return { ok: false, msg: `Repo accessible but ${MAIN_FILE} not found. Did you upload the split data files?` }
    }
    if (!fileR.ok) {
      return { ok: false, msg: `Could not access ${MAIN_FILE}: HTTP ${fileR.status}` }
    }

    return { ok: true }
  } catch (e) {
    return { ok: false, msg: `Network error: ${e.message}. Check your token and repo name.` }
  }
}

// No-op stubs — keep App.jsx import surface identical
export function resetClient() {}
export function subscribeToChanges() { return () => {} }
