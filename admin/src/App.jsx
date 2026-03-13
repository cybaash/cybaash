import { useState, useEffect, useRef, useCallback } from 'react'
import {
  getSupabaseConfig, saveSupabaseConfig, clearSupabaseConfig, resetClient,
  loadAll, saveSection, subscribeToChanges, testConnection
} from './supabase.js'

// ── Fonts ──────────────────────────────────────────────────────────────────
const FontLink = () => (
  <style>{`@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@400;500;600;700&display=swap');`}</style>
)

// ── Constants ──────────────────────────────────────────────────────────────
const ADMIN_CREDS_KEY = 'portfolio_admin_pw'
const DEFAULT_PW      = 'Aasiq@2025'

const NAV = [
  { id:'dashboard',   label:'Dashboard',   icon:'⬡' },
  { id:'about',       label:'About',        icon:'◈' },
  { id:'skills',      label:'Skills',       icon:'◎' },
  { id:'credentials', label:'Credentials',  icon:'◆' },
  { id:'projects',    label:'Projects',     icon:'◉' },
  { id:'flags',       label:'CTF Flags',    icon:'🚩' },
  { id:'experience',  label:'Experience',   icon:'◍' },
  { id:'contact',     label:'Contact',      icon:'◌' },
  { id:'settings',    label:'Settings',     icon:'⚙' },
]
const SKILL_LEVELS = ['Beginner','Intermediate','Advanced','Expert']
const FLAG_EMOJIS  = ['🇮🇳','🇺🇸','🇬🇧','🇦🇺','🇨🇦','🇩🇪','🇫🇷','🇸🇬','🇦🇪','🇯🇵','🇰🇷','🇧🇷','🇲🇾','🇳🇱','🇸🇪']

const uid  = () => Math.random().toString(36).slice(2,10)
const now  = () => new Date().toLocaleDateString('en-GB',{day:'2-digit',month:'short',year:'numeric'})

function fileToB64(file) {
  return new Promise(res => { const r=new FileReader(); r.onload=e=>res(e.target.result); r.readAsDataURL(file) })
}
function getAdminPw() { return localStorage.getItem(ADMIN_CREDS_KEY) || DEFAULT_PW }
function setAdminPw(pw) { localStorage.setItem(ADMIN_CREDS_KEY, pw) }

// ── CSS ────────────────────────────────────────────────────────────────────
const CSS = `
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #04080a; color: #c8d8c0; font-family: 'Rajdhani', sans-serif; }
  :root {
    --g:#00ff41; --g2:#00cc34; --g3:#008822;
    --bg:#04080a; --bg1:#080f0a; --bg2:#0c160d; --bg3:#101e11;
    --bd:#1a2e1c; --tx:#c8d8c0; --tx2:#88a880; --tx3:#4a6048;
    --red:#ff4040; --amber:#ffaa00; --blue:#44aaff;
  }
  ::-webkit-scrollbar{width:6px;height:6px}
  ::-webkit-scrollbar-track{background:var(--bg1)}
  ::-webkit-scrollbar-thumb{background:var(--g3);border-radius:3px}

  .shell{display:flex;height:100vh;overflow:hidden;position:relative}
  .shell::before{content:'';position:fixed;inset:0;pointer-events:none;z-index:0;
    background-image:linear-gradient(rgba(0,255,65,.03) 1px,transparent 1px),linear-gradient(90deg,rgba(0,255,65,.03) 1px,transparent 1px);
    background-size:40px 40px}

  .sidebar{width:220px;min-width:220px;background:var(--bg1);border-right:1px solid var(--bd);
    display:flex;flex-direction:column;z-index:10;position:relative}
  .sidebar::after{content:'';position:absolute;top:0;right:0;width:1px;height:100%;
    background:linear-gradient(to bottom,transparent,var(--g3),transparent)}
  .logo{padding:24px 20px 18px;border-bottom:1px solid var(--bd);font-family:'Share Tech Mono',monospace}
  .logo-title{color:var(--g);font-size:15px;letter-spacing:3px}
  .logo-sub{color:var(--tx3);font-size:10px;letter-spacing:2px;margin-top:3px}
  .sb-status{display:flex;align-items:center;gap:6px;padding:8px 20px;font-family:'Share Tech Mono',monospace;font-size:9px;letter-spacing:1px;border-bottom:1px solid var(--bd)}
  .nav{flex:1;padding:12px 0;overflow-y:auto}
  .nav-item{display:flex;align-items:center;gap:10px;padding:10px 20px;cursor:pointer;
    font-size:14px;font-weight:600;letter-spacing:1px;color:var(--tx2);transition:all .2s;
    border-left:2px solid transparent;user-select:none}
  .nav-item:hover{color:var(--g);background:rgba(0,255,65,.04);border-left-color:var(--g3)}
  .nav-item.active{color:var(--g);background:rgba(0,255,65,.08);border-left-color:var(--g)}
  .nav-icon{font-size:16px;width:20px;text-align:center}
  .nav-badge{margin-left:auto;background:var(--g3);color:var(--bg);font-size:10px;font-weight:700;padding:1px 6px;border-radius:10px;font-family:'Share Tech Mono',monospace}
  .sidebar-footer{padding:16px 20px;border-top:1px solid var(--bd);font-family:'Share Tech Mono',monospace;font-size:10px;color:var(--tx3)}
  .logout-btn{width:100%;padding:8px;background:transparent;border:1px solid var(--bd);color:var(--tx3);
    cursor:pointer;font-family:'Share Tech Mono',monospace;font-size:11px;letter-spacing:2px;
    transition:all .2s;margin-top:10px;text-transform:uppercase}
  .logout-btn:hover{border-color:var(--red);color:var(--red)}

  .main{flex:1;display:flex;flex-direction:column;overflow:hidden;z-index:1}
  .topbar{display:flex;align-items:center;justify-content:space-between;padding:0 28px;height:56px;
    border-bottom:1px solid var(--bd);background:var(--bg1);flex-shrink:0}
  .topbar-left{display:flex;align-items:center;gap:12px}
  .topbar-title{font-family:'Share Tech Mono',monospace;font-size:13px;color:var(--g);letter-spacing:2px}
  .topbar-breadcrumb{color:var(--tx3);font-size:12px;letter-spacing:1px}
  .topbar-right{display:flex;align-items:center;gap:16px}
  .status-dot{width:8px;height:8px;border-radius:50%;background:var(--g);box-shadow:0 0 8px var(--g);animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  .topbar-time{font-family:'Share Tech Mono',monospace;font-size:11px;color:var(--tx3)}
  .content{flex:1;overflow-y:auto;padding:28px}

  .section-header{display:flex;align-items:center;justify-content:space-between;
    margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid var(--bd)}
  .section-title{font-family:'Share Tech Mono',monospace;font-size:16px;color:var(--g);letter-spacing:3px;text-transform:uppercase}
  .section-count{color:var(--tx3);font-size:12px;margin-left:12px}

  .btn{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;border:1px solid;cursor:pointer;
    font-family:'Rajdhani',sans-serif;font-weight:600;font-size:13px;letter-spacing:1px;
    transition:all .2s;text-transform:uppercase;background:transparent}
  .btn-green{border-color:var(--g);color:var(--g)} .btn-green:hover{background:var(--g);color:var(--bg)}
  .btn-red{border-color:var(--red);color:var(--red)} .btn-red:hover{background:var(--red);color:#fff}
  .btn-amber{border-color:var(--amber);color:var(--amber)} .btn-amber:hover{background:var(--amber);color:var(--bg)}
  .btn-ghost{border-color:var(--bd);color:var(--tx2)} .btn-ghost:hover{border-color:var(--tx2);color:var(--tx)}
  .btn-blue{border-color:var(--blue);color:var(--blue)} .btn-blue:hover{background:var(--blue);color:var(--bg)}
  .btn-sm{padding:5px 10px;font-size:11px} .btn-icon{padding:6px 8px}
  .btn:disabled{opacity:.4;cursor:not-allowed}

  .card{background:var(--bg1);border:1px solid var(--bd);padding:20px;position:relative;transition:border-color .2s}
  .card:hover{border-color:var(--g3)}
  .card-corner{position:absolute;width:8px;height:8px;border-color:var(--g3);border-style:solid}
  .card-corner.tl{top:-1px;left:-1px;border-width:2px 0 0 2px}
  .card-corner.tr{top:-1px;right:-1px;border-width:2px 2px 0 0}
  .card-corner.bl{bottom:-1px;left:-1px;border-width:0 0 2px 2px}
  .card-corner.br{bottom:-1px;right:-1px;border-width:0 2px 2px 0}

  .grid-2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  .grid-3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px}

  .data-table{width:100%;border-collapse:collapse;font-size:13px}
  .data-table th{text-align:left;padding:10px 14px;font-family:'Share Tech Mono',monospace;font-size:10px;
    letter-spacing:2px;color:var(--g);text-transform:uppercase;border-bottom:1px solid var(--bd);
    background:var(--bg2);font-weight:400}
  .data-table td{padding:12px 14px;border-bottom:1px solid rgba(26,46,28,.5);color:var(--tx);vertical-align:middle}
  .data-table tr:hover td{background:rgba(0,255,65,.02)}
  .data-table tr:last-child td{border-bottom:none}

  .badge{display:inline-block;padding:2px 8px;font-size:11px;font-weight:700;letter-spacing:1px;border:1px solid}
  .badge-green{border-color:var(--g3);color:var(--g);background:rgba(0,255,65,.08)}
  .badge-amber{border-color:#664400;color:var(--amber);background:rgba(255,170,0,.08)}
  .badge-red{border-color:#660000;color:var(--red);background:rgba(255,64,64,.08)}
  .badge-blue{border-color:#004488;color:#44aaff;background:rgba(68,170,255,.08)}

  .form-group{margin-bottom:16px}
  .form-label{display:block;margin-bottom:6px;font-family:'Share Tech Mono',monospace;
    font-size:10px;letter-spacing:2px;color:var(--g);text-transform:uppercase}
  .form-input,.form-select,.form-textarea{width:100%;padding:10px 14px;background:var(--bg2);
    border:1px solid var(--bd);color:var(--tx);font-family:'Rajdhani',sans-serif;font-size:14px;
    font-weight:500;transition:border-color .2s;outline:none}
  .form-input:focus,.form-select:focus,.form-textarea:focus{border-color:var(--g3);box-shadow:0 0 0 1px var(--g3)}
  .form-select{cursor:pointer} .form-select option{background:var(--bg2)}
  .form-textarea{resize:vertical;min-height:90px;line-height:1.5}
  .form-row{display:grid;gap:16px} .form-row-2{grid-template-columns:1fr 1fr} .form-row-3{grid-template-columns:1fr 1fr 1fr}
  .form-hint{font-family:'Share Tech Mono',monospace;font-size:10px;color:var(--tx3);margin-top:4px;letter-spacing:.5px}

  .file-drop{border:2px dashed var(--bd);padding:24px;text-align:center;cursor:pointer;transition:all .2s;position:relative}
  .file-drop:hover,.file-drop.dragging{border-color:var(--g3);background:rgba(0,255,65,.03)}
  .file-drop input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%}
  .file-drop-icon{font-size:28px;margin-bottom:8px}
  .file-drop-text{font-family:'Share Tech Mono',monospace;font-size:11px;color:var(--tx3);letter-spacing:1px}
  .file-drop-sub{font-size:10px;color:var(--tx3);margin-top:4px;opacity:.6}
  .file-preview{margin-top:12px;padding:10px 14px;background:var(--bg2);border:1px solid var(--g3);display:flex;align-items:center;gap:10px}
  .file-preview img{width:40px;height:40px;object-fit:cover;border:1px solid var(--bd)}
  .file-preview-name{font-family:'Share Tech Mono',monospace;font-size:11px;color:var(--g);flex:1;word-break:break-all}

  .modal-overlay{position:fixed;inset:0;background:rgba(4,8,10,.88);display:flex;align-items:center;
    justify-content:center;z-index:100;backdrop-filter:blur(4px);animation:fadeIn .15s ease}
  @keyframes fadeIn{from{opacity:0;transform:scale(.97)}to{opacity:1;transform:scale(1)}}
  .modal{background:var(--bg1);border:1px solid var(--bd);width:min(680px,95vw);max-height:88vh;
    overflow-y:auto;position:relative;box-shadow:0 0 60px rgba(0,255,65,.08)}
  .modal-header{display:flex;align-items:center;justify-content:space-between;padding:18px 24px;
    border-bottom:1px solid var(--bd);position:sticky;top:0;background:var(--bg1);z-index:1}
  .modal-title{font-family:'Share Tech Mono',monospace;font-size:13px;color:var(--g);letter-spacing:2px}
  .modal-close{background:none;border:1px solid var(--bd);color:var(--tx3);width:28px;height:28px;
    cursor:pointer;font-size:16px;transition:all .2s;display:flex;align-items:center;justify-content:center}
  .modal-close:hover{border-color:var(--red);color:var(--red)}
  .modal-body{padding:24px}
  .modal-footer{display:flex;justify-content:flex-end;gap:10px;padding:16px 24px;
    border-top:1px solid var(--bd);position:sticky;bottom:0;background:var(--bg1)}

  /* Setup wizard / Login */
  .auth-shell{min-height:100vh;display:flex;align-items:center;justify-content:center;
    background:var(--bg);position:relative;overflow:hidden}
  .auth-shell::before{content:'';position:fixed;inset:0;pointer-events:none;
    background-image:linear-gradient(rgba(0,255,65,.04) 1px,transparent 1px),linear-gradient(90deg,rgba(0,255,65,.04) 1px,transparent 1px);
    background-size:40px 40px}
  .auth-box{width:460px;background:var(--bg1);border:1px solid var(--bd);padding:40px;
    position:relative;z-index:1;box-shadow:0 0 80px rgba(0,255,65,.06);animation:fadeIn .3s ease}
  .auth-logo{text-align:center;margin-bottom:32px}
  .auth-logo-icon{font-size:36px;margin-bottom:10px;filter:drop-shadow(0 0 12px var(--g))}
  .auth-logo-title{font-family:'Share Tech Mono',monospace;color:var(--g);font-size:14px;letter-spacing:4px}
  .auth-logo-sub{color:var(--tx3);font-size:11px;letter-spacing:2px;margin-top:4px}
  .auth-error{padding:10px 14px;border:1px solid var(--red);color:var(--red);
    font-family:'Share Tech Mono',monospace;font-size:11px;letter-spacing:1px;
    margin-bottom:16px;background:rgba(255,64,64,.06)}
  .auth-success{padding:10px 14px;border:1px solid var(--g);color:var(--g);
    font-family:'Share Tech Mono',monospace;font-size:11px;letter-spacing:1px;
    margin-bottom:16px;background:rgba(0,255,65,.06)}
  .step-indicator{display:flex;gap:8px;justify-content:center;margin-bottom:28px}
  .step-dot{width:8px;height:8px;border-radius:50%;background:var(--bg3);border:1px solid var(--bd);transition:all .3s}
  .step-dot.active{background:var(--g);border-color:var(--g);box-shadow:0 0 8px var(--g)}
  .step-dot.done{background:var(--g3);border-color:var(--g3)}

  /* Dashboard */
  .stat-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:28px}
  .stat-card{background:var(--bg1);border:1px solid var(--bd);padding:20px;position:relative;overflow:hidden}
  .stat-card::after{content:'';position:absolute;bottom:0;left:0;right:0;height:2px;background:var(--g3)}
  .stat-label{font-family:'Share Tech Mono',monospace;font-size:9px;letter-spacing:2px;color:var(--tx3);text-transform:uppercase}
  .stat-value{font-family:'Share Tech Mono',monospace;font-size:32px;color:var(--g);margin:6px 0 2px;line-height:1}
  .stat-sub{font-size:11px;color:var(--tx3)}
  .activity-item{display:flex;align-items:flex-start;gap:12px;padding:12px 0;border-bottom:1px solid rgba(26,46,28,.4)}
  .activity-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;margin-top:4px}
  .activity-text{font-size:13px;color:var(--tx);line-height:1.4}
  .activity-time{font-family:'Share Tech Mono',monospace;font-size:10px;color:var(--tx3);margin-top:2px}

  /* Skills */
  .skill-bar-wrap{background:var(--bg2);height:4px;border-radius:2px;overflow:hidden;flex:1}
  .skill-bar{height:100%;background:var(--g);border-radius:2px}

  /* Tags */
  .tag-wrap{display:flex;flex-wrap:wrap;gap:6px;padding:8px;background:var(--bg2);
    border:1px solid var(--bd);min-height:42px;cursor:text}
  .tag-wrap:focus-within{border-color:var(--g3)}
  .tag{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;
    background:rgba(0,255,65,.1);border:1px solid var(--g3);color:var(--g);font-size:12px;font-weight:600}
  .tag-remove{background:none;border:none;color:var(--g3);cursor:pointer;font-size:14px;line-height:1;padding:0}
  .tag-input{flex:1;min-width:80px;background:none;border:none;outline:none;color:var(--tx);
    font-family:'Rajdhani',sans-serif;font-size:14px}

  /* Empty */
  .empty-state{text-align:center;padding:60px 20px;font-family:'Share Tech Mono',monospace;color:var(--tx3)}
  .empty-state-icon{font-size:36px;margin-bottom:12px;opacity:.5}
  .empty-state-text{font-size:12px;letter-spacing:2px}

  /* Misc */
  .img-thumb{width:48px;height:48px;object-fit:cover;border:1px solid var(--bd)}
  .confirm-box{background:var(--bg1);border:1px solid var(--red);padding:28px 32px;width:340px;
    text-align:center;box-shadow:0 0 40px rgba(255,64,64,.12);animation:fadeIn .15s ease}
  .confirm-icon{font-size:32px;margin-bottom:12px}
  .confirm-msg{font-size:14px;color:var(--tx);margin-bottom:20px;line-height:1.5}
  .confirm-btns{display:flex;gap:10px;justify-content:center}
  .toggle{position:relative;display:inline-block;width:36px;height:20px}
  .toggle input{opacity:0;width:0;height:0}
  .toggle-slider{position:absolute;cursor:pointer;inset:0;background:var(--bg2);border:1px solid var(--bd);transition:.2s;border-radius:20px}
  .toggle-slider::before{content:'';position:absolute;height:12px;width:12px;left:3px;bottom:3px;background:var(--tx3);transition:.2s;border-radius:50%}
  .toggle input:checked + .toggle-slider{background:rgba(0,255,65,.15);border-color:var(--g)}
  .toggle input:checked + .toggle-slider::before{transform:translateX(16px);background:var(--g)}
  .divider{border:none;border-top:1px solid var(--bd);margin:20px 0}

  /* Sync toast */
  .sync-toast{position:fixed;bottom:20px;right:20px;z-index:200;background:var(--bg1);
    border:1px solid var(--g3);padding:10px 18px;font-family:'Share Tech Mono',monospace;
    font-size:11px;color:var(--g);letter-spacing:1px;box-shadow:0 0 20px rgba(0,255,65,.15);
    display:flex;align-items:center;gap:8px;animation:fadeIn .2s ease}
  .sync-spinner{width:10px;height:10px;border:1.5px solid var(--g3);border-top-color:var(--g);
    border-radius:50%;animation:spin .7s linear infinite}
  @keyframes spin{to{transform:rotate(360deg)}}

  /* Saving overlay */
  .saving-bar{position:fixed;top:0;left:0;right:0;height:2px;background:var(--g);
    box-shadow:0 0 8px var(--g);z-index:300;animation:slideIn .3s ease}
  @keyframes slideIn{from{transform:scaleX(0);transform-origin:left}to{transform:scaleX(1)}}

  @media(max-width:768px){
    .sidebar{width:60px;min-width:60px}
    .nav-item span,.logo-sub,.sb-status,.sidebar-footer div:not(:last-child){display:none}
    .stat-grid{grid-template-columns:1fr 1fr}
    .form-row-2,.form-row-3{grid-template-columns:1fr}
    .grid-2,.grid-3{grid-template-columns:1fr}
  }
`

// ── Shared components ──────────────────────────────────────────────────────
function TagInput({ value=[], onChange, placeholder='Add tag, press Enter' }) {
  const [input, setInput] = useState('')
  const add = e => {
    if ((e.key==='Enter'||e.key===',') && input.trim()) {
      e.preventDefault()
      if (!value.includes(input.trim())) onChange([...value, input.trim()])
      setInput('')
    }
  }
  return (
    <div className="tag-wrap" onClick={e=>e.currentTarget.querySelector('input').focus()}>
      {value.map(t=>(
        <span className="tag" key={t}>{t}
          <button className="tag-remove" onClick={()=>onChange(value.filter(x=>x!==t))}>×</button>
        </span>
      ))}
      <input className="tag-input" value={input} onChange={e=>setInput(e.target.value)}
        onKeyDown={add} placeholder={value.length?'':' '+placeholder}/>
    </div>
  )
}

function FileUpload({ value, onChange, accept='image/*', label='Upload File' }) {
  const [dragging, setDragging] = useState(false)
  const handleFile = async f => { if(!f) return; const b=await fileToB64(f); onChange(b) }
  const isImg = value && value.startsWith('data:image')
  const isPDF = value && value.includes('pdf')
  return (
    <div>
      <div className={`file-drop${dragging?' dragging':''}`}
        onDragOver={e=>{e.preventDefault();setDragging(true)}}
        onDragLeave={()=>setDragging(false)}
        onDrop={e=>{e.preventDefault();setDragging(false);handleFile(e.dataTransfer.files[0])}}>
        <input type="file" accept={accept} onChange={e=>handleFile(e.target.files[0])}/>
        <div className="file-drop-icon">📁</div>
        <div className="file-drop-text">{label}</div>
        <div className="file-drop-sub">Drag & drop or click · Max 5MB</div>
      </div>
      {value && (
        <div className="file-preview">
          {isImg && <img src={value} alt="" className="img-thumb"/>}
          {isPDF && <span style={{fontSize:24}}>📄</span>}
          <span className="file-preview-name">File attached ✓</span>
          <button className="btn btn-red btn-sm" onClick={()=>onChange(null)}>Remove</button>
        </div>
      )}
    </div>
  )
}

function Confirm({ msg, onConfirm, onCancel }) {
  return (
    <div className="modal-overlay">
      <div className="confirm-box">
        <div className="confirm-icon">⚠</div>
        <div className="confirm-msg">{msg}</div>
        <div className="confirm-btns">
          <button className="btn btn-ghost" onClick={onCancel}>Cancel</button>
          <button className="btn btn-red" onClick={onConfirm}>Delete</button>
        </div>
      </div>
    </div>
  )
}

function Clock() {
  const [t, setT] = useState(new Date())
  useEffect(()=>{ const i=setInterval(()=>setT(new Date()),1000); return()=>clearInterval(i) },[])
  return <span className="topbar-time">{t.toLocaleTimeString('en-GB',{hour12:false})}</span>
}

function SyncToast({ state }) {
  if (state==='idle') return null
  return (
    <div className="sync-toast">
      {state==='saving' ? <><div className="sync-spinner"/> SAVING...</> : <>✓ SYNCED TO SUPABASE</>}
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// SETUP WIZARD (first run — no Supabase config yet)
// ══════════════════════════════════════════════════════════════════════════
function SetupWizard({ onComplete }) {
  const [step, setStep]   = useState(0) // 0=intro, 1=supabase, 2=done
  const [url, setUrl]     = useState('')
  const [key, setKey]     = useState('')
  const [testing, setTesting] = useState(false)
  const [err, setErr]     = useState('')
  const [ok, setOk]       = useState(false)

  const testAndSave = async () => {
    setTesting(true); setErr(''); setOk(false)
    const result = await testConnection(url.trim(), key.trim())
    setTesting(false)
    if (!result.ok) { setErr('Connection failed: ' + result.msg); return }
    saveSupabaseConfig(url.trim(), key.trim())
    setOk(true)
    setTimeout(() => { setStep(2) }, 1200)
  }

  return (
    <>
      <FontLink/>
      <style>{CSS}</style>
      <div className="auth-shell">
        <div className="auth-box" style={{maxWidth:500}}>
          <div style={{position:'absolute',top:-1,left:-1,right:-1,height:2,background:'var(--g)',boxShadow:'0 0 12px var(--g)'}}/>
          <div className="auth-logo">
            <div className="auth-logo-icon">⬡</div>
            <div className="auth-logo-title">PORTFOLIO OS</div>
            <div className="auth-logo-sub">FIRST RUN SETUP · v3.0</div>
          </div>

          <div className="step-indicator">
            {[0,1,2].map(i=>(
              <div key={i} className={`step-dot${step===i?' active':step>i?' done':''}`}/>
            ))}
          </div>

          {step===0 && (
            <div>
              <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:12,color:'var(--g)',letterSpacing:2,marginBottom:16}}>WELCOME TO PORTFOLIO OS</div>
              <p style={{fontSize:13,color:'var(--tx2)',lineHeight:1.7,marginBottom:20}}>
                This admin panel uses <strong style={{color:'var(--g)'}}>Supabase</strong> to store your portfolio data — so any device, any browser, anywhere in the world sees the same content in real-time.
              </p>
              <div style={{background:'var(--bg2)',border:'1px solid var(--bd)',padding:16,marginBottom:20,fontFamily:"'Share Tech Mono',monospace",fontSize:11}}>
                <div style={{color:'var(--g)',marginBottom:10,letterSpacing:2}}>WHAT YOU NEED:</div>
                {['A free Supabase account at supabase.com','A project with the portfolio_data table (SQL below)','Your Project URL + anon public key'].map((s,i)=>(
                  <div key={i} style={{color:'var(--tx2)',marginBottom:6,display:'flex',gap:8}}>
                    <span style={{color:'var(--g3)'}}>{'>'}</span>{s}
                  </div>
                ))}
              </div>
              <button className="btn btn-green" style={{width:'100%',justifyContent:'center'}} onClick={()=>setStep(1)}>
                ▶ START SETUP
              </button>
            </div>
          )}

          {step===1 && (
            <div>
              <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:12,color:'var(--g)',letterSpacing:2,marginBottom:16}}>STEP 1 — SUPABASE CONFIG</div>

              {/* SQL snippet */}
              <div style={{background:'var(--bg)',border:'1px solid var(--bd)',padding:14,marginBottom:20,borderRadius:0}}>
                <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:9,color:'var(--tx3)',letterSpacing:2,marginBottom:8}}>RUN THIS SQL IN SUPABASE → SQL EDITOR</div>
                <pre style={{fontFamily:"'Share Tech Mono',monospace",fontSize:11,color:'var(--g2)',lineHeight:1.6,whiteSpace:'pre-wrap',userSelect:'all'}}>
{`create table if not exists portfolio_data (
  id text primary key,
  data jsonb not null default '{}',
  updated_at timestamptz default now()
);

-- Public read (portfolio visitors)
alter table portfolio_data enable row level security;
create policy "public_read" on portfolio_data
  for select using (true);

-- Allow all writes (admin auth is handled by app)
create policy "admin_write" on portfolio_data
  for all using (true) with check (true);

-- Enable realtime
alter publication supabase_realtime add table portfolio_data;`}
                </pre>
                <button className="btn btn-ghost btn-sm" style={{marginTop:8}} onClick={()=>navigator.clipboard?.writeText(`create table if not exists portfolio_data (\n  id text primary key,\n  data jsonb not null default '{}',\n  updated_at timestamptz default now()\n);\nalter table portfolio_data enable row level security;\ncreate policy "public_read" on portfolio_data for select using (true);\ncreate policy "admin_write" on portfolio_data for all using (true) with check (true);\nalter publication supabase_realtime add table portfolio_data;`)}>
                  Copy SQL
                </button>
              </div>

              {err && <div className="auth-error">⚠ {err}</div>}
              {ok  && <div className="auth-success">✓ Connected successfully!</div>}

              <div className="form-group">
                <label className="form-label">Project URL</label>
                <input className="form-input" value={url} onChange={e=>setUrl(e.target.value)}
                  placeholder="https://xxxxxxxxxxxx.supabase.co"/>
                <div className="form-hint">Settings → API → Project URL</div>
              </div>
              <div className="form-group">
                <label className="form-label">Anon Public Key</label>
                <input className="form-input" value={key} onChange={e=>setKey(e.target.value)}
                  placeholder="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."/>
                <div className="form-hint">Settings → API → Project API Keys → anon public</div>
              </div>
              <button className="btn btn-green" style={{width:'100%',justifyContent:'center'}}
                onClick={testAndSave} disabled={testing||!url||!key}>
                {testing ? '⟳ TESTING CONNECTION...' : '▶ TEST & SAVE'}
              </button>
            </div>
          )}

          {step===2 && (
            <div style={{textAlign:'center'}}>
              <div style={{fontSize:48,marginBottom:16}}>✓</div>
              <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:14,color:'var(--g)',letterSpacing:2,marginBottom:12}}>SETUP COMPLETE</div>
              <p style={{fontSize:13,color:'var(--tx2)',lineHeight:1.7,marginBottom:24}}>Supabase is connected. Now log in with your admin password to start editing.</p>
              <button className="btn btn-green" style={{width:'100%',justifyContent:'center'}} onClick={onComplete}>
                ▶ GO TO LOGIN
              </button>
            </div>
          )}
        </div>
      </div>
    </>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// LOGIN
// ══════════════════════════════════════════════════════════════════════════
function Login({ onAuth }) {
  const [pw, setPw]   = useState('')
  const [err, setErr] = useState('')
  const attempt = () => {
    if (pw === getAdminPw()) { onAuth(); setErr('') }
    else setErr('Invalid password. Access denied.')
  }
  return (
    <>
      <FontLink/>
      <style>{CSS}</style>
      <div className="auth-shell">
        <div className="auth-box">
          <div style={{position:'absolute',top:-1,left:-1,right:-1,height:2,background:'var(--g)',boxShadow:'0 0 12px var(--g)'}}/>
          <div className="auth-logo">
            <div className="auth-logo-icon">⬡</div>
            <div className="auth-logo-title">PORTFOLIO OS</div>
            <div className="auth-logo-sub">ADMIN CONTROL PANEL · v3.0</div>
          </div>
          {err && <div className="auth-error">⚠ {err}</div>}
          <div className="form-group">
            <label className="form-label">Admin Password</label>
            <input className="form-input" type="password" value={pw}
              onChange={e=>setPw(e.target.value)}
              onKeyDown={e=>e.key==='Enter'&&attempt()}
              placeholder="••••••••" autoFocus/>
          </div>
          <button className="btn btn-green" style={{width:'100%',justifyContent:'center',padding:'12px',marginTop:8}} onClick={attempt}>
            ▶ AUTHENTICATE
          </button>
          <div style={{marginTop:20,fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--tx3)',textAlign:'center',letterSpacing:1}}>
            Contact admin if you forgot your password
          </div>
        </div>
      </div>
    </>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// DASHBOARD
// ══════════════════════════════════════════════════════════════════════════
function Dashboard({ data, lastSync, sbCfg }) {
  const counts = {
    skills: data.skills?.reduce((a,c)=>a+(c.items?.length||0),0)||0,
    credentials: data.credentials?.length||0,
    projects: data.projects?.length||0,
    experience: data.experience?.length||0,
  }
  return (
    <div>
      <div className="stat-grid">
        {[
          {label:'Skills',value:counts.skills,sub:'tracked'},
          {label:'Credentials',value:counts.credentials,sub:'certificates'},
          {label:'Projects',value:counts.projects,sub:'portfolio'},
          {label:'Experience',value:counts.experience,sub:'positions'},
        ].map(s=>(
          <div className="stat-card" key={s.label}>
            <div className="stat-label">{s.label}</div>
            <div className="stat-value">{String(s.value).padStart(2,'0')}</div>
            <div className="stat-sub">{s.sub}</div>
          </div>
        ))}
      </div>
      <div className="grid-2">
        <div className="card">
          <div className="card-corner tl"/><div className="card-corner tr"/>
          <div className="card-corner bl"/><div className="card-corner br"/>
          <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:11,color:'var(--g)',letterSpacing:2,marginBottom:16}}>RECENT ACTIVITY</div>
          {[
            {color:'var(--g)',    text:'Admin panel connected',       time:now()},
            {color:'var(--blue)',text:'Supabase realtime active',     time:now()},
            {color:'var(--amber)',text:`${counts.credentials} credentials loaded`,time:now()},
            {color:'var(--g)',   text:`Last sync: ${lastSync||'—'}`, time:''},
          ].map((a,i)=>(
            <div className="activity-item" key={i}>
              <div className="activity-dot" style={{background:a.color,boxShadow:`0 0 6px ${a.color}`}}/>
              <div>
                <div className="activity-text">{a.text}</div>
                {a.time&&<div className="activity-time">{a.time}</div>}
              </div>
            </div>
          ))}
        </div>
        <div className="card">
          <div className="card-corner tl"/><div className="card-corner tr"/>
          <div className="card-corner bl"/><div className="card-corner br"/>
          <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:11,color:'var(--g)',letterSpacing:2,marginBottom:16}}>SYSTEM STATUS</div>
          {[
            {label:'Storage',   val:'Supabase (cloud)',    ok:true},
            {label:'Realtime',  val:'Subscribed',          ok:true},
            {label:'Access',    val:'Admin write / Public read', ok:true},
            {label:'Last Save', val:lastSync||'—',         ok:!!lastSync},
            {label:'Project',   val:sbCfg?.url?.split('//')[1]?.split('.')[0]||'—', ok:true},
          ].map(s=>(
            <div key={s.label} style={{display:'flex',justifyContent:'space-between',alignItems:'center',padding:'10px 0',borderBottom:'1px solid rgba(26,46,28,.4)'}}>
              <span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:11,color:'var(--tx3)',letterSpacing:1}}>{s.label}</span>
              <span className={`badge badge-${s.ok?'green':'amber'}`}>{s.val}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// ABOUT
// ══════════════════════════════════════════════════════════════════════════
function AboutSection({ data, onSave }) {
  const [d, setD] = useState(data||{})
  const [saving, setSaving] = useState(false)
  useEffect(()=>{ setD(data||{}) }, [data])
  const [saved, setSaved] = useState(false)
  const u = k => e => setD(p=>({...p,[k]:e.target.value}))
  const handleSave = async () => {
    setSaving(true)
    await onSave(d)
    setSaving(false); setSaved(true); setTimeout(()=>setSaved(false),2000)
  }
  return (
    <div>
      <div className="section-header">
        <div><span className="section-title">About</span></div>
        <button className="btn btn-green" onClick={handleSave} disabled={saving}>
          {saving?'⟳ Saving…':saved?'✓ Saved':'Save Changes'}
        </button>
      </div>
      <div className="card" style={{marginBottom:20}}>
        <div className="card-corner tl"/><div className="card-corner tr"/>
        <div className="card-corner bl"/><div className="card-corner br"/>
        <div className="form-row form-row-2">
          <div className="form-group"><label className="form-label">Full Name</label><input className="form-input" value={d.name||''} onChange={u('name')} placeholder="Mohamed Aasiq"/></div>
          <div className="form-group"><label className="form-label">Title / Role</label><input className="form-input" value={d.title||''} onChange={u('title')} placeholder="Cybersecurity Analyst"/></div>
        </div>
        <div className="form-group"><label className="form-label">Tagline</label><input className="form-input" value={d.tagline||''} onChange={u('tagline')} placeholder="Short punchy headline"/></div>
        <div className="form-group"><label className="form-label">Bio</label><textarea className="form-textarea" rows={5} value={d.bio||''} onChange={u('bio')} placeholder="Write about yourself..."/></div>
        <div className="form-row form-row-3">
          <div className="form-group"><label className="form-label">Location</label><input className="form-input" value={d.location||''} onChange={u('location')} placeholder="Madurai, India"/></div>
          <div className="form-group">
            <label className="form-label">Availability</label>
            <select className="form-select" value={d.availability||'open'} onChange={u('availability')}>
              <option value="open">Open to Work</option>
              <option value="employed">Employed</option>
              <option value="freelance">Freelance Only</option>
              <option value="unavailable">Not Available</option>
            </select>
          </div>
          <div className="form-group"><label className="form-label">Years Exp</label><input className="form-input" type="number" value={d.yearsExp||''} onChange={u('yearsExp')} placeholder="3"/></div>
        </div>
        <hr className="divider"/>
        <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--g)',letterSpacing:2,marginBottom:14}}>PROFILE PHOTO</div>
        <FileUpload value={d.avatar} accept="image/*" label="Upload Profile Photo" onChange={b=>setD(p=>({...p,avatar:b}))}/>
      </div>
      <div className="card">
        <div className="card-corner tl"/><div className="card-corner tr"/>
        <div className="card-corner bl"/><div className="card-corner br"/>
        <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--g)',letterSpacing:2,marginBottom:14}}>SOCIAL LINKS</div>
        <div className="form-row form-row-2">
          {['linkedin','github','twitter','email','portfolio','tryhackme'].map(k=>(
            <div className="form-group" key={k}>
              <label className="form-label">{k.toUpperCase()}</label>
              <input className="form-input" value={d[k]||''} onChange={u(k)} placeholder={`https://${k}.com/...`}/>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// SKILLS
// ══════════════════════════════════════════════════════════════════════════
const BLANK_CAT   = ()=>({id:uid(),name:'',items:[]})
const BLANK_SKILL = ()=>({id:uid(),name:'',level:'Intermediate',badge:''})

function SkillsSection({ data, onSave }) {
  const [cats, setCats]       = useState(data||[])
  const [modal, setModal]     = useState(null)
  const [form, setForm]       = useState({})
  const [confirm, setConfirm] = useState(null)
  const [saving, setSaving]   = useState(false)
  useEffect(()=>{ setCats(data||[]) }, [data])
  const commit = async updated => { setCats(updated); setSaving(true); await onSave(updated); setSaving(false) }
  const levelPct = {Beginner:25,Intermediate:55,Advanced:80,Expert:100}

  const openCatModal   = (ci=null) => { setForm(ci===null?BLANK_CAT():{...cats[ci]}); setModal({mode:'cat',ci}) }
  const saveCat        = async () => { const u=modal.ci===null?[...cats,{...form,items:form.items||[]}]:cats.map((c,i)=>i===modal.ci?{...c,...form}:c); await commit(u); setModal(null) }
  const delCat         = async ci  => { await commit(cats.filter((_,i)=>i!==ci)); setConfirm(null) }
  const openSkillModal = (ci,si=null) => { setForm(si===null?BLANK_SKILL():{...cats[ci].items[si]}); setModal({mode:'skill',ci,si}) }
  const saveSkill      = async () => { const u=cats.map((c,ci)=>{ if(ci!==modal.ci)return c; const items=modal.si===null?[...c.items,form]:c.items.map((s,si)=>si===modal.si?form:s); return{...c,items} }); await commit(u); setModal(null) }
  const delSkill       = async (ci,si) => { await commit(cats.map((c,i)=>i!==ci?c:{...c,items:c.items.filter((_,j)=>j!==si)})); setConfirm(null) }

  return (
    <div>
      <div className="section-header">
        <div><span className="section-title">Skills</span><span className="section-count">({cats.length} categories)</span></div>
        <div style={{display:'flex',gap:10,alignItems:'center'}}>
          {saving&&<span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--amber)'}}>⟳ Syncing…</span>}
          <button className="btn btn-green" onClick={()=>openCatModal()}>+ Category</button>
        </div>
      </div>
      {cats.length===0&&<div className="empty-state"><div className="empty-state-icon">◎</div><div className="empty-state-text">No skill categories yet</div></div>}
      {cats.map((cat,ci)=>(
        <div className="card" key={cat.id} style={{marginBottom:16}}>
          <div className="card-corner tl"/><div className="card-corner tr"/>
          <div className="card-corner bl"/><div className="card-corner br"/>
          <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:16}}>
            <span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:12,color:'var(--g)',letterSpacing:2}}>{cat.name||'Unnamed Category'}</span>
            <div style={{display:'flex',gap:8}}>
              <button className="btn btn-ghost btn-sm" onClick={()=>openSkillModal(ci)}>+ Skill</button>
              <button className="btn btn-amber btn-sm btn-icon" onClick={()=>openCatModal(ci)}>✎</button>
              <button className="btn btn-red btn-sm btn-icon" onClick={()=>setConfirm({type:'cat',ci})}>✕</button>
            </div>
          </div>
          <div style={{display:'flex',flexDirection:'column',gap:10}}>
            {cat.items?.map((sk,si)=>(
              <div key={sk.id} style={{display:'flex',alignItems:'center',gap:12}}>
                <span style={{width:160,fontSize:13,fontWeight:600,color:'var(--tx)',flexShrink:0}}>{sk.name}</span>
                <div className="skill-bar-wrap"><div className="skill-bar" style={{width:`${levelPct[sk.level]||50}%`}}/></div>
                <span className="badge badge-blue" style={{width:90,textAlign:'center',flexShrink:0,fontSize:10}}>{sk.level}</span>
                <button className="btn btn-amber btn-sm btn-icon" onClick={()=>openSkillModal(ci,si)}>✎</button>
                <button className="btn btn-red btn-sm btn-icon" onClick={()=>setConfirm({type:'skill',ci,si})}>✕</button>
              </div>
            ))}
          </div>
        </div>
      ))}
      {modal&&(
        <div className="modal-overlay">
          <div className="modal">
            <div className="modal-header">
              <span className="modal-title">{modal.mode==='cat'?(modal.ci===null?'NEW CATEGORY':'EDIT CATEGORY'):(modal.si===null?'NEW SKILL':'EDIT SKILL')}</span>
              <button className="modal-close" onClick={()=>setModal(null)}>×</button>
            </div>
            <div className="modal-body">
              {modal.mode==='cat'?(
                <div className="form-group"><label className="form-label">Category Name</label><input className="form-input" value={form.name||''} onChange={e=>setForm(p=>({...p,name:e.target.value}))} placeholder="Cybersecurity, Cloud..."/></div>
              ):(
                <>
                  <div className="form-row form-row-2">
                    <div className="form-group"><label className="form-label">Skill Name</label><input className="form-input" value={form.name||''} onChange={e=>setForm(p=>({...p,name:e.target.value}))} placeholder="Python, Nmap..."/></div>
                    <div className="form-group"><label className="form-label">Level</label>
                      <select className="form-select" value={form.level||'Intermediate'} onChange={e=>setForm(p=>({...p,level:e.target.value}))}>
                        {SKILL_LEVELS.map(l=><option key={l}>{l}</option>)}
                      </select>
                    </div>
                  </div>
                  <div className="form-group"><label className="form-label">Badge URL (optional)</label><input className="form-input" value={form.badge||''} onChange={e=>setForm(p=>({...p,badge:e.target.value}))} placeholder="https://..."/></div>
                </>
              )}
            </div>
            <div className="modal-footer">
              <button className="btn btn-ghost" onClick={()=>setModal(null)}>Cancel</button>
              <button className="btn btn-green" onClick={modal.mode==='cat'?saveCat:saveSkill}>Save</button>
            </div>
          </div>
        </div>
      )}
      {confirm&&<Confirm msg={confirm.type==='cat'?'Delete category and all skills?':'Remove this skill?'} onConfirm={()=>confirm.type==='cat'?delCat(confirm.ci):delSkill(confirm.ci,confirm.si)} onCancel={()=>setConfirm(null)}/>}
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// CREDENTIALS  (3-section tabbed: Credly · Professional · LinkedIn/Other)
// ══════════════════════════════════════════════════════════════════════════

// Blank templates per category
const BLANK_CREDLY = () => ({
  id: uid(), type: 'credly',
  title: '', issuer: '', date: '', url: '', image: null, pdf: null,
  tags: [], featured: false, logo: '', logoUpload: null,
  credlyBadgeId: '', credlyEarnerUrl: '', credlyImageUrl: '',
})
const BLANK_PROFESSIONAL = () => ({
  id: uid(), type: 'certificate',
  title: '', issuer: '', date: '', url: '', image: null, pdf: null,
  tags: [], featured: false, logo: '', logoUpload: null,
  certNumber: '', duration: '', examCode: '',
})
const BLANK_LINKEDIN = () => ({
  id: uid(), type: 'linkedin',
  title: '', issuer: 'LinkedIn Learning', date: '', url: '', image: null, pdf: null,
  tags: [], featured: false, logo: '', logoUpload: null,
  courseId: '', learningPathName: '',
})

// Which types belong to each tab
const TAB_TYPES = {
  credly:       ['badge', 'credly'],
  professional: ['certificate', 'exam'],
  linkedin:     ['linkedin', 'learning-path', 'other'],
}

const CRED_TABS = [
  { id: 'credly',       label: 'Credly Badges',              icon: '🏅', color: 'var(--amber)',  blank: BLANK_CREDLY },
  { id: 'professional', label: 'Professional Certificates',  icon: '📜', color: 'var(--g)',      blank: BLANK_PROFESSIONAL },
  { id: 'linkedin',     label: 'LinkedIn & Others',          icon: '🔗', color: 'var(--blue)',   blank: BLANK_LINKEDIN },
]

// Sub-section CSS additions (tab strip + section divider)
const CRED_CSS = `
  .cred-tabs{display:flex;gap:0;border-bottom:1px solid var(--bd);margin-bottom:24px}
  .cred-tab{padding:10px 22px;cursor:pointer;font-family:'Share Tech Mono',monospace;font-size:11px;
    letter-spacing:2px;color:var(--tx3);border-bottom:2px solid transparent;transition:all .2s;
    display:flex;align-items:center;gap:8px;user-select:none;text-transform:uppercase}
  .cred-tab:hover{color:var(--tx)}
  .cred-tab.active-credly{color:var(--amber);border-bottom-color:var(--amber)}
  .cred-tab.active-professional{color:var(--g);border-bottom-color:var(--g)}
  .cred-tab.active-linkedin{color:var(--blue);border-bottom-color:var(--blue)}
  .cred-tab-count{font-size:10px;padding:1px 6px;border-radius:10px;font-weight:700}
  .cred-tab-count-credly{background:rgba(255,170,0,.15);color:var(--amber);border:1px solid #664400}
  .cred-tab-count-professional{background:rgba(0,255,65,.08);color:var(--g);border:1px solid var(--g3)}
  .cred-tab-count-linkedin{background:rgba(68,170,255,.1);color:var(--blue);border:1px solid #004488}
  .cred-section-banner{padding:12px 16px;margin-bottom:16px;border-left:3px solid;
    font-family:'Share Tech Mono',monospace;font-size:10px;letter-spacing:1.5px;background:var(--bg2)}
  .cred-section-banner-credly{border-color:var(--amber);color:var(--amber)}
  .cred-section-banner-professional{border-color:var(--g);color:var(--g)}
  .cred-section-banner-linkedin{border-color:var(--blue);color:var(--blue)}
`

function CredentialsSection({ data, onSave }) {
  const [creds, setCreds]     = useState(data || [])
  const [credTab, setCredTab] = useState('credly')
  const [modal, setModal]     = useState(null)
  const [form, setForm]       = useState({})
  const [confirm, setConfirm] = useState(null)
  const [search, setSearch]   = useState('')
  const [saving, setSaving]   = useState(false)
  useEffect(()=>{ setCreds(data||[]) }, [data])

  const commit = async u => { setCreds(u); setSaving(true); await onSave(u); setSaving(false) }

  const open = (id = null) => {
    if (id) {
      const existing = creds.find(c => c.id === id)
      // If the stored logo is a base64 data URL, pre-populate logoUpload for preview
      const logoUpload = existing?.logo?.startsWith('data:image') ? existing.logo : null
      setForm({ ...existing, logoUpload })
    } else {
      const tabCfg = CRED_TABS.find(t => t.id === credTab)
      setForm(tabCfg.blank())
    }
    setModal(id || 'new')
  }

  const save = async () => {
    // If a logo was uploaded (base64), use it as the final logo value
    const finalForm = { ...form, logo: form.logoUpload || form.logo }
    await commit(modal === 'new' ? [...creds, finalForm] : creds.map(c => c.id === modal ? finalForm : c))
    setModal(null)
  }
  const del = async id => { await commit(creds.filter(c => c.id !== id)); setConfirm(null) }
  const u   = k => e => setForm(p => ({ ...p, [k]: e.target.value }))

  // Items for the active tab
  const tabTypes   = TAB_TYPES[credTab] || []
  const tabItems   = creds.filter(c => tabTypes.includes(c.type))
  const filtered   = tabItems.filter(c =>
    c.title?.toLowerCase().includes(search.toLowerCase()) ||
    c.issuer?.toLowerCase().includes(search.toLowerCase())
  )

  const tabCfg = CRED_TABS.find(t => t.id === credTab)

  // Count per tab
  const countFor = id => creds.filter(c => TAB_TYPES[id]?.includes(c.type)).length

  return (
    <div>
      <style>{CRED_CSS}</style>

      {/* Section Header */}
      <div className="section-header">
        <div>
          <span className="section-title">Credentials</span>
          <span className="section-count">({creds.length} total)</span>
        </div>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          {saving && <span style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 10, color: 'var(--amber)' }}>⟳ Syncing…</span>}
          <input
            className="form-input"
            style={{ width: 200 }}
            placeholder="🔍  Search..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
          <button
            className="btn"
            style={{ borderColor: tabCfg.color, color: tabCfg.color }}
            onClick={() => open()}
          >
            + Add {credTab === 'credly' ? 'Badge' : credTab === 'professional' ? 'Certificate' : 'Course'}
          </button>
        </div>
      </div>

      {/* Tab Strip */}
      <div className="cred-tabs">
        {CRED_TABS.map(tab => (
          <div
            key={tab.id}
            className={`cred-tab${credTab === tab.id ? ` active-${tab.id}` : ''}`}
            onClick={() => setCredTab(tab.id)}
          >
            <span>{tab.icon}</span>
            <span>{tab.label}</span>
            {countFor(tab.id) > 0 && (
              <span className={`cred-tab-count cred-tab-count-${tab.id}`}>{countFor(tab.id)}</span>
            )}
          </div>
        ))}
      </div>

      {/* Section Banner */}
      <div className={`cred-section-banner cred-section-banner-${credTab}`}>
        {credTab === 'credly' && '▸ CREDLY DIGITAL BADGES — Paste your Credly badge URL or upload badge image'}
        {credTab === 'professional' && '▸ PROFESSIONAL CERTIFICATES — Industry certifications, exam passes, learning paths'}
        {credTab === 'linkedin' && '▸ LINKEDIN LEARNING & OTHERS — LinkedIn courses, MOOCs, online learning, misc certs'}
      </div>

      {/* Table */}
      <div className="card">
        <div className="card-corner tl"/><div className="card-corner tr"/>
        <div className="card-corner bl"/><div className="card-corner br"/>
        {filtered.length === 0
          ? (
            <div className="empty-state">
              <div className="empty-state-icon">{tabCfg.icon}</div>
              <div className="empty-state-text">
                No {tabCfg.label} yet — click "+ Add" to get started
              </div>
            </div>
          ) : (
            <table className="data-table">
              <thead>
                <tr>
                  <th>
                    {credTab === 'credly' ? 'Badge' : credTab === 'professional' ? 'Certificate' : 'Course'}
                  </th>
                  <th>Issuer</th>
                  <th>Date</th>
                  {credTab === 'credly' && <th>Badge ID</th>}
                  {credTab === 'professional' && <th>Cert #</th>}
                  {credTab === 'linkedin' && <th>Course ID</th>}
                  <th>Tags</th>
                  <th style={{ width: 100 }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map(c => (
                  <tr key={c.id}>
                    <td>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                        {c.logo && (
                          <img src={c.logo} alt="" style={{ width: 28, height: 28, objectFit: 'contain', border: '1px solid var(--bd)' }} onError={e => e.target.style.display = 'none'}/>
                        )}
                        <div>
                          <div style={{ fontWeight: 700 }}>{c.title}</div>
                          {c.featured && <span className="badge badge-amber" style={{ fontSize: 9, marginTop: 2, display: 'inline-block' }}>★ FEATURED</span>}
                        </div>
                      </div>
                    </td>
                    <td style={{ color: 'var(--tx2)' }}>{c.issuer}</td>
                    <td style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 11, color: 'var(--tx3)' }}>{c.date}</td>
                    {credTab === 'credly' && (
                      <td style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 10, color: 'var(--tx3)' }}>
                        {c.credlyBadgeId ? <span className="badge badge-amber" style={{ fontSize: 9 }}>{c.credlyBadgeId.slice(0, 12)}…</span> : '—'}
                      </td>
                    )}
                    {credTab === 'professional' && (
                      <td style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 10, color: 'var(--tx3)' }}>
                        {c.certNumber || <span style={{ opacity: .4 }}>—</span>}
                      </td>
                    )}
                    {credTab === 'linkedin' && (
                      <td style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 10, color: 'var(--tx3)' }}>
                        {c.courseId || <span style={{ opacity: .4 }}>—</span>}
                      </td>
                    )}
                    <td>{c.tags?.slice(0, 3).map(t => <span className="badge badge-blue" key={t} style={{ fontSize: 9, marginRight: 3 }}>{t}</span>)}</td>
                    <td>
                      <div style={{ display: 'flex', gap: 6 }}>
                        <button className="btn btn-amber btn-sm btn-icon" onClick={() => open(c.id)}>✎</button>
                        <button className="btn btn-red btn-sm btn-icon" onClick={() => setConfirm(c.id)}>✕</button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        }
      </div>

      {/* ── MODAL ── */}
      {modal && (
        <div className="modal-overlay"><div className="modal">
          <div className="modal-header">
            <span className="modal-title" style={{ color: tabCfg.color }}>
              {modal === 'new'
                ? `NEW ${credTab === 'credly' ? 'CREDLY BADGE' : credTab === 'professional' ? 'PROFESSIONAL CERT' : 'LINKEDIN / OTHER'}`
                : `EDIT ${credTab === 'credly' ? 'CREDLY BADGE' : credTab === 'professional' ? 'PROFESSIONAL CERT' : 'LINKEDIN / OTHER'}`
              }
            </span>
            <button className="modal-close" onClick={() => setModal(null)}>×</button>
          </div>
          <div className="modal-body">

            {/* ─ Common fields ─ */}
            <div className="form-row form-row-2">
              <div className="form-group">
                <label className="form-label">
                  {credTab === 'credly' ? 'Badge Title' : credTab === 'professional' ? 'Certificate Name' : 'Course / Cert Title'}
                </label>
                <input className="form-input" value={form.title || ''} onChange={u('title')}
                  placeholder={credTab === 'credly' ? 'e.g. Certified Ethical Hacker (CEH)' : credTab === 'professional' ? 'e.g. CompTIA Security+' : 'e.g. Ethical Hacking Essentials'}/>
              </div>
              <div className="form-group">
                <label className="form-label">Issuing Organization</label>
                <input className="form-input" value={form.issuer || ''} onChange={u('issuer')}
                  placeholder={credTab === 'credly' ? 'e.g. EC-Council via Credly' : credTab === 'professional' ? 'e.g. CompTIA, ISACA' : 'e.g. LinkedIn Learning, Coursera'}/>
              </div>
            </div>

            <div className="form-row form-row-2">
              <div className="form-group">
                <label className="form-label">Issue Date</label>
                <input className="form-input" type="month" value={form.date || ''} onChange={u('date')}/>
              </div>
              <div className="form-group">
                <label className="form-label">Verify / View URL</label>
                <input className="form-input" value={form.url || ''} onChange={u('url')}
                  placeholder={credTab === 'credly' ? 'https://www.credly.com/badges/...' : credTab === 'professional' ? 'https://verify.comptia.org/...' : 'https://www.linkedin.com/learning/...'}/>
              </div>
            </div>

            <div className="form-group">
              <label className="form-label">Logo / Issuer Image URL</label>
              <input className="form-input" value={form.logo || ''} onChange={u('logo')}
                placeholder="https://logo.clearbit.com/company.com  or paste a direct image URL"/>
              <div className="form-hint">Paste a URL above — OR upload a logo image below</div>
              <div style={{marginTop:10}}>
                <FileUpload
                  value={form.logoUpload || null}
                  accept="image/*"
                  label="Upload Logo Image"
                  onChange={b => setForm(p => ({ ...p, logoUpload: b, logo: b || p.logo }))}
                />
              </div>
              {form.logo && (
                <div style={{marginTop:8,display:'flex',alignItems:'center',gap:10}}>
                  <span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--tx3)',letterSpacing:1}}>PREVIEW:</span>
                  <img src={form.logo} alt="logo preview" style={{width:40,height:40,objectFit:'contain',border:'1px solid var(--bd)',background:'var(--bg2)',padding:4}} onError={e=>e.target.style.display='none'}/>
                </div>
              )}
            </div>

            {/* ─ Credly-specific ─ */}
            {credTab === 'credly' && (
              <>
                <hr className="divider"/>
                <div style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 10, color: 'var(--amber)', letterSpacing: 2, marginBottom: 12 }}>CREDLY DETAILS</div>
                <div className="form-row form-row-2">
                  <div className="form-group">
                    <label className="form-label">Credly Badge ID</label>
                    <input className="form-input" value={form.credlyBadgeId || ''} onChange={u('credlyBadgeId')}
                      placeholder="UUID from credly.com/badges/..."/>
                    <div className="form-hint">The unique ID segment in your Credly badge URL</div>
                  </div>
                  <div className="form-group">
                    <label className="form-label">Earner Profile URL</label>
                    <input className="form-input" value={form.credlyEarnerUrl || ''} onChange={u('credlyEarnerUrl')}
                      placeholder="https://www.credly.com/users/username"/>
                  </div>
                </div>
                <div className="form-group">
                  <label className="form-label">Badge Image URL (from Credly)</label>
                  <input className="form-input" value={form.credlyImageUrl || ''} onChange={u('credlyImageUrl')}
                    placeholder="https://images.credly.com/images/.../badge.png"/>
                  <div className="form-hint">Paste the direct image URL from your Credly badge page</div>
                </div>
              </>
            )}

            {/* ─ Professional-specific ─ */}
            {credTab === 'professional' && (
              <>
                <hr className="divider"/>
                <div style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 10, color: 'var(--g)', letterSpacing: 2, marginBottom: 12 }}>CERTIFICATE DETAILS</div>
                <div className="form-row form-row-3">
                  <div className="form-group">
                    <label className="form-label">Certificate Number</label>
                    <input className="form-input" value={form.certNumber || ''} onChange={u('certNumber')}
                      placeholder="e.g. COMP001234567"/>
                  </div>
                  <div className="form-group">
                    <label className="form-label">Exam / Course Code</label>
                    <input className="form-input" value={form.examCode || ''} onChange={u('examCode')}
                      placeholder="e.g. SY0-701"/>
                  </div>
                  <div className="form-group">
                    <label className="form-label">Duration (hours)</label>
                    <input className="form-input" type="number" value={form.duration || ''} onChange={u('duration')}
                      placeholder="e.g. 40"/>
                  </div>
                </div>
                <div className="form-group">
                  <label className="form-label">Certificate Sub-Type</label>
                  <select className="form-select" value={form.type || 'certificate'} onChange={u('type')}>
                    <option value="certificate">Certificate of Completion</option>
                    <option value="exam">Exam / Proctored Certification</option>
                  </select>
                </div>
              </>
            )}

            {/* ─ LinkedIn-specific ─ */}
            {credTab === 'linkedin' && (
              <>
                <hr className="divider"/>
                <div style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 10, color: 'var(--blue)', letterSpacing: 2, marginBottom: 12 }}>COURSE DETAILS</div>
                <div className="form-row form-row-2">
                  <div className="form-group">
                    <label className="form-label">LinkedIn Course ID</label>
                    <input className="form-input" value={form.courseId || ''} onChange={u('courseId')}
                      placeholder="e.g. ethical-hacking-2023"/>
                    <div className="form-hint">The slug from the LinkedIn Learning URL</div>
                  </div>
                  <div className="form-group">
                    <label className="form-label">Learning Path Name</label>
                    <input className="form-input" value={form.learningPathName || ''} onChange={u('learningPathName')}
                      placeholder="e.g. Become an Ethical Hacker (optional)"/>
                  </div>
                </div>
                <div className="form-group">
                  <label className="form-label">Source / Platform</label>
                  <select className="form-select" value={form.type || 'linkedin'} onChange={u('type')}>
                    <option value="linkedin">LinkedIn Learning</option>
                    <option value="other">Other (Coursera, Udemy, edX, etc.)</option>
                  </select>
                </div>
              </>
            )}

            {/* ─ Shared bottom fields ─ */}
            <hr className="divider"/>
            <div className="form-group">
              <label className="form-label">Tags / Skills</label>
              <TagInput value={form.tags || []} onChange={v => setForm(p => ({ ...p, tags: v }))}
                placeholder="CEH, Networking, Python…"/>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
              <label className="toggle">
                <input type="checkbox" checked={form.featured || false}
                  onChange={e => setForm(p => ({ ...p, featured: e.target.checked }))}/>
                <span className="toggle-slider"/>
              </label>
              <span style={{ fontSize: 13, color: 'var(--tx2)' }}>Featured on portfolio</span>
            </div>

            <hr className="divider"/>
            <div style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 10, color: 'var(--tx3)', letterSpacing: 2, marginBottom: 10 }}>
              {credTab === 'credly' ? 'BADGE IMAGE (upload backup)' : 'CERTIFICATE IMAGE'}
            </div>
            <FileUpload value={form.image} accept="image/*" label="Upload Image" onChange={b => setForm(p => ({ ...p, image: b }))}/>
            <div style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 10, color: 'var(--tx3)', letterSpacing: 2, margin: '16px 0 10px' }}>
              CERTIFICATE PDF
            </div>
            <FileUpload value={form.pdf} accept="application/pdf,image/*" label="Upload PDF" onChange={b => setForm(p => ({ ...p, pdf: b }))}/>
          </div>
          <div className="modal-footer">
            <button className="btn btn-ghost" onClick={() => setModal(null)}>Cancel</button>
            <button
              className="btn"
              style={{ borderColor: tabCfg.color, color: tabCfg.color }}
              onClick={save}
            >
              Save
            </button>
          </div>
        </div></div>
      )}

      {confirm && <Confirm msg="Delete this credential?" onConfirm={() => del(confirm)} onCancel={() => setConfirm(null)}/>}
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// PROJECTS
// ══════════════════════════════════════════════════════════════════════════
const BLANK_PROJ = ()=>({id:uid(),title:'',desc:'',tech:[],status:'Completed',liveUrl:'',githubUrl:'',image:null,featured:false})
const STATUS_COLOR = {Completed:'green','In Progress':'amber',Planned:'blue',Archived:'red'}

function ProjectsSection({ data, onSave }) {
  const [items, setItems]     = useState(data||[])
  const [modal, setModal]     = useState(null)
  const [form, setForm]       = useState({})
  const [confirm, setConfirm] = useState(null)
  const [saving, setSaving]   = useState(false)
  useEffect(()=>{ setItems(data||[]) }, [data])
  const commit = async u => { setItems(u); setSaving(true); await onSave(u); setSaving(false) }
  const open   = (id=null) => { setForm(id?{...items.find(p=>p.id===id)}:BLANK_PROJ()); setModal(id||'new') }
  const save   = async () => { await commit(modal==='new'?[...items,form]:items.map(p=>p.id===modal?form:p)); setModal(null) }
  const del    = async id  => { await commit(items.filter(p=>p.id!==id)); setConfirm(null) }
  const u      = k => e => setForm(p=>({...p,[k]:e.target.value}))
  return (
    <div>
      <div className="section-header">
        <div><span className="section-title">Projects</span><span className="section-count">({items.length})</span></div>
        <div style={{display:'flex',gap:10,alignItems:'center'}}>
          {saving&&<span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--amber)'}}>⟳ Syncing…</span>}
          <button className="btn btn-green" onClick={()=>open()}>+ Add Project</button>
        </div>
      </div>
      {items.length===0&&<div className="empty-state"><div className="empty-state-icon">◉</div><div className="empty-state-text">No projects yet</div></div>}
      <div className="grid-2" style={{marginBottom:16}}>
        {items.map(p=>(
          <div className="card" key={p.id}>
            <div className="card-corner tl"/><div className="card-corner tr"/>
            <div className="card-corner bl"/><div className="card-corner br"/>
            {p.image&&<img src={p.image} alt="" style={{width:'100%',height:120,objectFit:'cover',marginBottom:14,border:'1px solid var(--bd)'}}/>}
            <div style={{display:'flex',justifyContent:'space-between',marginBottom:8}}>
              <span style={{fontWeight:700,color:'var(--tx)'}}>{p.title||'Untitled'}</span>
              <span className={`badge badge-${STATUS_COLOR[p.status]||'green'}`} style={{fontSize:9,marginLeft:8}}>{p.status}</span>
            </div>
            {p.featured&&<div style={{marginBottom:6}}><span className="badge badge-amber" style={{fontSize:9}}>★ FEATURED</span></div>}
            <p style={{fontSize:12,color:'var(--tx2)',lineHeight:1.5,marginBottom:12}}>{p.desc}</p>
            <div style={{display:'flex',flexWrap:'wrap',gap:4,marginBottom:14}}>{p.tech?.map(t=><span className="badge badge-blue" key={t} style={{fontSize:10}}>{t}</span>)}</div>
            <div style={{display:'flex',gap:8}}>
              {p.liveUrl&&<a href={p.liveUrl} target="_blank" rel="noreferrer" className="btn btn-ghost btn-sm">↗ Live</a>}
              {p.githubUrl&&<a href={p.githubUrl} target="_blank" rel="noreferrer" className="btn btn-ghost btn-sm">⌥ GitHub</a>}
              <button className="btn btn-amber btn-sm btn-icon" style={{marginLeft:'auto'}} onClick={()=>open(p.id)}>✎</button>
              <button className="btn btn-red btn-sm btn-icon" onClick={()=>setConfirm(p.id)}>✕</button>
            </div>
          </div>
        ))}
      </div>
      {modal&&(
        <div className="modal-overlay"><div className="modal">
          <div className="modal-header"><span className="modal-title">{modal==='new'?'NEW PROJECT':'EDIT PROJECT'}</span><button className="modal-close" onClick={()=>setModal(null)}>×</button></div>
          <div className="modal-body">
            <div className="form-row form-row-2">
              <div className="form-group"><label className="form-label">Title</label><input className="form-input" value={form.title||''} onChange={u('title')} placeholder="Project Name"/></div>
              <div className="form-group"><label className="form-label">Status</label>
                <select className="form-select" value={form.status||'Completed'} onChange={u('status')}>
                  {['Completed','In Progress','Planned','Archived'].map(s=><option key={s}>{s}</option>)}
                </select>
              </div>
            </div>
            <div className="form-group"><label className="form-label">Description</label><textarea className="form-textarea" value={form.desc||''} onChange={u('desc')} placeholder="What does this project do?"/></div>
            <div className="form-group"><label className="form-label">Tech Stack</label><TagInput value={form.tech||[]} onChange={v=>setForm(p=>({...p,tech:v}))} placeholder="React, Python..."/></div>
            <div className="form-row form-row-2">
              <div className="form-group"><label className="form-label">Live URL</label><input className="form-input" value={form.liveUrl||''} onChange={u('liveUrl')} placeholder="https://..."/></div>
              <div className="form-group"><label className="form-label">GitHub URL</label><input className="form-input" value={form.githubUrl||''} onChange={u('githubUrl')} placeholder="https://github.com/..."/></div>
            </div>
            <div style={{display:'flex',alignItems:'center',gap:10,marginBottom:16}}>
              <label className="toggle"><input type="checkbox" checked={form.featured||false} onChange={e=>setForm(p=>({...p,featured:e.target.checked}))}/><span className="toggle-slider"/></label>
              <span style={{fontSize:13,color:'var(--tx2)'}}>Featured</span>
            </div>
            <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--g)',letterSpacing:2,marginBottom:10}}>SCREENSHOT</div>
            <FileUpload value={form.image} accept="image/*" label="Upload Image" onChange={b=>setForm(p=>({...p,image:b}))}/>
          </div>
          <div className="modal-footer"><button className="btn btn-ghost" onClick={()=>setModal(null)}>Cancel</button><button className="btn btn-green" onClick={save}>Save</button></div>
        </div></div>
      )}
      {confirm&&<Confirm msg="Delete this project?" onConfirm={()=>del(confirm)} onCancel={()=>setConfirm(null)}/>}
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// CTF FLAGS
// ══════════════════════════════════════════════════════════════════════════
const BLANK_FLAG = ()=>({id:uid(),platform:'TryHackMe',room:'',title:'',desc:'',difficulty:'Medium',date:now(),url:'',tags:[]})
const DIFF_COLORS = {Easy:'green',Medium:'amber',Hard:'red',Insane:'red'}

function FlagsSection({ data, onSave }) {
  const [items, setItems]   = useState(data||[])
  const [modal, setModal]   = useState(null)
  const [form, setForm]     = useState({})
  const [confirm, setConfirm] = useState(null)
  const [saving, setSaving] = useState(false)
  useEffect(()=>{ setItems(data||[]) }, [data])
  const commit = async u => { setItems(u); setSaving(true); await onSave(u); setSaving(false) }
  const open   = (id=null) => { setForm(id?{...items.find(f=>f.id===id)}:BLANK_FLAG()); setModal(id||'new') }
  const save   = async () => { await commit(modal==='new'?[...items,form]:items.map(f=>f.id===modal?form:f)); setModal(null) }
  const del    = async id  => { await commit(items.filter(f=>f.id!==id)); setConfirm(null) }
  const u      = k => e => setForm(p=>({...p,[k]:e.target.value}))
  return (
    <div>
      <div className="section-header">
        <div><span className="section-title">CTF Flags</span><span className="section-count">({items.length})</span></div>
        <div style={{display:'flex',gap:10,alignItems:'center'}}>
          {saving&&<span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--amber)'}}>⟳ Syncing…</span>}
          <button className="btn btn-green" onClick={()=>open()}>+ Add Flag</button>
        </div>
      </div>
      {items.length===0&&<div className="empty-state"><div className="empty-state-icon">🚩</div><div className="empty-state-text">No CTF flags yet — start capturing!</div></div>}
      <div className="grid-2" style={{marginBottom:16}}>
        {items.map(f=>(
          <div className="card" key={f.id}>
            <div className="card-corner tl"/><div className="card-corner tr"/>
            <div className="card-corner bl"/><div className="card-corner br"/>
            <div style={{display:'flex',justifyContent:'space-between',marginBottom:6}}>
              <span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:9,color:'var(--red)',letterSpacing:2}}>{f.platform}</span>
              <span className={`badge badge-${DIFF_COLORS[f.difficulty]||'amber'}`} style={{fontSize:9}}>{f.difficulty}</span>
            </div>
            <div style={{fontFamily:"'Orbitron',monospace",fontSize:12,color:'var(--tx)',marginBottom:6}}>{f.room||f.title||'Unnamed Room'}</div>
            {f.desc&&<p style={{fontSize:11,color:'var(--tx2)',lineHeight:1.6,marginBottom:8}}>{f.desc}</p>}
            <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginTop:8}}>
              <span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--tx3)'}}>{f.date}</span>
              <div style={{display:'flex',gap:8}}>
                {f.url&&<a href={f.url} target="_blank" rel="noreferrer" className="btn btn-ghost btn-sm">↗ Room</a>}
                <button className="btn btn-amber btn-sm btn-icon" onClick={()=>open(f.id)}>✎</button>
                <button className="btn btn-red btn-sm btn-icon" onClick={()=>setConfirm(f.id)}>✕</button>
              </div>
            </div>
          </div>
        ))}
      </div>
      {modal&&(
        <div className="modal-overlay"><div className="modal">
          <div className="modal-header"><span className="modal-title">{modal==='new'?'CAPTURE NEW FLAG':'EDIT FLAG'}</span><button className="modal-close" onClick={()=>setModal(null)}>×</button></div>
          <div className="modal-body">
            <div className="form-row form-row-2">
              <div className="form-group"><label className="form-label">Platform</label>
                <select className="form-select" value={form.platform||'TryHackMe'} onChange={u('platform')}>
                  {['TryHackMe','HackTheBox','CTF Competition','PicoCTF','VulnHub','PortSwigger','Custom'].map(p=><option key={p}>{p}</option>)}
                </select>
              </div>
              <div className="form-group"><label className="form-label">Difficulty</label>
                <select className="form-select" value={form.difficulty||'Medium'} onChange={u('difficulty')}>
                  {['Easy','Medium','Hard','Insane'].map(d=><option key={d}>{d}</option>)}
                </select>
              </div>
            </div>
            <div className="form-row form-row-2">
              <div className="form-group"><label className="form-label">Room / Challenge Name</label><input className="form-input" value={form.room||''} onChange={u('room')} placeholder="Mr Robot"/></div>
              <div className="form-group"><label className="form-label">Date</label><input className="form-input" type="date" value={form.date||''} onChange={u('date')}/></div>
            </div>
            <div className="form-group"><label className="form-label">Description</label><textarea className="form-textarea" rows={3} value={form.desc||''} onChange={u('desc')} placeholder="Describe what you learned or how you solved it..."/></div>
            <div className="form-group"><label className="form-label">Room URL</label><input className="form-input" value={form.url||''} onChange={u('url')} placeholder="https://tryhackme.com/room/..."/></div>
            <div className="form-group"><label className="form-label">Tags</label><TagInput value={form.tags||[]} onChange={v=>setForm(p=>({...p,tags:v}))} placeholder="Privilege Escalation, Web..."/></div>
          </div>
          <div className="modal-footer"><button className="btn btn-ghost" onClick={()=>setModal(null)}>Cancel</button><button className="btn btn-green" onClick={save}>🚩 Capture Flag</button></div>
        </div></div>
      )}
      {confirm&&<Confirm msg="Remove this flag?" onConfirm={()=>del(confirm)} onCancel={()=>setConfirm(null)}/>}
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// EXPERIENCE
// ══════════════════════════════════════════════════════════════════════════
const BLANK_EXP = ()=>({id:uid(),company:'',role:'',startDate:'',endDate:'',current:false,location:'',country:'🇮🇳',desc:'',achievements:[],tags:[],type:'Full-time'})

function ExperienceSection({ data, onSave }) {
  const [items, setItems]     = useState(data||[])
  const [modal, setModal]     = useState(null)
  const [form, setForm]       = useState({})
  const [confirm, setConfirm] = useState(null)
  const [saving, setSaving]   = useState(false)
  useEffect(()=>{ setItems(data||[]) }, [data])
  const commit = async u => { setItems(u); setSaving(true); await onSave(u); setSaving(false) }
  const open   = (id=null) => { setForm(id?{...items.find(e=>e.id===id)}:BLANK_EXP()); setModal(id||'new') }
  const save   = async () => { await commit(modal==='new'?[...items,form]:items.map(e=>e.id===modal?form:e)); setModal(null) }
  const del    = async id  => { await commit(items.filter(e=>e.id!==id)); setConfirm(null) }
  return (
    <div>
      <div className="section-header">
        <div><span className="section-title">Experience</span><span className="section-count">({items.length})</span></div>
        <div style={{display:'flex',gap:10,alignItems:'center'}}>
          {saving&&<span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--amber)'}}>⟳ Syncing…</span>}
          <button className="btn btn-green" onClick={()=>open()}>+ Add Position</button>
        </div>
      </div>
      {items.length===0&&<div className="empty-state"><div className="empty-state-icon">◍</div><div className="empty-state-text">No experience entries yet</div></div>}
      <div style={{display:'flex',flexDirection:'column',gap:14}}>
        {items.map(exp=>(
          <div className="card" key={exp.id}>
            <div className="card-corner tl"/><div className="card-corner tr"/>
            <div className="card-corner bl"/><div className="card-corner br"/>
            <div style={{display:'flex',justifyContent:'space-between'}}>
              <div style={{flex:1}}>
                <div style={{display:'flex',alignItems:'center',gap:10,marginBottom:4}}>
                  <span style={{fontSize:20}}>{exp.country}</span>
                  <span style={{fontWeight:700,fontSize:16,color:'var(--tx)'}}>{exp.role}</span>
                  {exp.current&&<span className="badge badge-green" style={{fontSize:9}}>CURRENT</span>}
                  <span className="badge badge-blue" style={{fontSize:9}}>{exp.type}</span>
                </div>
                <div style={{color:'var(--g)',fontSize:13,fontWeight:600,marginBottom:4}}>{exp.company}</div>
                <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--tx3)',marginBottom:8}}>{exp.startDate} → {exp.current?'Present':exp.endDate} · {exp.location}</div>
                <p style={{fontSize:12,color:'var(--tx2)',lineHeight:1.5}}>{exp.desc}</p>
              </div>
              <div style={{display:'flex',gap:8,marginLeft:16}}>
                <button className="btn btn-amber btn-sm btn-icon" onClick={()=>open(exp.id)}>✎</button>
                <button className="btn btn-red btn-sm btn-icon" onClick={()=>setConfirm(exp.id)}>✕</button>
              </div>
            </div>
          </div>
        ))}
      </div>
      {modal&&(
        <div className="modal-overlay"><div className="modal">
          <div className="modal-header"><span className="modal-title">{modal==='new'?'NEW POSITION':'EDIT POSITION'}</span><button className="modal-close" onClick={()=>setModal(null)}>×</button></div>
          <div className="modal-body">
            <div className="form-row form-row-2">
              <div className="form-group"><label className="form-label">Company</label><input className="form-input" value={form.company||''} onChange={e=>setForm(p=>({...p,company:e.target.value}))} placeholder="Company Name"/></div>
              <div className="form-group"><label className="form-label">Role</label><input className="form-input" value={form.role||''} onChange={e=>setForm(p=>({...p,role:e.target.value}))} placeholder="Security Analyst"/></div>
            </div>
            <div className="form-row form-row-3">
              <div className="form-group"><label className="form-label">Start</label><input className="form-input" type="month" value={form.startDate||''} onChange={e=>setForm(p=>({...p,startDate:e.target.value}))}/></div>
              <div className="form-group"><label className="form-label">End</label><input className="form-input" type="month" value={form.endDate||''} onChange={e=>setForm(p=>({...p,endDate:e.target.value}))} disabled={form.current}/></div>
              <div className="form-group" style={{display:'flex',flexDirection:'column',justifyContent:'flex-end'}}>
                <div style={{display:'flex',alignItems:'center',gap:8,paddingBottom:4}}>
                  <label className="toggle"><input type="checkbox" checked={form.current||false} onChange={e=>setForm(p=>({...p,current:e.target.checked}))}/><span className="toggle-slider"/></label>
                  <span style={{fontSize:13,color:'var(--tx2)'}}>Currently Here</span>
                </div>
              </div>
            </div>
            <div className="form-row form-row-2">
              <div className="form-group"><label className="form-label">Location</label><input className="form-input" value={form.location||''} onChange={e=>setForm(p=>({...p,location:e.target.value}))} placeholder="Chennai, India"/></div>
              <div className="form-group"><label className="form-label">Type</label>
                <select className="form-select" value={form.type||'Full-time'} onChange={e=>setForm(p=>({...p,type:e.target.value}))}>
                  {['Full-time','Part-time','Internship','Contract','Freelance','Volunteer'].map(t=><option key={t}>{t}</option>)}
                </select>
              </div>
            </div>
            <div className="form-group"><label className="form-label">Country Flag</label>
              <div style={{display:'flex',flexWrap:'wrap',gap:8,padding:10,background:'var(--bg2)',border:'1px solid var(--bd)'}}>
                {FLAG_EMOJIS.map(f=><span key={f} onClick={()=>setForm(p=>({...p,country:f}))} style={{fontSize:22,cursor:'pointer',padding:4,border:`2px solid ${form.country===f?'var(--g)':'transparent'}`,borderRadius:4}}>{f}</span>)}
              </div>
            </div>
            <div className="form-group"><label className="form-label">Description</label><textarea className="form-textarea" value={form.desc||''} onChange={e=>setForm(p=>({...p,desc:e.target.value}))} placeholder="Describe responsibilities..."/></div>
            <div className="form-group"><label className="form-label">Achievements (one per line)</label>
              <textarea className="form-textarea" rows={4} value={(form.achievements||[]).join('\n')} onChange={e=>setForm(p=>({...p,achievements:e.target.value.split('\n').filter(Boolean)}))} placeholder="• Reduced incident response by 40%&#10;• Implemented SIEM solution..."/>
            </div>
            <div className="form-group"><label className="form-label">Tags</label><TagInput value={form.tags||[]} onChange={v=>setForm(p=>({...p,tags:v}))} placeholder="Leadership, Python, AWS..."/></div>
          </div>
          <div className="modal-footer"><button className="btn btn-ghost" onClick={()=>setModal(null)}>Cancel</button><button className="btn btn-green" onClick={save}>Save</button></div>
        </div></div>
      )}
      {confirm&&<Confirm msg="Delete this experience?" onConfirm={()=>del(confirm)} onCancel={()=>setConfirm(null)}/>}
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// CONTACT
// ══════════════════════════════════════════════════════════════════════════
function ContactSection({ data, onSave }) {
  const [d, setD]     = useState(data||{})
  const [saving, setSaving] = useState(false)
  const [saved, setSaved]   = useState(false)
  useEffect(()=>{ setD(data||{}) }, [data])
  const u = k => e => setD(p=>({...p,[k]:e.target.value}))
  const handleSave = async () => { setSaving(true); await onSave(d); setSaving(false); setSaved(true); setTimeout(()=>setSaved(false),2000) }
  return (
    <div>
      <div className="section-header">
        <div><span className="section-title">Contact</span></div>
        <button className="btn btn-green" onClick={handleSave} disabled={saving}>
          {saving?'⟳ Saving…':saved?'✓ Saved':'Save Changes'}
        </button>
      </div>
      <div className="card" style={{marginBottom:20}}>
        <div className="card-corner tl"/><div className="card-corner tr"/>
        <div className="card-corner bl"/><div className="card-corner br"/>
        <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--g)',letterSpacing:2,marginBottom:14}}>PRIMARY CONTACT</div>
        <div className="form-row form-row-2">
          <div className="form-group"><label className="form-label">Email</label><input className="form-input" type="email" value={d.email||''} onChange={u('email')} placeholder="you@email.com"/></div>
          <div className="form-group"><label className="form-label">Phone</label><input className="form-input" type="tel" value={d.phone||''} onChange={u('phone')} placeholder="+91 9876543210"/></div>
        </div>
        <div className="form-group"><label className="form-label">Address</label><input className="form-input" value={d.address||''} onChange={u('address')} placeholder="Madurai, Tamil Nadu, India"/></div>
        <div className="form-group"><label className="form-label">CTA Message</label><textarea className="form-textarea" rows={3} value={d.ctaMessage||''} onChange={u('ctaMessage')} placeholder="I'm open to cybersecurity roles..."/></div>
      </div>
      <div className="card">
        <div className="card-corner tl"/><div className="card-corner tr"/>
        <div className="card-corner bl"/><div className="card-corner br"/>
        <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--g)',letterSpacing:2,marginBottom:14}}>SOCIAL PROFILES</div>
        <div className="form-row form-row-2">
          {[{k:'linkedin',ph:'linkedin.com/in/...'},{k:'github',ph:'github.com/...'},{k:'twitter',ph:'twitter.com/...'},{k:'tryhackme',ph:'tryhackme.com/p/...'},{k:'hackthebox',ph:'app.hackthebox.com/...'},{k:'discord',ph:'discord.com/users/...'},{k:'telegram',ph:'t.me/...'},{k:'youtube',ph:'youtube.com/@...'}].map(({k,ph})=>(
            <div className="form-group" key={k}>
              <label className="form-label">{k.toUpperCase()}</label>
              <input className="form-input" value={d[k]||''} onChange={u(k)} placeholder={ph}/>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// SETTINGS
// ══════════════════════════════════════════════════════════════════════════
function SettingsSection({ data, sbCfg, onDisconnect }) {
  const [pwd, setPwd] = useState({old:'',newP:'',confirm:''})
  const [msg, setMsg] = useState(null)
  const [testMsg, setTestMsg] = useState(null)
  const [testing, setTesting] = useState(false)

  const changePassword = () => {
    if (pwd.old!==getAdminPw())       { setMsg({err:true,txt:'Current password incorrect'}); return }
    if (pwd.newP!==pwd.confirm)       { setMsg({err:true,txt:'Passwords do not match'}); return }
    if (pwd.newP.length<6)            { setMsg({err:true,txt:'Minimum 6 characters'}); return }
    setAdminPw(pwd.newP)
    setMsg({err:false,txt:'✓ Password updated'}); setPwd({old:'',newP:'',confirm:''})
  }

  const testConn = async () => {
    setTesting(true); setTestMsg(null)
    const r = await testConnection(sbCfg.url, sbCfg.anonKey)
    setTesting(false)
    setTestMsg(r.ok?{ok:true,txt:'✓ Supabase connection healthy'}:{ok:false,txt:'⚠ '+r.msg})
  }

  const exportData = () => {
    const blob = new Blob([JSON.stringify({...data,_exported:new Date().toISOString()},null,2)],{type:'application/json'})
    const a = document.createElement('a'); a.href=URL.createObjectURL(blob); a.download=`portfolio-backup-${new Date().toISOString().slice(0,10)}.json`; a.click()
  }

  return (
    <div>
      <div className="section-header"><div><span className="section-title">Settings</span></div></div>
      <div className="grid-2" style={{marginBottom:20}}>
        <div className="card">
          <div className="card-corner tl"/><div className="card-corner tr"/>
          <div className="card-corner bl"/><div className="card-corner br"/>
          <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--g)',letterSpacing:2,marginBottom:14}}>CHANGE PASSWORD</div>
          {msg&&<div style={{padding:'10px 14px',border:`1px solid ${msg.err?'var(--red)':'var(--g)'}`,color:msg.err?'var(--red)':'var(--g)',fontFamily:"'Share Tech Mono',monospace",fontSize:11,marginBottom:14}}>{msg.txt}</div>}
          <div className="form-group"><label className="form-label">Current Password</label><input className="form-input" type="password" value={pwd.old} onChange={e=>setPwd(p=>({...p,old:e.target.value}))}/></div>
          <div className="form-group"><label className="form-label">New Password</label><input className="form-input" type="password" value={pwd.newP} onChange={e=>setPwd(p=>({...p,newP:e.target.value}))}/></div>
          <div className="form-group"><label className="form-label">Confirm</label><input className="form-input" type="password" value={pwd.confirm} onChange={e=>setPwd(p=>({...p,confirm:e.target.value}))}/></div>
          <button className="btn btn-green" onClick={changePassword}>Update Password</button>
        </div>
        <div className="card">
          <div className="card-corner tl"/><div className="card-corner tr"/>
          <div className="card-corner bl"/><div className="card-corner br"/>
          <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--g)',letterSpacing:2,marginBottom:14}}>SUPABASE CONNECTION</div>
          {testMsg&&<div style={{padding:'10px 14px',border:`1px solid ${testMsg.ok?'var(--g)':'var(--amber)'}`,color:testMsg.ok?'var(--g)':'var(--amber)',fontFamily:"'Share Tech Mono',monospace",fontSize:11,marginBottom:14}}>{testMsg.txt}</div>}
          {[{l:'Project URL',v:sbCfg?.url||'—'},{l:'Key',v:sbCfg?.anonKey?.slice(0,24)+'…'||'—'},{l:'Table',v:'portfolio_data'},{l:'Realtime',v:'Enabled'}].map(i=>(
            <div key={i.l} style={{display:'flex',justifyContent:'space-between',padding:'10px 0',borderBottom:'1px solid rgba(26,46,28,.4)'}}>
              <span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:11,color:'var(--tx3)'}}>{i.l}</span>
              <span style={{fontFamily:"'Share Tech Mono',monospace",fontSize:11,color:'var(--g)',maxWidth:200,overflow:'hidden',textOverflow:'ellipsis'}}>{i.v}</span>
            </div>
          ))}
          <div style={{display:'flex',gap:10,marginTop:16}}>
            <button className="btn btn-ghost btn-sm" onClick={testConn} disabled={testing}>{testing?'Testing…':'Test Connection'}</button>
            <button className="btn btn-red btn-sm" onClick={()=>{if(window.confirm('Disconnect Supabase?')){clearSupabaseConfig();resetClient();onDisconnect()}}}>Disconnect</button>
          </div>
        </div>
      </div>
      <div className="card">
        <div className="card-corner tl"/><div className="card-corner tr"/>
        <div className="card-corner bl"/><div className="card-corner br"/>
        <div style={{fontFamily:"'Share Tech Mono',monospace",fontSize:10,color:'var(--g)',letterSpacing:2,marginBottom:14}}>DATA BACKUP</div>
        <p style={{fontSize:13,color:'var(--tx2)',lineHeight:1.6,marginBottom:16}}>Export all portfolio data as JSON backup. Data is also safely stored in Supabase — this is just an extra copy.</p>
        <button className="btn btn-green" onClick={exportData}>↓ Export JSON Backup</button>
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// ROOT APP
// ══════════════════════════════════════════════════════════════════════════
const DEFAULTS = {
  about:       {name:'Mohamed Aasiq',title:'Cybersecurity Analyst',bio:'',location:'Madurai, India',availability:'open'},
  skills:      [],
  credentials: [],
  projects:    [],
  flags:       [],
  experience:  [],
  contact:     {email:'',phone:''},
}

export default function App() {
  const [setup,    setSetup]   = useState(!getSupabaseConfig())
  const [authed,   setAuthed]  = useState(false)
  const [page,     setPage]    = useState('dashboard')
  const [data,     setData]    = useState(DEFAULTS)
  const [loading,  setLoading] = useState(true)
  const [syncState,setSyncState] = useState('idle') // 'idle' | 'saving' | 'saved'
  const [lastSync, setLastSync]  = useState(null)
  const syncTimer = useRef(null)
  const sbCfg = getSupabaseConfig()

  // Load data from Supabase on auth
  useEffect(()=>{
    if (!authed) return
    ;(async()=>{
      setLoading(true)
      const loaded = await loadAll(DEFAULTS)
      setData(loaded)
      setLoading(false)
    })()
  }, [authed])

  // Subscribe to realtime changes (other editors)
  useEffect(()=>{
    if (!authed) return
    const unsub = subscribeToChanges(payload => {
      const section = payload.new?.id
      const value   = payload.new?.data
      if (section && value && DEFAULTS[section]!==undefined) {
        setData(d=>({...d,[section]:value}))
      }
    })
    return unsub
  }, [authed])

  const handleSave = useCallback(async (section, value) => {
    setSyncState('saving')
    try {
      await saveSection(section, value)
      setData(d=>({...d,[section]:value}))
      const ts = new Date().toLocaleTimeString('en-GB',{hour12:false})
      setLastSync(ts)
      setSyncState('saved')
      clearTimeout(syncTimer.current)
      syncTimer.current = setTimeout(()=>setSyncState('idle'), 2500)
    } catch(e) {
      setSyncState('idle')
      alert('Save failed: '+e.message)
    }
  }, [])

  const counts = {
    skills:      data.skills?.reduce((a,c)=>a+(c.items?.length||0),0)||0,
    credentials: data.credentials?.length||0,
    projects:    data.projects?.length||0,
    flags:       data.flags?.length||0,
    experience:  data.experience?.length||0,
  }

  if (setup) return <SetupWizard onComplete={()=>setSetup(false)}/>
  if (!authed) return <Login onAuth={()=>setAuthed(true)}/>

  if (loading) return (
    <>
      <FontLink/><style>{CSS}</style>
      <div style={{minHeight:'100vh',display:'flex',alignItems:'center',justifyContent:'center',background:'var(--bg)',fontFamily:"'Share Tech Mono',monospace",color:'var(--g)',fontSize:14,letterSpacing:3}}>
        LOADING FROM SUPABASE...
      </div>
    </>
  )

  return (
    <>
      <FontLink/>
      <style>{CSS}</style>
      {syncState==='saving' && <div className="saving-bar"/>}
      <div className="shell">
        <aside className="sidebar">
          <div className="logo">
            <div className="logo-title">⬡ PORTFOLIO OS</div>
            <div className="logo-sub">ADMIN PANEL · v3.0</div>
          </div>
          <div className="sb-status">
            <div style={{width:6,height:6,borderRadius:'50%',background:'var(--g)',boxShadow:'0 0 6px var(--g)'}}/>
            <span style={{color:'var(--g)'}}>SUPABASE LIVE</span>
          </div>
          <nav className="nav">
            {NAV.map(n=>(
              <div key={n.id} className={`nav-item${page===n.id?' active':''}`} onClick={()=>setPage(n.id)}>
                <span className="nav-icon">{n.icon}</span>
                <span>{n.label}</span>
                {n.id==='credentials'&&counts.credentials>0&&<span className="nav-badge">{counts.credentials}</span>}
                {n.id==='projects'&&counts.projects>0&&<span className="nav-badge">{counts.projects}</span>}
                {n.id==='flags'&&counts.flags>0&&<span className="nav-badge">{counts.flags}</span>}
                {n.id==='skills'&&counts.skills>0&&<span className="nav-badge">{counts.skills}</span>}
              </div>
            ))}
          </nav>
          <div className="sidebar-footer">
            <div style={{marginBottom:6,letterSpacing:1}}>Logged in as admin</div>
            <div style={{color:'var(--g)',marginBottom:2,letterSpacing:1}}>● SYSTEM ONLINE</div>
            <button className="logout-btn" onClick={()=>setAuthed(false)}>⏻ LOGOUT</button>
          </div>
        </aside>
        <div className="main">
          <div className="topbar">
            <div className="topbar-left">
              <div className="status-dot"/>
              <span className="topbar-title">PORTFOLIO ADMIN</span>
              <span className="topbar-breadcrumb">/ {NAV.find(n=>n.id===page)?.label}</span>
            </div>
            <div className="topbar-right">
              <Clock/>
              <span className="badge badge-green" style={{fontSize:10}}>LIVE</span>
            </div>
          </div>
          <div className="content">
            {page==='dashboard'   && <Dashboard   data={data} lastSync={lastSync} sbCfg={sbCfg}/>}
            {page==='about'       && <AboutSection       data={data.about}       onSave={v=>handleSave('about',v)}/>}
            {page==='skills'      && <SkillsSection       data={data.skills}      onSave={v=>handleSave('skills',v)}/>}
            {page==='credentials' && <CredentialsSection  data={data.credentials} onSave={v=>handleSave('credentials',v)}/>}
            {page==='projects'    && <ProjectsSection     data={data.projects}    onSave={v=>handleSave('projects',v)}/>}
            {page==='flags'       && <FlagsSection         data={data.flags}       onSave={v=>handleSave('flags',v)}/>}
            {page==='experience'  && <ExperienceSection   data={data.experience}  onSave={v=>handleSave('experience',v)}/>}
            {page==='contact'     && <ContactSection      data={data.contact}     onSave={v=>handleSave('contact',v)}/>}
            {page==='settings'    && <SettingsSection     data={data} sbCfg={sbCfg} onDisconnect={()=>setSetup(true)}/>}
          </div>
        </div>
      </div>
      <SyncToast state={syncState}/>
    </>
  )
}
