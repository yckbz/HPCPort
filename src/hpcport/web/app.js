// HPCPort Local Web Dashboard - Zero-build Vanilla ES Module App

const i18nUrl = new URL("./i18n.js", import.meta.url);
i18nUrl.search = new URL(import.meta.url).search;
const { createI18n } = await import(i18nUrl);

const ALLOWABLE_RULES = ["scancel", "outside", "root", "startup", "crontab", "remote", "killall"];
const VALID_TABS = new Set(["approvals", "jobs", "audit", "settings"]);

const i18n = createI18n(localStorage.getItem("hpcport_lang") || "auto");

const state = {
  authenticated: false,
  tab: "approvals",
  project: localStorage.getItem("hpcport_project") || "",
  theme: localStorage.getItem("hpcport_theme") || (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light"),
  alertsEnabled: localStorage.getItem("hpcport_alerts") === "1",
  showHistory: false,
  batchFilterStatus: "all",
  batchFilterCluster: "",
  auditTool: "",
  auditLimit: 50,

  // Data caches
  snapshot: null,
  snapshotRequest: 0,
  snapshotApplied: 0,
  tabRequest: 0,
  approvals: [],
  approvalsHistory: [],
  jobsData: null,
  expandedBatches: {},      // key "proj::batch" -> job rows
  auditRows: [],
  snippetsCache: {},        // project -> { codex, opencode, ... }
  activeSnippetTab: {},     // project -> agent key
  testResults: {},          // preset -> { ok, text }
  openScripts: new Set(),   // approval ids showing scripts
  openStanding: new Set(),  // approval ids showing standing form
  openDiffs: new Set(),     // approval ids showing unified diff
  expandedOps: new Set(),   // op ids showing full command/path inline
  openPolicies: new Set(),  // project names with Policies <details> open
  commentDrafts: {},        // approval id -> draft comment text
  standingDrafts: {},       // approval id -> { field: draft string }

  // Live SSE
  sse: null,
  sseConnected: false,
  lastPendingIds: new Set(),
  pendingIdsReady: false,
  pendingEvents: new Set(),
  refreshTimer: null,

  // Drawer state
  drawer: null,
};

// ------------------------------------------------------------------ helpers
function esc(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function fmtTime(ts) {
  if (!ts) return "-";
  const d = new Date(ts * 1000);
  return d.toLocaleString(i18n.lang === "zh" ? "zh-CN" : "en-US", {
    month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false,
  });
}

function fmtRel(ts) {
  if (!ts) return i18n.t("neverSynced");
  const diff = Math.max(0, Math.round(Date.now() / 1000 - ts));
  if (diff < 60) return `${diff}s`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h`;
  return `${Math.floor(diff / 86400)}d`;
}

function fmtNum(v) {
  if (v === null || v === undefined) return "-";
  const n = Number(v);
  if (Number.isNaN(n)) return String(v);
  if (n === 0) return "0";
  const a = Math.abs(n);
  if (a < 0.0001) return n > 0 ? "<0.0001" : ">-0.0001";
  let s;
  if (a >= 100) s = n.toFixed(1);
  else if (a >= 1) s = n.toFixed(2);
  else if (a >= 0.01) s = n.toFixed(3);
  else s = n.toFixed(4);
  return s.replace(/(\.\d*?[1-9])0+$/, "$1").replace(/\.0+$/, "");
}

function fmtDur(seconds) {
  if (seconds === null || seconds === undefined) return "-";
  const s = Number(seconds);
  if (Number.isNaN(s) || s < 0) return "-";
  if (s > 0 && s < 1) return "<1s";
  const total = Math.round(s);
  if (total < 60) return `${total}s`;
  const m = Math.floor(total / 60);
  const remS = total % 60;
  if (m < 60) return remS ? `${m}m ${String(remS).padStart(2, "0")}s` : `${m}m`;
  const h = Math.floor(m / 60);
  const remM = m % 60;
  return remM ? `${h}h ${String(remM).padStart(2, "0")}m` : `${h}h`;
}

function fmtHoursHint(v) {
  if (v === null || v === undefined) return "";
  const n = Number(v);
  if (Number.isNaN(n) || n <= 0 || n >= 1) return "";
  return `${n.toFixed(4)} h (${fmtDur(n * 3600)})`;
}

function fmtFailRate(rate) {
  if (!rate || rate <= 0) return "0%";
  const pct = rate * 100;
  if (pct < 1) return "<1%";
  return `${Math.round(pct)}%`;
}

function parseStrictNumber(raw, { integer = false, min = null, max = null } = {}) {
  const s = String(raw ?? "").trim();
  if (!s) return { ok: false, value: null };
  const n = Number(s);
  if (!Number.isFinite(n)) return { ok: false, value: null };
  if (integer && !Number.isInteger(n)) return { ok: false, value: null };
  if (min !== null && n < min) return { ok: false, value: null };
  if (max !== null && n > max) return { ok: false, value: null };
  return { ok: true, value: n };
}

function toast(msg, isError = false) {
  const root = document.getElementById("toast-root");
  const el = document.createElement("div");
  el.className = "toast" + (isError ? " error" : "");
  el.textContent = msg;
  root.appendChild(el);
  setTimeout(() => {
    el.style.opacity = "0";
    setTimeout(() => el.remove(), 220);
  }, 3200);
}

async function api(path, opts = {}) {
  const method = (opts.method || "GET").toUpperCase();
  const headers = Object.assign({}, opts.headers || {});
  if (method !== "GET") {
    headers["X-HPCPort-UI"] = "1";
    if (opts.body !== undefined) {
      headers["Content-Type"] = "application/json";
    }
  }
  const resp = await fetch(path, {
    method,
    headers,
    body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
    credentials: "same-origin",
  });
  let data = {};
  try {
    data = await resp.json();
  } catch {
    data = {};
  }
  if (resp.status === 401) {
    showAuthGate();
    throw new Error(data.error || "Authentication required");
  }
  if (!resp.ok) {
    const error = new Error(data.error || `HTTP ${resp.status}`);
    error.data = data;
    throw error;
  }
  return data;
}

function applyTheme() {
  document.documentElement.setAttribute("data-theme", state.theme);
  localStorage.setItem("hpcport_theme", state.theme);
  const sun = document.getElementById("theme-icon-sun");
  const moon = document.getElementById("theme-icon-moon");
  if (sun && moon) {
    sun.classList.toggle("hidden", state.theme !== "dark");
    moon.classList.toggle("hidden", state.theme === "dark");
  }
}

function updateLangButtonLabels() {
  for (const group of document.querySelectorAll(".lang-pill")) {
    for (const btn of group.querySelectorAll("button")) {
      btn.classList.toggle("active", btn.dataset.lang === i18n.lang);
      btn.textContent = btn.dataset.lang === "en" ? i18n.t("langEn") : i18n.t("langZh");
    }
  }
}

function applyStaticLabels() {
  document.querySelectorAll("[data-i18n]").forEach((el) => {
    el.textContent = i18n.t(el.dataset.i18n);
  });
  document.getElementById("auth-title").textContent = i18n.t("unlockTitle");
  document.getElementById("auth-desc").textContent = i18n.t("unlockDesc");
  document.getElementById("pin-input").placeholder = i18n.t("unlockPlaceholder");
  document.getElementById("pin-submit").textContent = i18n.t("unlockBtn");
  document.getElementById("auth-tip").textContent = i18n.t("unlockTip");
  document.getElementById("notif-btn-label").textContent = state.alertsEnabled ? i18n.t("alertsOn") : i18n.t("alertsOff");
  document.getElementById("sse-label").textContent = state.sseConnected ? i18n.t("liveConnected") : i18n.t("liveReconnecting");
  document.getElementById("drawer-refresh").textContent = i18n.t("refresh");
  updateLangButtonLabels();
}

async function setLanguage(lang, persistServer = true) {
  i18n.setLang(lang);
  localStorage.setItem("hpcport_lang", i18n.lang);
  applyStaticLabels();
  renderAll();
  if (persistServer && state.authenticated) {
    try {
      await api("/api/config", { method: "PATCH", body: { language: i18n.lang } });
    } catch {
      // Ignore if offline
    }
  }
}

// ------------------------------------------------------------------ Auth & Boot
function showAuthGate() {
  state.authenticated = false;
  closeModal();
  if (state.sse) {
    state.sse.close();
    state.sse = null;
  }
  document.getElementById("app-shell").classList.add("hidden");
  document.getElementById("auth-gate").classList.remove("hidden");
  applyStaticLabels();
}

function applyActiveTabDom(tab) {
  if (!VALID_TABS.has(tab)) tab = "approvals";
  state.tab = tab;
  document.querySelectorAll(".nav-tab").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === tab);
  });
  ["approvals", "jobs", "audit", "settings"].forEach((t) => {
    document.getElementById(`view-${t}`).classList.toggle("hidden", t !== tab);
  });
}

function showAppShell() {
  state.authenticated = true;
  document.getElementById("auth-gate").classList.add("hidden");
  document.getElementById("app-shell").classList.remove("hidden");
  applyActiveTabDom(state.tab);
  applyStaticLabels();
  connectSSE();
  refreshAll();
}

function readHashTab() {
  const h = (location.hash || "").replace(/^#/, "").trim();
  if (VALID_TABS.has(h)) return h;
  return "approvals";
}

function switchTab(tab, pushHash = true) {
  applyActiveTabDom(tab);
  if (pushHash && location.hash !== `#${state.tab}`) {
    history.replaceState(null, "", `#${state.tab}`);
  }
  refreshActiveTab();
}

// ------------------------------------------------------------------ SSE
function connectSSE() {
  if (state.sse) state.sse.close();
  const es = new EventSource("/api/events");
  state.sse = es;
  const pill = document.getElementById("sse-indicator");

  es.addEventListener("connected", () => {
    state.sseConnected = true;
    pill.classList.add("connected");
    document.getElementById("sse-label").textContent = i18n.t("liveConnected");
  });

  es.addEventListener("settings_changed", () => scheduleRefresh("settings_changed"));
  es.addEventListener("jobs_changed", () => scheduleRefresh("jobs_changed"));
  es.addEventListener("ops_changed", () => scheduleRefresh("ops_changed"));
  es.addEventListener("approvals_changed", () => scheduleRefresh("approvals_changed"));

  es.onerror = () => {
    state.sseConnected = false;
    pill.classList.remove("connected");
    document.getElementById("sse-label").textContent = i18n.t("liveReconnecting");
  };
}

function scheduleRefresh(eventName = "all") {
  state.pendingEvents.add(eventName);
  if (state.refreshTimer) clearTimeout(state.refreshTimer);
  state.refreshTimer = setTimeout(async () => {
    state.refreshTimer = null;
    const events = new Set(state.pendingEvents);
    state.pendingEvents.clear();
    await refreshAll(events);
  }, 180);
}

// ------------------------------------------------------------------ Data Fetching
async function syncGlobalPendingAlerts(notifyNew) {
  const globalPending = await api("/api/approvals");
  const allList = globalPending.approvals || [];
  if (notifyNew && state.pendingIdsReady && state.alertsEnabled
      && "Notification" in window && Notification.permission === "granted") {
    for (const a of allList) {
      if (!state.lastPendingIds.has(a.id)) {
        new Notification(`HPCPort: ${a.summary}`, { body: `${a.project || "global"} (#${a.id})` });
      }
    }
  }
  state.lastPendingIds = new Set(allList.map((a) => a.id));
  state.pendingIdsReady = true;
  return allList;
}

function isEditingInside(el) {
  const active = document.activeElement;
  if (!el || !active || !el.contains(active)) return false;
  return ["INPUT", "TEXTAREA", "SELECT"].includes(active.tagName);
}

function isRetiredProject(project = state.project) {
  return state.snapshot?.retired_projects?.includes(project) || false;
}

function resetBatchFilters() {
  state.batchFilterStatus = "all";
  state.batchFilterCluster = "";
}

async function refreshAll(eventsOrFlag = null) {
  if (!state.authenticated) return;
  const events = eventsOrFlag instanceof Set
    ? eventsOrFlag
    : new Set(eventsOrFlag === true ? ["all", "approvals_changed"] : ["all"]);
  const wantAll = events.has("all");
  const hasApprovals = events.has("approvals_changed");
  const hasJobs = events.has("jobs_changed") || events.has("ops_changed");
  const hasSettings = events.has("settings_changed");
  const request = ++state.snapshotRequest;
  try {
    const snap = await api("/api/state");
    if (request < state.snapshotApplied) return;
    state.snapshotApplied = request;
    const prevNames = (state.snapshot?.projects || []).map((p) => p.name).join("\0");
    const prevRetired = (state.snapshot?.retired_projects || []).join("\0");
    const wasRetired = isRetiredProject();
    state.snapshot = snap;
    const names = (snap.projects || []).map((p) => p.name);
    const retired = snap.retired_projects || [];
    if (state.project && !names.includes(state.project) && !isRetiredProject()) {
      state.project = "";
      localStorage.setItem("hpcport_project", "");
    }
    if (!wasRetired && isRetiredProject()) resetBatchFilters();
    if (wantAll || names.join("\0") !== prevNames || retired.join("\0") !== prevRetired) {
      renderProjectSelect();
    }
    updateApprovalsBadge(snap.pending_approvals || 0);
    if (!state.pendingIdsReady || hasApprovals) {
      await syncGlobalPendingAlerts(hasApprovals);
    }
    // Scope tab re-renders to relevant events so background job ticks do not wipe forms
    if (state.tab === "approvals" && !(wantAll || hasApprovals || hasSettings)) {
      return;
    }
    if (state.tab === "jobs" && !(wantAll || hasJobs || hasApprovals || hasSettings)) {
      return;
    }
    if (state.tab === "settings") {
      if (!(wantAll || hasSettings)) return;
      if (!wantAll && isEditingInside(document.getElementById("view-settings"))) return;
    }
    await refreshActiveTab();
  } catch (e) {
    if (state.authenticated) toast(e.message, true);
  }
}

async function refreshActiveTab() {
  if (!state.authenticated) return;
  const { project, tab } = state;
  const retired = isRetiredProject();
  const request = ++state.tabRequest;
  const current = () => request === state.tabRequest && state.project === project
    && state.tab === tab && isRetiredProject() === retired;
  try {
    const qProj = project ? `project=${encodeURIComponent(project)}` : "";
    if (tab === "approvals") {
      const pending = retired ? [] : (await api(`/api/approvals${qProj ? "?" + qProj : ""}`)).approvals;
      const all = state.showHistory || retired
        ? await api(`/api/approvals?all=1${qProj ? "&" + qProj : ""}`) : null;
      if (!current()) return;
      const list = pending || [];
      state.approvals = list;
      state.approvalsHistory = (all?.approvals || []).filter((a) => a.state !== "pending");
      updateApprovalsBadge(state.snapshot ? state.snapshot.pending_approvals : list.length);
      renderApprovals();
    }
    if (tab === "jobs") {
      const qs = [];
      if (qProj) qs.push(qProj);
      if (state.batchFilterCluster) qs.push(`cluster=${encodeURIComponent(state.batchFilterCluster)}`);
      const data = await api(`/api/jobs${qs.length ? "?" + qs.join("&") : ""}`);
      if (!current()) return;
      state.jobsData = data;
      const visibleKeys = new Set((state.jobsData.batches || []).map((b) => `${b.project}::${b.batch}`));
      for (const k of Object.keys(state.expandedBatches)) {
        if (!visibleKeys.has(k)) delete state.expandedBatches[k];
      }
      const openKeys = Object.keys(state.expandedBatches);
      if (openKeys.length) {
        await Promise.all(openKeys.map(async (k) => {
          const [proj, batch] = k.split("::");
          try {
            const res = await api(`/api/jobs/${encodeURIComponent(proj)}/${encodeURIComponent(batch)}`);
            if (current()) state.expandedBatches[k] = res.jobs || [];
          } catch {
            if (current()) delete state.expandedBatches[k];
          }
        }));
      }
      if (current()) renderJobs();
    }
    if (tab === "audit") {
      const qs = [`n=${state.auditLimit}`];
      if (qProj) qs.push(qProj);
      if (state.auditTool) qs.push(`tool=${encodeURIComponent(state.auditTool)}`);
      const res = await api(`/api/audit?${qs.join("&")}`);
      if (!current()) return;
      state.auditRows = res.audit || [];
      renderAudit();
    }
    if (tab === "settings") {
      await ensureSnippetsLoaded();
      if (current()) renderSettings();
    }
  } catch (e) {
    if (state.authenticated) toast(e.message, true);
  }
}

async function ensureSnippetsLoaded() {
  const projs = (state.snapshot && state.snapshot.projects) || [];
  await Promise.all(
    projs.map(async (p) => {
      if (!state.snippetsCache[p.name]) {
        try {
          const r = await api(`/api/projects/${encodeURIComponent(p.name)}/mcp-snippets`);
          state.snippetsCache[p.name] = r.snippets || {};
        } catch {
          state.snippetsCache[p.name] = {};
        }
      }
    })
  );
}

function updateApprovalsBadge(n) {
  const b = document.getElementById("approvals-badge");
  b.textContent = String(n);
  b.classList.toggle("hidden", !n);
  document.title = n > 0 ? `(${n}) HPCPort` : "HPCPort";
}

function renderProjectSelect() {
  const sel = document.getElementById("project-select");
  const projects = (state.snapshot && state.snapshot.projects) || [];
  const opts = [`<option value="">${esc(i18n.t("allProjects"))}</option>`];
  for (const p of projects) {
    opts.push(`<option value="${esc(p.name)}"${p.name === state.project ? " selected" : ""}>${esc(p.name)}</option>`);
  }
  for (const name of state.snapshot?.retired_projects || []) {
    const label = i18n.t("retiredProjectOption", { project: name });
    opts.push(`<option value="${esc(name)}"${name === state.project ? " selected" : ""}>${esc(label)}</option>`);
  }
  sel.innerHTML = opts.join("");
}

function renderAll() {
  renderProjectSelect();
  if (state.tab === "approvals") renderApprovals();
  if (state.tab === "jobs") renderJobs();
  if (state.tab === "audit") renderAudit();
  if (state.tab === "settings") renderSettings();
}

// ------------------------------------------------------------------ TAB 1: APPROVALS
function renderReasonChip(r) {
  const code = r.code || r.rule || "";
  if (code === "over") {
    return `<span class="pill pill-amber">${esc(i18n.t("reason_over"))}: ${esc(i18n.t("limField_" + r.what))} ${fmtNum(r.asked)} &gt; ${fmtNum(r.limit)}</span>`;
  }
  if (code === "total") {
    return `<span class="pill pill-amber">${esc(i18n.t("reason_total"))}: ${esc(i18n.t("limField_" + (TOTAL_LIMIT_KEY[r.what] || r.what)))} ${fmtNum(r.reached)} &gt; ${fmtNum(r.limit)}</span>`;
  }
  if (code === "unknown") {
    return `<span class="pill pill-amber">${esc(i18n.t("reason_unknown"))} (${esc((r.scripts || []).join(", "))})</span>`;
  }
  if (code === "no_standing") {
    return `<span class="pill pill-blue">${esc(i18n.t("reason_no_standing"))}</span>`;
  }
  if (code === "size") {
    return `<span class="pill pill-amber">${esc(i18n.t("guardRule_size"))}: ${fmtNum(r.size)} GB &gt; ${fmtNum(r.limit)} GB</span>`;
  }
  if (code) {
    const key = "guardRule_" + code;
    const translated = i18n.t(key);
    const desc = translated !== key ? translated : (r.detail || "");
    const target = r.what ? ` (${r.what})` : "";
    return `<span class="pill pill-amber">${esc(code)}${desc ? ": " + esc(desc) : ""}${esc(target)}</span>`;
  }
  return `<span class="pill pill-gray">${esc(JSON.stringify(r))}</span>`;
}

function renderUnifiedDiff(text) {
  if (!text) return "";
  return text.split("\n").map((line) => {
    if (line.startsWith("+++") || line.startsWith("---") || line.startsWith("@@")) {
      return `<span class="diff-line-hdr">${esc(line)}</span>`;
    }
    if (line.startsWith("+")) return `<span class="diff-line-add">${esc(line)}</span>`;
    if (line.startsWith("-")) return `<span class="diff-line-del">${esc(line)}</span>`;
    return `<span>${esc(line)}\n</span>`;
  }).join("");
}

function renderApprovalCard(a) {
  const d = a.detail || {};
  const kindBadge =
    a.kind === "submit" ? `<span class="pill pill-blue">${esc(i18n.t("kindSubmit"))}</span>` :
    a.kind === "permission" ? `<span class="pill pill-amber">${esc(i18n.t("kindPermission"))}</span>` :
    `<span class="pill pill-purple">${esc(i18n.t(d.origin === "detected" ? "kindDiskEdit" : "kindProposal"))}</span>`;

  let bodyHtml = "";
  let actionButtons = "";

  if (a.kind === "submit") {
    const sub = d.submission || {};
    const why = d.why || [];
    const scripts = sub.script_texts || {};
    const scriptNames = Object.keys(scripts);
    const scriptsOpen = state.openScripts.has(a.id);
    const standingOpen = state.openStanding.has(a.id);
    const budget = budgetOf(a.project);
    const budgetActive = budgetMode(budget) === "active";
    const baseLimits = budgetActive ? budget.standing.limits : (budget?.defaults || {});
    const sd = state.standingDrafts[a.id] || {};
    const limValues = Object.fromEntries(LIMIT_FIELDS.map((f) => [
      f.key, Object.prototype.hasOwnProperty.call(sd, f.key) ? sd[f.key] : baseLimits[f.key],
    ]));

    bodyHtml = `
      <div class="muted text-sm">
        <span class="mono">${esc(sub.cluster || "")}</span> &middot;
        <span class="mono">${esc(sub.batch || "")}</span> &middot;
        <span class="mono">${esc(sub.workdir || "")}</span>
        ${sub.tag ? ` &middot; <span class="pill pill-gray">${esc(sub.tag)}</span>` : ""}
      </div>
      <div class="metrics-strip">
        <div class="metric-chip"><span class="k">${esc(i18n.t("metricJobs"))}</span><span class="v">${esc(sub.jobs ?? 0)}</span></div>
        <div class="metric-chip" title="${esc(fmtHoursHint(sub.core_hours))}"><span class="k">${esc(i18n.t("metricCoreH"))}</span><span class="v">${fmtNum(sub.core_hours)}</span></div>
        <div class="metric-chip" title="${esc(fmtHoursHint(sub.gpu_hours))}"><span class="k">${esc(i18n.t("metricGpuH"))}</span><span class="v">${fmtNum(sub.gpu_hours)}</span></div>
      </div>
      ${why.length ? `<div class="reason-row">${why.map(renderReasonChip).join("")}</div>` : ""}
      ${renderBudgetContext(budget)}
      ${scriptNames.length ? `
        <div style="margin-top:8px">
          <button type="button" class="btn btn-subtle btn-xs" data-toggle-scripts="${a.id}">
            ${esc(scriptsOpen ? i18n.t("hideScripts") : i18n.t("previewScripts"))} (${scriptNames.length})
          </button>
          ${scriptsOpen ? scriptNames.map((fn) => `
            <div style="margin-top:8px">
              <div class="mono text-xs muted">${esc(fn)}</div>
              <pre class="code-block">${esc(scripts[fn])}</pre>
            </div>
          `).join("") : ""}
        </div>
      ` : ""}
      ${standingOpen ? `
        <div class="standing-drawer" data-standing-box="${a.id}">
          <div class="text-sm" style="font-weight:600">${esc(i18n.t(budgetActive ? "standingAdjustTitle" : "standingFormTitle"))}</div>
          <div class="muted text-xs" style="margin-top:4px">
            ${esc(i18n.t(budgetActive ? "standingAdjustNote" : "standingGrantNote"))}
            ${esc(i18n.t("budgetBlankHint"))}
          </div>
          ${renderLimitInputs(limValues, { used: budgetActive ? budget.used : null, needs: budgetActive ? limitNeeds(why) : {} })}
          <div class="flex-row">
            <button type="button" class="btn btn-primary btn-sm" data-decide-standing="${a.id}">${esc(i18n.t(budgetActive ? "confirmAdjust" : "confirmGrant"))}</button>
            <button type="button" class="btn btn-outline btn-sm" data-toggle-standing="${a.id}">${esc(i18n.t("cancelBtn"))}</button>
          </div>
        </div>
      ` : ""}
    `;
    actionButtons = `
      <button type="button" class="btn btn-primary btn-sm" data-decide="approve" data-id="${a.id}">${esc(i18n.t("approveOnce"))}</button>
      ${budget && budget.approval ? `<button type="button" class="btn btn-soft btn-sm" data-toggle-standing="${a.id}">${esc(i18n.t(budgetActive ? "approveAndAdjust" : "approveAndStand"))}</button>` : ""}
      <button type="button" class="btn btn-danger btn-sm" data-decide="reject" data-id="${a.id}">${esc(i18n.t("reject"))}</button>
    `;
  } else if (a.kind === "permission") {
    const inp = d.input || {};
    const why = d.why || [];
    const preview = d.tool === "remote_exec"
      ? `cluster: ${inp.cluster || "-"}\ncwd:     ${inp.cwd || "(project root)"}\ncommand: ${inp.command || ""}`
      : JSON.stringify(inp, null, 2);
    bodyHtml = `
      ${why.length ? `<div class="reason-row">${why.map(renderReasonChip).join("")}</div>` : ""}
      <pre class="code-block">${esc(preview)}</pre>
    `;
    actionButtons = `
      <button type="button" class="btn btn-primary btn-sm" data-decide="approve" data-id="${a.id}">${esc(i18n.t("allowOnce"))}</button>
      <button type="button" class="btn btn-danger btn-sm" data-decide="reject" data-id="${a.id}">${esc(i18n.t("deny"))}</button>
    `;
  } else {
    // proposal
    const fields = d.fields || [];
    const diffOpen = state.openDiffs.has(a.id);
    const errMsg = d.error || "";
    bodyHtml = `
      ${d.shared ? `<div style="margin-bottom:8px"><span class="pill pill-purple">${esc(i18n.t("sharedBadge"))}</span></div>` : ""}
      ${errMsg ? `
        <div class="alert-banner-error">
          <div style="font-weight:600;font-size:13px">${esc(i18n.t("invalidYamlTitle"))}</div>
          <div class="mono text-xs" style="margin-top:4px;white-space:pre-wrap">${esc(errMsg)}</div>
          <div class="text-xs" style="margin-top:6px;opacity:0.9">${esc(i18n.t("invalidYamlHint"))}</div>
        </div>
      ` : ""}
      ${fields.length ? `
        <div class="table-wrap" style="margin-top:6px">
          <table>
            <thead>
              <tr>
                <th>${esc(i18n.t("fieldCol"))}</th>
                <th>${esc(i18n.t("currentCol"))}</th>
                <th>${esc(i18n.t("proposedCol"))}</th>
              </tr>
            </thead>
            <tbody>
              ${fields.map((f) => `
                <tr>
                  <td class="mono">
                    ${esc(f.name)}
                    ${f.safety ? `<span class="pill pill-red" style="margin-left:6px">${esc(i18n.t("safetyBadge"))}</span>` : ""}
                  </td>
                  <td class="mono muted">${esc(f.old)}</td>
                  <td class="mono" style="color:var(--green);font-weight:600">${esc(f.new)}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      ` : ""}
      ${d.diff ? `
        <div style="margin-top:10px">
          <button type="button" class="btn btn-subtle btn-xs" data-toggle-diff="${a.id}">${esc(i18n.t("unifiedDiff"))}</button>
          ${diffOpen ? `<pre class="code-block">${renderUnifiedDiff(d.diff)}</pre>` : ""}
        </div>
      ` : ""}
    `;
    actionButtons = errMsg ? `
      <button type="button" class="btn btn-primary btn-sm" disabled title="${esc(i18n.t("invalidYamlTitle"))}">${esc(i18n.t("approveApply"))}</button>
      <button type="button" class="btn btn-danger btn-sm" data-decide="reject" data-id="${a.id}">${esc(i18n.t("rejectRevert"))}</button>
    ` : `
      <button type="button" class="btn btn-primary btn-sm" data-decide="approve" data-id="${a.id}">${esc(i18n.t("approveApply"))}</button>
      <button type="button" class="btn btn-danger btn-sm" data-decide="reject" data-id="${a.id}">${esc(i18n.t("reject"))}</button>
    `;
  }

  return `
    <article class="approval-card" data-approval-card="${a.id}">
      <div class="approval-meta">
        ${kindBadge}
        ${a.project ? `<span class="pill pill-gray">${esc(a.project)}</span>` : ""}
        <span class="mono muted">#${a.id}</span>
        <span class="muted text-xs" style="margin-left:auto">${esc(fmtTime(a.created))}</span>
      </div>
      <h3 class="approval-summary">${esc(a.summary)}</h3>
      ${bodyHtml}
      <div class="approval-actions">
        <input type="text" data-comment-for="${a.id}" value="${esc(state.commentDrafts[a.id] || "")}" placeholder="${esc(i18n.t("commentPlaceholder"))}" />
        ${actionButtons}
      </div>
    </article>
  `;
}

function captureApprovalDrafts(el) {
  let focusInfo = null;
  const active = document.activeElement;
  el.querySelectorAll("[data-comment-for]").forEach((inp) => {
    const id = inp.dataset.commentFor;
    state.commentDrafts[id] = inp.value;
    if (active === inp) {
      focusInfo = {
        selector: `[data-comment-for="${id}"]`,
        start: inp.selectionStart,
        end: inp.selectionEnd,
      };
    }
  });
  el.querySelectorAll("[data-standing-box]").forEach((box) => {
    const id = box.dataset.standingBox;
    const bag = state.standingDrafts[id] || (state.standingDrafts[id] = {});
    box.querySelectorAll("input[name]").forEach((inp) => {
      bag[inp.name] = inp.value;
      if (active === inp) {
        focusInfo = {
          selector: `[data-standing-box="${id}"] input[name="${inp.name}"]`,
          start: null,
          end: null,
        };
      }
    });
  });
  return focusInfo;
}

function renderApprovals() {
  const el = document.getElementById("view-approvals");
  const focusInfo = captureApprovalDrafts(el);
  const retired = isRetiredProject();
  const pending = retired ? [] : state.approvals;
  const liveIds = new Set(pending.map((a) => String(a.id)));
  for (const k of Object.keys(state.commentDrafts)) {
    if (!liveIds.has(k)) delete state.commentDrafts[k];
  }
  for (const k of Object.keys(state.standingDrafts)) {
    if (!liveIds.has(k)) delete state.standingDrafts[k];
  }
  let html = `
    <div class="page-header">
      <div>
        <h1>${esc(i18n.t(retired ? "approvalHistory" : "approvalsTitle"))}</h1>
        <p>${esc(i18n.t(retired ? "approvalHistoryDesc" : "approvalsSubtitle", { project: state.project }))}</p>
      </div>
      ${retired ? "" : `<button type="button" class="btn btn-subtle btn-sm" id="toggle-history-btn">
        ${esc(state.showHistory ? i18n.t("hideHistory") : i18n.t("showHistory"))}
      </button>`}
    </div>
  `;

  if (!retired && !pending.length) {
    html += `
      <div class="empty-state">
        <div class="empty-icon">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none">
            <path d="M5 13l4 4L19 7" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </div>
        <h3>${esc(i18n.t("noApprovalsTitle"))}</h3>
        <p>${esc(i18n.t("noApprovalsDesc"))}</p>
      </div>
    `;
  } else {
    html += pending.map(renderApprovalCard).join("");
  }

  if (retired || state.showHistory) {
    const hist = retired ? state.approvalsHistory : state.approvalsHistory.slice(0, 30);
    html += `
      <div class="card" style="margin-top:20px">
        ${retired ? "" : `<div class="card-header"><h2 class="card-title">${esc(i18n.t("recentDecisions"))}</h2></div>`}
        ${!hist.length ? `<p class="muted text-sm">${esc(i18n.t("noDecisions"))}</p>` : `
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  <th>${esc(i18n.t("colProject"))}</th>
                  <th>${esc(i18n.t("colKind"))}</th>
                  <th>${esc(i18n.t("colSummary"))}</th>
                  <th>${esc(i18n.t("colStatus"))}</th>
                  <th>${esc(i18n.t("colTime"))}</th>
                </tr>
              </thead>
              <tbody>
                ${hist.map((a) => `
                  <tr>
                    <td class="mono muted">#${a.id}</td>
                    <td>${esc(a.project || "-")}</td>
                    <td><span class="pill pill-gray">${esc(a.kind)}</span></td>
                    <td>
                      <div>${esc(a.summary)}</div>
                      ${a.comment ? `<div class="muted text-xs">${esc(a.comment)}</div>` : ""}
                    </td>
                    <td>${renderStatePill(a.state)}</td>
                    <td class="muted text-xs tabular">${esc(fmtTime(a.decided || a.created))}</td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
          </div>
        `}
      </div>
    `;
  }

  el.innerHTML = html;

  el.querySelectorAll("[data-comment-for]").forEach((inp) => {
    inp.oninput = () => {
      state.commentDrafts[inp.dataset.commentFor] = inp.value;
    };
  });
  el.querySelectorAll("[data-standing-box]").forEach((box) => {
    const id = box.dataset.standingBox;
    bindLimitInputs(box);
    box.querySelectorAll("input[name]").forEach((inp) => {
      inp.oninput = () => {
        const bag = state.standingDrafts[id] || (state.standingDrafts[id] = {});
        bag[inp.name] = inp.value;
      };
    });
  });
  if (focusInfo) {
    const target = el.querySelector(focusInfo.selector);
    if (target) {
      target.focus();
      if (focusInfo.start !== null && typeof target.setSelectionRange === "function") {
        try {
          target.setSelectionRange(focusInfo.start, focusInfo.end);
        } catch {
          // Number inputs do not support selection ranges
        }
      }
    }
  }

  const toggleHistory = document.getElementById("toggle-history-btn");
  if (toggleHistory) {
    toggleHistory.onclick = async () => {
      state.showHistory = !state.showHistory;
      await refreshActiveTab();
    };
  }

  el.querySelectorAll("[data-toggle-scripts]").forEach((btn) => {
    btn.onclick = () => {
      const id = Number(btn.dataset.toggleScripts);
      if (state.openScripts.has(id)) state.openScripts.delete(id);
      else state.openScripts.add(id);
      renderApprovals();
    };
  });

  el.querySelectorAll("[data-toggle-standing]").forEach((btn) => {
    btn.onclick = () => {
      const id = Number(btn.dataset.toggleStanding);
      if (state.openStanding.has(id)) state.openStanding.delete(id);
      else state.openStanding.add(id);
      renderApprovals();
    };
  });

  el.querySelectorAll("[data-toggle-diff]").forEach((btn) => {
    btn.onclick = () => {
      const id = Number(btn.dataset.toggleDiff);
      if (state.openDiffs.has(id)) state.openDiffs.delete(id);
      else state.openDiffs.add(id);
      renderApprovals();
    };
  });

  el.querySelectorAll("[data-decide]").forEach((btn) => {
    btn.onclick = async () => {
      const id = Number(btn.dataset.id);
      const action = btn.dataset.decide;
      const comment = (el.querySelector(`[data-comment-for="${id}"]`)?.value || "").trim();
      btn.disabled = true;
      try {
        await api(`/api/approvals/${id}/${action}`, { method: "POST", body: { comment } });
        delete state.commentDrafts[String(id)];
        delete state.standingDrafts[String(id)];
        toast(i18n.t(action === "approve" ? "approvedToast" : "rejectedToast", { id }));
        await refreshAll();
      } catch (e) {
        toast(e.message, true);
        btn.disabled = false;
      }
    };
  });

  el.querySelectorAll("[data-decide-standing]").forEach((btn) => {
    btn.onclick = async () => {
      const id = Number(btn.dataset.decideStanding);
      const box = el.querySelector(`[data-standing-box="${id}"]`);
      const comment = (el.querySelector(`[data-comment-for="${id}"]`)?.value || "").trim();
      const limits = readLimitInputs(box);
      if (!limits) return;
      const payload = { always: true, comment, ...limits };
      btn.disabled = true;
      try {
        await api(`/api/approvals/${id}/approve`, { method: "POST", body: payload });
        state.openStanding.delete(id);
        delete state.commentDrafts[String(id)];
        delete state.standingDrafts[String(id)];
        toast(i18n.t("approvedToast", { id }));
        await refreshAll();
      } catch (e) {
        toast(e.message, true);
        btn.disabled = false;
      }
    };
  });
}

// ------------------------------------------------------------------ TAB 2: OVERVIEW & JOBS
function renderStatePill(st) {
  const s = String(st || "").toLowerCase();
  if (["done", "completed", "approved", "ended"].includes(s)) return `<span class="pill pill-green">${esc(st)}</span>`;
  if (["running", "queued", "active"].includes(s)) return `<span class="pill pill-blue">${esc(st)}</span>`;
  if (["held", "unapproved", "pending", "stopping", "timeout"].includes(s)) return `<span class="pill pill-amber">${esc(st)}</span>`;
  if (["failed", "rejected", "submit_failed", "lost", "error", "cancelled"].includes(s)) return `<span class="pill pill-red">${esc(st)}</span>`;
  return `<span class="pill pill-gray">${esc(st)}</span>`;
}

// ------------------------------------------------------------------ Standing budget (shared)
const LIMIT_GROUPS = [
  { title: "limGroupPerSubmit", fields: [
    { key: "core_hours" },
    { key: "gpu_hours" },
  ] },
  { title: "limGroupTotal", fields: [
    { key: "total_jobs", used: "jobs", integer: true },
    { key: "total_core_hours", used: "core_hours" },
    { key: "total_gpu_hours", used: "gpu_hours" },
  ] },
];
const LIMIT_FIELDS = LIMIT_GROUPS.flatMap((g) => g.fields);
const TOTAL_LIMIT_KEY = { jobs: "total_jobs", core_hours: "total_core_hours", gpu_hours: "total_gpu_hours" };
const USAGE_ROWS = [
  { key: "jobs", label: "budgetUsedJobs", limit: "total_jobs" },
  { key: "core_hours", label: "budgetUsedCoreH", limit: "total_core_hours" },
  { key: "gpu_hours", label: "budgetUsedGpuH", limit: "total_gpu_hours" },
];

function budgetOf(project) {
  return state.snapshot?.budgets?.[project] || null;
}

// "off" (approval gate disabled), "none" (no budget yet), or "active"
function budgetMode(b) {
  if (!b) return null;
  if (!b.approval) return "off";
  return b.standing ? "active" : "none";
}

// Budgets only matter for projects that can submit Slurm jobs (or have no server yet).
function projectUsesSlurm(project) {
  const p = (state.snapshot?.projects || []).find((x) => x.name === project);
  const names = Object.keys(p?.data?.clusters || {});
  if (!names.length) return true;
  const presets = state.snapshot?.presets || [];
  return names.some((n) => (presets.find((pr) => pr.name === n)?.data?.scheduler || "slurm") === "slurm");
}

function fmtLimit(v) {
  return v === null || v === undefined ? i18n.t("unlimited") : fmtNum(v);
}

function budgetModePill(mode) {
  if (mode === "active") return `<span class="pill pill-green">${esc(i18n.t("standingActive"))}</span>`;
  if (mode === "off") return `<span class="pill pill-gray">${esc(i18n.t("standingAuto"))}</span>`;
  return `<span class="pill pill-gray">${esc(i18n.t("standingOff"))}</span>`;
}

// One line such as "Jobs 12 / 500 · Core-h 820 / 100000 · GPU-h 3 / Unlimited"
function budgetUsageLine(b) {
  const lims = b.standing?.limits || {};
  return USAGE_ROWS.map((r) => {
    const used = b.used?.[r.key] || 0;
    const limit = lims[r.limit];
    const over = limit !== null && limit !== undefined && used >= limit;
    return `<span class="${over ? "budget-over" : ""}">${esc(i18n.t(r.label))} ${fmtNum(used)} / ${esc(fmtLimit(limit))}</span>`;
  }).join(" &middot; ");
}

function budgetActionButton(project, mode, size = "btn-sm") {
  if (mode === "active") {
    return `<button type="button" class="btn btn-soft ${size}" data-budget-open="${esc(project)}">${esc(i18n.t("adjustBudget"))}</button>`;
  }
  if (mode === "none") {
    return `<button type="button" class="btn btn-primary ${size}" data-budget-open="${esc(project)}">${esc(i18n.t("grantBudget"))}</button>`;
  }
  return "";
}

function bindBudgetButtons(el) {
  el.querySelectorAll("[data-budget-open]").forEach((btn) => {
    btn.onclick = () => openBudgetModal(btn.dataset.budgetOpen);
  });
}

function usedHintText(used, raw) {
  const s = String(raw ?? "").trim();
  const limit = s === "" ? null : Number(s);
  if (limit !== null && Number.isFinite(limit) && limit < used) {
    return { text: i18n.t("usedOverHint", { n: fmtNum(used) }), warn: true };
  }
  return { text: i18n.t("usedHint", { n: fmtNum(used) }), warn: false };
}

// Limit inputs grouped as "per submission" and "cumulative". Blank means unlimited.
// `used` adds a live "Used N" hint under cumulative fields; `needs` marks the limits a
// waiting submission is blocked by.
function renderLimitInputs(values, { used = null, needs = {} } = {}) {
  return LIMIT_GROUPS.map((g) => `
    <div class="limit-group">
      <div class="limit-group-title">${esc(i18n.t(g.title))}</div>
      <div class="standing-grid">
        ${g.fields.map((f) => {
          const raw = values?.[f.key];
          const value = raw === null || raw === undefined ? "" : raw;
          const need = needs[f.key];
          const u = !need && used && f.used ? (used[f.used] || 0) : null;
          const hint = u === null ? null : usedHintText(u, value);
          const needText = need
            ? i18n.t(need.total ? "limitNeedTotal" : "limitNeedOver", { need: fmtNum(need.need), limit: fmtNum(need.limit) })
            : "";
          return `
            <label class="field-label">${esc(i18n.t("limField_" + f.key))}
              <input type="number" step="${f.integer ? "1" : "any"}" min="0" name="${f.key}"
                value="${esc(value)}" placeholder="${esc(i18n.t("unlimited"))}"${u === null ? "" : ` data-used="${u}"`}${need ? ` data-need="${need.need}"` : ""} />
              ${hint ? `<span class="field-hint${hint.warn ? " warn" : ""}" data-used-hint="${f.key}">${esc(hint.text)}</span>` : ""}
              ${need ? `<span class="field-hint warn" data-need-hint="${f.key}"${needResolved(need.need, value) ? " hidden" : ""}>${esc(needText)}</span>` : ""}
            </label>
          `;
        }).join("")}
      </div>
    </div>
  `).join("");
}

function needResolved(need, raw) {
  const s = String(raw ?? "").trim();
  return s === "" || (Number.isFinite(Number(s)) && Number(s) >= need);
}

// Keep usage and "needed" hints live while typing; onChange lets callers react.
function bindLimitInputs(container, onChange = null) {
  container.querySelectorAll("input[data-need]").forEach((inp) => {
    const hintEl = container.querySelector(`[data-need-hint="${inp.name}"]`);
    inp.addEventListener("input", () => {
      if (hintEl) hintEl.hidden = needResolved(Number(inp.dataset.need), inp.value);
    });
  });
  container.querySelectorAll("input[data-used]").forEach((inp) => {
    const hintEl = container.querySelector(`[data-used-hint="${inp.name}"]`);
    inp.addEventListener("input", () => {
      if (!hintEl) return;
      const h = usedHintText(Number(inp.dataset.used), inp.value);
      hintEl.textContent = h.text;
      hintEl.classList.toggle("warn", h.warn);
    });
  });
  if (onChange) {
    container.querySelectorAll("input[name]").forEach((inp) => {
      inp.addEventListener("input", () => onChange());
    });
  }
}

// Returns the five limits (null = unlimited), or null after reporting the first invalid field.
function readLimitInputs(container, { report = true } = {}) {
  const out = {};
  for (const f of LIMIT_FIELDS) {
    const inp = container.querySelector(`input[name="${f.key}"]`);
    const raw = (inp?.value || "").trim();
    if (raw === "") {
      out[f.key] = null;
      continue;
    }
    const parsed = parseStrictNumber(raw, { integer: Boolean(f.integer), min: 0 });
    if (!parsed.ok) {
      if (report) {
        toast(i18n.t("invalidNumber", { field: i18n.t("limName_" + f.key) }), true);
        inp?.focus();
      }
      return null;
    }
    out[f.key] = parsed.value;
  }
  return out;
}

function sameLimits(a, b) {
  return LIMIT_FIELDS.every((f) => (a?.[f.key] ?? null) === (b?.[f.key] ?? null));
}

// Which limits a waiting submission is blocked by, keyed by limit name.
function limitNeeds(why) {
  const needs = {};
  for (const r of why || []) {
    if (r.code === "over" && (r.what === "core_hours" || r.what === "gpu_hours")) {
      needs[r.what] = { need: r.asked, limit: r.limit, total: false };
    }
    if (r.code === "total" && TOTAL_LIMIT_KEY[r.what]) {
      needs[TOTAL_LIMIT_KEY[r.what]] = { need: r.reached, limit: r.limit, total: true };
    }
  }
  return needs;
}

function renderBudgetBars(b) {
  const lims = b.standing?.limits || {};
  const used = b.used;
  return `
    <div class="budget-grid">
      ${USAGE_ROWS.map((r) => {
        const limit = lims[r.limit];
        const total = used ? (used[r.key] || 0) : 0;
        const done = used?.measured?.[r.key] || 0;
        const wait = used?.waiting?.[r.key] || 0;
        const active = used?.active?.[r.key] !== undefined ? used.active[r.key] : Math.max(0, total - done - wait);
        const denom = limit && limit > 0 ? limit : Math.max(total, 1);
        const pDone = Math.min(100, (done / denom) * 100);
        const pAct = Math.min(100 - pDone, (active / denom) * 100);
        const pWait = Math.min(100 - pDone - pAct, (wait / denom) * 100);
        const seg = (v, p) => (v > 0 ? `width:${p}%;min-width:4px` : "width:0%");
        const hint = r.key === "jobs" ? "" : fmtHoursHint(total);
        const over = limit !== null && limit !== undefined && total >= limit;
        return `
          <div class="budget-item" title="${esc(hint)}">
            <div class="budget-item-top tabular">
              <span>${esc(i18n.t("limName_" + r.limit))}</span>
              <span class="${over ? "budget-over" : ""}">${fmtNum(total)} / ${esc(fmtLimit(limit))}</span>
            </div>
            <div class="progress-track">
              <div class="progress-seg-done" style="${seg(done, pDone)}"></div>
              <div class="progress-seg-active" style="${seg(active, pAct)}"></div>
              <div class="progress-seg-wait" style="${seg(wait, pWait)}"></div>
            </div>
            <div class="muted text-xs tabular" style="margin-top:6px">
              ${esc(i18n.t("budgetMeasured"))} ${fmtNum(done)} &middot;
              ${esc(i18n.t("budgetActive"))} ${fmtNum(active)} &middot;
              ${esc(i18n.t("budgetWaiting"))} ${fmtNum(wait)}
            </div>
          </div>
        `;
      }).join("")}
    </div>
  `;
}

function budgetGrantedLine(st) {
  if (!st) return "";
  return st.approval
    ? i18n.t("standingGrantedVia", { time: fmtTime(st.created), id: st.approval })
    : i18n.t("standingGrantedAt", { time: fmtTime(st.created) });
}

function fmtLimitList(lims) {
  return LIMIT_FIELDS.map((f) => `${i18n.t("limName_" + f.key)} ${fmtLimit(lims?.[f.key])}`).join(" · ");
}

function fmtUsageList(used) {
  return USAGE_ROWS.map((r) => `${i18n.t(r.label)} ${fmtNum(used?.[r.key] || 0)}`).join(" · ");
}

function renderBudgetHistory(history) {
  if (!history || !history.length) return "";
  return `
    <details class="budget-history">
      <summary>${esc(i18n.t("budgetHistory", { n: history.length }))}</summary>
      <ul>
        ${history.map((ev) => {
          let detail = "";
          if (ev.action === "granted") detail = fmtLimitList(ev.limits);
          if (ev.action === "adjusted") {
            detail = LIMIT_FIELDS
              .filter((f) => (ev.previous?.[f.key] ?? null) !== (ev.limits?.[f.key] ?? null))
              .map((f) => `${i18n.t("limName_" + f.key)} ${fmtLimit(ev.previous?.[f.key])} → ${fmtLimit(ev.limits?.[f.key])}`)
              .join(" · ");
          }
          if (ev.action === "revoked" && ev.used) {
            detail = i18n.t("budgetUsedAtRevoke", { usage: fmtUsageList(ev.used) });
          }
          const origin = i18n.t("budgetOrigin_" + ev.origin, { id: ev.approval ?? "" });
          return `
            <li>
              <div class="flex-row" style="gap:8px">
                <strong class="text-sm">${esc(i18n.t("budgetEvent_" + ev.action))}</strong>
                <span class="muted text-xs">${esc(origin)}</span>
                <span class="muted text-xs tabular" style="margin-left:auto">${esc(fmtTime(ev.ts))}</span>
              </div>
              ${detail ? `<div class="muted text-xs tabular">${esc(detail)}</div>` : ""}
            </li>
          `;
        }).join("")}
      </ul>
    </details>
  `;
}

// Jobs page: the selected project's budget card, or a table of all projects.
function renderStandingCard(standing) {
  if (!state.project) return renderBudgetOverview();
  if (!standing || !projectUsesSlurm(state.project)) return "";
  const mode = budgetMode(standing);
  const st = standing.standing;
  let body = "";
  if (mode === "off") {
    body = `<p class="muted text-sm" style="margin:0">${esc(i18n.t("standingAutoDesc"))}</p>`;
  } else if (mode === "none") {
    body = `<p class="muted text-sm" style="margin:0">${esc(i18n.t("standingNone"))}</p>`;
  } else {
    body = `
      <div class="muted text-xs tabular">
        ${esc(i18n.t("limGroupPerSubmit"))}:
        ${esc(i18n.t("limField_core_hours"))} ${esc(fmtLimit(st.limits.core_hours))} &middot;
        ${esc(i18n.t("limField_gpu_hours"))} ${esc(fmtLimit(st.limits.gpu_hours))}
      </div>
      ${renderBudgetBars(standing)}
    `;
  }
  return `
    <div class="card">
      <div class="card-header">
        <div>
          <div class="flex-row" style="gap:8px">
            <h2 class="card-title">${esc(i18n.t("standingTitle"))}</h2>
            ${budgetModePill(mode)}
          </div>
          ${mode === "active" ? `<div class="muted text-xs" style="margin-top:2px">${esc(budgetGrantedLine(st))}</div>` : ""}
        </div>
        ${budgetActionButton(state.project, mode)}
      </div>
      ${body}
    </div>
  `;
}

function renderBudgetOverview() {
  const names = (state.snapshot?.projects || []).map((p) => p.name).filter(projectUsesSlurm);
  if (!names.length) return "";
  return `
    <div class="card">
      <div class="card-header">
        <div>
          <h2 class="card-title">${esc(i18n.t("standingTitle"))}</h2>
          <div class="muted text-xs" style="margin-top:2px">${esc(i18n.t("budgetOverviewDesc"))}</div>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>${esc(i18n.t("colProject"))}</th>
              <th>${esc(i18n.t("colStatus"))}</th>
              ${USAGE_ROWS.map((r) => `<th>${esc(i18n.t("limName_" + r.limit))}</th>`).join("")}
              <th></th>
            </tr>
          </thead>
          <tbody>
            ${names.map((name) => {
              const b = budgetOf(name);
              const mode = budgetMode(b);
              const lims = b?.standing?.limits || {};
              return `
                <tr>
                  <td><strong>${esc(name)}</strong></td>
                  <td>${budgetModePill(mode)}</td>
                  ${USAGE_ROWS.map((r) => {
                    if (mode !== "active") return `<td class="muted">—</td>`;
                    const used = b.used?.[r.key] || 0;
                    const limit = lims[r.limit];
                    const over = limit !== null && limit !== undefined && used >= limit;
                    return `<td class="tabular ${over ? "budget-over" : ""}">${fmtNum(used)} / ${esc(fmtLimit(limit))}</td>`;
                  }).join("")}
                  <td style="text-align:right">${budgetActionButton(name, mode, "btn-xs")}</td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

// Settings page: one-line budget summary inside each project card.
function renderProjectBudgetRow(project) {
  if (!projectUsesSlurm(project)) return "";
  const b = budgetOf(project);
  if (!b) return "";
  const mode = budgetMode(b);
  const text = mode === "active"
    ? `<span class="text-sm tabular">${budgetUsageLine(b)}</span>`
    : `<span class="muted text-sm">${esc(i18n.t(mode === "off" ? "budgetSummaryOff" : "budgetSummaryNone"))}</span>`;
  return `
    <div style="margin-bottom:14px">
      <div class="text-xs muted" style="font-weight:600;margin-bottom:6px">${esc(i18n.t("standingTitle"))}</div>
      <div class="budget-summary">
        ${budgetModePill(mode)}
        ${text}
        <span style="margin-left:auto">${budgetActionButton(project, mode, "btn-xs")}</span>
      </div>
    </div>
  `;
}

// Approvals page: the active budget's usage as context for a waiting submission.
// Without a budget the "no standing budget" reason chip already says so.
function renderBudgetContext(b) {
  if (budgetMode(b) !== "active") return "";
  return `
    <div class="budget-context">
      <span class="muted">${esc(i18n.t("budgetContextLabel"))}</span>
      ${budgetUsageLine(b)}
    </div>
  `;
}

// The single place to grant, adjust, or revoke a project's standing budget.
async function openBudgetModal(project) {
  const url = `/api/standing/${encodeURIComponent(project)}`;
  const title = i18n.t("budgetModalTitle", { project });
  const backdrop = document.getElementById("modal-backdrop");
  const showing = (element) => element.isConnected && !backdrop.classList.contains("hidden");
  openModal(title, `<p id="budget-loading" role="status">${esc(i18n.t("budgetLoading"))}</p>`);
  const loading = document.getElementById("budget-loading");

  async function reload(notice = "") {
    const b = await api(url);
    render(b, notice);
  }

  function renderRevokeConfirm(b) {
    openModal(title, `
      <form id="budget-revoke-form">
        <p>${esc(i18n.t("revokeConfirmBody", { project }))}</p>
        <p class="muted text-sm">${esc(i18n.t("revokeConfirmNote"))}</p>
        <div class="modal-footer">
          <button type="button" id="budget-revoke-back" class="btn btn-outline btn-sm">${esc(i18n.t("backBtn"))}</button>
          <button type="submit" class="btn btn-danger btn-sm">${esc(i18n.t("revokeConfirmBtn"))}</button>
        </div>
      </form>
    `);
    const form = document.getElementById("budget-revoke-form");
    const back = document.getElementById("budget-revoke-back");
    back.onclick = () => render(b);
    back.focus();
    form.onsubmit = async (event) => {
      event.preventDefault();
      const btn = form.querySelector('[type="submit"]');
      btn.disabled = true;
      try {
        await api(url, { method: "DELETE" });
        if (showing(form)) closeModal();
        toast(i18n.t("revokedToast", { project }));
        await refreshAll();
      } catch (err) {
        toast(err.message, true);
        btn.disabled = false;
      }
    };
  }

  function render(b, notice = "") {
    const st = b.standing;
    const mode = st ? "adjust" : "grant";
    const values = st ? st.limits : b.defaults;
    openModal(title, `
      <form id="budget-form" novalidate>
        ${notice ? `<p class="alert-banner" role="status">${esc(notice)}</p>` : ""}
        ${!b.approval ? `<p class="alert-banner">${esc(i18n.t("standingAutoDesc"))}</p>` : ""}
        ${st ? `<div class="muted text-xs" style="margin-bottom:6px">${esc(budgetGrantedLine(st))}</div>` : ""}
        <p class="text-sm" style="margin-top:0">${esc(i18n.t(mode === "adjust" ? "budgetAdjustIntro" : "budgetGrantIntro"))}</p>
        ${mode === "grant" && b.pending ? `<p class="text-sm" style="margin-top:0">${esc(i18n.t("budgetGrantPending", { n: b.pending }))}</p>` : ""}
        <div class="muted text-xs">${esc(i18n.t("budgetBlankHint"))}</div>
        ${renderLimitInputs(values, { used: st ? b.used : null })}
        ${renderBudgetHistory(b.history)}
        <div class="modal-footer">
          ${st ? `<button type="button" id="budget-revoke" class="btn btn-danger btn-sm footer-left">${esc(i18n.t("revokeStanding"))}</button>` : ""}
          <button type="button" id="budget-cancel" class="btn btn-outline btn-sm">${esc(i18n.t("cancelBtn"))}</button>
          <button type="submit" class="btn btn-primary btn-sm">${esc(i18n.t(mode === "adjust" ? "budgetSaveAdjust" : "budgetSaveGrant"))}</button>
        </div>
      </form>
    `);
    const form = document.getElementById("budget-form");
    const save = form.querySelector('[type="submit"]');
    document.getElementById("budget-cancel").onclick = closeModal;
    const revoke = document.getElementById("budget-revoke");
    if (revoke) revoke.onclick = () => renderRevokeConfirm(b);
    // Adjusting is only meaningful once something differs from the current limits.
    const syncSave = () => {
      if (mode !== "adjust") return;
      const now = readLimitInputs(form, { report: false });
      save.disabled = now !== null && sameLimits(now, st.limits);
    };
    bindLimitInputs(form, syncSave);
    syncSave();
    form.querySelector("input[name]")?.focus();
    form.onsubmit = async (event) => {
      event.preventDefault();
      const limits = readLimitInputs(form);
      if (!limits) return;
      save.disabled = true;
      try {
        const r = await api(url, { method: "PUT", body: { mode, limits } });
        if (showing(form)) closeModal();
        let msg = i18n.t(mode === "adjust" ? "budgetAdjustedToast" : "budgetGrantedToast", { project });
        if (r.released && r.released.length) msg += i18n.t("budgetReleasedToast", { n: r.released.length });
        toast(msg);
        await refreshAll();
      } catch (err) {
        if (["standing_exists", "standing_missing"].includes(err.data?.code) && showing(form)) {
          try {
            await reload(i18n.t("budgetStale"));
          } catch (e) {
            toast(e.message, true);
          }
        } else {
          toast(err.message, true);
          save.disabled = false;
        }
      }
    };
  }

  try {
    const b = await api(url);
    if (showing(loading)) render(b);
  } catch (err) {
    if (showing(loading)) {
      closeModal();
      toast(err.message, true);
    }
  }
}

function renderJobs() {
  const el = document.getElementById("view-jobs");
  const data = state.jobsData || { batches: [], ops: [], unregistered: [], usage: {}, standing: null };
  const health = (state.snapshot && state.snapshot.health) || {};
  const batches = data.batches || [];
  const ops = data.ops || [];
  const unreg = data.unregistered || [];
  const usage = data.usage || {};

  const activeBatches = batches.filter((b) => !b.finished).length;
  const activeJobs = batches.reduce((acc, b) => acc + (b.jobs.running || 0) + (b.jobs.queued || 0), 0);
  const runningOps = ops.filter((o) => o.state === "running").length;
  const serverNames = Object.keys(health);
  const onlineServers = serverNames.filter((n) => health[n].reachable).length;

  const filteredBatches = batches.filter((b) => {
    if (state.batchFilterStatus === "active") return !b.finished;
    if (state.batchFilterStatus === "finished") return b.finished;
    if (state.batchFilterStatus === "failed") {
      return (b.jobs.failed || 0) + (b.jobs.timeout || 0) + (b.jobs.lost || 0) + (b.jobs.submit_failed || 0) > 0;
    }
    return true;
  });

  let html = `
    <div class="page-header">
      <div>
        <h1>${esc(i18n.t("overviewTitle"))}</h1>
        <p>${esc(i18n.t("overviewSubtitle"))}</p>
      </div>
    </div>

    <div class="stats-grid">
      <div class="stat-card"><div class="stat-label">${esc(i18n.t("statActiveBatches"))}</div><div class="stat-value">${activeBatches}</div></div>
      <div class="stat-card"><div class="stat-label">${esc(i18n.t("statRunningJobs"))}</div><div class="stat-value">${activeJobs}</div></div>
      <div class="stat-card"><div class="stat-label">${esc(i18n.t("statBgOps"))}</div><div class="stat-value">${runningOps}</div></div>
      <div class="stat-card"><div class="stat-label">${esc(i18n.t("statServers"))}</div><div class="stat-value">${onlineServers} / ${serverNames.length}</div></div>
    </div>
  `;

  if (unreg.length) {
    html += `
      <div class="alert-banner">
        <strong>${esc(i18n.t("unregBannerTitle"))}</strong>
        <div class="text-sm" style="margin-top:4px">${esc(i18n.t("unregBannerDesc"))}</div>
        <div class="mono text-xs" style="margin-top:6px">
          ${unreg.map((u) => `${esc(u.cluster)}:${esc(u.slurm_id)} (${esc(u.workdir || "-")})`).join("  |  ")}
        </div>
      </div>
    `;
  }

  html += renderStandingCard(data.standing);

  if (serverNames.length) {
    html += `
      <div class="card">
        <div class="card-header">
          <h2 class="card-title">${esc(i18n.t("serversHealthTitle"))}</h2>
        </div>
        <div class="servers-grid">
          ${serverNames.map((name) => {
            const h = health[name];
            const u = usage[name];
            const pct = u && u.max_jobs ? Math.min(100, Math.round((u.in_slurm / u.max_jobs) * 100)) : 0;
            return `
              <div class="server-card">
                <div class="server-card-top">
                  <div class="flex-row" style="gap:7px">
                    <span class="status-dot ${h.reachable ? "green" : "red"}"></span>
                    <strong class="mono">${esc(name)}</strong>
                  </div>
                  <span class="pill ${h.scheduler === "slurm" ? "pill-blue" : "pill-gray"}">
                    ${esc(h.scheduler === "slurm" ? i18n.t("schedulerSlurm") : i18n.t("schedulerNone"))}
                  </span>
                </div>
                <div class="muted text-xs mono">${esc(h.connect === "local" ? "local" : (h.host || "ssh"))}</div>
                <div class="muted text-xs" style="margin-top:4px">
                  ${esc(h.reachable ? i18n.t("reachable") : i18n.t("unreachable"))}
                  ${h.scheduler === "slurm" ? ` &middot; ${esc(i18n.t("lastSync"))}: ${esc(fmtRel(h.last_sync))}` : ""}
                </div>
                ${u && h.scheduler === "slurm" ? `
                  <div style="margin-top:10px">
                    <div class="flex-between text-xs tabular">
                      <span>${esc(i18n.t("slotsLabel"))}</span>
                      <span>${u.in_slurm} / ${u.max_jobs}</span>
                    </div>
                    <div class="progress-track" style="margin-top:5px">
                      <div class="progress-seg-active" style="width:${pct}%"></div>
                    </div>
                    <div class="muted text-xs tabular" style="margin-top:4px">
                      ${u.held} ${esc(i18n.t("heldLabel"))} &middot; ${u.unapproved} ${esc(i18n.t("unapprovedLabel"))}
                    </div>
                  </div>
                ` : ""}
              </div>
            `;
          }).join("")}
        </div>
      </div>
    `;
  }

  // Slurm Batches Card
  html += `
    <div class="card">
      <div class="card-header">
        <h2 class="card-title">${esc(i18n.t("batchesTitle"))}</h2>
        <div class="filter-bar">
          <select id="batch-cluster-filter" class="select-compact">
            <option value="">${esc(i18n.t("allClusters"))}</option>
            ${serverNames.map((n) => `<option value="${esc(n)}"${n === state.batchFilterCluster ? " selected" : ""}>${esc(n)}</option>`).join("")}
          </select>
          ${["all", "active", "finished", "failed"].map((st) => `
            <button type="button" class="btn btn-xs ${state.batchFilterStatus === st ? "btn-primary" : "btn-subtle"}" data-batch-filter="${st}">
              ${esc(i18n.t("filter" + st[0].toUpperCase() + st.slice(1)))}
            </button>
          `).join("")}
        </div>
      </div>
      ${!filteredBatches.length ? `<p class="muted text-sm">${esc(i18n.t("noBatches"))}</p>` : `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>${esc(i18n.t("colBatch"))}</th>
                <th>${esc(i18n.t("colProject"))}</th>
                <th>${esc(i18n.t("colServer"))}</th>
                <th>${esc(i18n.t("colProgress"))}</th>
                <th>${esc(i18n.t("colFailRate"))}</th>
                <th>${esc(i18n.t("colUpdated"))}</th>
                <th>${esc(i18n.t("colActions"))}</th>
              </tr>
            </thead>
            <tbody>
              ${filteredBatches.map((b) => {
                const key = `${b.project}::${b.batch}`;
                const rows = state.expandedBatches[key];
                const c = b.jobs || {};
                const tot = Math.max(b.n_jobs || 1, 1);
                const pDone = ((c.done || 0) / tot) * 100;
                const pAct = (((c.running || 0) + (c.queued || 0)) / tot) * 100;
                const pWait = (((c.held || 0) + (c.unapproved || 0)) / tot) * 100;
                const pFail = (((c.failed || 0) + (c.timeout || 0) + (c.lost || 0) + (c.submit_failed || 0)) / tot) * 100;
                const pCancel = ((c.cancelled || 0) / tot) * 100;
                const summaryChips = Object.entries(c).map(([k, v]) => `${k}:${v}`).join(" ");
                return `
                  <tr>
                    <td>
                      <div class="flex-row" style="gap:6px">
                        <strong class="mono">${esc(b.batch)}</strong>
                        ${b.tag ? `<span class="pill pill-gray">${esc(b.tag)}</span>` : ""}
                      </div>
                      <div class="muted text-xs">${esc(b.desc || "")}</div>
                    </td>
                    <td>${esc(b.project)}</td>
                    <td class="mono">${esc(b.cluster)}</td>
                    <td style="min-width:190px">
                      <div class="progress-track">
                        <div class="progress-seg-done" style="width:${pDone}%"></div>
                        <div class="progress-seg-active" style="width:${pAct}%"></div>
                        <div class="progress-seg-wait" style="width:${pWait}%"></div>
                        <div class="progress-seg-fail" style="width:${pFail}%"></div>
                        <div class="progress-seg-cancel" style="width:${pCancel}%"></div>
                      </div>
                      <div class="muted text-xs tabular" style="margin-top:4px">${esc(summaryChips)}</div>
                    </td>
                    <td class="tabular">${fmtFailRate(b.fail_rate)}</td>
                    <td class="muted text-xs tabular">${esc(fmtTime(b.changed))}</td>
                    <td>
                      <div class="flex-row" style="gap:6px">
                        <button type="button" class="btn btn-subtle btn-xs" data-expand-batch="${esc(key)}">${esc(i18n.t("inspectJobs"))} (${b.n_jobs})</button>
                        ${!b.finished ? `<button type="button" class="btn btn-danger btn-xs" data-cancel-batch="${esc(key)}">${esc(i18n.t("cancelBatch"))}</button>` : ""}
                      </div>
                    </td>
                  </tr>
                  ${rows ? `
                    <tr class="subrow">
                      <td colspan="7">
                        <table>
                          <thead>
                            <tr>
                              <th>${esc(i18n.t("colJobId"))}</th>
                              <th>${esc(i18n.t("colSlurmId"))}</th>
                              <th>${esc(i18n.t("colScript"))}</th>
                              <th>${esc(i18n.t("colState"))}</th>
                              <th>${esc(i18n.t("colResources"))}</th>
                              <th>${esc(i18n.t("colElapsed"))}</th>
                              <th>${esc(i18n.t("colActions"))}</th>
                            </tr>
                          </thead>
                          <tbody>
                            ${rows.map((j) => {
                              const liveRun = j.run_s != null
                                ? j.run_s
                                : (j.state === "running" && j.t_start ? Math.max(0, Date.now() / 1000 - j.t_start) : null);
                              return `
                              <tr>
                                <td class="mono muted">#${j.id}</td>
                                <td class="mono">${esc(j.slurm_id || "-")}</td>
                                <td class="mono">${esc(j.script)}</td>
                                <td>${renderStatePill(j.state)}${j.slurm_state ? ` <span class="muted text-xs">${esc(j.slurm_state)}</span>` : ""}</td>
                                <td class="tabular text-xs">${esc(j.cores ?? "-")}c / ${esc(j.gpus ?? "-")}g</td>
                                <td class="tabular text-xs muted">${esc(fmtDur(liveRun))}</td>
                                <td>
                                  <div class="flex-row" style="gap:6px">
                                    <button type="button" class="btn btn-subtle btn-xs" data-inspect-job="${j.id}">${esc(i18n.t("inspectFiles"))}</button>
                                    ${["unapproved", "held", "queued", "running"].includes(j.state) ? `<button type="button" class="btn btn-danger btn-xs" data-cancel-job="${esc(j.slurm_id || ("#" + j.id))}" data-job-project="${esc(j.project)}" data-job-cluster="${esc(j.cluster)}">${esc(i18n.t("cancelJob"))}</button>` : ""}
                                  </div>
                                </td>
                              </tr>
                            `;
                            }).join("")}
                          </tbody>
                        </table>
                      </td>
                    </tr>
                  ` : ""}
                `;
              }).join("")}
            </tbody>
          </table>
        </div>
      `}
    </div>
  `;

  // Background Ops Card
  html += `
    <div class="card">
      <div class="card-header">
        <h2 class="card-title">${esc(i18n.t("opsTitle"))}</h2>
      </div>
      ${!ops.length ? `<p class="muted text-sm">${esc(i18n.t("noOps"))}</p>` : `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>${esc(i18n.t("colOp"))}</th>
                <th>${esc(i18n.t("colProject"))}</th>
                <th>${esc(i18n.t("colServer"))}</th>
                <th>${esc(i18n.t("colKind"))}</th>
                <th>${esc(i18n.t("colCommand"))}</th>
                <th>${esc(i18n.t("colElapsed"))}</th>
                <th>${esc(i18n.t("colStatus"))}</th>
                <th>${esc(i18n.t("colActions"))}</th>
              </tr>
            </thead>
            <tbody>
              ${ops.map((o) => {
                const sp = o.spec || {};
                const cmdText = o.kind === "exec" ? (sp.command || "") : `${sp.src || ""} -> ${sp.dst || ""}`;
                const isExpanded = state.expandedOps.has(o.id);
                const elapsed = o.result && o.result.elapsed != null
                  ? o.result.elapsed
                  : (o.state === "running" && o.started ? Math.max(0, Date.now() / 1000 - o.started)
                    : (o.ended && o.started ? Math.max(0, o.ended - o.started) : null));
                return `
                  <tr>
                    <td class="mono"><strong>op:${o.id}</strong></td>
                    <td>${esc(o.project)}</td>
                    <td class="mono">${esc(o.cluster || "-")}</td>
                    <td><span class="pill pill-gray">${esc(o.kind)}</span></td>
                    <td>
                      <div class="cmd-expandable mono${isExpanded ? " expanded" : ""}" data-toggle-op-cmd="${o.id}" title="${esc(cmdText)}">${esc(cmdText)}</div>
                    </td>
                    <td class="tabular text-xs muted">${esc(fmtDur(elapsed))}</td>
                    <td>${renderStatePill(o.state)}</td>
                    <td>
                      <div class="flex-row" style="gap:6px">
                        ${o.kind === "exec" ? `<button type="button" class="btn btn-subtle btn-xs" data-inspect-op="${o.id}">${esc(i18n.t("viewOutput"))}</button>` : ""}
                        ${o.state === "running" ? `<button type="button" class="btn btn-danger btn-xs" data-stop-op="${o.id}">${esc(i18n.t("stopOp"))}</button>` : ""}
                      </div>
                    </td>
                  </tr>
                `;
              }).join("")}
            </tbody>
          </table>
        </div>
      `}
    </div>
  `;

  el.innerHTML = html;

  bindBudgetButtons(el);

  const cf = document.getElementById("batch-cluster-filter");
  if (cf) {
    cf.onchange = () => {
      state.batchFilterCluster = cf.value;
      refreshActiveTab();
    };
  }

  el.querySelectorAll("[data-batch-filter]").forEach((btn) => {
    btn.onclick = () => {
      state.batchFilterStatus = btn.dataset.batchFilter;
      renderJobs();
    };
  });

  el.querySelectorAll("[data-expand-batch]").forEach((btn) => {
    btn.onclick = async () => {
      const key = btn.dataset.expandBatch;
      if (state.expandedBatches[key]) {
        delete state.expandedBatches[key];
        renderJobs();
        return;
      }
      const [proj, batch] = key.split("::");
      try {
        const res = await api(`/api/jobs/${encodeURIComponent(proj)}/${encodeURIComponent(batch)}`);
        state.expandedBatches[key] = res.jobs || [];
        renderJobs();
      } catch (e) {
        toast(e.message, true);
      }
    };
  });

  el.querySelectorAll("[data-cancel-batch]").forEach((btn) => {
    btn.onclick = () => {
      const [proj, batch] = btn.dataset.cancelBatch.split("::");
      openCancelModal({ project: proj, batch });
    };
  });

  el.querySelectorAll("[data-cancel-job]").forEach((btn) => {
    btn.onclick = () => {
      openCancelModal({
        project: btn.dataset.jobProject,
        cluster: btn.dataset.jobCluster || undefined,
        job_ids: [String(btn.dataset.cancelJob)],
      });
    };
  });

  el.querySelectorAll("[data-inspect-job]").forEach((btn) => {
    btn.onclick = () => openJobDrawer(Number(btn.dataset.inspectJob));
  });

  el.querySelectorAll("[data-toggle-op-cmd]").forEach((node) => {
    const id = Number(node.dataset.toggleOpCmd);
    const isExpanded = state.expandedOps.has(id);
    const hasNewline = (node.textContent || "").includes("\n");
    const isTruncated = node.scrollWidth > node.clientWidth + 1;
    if (isExpanded || isTruncated || hasNewline) {
      node.classList.add("is-expandable");
      node.setAttribute("role", "button");
      node.setAttribute("tabindex", "0");
      node.setAttribute("aria-expanded", isExpanded ? "true" : "false");
      const toggle = () => {
        if (state.expandedOps.has(id)) state.expandedOps.delete(id);
        else state.expandedOps.add(id);
        renderJobs();
      };
      node.onclick = toggle;
      node.onkeydown = (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          toggle();
        }
      };
    }
  });

  el.querySelectorAll("[data-inspect-op]").forEach((btn) => {
    btn.onclick = () => openOpDrawer(Number(btn.dataset.inspectOp));
  });

  el.querySelectorAll("[data-stop-op]").forEach((btn) => {
    btn.onclick = async () => {
      const id = Number(btn.dataset.stopOp);
      btn.disabled = true;
      try {
        await api(`/api/ops/${id}/stop`, { method: "POST" });
        toast(i18n.t("stoppedToast", { id }));
        await refreshAll();
      } catch (e) {
        toast(e.message, true);
        btn.disabled = false;
      }
    };
  });
}

function openCancelModal(target) {
  openModal(i18n.t("cancelPromptTitle"), `
    <p class="muted text-sm" style="margin-top:0">${esc(i18n.t("cancelPromptDesc"))}</p>
    <input type="text" id="cancel-comment-input" style="width:100%" placeholder="${esc(i18n.t("commentPlaceholder"))}" />
    <div class="modal-footer">
      <button type="button" class="btn btn-outline btn-sm" id="modal-cancel-btn">${esc(i18n.t("cancelBtn"))}</button>
      <button type="button" class="btn btn-danger btn-sm" id="modal-confirm-cancel">${esc(i18n.t("cancelPromptConfirm"))}</button>
    </div>
  `);
  document.getElementById("modal-cancel-btn").onclick = closeModal;
  document.getElementById("modal-confirm-cancel").onclick = async () => {
    const comment = document.getElementById("cancel-comment-input").value.trim();
    try {
      await api("/api/cancel", { method: "POST", body: Object.assign({}, target, { comment }) });
      closeModal();
      toast(i18n.t("cancelledToast"));
      await refreshAll();
    } catch (e) {
      toast(e.message, true);
    }
  };
}

// ------------------------------------------------------------------ DRAWER (Job Files / Op Output)
async function openJobDrawer(jobId) {
  state.drawer = { type: "job", id: jobId, activeTab: "script", data: null };
  showDrawer(i18n.t("drawerJobTitle", { id: jobId }), "Loading...");
  await loadDrawerData();
}

async function openOpDrawer(opId) {
  state.drawer = { type: "op", id: opId, activeTab: "output", data: null };
  showDrawer(i18n.t("drawerOpTitle", { id: opId }), "Loading...");
  await loadDrawerData();
}

function showDrawer(title, subtitle) {
  document.getElementById("drawer-title").textContent = title;
  document.getElementById("drawer-subtitle").textContent = subtitle || "";
  document.getElementById("drawer-backdrop").classList.remove("hidden");
  document.getElementById("drawer").classList.remove("hidden");
  document.getElementById("drawer").setAttribute("aria-hidden", "false");
}

function closeDrawer() {
  state.drawer = null;
  document.getElementById("drawer-backdrop").classList.add("hidden");
  document.getElementById("drawer").classList.add("hidden");
  document.getElementById("drawer").setAttribute("aria-hidden", "true");
}

async function loadDrawerData() {
  if (!state.drawer) return;
  try {
    if (state.drawer.type === "job") {
      const r = await api(`/api/job/${state.drawer.id}/files`);
      state.drawer.data = r;
      const j = r.job || {};
      document.getElementById("drawer-subtitle").textContent =
        `${j.project || ""} / ${j.cluster || ""} / ${j.script || ""} (Slurm ${j.slurm_id || "-"})`;
    } else {
      const r = await api(`/api/ops/${state.drawer.id}/output`);
      state.drawer.data = r;
      const o = r.op || {};
      document.getElementById("drawer-subtitle").textContent =
        `${o.project || ""} / ${o.cluster || ""} (${o.state || ""})`;
    }
    renderDrawerContent();
  } catch (e) {
    document.getElementById("drawer-body").innerHTML = `<div class="inline-error">${esc(e.message)}</div>`;
  }
}

function renderDrawerContent() {
  if (!state.drawer || !state.drawer.data) return;
  const tabsEl = document.getElementById("drawer-tabs");
  const bodyEl = document.getElementById("drawer-body");
  if (state.drawer.type === "job") {
    const tabs = [
      { key: "script", label: i18n.t("tabScript") },
      { key: "stdout", label: i18n.t("tabStdout") },
      { key: "stderr", label: i18n.t("tabStderr") },
    ];
    tabsEl.innerHTML = tabs.map((t) => `
      <button type="button" class="snippet-tab ${state.drawer.activeTab === t.key ? "active" : ""}" data-dtab="${t.key}">
        ${esc(t.label)}
      </button>
    `).join("");
    tabsEl.querySelectorAll("[data-dtab]").forEach((b) => {
      b.onclick = () => {
        state.drawer.activeTab = b.dataset.dtab;
        renderDrawerContent();
      };
    });
    const text = state.drawer.data[state.drawer.activeTab] || "";
    bodyEl.innerHTML = `<pre>${esc(text || i18n.t("emptyOutput"))}</pre>`;
  } else {
    tabsEl.innerHTML = `<span class="pill pill-blue">${esc(i18n.t("tabOutput"))}</span>`;
    const text = state.drawer.data.output || "";
    bodyEl.innerHTML = `<pre>${esc(text || i18n.t("emptyOutput"))}</pre>`;
  }
}

// ------------------------------------------------------------------ TAB 3: AUDIT
function renderAudit() {
  const el = document.getElementById("view-audit");
  const rows = state.auditRows || [];
  const tools = ["info", "submit", "remote_exec", "transfer", "remote_stop", "jobs", "cancel", "wait_for", "propose_preset", "open_dashboard"];
  el.innerHTML = `
    <div class="page-header">
      <div>
        <h1>${esc(i18n.t("auditTitle"))}</h1>
        <p>${esc(i18n.t("auditSubtitle"))}</p>
      </div>
      <div class="filter-bar">
        <select id="audit-tool-filter" class="select-compact">
          <option value="">${esc(i18n.t("allTools"))}</option>
          ${tools.map((t) => `<option value="${t}"${t === state.auditTool ? " selected" : ""}>${t}</option>`).join("")}
        </select>
        <select id="audit-limit-filter" class="select-compact">
          ${[50, 200, 500].map((n) => `<option value="${n}"${n === state.auditLimit ? " selected" : ""}>${n}</option>`).join("")}
        </select>
      </div>
    </div>
    <div class="card">
      ${!rows.length ? `<p class="muted text-sm">${esc(i18n.t("noAudit"))}</p>` : `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>${esc(i18n.t("colTime"))}</th>
                <th>${esc(i18n.t("colProject"))}</th>
                <th>${esc(i18n.t("colTool"))}</th>
                <th>${esc(i18n.t("colResult"))}</th>
                <th>${esc(i18n.t("colArgs"))}</th>
              </tr>
            </thead>
            <tbody>
              ${rows.map((r) => {
                const res = String(r.result || "");
                const resCls = res === "ok" ? "pill-green" : res.startsWith("task:") ? "pill-blue" : res.startsWith("pending") ? "pill-amber" : "pill-red";
                return `
                  <tr>
                    <td class="muted text-xs tabular" style="white-space:nowrap">${esc(fmtTime(r.ts))}</td>
                    <td>${esc(r.project || "-")}</td>
                    <td><span class="pill pill-gray mono">${esc(r.tool)}</span></td>
                    <td><span class="pill ${resCls}">${esc(res)}</span></td>
                    <td class="mono text-xs" style="max-width:460px;word-break:break-all">${esc(r.args || "")}</td>
                  </tr>
                `;
              }).join("")}
            </tbody>
          </table>
        </div>
      `}
    </div>
  `;

  document.getElementById("audit-tool-filter").onchange = (e) => {
    state.auditTool = e.target.value;
    refreshActiveTab();
  };
  document.getElementById("audit-limit-filter").onchange = (e) => {
    state.auditLimit = Number(e.target.value);
    refreshActiveTab();
  };
}

// ------------------------------------------------------------------ TAB 4: SETTINGS
function renderSettings() {
  const el = document.getElementById("view-settings");
  const snap = state.snapshot || { config: {}, presets: [], projects: [] };
  const cfg = snap.config || {};
  const presets = snap.presets || [];
  const projects = snap.projects || [];

  el.innerHTML = `
    <div class="page-header">
      <div>
        <h1>${esc(i18n.t("settingsTitle"))}</h1>
        <p>${esc(i18n.t("settingsSubtitle"))}</p>
      </div>
    </div>

    <!-- Global Preferences -->
    <div class="card">
      <div class="card-header">
        <h2 class="card-title">${esc(i18n.t("globalPrefsTitle"))}</h2>
        <span class="pill pill-gray mono">http://127.0.0.1:${esc(cfg.port || 8765)}</span>
      </div>
      <div class="pref-row">
        <div>
          <div style="font-weight:580">${esc(i18n.t("prefAutoOpen"))}</div>
          <div class="muted text-xs">${esc(i18n.t("prefAutoOpenDesc"))}</div>
        </div>
        <label class="switch">
          <input type="checkbox" id="pref-auto-open"${cfg.auto_open_dashboard ? " checked" : ""} />
          <span class="slider"></span>
        </label>
      </div>
      <div class="pref-row">
        <div>
          <div style="font-weight:580">${esc(i18n.t("prefWebOnly"))}</div>
          <div class="muted text-xs">${esc(i18n.t("prefWebOnlyDesc"))}</div>
        </div>
        <label class="switch">
          <input type="checkbox" id="pref-web-only"${cfg.web_only_approval ? " checked" : ""} />
          <span class="slider"></span>
        </label>
      </div>
    </div>

    <!-- Compute Server Presets -->
    <div class="card">
      <div class="card-header">
        <div>
          <h2 class="card-title">${esc(i18n.t("presetsTitle"))}</h2>
          <div class="muted text-xs" style="margin-top:2px">${esc(i18n.t("presetsDesc"))}</div>
        </div>
        <button type="button" class="btn btn-primary btn-sm" id="add-preset-btn">+ ${esc(i18n.t("addPreset"))}</button>
      </div>
      ${!presets.length ? `<p class="muted text-sm">${esc(i18n.t("noPresets"))}</p>` : `
        <div class="settings-grid">
          ${presets.map((pr) => {
            const d = pr.data || {};
            const tr = state.testResults[pr.name];
            const inUse = (pr.attached_projects || []).length > 0;
            return `
              <div class="preset-card">
                <div class="flex-between">
                  <div class="flex-row" style="gap:7px">
                    <strong class="mono" style="font-size:14.5px">${esc(pr.name)}</strong>
                    <span class="pill ${d.scheduler === "slurm" ? "pill-blue" : "pill-gray"}">
                      ${esc(d.scheduler === "slurm" ? i18n.t("schedulerSlurm") : i18n.t("schedulerNone"))}
                    </span>
                  </div>
                  <span class="mono muted text-xs">v${esc(pr.version ?? 1)}</span>
                </div>
                <div class="text-xs muted mono">
                  ${esc(d.connect === "local" ? "local" : `ssh://${d.host || ""}`)}
                  ${d.projects_root ? `<br/>root: ${esc(d.projects_root)}` : ""}
                  ${d.scheduler === "slurm" ? `<br/>max_jobs: ${esc(d.max_jobs ?? 10)}${d.partitions && d.partitions.length ? " | " + esc(d.partitions.join(", ")) : ""}` : ""}
                </div>
                <div class="text-xs muted">
                  ${inUse ? `${esc(i18n.t("attachedTo"))}: ${pr.attached_projects.map((n) => `<span class="pill pill-gray" style="margin-left:3px">${esc(n)}</span>`).join("")}` : esc(i18n.t("unusedPreset"))}
                </div>
                ${tr ? `<div class="text-xs ${tr.ok ? "pill pill-green" : "pill pill-red"}" style="align-self:flex-start">${esc(tr.text)}</div>` : ""}
                <div class="flex-row" style="margin-top:auto;padding-top:6px">
                  <button type="button" class="btn btn-subtle btn-xs" data-test-preset="${esc(pr.name)}">${esc(i18n.t("testConn"))}</button>
                  <button type="button" class="btn btn-soft btn-xs" data-edit-preset="${esc(pr.name)}">${esc(i18n.t("edit"))}</button>
                  <button type="button" class="btn btn-danger btn-xs" data-del-preset="${esc(pr.name)}" ${inUse ? `disabled title="${esc(i18n.t("cannotDeleteInUse"))}"` : ""}>${esc(i18n.t("deleteBtn"))}</button>
                </div>
              </div>
            `;
          }).join("")}
        </div>
      `}
    </div>

    <!-- Projects & Agent MCP Setup -->
    <div style="margin-top:22px">
      <div class="flex-between" style="margin-bottom:10px">
        <div>
          <h2 class="card-title" style="font-size:17px">${esc(i18n.t("projectsTitle"))}</h2>
          <div class="muted text-xs">${esc(i18n.t("projectsDesc"))}</div>
        </div>
        <button type="button" class="btn btn-primary btn-sm" id="new-project-btn">+ ${esc(i18n.t("newProject"))}</button>
      </div>
      ${!projects.length ? `<div class="card"><p class="muted text-sm" style="margin:0">${esc(i18n.t("noProjects"))}</p></div>` :
        projects.map((p) => renderProjectCard(p)).join("")}
    </div>
  `;

  document.getElementById("pref-auto-open").onchange = async (e) => {
    try {
      await api("/api/config", { method: "PATCH", body: { auto_open_dashboard: e.target.checked } });
      toast(i18n.t("savedToast"));
      await refreshAll();
    } catch (err) {
      toast(err.message, true);
    }
  };

  document.getElementById("pref-web-only").onchange = async (e) => {
    try {
      await api("/api/config", { method: "PATCH", body: { web_only_approval: e.target.checked } });
      toast(i18n.t("savedToast"));
      await refreshAll();
    } catch (err) {
      toast(err.message, true);
    }
  };

  document.getElementById("add-preset-btn").onclick = () => openPresetModal(null);
  document.getElementById("new-project-btn").onclick = () => openNewProjectModal();

  el.querySelectorAll("[data-test-preset]").forEach((btn) => {
    btn.onclick = async () => {
      const name = btn.dataset.testPreset;
      btn.disabled = true;
      btn.textContent = i18n.t("testing");
      try {
        const r = await api(`/api/presets/${encodeURIComponent(name)}/test`, { method: "POST" });
        state.testResults[name] = {
          ok: true,
          text: i18n.t("testOk", { home: r.home || "-", slurm: r.has_slurm ? i18n.t("yes") : i18n.t("no") }),
        };
      } catch (e) {
        state.testResults[name] = { ok: false, text: e.message };
      }
      renderSettings();
    };
  });

  el.querySelectorAll("[data-edit-preset]").forEach((btn) => {
    btn.onclick = () => {
      const pr = presets.find((x) => x.name === btn.dataset.editPreset);
      if (pr) openPresetModal(pr);
    };
  });

  el.querySelectorAll("[data-del-preset]").forEach((btn) => {
    btn.onclick = async () => {
      const name = btn.dataset.delPreset;
      try {
        await api(`/api/presets/${encodeURIComponent(name)}`, { method: "DELETE" });
        toast(i18n.t("deletedToast", { name }));
        await refreshAll();
      } catch (e) {
        toast(e.message, true);
      }
    };
  });

  el.querySelectorAll("[data-attach-project]").forEach((btn) => {
    btn.onclick = () => openAttachModal(btn.dataset.attachProject);
  });

  bindBudgetButtons(el);

  el.querySelectorAll("[data-retire-project]").forEach((btn) => {
    btn.onclick = () => openProjectRetirementModal(btn.dataset.retireProject);
  });

  el.querySelectorAll("[data-detach-cluster]").forEach((btn) => {
    btn.onclick = async () => {
      const [proj, cluster] = btn.dataset.detachCluster.split("::");
      btn.disabled = true;
      try {
        await api(`/api/projects/${encodeURIComponent(proj)}/clusters/${encodeURIComponent(cluster)}`, {
          method: "DELETE",
        });
        toast(i18n.t("detachedToast", { cluster, project: proj }));
        await refreshAll();
      } catch (e) {
        toast(e.message, true);
        btn.disabled = false;
      }
    };
  });

  el.querySelectorAll("[data-snippet-tab]").forEach((btn) => {
    btn.onclick = () => {
      const [proj, agent] = btn.dataset.snippetTab.split("::");
      state.activeSnippetTab[proj] = agent;
      renderSettings();
    };
  });

  el.querySelectorAll("[data-copy-snippet]").forEach((btn) => {
    btn.onclick = async () => {
      const [proj, agent] = btn.dataset.copySnippet.split("::");
      const code = state.snippetsCache[proj]?.[agent]?.code || "";
      try {
        await navigator.clipboard.writeText(code);
        toast(i18n.t("copiedToast"));
      } catch {
        toast("Clipboard unavailable", true);
      }
    };
  });

  el.querySelectorAll("[data-policy-details]").forEach((det) => {
    det.ontoggle = () => {
      const proj = det.dataset.policyDetails;
      if (det.open) state.openPolicies.add(proj);
      else state.openPolicies.delete(proj);
    };
  });

  el.querySelectorAll("[data-policy-form]").forEach((form) => {
    form.onsubmit = async (e) => {
      e.preventDefault();
      const proj = form.dataset.policyForm;
      const fd = new FormData(form);
      const subWait = parseStrictNumber(fd.get("submit_wait_s"), { min: 0, max: 180 });
      const wakeWait = parseStrictNumber(fd.get("wake_wait_s"), { min: 0, max: 180 });
      const failFrac = parseStrictNumber(fd.get("wake_fail_frac"), { min: 0, max: 1 });
      const timeoutH = parseStrictNumber(fd.get("wake_timeout_h"), { min: 0.0001 });
      const transferGb = parseStrictNumber(fd.get("gw_transfer_gb"), { min: 0 });
      const maxBg = parseStrictNumber(fd.get("gw_max_bg"), { integer: true, min: 0 });
      const checks = [
        ["submit.wait_s", subWait],
        ["wake.default_wait_s", wakeWait],
        ["wake.fail_frac", failFrac],
        ["wake.timeout_h", timeoutH],
        ["gateway.transfer_max_gb", transferGb],
        ["gateway.max_background", maxBg],
      ];
      for (const [field, res] of checks) {
        if (!res.ok) {
          toast(i18n.t("invalidNumber", { field }), true);
          return;
        }
      }
      const defaultsBox = form.querySelector("[data-policy-defaults]");
      const defaultLimits = defaultsBox ? readLimitInputs(defaultsBox) : {};
      if (!defaultLimits) return;
      const changes = {
        local_dir: (fd.get("local_dir") || "").toString().trim() || null,
        submit: {
          approval: form.querySelector('input[name="submit_approval"]').checked,
          wait_s: subWait.value,
          ...defaultLimits,
        },
        wake: {
          default_wait_s: wakeWait.value,
          fail_frac: failFrac.value,
          timeout_h: timeoutH.value,
        },
        gateway: {
          transfer_max_gb: transferGb.value,
          max_background: maxBg.value,
        },
      };
      try {
        await api(`/api/projects/${encodeURIComponent(proj)}`, {
          method: "PATCH",
          body: { changes, note: "updated via web ui" },
        });
        toast(i18n.t("savedToast"));
        await refreshAll();
      } catch (err) {
        toast(err.message, true);
      }
    };
  });
}

function renderProjectCard(p) {
  const d = p.data || {};
  const clusters = d.clusters || {};
  const cnames = Object.keys(clusters);
  const snips = state.snippetsCache[p.name] || {};
  const agentKeys = Object.keys(snips);
  const activeAgent = state.activeSnippetTab[p.name] || agentKeys[0] || "codex";
  const activeSnip = snips[activeAgent] || {};
  const labels = {
    codex: "Codex",
    opencode: "OpenCode",
    antigravity: "Antigravity",
    dsh: "DSH",
    claude_code: "Claude Code",
  };

  return `
    <div class="project-card">
      <div class="flex-between" style="margin-bottom:12px">
        <div>
          <div class="flex-row" style="gap:8px">
            <strong style="font-size:16px">${esc(p.name)}</strong>
            <span class="mono muted text-xs">v${esc(p.version ?? 1)}</span>
          </div>
          <div class="muted text-xs mono" style="margin-top:2px">
            ${esc(i18n.t("localDirLabel"))}: ${esc(d.local_dir || i18n.t("noLocalDir"))}
          </div>
        </div>
        <div class="flex-row" style="gap:8px">
          <button type="button" class="btn btn-soft btn-sm" data-attach-project="${esc(p.name)}">+ ${esc(i18n.t("attachServer"))}</button>
          <button type="button" class="btn btn-danger btn-sm" data-retire-project="${esc(p.name)}">${esc(i18n.t("retireProject"))}</button>
        </div>
      </div>

      <div style="margin-bottom:14px">
        <div class="text-xs muted" style="font-weight:600;margin-bottom:6px">${esc(i18n.t("attachedServers"))}</div>
        ${!cnames.length ? `<div class="muted text-xs">${esc(i18n.t("noAttachedServers"))}</div>` : `
          <div class="flex-row" style="gap:8px">
            ${cnames.map((cn) => `
              <div class="metric-chip" style="min-width:auto">
                <div class="flex-between" style="gap:8px">
                  <span class="k">${esc(cn)}${clusters[cn].max_jobs ? ` (max ${clusters[cn].max_jobs})` : ""}</span>
                  <button type="button" class="btn btn-danger btn-xs" style="padding:1px 6px;font-size:11px" data-detach-cluster="${esc(p.name)}::${esc(cn)}">${esc(i18n.t("detachBtn"))}</button>
                </div>
                <span class="mono text-xs">${esc(clusters[cn].root)}</span>
              </div>
            `).join("")}
          </div>
        `}
      </div>

      ${renderProjectBudgetRow(p.name)}

      ${agentKeys.length ? `
        <div style="margin-top:14px;padding-top:14px;border-top:1px solid var(--border)">
          <div class="flex-between" style="margin-bottom:8px">
            <span class="text-xs muted" style="font-weight:600">${esc(i18n.t("mcpSetupTitle"))}</span>
            <button type="button" class="btn btn-subtle btn-xs" data-copy-snippet="${esc(p.name)}::${esc(activeAgent)}">
              ${esc(i18n.t("copyConfig"))}
            </button>
          </div>
          <div class="snippet-tabs">
            ${agentKeys.map((k) => `
              <button type="button" class="snippet-tab ${k === activeAgent ? "active" : ""}" data-snippet-tab="${esc(p.name)}::${esc(k)}">
                ${esc(labels[k] || k)}
              </button>
            `).join("")}
          </div>
          <div class="muted text-xs" style="margin-bottom:4px">${esc(activeSnip.title || "")}</div>
          <pre class="code-block" style="margin-top:0">${esc(activeSnip.code || "")}</pre>
          ${activeSnip.http_note ? `<div class="muted text-xs mono" style="margin-top:6px">${esc(activeSnip.http_note)}</div>` : ""}
        </div>
      ` : ""}

      <details data-policy-details="${esc(p.name)}"${state.openPolicies.has(p.name) ? " open" : ""} style="margin-top:14px;padding-top:12px;border-top:1px solid var(--border)">
        <summary class="text-sm" style="cursor:pointer;font-weight:580;color:var(--text-secondary)">
          ${esc(i18n.t("policiesTitle"))}
        </summary>
        <form data-policy-form="${esc(p.name)}" class="form-grid" style="margin-top:12px">
          <label class="field-label form-full">${esc(i18n.t("fieldLocalDir"))}
            <input type="text" name="local_dir" value="${esc(d.local_dir || "")}" placeholder="/Users/alice/my-project" />
          </label>
          <label class="checkbox-pill form-full" style="justify-content:space-between">
            <span>${esc(i18n.t("policyApproval"))}</span>
            <input type="checkbox" name="submit_approval"${(d.submit && d.submit.approval !== false) ? " checked" : ""} />
          </label>
          <label class="field-label">${esc(i18n.t("policySubmitWait"))}
            <input type="number" step="1" min="0" max="180" required name="submit_wait_s" value="${esc(d.submit?.wait_s ?? 180)}" />
          </label>
          <label class="field-label">${esc(i18n.t("policyWakeWait"))}
            <input type="number" step="1" min="0" max="180" required name="wake_wait_s" value="${esc(d.wake?.default_wait_s ?? 180)}" />
          </label>
          <label class="field-label">${esc(i18n.t("policyFailFrac"))}
            <input type="number" step="0.05" min="0" max="1" required name="wake_fail_frac" value="${esc(d.wake?.fail_frac ?? 0.1)}" />
          </label>
          <label class="field-label">${esc(i18n.t("policyTimeoutH"))}
            <input type="number" step="any" min="0.1" required name="wake_timeout_h" value="${esc(d.wake?.timeout_h ?? 48)}" />
          </label>
          <label class="field-label">${esc(i18n.t("policyTransferGb"))}
            <input type="number" step="0.5" min="0" required name="gw_transfer_gb" value="${esc(d.gateway?.transfer_max_gb ?? 5)}" />
          </label>
          <label class="field-label">${esc(i18n.t("policyMaxBg"))}
            <input type="number" step="1" min="0" required name="gw_max_bg" value="${esc(d.gateway?.max_background ?? 4)}" />
          </label>
          ${projectUsesSlurm(p.name) ? `
            <div class="form-full policy-defaults" data-policy-defaults>
              <div class="text-sm" style="font-weight:600">${esc(i18n.t("policyDefaultsTitle"))}</div>
              <div class="field-hint">${esc(i18n.t("policyDefaultsHint"))}</div>
              ${renderLimitInputs(d.submit || {})}
            </div>
          ` : ""}
          <div class="form-full">
            <button type="submit" class="btn btn-primary btn-sm">${esc(i18n.t("savePolicies"))}</button>
          </div>
        </form>
      </details>
    </div>
  `;
}

// ------------------------------------------------------------------ MODALS
function openModal(title, bodyHtml) {
  document.getElementById("modal-title").textContent = title;
  document.getElementById("modal-body").innerHTML = bodyHtml;
  document.getElementById("modal-backdrop").classList.remove("hidden");
}

function closeModal() {
  document.getElementById("modal-backdrop").classList.add("hidden");
}

function openPresetModal(presetObj) {
  const isEdit = Boolean(presetObj);
  const d = (presetObj && presetObj.data) || {
    connect: "ssh", scheduler: "slurm", max_jobs: 10, poll_interval_s: 120,
    exec_timeout_s: 3600, python: "python3", partitions: [], extra_paths: [], gateway_allow: [],
  };
  let subTab = "form";

  function renderBody() {
    const title = isEdit ? i18n.t("presetModalEdit", { name: presetObj.name }) : i18n.t("presetModalNew");
    const allowSet = new Set(d.gateway_allow || []);
    const tabBar = isEdit ? `
      <div class="snippet-tabs" style="margin-bottom:14px">
        <button type="button" class="snippet-tab ${subTab === "form" ? "active" : ""}" id="pm-tab-form">${esc(i18n.t("tabForm"))}</button>
        <button type="button" class="snippet-tab ${subTab === "yaml" ? "active" : ""}" id="pm-tab-yaml">${esc(i18n.t("tabYaml"))}</button>
      </div>
    ` : "";

    const formHtml = `
      <form id="preset-form" class="form-grid">
        ${!isEdit ? `
          <label class="field-label form-full">${esc(i18n.t("fieldName"))}
            <input type="text" name="name" required placeholder="gpu01" />
            <span class="field-hint">${esc(i18n.t("fieldNameHint"))}</span>
          </label>
        ` : ""}
        <label class="field-label">${esc(i18n.t("fieldScheduler"))}
          <select name="scheduler">
            <option value="slurm"${d.scheduler === "slurm" ? " selected" : ""}>${esc(i18n.t("optSlurm"))}</option>
            <option value="none"${d.scheduler === "none" ? " selected" : ""}>${esc(i18n.t("optNone"))}</option>
          </select>
        </label>
        <label class="field-label">${esc(i18n.t("fieldConnect"))}
          <select name="connect">
            <option value="ssh"${d.connect === "ssh" ? " selected" : ""}>${esc(i18n.t("optSsh"))}</option>
            <option value="local"${d.connect === "local" ? " selected" : ""}>${esc(i18n.t("optLocal"))}</option>
          </select>
        </label>
        <label class="field-label form-full" data-connect-ssh>${esc(i18n.t("fieldHost"))}
          <input type="text" name="host" value="${esc(d.host || "")}" placeholder="login.cluster.edu" />
          <span class="field-hint">${esc(i18n.t("fieldHostHint"))}</span>
        </label>
        <label class="field-label form-full">${esc(i18n.t("fieldProjectsRoot"))}
          <input type="text" name="projects_root" value="${esc(d.projects_root || "")}" placeholder="/scratch/alice/hpcport" />
          <span class="field-hint">${esc(i18n.t("fieldProjectsRootHint"))}</span>
        </label>
        <label class="field-label" data-slurm-only>${esc(i18n.t("fieldPartitions"))}
          <input type="text" name="partitions" value="${esc((d.partitions || []).join(", "))}" placeholder="a100, cpu" />
        </label>
        <label class="field-label" data-slurm-only>${esc(i18n.t("fieldMaxJobs"))}
          <input type="number" min="1" name="max_jobs" value="${esc(d.max_jobs ?? 10)}" />
        </label>
        <label class="field-label" data-slurm-only>${esc(i18n.t("fieldCoresPerNode"))}
          <input type="number" min="1" name="cores_per_node" value="${esc(d.cores_per_node ?? "")}" />
        </label>
        <label class="field-label" data-slurm-only>${esc(i18n.t("fieldGpusPerNode"))}
          <input type="number" min="0" name="gpus_per_node" value="${esc(d.gpus_per_node ?? "")}" placeholder="0" />
        </label>
        <label class="field-label" data-slurm-only>${esc(i18n.t("fieldPollInterval"))}
          <input type="number" min="1" name="poll_interval_s" value="${esc(d.poll_interval_s ?? 120)}" />
        </label>
        <label class="field-label">${esc(i18n.t("fieldExecTimeout"))}
          <input type="number" min="1" name="exec_timeout_s" value="${esc(d.exec_timeout_s ?? 3600)}" />
        </label>
        <label class="field-label">${esc(i18n.t("fieldPython"))}
          <input type="text" name="python" value="${esc(d.python || "python3")}" />
        </label>
        <label class="field-label form-full">${esc(i18n.t("fieldExtraPaths"))}
          <textarea name="extra_paths" rows="2" placeholder="/tmp">${esc((d.extra_paths || []).join("\n"))}</textarea>
        </label>
        <div class="field-label form-full">
          <span>${esc(i18n.t("fieldGatewayAllow"))}</span>
          <div class="checkbox-grid">
            ${ALLOWABLE_RULES.map((r) => `
              <label class="checkbox-pill">
                <input type="checkbox" name="gateway_allow" value="${r}"${allowSet.has(r) ? " checked" : ""} />
                <span class="mono">${r}</span>
              </label>
            `).join("")}
          </div>
        </div>
        <label class="field-label form-full">${esc(i18n.t("fieldGuide"))}
          <textarea name="guide" rows="3" placeholder="module load cuda/12.2 ...">${esc(d.guide || "")}</textarea>
        </label>
        <label class="field-label form-full" data-slurm-only>${esc(i18n.t("fieldJobTemplate"))}
          <textarea name="job_template" rows="3" class="mono" placeholder="#!/bin/bash&#10;#SBATCH --partition=a100">${esc(d.job_template || "")}</textarea>
        </label>
        <div class="modal-footer form-full">
          <button type="button" class="btn btn-outline btn-sm" id="pm-cancel">${esc(i18n.t("cancelBtn"))}</button>
          <button type="submit" class="btn btn-primary btn-sm">${esc(isEdit ? i18n.t("saveChanges") : i18n.t("createBtn"))}</button>
        </div>
      </form>
    `;

    const yamlHtml = isEdit ? `
      <pre class="code-block" style="margin-top:0;max-height:360px">${esc(presetObj.yaml || "")}</pre>
      <div style="margin-top:16px">
        <div class="text-sm" style="font-weight:600;margin-bottom:8px">${esc(i18n.t("versionHistory"))}</div>
        <div class="flex-row" style="gap:8px">
          ${(presetObj.versions || []).slice().reverse().map((v) => `
            <div class="metric-chip flex-row" style="min-width:auto;padding:5px 10px">
              <span class="mono">v${v}</span>
              ${v === presetObj.version ? `<span class="pill pill-green">${esc(i18n.t("versionActive"))}</span>` :
                `<button type="button" class="btn btn-subtle btn-xs" data-restore-v="${v}">${esc(i18n.t("restoreBtn"))}</button>`}
            </div>
          `).join("")}
        </div>
      </div>
    ` : "";

    openModal(title, tabBar + (subTab === "form" ? formHtml : yamlHtml));

    if (isEdit) {
      document.getElementById("pm-tab-form").onclick = () => { subTab = "form"; renderBody(); };
      document.getElementById("pm-tab-yaml").onclick = () => { subTab = "yaml"; renderBody(); };
    }

    const cancelBtn = document.getElementById("pm-cancel");
    if (cancelBtn) cancelBtn.onclick = closeModal;

    const form = document.getElementById("preset-form");
    if (form) {
      const syncVisibility = () => {
        const isSlurm = form.querySelector('select[name="scheduler"]').value === "slurm";
        const isSsh = form.querySelector('select[name="connect"]').value === "ssh";
        form.querySelectorAll("[data-slurm-only]").forEach((node) => node.classList.toggle("hidden", !isSlurm));
        form.querySelectorAll("[data-connect-ssh]").forEach((node) => node.classList.toggle("hidden", !isSsh));
      };
      form.querySelector('select[name="scheduler"]').onchange = syncVisibility;
      form.querySelector('select[name="connect"]').onchange = syncVisibility;
      syncVisibility();

      form.onsubmit = async (e) => {
        e.preventDefault();
        const fd = new FormData(form);
        const connect = fd.get("connect");
        const scheduler = fd.get("scheduler");
        const host = (fd.get("host") || "").toString().trim();
        const projects_root = (fd.get("projects_root") || "").toString().trim();
        const partitions = (fd.get("partitions") || "").toString().split(",").map((s) => s.trim()).filter(Boolean);
        const extra_paths = (fd.get("extra_paths") || "").toString().split("\n").map((s) => s.trim()).filter(Boolean);
        const gateway_allow = fd.getAll("gateway_allow").map(String);
        const cpnRaw = (fd.get("cores_per_node") || "").toString().trim();
        const gpnRaw = (fd.get("gpus_per_node") || "").toString().trim();
        const guide = (fd.get("guide") || "").toString().trim();
        const job_template = (fd.get("job_template") || "").toString();

        const maxJobs = parseStrictNumber((fd.get("max_jobs") || "10").toString(), { integer: true, min: 1 });
        const pollInt = parseStrictNumber((fd.get("poll_interval_s") || "120").toString(), { integer: true, min: 1 });
        const execTo = parseStrictNumber((fd.get("exec_timeout_s") || "3600").toString(), { integer: true, min: 1 });
        const cpn = cpnRaw ? parseStrictNumber(cpnRaw, { integer: true, min: 1 }) : { ok: true, value: null };
        const gpn = gpnRaw !== "" ? parseStrictNumber(gpnRaw, { integer: true, min: 0 }) : { ok: true, value: null };
        for (const [field, res] of [
          ["max_jobs", maxJobs],
          ["poll_interval_s", pollInt],
          ["exec_timeout_s", execTo],
          ["cores_per_node", cpn],
          ["gpus_per_node", gpn],
        ]) {
          if (!res.ok) {
            toast(i18n.t("invalidNumber", { field }), true);
            return;
          }
        }

        const data = {
          connect,
          scheduler,
          host: connect === "ssh" ? host : null,
          projects_root: projects_root || null,
          partitions,
          max_jobs: maxJobs.value,
          cores_per_node: cpn.value,
          gpus_per_node: gpn.value,
          poll_interval_s: pollInt.value,
          exec_timeout_s: execTo.value,
          python: (fd.get("python") || "python3").toString().trim() || "python3",
          extra_paths,
          gateway_allow,
          guide: guide || null,
          job_template: job_template.trim() ? job_template : null,
        };
        if (!isEdit) {
          // Remove null fields on create so preset_to_dict stays concise
          for (const k of Object.keys(data)) {
            if (data[k] === null || (Array.isArray(data[k]) && !data[k].length)) delete data[k];
          }
        }
        try {
          if (isEdit) {
            await api(`/api/presets/${encodeURIComponent(presetObj.name)}`, {
              method: "PATCH",
              body: { changes: data, note: "edited in web ui" },
            });
          } else {
            const name = (fd.get("name") || "").toString().trim();
            await api("/api/presets", { method: "POST", body: { name, data } });
          }
          closeModal();
          toast(i18n.t("savedToast"));
          await refreshAll();
        } catch (err) {
          toast(err.message, true);
        }
      };
    }

    document.querySelectorAll("[data-restore-v]").forEach((btn) => {
      btn.onclick = async () => {
        const v = Number(btn.dataset.restoreV);
        try {
          await api(`/api/presets/${encodeURIComponent(presetObj.name)}/restore`, {
            method: "POST",
            body: { version: v },
          });
          closeModal();
          toast(i18n.t("restoredToast", { name: presetObj.name, v }));
          await refreshAll();
        } catch (err) {
          toast(err.message, true);
        }
      };
    });
  }

  renderBody();
}

function openNewProjectModal() {
  openModal(i18n.t("projectModalTitle"), `
    <form id="new-project-form" class="form-grid">
      <label class="field-label form-full">${esc(i18n.t("fieldProjectName"))}
        <input type="text" name="name" required placeholder="my-research" />
      </label>
      <label class="field-label form-full">${esc(i18n.t("fieldLocalDir"))}
        <input type="text" name="local_dir" placeholder="/Users/alice/projects/my-research" />
        <span class="field-hint">${esc(i18n.t("fieldLocalDirHint"))}</span>
      </label>
      <div class="modal-footer form-full">
        <button type="button" class="btn btn-outline btn-sm" id="np-cancel">${esc(i18n.t("cancelBtn"))}</button>
        <button type="submit" class="btn btn-primary btn-sm">${esc(i18n.t("createBtn"))}</button>
      </div>
    </form>
  `);
  document.getElementById("np-cancel").onclick = closeModal;
  document.getElementById("new-project-form").onsubmit = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const name = (fd.get("name") || "").toString().trim();
    const local_dir = (fd.get("local_dir") || "").toString().trim() || null;
    try {
      await api("/api/projects", { method: "POST", body: { name, local_dir } });
      closeModal();
      toast(i18n.t("savedToast"));
      await refreshAll();
    } catch (err) {
      toast(err.data?.code === "project_retired" ? i18n.t("retiredNameReserved", { project: name }) : err.message, true);
    }
  };
}

async function openProjectRetirementModal(project) {
  const url = `/api/projects/${encodeURIComponent(project)}/retirement`;
  const title = i18n.t("retireProjectTitle", { project });
  const backdrop = document.getElementById("modal-backdrop");
  const showing = (element) => element.isConnected && !backdrop.classList.contains("hidden");
  openModal(title, `<p id="retirement-loading" role="status">${esc(i18n.t("retirePreviewLoading"))}</p>`);
  const loading = document.getElementById("retirement-loading");

  function render(preview, bindingChanged = false) {
    const { local_dir: localDir, next_project: nextProject } = preview.binding;
    const binding = localDir
      ? i18n.t(nextProject ? "retireBindingProject" : "retireBindingUnbound", { localDir, nextProject })
      : i18n.t("retireNoLocalDir");
    const blocked = preview.blockers.length > 0;
    openModal(title, `
      <form id="retire-project-form">
        ${bindingChanged ? `<p class="alert-banner" role="alert">${esc(i18n.t("retireBindingChanged"))}</p>` : ""}
        <p style="overflow-wrap:anywhere">${esc(binding)}</p>
        <p>${esc(i18n.t("retireProjectEffects"))}</p>
        ${blocked ? `
          <div class="alert-banner" role="status">
            ${esc(i18n.t("retireBlocked"))}
            <ul>${preview.blockers.map((kind) => `<li>${esc(i18n.t("retireBlocker_" + kind))}</li>`).join("")}</ul>
          </div>
        ` : ""}
        <div class="modal-footer">
          <button type="button" id="retire-project-cancel" class="btn btn-outline btn-sm">${esc(i18n.t("cancelBtn"))}</button>
          <button type="submit" class="btn btn-danger btn-sm"${blocked ? " disabled" : ""}>${esc(i18n.t("retireProject"))}</button>
        </div>
      </form>
    `);
    const form = document.getElementById("retire-project-form");
    const cancel = document.getElementById("retire-project-cancel");
    const confirm = form.querySelector('[type="submit"]');
    cancel.onclick = closeModal;
    cancel.focus();
    form.onsubmit = async (event) => {
      event.preventDefault();
      if (confirm.disabled) return;
      confirm.disabled = true;
      try {
        await api(url, { method: "POST", body: { binding: preview.binding } });
        if (showing(form)) closeModal();
        toast(i18n.t("retiredProjectToast", { project }));
        await refreshAll();
      } catch (err) {
        if (err.data?.preview) {
          if (showing(form)) render(err.data.preview, err.data.code === "binding_changed");
        } else {
          toast(err.message, true);
          confirm.disabled = false;
        }
      }
    };
  }

  try {
    const preview = await api(url);
    if (showing(loading)) render(preview);
  } catch (err) {
    if (showing(loading)) {
      closeModal();
      toast(err.message, true);
    }
  }
}

function openAttachModal(projectName) {
  const presets = (state.snapshot && state.snapshot.presets) || [];
  if (!presets.length) {
    openModal(i18n.t("attachModalTitle", { project: projectName }), `
      <p class="muted text-sm" style="margin-top:0">${esc(i18n.t("noPresetsToAttach"))}</p>
      <div class="modal-footer">
        <button type="button" class="btn btn-primary btn-sm" id="at-ok">${esc(i18n.t("close"))}</button>
      </div>
    `);
    document.getElementById("at-ok").onclick = closeModal;
    return;
  }
  openModal(i18n.t("attachModalTitle", { project: projectName }), `
    <form id="attach-form" class="form-grid">
      <label class="field-label form-full">${esc(i18n.t("fieldSelectServer"))}
        <select name="cluster" required>
          ${presets.map((pr) => `<option value="${esc(pr.name)}">${esc(pr.name)} (${esc(pr.data?.scheduler || "slurm")})</option>`).join("")}
        </select>
      </label>
      <label class="field-label form-full">${esc(i18n.t("fieldRemoteRoot"))}
        <input type="text" name="root" placeholder="/scratch/alice/hpcport/${esc(projectName)}" />
        <span class="field-hint">${esc(i18n.t("fieldRemoteRootHint"))}</span>
      </label>
      <label class="field-label form-full">${esc(i18n.t("fieldOverrideMaxJobs"))}
        <input type="number" min="1" name="max_jobs" placeholder="" />
      </label>
      <div class="modal-footer form-full">
        <button type="button" class="btn btn-outline btn-sm" id="at-cancel">${esc(i18n.t("cancelBtn"))}</button>
        <button type="submit" class="btn btn-primary btn-sm">${esc(i18n.t("attachConfirm"))}</button>
      </div>
    </form>
  `);
  document.getElementById("at-cancel").onclick = closeModal;
  document.getElementById("attach-form").onsubmit = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const cluster = fd.get("cluster");
    const root = (fd.get("root") || "").toString().trim() || null;
    const mjRaw = (fd.get("max_jobs") || "").toString().trim();
    const max_jobs = mjRaw ? parseInt(mjRaw, 10) : null;
    try {
      await api(`/api/projects/${encodeURIComponent(projectName)}/clusters`, {
        method: "POST",
        body: { cluster, root, max_jobs },
      });
      closeModal();
      toast(i18n.t("attachedToast", { cluster, project: projectName }));
      await refreshAll();
    } catch (err) {
      toast(err.message, true);
    }
  };
}

// ------------------------------------------------------------------ EVENT WIRING & INIT
function wireEvents() {
  // Navigation tabs
  document.querySelectorAll(".nav-tab").forEach((btn) => {
    btn.onclick = () => switchTab(btn.dataset.tab);
  });

  window.addEventListener("hashchange", () => {
    const t = readHashTab();
    if (!state.authenticated) {
      state.tab = t;
      return;
    }
    if (t !== state.tab) switchTab(t, false);
  });

  // Project switcher
  document.getElementById("project-select").onchange = (e) => {
    if (isRetiredProject() || isRetiredProject(e.target.value)) resetBatchFilters();
    state.project = e.target.value;
    localStorage.setItem("hpcport_project", state.project);
    refreshActiveTab();
  };

  // Manual Poll
  document.getElementById("poll-btn").onclick = async () => {
    const btn = document.getElementById("poll-btn");
    const lbl = document.getElementById("poll-btn-label");
    btn.disabled = true;
    lbl.textContent = i18n.t("polling");
    try {
      await api("/api/poll", { method: "POST" });
      toast(i18n.t("polledToast"));
      await refreshAll();
    } catch (e) {
      toast(e.message, true);
    } finally {
      btn.disabled = false;
      lbl.textContent = i18n.t("pollNow");
    }
  };

  // Browser notification toggle
  document.getElementById("notif-btn").onclick = async () => {
    if (!("Notification" in window)) {
      toast(i18n.t("alertsDeniedToast"), true);
      return;
    }
    if (state.alertsEnabled) {
      state.alertsEnabled = false;
      localStorage.setItem("hpcport_alerts", "0");
      applyStaticLabels();
      return;
    }
    const perm = await Notification.requestPermission();
    if (perm === "granted") {
      state.alertsEnabled = true;
      localStorage.setItem("hpcport_alerts", "1");
      applyStaticLabels();
      toast(i18n.t("alertsEnabledToast"));
    } else {
      toast(i18n.t("alertsDeniedToast"), true);
    }
  };

  // Theme toggle
  document.getElementById("theme-btn").onclick = () => {
    state.theme = state.theme === "dark" ? "light" : "dark";
    applyTheme();
  };

  // Language pills (header + auth gate)
  document.querySelectorAll(".lang-pill button").forEach((btn) => {
    btn.onclick = () => setLanguage(btn.dataset.lang, true);
  });

  // PIN unlock form
  document.getElementById("pin-form").onsubmit = async (e) => {
    e.preventDefault();
    const pin = document.getElementById("pin-input").value.trim();
    const errEl = document.getElementById("pin-error");
    errEl.classList.add("hidden");
    try {
      const resp = await fetch("/api/auth/pin", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-HPCPort-UI": "1" },
        body: JSON.stringify({ pin }),
        credentials: "same-origin",
      });
      if (!resp.ok) {
        let data = {};
        try {
          data = await resp.json();
        } catch {
          data = {};
        }
        errEl.textContent = resp.status === 429
          ? i18n.t("unlockRateLimited", { s: data.retry_after || 60 })
          : i18n.t("unlockError");
        errEl.classList.remove("hidden");
        return;
      }
      document.getElementById("pin-input").value = "";
      state.tab = readHashTab();
      showAppShell();
    } catch {
      errEl.textContent = i18n.t("unlockError");
      errEl.classList.remove("hidden");
    }
  };

  // Drawer & Modal closing
  document.getElementById("drawer-close").onclick = closeDrawer;
  document.getElementById("drawer-backdrop").onclick = closeDrawer;
  document.getElementById("drawer-refresh").onclick = loadDrawerData;
  document.getElementById("modal-close").onclick = closeModal;
  document.getElementById("modal-backdrop").onclick = (e) => {
    if (e.target.id === "modal-backdrop") closeModal();
  };
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      closeModal();
      closeDrawer();
    }
  });
}

async function boot() {
  applyTheme();
  wireEvents();
  try {
    const resp = await fetch("/api/auth/status", { credentials: "same-origin" });
    const st = await resp.json();
    if (!localStorage.getItem("hpcport_lang") && st.language) {
      i18n.setLang(st.language);
    } else {
      i18n.setLang(i18n.lang);
    }
    state.tab = readHashTab();
    if (st.authenticated) {
      showAppShell();
    } else {
      showAuthGate();
    }
  } catch {
    showAuthGate();
  }
}

boot();
