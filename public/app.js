const $ = (sel) => document.querySelector(sel);

const form = $("#jobForm");
const statusChip = $("#statusChip");
const logBox = $("#logBox");
const progressFill = $("#progressFill");
const pctLabel = $("#pctLabel");
const jobMeta = $("#jobMeta");

const downloadWorkbook = $("#downloadWorkbook");
const downloadAssets = $("#downloadAssets");
const downloadLog = $("#downloadLog");

const startBtn = $("#startBtn");
const clearBtn = $("#clearBtn");

const releaseDateInput = $("#releaseDate");
const dateIconBtn = $("#dateIconBtn");
const copyLogsBtn = $("#copyLogsBtn");

let currentJobId = null;
let pollTimer = null;

function setChip(text, tone = "muted") {
  statusChip.textContent = text;
  statusChip.style.color =
    tone === "good" ? "rgba(52,211,153,.95)" :
    tone === "bad" ? "rgba(251,113,133,.95)" :
    "rgba(255,255,255,.62)";
}

function setProgress(pct) {
  const v = Math.max(0, Math.min(100, Number(pct) || 0));
  progressFill.style.width = `${v}%`;
  pctLabel.textContent = `${v}%`;
}

function setDownloadsEnabled({ workbook, assets, log }) {
  const set = (el, ok) => {
    if (ok) el.classList.remove("disabled");
    else el.classList.add("disabled");
    el.setAttribute("aria-disabled", ok ? "false" : "true");
  };
  set(downloadWorkbook, workbook);
  set(downloadAssets, assets);
  set(downloadLog, log);
}

function appendLogs(lines) {
  // replace full view (cleaner)
  logBox.textContent = lines.join("\n");
  logBox.scrollTop = logBox.scrollHeight;
}

async function createJob(formData) {
  const res = await fetch("/api/jobs", {
    method: "POST",
    body: formData,
  });
  const json = await res.json();
  if (!json.ok) throw new Error(json.error || "Failed to create job");
  return json.jobId;
}

async function fetchJob(jobId) {
  const res = await fetch(`/api/jobs/${jobId}`);
  const json = await res.json();
  if (!json.ok) throw new Error(json.error || "Failed to fetch job status");
  return json;
}

function stopPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = null;
}

function startPolling(jobId) {
  stopPolling();

  pollTimer = setInterval(async () => {
    try {
      const j = await fetchJob(jobId);

      setProgress(j.progress);
      jobMeta.textContent = `Job: ${j.id} • Status: ${j.status}`;

      if (j.status === "done") setChip("Done", "good");
      else if (j.status === "error") setChip("Error", "bad");
      else if (j.status === "processing") setChip("Processing", "muted");
      else setChip("Queued", "muted");

      if (Array.isArray(j.logs)) appendLogs(j.logs);

      // Link downloads
      downloadWorkbook.href = `/api/jobs/${jobId}/workbook.xlsx`;
      downloadAssets.href = `/api/jobs/${jobId}/assets.zip`;
      downloadLog.href = `/api/jobs/${jobId}/job.log`;

      setDownloadsEnabled({
        workbook: !!j.hasWorkbook,
        assets: !!j.hasAssetsZip,
        log: true, // log exists early
      });

      // stop when finished
      if (j.status === "done" || j.status === "error") {
        stopPolling();
        startBtn.disabled = false;
        startBtn.textContent = "Start job";
      }
    } catch (e) {
      // don’t kill UI on polling error — just show it once
      setChip("Disconnected", "bad");
    }
  }, 1000);
}

// --- Date picker behavior ---
// Want: clicking input opens picker, clicking icon does NOT.

releaseDateInput.addEventListener("pointerdown", (e) => {
  // Only when the actual input is the target (not wrapper, not icon)
  if (e.target !== releaseDateInput) return;

  // If browser supports showPicker(), open the dropdown.
  // pointerdown is more reliable than click (prevents double-trigger)
  if (typeof releaseDateInput.showPicker === "function") {
    e.preventDefault(); // stops the native calendar icon behavior competing
    releaseDateInput.showPicker();
  }
});

// Icon button should NOT trigger picker
dateIconBtn.addEventListener("pointerdown", (e) => {
  e.preventDefault();
  e.stopPropagation();
  releaseDateInput.focus(); // keep it nice, but don't open picker
});

// Safety: also block click in case browser ignores pointerdown
dateIconBtn.addEventListener("click", (e) => {
  e.preventDefault();
  e.stopPropagation();
});

// Icon button should NOT trigger picker
dateIconBtn.addEventListener("click", (e) => {
  e.preventDefault();
  e.stopPropagation();
  // do nothing, or focus input without opening:
  releaseDateInput.focus();
});

copyLogsBtn.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(logBox.textContent || "");
    copyLogsBtn.textContent = "Copied";
    setTimeout(() => (copyLogsBtn.textContent = "Copy"), 900);
  } catch {
    // ignore
  }
});

clearBtn.addEventListener("click", () => {
  stopPolling();
  currentJobId = null;
  setChip("Idle");
  setProgress(0);
  jobMeta.textContent = "No job running.";
  logBox.textContent = "Ready.";
  setDownloadsEnabled({ workbook: false, assets: false, log: false });
  startBtn.disabled = false;
  startBtn.textContent = "Start job";
  form.reset();
});

form.addEventListener("submit", async (e) => {
  e.preventDefault();

  if (currentJobId) return;

  const projectCode = $("#projectCode").value.trim();
  const releaseDate = $("#releaseDate").value;
  const zips = $("#zips").files;

  if (!projectCode) return;
  if (!releaseDate) return;
  if (!zips || !zips.length) return;

  const fd = new FormData();
  fd.append("projectCode", projectCode);
  fd.append("releaseDate", releaseDate);
  for (const f of zips) fd.append("zips", f);

  try {
    startBtn.disabled = true;
    startBtn.textContent = "Uploading…";
    setChip("Uploading…");
    setProgress(1);
    setDownloadsEnabled({ workbook: false, assets: false, log: false });

    const jobId = await createJob(fd);
    currentJobId = jobId;

    setChip("Queued");
    jobMeta.textContent = `Job: ${jobId} • Status: queued`;
    startBtn.textContent = "Running…";

    // start polling immediately
    startPolling(jobId);
  } catch (err) {
    setChip("Error", "bad");
    startBtn.disabled = false;
    startBtn.textContent = "Start job";
    logBox.textContent = String(err?.message || err);
  }
});