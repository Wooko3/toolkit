// server.js
import express from "express";
import path from "path";
import fs from "fs";
import os from "os";
import crypto from "crypto";
import { fileURLToPath } from "url";

import multer from "multer";

import { enqueueJob } from "./src/jobs/queue.js";
import { createJobLogger } from "./src/jobs/logger.js";
import { buildExport } from "./src/jobs/runner.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ extended: true }));

// Serve client UI
app.use(express.static(path.join(__dirname, "public"), { extensions: ["html"] }));

// -------------------------
// Config
// -------------------------
const TEMPLATE_PATH = path.join(__dirname, "templates", "orchard_bulk_upload_template.xlsx");
console.log("Template path:", TEMPLATE_PATH);

// Upload zips to disk (required for huge zips)
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, os.tmpdir()),
    filename: (req, file, cb) => {
      const safe = file.originalname.replace(/[^\w.\-() ]+/g, "_");
      cb(null, `${Date.now()}_${safe}`);
    },
  }),
  limits: {
    fileSize: 8 * 1024 * 1024 * 1024, // 8GB
  },
});

const JOB_TTL_MS = 3 * 60 * 60 * 1000; // 3 hours
const jobs = new Map(); // jobId -> job

function newJobId() {
  return crypto.randomBytes(10).toString("hex");
}

function cleanupJob(jobId) {
  const job = jobs.get(jobId);
  if (!job) return;

  try {
    if (job.dir && fs.existsSync(job.dir)) fs.rmSync(job.dir, { recursive: true, force: true });
  } catch {}

  // delete uploaded zip files too
  try {
    for (const p of job.uploadedZipPaths || []) {
      if (p && fs.existsSync(p)) fs.rmSync(p, { force: true });
    }
  } catch {}

  jobs.delete(jobId);
}

setInterval(() => {
  const now = Date.now();
  for (const [id, job] of jobs.entries()) {
    if (now - job.createdAt > JOB_TTL_MS) cleanupJob(id);
  }
}, 10 * 60 * 1000);

// -------------------------
// API
// -------------------------

app.post("/api/jobs", upload.array("zips"), async (req, res) => {
  try {
    const projectCode = String(req.body.projectCode || "").trim();
    const releaseDate = String(req.body.releaseDate || "").trim();

    if (!projectCode) return res.status(400).json({ ok: false, error: "Missing projectCode" });
    if (!releaseDate) return res.status(400).json({ ok: false, error: "Missing releaseDate" });
    if (!req.files || !req.files.length) return res.status(400).json({ ok: false, error: "No zip files uploaded" });

    if (!fs.existsSync(TEMPLATE_PATH)) {
      return res.status(500).json({ ok: false, error: "Template not found on server." });
    }

    const jobId = newJobId();
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "toolkit-v2-1-"));

    // Normalize zipFiles to what runner expects (path + originalname)
    const zipFiles = req.files.map((f) => ({
      path: f.path,
      originalname: f.originalname,
      size: f.size,
    }));

    const job = {
      id: jobId,
      status: "queued",
      progress: 0,
      log: [],
      createdAt: Date.now(),
      dir,

      // outputs (runner uses these names)
      workbookPath: path.join(dir, `orchard_${projectCode}.xlsx`),
      assetsPath: path.join(dir, `assets_${projectCode}.zip`),

      // logs
      logPath: path.join(dir, `job_${jobId}.log`),

      // inputs
      templatePath: TEMPLATE_PATH,
      projectCode,
      releaseDate,
      zipFiles,
      uploadedZipPaths: req.files.map((f) => f.path).filter(Boolean),

      // stall / bytes (runner can update these if it wants)
      lastByteAt: Date.now(),
      bytesProcessed: 0,
    };

    jobs.set(jobId, job);

    // Attach logger to job (runner will call jobLog -> logger.log)
    job.logger = createJobLogger(job);

    // Enqueue job (single-concurrency by default)
    enqueueJob(async () => {
      try {
        job.status = "processing";
        job.progress = 1;

        job.logger.log(`Project Code: ${projectCode}`);
        job.logger.log(`Release Date: ${releaseDate}`);
        job.logger.log(`Zips: ${zipFiles.length}`);

        // Run the build (this writes workbookPath + assetsPath)
        await buildExport({
          projectCode,
          releaseDate,
          zipFiles,
          job,
          templatePath: job.templatePath,
          jobLog: (jobObj, msg) => jobObj.logger.log(msg),
          setProgress: (jobObj, pct) => {
            jobObj.progress = Math.max(0, Math.min(100, Math.round(pct)));
          },
        });

        job.status = "done";
        job.progress = 100;
        job.logger.log("✅ Build complete");
      } catch (e) {
        job.status = "error";
        job.progress = 100;
        job.logger.log(`❌ Error: ${String(e?.message || e)}`);
      } finally {
        // remove uploaded zips after job (save disk)
        try {
          for (const p of job.uploadedZipPaths || []) {
            if (p && fs.existsSync(p)) fs.rmSync(p, { force: true });
          }
          job.uploadedZipPaths = [];
        } catch {}
      }
    });

    return res.json({ ok: true, jobId });
  } catch (err) {
    console.error("Server error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

// status + logs
app.get("/api/jobs/:id", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ ok: false, error: "Job not found" });

  const tail = (job.log || []).slice(-400);

  return res.json({
    ok: true,
    id: job.id,
    status: job.status,
    progress: job.progress ?? 0,
    createdAt: job.createdAt,
    logs: tail,
    hasWorkbook: !!job.workbookPath && fs.existsSync(job.workbookPath),
    hasAssetsZip: !!job.assetsPath && fs.existsSync(job.assetsPath),
    logPath: job.logPath || null,
  });
});

// download workbook
app.get("/api/jobs/:id/workbook.xlsx", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).send("Job not found");
  if (!fs.existsSync(job.workbookPath)) return res.status(404).send("Workbook not ready");

  res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  res.setHeader("Content-Disposition", `attachment; filename="orchard_export_${job.id}.xlsx"`);
  fs.createReadStream(job.workbookPath).pipe(res);
});

// download assets zip
app.get("/api/jobs/:id/assets.zip", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).send("Job not found");
  if (!fs.existsSync(job.assetsPath)) return res.status(404).send("Assets not ready");

  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename="processed_assets_${job.id}.zip"`);
  fs.createReadStream(job.assetsPath).pipe(res);
});

// download full log
app.get("/api/jobs/:id/job.log", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).send("Job not found");
  if (!fs.existsSync(job.logPath)) return res.status(404).send("Log not ready");

  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename="job_${job.id}.log"`);
  fs.createReadStream(job.logPath).pipe(res);
});

// manual cleanup
app.delete("/api/jobs/:id", (req, res) => {
  const id = req.params.id;
  if (!jobs.has(id)) return res.status(404).json({ ok: false, error: "Job not found" });
  cleanupJob(id);
  return res.json({ ok: true });
});

app.get("/health", (req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Toolkit v2.1 server running on http://localhost:${PORT}`);
});