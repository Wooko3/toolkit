// src/jobs/runner.js
import fs from "fs";
import path from "path";
import os from "os";
import ExcelJS from "exceljs";
import sharp from "sharp";
import archiver from "archiver";

import {
  normalizeZipPath,
  isSafeEntryName,
  detectLanguageFromPath,
  detectLanguagesFromZipName,
  isAudioFile,
  isArtworkFile,
  isCreditsFile,
  isAudioFolderPath,
  safeJoin,
  mkdirp,
} from "../lib/util.js";

import { listEntries, extractEntryToFile, readEntryToBuffer } from "../lib/zip.js";

// -------------------------
// Config / Defaults
// -------------------------
const LANG_MAP = {
  EN: "English",
  ES: "Spanish; Castilian",
  PT: "Portuguese",
  DE: "German",
  FR: "French",
  ID: "Indonesian",
  IT: "Italian",
  KO: "Korean",
  VI: "Vietnamese",
};

const DEFAULT_PRIMARY_ARTIST = "Pinkfong";
const DEFAULT_PRODUCER = "Pinkfong";
const DEFAULT_SONGWRITER = "Pinkfong";
const DEFAULT_PUBLISHERS = "SmartStudy Music (BMI)";
const DEFAULT_LINE_ENTITY = "The Pinkfong Company, Inc.";
const DEFAULT_SPECIAL_INSTRUCTIONS = "Please exclude Tencent & YT CID";
const DEFAULT_NOT_CLEARED = "Russia, South Korea";
const DEFAULT_GENRE = "Children's";
const DEFAULT_SUBGENRE = "Children's";
const DEFAULT_ITUNES_PREORDER = "No Pre-order";
const DEFAULT_ALBUM_PRICING = "Mid/Front";
const DEFAULT_TRACK_PRICING = "Front";

// -------------------------
// Small helpers
// -------------------------
function yyyy(dateStr) {
  const d = new Date(dateStr);
  return isNaN(d) ? "" : String(d.getFullYear());
}

function inferFormat(trackCount) {
  if (trackCount <= 3) return "Single";
  if (trackCount <= 6) return "EP";
  return "Album";
}

function splitArtistsArray(s) {
  if (!s) return [];
  return String(s)
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

// -------------------------
// Excel header helpers
// -------------------------
function normHeader(s) {
  return String(s ?? "")
    .toLowerCase()
    .replace(/\*/g, "")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function findHeaderRow(worksheet, requiredHeaders, maxScanRows = 80) {
  const required = requiredHeaders.map(normHeader);
  for (let r = 1; r <= maxScanRows; r++) {
    const row = worksheet.getRow(r);
    const vals = (row.values || []).map(normHeader);
    const hits = required.filter((h) => vals.includes(h)).length;
    if (hits >= Math.min(required.length, 3)) return r;
  }
  return null;
}

function buildHeaderMap(worksheet, headerRowNumber) {
  const row = worksheet.getRow(headerRowNumber);
  const map = {};
  (row.values || []).forEach((v, idx) => {
    if (!idx) return;
    const key = normHeader(v);
    if (key) map[key] = idx;
  });
  return map;
}

function rowHasAnyValue(row) {
  return (row.values || []).some((v, idx) => {
    if (!idx) return false;
    return v !== null && v !== undefined && String(v).trim() !== "";
  });
}

function findNextEmptyRow(worksheet, startRow) {
  let r = startRow;
  while (true) {
    const row = worksheet.getRow(r);
    if (!rowHasAnyValue(row)) return r;
    r++;
    if (r > worksheet.rowCount + 10000) return r;
  }
}

function appendRowByHeaders(worksheet, headerMap, headerRowNumber, rowObj) {
  const r = findNextEmptyRow(worksheet, headerRowNumber + 1);
  const row = worksheet.getRow(r);

  for (const [k, v] of Object.entries(rowObj)) {
    const col = headerMap[normHeader(k)];
    if (!col) continue;
    row.getCell(col).value = v ?? "";
  }

  row.commit?.();
  return r;
}

// Try multiple header variants; picks first that exists in the template
function setByAnyHeader(rowObj, keyVariants, value, headersPresentMap) {
  for (const k of keyVariants) {
    if (headersPresentMap[normHeader(k)]) {
      rowObj[k] = value;
      return;
    }
  }
  // fallback (still writes, even if template uses a different one)
  rowObj[keyVariants[0]] = value;
}

// -------------------------
// Credits parsing
// -------------------------
function cellToText(val) {
  if (val == null) return "";
  if (typeof val === "object") {
    if (val.text) return String(val.text).trim();
    if (val.richText) return val.richText.map((x) => x.text).join("").trim();
    if (val.result != null) return String(val.result).trim();
    if (val.hyperlink && val.text) return String(val.text).trim();
  }
  return String(val).trim();
}

async function parseCreditsXlsx(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);

  const ws = wb.getWorksheet("Credits Info") || wb.worksheets[0];

  const headerRowNum =
    findHeaderRow(ws, ["Album Title", "Song Title", "Track No.", "Language", "Artist"], 120) || 1;
  const headerMap = buildHeaderMap(ws, headerRowNum);

  const get = (row, candidates) => {
    for (const c of candidates) {
      const col = headerMap[normHeader(c)];
      if (!col) continue;
      const v = row.getCell(col).value;
      const t = cellToText(v);
      if (t) return t;
    }
    return "";
  };

  const rows = [];
  ws.eachRow((row, rowNumber) => {
    if (rowNumber <= headerRowNum) return;

    const trackNo = get(row, ["Track No.", "Track No", "Track Number", "No"]);
    const trackName = get(row, ["Track Name", "Song Title"]);
    const albumTitle = get(row, ["Album Title"]);
    const language = get(row, ["Language"]).toUpperCase().trim();

    const artist = get(row, ["Artist Name/Performed by", "Artist"]);
    const songwriter = get(row, ["Songwriter", "Writers"]);
    const producer = get(row, ["Producer"]);
    const publishers = get(row, ["Publishers", "Publisher"]);
    const isrc = get(row, ["ISRC Code", "ISRC"]);

    if (!trackNo && !trackName && !albumTitle) return;

    rows.push({
      language,
      albumTitle,
      trackNo: String(trackNo).trim(),
      trackName,
      artist,
      songwriter,
      producer,
      publishers,
      isrc,
    });
  });

  return rows;
}

// -------------------------
// Artwork conversion
// -------------------------
async function toArtworkJpeg3000(buffer) {
  return await sharp(buffer)
    .resize(3000, 3000, { fit: "cover" })
    .jpeg({ quality: 92 })
    .toBuffer();
}

// -------------------------
// Zip folder -> assets zip (REAL heartbeat)
// -------------------------
async function zipFolderToFile({ folderPath, outZipPath, job, jobLog, zipRootName }) {
  if (!fs.existsSync(folderPath)) {
    throw new Error(`Extract folder missing: ${folderPath}`);
  }

  const countFiles = (dir) => {
    let n = 0;
    for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
      const p = path.join(dir, ent.name);
      if (ent.isDirectory()) n += countFiles(p);
      else n += 1;
    }
    return n;
  };

  const totalFiles = countFiles(folderPath);
  jobLog(job, `Assets folder: ${folderPath} (${totalFiles} files)`);

  if (totalFiles === 0) {
    throw new Error(`Extracted assets folder is empty: ${folderPath}`);
  }

  await new Promise((resolve, reject) => {
    const out = fs.createWriteStream(outZipPath);
    const archive = archiver("zip", { zlib: { level: 9 } });

    let lastPointer = 0;
    let lastChangeAt = Date.now();

    const stallTimer = setInterval(() => {
      const p = archive.pointer();
      if (p !== lastPointer) {
        lastPointer = p;
        lastChangeAt = Date.now();
        const mb = (p / (1024 * 1024)).toFixed(1);
        jobLog(job, `Zipping… ${mb} MB written`);
        return;
      }
      const idleMs = Date.now() - lastChangeAt;
      if (idleMs > 60_000) {
        jobLog(job, `⚠️ Stall detected: no archive output for ${Math.round(idleMs / 1000)}s`);
      }
    }, 10_000);

    const cleanup = () => clearInterval(stallTimer);

    out.on("close", () => {
      cleanup();
      resolve();
    });
    out.on("error", (e) => {
      cleanup();
      reject(e);
    });

    archive.on("warning", (e) => jobLog(job, `⚠️ archiver warning: ${e.message}`));
    archive.on("error", (e) => {
      cleanup();
      reject(e);
    });

    archive.pipe(out);

    const root = zipRootName || path.basename(folderPath);
    archive.directory(folderPath, root);

    archive.finalize();
  });
}

// -------------------------
// MAIN EXPORT
// -------------------------
export async function buildExport({ projectCode, releaseDate, zipFiles, job, jobLog, setProgress, templatePath }) {
  job.status = "processing";
  setProgress(job, 1);

  const year = yyyy(releaseDate);

  jobLog(job, `Project Code: ${projectCode}`);
  jobLog(job, `Release Date: ${releaseDate}`);
  jobLog(job, `Zips: ${zipFiles.length}`);

  // -------------------------
  // Load template workbook
  // -------------------------
  setProgress(job, 3);
  jobLog(job, "Loading Orchard template…");

  const templateBytes = fs.readFileSync(templatePath);
  const orchardWb = new ExcelJS.Workbook();
  await orchardWb.xlsx.load(templateBytes);

  jobLog(job, `Template sheets: ${orchardWb.worksheets.map((w) => w.name).join(", ")}`);

  // Required sheets
  const wsProjects = orchardWb.getWorksheet("Projects");
  const wsProducts = orchardWb.getWorksheet("Products");
  const wsProdContrib = orchardWb.getWorksheet("Product Contributors");
  const wsTracks = orchardWb.getWorksheet("Tracks");
  const wsOtherTrackContrib = orchardWb.getWorksheet("Other Track Contributors");
  const wsTerr = orchardWb.getWorksheet("Accepted Territories");

  if (!wsProjects || !wsProducts || !wsProdContrib || !wsTracks || !wsOtherTrackContrib || !wsTerr) {
    throw new Error("One or more required sheets not found in template (sheet names changed).");
  }

  // Header rows + maps
  const projectsHeaderRow = findHeaderRow(wsProjects, ["Project Code", "Project Name", "Project Artist"]);
  const productsHeaderRow = findHeaderRow(wsProducts, ["Product Code", "Product name", "Product Metadata Language"]);
  const prodContribHeaderRow = findHeaderRow(wsProdContrib, ["Product Code", "Contributor Role", "Contributor Name"]);
  const tracksHeaderRow = findHeaderRow(wsTracks, ["Product Code", "Track Number", "Track Name", "File Name"]);
  const otherTrackContribHeaderRow = findHeaderRow(wsOtherTrackContrib, ["Product Code", "Track Number", "Contributor Name"]);

  if (!projectsHeaderRow || !productsHeaderRow || !prodContribHeaderRow || !tracksHeaderRow || !otherTrackContribHeaderRow) {
    throw new Error("Could not locate header rows in one or more sheets (template layout changed).");
  }

  const projectsHeaders = buildHeaderMap(wsProjects, projectsHeaderRow);
  const productsHeaders = buildHeaderMap(wsProducts, productsHeaderRow);
  const prodContribHeaders = buildHeaderMap(wsProdContrib, prodContribHeaderRow);
  const tracksHeaders = buildHeaderMap(wsTracks, tracksHeaderRow);
  const otherTrackContribHeaders = buildHeaderMap(wsOtherTrackContrib, otherTrackContribHeaderRow);

  // -------------------------
  // Projects row (single job)
  // -------------------------
  const projectRowObj = {};
  setByAnyHeader(projectRowObj, ["Subaccount ID"], "", projectsHeaders);
  setByAnyHeader(projectRowObj, ["Project Name"], projectCode, projectsHeaders);
  setByAnyHeader(projectRowObj, ["Project Code"], projectCode, projectsHeaders);
  setByAnyHeader(projectRowObj, ["Project Artist"], DEFAULT_PRIMARY_ARTIST, projectsHeaders);
  setByAnyHeader(projectRowObj, ["Apple ID"], "", projectsHeaders);
  setByAnyHeader(projectRowObj, ["Spotify ID"], "", projectsHeaders);
  setByAnyHeader(projectRowObj, ["Project Description"], "", projectsHeaders);

  appendRowByHeaders(wsProjects, projectsHeaders, projectsHeaderRow, projectRowObj);

  // -------------------------
  // Extraction folders
  // -------------------------
  jobLog(job, "Preparing extraction…");
  const extractedRoot = path.join(job.dir, "extracted");
  const extractedProjectDir = path.join(extractedRoot, projectCode);
  fs.mkdirSync(extractedProjectDir, { recursive: true });

  let englishTitle = "";

  // -------------------------
  // Progress estimation (rough)
  // -------------------------
  let totalWorkUnits = 0;
  let doneUnits = 0;

  for (const f of zipFiles) {
    try {
      jobLog(job, `Scanning zip: ${f.originalname || path.basename(f.path)}`);
      const entries = await listEntries(f.path);
      for (const e of entries) {
        if (!isSafeEntryName(e.name)) continue;
        if (e.isDirectory) continue;
        if (isAudioFile(e.name) && isAudioFolderPath(e.name)) totalWorkUnits += 1;
        else if (isArtworkFile(e.name)) totalWorkUnits += 0.25;
        else if (isCreditsFile(e.name)) totalWorkUnits += 0.25;
      }
    } catch {
      // ignore estimate failure
    }
  }

  if (!totalWorkUnits || totalWorkUnits < 1) totalWorkUnits = 1;

  const bump = (inc) => {
    doneUnits += inc;
    const pct = 5 + (doneUnits / totalWorkUnits) * 80; // leave headroom for write + zip
    setProgress(job, pct);
  };

  setProgress(job, 5);

  // -------------------------
  // Process each uploaded zip
  // -------------------------
  for (const file of zipFiles) {
    const zipName = file.originalname || "release.zip";
    jobLog(job, `--- Processing ${zipName} ---`);

    const zipPath = file.path;

    const projectNameGuess = zipName
      .replace(/\.zip$/i, "")
      .replace(/^\d+\.\s*/, "")
      .replace(/\s*\(.*\)\s*$/, "")
      .trim();

    // Get list of safe entries once
    const entries = await listEntries(zipPath);
    const allFiles = entries
      .filter((e) => !e.isDirectory)
      .map((e) => e.name)
      .filter((p) => isSafeEntryName(p));

    // detect languages from folders
    const langs = new Set();
    for (const p of allFiles) {
      const l = detectLanguageFromPath(p);
      if (l) langs.add(l);
    }

    const zipNameLangs = detectLanguagesFromZipName(zipName);
    const zipHasLangFolders = langs.size > 0;

    if (!zipHasLangFolders) {
      if (zipNameLangs.length) zipNameLangs.forEach((l) => langs.add(l));
      else langs.add("EN");
    }

    for (const lang of Array.from(langs).sort()) {
      const langName = LANG_MAP[lang] || lang;

      const langFiles = !zipHasLangFolders
        ? allFiles
        : allFiles.filter((p) => detectLanguageFromPath(p) === lang);

      const creditsPath = langFiles.find(isCreditsFile);
      const artworkPath = langFiles.find(isArtworkFile);

      const audioPaths = langFiles
        .filter((p) => isAudioFile(p) && isAudioFolderPath(p))
        .sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }));

      if (!creditsPath) jobLog(job, `[${lang}] No Credits .xlsx found (will default titles/credits)`);

      let creditsRows = [];
      if (creditsPath) {
        jobLog(job, `[${lang}] Reading credits sheet… ${creditsPath}`);
        const buf = await readEntryToBuffer({
          zipPath,
          entryName: creditsPath,
        });
        creditsRows = await parseCreditsXlsx(buf);
        bump(0.25);
      }

      const filteredCredits =
        creditsRows.some((r) => r.language)
          ? creditsRows.filter((r) => (r.language || "").toUpperCase() === lang)
          : creditsRows;

      const albumTitle = (filteredCredits[0]?.albumTitle || "").trim();
      if (lang === "EN") {
        englishTitle = albumTitle || projectNameGuess || projectCode;
      }

      const artistsRaw = (filteredCredits[0]?.artist || "").trim();
      const artistsArr = splitArtistsArray(artistsRaw);
      const primaryArtist = artistsArr[0] || DEFAULT_PRIMARY_ARTIST;
      const additionalArtists = artistsArr.slice(1);

      const trackCount = Math.max(audioPaths.length, filteredCredits.length || 0);
      const productFormat = inferFormat(trackCount);

      const productCode = `${projectCode}${lang}`;

      const cLine = year ? `${year} ${DEFAULT_LINE_ENTITY}` : "";
      const pLine = year ? `${year} ${DEFAULT_LINE_ENTITY}` : "";

      // -------------------------
      // Products row
      // -------------------------
      const prodRowObj = {};
      setByAnyHeader(prodRowObj, ["Project Code"], projectCode, productsHeaders);
      setByAnyHeader(prodRowObj, ["UPC"], "", productsHeaders);
      setByAnyHeader(prodRowObj, ["Manufacturers UPC"], "", productsHeaders);
      setByAnyHeader(prodRowObj, ["Product Code"], productCode, productsHeaders);
      setByAnyHeader(prodRowObj, ["Product Metadata Language"], langName, productsHeaders);

      setByAnyHeader(
        prodRowObj,
        ["Product name", "Product Name"],
        albumTitle || projectNameGuess || projectCode,
        productsHeaders
      );

      setByAnyHeader(prodRowObj, ["Product Version"], "", productsHeaders);
      setByAnyHeader(prodRowObj, ["Product Version Notes"], lang, productsHeaders);

      setByAnyHeader(prodRowObj, ["File Name"], `${productCode}.jpg`, productsHeaders);

      setByAnyHeader(prodRowObj, ["Primary Artist"], primaryArtist, productsHeaders);
      setByAnyHeader(prodRowObj, ["Primary Apple ID"], "", productsHeaders);
      setByAnyHeader(prodRowObj, ["Primary Spotify ID"], "", productsHeaders);

      setByAnyHeader(prodRowObj, ["Genre"], DEFAULT_GENRE, productsHeaders);
      setByAnyHeader(prodRowObj, ["Subgenre"], DEFAULT_SUBGENRE, productsHeaders);

      setByAnyHeader(prodRowObj, ["Format"], productFormat, productsHeaders);
      setByAnyHeader(prodRowObj, ["Imprint"], "Pinkfong", productsHeaders);

      setByAnyHeader(prodRowObj, ["(C) Line*", "(C) Line", "C Line"], cLine, productsHeaders);

      setByAnyHeader(prodRowObj, ["Special instructions"], DEFAULT_SPECIAL_INSTRUCTIONS, productsHeaders);
      setByAnyHeader(prodRowObj, ["Release Date"], releaseDate, productsHeaders);
      setByAnyHeader(prodRowObj, ["Sale Start Date"], releaseDate, productsHeaders);

      setByAnyHeader(
        prodRowObj,
        ["iTunes Pre-Order*", "iTunes Pre-order", "iTunes Pre-Order"],
        DEFAULT_ITUNES_PREORDER,
        productsHeaders
      );

      setByAnyHeader(prodRowObj, ["Album Pricing"], DEFAULT_ALBUM_PRICING, productsHeaders);
      setByAnyHeader(prodRowObj, ["Track Pricing"], DEFAULT_TRACK_PRICING, productsHeaders);

      setByAnyHeader(prodRowObj, ["NOT Cleared for Sale"], DEFAULT_NOT_CLEARED, productsHeaders);

      appendRowByHeaders(wsProducts, productsHeaders, productsHeaderRow, prodRowObj);

      // -------------------------
      // Product Contributors
      // -------------------------
      for (const name of additionalArtists) {
        appendRowByHeaders(wsProdContrib, prodContribHeaders, prodContribHeaderRow, {
          "Product Code": productCode,
          "Contributor Role": "Primary Artist",
          "Contributor Name": name,
          "Apple ID": "",
          "Spotify ID": "",
          "Featured to primary": "",
        });
      }

      appendRowByHeaders(wsProdContrib, prodContribHeaders, prodContribHeaderRow, {
        "Product Code": productCode,
        "Contributor Role": "Producer",
        "Contributor Name": DEFAULT_PRODUCER,
        "Apple ID": "",
        "Spotify ID": "",
        "Featured to primary": "",
      });

      // -------------------------
      // Tracks + Other Track Contributors
      // -------------------------
      const maxTracks = Math.max(audioPaths.length, filteredCredits.length || 0);

      for (let i = 1; i <= maxTracks; i++) {
        const credit =
          filteredCredits.find((r) => String(r.trackNo).trim() === String(i)) ||
          filteredCredits[i - 1] ||
          null;

        const trackName = (credit?.trackName || "").trim();
        const trackArtistsRaw = (credit?.artist || artistsRaw || "").trim();
        const trackArtistsArr = splitArtistsArray(trackArtistsRaw);

        const trackPrimaryArtist = trackArtistsArr[0] || primaryArtist;
        const trackExtraArtists = trackArtistsArr.slice(1);

        const isrc = (credit?.isrc || "").trim();

        const songwriter = (credit?.songwriter || "").trim() || DEFAULT_SONGWRITER;
        const producer = (credit?.producer || "").trim() || DEFAULT_PRODUCER;
        const publishers = (credit?.publishers || "").trim() || DEFAULT_PUBLISHERS;

        const trackRowObj = {};
        setByAnyHeader(trackRowObj, ["Product Code"], productCode, tracksHeaders);
        setByAnyHeader(trackRowObj, ["ISRC"], isrc, tracksHeaders);
        setByAnyHeader(trackRowObj, ["Volume"], 1, tracksHeaders);
        setByAnyHeader(trackRowObj, ["Track Number"], i, tracksHeaders);
        setByAnyHeader(trackRowObj, ["Track Name"], trackName, tracksHeaders);
        setByAnyHeader(trackRowObj, ["Track Version"], "", tracksHeaders);

        // ✅ Missing fields fixed (use header variants)
        setByAnyHeader(trackRowObj, ["Lyrics Language*", "Lyrics Language"], langName, tracksHeaders);
        setByAnyHeader(trackRowObj, ["Explicit Content*", "Explicit Content"], "No", tracksHeaders);

        setByAnyHeader(trackRowObj, ["Track Lyrics"], "", tracksHeaders);

        setByAnyHeader(trackRowObj, ["File Name"], `${productCode}_${i}.wav`, tracksHeaders);

        setByAnyHeader(trackRowObj, ["Primary Artist"], trackPrimaryArtist, tracksHeaders);
        setByAnyHeader(trackRowObj, ["Primary Artist Apple ID"], "", tracksHeaders);
        setByAnyHeader(trackRowObj, ["Primary Artist Spotify ID"], "", tracksHeaders);

        setByAnyHeader(trackRowObj, ["Songwriter"], songwriter, tracksHeaders);
        setByAnyHeader(trackRowObj, ["Producer"], producer, tracksHeaders);

        setByAnyHeader(
          trackRowObj,
          ["Primary Performer Legal Name*", "Primary Performer Legal Name"],
          DEFAULT_PRIMARY_ARTIST,
          tracksHeaders
        );
        setByAnyHeader(
          trackRowObj,
          ["Primary Performer Role*", "Primary Performer Role"],
          "Vocals - Lead Vocals",
          tracksHeaders
        );

        setByAnyHeader(trackRowObj, ["(P) Line*", "(P) Line", "P Line"], pLine, tracksHeaders);

        setByAnyHeader(
          trackRowObj,
          ["Ownership of Sound Recording", "Ownership of Sound Recording*"],
          "I am the original master copyright owner",
          tracksHeaders
        );

        setByAnyHeader(trackRowObj, ["Country of Recording*", "Country of Recording"], "South Korea", tracksHeaders);
        setByAnyHeader(
          trackRowObj,
          ["Nationality of Original Copyright Owner*", "Nationality of Original Copyright Owner"],
          "South Korea",
          tracksHeaders
        );
        setByAnyHeader(
          trackRowObj,
          ["US Publishing Obligation*", "US Publishing Obligation"],
          "100% controlled or administered by my label",
          tracksHeaders
        );

        setByAnyHeader(trackRowObj, ["Publishers"], publishers, tracksHeaders);

        appendRowByHeaders(wsTracks, tracksHeaders, tracksHeaderRow, trackRowObj);

        for (const name of trackExtraArtists) {
          appendRowByHeaders(wsOtherTrackContrib, otherTrackContribHeaders, otherTrackContribHeaderRow, {
            "Product Code": productCode,
            "Volume": 1,
            "Track Number": i,
            "Contributor Type": "Primary Artist",
            "Contributor Role": "Primary Artist",
            "Contributor Name": name,
            "Apple ID": "",
            "Spotify ID": "",
            "Featured to Primary": "",
          });
        }
      }

      // -------------------------
      // Extract assets to disk
      // -------------------------
      if (audioPaths.length) jobLog(job, `[${lang}] Extracting audio… (${audioPaths.length} files)`);

      for (let i = 0; i < audioPaths.length; i++) {
        const srcPath = audioPaths[i];
        const outName = `${productCode}_${i + 1}.wav`;

        jobLog(job, `[${lang}] -> ${srcPath} => ${outName}`);

        const outPath = path.join(extractedProjectDir, outName);

        await extractEntryToFile({
          zipPath,
          entryName: srcPath,
          outPath,
          onHeartbeat: ({ bytes, startedAt }) => {
            const mb = (bytes / (1024 * 1024)).toFixed(1);
            const secs = Math.round((Date.now() - startedAt) / 1000);
            jobLog(job, `[${lang}] ${outName} streaming… ${mb} MB (${secs}s)`);
          },
        });

        bump(1);
      }

      // Artwork -> JPG
      if (artworkPath) {
        jobLog(job, `[${lang}] Converting artwork -> JPG… ${artworkPath}`);
        const artBytes = await readEntryToBuffer({ zipPath, entryName: artworkPath });
        const jpeg = await toArtworkJpeg3000(artBytes);
        const artOutName = `${productCode}.jpg`;
        fs.writeFileSync(path.join(extractedProjectDir, artOutName), jpeg);
        bump(0.25);
      } else {
        jobLog(job, `[${lang}] No artwork found`);
      }

      jobLog(job, `[${lang}] Done: ${productCode} (tracks: ${trackCount}, format: ${productFormat})`);
    }
  }

  // -------------------------
  // Update Projects → Project Name to EN title
  // -------------------------
  if (englishTitle) {
    const nameCol = projectsHeaders[normHeader("Project Name")];
    const codeCol = projectsHeaders[normHeader("Project Code")];

    if (nameCol && codeCol) {
      for (let r = projectsHeaderRow + 1; r <= wsProjects.rowCount; r++) {
        const row = wsProjects.getRow(r);
        const codeVal = String(row.getCell(codeCol).value || "").trim();
        if (codeVal === projectCode) {
          row.getCell(nameCol).value = englishTitle;
          break;
        }
      }
    }
  }

  // -------------------------
  // Write workbook
  // -------------------------
  setProgress(job, 90);
  jobLog(job, "Writing workbook…");
  await orchardWb.xlsx.writeFile(job.workbookPath);

  // -------------------------
  // Zip extracted assets
  // -------------------------
  setProgress(job, 94);
  jobLog(job, "Zipping extracted assets folder…");
  await zipFolderToFile({
    folderPath: extractedProjectDir,
    outZipPath: job.assetsPath,
    job,
    jobLog,
    zipRootName: projectCode, // ensures PF321/ inside the zip
  });

  setProgress(job, 100);
  job.status = "done";
  jobLog(job, "✅ Build complete");
}