// src/lib/util.js
import path from "path";
import fs from "fs";

export function normalizeZipPath(p) {
  // IMPORTANT:
  // - convert backslashes
  // - strip any leading slashes so yauzl doesn't complain / we don't treat entries as absolute
  return String(p || "")
    .replace(/\\/g, "/")
    .replace(/^\/+/, "");
}

export function isSafeEntryName(name) {
  const p = normalizeZipPath(name);

  if (!p) return false;
  if (p === "." || p === "./") return false;

  // after normalizeZipPath, this should never happen, but keep it anyway
  if (p.startsWith("/")) return false;

  // traversal
  if (p.includes("..")) return false;

  // Windows drive
  if (/^[a-zA-Z]:\//.test(p)) return false;

  return true;
}

export function detectLanguageFromPath(filePath) {
  const p = normalizeZipPath(filePath);
  const parts = p.split("/").filter(Boolean);
  if (parts.length >= 2 && /^[A-Za-z]{2,3}$/.test(parts[0])) return parts[0].toUpperCase();
  return null;
}

// If zip is like "... (EN, ES, KO ...).zip" this finds LANGs; for single lang FR.zip, EN.zip works too.
export function detectLanguagesFromZipName(zipName) {
  const up = String(zipName || "").toUpperCase();
  const inParens = up.match(/\(([^)]+)\)/);
  if (inParens) {
    return inParens[1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
      .filter((s) => /^[A-Z]{2,3}$/.test(s));
  }
  const m = up.match(/\b([A-Z]{2,3})\b/);
  return m ? [m[1]] : [];
}

export function isAudioFile(p) {
  return /\.(wav|wave|aif|aiff)$/i.test(p);
}
export function isArtworkFile(p) {
  return /\.(png|jpe?g)$/i.test(p);
}
export function isCreditsFile(p) {
  return /credits\.(xlsx|xls)$/i.test(p);
}
export function isAudioFolderPath(p) {
  const norm = normalizeZipPath(p).toLowerCase();
  return /\/(audio|audio file|audio files)\//i.test(norm);
}

export function mkdirp(dir) {
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

// Safe join (prevents writing outside baseDir)
export function safeJoin(baseDir, rel) {
  const cleaned = String(rel || "").replace(/^\/+/, "");
  const out = path.join(baseDir, cleaned);
  const resolvedBase = path.resolve(baseDir);
  const resolvedOut = path.resolve(out);
  if (!resolvedOut.startsWith(resolvedBase + path.sep) && resolvedOut !== resolvedBase) {
    throw new Error(`Unsafe output path: ${rel}`);
  }
  return out;
}