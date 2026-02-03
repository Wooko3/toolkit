// src/lib/zip.js
import fs from "fs";
import path from "path";
import yauzl from "yauzl";
import { isSafeEntryName, normalizeZipPath, mkdirp } from "./util.js";

export function openZip(zipPath) {
  return new Promise((resolve, reject) => {
    yauzl.open(
      zipPath,
      {
        lazyEntries: true,
        autoClose: false,

        // allow non-UTF8 / odd filenames (common in vendor zips)
        strictFileNames: false,

        // keeps yauzl stricter about entry size consistency
        validateEntrySizes: true,
      },
      (err, zipfile) => {
        if (err) return reject(err);
        resolve(zipfile);
      }
    );
  });
}

export async function listEntries(zipPath) {
  const zip = await openZip(zipPath);
  const entries = [];

  return await new Promise((resolve, reject) => {
    let done = false;

    const finish = (err) => {
      if (done) return;
      done = true;
      try {
        zip.close();
      } catch {}
      if (err) reject(err);
      else resolve(entries);
    };

    zip.readEntry();

    zip.on("entry", (entry) => {
      const name = normalizeZipPath(entry.fileName);

      if (!isSafeEntryName(name)) {
        zip.readEntry();
        return;
      }

      entries.push({
        name,
        isDirectory: /\/$/.test(name),
        uncompressedSize: entry.uncompressedSize,
        compressedSize: entry.compressedSize,
      });

      zip.readEntry();
    });

    zip.on("end", () => finish());
    zip.on("error", (e) => finish(e));
  });
}

// Stream an entry to a file (best for large audio files)
export async function extractEntryToFile({ zipPath, entryName, outPath, onHeartbeat, onBytes }) {
  const zip = await openZip(zipPath);

  return await new Promise((resolve, reject) => {
    let done = false;
    let found = false;

    const finish = (err, payload) => {
      if (done) return;
      done = true;
      try {
        zip.close();
      } catch {}
      if (err) reject(err);
      else resolve(payload);
    };

    const target = normalizeZipPath(entryName);

    zip.readEntry();

    zip.on("entry", (entry) => {
      const name = normalizeZipPath(entry.fileName);

      if (name !== target) {
        zip.readEntry();
        return;
      }

      if (!isSafeEntryName(name)) {
        return finish(new Error(`Unsafe entry name: ${entryName}`));
      }

      found = true;

      zip.openReadStream(entry, (err, readStream) => {
        if (err) return finish(err);

        mkdirp(path.dirname(outPath));

        const ws = fs.createWriteStream(outPath);

        let bytes = 0;
        const startedAt = Date.now();

        const t = setInterval(() => {
          onHeartbeat?.({ bytes, startedAt, entryName: name, outPath });
        }, 5000);

        const cleanup = () => clearInterval(t);

        readStream.on("data", (chunk) => {
          bytes += chunk.length;
          onBytes?.(chunk.length);
        });

        readStream.on("error", (e) => {
          cleanup();
          finish(e);
        });

        ws.on("error", (e) => {
          cleanup();
          finish(e);
        });

        ws.on("finish", () => {
          cleanup();
          finish(null, { bytes });
        });

        readStream.pipe(ws);
      });
    });

    zip.on("end", () => {
      if (!found) finish(new Error(`Entry not found in zip: ${entryName}`));
    });

    zip.on("error", (e) => finish(e));
  });
}

// Read an entry fully into a Buffer (credits xlsx / artwork png)
export async function readEntryToBuffer({ zipPath, entryName, onHeartbeat, onBytes }) {
  const zip = await openZip(zipPath);

  return await new Promise((resolve, reject) => {
    let done = false;
    let found = false;

    const finish = (err, payload) => {
      if (done) return;
      done = true;
      try {
        zip.close();
      } catch {}
      if (err) reject(err);
      else resolve(payload);
    };

    const target = normalizeZipPath(entryName);

    zip.readEntry();

    zip.on("entry", (entry) => {
      const name = normalizeZipPath(entry.fileName);

      if (name !== target) {
        zip.readEntry();
        return;
      }

      if (!isSafeEntryName(name)) {
        return finish(new Error(`Unsafe entry name: ${entryName}`));
      }

      found = true;

      zip.openReadStream(entry, (err, readStream) => {
        if (err) return finish(err);

        let bytes = 0;
        const startedAt = Date.now();
        const chunks = [];

        const t = setInterval(() => {
          onHeartbeat?.({ bytes, startedAt, entryName: name });
        }, 5000);

        const cleanup = () => clearInterval(t);

        readStream.on("data", (chunk) => {
          bytes += chunk.length;
          chunks.push(chunk);
          onBytes?.(chunk.length);
        });

        readStream.on("error", (e) => {
          cleanup();
          finish(e);
        });

        readStream.on("end", () => {
          cleanup();
          finish(null, Buffer.concat(chunks));
        });
      });
    });

    zip.on("end", () => {
      if (!found) finish(new Error(`Entry not found in zip: ${entryName}`));
    });

    zip.on("error", (e) => finish(e));
  });
}