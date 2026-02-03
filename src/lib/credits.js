// src/lib/credits.js
import ExcelJS from "exceljs";

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

export async function parseCreditsXlsx(buf) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buf);

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