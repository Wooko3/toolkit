// src/lib/template.js
import fs from "fs";
import path from "path";
import JSZip from "jszip";
import { DOMParser, XMLSerializer } from "xmldom";

/**
 * Read the Orchard template .xlsx from disk and sanitize it so ExcelJS is less likely
 * to crash on comments / threaded comments / VML.
 */
export async function loadAndSanitizeTemplate(templatePath) {
  const raw = fs.readFileSync(templatePath);
  const sanitized = await sanitizeXlsxTemplate(raw);
  return sanitized; // Buffer
}

/**
 * Remove comments + their relationship entries (ExcelJS sometimes chokes on them).
 */
export async function sanitizeXlsxTemplate(buffer) {
  const zip = await JSZip.loadAsync(buffer);

  // Remove comment-related files
  Object.keys(zip.files).forEach((p) => {
    if (/^xl\/comments\d*\.xml$/i.test(p)) zip.remove(p);
    if (/^xl\/threadedComments\d*\.xml$/i.test(p)) zip.remove(p);
    if (/^xl\/persons\.xml$/i.test(p)) zip.remove(p);
    if (/^xl\/drawings\/vmlDrawing\d*\.vml$/i.test(p)) zip.remove(p);
  });

  const serializer = new XMLSerializer();
  const parser = new DOMParser();

  function scrubRelsXml(xmlText) {
    try {
      const doc = parser.parseFromString(xmlText, "application/xml");
      const rels = doc.getElementsByTagName("Relationship");
      const toRemove = [];

      for (let i = 0; i < rels.length; i++) {
        const node = rels[i];
        const type = (node.getAttribute("Type") || "").toLowerCase();
        const target = (node.getAttribute("Target") || "").toLowerCase();

        const isCommentRel =
          type.includes("/comments") ||
          type.includes("/threadedcomments") ||
          type.includes("/vml") ||
          target.includes("comments") ||
          target.includes("threadedcomments") ||
          target.includes("vmldrawing");

        if (isCommentRel) toRemove.push(node);
      }

      toRemove.forEach((n) => n.parentNode.removeChild(n));
      return serializer.serializeToString(doc);
    } catch {
      return xmlText;
    }
  }

  // Sheet rels
  const relPaths = Object.keys(zip.files).filter((p) =>
    /^xl\/worksheets\/_rels\/sheet\d+\.xml\.rels$/i.test(p)
  );

  for (const rp of relPaths) {
    const xml = await zip.files[rp].async("string");
    zip.file(rp, scrubRelsXml(xml));
  }

  // Workbook rels
  const wbRelsPath = "xl/_rels/workbook.xml.rels";
  if (zip.files[wbRelsPath]) {
    const xml = await zip.files[wbRelsPath].async("string");
    zip.file(wbRelsPath, scrubRelsXml(xml));
  }

  return await zip.generateAsync({ type: "nodebuffer" });
}