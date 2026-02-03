// src/jobs/logger.js
import fs from "fs";

export function createJobLogger(job) {
  return {
    log(msg) {
      const line = `[${new Date().toISOString()}] ${msg}`;
      // in-memory for frontend
      job.log.push(line);
      if (job.log.length > 2000) job.log.shift();

      // terminal
      process.stdout.write(line + "\n");

      // disk
      if (job.logPath) {
        try {
          fs.appendFileSync(job.logPath, line + "\n");
        } catch (e) {
          process.stdout.write(`[${new Date().toISOString()}] ❌ Failed writing job log: ${e?.message || e}\n`);
        }
      }
    }
  };
}