// src/jobs/queue.js
let running = false;
const q = [];

export function enqueueJob(fn) {
  q.push(fn);
  drain();
}

async function drain() {
  if (running) return;
  const next = q.shift();
  if (!next) return;

  running = true;
  try {
    await next();
  } finally {
    running = false;
    drain();
  }
}