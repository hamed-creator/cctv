const WebSocket = require('ws');
const { performance } = require('perf_hooks');

const url = 'ws://localhost:8080/camera/1';
console.log(`Connecting to ${url}...`);

const ws = new WebSocket(url);
const frames = [];

ws.on('open', () => {
  console.log('Connected. Collecting frames for 15 seconds...');
  setTimeout(() => {
    ws.close();
    analyzeTrace();
  }, 15000);
});

ws.on('message', (data) => {
  const now = performance.now();
  // Simple check for IDR NAL unit type (5)
  const isKeyframe = data.length > 4 && (data[4] & 0x1f) === 5;
  frames.push({
    arrivalMs: now,
    size: data.length,
    isKeyframe
  });
});

ws.on('error', (err) => {
  console.error('WebSocket Error:', err.message);
});

function analyzeTrace() {
  if (frames.length === 0) {
    console.log('No frames received.');
    return;
  }

  console.log(`\n--- TRACE RESULTS ---`);
  console.log(`Total Frames: ${frames.length}`);
  
  const elapsed = (frames[frames.length - 1].arrivalMs - frames[0].arrivalMs) / 1000;
  console.log(`Duration: ${elapsed.toFixed(2)}s`);
  console.log(`Average FPS: ${(frames.length / elapsed).toFixed(2)}`);

  console.log('\n--- BURST ANALYSIS ---');
  let intervals = [];
  let burstCount = 0;
  let burstSizeTotal = 0;

  for (let i = 1; i < frames.length; i++) {
    const prev = frames[i - 1];
    const curr = frames[i];
    const interval = curr.arrivalMs - prev.arrivalMs;
    intervals.push(interval);

    if (interval < 5) {
      burstCount++;
      burstSizeTotal += curr.size;
    }
  }

  intervals.sort((a, b) => a - b);
  console.log(`Min interval: ${intervals[0].toFixed(2)}ms`);
  console.log(`Max interval: ${intervals[intervals.length - 1].toFixed(2)}ms`);
  console.log(`Median interval: ${intervals[Math.floor(intervals.length / 2)].toFixed(2)}ms`);
  console.log(`Frames in <5ms bursts: ${burstCount} (${((burstCount / frames.length) * 100).toFixed(1)}%)`);

  console.log('\n--- TIMELINE LOG (First 50 frames) ---');
  for (let i = 0; i < Math.min(50, frames.length); i++) {
    const f = frames[i];
    const delta = i === 0 ? 0 : f.arrivalMs - frames[i - 1].arrivalMs;
    console.log(`Frame ${i.toString().padStart(3, '0')} | +${delta.toFixed(1).padStart(6, ' ')}ms | Size: ${(f.size / 1024).toFixed(1).padStart(5, ' ')} KB | ${f.isKeyframe ? 'KEY' : '   '}`);
  }
}
