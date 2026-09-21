const WEBSOCKET_URL = `ws://${window.location.host || 'localhost:8080'}`;
const H264_CODEC = 'avc1.64001F';
const FRAME_DURATION_MICROSECONDS = Math.round(1000000 / 30);
const MAX_BUFFERED_FRAMES = 60;
const TARGET_BUFFER_MS = 250;

const canvas = document.querySelector('#video');
const context = canvas.getContext('2d');
const status = document.querySelector('#status');

if (!context) {
  throw new Error('A 2D canvas context is required');
}

if (!('VideoDecoder' in window) || !('EncodedVideoChunk' in window)) {
  status.textContent = 'This browser does not support the WebCodecs VideoDecoder API.';
  throw new Error('WebCodecs VideoDecoder is unavailable');
}

function nalTypesIn(data) {
  const types = [];

  for (let index = 0; index + 3 < data.length; index += 1) {
    if (data[index] !== 0 || data[index + 1] !== 0) {
      continue;
    }

    if (data[index + 2] === 1) {
      types.push(data[index + 3] & 0x1f);
      index += 2;
      continue;
    }

    if (data[index + 2] === 0 && data[index + 3] === 1 && index + 4 < data.length) {
      types.push(data[index + 4] & 0x1f);
      index += 3;
    }
  }

  return types;
}

let nextTimestamp = 0;
let needsKeyframe = true;
let renderedFrames = 0;
let receivedChunks = 0;
let decodedFrames = 0;
let fpsWindowStart = performance.now();

// Jitter buffer. FFmpeg delivers in bursts, so decoded frames are queued and
// released on a clock rather than all drawn inside one task.
const frameQueue = [];
let framePeriodMs = 1000 / 30;
let lastDrawTime = 0;

function drawFrame(frame) {
  if (canvas.width !== frame.displayWidth || canvas.height !== frame.displayHeight) {
    canvas.width = frame.displayWidth;
    canvas.height = frame.displayHeight;
  }

  context.drawImage(frame, 0, 0, canvas.width, canvas.height);
  frame.close();
  renderedFrames += 1;
}

function pumpFrameQueue() {
  requestAnimationFrame(pumpFrameQueue);

  if (frameQueue.length === 0) {
    return;
  }

  const now = performance.now();
  const bufferedMs = frameQueue.length * framePeriodMs;

  // Play out slightly fast when the buffer overfills, so bursts drain instead
  // of accumulating latency.
  const period = bufferedMs > TARGET_BUFFER_MS * 2 ? framePeriodMs / 2 : framePeriodMs;

  if (now - lastDrawTime < period) {
    return;
  }

  lastDrawTime = now;
  drawFrame(frameQueue.shift());
}

requestAnimationFrame(pumpFrameQueue);

function enqueueFrame(frame) {
  decodedFrames += 1;

  while (frameQueue.length >= MAX_BUFFERED_FRAMES) {
    frameQueue.shift().close();
  }

  frameQueue.push(frame);
}

function createDecoder() {
  const instance = new VideoDecoder({
    output: enqueueFrame,
    error(error) {
      console.error('VideoDecoder error:', error);
      resetDecoder();
    },
  });

  instance.configure({
    codec: H264_CODEC,
    optimizeForLatency: true,
    hardwareAcceleration: 'prefer-hardware',
  });

  return instance;
}

let decoder = createDecoder();

function resetDecoder() {
  try {
    if (decoder.state !== 'closed') {
      decoder.close();
    }
  } catch (error) {
    console.warn('Decoder close failed:', error.message);
  }

  while (frameQueue.length > 0) {
    frameQueue.shift().close();
  }

  decoder = createDecoder();
  needsKeyframe = true;
  status.textContent = 'Resynchronizing video...';
}

setInterval(() => {
  const now = performance.now();
  const elapsedSeconds = (now - fpsWindowStart) / 1000;
  const fps = renderedFrames / elapsedSeconds;
  const receiveRate = receivedChunks / elapsedSeconds;
  const decodeRate = decodedFrames / elapsedSeconds;

  // Track the real stream rate so the jitter buffer paces to it.
  if (decodeRate > 1) {
    framePeriodMs = (framePeriodMs * 3 + 1000 / decodeRate) / 4;
  }

  renderedFrames = 0;
  receivedChunks = 0;
  decodedFrames = 0;
  fpsWindowStart = now;

  if (!needsKeyframe && socket.readyState === WebSocket.OPEN) {
    status.textContent = `${fps.toFixed(1)} FPS drawn | ${receiveRate.toFixed(1)} recv/s `
      + `| ${decodeRate.toFixed(1)} dec/s | buffer ${frameQueue.length}`;
  }
}, 1000);

const socket = new WebSocket(WEBSOCKET_URL);
socket.binaryType = 'arraybuffer';

socket.addEventListener('open', () => {
  status.textContent = 'Connected. Waiting for video...';
});

socket.addEventListener('message', (event) => {
  if (!(event.data instanceof ArrayBuffer) || decoder.state !== 'configured') {
    return;
  }

  const data = new Uint8Array(event.data);

  try {
    receivedChunks += 1;

    const types = nalTypesIn(data);
    const isKeyframe = types.includes(5) || types.includes(7);

    // VideoDecoder rejects delta frames until it has decoded an IDR frame.
    if (needsKeyframe && !isKeyframe) {
      return;
    }

    decoder.decode(new EncodedVideoChunk({
      type: isKeyframe ? 'key' : 'delta',
      timestamp: nextTimestamp,
      data,
    }));

    nextTimestamp += FRAME_DURATION_MICROSECONDS;
    needsKeyframe = false;
  } catch (error) {
    console.error('Unable to decode chunk:', error, 'bytes:', data.length);
    resetDecoder();
  }
});

socket.addEventListener('close', () => {
  status.textContent = 'WebSocket disconnected.';
});

socket.addEventListener('error', () => {
  status.textContent = 'WebSocket connection failed.';
});