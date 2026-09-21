require('dotenv').config();

const fs = require('fs');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');
const ffmpeg = require('fluent-ffmpeg');
const bundledFfmpegPath = require('ffmpeg-static');

const PORT = 8080;
const RTSP_URL = process.env.RTSP_URL;
const RECONNECT_DELAY_MS = 5000;

if (!RTSP_URL) {
  throw new Error('RTSP_URL is not configured. Add it to .env or the process environment.');
}

const ffmpegPath = process.env.FFMPEG_PATH || bundledFfmpegPath;
if (!ffmpegPath) {
  throw new Error('FFmpeg is unavailable. Install FFmpeg or set FFMPEG_PATH.');
}

ffmpeg.setFfmpegPath(ffmpegPath);

const httpServer = http.createServer((request, response) => {
  const requestPath = new URL(request.url, `http://${request.headers.host}`).pathname;
  const fileName = requestPath === '/' ? 'index.html' : requestPath.slice(1);

  if (!['index.html', 'app.js'].includes(fileName)) {
    response.writeHead(404);
    response.end('Not found');
    return;
  }

  fs.readFile(path.join(__dirname, fileName), (error, file) => {
    if (error) {
      response.writeHead(404);
      response.end('Not found');
      return;
    }

    const contentType = fileName.endsWith('.js')
      ? 'text/javascript; charset=utf-8'
      : 'text/html; charset=utf-8';
    response.writeHead(200, { 'Content-Type': contentType });
    response.end(file);
  });
});

const webSocketServer = new WebSocketServer({ server: httpServer });

const audit = {
  accessUnits: 0,
  videoSlices: 0,
  dataEvents: 0,
  bytes: 0,
};

function broadcastAccessUnit(buffer) {
  if (!Buffer.isBuffer(buffer)) {
    throw new TypeError('broadcastAccessUnit expects a Buffer');
  }

  audit.accessUnits += 1;

  for (const client of webSocketServer.clients) {
    if (client.readyState === client.OPEN && client.bufferedAmount < 1024 * 1024) {
      client.send(buffer);
    }
  }
}

httpServer.on('listening', () => {
  console.log(`HTTP and WebSocket server listening on http://localhost:${PORT}`);
});

function startCodeLength(nalUnit) {
  return nalUnit[2] === 1 ? 3 : 4;
}

function nalType(nalUnit) {
  return nalUnit[startCodeLength(nalUnit)] & 0x1f;
}

/**
 * Parse first_mb_in_slice (the leading ue(v) of the slice header) so multi-slice
 * pictures are not mistaken for separate access units.
 */
function firstMbInSlice(nalUnit) {
  const offset = startCodeLength(nalUnit) + 1;
  const rbsp = [];
  let zeroCount = 0;

  for (let index = offset; index < nalUnit.length && rbsp.length < 8; index += 1) {
    const byte = nalUnit[index];
    if (zeroCount === 2 && byte === 3) {
      zeroCount = 0;
      continue;
    }
    rbsp.push(byte);
    zeroCount = byte === 0 ? zeroCount + 1 : 0;
  }

  let bitIndex = 0;
  const readBit = () => {
    if (bitIndex >= rbsp.length * 8) {
      return 1;
    }
    const bit = (rbsp[Math.floor(bitIndex / 8)] >> (7 - (bitIndex % 8))) & 1;
    bitIndex += 1;
    return bit;
  };

  let leadingZeroBits = 0;
  while (readBit() === 0 && leadingZeroBits < 31) {
    leadingZeroBits += 1;
  }

  let codeNum = (2 ** leadingZeroBits) - 1;
  for (let bit = leadingZeroBits - 1; bit >= 0; bit -= 1) {
    codeNum += readBit() * (2 ** bit);
  }

  return codeNum;
}

/**
 * Find the next Annex B start code at or after `offset`.
 * `offset` must be >= 3 when the buffer already begins with a start code,
 * otherwise a four-byte start code matches its own trailing three bytes.
 */
function findStartCode(buffer, offset) {
  for (let index = offset; index + 2 < buffer.length; index += 1) {
    if (buffer[index] !== 0 || buffer[index + 1] !== 0) {
      continue;
    }

    if (buffer[index + 2] === 1) {
      return index;
    }

    if (index + 3 < buffer.length && buffer[index + 2] === 0 && buffer[index + 3] === 1) {
      return index;
    }
  }

  return -1;
}

let reconnectTimer;
let activeCommand;

function scheduleReconnect() {
  if (reconnectTimer) {
    return;
  }

  console.log(`RTSP stream unavailable; retrying in ${RECONNECT_DELAY_MS} ms`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = undefined;
    startRtspCapture();
  }, RECONNECT_DELAY_MS);
}

function startRtspCapture() {
  if (activeCommand) {
    return;
  }

  console.log(`Connecting to RTSP stream: ${RTSP_URL}`);

  let stopped = false;

  function finish(reason) {
    if (stopped) {
      return;
    }
    stopped = true;
    if (activeCommand === command) {
      activeCommand = undefined;
    }
    console.warn(reason);
    scheduleReconnect();
  }

  const command = ffmpeg(RTSP_URL)
    .inputOptions([
      '-rtsp_transport tcp',
      '-fflags nobuffer',
      '-flags low_delay',
      '-analyzeduration 0',
      '-probesize 32',
    ])
    .outputOptions([
      '-c:v copy',
      '-an',
      '-bsf:v dump_extra=freq=keyframe',
      '-flush_packets 1',
      // Disables FFmpeg's 32 KB AVIO write buffer on the output pipe. Without
      // this, a low-bitrate substream only reaches Node every few seconds.
      '-avioflags direct',
      '-muxdelay 0',
      '-muxpreload 0',
    ])
    .format('h264')
    .on('start', (commandLine) => {
      console.log(`FFmpeg started: ${commandLine}`);
    })
    .on('stderr', (line) => {
      // Progress lines are noise; keep genuine warnings and errors.
      if (!line.startsWith('frame=') && !line.startsWith('size=')) {
        console.error(`FFmpeg: ${line}`);
      }
    })
    .on('error', (error) => finish(`FFmpeg capture failed: ${error.message}`))
    .on('end', () => finish('FFmpeg ended because the RTSP stream disconnected'));

  activeCommand = command;
  const outputStream = command.pipe();

  let pendingBuffer = Buffer.alloc(0);
  let accessUnit = [];
  let containsVideoSlice = false;
  let latestSps;
  let latestPps;

  function flushAccessUnit() {
    if (accessUnit.length === 0) {
      return;
    }

    const types = accessUnit.map(nalType);
    let unit = accessUnit;

    // Guarantee every IDR carries its parameter sets so late-joining clients
    // and post-error resyncs can configure the decoder.
    if (types.includes(5) && !types.includes(7) && latestSps && latestPps) {
      unit = [latestSps, latestPps, ...accessUnit];
    }

    broadcastAccessUnit(Buffer.concat(unit));
    accessUnit = [];
    containsVideoSlice = false;
  }

  function appendNalUnit(nalUnit) {
    if (nalUnit.length <= 4) {
      return;
    }

    const type = nalType(nalUnit);

    if (type === 7) {
      latestSps = Buffer.from(nalUnit);
    }
    if (type === 8) {
      latestPps = Buffer.from(nalUnit);
    }

    const isVideoSlice = type === 1 || type === 5;
    if (isVideoSlice) {
      audit.videoSlices += 1;
    }

    // Access unit delimiter, or the first slice of the next picture.
    const startsPicture = (type === 9 && containsVideoSlice)
      || (isVideoSlice && containsVideoSlice && firstMbInSlice(nalUnit) === 0);

    if (startsPicture) {
      flushAccessUnit();
    }

    accessUnit.push(nalUnit);
    containsVideoSlice ||= isVideoSlice;
  }

  outputStream.on('data', (chunk) => {
    audit.dataEvents += 1;
    audit.bytes += chunk.length;

    pendingBuffer = pendingBuffer.length === 0
      ? chunk
      : Buffer.concat([pendingBuffer, chunk]);

    let nextStartCodeIndex;
    while ((nextStartCodeIndex = findStartCode(pendingBuffer, 3)) !== -1) {
      appendNalUnit(pendingBuffer.subarray(0, nextStartCodeIndex));
      pendingBuffer = pendingBuffer.subarray(nextStartCodeIndex);
    }
  });

  outputStream.on('error', (error) => finish(`FFmpeg output stream failed: ${error.message}`));
  outputStream.on('close', () => finish('FFmpeg output stream closed'));
}

httpServer.listen(PORT);
startRtspCapture();

setInterval(() => {
  console.log(
    `[AUDIT] ${audit.accessUnits} AU/s | ${audit.videoSlices} slices/s | `
    + `${audit.dataEvents} pipe reads/s | ${(audit.bytes / 1024).toFixed(1)} KB/s`,
  );
  audit.accessUnits = 0;
  audit.videoSlices = 0;
  audit.dataEvents = 0;
  audit.bytes = 0;
}, 1000);

module.exports = { broadcastAccessUnit, webSocketServer };