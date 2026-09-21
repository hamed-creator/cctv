'use strict';

require('dotenv').config();

const fs = require('fs');
const http = require('http');
const path = require('path');
const { EventEmitter } = require('events');
const { spawn } = require('child_process');
const { WebSocketServer } = require('ws');
const bundledFfmpegPath = require('ffmpeg-static');

const PORT = Number(process.env.PORT || 8080);

// How long a camera's FFmpeg process lingers after its last client leaves.
// Non-zero so a browser refresh does not thrash the process.
const IDLE_SHUTDOWN_MS = Number(process.env.IDLE_SHUTDOWN_MS || 15000);

// If FFmpeg produces no bytes for this long, assume the RTSP session is wedged
// and restart it. Version-independent alternative to -stimeout/-timeout.
const DATA_WATCHDOG_MS = Number(process.env.DATA_WATCHDOG_MS || 10000);
const RTSP_ANALYZE_DURATION = process.env.RTSP_ANALYZE_DURATION || '1000000';
const RTSP_PROBE_SIZE = process.env.RTSP_PROBE_SIZE || '1000000';
const RTSP_BUFFER_SIZE = process.env.RTSP_BUFFER_SIZE || '32M';

const RESTART_BASE_MS = 1000;
const RESTART_MAX_MS = 30000;
const SIGKILL_GRACE_MS = 3000;

// Per-client send buffer ceiling. Past this, delta frames are dropped for that
// client so one slow browser cannot balloon server memory.
const CLIENT_BUFFER_LIMIT_BYTES = 2 * 1024 * 1024;

const ffmpegPath = process.env.FFMPEG_PATH || bundledFfmpegPath;
if (!ffmpegPath) {
  throw new Error('FFmpeg is unavailable. Install FFmpeg or set FFMPEG_PATH.');
}

/* ------------------------------------------------------------------------- *
 * Configuration
 * ------------------------------------------------------------------------- */

/**
 * Cameras come from cameras.json if present, otherwise from RTSP_URL_<id>
 * environment variables, otherwise from a single legacy RTSP_URL.
 */
function loadCameraConfig() {
  const configPath = process.env.CAMERAS_CONFIG || path.join(__dirname, 'cameras.json');

  if (fs.existsSync(configPath)) {
    const parsed = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    if (!Array.isArray(parsed) || parsed.length === 0) {
      throw new Error(`${configPath} must contain a non-empty array of cameras`);
    }
    return parsed.map((entry) => ({
      id: String(entry.id),
      name: entry.name || `Camera ${entry.id}`,
      url: entry.url,
    }));
  }

  const fromEnv = Object.keys(process.env)
    .filter((key) => /^RTSP_URL_\d+$/.test(key))
    .map((key) => ({
      id: key.replace('RTSP_URL_', ''),
      name: process.env[`CAMERA_NAME_${key.replace('RTSP_URL_', '')}`]
        || `Camera ${key.replace('RTSP_URL_', '')}`,
      url: process.env[key],
    }))
    .sort((a, b) => Number(a.id) - Number(b.id));

  if (fromEnv.length > 0) {
    return fromEnv;
  }

  if (process.env.RTSP_URL) {
    return [{ id: '1', name: 'Camera 1', url: process.env.RTSP_URL }];
  }

  throw new Error('No cameras configured. Provide cameras.json or RTSP_URL_1..N.');
}

/* ------------------------------------------------------------------------- *
 * Annex B demuxer
 *
 * Splits a raw H.264 elementary stream into complete access units (one coded
 * picture each), caching parameter sets so every keyframe is self-contained.
 * ------------------------------------------------------------------------- */

class AnnexBDemuxer extends EventEmitter {
  constructor() {
    super();
    this.reset();
  }

  reset() {
    this.pending = Buffer.alloc(0);
    this.accessUnit = [];
    this.containsVideoSlice = false;
    this.sps = null;
    this.pps = null;
  }

  static startCodeLength(nalUnit) {
    return nalUnit[2] === 1 ? 3 : 4;
  }

  static nalType(nalUnit) {
    return nalUnit[AnnexBDemuxer.startCodeLength(nalUnit)] & 0x1f;
  }

  /**
   * Next start code at or after `offset`. Offset must be >= 3 when the buffer
   * already begins with one, or a four-byte start code matches its own
   * trailing three bytes.
   */
  static findStartCode(buffer, offset) {
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

  /** Leading ue(v) of the slice header, so multi-slice pictures stay together. */
  static firstMbInSlice(nalUnit) {
    const offset = AnnexBDemuxer.startCodeLength(nalUnit) + 1;
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

  push(chunk) {
    this.pending = this.pending.length === 0
      ? chunk
      : Buffer.concat([this.pending, chunk]);

    let index;
    while ((index = AnnexBDemuxer.findStartCode(this.pending, 3)) !== -1) {
      this.#appendNalUnit(this.pending.subarray(0, index));
      this.pending = this.pending.subarray(index);
    }
  }

  #appendNalUnit(nalUnit) {
    if (nalUnit.length <= 4) {
      return;
    }

    const type = AnnexBDemuxer.nalType(nalUnit);

    if (type === 7) {
      this.sps = Buffer.from(nalUnit);
    }
    if (type === 8) {
      this.pps = Buffer.from(nalUnit);
    }

    const isVideoSlice = type === 1 || type === 5;
    const startsPicture = (type === 9 && this.containsVideoSlice)
      || (isVideoSlice && this.containsVideoSlice
        && AnnexBDemuxer.firstMbInSlice(nalUnit) === 0);

    if (startsPicture) {
      this.#flush();
    }

    this.accessUnit.push(nalUnit);
    this.containsVideoSlice ||= isVideoSlice;
  }

  #flush() {
    if (this.accessUnit.length === 0) {
      return;
    }

    const types = this.accessUnit.map(AnnexBDemuxer.nalType);
    const keyframe = types.includes(5);
    let unit = this.accessUnit;

    // Every IDR carries its parameter sets, so a client joining mid-stream or
    // recovering from a decoder error can configure immediately.
    if (keyframe && !types.includes(7) && this.sps && this.pps) {
      unit = [this.sps, this.pps, ...this.accessUnit];
    }

    this.accessUnit = [];
    this.containsVideoSlice = false;

    this.emit('access-unit', Buffer.concat(unit), keyframe || types.includes(7));
  }
}

/* ------------------------------------------------------------------------- *
 * Camera
 *
 * Owns one RTSP source: its FFmpeg child process, its demuxer, its client set,
 * and its lifecycle. Starts on first client, stops after the last one leaves.
 * ------------------------------------------------------------------------- */

const CameraState = Object.freeze({
  IDLE: 'idle',
  STARTING: 'starting',
  STREAMING: 'streaming',
  RETRYING: 'retrying',
  STOPPING: 'stopping',
});

class Camera extends EventEmitter {
  constructor({ id, name, url }) {
    super();

    if (!url) {
      throw new Error(`Camera ${id} has no RTSP URL`);
    }

    this.id = id;
    this.name = name;
    this.url = url;

    this.clients = new Set();
    this.state = CameraState.IDLE;

    this.process = null;
    this.demuxer = null;
    this.lastKeyframe = null;

    this.idleTimer = null;
    this.restartTimer = null;
    this.watchdogTimer = null;
    this.restartAttempts = 0;

    this.stats = {
      accessUnits: 0,
      keyframes: 0,
      bytes: 0,
      startedAt: null,
      lastDataAt: null,
    };
  }

  get clientCount() {
    return this.clients.size;
  }

  /** Redacts credentials so URLs are safe to log. */
  get safeUrl() {
    return this.url.replace(/\/\/[^@/]+@/, '//***@');
  }

  addClient(socket) {
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }

    this.clients.add(socket);

    socket.on('close', () => this.removeClient(socket));
    socket.on('error', () => this.removeClient(socket));

    socket.send(JSON.stringify({
      type: 'hello',
      cameraId: this.id,
      name: this.name,
      state: this.state,
    }));

    // Prime late joiners with the most recent keyframe so they see a picture
    // without waiting a full GOP.
    if (this.lastKeyframe) {
      socket.send(this.lastKeyframe);
    }

    this.log(`client connected (${this.clients.size} total)`);
    this.start();
  }

  removeClient(socket) {
    if (!this.clients.delete(socket)) {
      return;
    }

    this.log(`client disconnected (${this.clients.size} remaining)`);

    if (this.clients.size === 0) {
      this.#scheduleIdleShutdown();
    }
  }

  #scheduleIdleShutdown() {
    if (this.idleTimer || this.state === CameraState.IDLE) {
      return;
    }

    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      if (this.clients.size === 0) {
        this.log(`idle for ${IDLE_SHUTDOWN_MS} ms; releasing FFmpeg`);
        this.stop();
      }
    }, IDLE_SHUTDOWN_MS);
  }

  start() {
    if (this.process || this.state === CameraState.STARTING) {
      return;
    }
    if (this.restartTimer) {
      return;
    }
    if (this.clients.size === 0) {
      return;
    }

    this.#setState(CameraState.STARTING);
    this.#spawnFfmpeg();
  }

  #ffmpegArgs() {
    return [
      '-hide_banner',
      '-loglevel', 'warning',
      '-rtsp_transport', 'tcp',
      '-rtbufsize', RTSP_BUFFER_SIZE,
      '-analyzeduration', RTSP_ANALYZE_DURATION,
      '-probesize', RTSP_PROBE_SIZE,
      '-i', this.url,
      '-an',
      '-c:v', 'copy',
      '-bsf:v', 'dump_extra=freq=keyframe',
      '-f', 'h264',
      '-flush_packets', '1',
      // Disables FFmpeg's 32 KB AVIO write buffer. Without it, a low-bitrate
      // substream only reaches Node every few seconds.
      '-avioflags', 'direct',
      '-muxdelay', '0',
      '-muxpreload', '0',
      'pipe:1',
    ];
  }

  #spawnFfmpeg() {
    this.log(`connecting to ${this.safeUrl}`);

    const child = spawn(ffmpegPath, this.#ffmpegArgs(), {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    this.process = child;
    this.stats.startedAt = Date.now();

    this.demuxer = new AnnexBDemuxer();
    this.demuxer.on('access-unit', (buffer, keyframe) => {
      this.stats.accessUnits += 1;
      if (keyframe) {
        this.stats.keyframes += 1;
        this.lastKeyframe = buffer;
      }
      this.broadcast(buffer, keyframe);
    });

    child.stdout.on('data', (chunk) => {
      if (this.state !== CameraState.STREAMING) {
        this.#setState(CameraState.STREAMING);
        this.restartAttempts = 0;
      }

      this.stats.bytes += chunk.length;
      this.stats.lastDataAt = Date.now();
      this.#kickWatchdog();
      this.demuxer.push(chunk);
    });

    child.stderr.on('data', (chunk) => {
      const text = chunk.toString().trim();
      if (text) {
        this.log(`ffmpeg: ${text}`, 'warn');
      }
    });

    child.on('error', (error) => {
      this.log(`failed to spawn FFmpeg: ${error.message}`, 'error');
      this.#handleProcessExit();
    });

    child.on('exit', (code, signal) => {
      this.log(`FFmpeg exited (code=${code}, signal=${signal})`, 'warn');
      this.#handleProcessExit();
    });

    this.#kickWatchdog();
  }

  #kickWatchdog() {
    if (this.watchdogTimer) {
      clearTimeout(this.watchdogTimer);
    }

    this.watchdogTimer = setTimeout(() => {
      this.log(`no data for ${DATA_WATCHDOG_MS} ms; restarting FFmpeg`, 'warn');
      this.#killProcess();
    }, DATA_WATCHDOG_MS);
  }

  #handleProcessExit() {
    this.#clearTimers();
    this.process = null;
    this.demuxer = null;

    if (this.state === CameraState.STOPPING || this.clients.size === 0) {
      this.#setState(CameraState.IDLE);
      return;
    }

    this.#scheduleRestart();
  }

  #scheduleRestart() {
    if (this.restartTimer) {
      return;
    }

    const delay = Math.min(
      RESTART_BASE_MS * (2 ** this.restartAttempts),
      RESTART_MAX_MS,
    );
    this.restartAttempts += 1;

    this.#setState(CameraState.RETRYING);
    this.log(`retrying in ${delay} ms (attempt ${this.restartAttempts})`, 'warn');

    this.restartTimer = setTimeout(() => {
      this.restartTimer = null;
      if (this.clients.size > 0) {
        this.start();
      } else {
        this.#setState(CameraState.IDLE);
      }
    }, delay);
  }

  #clearTimers() {
    for (const key of ['watchdogTimer', 'restartTimer', 'idleTimer']) {
      if (this[key]) {
        clearTimeout(this[key]);
        this[key] = null;
      }
    }
  }

  #killProcess() {
    const child = this.process;
    if (!child) {
      return;
    }

    child.kill('SIGTERM');

    // FFmpeg occasionally ignores SIGTERM while blocked on a dead RTSP socket.
    const hardKill = setTimeout(() => {
      if (!child.killed || child.exitCode === null) {
        child.kill('SIGKILL');
      }
    }, SIGKILL_GRACE_MS);

    child.once('exit', () => clearTimeout(hardKill));
  }

  /** Graceful teardown. Frees the FFmpeg process, its pipes and its buffers. */
  stop() {
    this.#setState(CameraState.STOPPING);
    this.#clearTimers();
    this.restartAttempts = 0;
    this.lastKeyframe = null;

    if (this.demuxer) {
      this.demuxer.removeAllListeners();
      this.demuxer.reset();
      this.demuxer = null;
    }

    if (this.process) {
      this.#killProcess();
    } else {
      this.#setState(CameraState.IDLE);
    }
  }

  broadcast(buffer, keyframe) {
    for (const client of this.clients) {
      if (client.readyState !== client.OPEN) {
        continue;
      }

      // Shed delta frames for clients that cannot keep up; always let a
      // keyframe through so they can resynchronise.
      if (!keyframe && client.bufferedAmount > CLIENT_BUFFER_LIMIT_BYTES) {
        continue;
      }

      client.send(buffer);
    }
  }

  #setState(state) {
    if (this.state === state) {
      return;
    }
    this.state = state;
    this.emit('state', state);
  }

  toJSON() {
    return {
      id: this.id,
      name: this.name,
      state: this.state,
      clients: this.clients.size,
      accessUnits: this.stats.accessUnits,
      keyframes: this.stats.keyframes,
      kilobytes: Math.round(this.stats.bytes / 1024),
      uptimeSeconds: this.stats.startedAt
        ? Math.round((Date.now() - this.stats.startedAt) / 1000)
        : 0,
    };
  }

  log(message, level = 'log') {
    console[level](`[camera ${this.id}] ${message}`);
  }
}

/* ------------------------------------------------------------------------- *
 * CameraManager
 * ------------------------------------------------------------------------- */

class CameraManager {
  constructor(config) {
    this.cameras = new Map();

    for (const entry of config) {
      this.cameras.set(entry.id, new Camera(entry));
    }
  }

  get(id) {
    return this.cameras.get(id);
  }

  list() {
    return [...this.cameras.values()].map((camera) => camera.toJSON());
  }

  get activeProcessCount() {
    return [...this.cameras.values()].filter((camera) => camera.process).length;
  }

  attach(id, socket) {
    const camera = this.get(id);
    if (!camera) {
      socket.close(4404, `Unknown camera ${id}`);
      return false;
    }
    camera.addClient(socket);
    return true;
  }

  shutdown() {
    for (const camera of this.cameras.values()) {
      for (const client of camera.clients) {
        client.close(1001, 'Server shutting down');
      }
      camera.stop();
    }
  }
}

/* ------------------------------------------------------------------------- *
 * HTTP + WebSocket wiring
 * ------------------------------------------------------------------------- */

const manager = new CameraManager(loadCameraConfig());

const STATIC_FILES = new Map([
  ['index.html', 'text/html; charset=utf-8'],
  ['app.js', 'text/javascript; charset=utf-8'],
]);

const httpServer = http.createServer((request, response) => {
  const url = new URL(request.url, `http://${request.headers.host}`);

  if (url.pathname === '/api/cameras') {
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify(manager.list(), null, 2));
    return;
  }

  if (url.pathname === '/healthz') {
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify({
      ok: true,
      cameras: manager.cameras.size,
      activeProcesses: manager.activeProcessCount,
    }));
    return;
  }

  const fileName = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
  const contentType = STATIC_FILES.get(fileName);

  if (!contentType) {
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
    response.writeHead(200, { 'Content-Type': contentType });
    response.end(file);
  });
});

// noServer mode so we can route by path before completing the handshake.
const webSocketServer = new WebSocketServer({ noServer: true });
const CAMERA_PATH = /^\/camera\/([A-Za-z0-9_-]+)$/;

httpServer.on('upgrade', (request, socket, head) => {
  const { pathname } = new URL(request.url, `http://${request.headers.host}`);
  const match = CAMERA_PATH.exec(pathname);

  if (!match) {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }

  const cameraId = match[1];

  if (!manager.get(cameraId)) {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }

  webSocketServer.handleUpgrade(request, socket, head, (client) => {
    client.binaryType = 'nodebuffer';
    manager.attach(cameraId, client);
  });
});

// Drop half-open connections so idle shutdown actually fires.
const heartbeat = setInterval(() => {
  for (const client of webSocketServer.clients) {
    if (client.isAlive === false) {
      client.terminate();
      continue;
    }
    client.isAlive = false;
    client.ping();
  }
}, 30000);

webSocketServer.on('connection', (client) => {
  client.isAlive = true;
  client.on('pong', () => {
    client.isAlive = true;
  });
});

const statsTimer = setInterval(() => {
  const active = manager.list().filter((camera) => camera.state !== 'idle');
  if (active.length === 0) {
    return;
  }
  console.log('[stats]', active.map((camera) => (
    `${camera.id}:${camera.state}/${camera.clients}c/${camera.accessUnits}au`
  )).join(' '));

  for (const camera of manager.cameras.values()) {
    camera.stats.accessUnits = 0;
    camera.stats.keyframes = 0;
    camera.stats.bytes = 0;
  }
}, 10000);

httpServer.listen(PORT, () => {
  console.log(`Listening on http://localhost:${PORT}`);
  console.log(`Cameras configured: ${[...manager.cameras.keys()].join(', ')}`);
  console.log('Connect a client to ws://localhost:%d/camera/<id>', PORT);
});

let shuttingDown = false;

function shutdown(signal) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;

  console.log(`\n${signal} received; shutting down`);
  clearInterval(heartbeat);
  clearInterval(statsTimer);
  manager.shutdown();
  httpServer.close();

  setTimeout(() => process.exit(0), SIGKILL_GRACE_MS + 500).unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

module.exports = {
  AnnexBDemuxer, Camera, CameraManager, CameraState, manager,
};