/**
 * CameraStream
 *
 * One instance drives one <canvas> from one WebSocket endpoint. All decoder,
 * jitter buffer and resync state lives on the instance, so any number of
 * instances can coexist on a page without interfering.
 *
 * Usage:
 *   const stream = new CameraStream('ws://host/camera/1', canvasElement);
 *   stream.start();
 *   ...
 *   stream.destroy();
 */

const DEFAULTS = {
  codec: 'avc1.640029',
  nominalFps: 30,
  maxBufferedFrames: 60,
  targetBufferMs: 250,
  reconnectBaseMs: 1000,
  reconnectMaxMs: 15000,
  autoReconnect: true,
  // Frame with no new decoded output for this long while 'streaming' is
  // reported as 'buffering' rather than silently stalling.
  stallThresholdMs: 800,
  // Number of initial inter-arrival samples averaged with equal weight before
  // switching to a slower steady-state EMA. Keeps startup pacing converging
  // in roughly this many frames instead of several seconds.
  bootstrapSamples: 10,
  steadyStateAlpha: 0.15,
  // Consecutive decoder failures before giving up and reporting 'unsupported'
  // instead of resetting again.
  maxConsecutiveDecoderErrors: 5,
};

export class CameraStream extends EventTarget {
  static isSupported() {
    return typeof window !== 'undefined'
      && 'VideoDecoder' in window
      && 'EncodedVideoChunk' in window;
  }

  constructor(url, canvas, options = {}) {
    super();

    if (!url) {
      throw new Error('CameraStream requires a WebSocket URL');
    }
    if (!canvas || !(canvas instanceof HTMLCanvasElement)) {
      throw new Error('CameraStream requires a target <canvas> element');
    }

    this.url = url;
    this.canvas = canvas;
    this.options = { ...DEFAULTS, ...options };

    this.context = canvas.getContext('2d');
    if (!this.context) {
      throw new Error('A 2D canvas context is required');
    }

    // --- Encapsulated per-instance state -----------------------------------
    this.socket = null;
    this.decoder = null;
    this.sps = null;
    this.pps = null;
    this.codecDescription = null;
    this.frameQueue = [];
    this.needsKeyframe = true;
    this.nextTimestamp = 0;

    this.destroyed = false;
    this.running = false;
    this.reconnectAttempts = 0;
    this.reconnectTimer = null;
    this.rafHandle = null;
    this.statsTimer = null;

    this.framePeriodMs = 1000 / this.options.nominalFps;
    this.lastDrawTime = 0;

    // Per-frame arrival pacing (replaces the old once-per-second estimate).
    this._lastFrameArrival = null;
    this._arrivalSampleCount = 0;

    // Stall detection.
    this._lastActivityAt = 0;

    this._consecutiveDecoderErrors = 0;

    this.stats = {
      fps: 0,
      receivedPerSecond: 0,
      decodedPerSecond: 0,
      bufferedFrames: 0,
      state: 'idle',
    };

    this._counters = { rendered: 0, received: 0, decoded: 0 };
    this._windowStart = 0;

    // Bound once so removeEventListener and cancelAnimationFrame work.
    this._onOpen = this._onOpen.bind(this);
    this._onMessage = this._onMessage.bind(this);
    this._onClose = this._onClose.bind(this);
    this._onSocketError = this._onSocketError.bind(this);
    this._pump = this._pump.bind(this);
    this._onDecodedFrame = this._onDecodedFrame.bind(this);
    this._onDecoderError = this._onDecoderError.bind(this);
    this._sampleStats = this._sampleStats.bind(this);
  }

  /* --------------------------------------------------------------------- *
   * Lifecycle
   * --------------------------------------------------------------------- */

  start() {
    if (this.destroyed) {
      throw new Error('CameraStream has been destroyed');
    }
    if (this.running) {
      return this;
    }
    if (!CameraStream.isSupported()) {
      this._setState('unsupported');
      return this;
    }

    this.running = true;
    this._windowStart = performance.now();
    this._lastActivityAt = this._windowStart;

    // The decoder is created after the first access unit supplies SPS/PPS.
    this._connect();

    this.rafHandle = requestAnimationFrame(this._pump);
    this.statsTimer = setInterval(this._sampleStats, 1000);

    return this;
  }

  /**
   * Full teardown. Closes the socket, closes the decoder, and explicitly
   * closes every VideoFrame still held in the jitter buffer — VideoFrames hold
   * GPU-side memory that garbage collection will not reclaim on its own.
   */
  destroy() {
    if (this.destroyed) {
      return;
    }

    this.destroyed = true;
    this.running = false;

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.statsTimer) {
      clearInterval(this.statsTimer);
      this.statsTimer = null;
    }
    if (this.rafHandle !== null) {
      cancelAnimationFrame(this.rafHandle);
      this.rafHandle = null;
    }

    this._teardownSocket(1000, 'Client closed stream');
    this._closeDecoder();
    this._drainFrameQueue();

    this._setState('destroyed');
  }

  /* --------------------------------------------------------------------- *
   * WebSocket
   * --------------------------------------------------------------------- */

  _connect() {
    if (this.destroyed || !this.running) {
      return;
    }

    this._setState('connecting');

    const socket = new WebSocket(this.url);
    socket.binaryType = 'arraybuffer';
    this.socket = socket;

    socket.addEventListener('open', this._onOpen);
    socket.addEventListener('message', this._onMessage);
    socket.addEventListener('close', this._onClose);
    socket.addEventListener('error', this._onSocketError);
  }

  _teardownSocket(code, reason) {
    const socket = this.socket;
    if (!socket) {
      return;
    }

    socket.removeEventListener('open', this._onOpen);
    socket.removeEventListener('message', this._onMessage);
    socket.removeEventListener('close', this._onClose);
    socket.removeEventListener('error', this._onSocketError);

    if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
      try {
        socket.close(code, reason);
      } catch {
        // Closing an already-closing socket is not an error worth surfacing.
      }
    }

    this.socket = null;
  }

  _onOpen() {
    this.reconnectAttempts = 0;
    this._setState('connected');
  }

  _onClose() {
    if (this.destroyed) {
      return;
    }
    this._setState('disconnected');
    this._scheduleReconnect();
  }

  _onSocketError() {
    if (this.destroyed) {
      return;
    }
    this._emit('error', { message: 'WebSocket connection failed' });
  }

  _scheduleReconnect() {
    if (!this.options.autoReconnect || this.destroyed || this.reconnectTimer) {
      return;
    }

    const delay = Math.min(
      this.options.reconnectBaseMs * (2 ** this.reconnectAttempts),
      this.options.reconnectMaxMs,
    );
    this.reconnectAttempts += 1;

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this._teardownSocket();
      this._resetDecoder();
      this._connect();
    }, delay);
  }

  _onMessage(event) {
    if (this.destroyed) {
      return;
    }

    // Control frames arrive as text; video as ArrayBuffer.
    if (typeof event.data === 'string') {
      try {
        this._emit('control', JSON.parse(event.data));
      } catch {
        // Ignore malformed control messages.
      }
      return;
    }

    if (!(event.data instanceof ArrayBuffer)) {
      return;
    }

    const data = new Uint8Array(event.data);
    const nalUnits = CameraStream.parseAnnexB(data);
    if (nalUnits.length === 0) {
      return;
    }

    if (!this.codecDescription) {
      this.sps ||= nalUnits.find((nalUnit) => (nalUnit[0] & 0x1f) === 7);
      this.pps ||= nalUnits.find((nalUnit) => (nalUnit[0] & 0x1f) === 8);
      if (this.sps && this.pps) {
        this.codecDescription = CameraStream.createAvcC(this.sps, this.pps);
      }
    }
    if (!this.codecDescription) {
      return;
    }
    if (!this.decoder) {
      this._createDecoder();
    }
    if (!this.decoder || this.decoder.state !== 'configured') {
      return;
    }

    this._counters.received += 1;
    // Pacing is measured off message arrival, not decode output. Decode
    // output timing is distorted right after (re)connect: the server primes
    // a new client with its cached keyframe, then real-time frames resume
    // immediately after, so the decoder briefly drains a small backlog
    // faster than real-time. That produced a startup FPS overshoot when
    // pacing was measured from decoded-frame timing instead.
    this._trackArrivalInterval(performance.now());

    try {
      const isKeyframe = nalUnits.some((nalUnit) => (nalUnit[0] & 0x1f) === 5);

      // VideoDecoder rejects delta frames until it has decoded an IDR.
      if (this.needsKeyframe && !isKeyframe) {
        return;
      }

      const sample = CameraStream.toAvcSample(nalUnits);
      if (sample.byteLength === 0) {
        return;
      }

      this.decoder.decode(new EncodedVideoChunk({
        type: isKeyframe ? 'key' : 'delta',
        timestamp: this.nextTimestamp,
        data: sample,
      }));

      this.nextTimestamp += Math.round(1000000 / this.options.nominalFps);
      this.needsKeyframe = false;
    } catch (error) {
      this._emit('error', { message: `Decode failed: ${error.message}` });
      this._resetDecoder();
    }
  }

  /* --------------------------------------------------------------------- *
   * Bitstream inspection
   * --------------------------------------------------------------------- */

  static parseAnnexB(data) {
    const nalUnits = [];
    let nalStart = -1;
    let startCodeLength = 0;

    for (let index = 0; index + 2 < data.length; index += 1) {
      if (data[index] !== 0 || data[index + 1] !== 0) {
        continue;
      }

      const currentStartCodeLength = data[index + 2] === 1
        ? 3
        : data[index + 2] === 0 && data[index + 3] === 1
          ? 4
          : 0;
      if (currentStartCodeLength === 0) {
        continue;
      }

      if (nalStart !== -1) {
        const nalUnit = data.subarray(nalStart + startCodeLength, index);
        if (nalUnit.length > 0) {
          nalUnits.push(nalUnit);
        }
      }

      nalStart = index;
      startCodeLength = currentStartCodeLength;
      index += currentStartCodeLength - 1;
    }

    if (nalStart !== -1) {
      const nalUnit = data.subarray(nalStart + startCodeLength);
      if (nalUnit.length > 0) {
        nalUnits.push(nalUnit);
      }
    }

    return nalUnits;
  }

  static createAvcC(sps, pps) {
    if (sps.length < 4 || pps.length === 0) {
      return null;
    }

    const description = new Uint8Array(11 + sps.length + pps.length);
    let offset = 0;
    description[offset++] = 1;
    description[offset++] = sps[1];
    description[offset++] = sps[2];
    description[offset++] = sps[3];
    description[offset++] = 0xff;
    description[offset++] = 0xe1;
    description[offset++] = sps.length >> 8;
    description[offset++] = sps.length & 0xff;
    description.set(sps, offset);
    offset += sps.length;
    description[offset++] = 1;
    description[offset++] = pps.length >> 8;
    description[offset++] = pps.length & 0xff;
    description.set(pps, offset);
    return description;
  }

  static toAvcSample(nalUnits) {
    const samples = nalUnits.filter((nalUnit) => {
      const type = nalUnit[0] & 0x1f;
      return type !== 7 && type !== 8;
    });
    const size = samples.reduce((total, nalUnit) => total + 4 + nalUnit.length, 0);
    const sample = new Uint8Array(size);
    let offset = 0;

    for (const nalUnit of samples) {
      sample[offset++] = nalUnit.length >>> 24;
      sample[offset++] = nalUnit.length >>> 16;
      sample[offset++] = nalUnit.length >>> 8;
      sample[offset++] = nalUnit.length;
      sample.set(nalUnit, offset);
      offset += nalUnit.length;
    }

    return sample;
  }

  /* --------------------------------------------------------------------- *
   * Decoder
   * --------------------------------------------------------------------- */

  _createDecoder() {
    const decoder = new VideoDecoder({
      output: this._onDecodedFrame,
      error: this._onDecoderError,
    });

    const config = {
      codec: this.options.codec,
      optimizeForLatency: true,
      description: this.codecDescription,
    };

    try {
      decoder.configure(config);
    } catch {
      decoder.close();
      this._emit('error', { message: 'Decoder configuration failed' });
      this._setState('unsupported');
      return;
    }

    this.decoder = decoder;
    this.needsKeyframe = true;
  }

  _closeDecoder() {
    if (!this.decoder) {
      return;
    }

    try {
      if (this.decoder.state !== 'closed') {
        this.decoder.close();
      }
    } catch {
      // A decoder already torn down by a fatal error throws here; harmless.
    }

    this.decoder = null;
  }

  _resetDecoder() {
    if (this.destroyed) {
      return;
    }

    this._closeDecoder();
    this._drainFrameQueue();
    this._createDecoder();
    // Don't let the gap across a resync be counted as a real frame interval.
    this._lastFrameArrival = null;
    this._setState('resyncing');
  }

  _onDecoderError(error) {
    this._emit('error', { message: `Decoder: ${error.message}` });
    this._consecutiveDecoderErrors += 1;

    if (this._consecutiveDecoderErrors >= this.options.maxConsecutiveDecoderErrors) {
      this._teardownSocket();
      this._closeDecoder();
      this._drainFrameQueue();
      this._setState('unsupported');
      return;
    }

    this._resetDecoder();
  }

  /* --------------------------------------------------------------------- *
   * Jitter buffer and rendering
   * --------------------------------------------------------------------- */

  _onDecodedFrame(frame) {
    if (this.destroyed) {
      frame.close();
      return;
    }

    this._counters.decoded += 1;
    this._lastActivityAt = performance.now();
    this._consecutiveDecoderErrors = 0;

    while (this.frameQueue.length >= this.options.maxBufferedFrames) {
      this.frameQueue.shift().close();
    }

    this.frameQueue.push(frame);
  }

  /**
   * Paces the jitter buffer directly off decoded-frame arrival intervals
   * instead of a once-per-second rate estimate. The first `bootstrapSamples`
   * intervals are averaged with equal weight (fast, unbiased convergence);
   * afterward a slower EMA smooths out normal jitter. This means playback
   * pacing reflects the real stream rate within a handful of frames rather
   * than the several seconds a 1 Hz estimate starting from a 30fps guess
   * would take.
   */
  _trackArrivalInterval(now) {
    if (this._lastFrameArrival === null) {
      this._lastFrameArrival = now;
      return;
    }

    const interval = now - this._lastFrameArrival;
    this._lastFrameArrival = now;

    // Ignore outliers (e.g. the gap across a reconnect) rather than letting
    // one bad sample distort pacing.
    if (interval <= 0 || interval > 5000) {
      return;
    }

    this._arrivalSampleCount += 1;

    const alpha = this._arrivalSampleCount <= this.options.bootstrapSamples
      ? 1 / this._arrivalSampleCount
      : this.options.steadyStateAlpha;

    this.framePeriodMs += alpha * (interval - this.framePeriodMs);
  }

  /** Closes every buffered VideoFrame. Called on reset and on destroy. */
  _drainFrameQueue() {
    while (this.frameQueue.length > 0) {
      const frame = this.frameQueue.shift();
      try {
        frame.close();
      } catch {
        // Already closed.
      }
    }
  }

  _pump() {
    if (this.destroyed) {
      return;
    }

    this.rafHandle = requestAnimationFrame(this._pump);

    if (this.frameQueue.length === 0) {
      const now = performance.now();
      if (
        this.stats.state === 'streaming'
        && this._lastActivityAt > 0
        && now - this._lastActivityAt > this.options.stallThresholdMs
      ) {
        this._setState('buffering');
      }
      return;
    }

    const now = performance.now();
    const bufferedMs = this.frameQueue.length * this.framePeriodMs;

    // Play out faster when the buffer overfills, so a burst drains instead of
    // becoming permanent latency.
    const period = bufferedMs > this.options.targetBufferMs * 2
      ? this.framePeriodMs / 2
      : this.framePeriodMs;

    if (now - this.lastDrawTime < period) {
      return;
    }

    this.lastDrawTime = now;
    this._draw(this.frameQueue.shift());
  }

  _draw(frame) {
    const { canvas, context } = this;

    if (canvas.width !== frame.displayWidth || canvas.height !== frame.displayHeight) {
      canvas.width = frame.displayWidth;
      canvas.height = frame.displayHeight;
    }

    context.drawImage(frame, 0, 0, canvas.width, canvas.height);
    frame.close();
    this._counters.rendered += 1;

    // Covers both the initial keyframe wait and recovery from a stall.
    if ((this.stats.state === 'connected' || this.stats.state === 'buffering') && !this.needsKeyframe) {
      this._setState('streaming');
    }
  }

  /* --------------------------------------------------------------------- *
   * Stats and events
   * --------------------------------------------------------------------- */

  _sampleStats() {
    const now = performance.now();
    const elapsed = (now - this._windowStart) / 1000;
    if (elapsed <= 0) {
      return;
    }

    const decodedPerSecond = this._counters.decoded / elapsed;

    // Pacing itself now happens per-frame in _trackArrivalInterval; this is
    // reporting only.
    this.stats = {
      fps: this._counters.rendered / elapsed,
      receivedPerSecond: this._counters.received / elapsed,
      decodedPerSecond,
      bufferedFrames: this.frameQueue.length,
      state: this.stats.state,
    };

    this._counters = { rendered: 0, received: 0, decoded: 0 };
    this._windowStart = now;

    this._emit('stats', this.stats);
  }

  _setState(state) {
    if (this.stats.state === state) {
      return;
    }
    this.stats.state = state;
    this._emit('state', { state });
  }

  _emit(type, detail) {
    this.dispatchEvent(new CustomEvent(type, { detail }));
  }
}

export default CameraStream;