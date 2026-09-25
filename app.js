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
  codec: 'avc1.64001F',
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
  // Consecutive decoder failures (after the hardware hint has already been
  // dropped) before giving up and reporting 'unsupported' instead of
  // resetting again.
  maxConsecutiveDecoderErrors: 5,
};

export class CameraStream extends EventTarget {
  static isSupported() {
    return typeof window !== 'undefined'
      && 'VideoDecoder' in window
      && 'EncodedVideoChunk' in window;
  }

  // Shared across every instance: hardware decode support for a codec is a
  // property of the machine/browser, not of any one stream, so the same
  // answer applies to every CameraStream on the page and only needs
  // checking once per codec string for the lifetime of the page.
  static _hardwareSupportCache = new Map();

  static _supportsHardware(codec) {
    if (!CameraStream._hardwareSupportCache.has(codec)) {
      const probe = VideoDecoder.isConfigSupported({
        codec,
        hardwareAcceleration: 'prefer-hardware',
      })
        .then((result) => result.supported === true)
        .catch(() => false);
      CameraStream._hardwareSupportCache.set(codec, probe);
    }
    return CameraStream._hardwareSupportCache.get(codec);
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

    // Hardware decode is tried first, but a NotSupportedError from the
    // decoder's async error() callback means THIS browser/GPU cannot decode
    // this stream's actual profile/level with hardware acceleration —
    // retrying the identical config just repeats the same failure forever.
    // Drop the hint once, permanently for this instance, then fall back to
    // software. If software also fails, stop resetting the decoder rather
    // than spinning indefinitely.
    this._preferHardware = true;
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

    // Hardware decode support for a given codec is a fixed property of this
    // machine/browser, not of any particular stream — check once (cached
    // across every CameraStream instance on the page) instead of always
    // attempting hardware first and paying for a guaranteed failure on
    // every single stream start when it's already known to be unsupported.
    CameraStream._supportsHardware(this.options.codec).then((supported) => {
      if (this.destroyed) {
        return;
      }
      this._preferHardware = supported;
      this._createDecoder();
    });

    // The socket doesn't need the decoder to exist yet — incoming messages
    // are safely dropped by _onMessage until the decoder above is ready.
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

    if (!(event.data instanceof ArrayBuffer) || !this.decoder) {
      return;
    }
    if (this.decoder.state !== 'configured') {
      return;
    }

    const data = new Uint8Array(event.data);
    this._counters.received += 1;
    // Pacing is measured off message arrival, not decode output. Decode
    // output timing is distorted right after (re)connect: the server primes
    // a new client with its cached keyframe, then real-time frames resume
    // immediately after, so the decoder briefly drains a small backlog
    // faster than real-time. That produced a startup FPS overshoot when
    // pacing was measured from decoded-frame timing instead.
    this._trackArrivalInterval(performance.now());

    try {
      const isKeyframe = CameraStream.containsKeyframe(data);

      // VideoDecoder rejects delta frames until it has decoded an IDR.
      if (this.needsKeyframe && !isKeyframe) {
        return;
      }

      this.decoder.decode(new EncodedVideoChunk({
        type: isKeyframe ? 'key' : 'delta',
        timestamp: this.nextTimestamp,
        data,
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

  /**
   * True when the access unit contains an IDR slice or a sequence parameter
   * set. Emulation prevention guarantees 00 00 01 never appears inside payload
   * data, so a linear start-code scan is safe.
   */
  static containsKeyframe(data) {
    for (let index = 0; index + 3 < data.length; index += 1) {
      if (data[index] !== 0 || data[index + 1] !== 0) {
        continue;
      }

      let type = -1;
      if (data[index + 2] === 1) {
        type = data[index + 3] & 0x1f;
        index += 2;
      } else if (data[index + 2] === 0 && data[index + 3] === 1 && index + 4 < data.length) {
        type = data[index + 4] & 0x1f;
        index += 3;
      }

        if (type === 5 || type === 7) {
          return true;
        }
        if (type === 1) {
          return false; // Early exit prevents scanning the entire delta payload
        }
    }
    return false;
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
    };

    // Only requested while we haven't yet proven this browser/GPU rejects
    // it for this stream's actual profile/level (see _onDecoderError).
    if (this._preferHardware) {
      config.hardwareAcceleration = 'prefer-hardware';
    }

    try {
      decoder.configure(config);
    } catch {
      // Narrow safety net for a malformed config object throwing
      // synchronously — distinct from the async NotSupportedError path
      // that _onDecoderError handles.
      decoder.configure({ codec: this.options.codec, optimizeForLatency: true });
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

    // First failure while still preferring hardware: this browser/GPU has
    // just told us (asynchronously — configure() itself didn't throw) that
    // it cannot hardware-decode this stream's actual profile/level. Retrying
    // the identical hardware config produces the identical failure every
    // time, so drop the hint once and try software instead of looping.
    if (this._preferHardware) {
      this._preferHardware = false;
      this._resetDecoder();
      return;
    }

    // Software decode also failed, or kept failing repeatedly. At this
    // point resetting again cannot succeed — stop spinning and surface it
    // as a terminal state instead of an endless "resyncing" flicker.
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