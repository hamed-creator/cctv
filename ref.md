Project Architecture Anchor
Goal: Ultra-low latency RTSP video streaming without transcoding.
Backend: Node.js, fluent-ffmpeg, and ws. Extract raw H.264/H.265 NAL units from an RTSP stream (NO transcoding, NO segmenting). Send binary chunks via secure WebSocket (wss://).
Frontend: Vanilla JS. Receive binary WebSocket messages. Use the standard WebCodecs API (VideoDecoder) to decode NAL units in hardware and paint directly to an HTML <canvas>.
Constraints: Do not suggest MSE (Media Source Extensions), WebRTC, HLS, or DASH. Strictly WebCodecs + WebSockets.
refer to my context in ref.md file.