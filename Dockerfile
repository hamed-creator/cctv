# node:*-alpine is NOT used here: ffmpeg-static ships a glibc-linked binary
# that fails silently or crashes on musl (Alpine). bookworm-slim is Debian
# (glibc) and small enough to not matter for a server with no build step.
FROM node:20-bookworm-slim

WORKDIR /app

# Installed separately from the app source so this layer is cached across
# rebuilds that only change server.js/app.js/index.html.
COPY package.json ./
RUN npm install --omit=dev

COPY server.js app.js index.html ./

ENV PORT=8080
EXPOSE 8080

CMD ["node", "server.js"]
