# --- Stage 1: build the React app into static files ---
FROM node:22-slim AS build
WORKDIR /app

# Install exactly the versions in package-lock.json (npm ci = clean install).
COPY package.json package-lock.json ./
RUN npm ci

# Copy the source and build: output goes to /app/dist.
COPY . .
RUN npm run build

# --- Stage 2: compile Caddy with the rate-limit plugin ---
# Standard Caddy has no rate limiting; xcaddy (in the official builder image)
# compiles a Caddy binary that includes the caddy-ratelimit module. Both the
# Caddy version and the plugin commit are pinned so builds are repeatable.
FROM caddy:2.11.7-builder-alpine AS caddy-build
RUN xcaddy build v2.11.7 --with github.com/mholt/caddy-ratelimit@5625512f24f6

# --- Stage 3: serve the built files with Caddy ---
# Only dist/ and the custom Caddy binary are carried over: no Node,
# no node_modules, no Go toolchain, no source code.
FROM caddy:2.11.7-alpine
COPY --from=caddy-build /usr/bin/caddy /usr/bin/caddy
COPY Caddyfile /etc/caddy/Caddyfile
COPY --from=build /app/dist /srv
