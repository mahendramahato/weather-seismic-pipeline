# --- Stage 1: build the React app into static files ---
FROM node:22-slim AS build
WORKDIR /app

# Install exactly the versions in package-lock.json (npm ci = clean install).
COPY package.json package-lock.json ./
RUN npm ci

# Copy the source and build: output goes to /app/dist.
COPY . .
RUN npm run build

# --- Stage 2: serve the built files with Caddy ---
# Only dist/ is carried over: no Node, no node_modules, no source code.
FROM caddy:2-alpine
COPY Caddyfile /etc/caddy/Caddyfile
COPY --from=build /app/dist /srv
