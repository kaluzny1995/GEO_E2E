# syntax=docker/dockerfile:1.7
#
# geo_react_app — two-stage build:
#   * Node 20 compiles the Vite app
#   * nginx Alpine serves the static dist (no source, tests or lint tooling in the final image)
#
# Rebuild after changing GEO_APP with `docker compose build geo_react_app`.
# VITE_API_BASE is baked at build time because Vite inlines import.meta.env values.

ARG NODE_VERSION=20
FROM node:${NODE_VERSION}-alpine AS build

WORKDIR /opt/geo/GEO_APP

COPY GEO_APP/package.json GEO_APP/package-lock.json ./
# The repo's dev dependency graph pairs vitest 2 with @vitest/coverage-v8 5 in the
# lockfile; npm 10 rejects that peer range under strict `npm ci`. The app build itself
# does not touch vitest, so mirror the existing local install with legacy peer resolution.
RUN npm ci --legacy-peer-deps --no-audit --no-fund

COPY GEO_APP/index.html GEO_APP/vite.config.js ./
COPY GEO_APP/public/ ./public/
COPY GEO_APP/src/ ./src/

ARG VITE_API_BASE=http://localhost:8000
ENV VITE_API_BASE=${VITE_API_BASE}
RUN npm run build


FROM nginx:1.27-alpine

COPY GEO_E2E/docker/geo_react_app/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /opt/geo/GEO_APP/dist/ /usr/share/nginx/html/

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1/ || exit 1
