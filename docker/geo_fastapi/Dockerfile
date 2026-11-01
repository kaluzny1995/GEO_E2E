# syntax=docker/dockerfile:1.7
#
# geo_fastapi — self-contained GEO_API image:
#   * Anaconda at /opt/conda with the geo_api_311 environment
#   * runtime files only under /opt/geo/GEO_API
#   * .env refreshed from the Docker Compose environment at container start
#
# Rebuild after changing GEO_API with `docker compose build geo_fastapi`
# (or `docker compose up -d --build geo_fastapi`).

ARG ANACONDA_TAG=2024.10-1
FROM continuumio/anaconda3:${ANACONDA_TAG}

SHELL ["/bin/bash", "-lc"]

COPY GEO_E2E/docker/geo_fastapi/environment.yml /tmp/geo-api-environment.yml
RUN conda env create -f /tmp/geo-api-environment.yml \
    && conda clean --all --yes \
    && rm -f /tmp/geo-api-environment.yml

# Unprivileged runtime user.
RUN groupadd --gid 10001 geoapi \
    && useradd --uid 10001 --gid geoapi --create-home --shell /bin/bash geoapi

ENV CONDA_ROOT=/opt/conda
ENV PATH="/opt/conda/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/geo/GEO_API

# Runtime files only — no docs, tests, tox/mypy/lint setup, caches, logs or .git.
COPY --chown=geoapi:geoapi \
    GEO_API/main.py \
    GEO_API/run.py \
    GEO_API/config.json \
    GEO_API/metadata.json \
    GEO_API/error.py \
    GEO_API/error_handlers.py \
    GEO_API/util.py \
    /opt/geo/GEO_API/

COPY --chown=geoapi:geoapi GEO_API/model/ /opt/geo/GEO_API/model/
COPY --chown=geoapi:geoapi GEO_API/provider/ /opt/geo/GEO_API/provider/
COPY --chown=geoapi:geoapi GEO_API/routes/ /opt/geo/GEO_API/routes/

COPY --chown=geoapi:geoapi GEO_E2E/docker/geo_fastapi/entrypoint.sh /usr/local/bin/geo-api-entrypoint.sh
RUN chmod 755 /usr/local/bin/geo-api-entrypoint.sh \
    && mkdir -p /opt/geo/GEO_API/logs \
    && touch /opt/geo/GEO_API/.env \
    && chown -R geoapi:geoapi /opt/geo/GEO_API \
    && chmod 600 /opt/geo/GEO_API/.env

USER geoapi

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=5)"

ENTRYPOINT ["/usr/local/bin/geo-api-entrypoint.sh"]
