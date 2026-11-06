# syntax=docker/dockerfile:1.7
#
# geo_streamlit — self-contained GEO_Visual image:
#   * Anaconda at /opt/conda with the geo_visual_311 environment
#   * runtime files only under /opt/geo/GEO_Visual
#   * .env refreshed from the Docker Compose environment at container start
#
# Rebuild after changing GEO_Visual with `docker compose build geo_streamlit`
# (or `docker compose up -d --build geo_streamlit`).

ARG ANACONDA_TAG=2024.10-1
FROM continuumio/anaconda3:${ANACONDA_TAG}

SHELL ["/bin/bash", "-lc"]

ENV CONDA_SOLVER=libmamba \
    PIP_NO_CACHE_DIR=1

COPY GEO_E2E/docker/geo_streamlit/environment.yml /tmp/geo-visual-environment.yml
RUN conda env create --solver=libmamba -f /tmp/geo-visual-environment.yml \
    && conda clean --all --yes \
    && rm -f /tmp/geo-visual-environment.yml

# Unprivileged runtime user.
RUN groupadd --gid 10002 geovisual \
    && useradd --uid 10002 --gid geovisual --create-home --shell /bin/bash geovisual

ENV CONDA_ROOT=/opt/conda
ENV PATH="/opt/conda/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/geo/GEO_Visual

# Runtime files only — no docs, tests, tox/mypy/lint setup, caches, logs or .git.
COPY --chown=geovisual:geovisual \
    GEO_Visual/app.py \
    GEO_Visual/config.json \
    GEO_Visual/error.py \
    /opt/geo/GEO_Visual/

COPY --chown=geovisual:geovisual GEO_Visual/model/ /opt/geo/GEO_Visual/model/
COPY --chown=geovisual:geovisual GEO_Visual/provider/ /opt/geo/GEO_Visual/provider/
COPY --chown=geovisual:geovisual GEO_Visual/utils/ /opt/geo/GEO_Visual/utils/
COPY --chown=geovisual:geovisual GEO_Visual/.streamlit/ /opt/geo/GEO_Visual/.streamlit/

COPY --chown=geovisual:geovisual GEO_E2E/docker/geo_streamlit/entrypoint.sh /usr/local/bin/geo-visual-entrypoint.sh
RUN chmod 755 /usr/local/bin/geo-visual-entrypoint.sh \
    && touch /opt/geo/GEO_Visual/.env \
    && chown -R geovisual:geovisual /opt/geo/GEO_Visual \
    && chmod 600 /opt/geo/GEO_Visual/.env

USER geovisual

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=5)"

ENTRYPOINT ["/usr/local/bin/geo-visual-entrypoint.sh"]
