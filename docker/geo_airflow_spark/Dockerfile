# syntax=docker/dockerfile:1.7
#
# geo_airflow_spark — the single image behind both the Airflow services and the
# Spark standalone master/workers. It contains:
#   * Apache Airflow 3.3.2 (Python 3.11)
#   * Anaconda at /opt/conda with the geo_e2e_311 / geo_download_311 / geo_etl_311 envs
#   * Apache Spark 3.5.5 at /opt/spark (PostgreSQL JDBC + spark-monitor JARs)
#   * the runtime files of GEO_E2E, GEO_Download and GEO_ETL under /opt/geo
#
# The image is self-contained: no host conda, no host source bind mounts. The data folder
# is the one host bind mount (Compose mounts GEO_Download/data into the containers).
# Rebuilding after a source change is therefore just `docker compose build` / `up --build`.

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.11
ARG SPARK_VERSION=3.5.5
ARG ANACONDA_TAG=2024.10-1

# Anaconda is copied from the official image instead of running the large installer
# twice (the same base layer is shared with the FastAPI image).
FROM continuumio/anaconda3:${ANACONDA_TAG} AS anaconda

# Spark binaries come from the pinned upstream image; only /opt/spark is copied.
FROM apache/spark:${SPARK_VERSION} AS spark

# Spark 3.5.5 + the Java-21-compiled spark-monitor.jar need a Java 21 runtime.
FROM eclipse-temurin:21-jre AS temurin21

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

COPY --from=temurin21 /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --from=spark /opt/spark /opt/spark

# Anaconda + conda in the default PATH (the base interpreter stays Airflow's Python —
# only the conda command is symlinked, never /opt/conda/bin ahead of the system python).
COPY --from=anaconda /opt/conda /opt/conda
ENV CONDA_ROOT=/opt/conda \
    CONDA_SOLVER=libmamba \
    PIP_NO_CACHE_DIR=1
RUN ln -s /opt/conda/bin/conda /usr/local/bin/conda \
    && conda --version

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps curl tzdata \
    && rm -rf /var/lib/apt/lists/*

# Spark runs as the airflow user and needs writable work/log directories.
RUN mkdir -p /opt/spark/work /opt/spark/logs \
    && chown -R airflow:root /opt/spark/work /opt/spark/logs

# Runtime-only conda environments. The repository environment.yml files also contain
# pytest/ruff/tox/mypy; those are intentionally absent from the image.
COPY GEO_E2E/docker/geo_airflow_spark/environment/ /tmp/geo-environments/
RUN conda env create --solver=libmamba -f /tmp/geo-environments/geo_e2e_311.yml \
    && conda env create --solver=libmamba -f /tmp/geo-environments/geo_download_311.yml \
    && conda env create --solver=libmamba -f /tmp/geo-environments/geo_etl_311.yml \
    && conda clean --all --yes \
    && rm -rf /tmp/geo-environments

# PostgreSQL JDBC (Supabase writes/reads) and the Java SparkListener used by GEO_ETL.
COPY GEO_E2E/docker/geo_airflow_spark/jars/postgresql-42.7.4.jar /opt/spark/jars/
COPY GEO_E2E/docker/geo_airflow_spark/jars/spark-monitor.jar /opt/spark/jars/
# PySpark drivers run inside the geo_etl_311 environment; its bundled PySpark needs the
# same two JARs on the driver classpath.
RUN cp /opt/spark/jars/postgresql-42.7.4.jar \
       /opt/spark/jars/spark-monitor.jar \
       /opt/conda/envs/geo_etl_311/lib/python3.11/site-packages/pyspark/jars/

# Runtime project files only — no docs, tests, tox/mypy/lint setup, caches, logs or .git.
RUN mkdir -p /opt/geo/GEO_E2E/docker/airflow /opt/geo/GEO_Download /opt/geo/GEO_ETL /opt/geo/bin

COPY --chown=airflow:root \
    GEO_E2E/geo_pipeline.py \
    GEO_E2E/geo_dag.py \
    GEO_E2E/model.py \
    GEO_E2E/util.py \
    GEO_E2E/config.json \
    GEO_E2E/jobs.json \
    GEO_E2E/.airflowignore \
    GEO_E2E/setup.sh \
    /opt/geo/GEO_E2E/

COPY --chown=airflow:root \
    GEO_E2E/docker/geo_airflow_spark/create_schema.py \
    GEO_E2E/docker/geo_airflow_spark/seed_pools.py \
    /opt/geo/GEO_E2E/docker/airflow/

COPY --chown=airflow:root \
    GEO_Download/run_download.py \
    GEO_Download/downloader_factory.py \
    GEO_Download/reverse_geocode.py \
    GEO_Download/run_openmeteo_thresholds.py \
    GEO_Download/util.py \
    GEO_Download/config.json \
    /opt/geo/GEO_Download/

COPY --chown=airflow:root GEO_Download/downloader/ /opt/geo/GEO_Download/downloader/
COPY --chown=airflow:root GEO_Download/model/ /opt/geo/GEO_Download/model/
COPY --chown=airflow:root GEO_Download/data/ /opt/geo/GEO_Download/data/

COPY --chown=airflow:root \
    GEO_ETL/run_etl.py \
    GEO_ETL/config.json \
    /opt/geo/GEO_ETL/

COPY --chown=airflow:root GEO_ETL/factory/ /opt/geo/GEO_ETL/factory/
COPY --chown=airflow:root GEO_ETL/ingestion/ /opt/geo/GEO_ETL/ingestion/
COPY --chown=airflow:root GEO_ETL/jobs/ /opt/geo/GEO_ETL/jobs/
COPY --chown=airflow:root GEO_ETL/model/ /opt/geo/GEO_ETL/model/
COPY --chown=airflow:root GEO_ETL/single_etl/ /opt/geo/GEO_ETL/single_etl/
COPY --chown=airflow:root GEO_ETL/utils/ /opt/geo/GEO_ETL/utils/
COPY --chown=airflow:root GEO_ETL/views/ /opt/geo/GEO_ETL/views/

COPY --chown=airflow:root \
    GEO_E2E/docker/geo_airflow_spark/refresh-env.sh \
    GEO_E2E/docker/geo_airflow_spark/airflow-entrypoint.sh \
    /opt/geo/bin/
# The parent dirs were created by root; COPY --chown only owns the copied entries.
RUN chmod 755 /opt/geo/bin/refresh-env.sh /opt/geo/bin/airflow-entrypoint.sh /opt/geo/GEO_E2E/setup.sh \
    && chown -R airflow:root /opt/geo

# Airflow-level Python dependencies used by the DAG files themselves (the task
# subprocesses use the conda environments above).
USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt" \
    "apache-airflow-providers-standard==1.19.0" \
    "psutil==7.2.2" \
    "pydantic==2.13.5"

# Generate the in-image .env with container paths (no host paths, no host conda).
# airflow-init re-runs this at startup to inject the credentials supplied by Compose.
# The service containers run as the host UID (AIRFLOW_UID) with gid 0, so the .env files
# and their parent directories must be writable by group 0: each container refreshes its
# own copy of these files on startup.
RUN cd /opt/geo/GEO_E2E \
    && GEO_DOCKER=1 ./setup.sh --force \
    && touch /opt/geo/GEO_ETL/.env \
    && chmod 775 /opt/geo/GEO_E2E /opt/geo/GEO_Download /opt/geo/GEO_ETL \
    && chmod 664 /opt/geo/GEO_E2E/.env /opt/geo/GEO_ETL/.env

USER airflow

EXPOSE 8090 7078 8083 8084 8085
