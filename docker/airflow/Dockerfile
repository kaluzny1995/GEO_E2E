# PySpark tasks run as Spark drivers inside the Airflow containers and need a JVM.
# The GEO_ETL spark-monitor.jar is compiled for Java 21 (class file 65) — Debian bookworm
# only packages OpenJDK 17 (max 61), so copy OpenJDK 21 from Eclipse Temurin instead.
FROM eclipse-temurin:21-jre AS temurin21

FROM apache/airflow:3.3.2-python3.11

COPY --from=temurin21 /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER root

# conda lives on the host and is bind-mounted at the same absolute path at runtime,
# so the symlink target only resolves inside the running container (works when dangling).
# The real value is passed as a build arg from .env via docker-compose.yaml.
ARG CONDA_ROOT=/opt/conda
RUN ln -s "${CONDA_ROOT}/bin/conda" /usr/local/bin/conda

USER airflow

# Dependencies of model.py / util.py / geo_pipeline.py, pinned to the Airflow 3.3.2 constraints
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt" \
    "apache-airflow-providers-standard==1.19.0" \
    "psutil==7.2.2" \
    "pydantic==2.13.5"
