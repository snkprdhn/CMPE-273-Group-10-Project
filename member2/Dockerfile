FROM python:3.12.9-slim-bookworm@sha256:48a11b7ba705fd53bf15248d1f94d36c39549903c5d59edcfa2f3f84126e7b44 AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential openjdk-17-jdk-headless curl && rm -rf /var/lib/apt/lists/*
RUN python -m venv /opt/venv
COPY member2/requirements.txt /requirements.txt
RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(command -v javac)))) && /opt/venv/bin/pip wheel --require-hashes --no-deps --wheel-dir /wheels -r /requirements.txt
RUN /opt/venv/bin/python -c "from pathlib import Path; Path('/pins.txt').write_text('\\n'.join(line.split()[0] for line in Path('/requirements.txt').read_text().splitlines() if '==' in line))"
RUN sha256sum /wheels/* > /wheel-sha256.txt
RUN /opt/venv/bin/pip install --no-index --no-deps --find-links=/wheels -r /pins.txt && /opt/venv/bin/pip check && /opt/venv/bin/pip freeze > /resolved.txt
RUN curl -fsSL https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar -o /kafka.jar && echo '5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616  /kafka.jar' | sha256sum -c -

FROM flink:2.2.1-scala_2.12-java17@sha256:f0aa6be1ea6979e0c0c29d02bb1a41eee0bc1a350b86a39056351db165f9f3ff
USER root
COPY --from=build /usr/local /usr/local
COPY --from=build /opt/venv /opt/venv
COPY --from=build /wheel-sha256.txt /opt/member2-wheel-sha256.txt
COPY --from=build /resolved.txt /opt/member2-resolved.txt
COPY --from=build /kafka.jar /opt/flink/lib/kafka.jar
ENV PATH=/opt/venv/bin:$PATH PYTHONPATH=/app PYFLINK_CLIENT_EXECUTABLE=/opt/venv/bin/python
WORKDIR /app
COPY observability /app/observability
RUN /opt/venv/bin/python -m pip install --no-cache-dir --only-binary=:all: --require-hashes -r observability/requirements.txt
COPY member2 /app/member2
COPY schemas /app/schemas
COPY tests/member2 /app/tests/member2
RUN mkdir -p /checkpoints && chown -R flink:flink /checkpoints /app && /opt/venv/bin/python -c 'import pyflink; import platform; print(platform.machine())'
USER flink

COPY member1/fixtures/topology.json /app/member2/integration-topology.json
