FROM python:3.12.9-slim-bookworm@sha256:48a11b7ba705fd53bf15248d1f94d36c39549903c5d59edcfa2f3f84126e7b44 AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PATH=/opt/venv/bin:$PATH
WORKDIR /app
COPY observability /app/observability
RUN python -m venv /opt/venv
RUN /opt/venv/bin/python -m pip install --no-cache-dir --only-binary=:all: --require-hashes -r observability/requirements.txt
COPY schemas/v1/bundle.json schemas/v1/openapi.json /app/schemas/v1/

FROM base AS action
COPY action /app/action
COPY devices/requirements-registry.txt /tmp/registry.txt
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r action/requirements.txt -r /tmp/registry.txt
COPY .tools/generated/registry_pb2.py .tools/generated/registry_pb2_grpc.py /app/.tools/generated/
USER 10001:10001
ENTRYPOINT ["/opt/venv/bin/python", "-m", "action.worker"]

FROM base AS api
COPY projection /app/projection
COPY identity /app/identity
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r projection/requirements.txt
USER 10001:10001
ENTRYPOINT ["/opt/venv/bin/python", "-m", "projection"]

FROM base AS ui
COPY dashboard /app/dashboard
COPY identity /app/identity
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r dashboard/requirements.txt
USER 10001:10001
ENTRYPOINT ["/opt/venv/bin/python", "-m", "dashboard"]

# Bootstrap and smoke are integration clients, not application dependencies.
FROM action AS acceptance
USER root
COPY devices /app/devices
COPY projection /app/projection
COPY tools/integration.py /app/tools/integration.py
COPY tools/kind_workload.py /app/tools/kind_workload.py
USER 10001:10001
ENTRYPOINT ["/opt/venv/bin/python", "-m", "tools.kind_workload"]

FROM base AS copilot
COPY copilot /app/copilot
COPY identity /app/identity
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r copilot/requirements.txt
USER 10001:10001
ENTRYPOINT ["/opt/venv/bin/python", "-m", "copilot"]

# Continuous synthetic fleet. Same dependencies and user as the acceptance client.
FROM acceptance AS fleet
ENTRYPOINT ["/opt/venv/bin/python", "-m", "devices.fleet"]
