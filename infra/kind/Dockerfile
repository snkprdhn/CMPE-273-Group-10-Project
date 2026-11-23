FROM python:3.12.9-slim-bookworm@sha256:40011d5cf225eb43b650ec158d4a59ca3110e6566ddfd3f506824db6153761cf AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PATH=/opt/venv/bin:$PATH
WORKDIR /app
COPY observability /app/observability
RUN python -m venv /opt/venv
RUN /opt/venv/bin/python -m pip install --no-cache-dir --only-binary=:all: --require-hashes -r observability/requirements.txt
COPY schemas/v1/bundle.json schemas/v1/openapi.json /app/schemas/v1/

FROM base AS action
COPY member3 /app/member3
COPY member1/requirements-registry.txt /tmp/registry.txt
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r member3/requirements.txt -r /tmp/registry.txt
COPY .tools/generated/registry_pb2.py .tools/generated/registry_pb2_grpc.py /app/.tools/generated/
USER 10001:10001
ENTRYPOINT ["/opt/venv/bin/python", "-m", "member3.worker"]

FROM base AS api
COPY member4 /app/member4
COPY identity /app/identity
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r member4/requirements.txt
USER 10001:10001
ENTRYPOINT ["/opt/venv/bin/python", "-m", "member4"]

FROM base AS ui
COPY dashboard /app/dashboard
COPY identity /app/identity
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r dashboard/requirements.txt
USER 10001:10001
ENTRYPOINT ["/opt/venv/bin/python", "-m", "dashboard"]

# Bootstrap and smoke are integration clients, not application dependencies.
FROM action AS acceptance
USER root
COPY member1 /app/member1
COPY member4 /app/member4
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
