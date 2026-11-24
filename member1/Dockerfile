FROM python:3.12.9-slim-bookworm@sha256:48a11b7ba705fd53bf15248d1f94d36c39549903c5d59edcfa2f3f84126e7b44 AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PATH=/opt/venv/bin:$PATH
WORKDIR /app
COPY observability /app/observability
RUN python -m venv /opt/venv
RUN /opt/venv/bin/python -m pip install --no-cache-dir --only-binary=:all: --require-hashes -r observability/requirements.txt && mkdir /state && chown 10001:10001 /state
COPY member1 /app/member1
COPY schemas/v1/bundle.json /app/schemas/v1/bundle.json
ENTRYPOINT ["/opt/venv/bin/python", "-m", "member1"]

FROM base AS simulator
RUN /opt/venv/bin/python -m pip install --no-cache-dir --only-binary=:all: --require-hashes -r member1/requirements-simulator.txt
USER 10001:10001
EXPOSE 8002
HEALTHCHECK --interval=10s --timeout=2s --start-period=10s CMD ["/opt/venv/bin/python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8002/readyz', timeout=1)"]
CMD ["simulate", "--bootstrap", "kafka:9092", "--state", "/state/edge.sqlite", "--bind", "0.0.0.0"]

FROM base AS registry
COPY .tools/generated/registry_pb2.py .tools/generated/registry_pb2_grpc.py /app/.tools/generated/
RUN /opt/venv/bin/python -m pip install --no-cache-dir --only-binary=:all: --require-hashes -r member1/requirements-registry.txt
USER 10001:10001
EXPOSE 50051 8001
HEALTHCHECK --interval=10s --timeout=2s --start-period=10s CMD ["/opt/venv/bin/python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8001/readyz', timeout=1)"]
CMD ["registry", "--registry", "0.0.0.0:50051", "--bind", "0.0.0.0"]
