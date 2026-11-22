# Copy fixed configuration at build time: no macOS file-sharing watchers at runtime.
FROM quay.io/strimzi/kafka:0.45.2-kafka-3.9.2@sha256:5d96fe7c68db643f4916146176a7587aaf9f91c32fa9da7300f013e3e79298d8 AS kafka
USER root
RUN install -d -o 1001 -g 0 -m 0770 /var/lib/kafka/data /opt/kafka/logs
USER 1001
COPY kafka.properties /config/kafka.properties

FROM postgres:17.4-bookworm@sha256:304ab813518754228f9f792f79d6da36359b82d8ecf418096c636725f8c930ad AS postgres
COPY init.sql /docker-entrypoint-initdb.d/10-stores.sql

FROM ghcr.io/shopify/toxiproxy:2.12.0@sha256:9378ed52a28bc50edc1350f936f518f31fa95f0d15917d6eb40b8e376d1a214e AS toxiproxy
COPY toxiproxy.json /config/proxies.json
