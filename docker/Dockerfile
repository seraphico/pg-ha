# Lightweight: PostgreSQL + Linux pg-ha binary
# PG version is determined by the base image tag (e.g. postgres:16, postgres:18).
# RUST_TARGET is passed by Makefile / docker compose (Mac Intel / Mac ARM / Linux amd64).
ARG PG_VERSION=19
FROM postgres:${PG_VERSION}

ARG RUST_TARGET=x86_64-unknown-linux-gnu
COPY target/${RUST_TARGET}/release/pg-ha /usr/local/bin/pg-ha
COPY target/${RUST_TARGET}/release/pg-ha-ctl /usr/local/bin/pg-ha-ctl
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/pg-ha /usr/local/bin/pg-ha-ctl

EXPOSE 5432 8008 2380 6432 6433
ENTRYPOINT ["/entrypoint.sh"]
