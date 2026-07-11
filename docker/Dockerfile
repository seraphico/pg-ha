# Lightweight: PostgreSQL 16 + Linux pg-ha binary
# RUST_TARGET is passed by Makefile / docker compose (Mac Intel / Mac ARM / Linux amd64).
FROM postgres:16-bookworm

ARG RUST_TARGET=x86_64-unknown-linux-gnu
COPY target/${RUST_TARGET}/release/pg-ha /usr/local/bin/pg-ha
COPY target/${RUST_TARGET}/release/pg-ha-ctl /usr/local/bin/pg-ha-ctl
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/pg-ha /usr/local/bin/pg-ha-ctl

EXPOSE 5432 8008 2380 6432 6433
ENTRYPOINT ["/entrypoint.sh"]
