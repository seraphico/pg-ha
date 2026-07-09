# Lightweight: PostgreSQL 16 + cross-compiled pg-ha Linux binary
FROM postgres:16-bookworm

COPY target/x86_64-unknown-linux-gnu/release/pg-ha /usr/local/bin/pg-ha
COPY target/x86_64-unknown-linux-gnu/release/pg-ha-ctl /usr/local/bin/pg-ha-ctl
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/pg-ha /usr/local/bin/pg-ha-ctl

EXPOSE 5432 8008 2380 6432 6433
ENTRYPOINT ["/entrypoint.sh"]
