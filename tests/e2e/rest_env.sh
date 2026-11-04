#!/bin/bash
# tests/e2e/rest_env.sh — REST scheme / curl options derived from REST_TLS.
# Sourced by lib.sh and by the standalone e2e scripts (no shell options set here).
#   REST_TLS=off   (default) plaintext HTTP
#   REST_TLS=tls   HTTPS, server-auth (curl trusts docker/tls/ca.crt)
#   REST_TLS=mtls  HTTPS + client certificate (docker/tls/client.{crt,key})
# CURL_OPTS always contains -s, so "${CURL_OPTS[@]}" is never an empty array.
# REST_AUTH is for management endpoints (POST/PUT/PATCH); health GETs are open.
E2E_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REST_TLS="${REST_TLS:-off}"
CURL_OPTS=(-s)
case "$REST_TLS" in
    off)  SCHEME="http" ;;
    tls)  SCHEME="https"
          CURL_OPTS+=(--cacert "$E2E_REPO_ROOT/docker/tls/ca.crt") ;;
    mtls) SCHEME="https"
          CURL_OPTS+=(--cacert "$E2E_REPO_ROOT/docker/tls/ca.crt"
                      --cert "$E2E_REPO_ROOT/docker/tls/client.crt"
                      --key "$E2E_REPO_ROOT/docker/tls/client.key") ;;
    *) echo "invalid REST_TLS='$REST_TLS' (expected off|tls|mtls)" >&2; exit 2 ;;
esac
REST_AUTH=(-u admin:secret)
