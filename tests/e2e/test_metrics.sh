#!/bin/bash
# tests/e2e/test_metrics.sh — Verify Prometheus metrics endpoint
#
# Tests:
#   - GET /metrics returns Prometheus format (text/plain with HELP/TYPE lines)
#   - Contains expected gauge names

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

log_section "TEST: Metrics"

# --- Test: /metrics returns 200 ---
test_metrics_endpoint_available() {
    log_info "Checking /metrics endpoint availability..."

    local primary_url
    primary_url=$(get_primary)
    assert_http_status "$primary_url/metrics" "200" "/metrics returns 200 on primary"

    # Also check a replica
    local replica_url
    replica_url=$(get_replicas | head -n 1)
    if [ -n "$replica_url" ]; then
        assert_http_status "$replica_url/metrics" "200" "/metrics returns 200 on replica"
    fi
}

# --- Test: Metrics are in Prometheus format ---
test_metrics_prometheus_format() {
    log_info "Checking Prometheus format..."

    local primary_url
    primary_url=$(get_primary)
    local metrics
    metrics=$(http_get "$primary_url/metrics")

    # Prometheus format should have # HELP and # TYPE lines
    assert_contains "$metrics" "# HELP" "Metrics contain # HELP lines"
    assert_contains "$metrics" "# TYPE" "Metrics contain # TYPE lines"
}

# --- Test: Expected metric names present ---
test_expected_metrics_present() {
    log_info "Checking expected metric names..."

    local primary_url
    primary_url=$(get_primary)
    local metrics
    metrics=$(http_get "$primary_url/metrics")

    # Core metrics we expect from the pg-ha system
    local expected_metrics=(
        "pg_ha_node_is_primary"
        "pg_ha_node_is_replica"
        "pg_ha_postgres_running"
        "pg_ha_replication_lag_bytes"
        "pg_ha_timeline"
        "pg_ha_dcs_last_seen_seconds"
        "pg_ha_failsafe_active"
        "pg_ha_loop_duration_seconds"
    )

    for metric_name in "${expected_metrics[@]}"; do
        TESTS_RUN=$((TESTS_RUN + 1))
        if echo "$metrics" | grep -q "$metric_name"; then
            TESTS_PASSED=$((TESTS_PASSED + 1))
            log_pass "Metric present: $metric_name"
        else
            TESTS_FAILED=$((TESTS_FAILED + 1))
            log_fail "Metric missing: $metric_name"
        fi
    done
}

# --- Test: Primary node reports is_primary=1 ---
test_primary_metric_value() {
    log_info "Checking primary reports is_primary=1..."

    local primary_url
    primary_url=$(get_primary)
    local metrics
    metrics=$(http_get "$primary_url/metrics")

    TESTS_RUN=$((TESTS_RUN + 1))
    if echo "$metrics" | grep -q 'pg_ha_node_is_primary.*1'; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "Primary node reports pg_ha_node_is_primary=1"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Primary node does not report pg_ha_node_is_primary=1"
    fi
}

# --- Test: Replica reports is_replica=1 ---
test_replica_metric_value() {
    log_info "Checking replica reports is_replica=1..."

    local replica_url
    replica_url=$(get_replicas | head -n 1)
    if [ -z "$replica_url" ]; then
        log_warn "No replica available, skipping"
        return
    fi

    local metrics
    metrics=$(http_get "$replica_url/metrics")

    TESTS_RUN=$((TESTS_RUN + 1))
    if echo "$metrics" | grep -q 'pg_ha_node_is_replica.*1'; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "Replica node reports pg_ha_node_is_replica=1"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Replica node does not report pg_ha_node_is_replica=1"
    fi
}

# --- Test: Replication lag metric on replica ---
test_replication_lag_metric() {
    log_info "Checking replication lag metric on replica..."

    local replica_url
    replica_url=$(get_replicas | head -n 1)
    if [ -z "$replica_url" ]; then
        log_warn "No replica available, skipping"
        return
    fi

    local metrics
    metrics=$(http_get "$replica_url/metrics")

    TESTS_RUN=$((TESTS_RUN + 1))
    if echo "$metrics" | grep -q 'pg_ha_replication_lag_bytes'; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "Replication lag metric present on replica"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Replication lag metric missing on replica"
    fi
}

# --- Run tests ---
test_metrics_endpoint_available
test_metrics_prometheus_format
test_expected_metrics_present
test_primary_metric_value
test_replica_metric_value
test_replication_lag_metric

print_results
