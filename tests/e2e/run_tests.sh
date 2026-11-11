#!/bin/bash
# tests/e2e/run_tests.sh — Main runner for pg-ha end-to-end integration tests
#
# Usage:
#   ./tests/e2e/run_tests.sh              # Run all tests
#   ./tests/e2e/run_tests.sh bootstrap    # Run only bootstrap tests
#   ./tests/e2e/run_tests.sh --no-build   # Skip build step
#   ./tests/e2e/run_tests.sh --no-setup   # Skip build + cluster setup (assumes running)
#
# Requires: docker, docker compose, jq, curl, psql (libpq client)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib.sh"
# lib.sh relaxes errexit/pipefail for the per-test assertion model. The runner
# itself wants fail-fast for its build/setup steps (test files run in their own
# `bash` subshell, so they keep lib.sh's relaxed mode regardless). Restore it.
set -euo pipefail

# --- Configuration ---
BUILD=${BUILD:-true}
SETUP=${SETUP:-true}
TEARDOWN=${TEARDOWN:-true}
SPECIFIC_TEST=""

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-build)
            BUILD=false
            shift
            ;;
        --no-setup)
            BUILD=false
            SETUP=false
            shift
            ;;
        --no-teardown)
            TEARDOWN=false
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS] [TEST_NAME]"
            echo ""
            echo "Options:"
            echo "  --no-build      Skip cargo build step"
            echo "  --no-setup      Skip build + cluster setup (assumes cluster is running)"
            echo "  --no-teardown   Don't stop cluster after tests"
            echo ""
            echo "Test names:"
            echo "  smoke            Quick smoke test (quick_test.sh)"
            echo "  bootstrap        Cluster bootstrap tests"
            echo "  failover         Auto failover tests"
            echo "  failover_ttl     /failover key TTL (SB-05) tests"
            echo "  switchover       Manual switchover tests"
            echo "  partition        Network partition tests"
            echo "  pause            Pause mode tests"
            echo "  config           Dynamic configuration tests"
            echo "  metrics          Prometheus metrics tests"
            echo "  connections      Database connection tests"
            echo "  rejoin           Old primary rejoin tests"
            echo "  tags             Node tags: DCS publication, candidate checks, nosync"
            echo "  consistency      Cross-layer consistency tests"
            echo "  ha_full          Full HA lifecycle tests"
            echo ""
            echo "If no test name is given, all tests run in order."
            exit 0
            ;;
        *)
            SPECIFIC_TEST="$1"
            shift
            ;;
    esac
done

# --- Pre-flight checks ---
preflight() {
    log_section "Pre-flight Checks"

    local missing=()

    command -v docker > /dev/null 2>&1 || missing+=("docker")
    command -v curl > /dev/null 2>&1 || missing+=("curl")
    command -v jq > /dev/null 2>&1 || missing+=("jq")
    command -v psql > /dev/null 2>&1 || missing+=("psql")

    if [ ${#missing[@]} -gt 0 ]; then
        log_fail "Missing required tools: ${missing[*]}"
        exit 1
    fi

    # Check docker compose (v2 plugin or standalone)
    if docker compose version > /dev/null 2>&1; then
        COMPOSE_CMD="docker compose"
    elif command -v docker-compose > /dev/null 2>&1; then
        COMPOSE_CMD="docker-compose"
    else
        log_fail "Missing: docker compose (plugin or standalone)"
        exit 1
    fi

    log_pass "All required tools available (docker, curl, jq, psql)"
    log_info "Using compose command: $COMPOSE_CMD"
}

# --- Build ---
build() {
    if [ "$BUILD" = "false" ]; then
        log_info "Skipping build (--no-build)"
        return
    fi

    log_section "Building pg-ha"

    # Match Docker host arch: Apple Silicon / linux arm64 → aarch64
    local rust_target docker_platform
    case "$(uname -m)" in
        arm64|aarch64)
            rust_target="aarch64-unknown-linux-gnu"
            docker_platform="linux/arm64"
            ;;
        *)
            rust_target="x86_64-unknown-linux-gnu"
            docker_platform="linux/amd64"
            ;;
    esac

    log_info "Building release binary ($rust_target)..."
    cd "$PROJECT_ROOT"

    # Cross-compile for Linux (docker image target)
    if command -v cargo-zigbuild > /dev/null 2>&1; then
        cargo zigbuild --release --target "$rust_target"
    elif command -v cross > /dev/null 2>&1; then
        cross build --release --target "$rust_target"
    else
        log_warn "Neither cargo-zigbuild nor cross found, trying Docker-native cargo build..."
        docker run --rm --platform "$docker_platform" \
            -v "$PROJECT_ROOT":/app -w /app rust:1.88-bookworm \
            cargo build --release --target "$rust_target" -p pg-ha -p pg-ha-ctl
    fi

    log_info "Building docker images..."
    $COMPOSE_CMD build --build-arg "RUST_TARGET=$rust_target"

    log_pass "Build complete"
}

# --- Cluster Setup ---
setup_cluster() {
    if [ "$SETUP" = "false" ]; then
        log_info "Skipping cluster setup (--no-setup)"
        return
    fi

    log_section "Starting Cluster"

    cd "$PROJECT_ROOT"

    # Clean up any previous run
    $COMPOSE_CMD down -v 2>/dev/null || true

    # Start the 3-node cluster
    log_info "Starting 3-node cluster..."
    $COMPOSE_CMD up -d

    # Wait for cluster to be ready
    wait_for_cluster_ready 120

    log_pass "Cluster is up and ready"
}

# --- Cluster Teardown ---
teardown_cluster() {
    if [ "$TEARDOWN" = "false" ]; then
        log_info "Skipping teardown (--no-teardown)"
        return
    fi

    log_section "Tearing Down Cluster"

    cd "$PROJECT_ROOT"
    $COMPOSE_CMD down -v 2>/dev/null || true

    log_pass "Cluster stopped and volumes removed"
}

# --- Test Execution ---
run_test_file() {
    local test_file="$1"
    local test_name="$2"

    log_section "Running: $test_name"

    if bash "$test_file"; then
        return 0
    else
        log_warn "Test suite '$test_name' had failures"
        return 1
    fi
}

run_all_tests() {
    local overall_failures=0

    # Order matters: non-destructive smoke/inspection first, then the
    # destructive HA scenarios. Every one of these scripts now propagates its
    # real failure count as its exit code, so run_test_file's non-zero branch
    # is trustworthy and overall_failures is an honest gate signal.
    #
    # quick_test.sh is a fast smoke test (bootstrap + metrics + config +
    # switchover) listed first so an obviously-broken cluster fails early.
    # The remaining entries are the full suite, including the five scripts the
    # old runner never registered (ha_full, consistency, db_connections,
    # rejoin, failover_ttl).
    local tests=(
        "quick_test.sh:Smoke (quick_test)"
        "test_bootstrap.sh:Bootstrap"
        "test_metrics.sh:Metrics"
        "test_dynamic_config.sh:Dynamic Configuration"
        "test_db_connections.sh:Database Connections"
        "test_switchover.sh:Manual Switchover"
        "test_failover.sh:Auto Failover"
        "test_failover_ttl.sh:Failover Key TTL"
        "test_network_partition.sh:Network Partition"
        "test_pause_mode.sh:Pause Mode"
        "test_rejoin.sh:Old Primary Rejoin"
        "test_tags.sh:Node Tags"
        "test_consistency.sh:Cross-layer Consistency"
        "test_ha_full.sh:Full HA Lifecycle"
    )

    for entry in "${tests[@]}"; do
        local file="${entry%%:*}"
        local name="${entry##*:}"

        # Every suite (including the first) starts from a known-ready cluster.
        # The destructive scripts kill/stop/pause nodes, so force a readiness
        # barrier between suites rather than a blind fixed sleep.
        log_info "Ensuring cluster is ready before: $name"
        wait_for_cluster_ready 120 || log_warn "Cluster not fully ready before '$name', continuing..."

        # Reset counters for each test file
        TESTS_RUN=0
        TESTS_PASSED=0
        TESTS_FAILED=0

        if ! run_test_file "$SCRIPT_DIR/$file" "$name"; then
            overall_failures=$((overall_failures + 1))
        fi
    done

    return $overall_failures
}

run_specific_test() {
    local test_name="$1"
    local file=""

    case "$test_name" in
        smoke|quick) file="quick_test.sh" ;;
        bootstrap)  file="test_bootstrap.sh" ;;
        failover)   file="test_failover.sh" ;;
        failover_ttl|ttl) file="test_failover_ttl.sh" ;;
        switchover) file="test_switchover.sh" ;;
        partition)  file="test_network_partition.sh" ;;
        pause)      file="test_pause_mode.sh" ;;
        config)     file="test_dynamic_config.sh" ;;
        metrics)    file="test_metrics.sh" ;;
        connections|db) file="test_db_connections.sh" ;;
        rejoin)     file="test_rejoin.sh" ;;
        tags)       file="test_tags.sh" ;;
        consistency) file="test_consistency.sh" ;;
        ha_full|full) file="test_ha_full.sh" ;;
        *)
            log_fail "Unknown test: $test_name"
            echo "Valid tests: smoke, bootstrap, failover, failover_ttl, switchover, partition, pause, config, metrics, connections, rejoin, consistency, ha_full"
            exit 1
            ;;
    esac

    run_test_file "$SCRIPT_DIR/$file" "$test_name"
}

# --- Main ---
main() {
    log_section "pg-ha End-to-End Integration Tests"
    log_info "Project root: $PROJECT_ROOT"
    log_info "Test directory: $SCRIPT_DIR"

    preflight
    build
    setup_cluster

    local exit_code=0

    if [ -n "$SPECIFIC_TEST" ]; then
        run_specific_test "$SPECIFIC_TEST" || exit_code=$?
    else
        run_all_tests || exit_code=$?
    fi

    # Always attempt teardown (even on failure)
    teardown_cluster

    echo ""
    if [ "$exit_code" -eq 0 ]; then
        log_pass "All test suites completed successfully!"
    else
        log_fail "$exit_code test suite(s) had failures"
    fi

    exit $exit_code
}

# Handle cleanup on interrupt
trap 'echo ""; log_warn "Interrupted!"; teardown_cluster; exit 130' INT TERM

main
