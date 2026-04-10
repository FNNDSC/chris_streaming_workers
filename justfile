# Task runner for chris_streaming_workers
# Install just: https://github.com/casey/just

test_compose := "docker compose -f docker-compose.test.yml"

# Run tests: just run unit-tests | integration-tests | e2e-tests | all-tests
run target="unit-tests":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{ target }}" = "all-tests" ]; then
        just run unit-tests
        just run integration-tests
        just run e2e-tests
    elif [ "{{ target }}" = "unit-tests" ]; then
        {{ test_compose }} build unit-tests
        {{ test_compose }} run --rm unit-tests; rc=$?
        {{ test_compose }} --profile unit down -v --remove-orphans
        exit $rc
    elif [ "{{ target }}" = "integration-tests" ]; then
        {{ test_compose }} --profile integration build
        {{ test_compose }} --profile integration up -d --wait
        {{ test_compose }} run --rm integration-tests; rc=$?
        {{ test_compose }} --profile integration down -v --remove-orphans
        exit $rc
    elif [ "{{ target }}" = "e2e-tests" ]; then
        echo "Starting full application stack..."
        docker compose up --build -d
        echo "Waiting for services to stabilize..."
        sleep 15
        {{ test_compose }} --profile e2e build e2e-tests
        {{ test_compose }} --profile e2e run --rm e2e-tests; rc=$?
        # Remove any job containers left behind by pfcon (e.g. from failure tests)
        docker ps -aq --filter "label=org.chrisproject.miniChRIS=plugininstance" | xargs -r docker rm -f
        {{ test_compose }} --profile e2e down -v --remove-orphans
        docker compose down -v
        exit $rc
    else
        echo "Unknown target: {{ target }}"
        echo "Usage: just run unit-tests | integration-tests | e2e-tests | all-tests"
        exit 1
    fi


# Start the full application stack
up:
    docker compose up --build -d

# Stop services
down:
    docker compose down

# Stop and remove all volumes (full reset)
nuke:
    docker compose down -v
    {{ test_compose }} --profile integration down -v --remove-orphans

# Run ruff in the test container
ruff *args="check chris_streaming/":
    {{ test_compose }} build unit-tests
    {{ test_compose }} run --rm --entrypoint ruff unit-tests {{ args }}
