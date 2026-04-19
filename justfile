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


# ============================================================================
# Kubernetes recipes
#
# Uses a local Docker registry at localhost:5005 (port 5000 conflicts with
# macOS AirPlay Receiver). Tested on Docker Desktop's built-in K8s.
# ============================================================================

registry_name := "chris-k8s-registry"
registry_port := "5005"
registry_image_prefix := "localhost:" + registry_port + "/fnndsc"
k8s_ns := "chris-streaming"
k8s_test_ns := "chris-streaming-test"

# Ensure the local Docker registry is running (idempotent).
k8s-registry:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker inspect {{ registry_name }} >/dev/null 2>&1; then
        echo "Starting local registry on 127.0.0.1:{{ registry_port }}..."
        docker run -d --restart=always \
            -p 127.0.0.1:{{ registry_port }}:5000 \
            --name {{ registry_name }} registry:2
    elif [ "$(docker inspect -f '{{{{.State.Running}}}}' {{ registry_name }})" != "true" ]; then
        docker start {{ registry_name }}
    fi

# Build all images and push them to the local registry.
k8s-build: k8s-registry
    #!/usr/bin/env bash
    set -euo pipefail
    build() {
        local dockerfile=$1 tag=$2
        echo "=== Building $tag ==="
        docker build -f "$dockerfile" -t "{{ registry_image_prefix }}/$tag:dev" .
        docker push "{{ registry_image_prefix }}/$tag:dev"
    }
    build Dockerfile.event_forwarder compute-event-forwarder
    build Dockerfile.status_consumer compute-status-consumer
    build Dockerfile.log_consumer    compute-log-consumer
    build Dockerfile.log_forwarder   compute-log-forwarder
    build Dockerfile.sse_service     sse_service
    build Dockerfile.test            chris_streaming_tests
    docker build -t "{{ registry_image_prefix }}/test_ui:dev" ./test_ui
    docker push "{{ registry_image_prefix }}/test_ui:dev"

# Bring up the full stack on Kubernetes.
k8s-up: k8s-build
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl kustomize --load-restrictor=LoadRestrictionsNone kubernetes/ | kubectl apply -f -
    echo "Waiting for pfcon storebase test data..."
    kubectl wait --for=condition=complete job/init-test-data -n {{ k8s_ns }} --timeout=120s || true
    echo "Waiting for deployments..."
    for dep in pfcon sse-service celery-worker event-forwarder status-consumer log-consumer log-forwarder redis test-ui; do
        kubectl rollout status deployment/$dep -n {{ k8s_ns }} --timeout=180s
    done
    for sts in opensearch postgres; do
        kubectl rollout status statefulset/$sts -n {{ k8s_ns }} --timeout=180s
    done
    echo
    echo "Test UI:     http://localhost:30888"
    echo "SSE service: http://localhost:30080"
    echo "pfcon:       http://localhost:30005"

# Delete all app resources (keeps PVCs so data survives).
k8s-down:
    kubectl kustomize --load-restrictor=LoadRestrictionsNone kubernetes/ | kubectl delete --ignore-not-found=true -f -

# Full reset: delete app + PVCs + test namespace + local registry container.
k8s-nuke:
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl kustomize --load-restrictor=LoadRestrictionsNone kubernetes/ | kubectl delete --ignore-not-found=true -f - || true
    kubectl delete pvc --all -n {{ k8s_ns }} --ignore-not-found=true || true
    kubectl delete namespace {{ k8s_ns }} --ignore-not-found=true || true
    kubectl delete namespace {{ k8s_test_ns }} --ignore-not-found=true || true
    if docker inspect {{ registry_name }} >/dev/null 2>&1; then
        docker rm -f {{ registry_name }}
    fi

# Tail logs for a component by label (e.g. `just k8s-logs sse-service`).
k8s-logs svc:
    kubectl logs -f -n {{ k8s_ns }} -l app={{ svc }} --tail=200 --max-log-requests 10

# Run tests in-cluster.
#   just k8s-run unit-tests | integration-tests | e2e-tests | all-tests
k8s-run target="unit-tests":
    #!/usr/bin/env bash
    set -euo pipefail

    run_job() {
        local ns=$1 manifest=$2 job=$3 timeout=$4
        kubectl delete job $job -n $ns --ignore-not-found=true
        kubectl apply -f $manifest
        echo "Waiting for $job to finish (timeout ${timeout})..."
        # Wait for either condition; the non-matching one returns non-zero,
        # so we swallow errors and then check status explicitly.
        kubectl wait --for=condition=complete job/$job -n $ns --timeout=$timeout &
        COMPLETE_PID=$!
        kubectl wait --for=condition=failed job/$job -n $ns --timeout=$timeout &
        FAILED_PID=$!
        wait -n $COMPLETE_PID $FAILED_PID || true
        kill $COMPLETE_PID $FAILED_PID 2>/dev/null || true
        kubectl logs -n $ns job/$job --tail=-1
        local status
        status=$(kubectl get job $job -n $ns -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || echo "")
        if [ "$status" = "True" ]; then
            return 0
        fi
        return 1
    }

    if [ "{{ target }}" = "all-tests" ]; then
        just k8s-run unit-tests
        just k8s-run integration-tests
        just k8s-run e2e-tests
    elif [ "{{ target }}" = "unit-tests" ]; then
        # Ensure the test image is built and pushed, and the test ns exists.
        just k8s-build
        kubectl apply -f kubernetes/tests/00-test-namespace.yaml
        run_job {{ k8s_test_ns }} kubernetes/tests/unit-tests-job.yaml unit-tests 600s
        rc=$?
        kubectl delete -f kubernetes/tests/unit-tests-job.yaml --ignore-not-found=true
        exit $rc
    elif [ "{{ target }}" = "integration-tests" ]; then
        just k8s-build
        kubectl kustomize --load-restrictor=LoadRestrictionsNone kubernetes/tests/ | kubectl apply -f -
        echo "Waiting for integration stack..."
        for dep in redis opensearch postgres; do
            kubectl rollout status deployment/$dep -n {{ k8s_test_ns }} --timeout=180s
        done
        run_job {{ k8s_test_ns }} kubernetes/tests/integration-tests-job.yaml integration-tests 900s
        rc=$?
        kubectl kustomize --load-restrictor=LoadRestrictionsNone kubernetes/tests/ | kubectl delete --ignore-not-found=true -f - || true
        kubectl delete namespace {{ k8s_test_ns }} --ignore-not-found=true
        exit $rc
    elif [ "{{ target }}" = "e2e-tests" ]; then
        just k8s-up
        run_job {{ k8s_ns }} kubernetes/tests/e2e-tests-job.yaml e2e-tests 1200s
        rc=$?
        # Clean up any leftover plugininstance jobs from failure tests.
        kubectl delete job -n {{ k8s_ns }} -l chrisproject.org/role=plugininstance --ignore-not-found=true || true
        kubectl delete -f kubernetes/tests/e2e-tests-job.yaml --ignore-not-found=true
        exit $rc
    else
        echo "Unknown target: {{ target }}"
        echo "Usage: just k8s-run unit-tests | integration-tests | e2e-tests | all-tests"
        exit 1
    fi
