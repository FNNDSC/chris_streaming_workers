// ============================================================================
// ChRIS Streaming Workers — Test UI
//
// Submits workflows to the SSE service (which orchestrates pfcon via Celery)
// and streams status/log events via SSE with historical replay.
// ============================================================================

const SSE_BASE = '/sse';

let statusSource = null;
let logSource = null;
let workflowPollInterval = null;

// ---------------------------------------------------------------------------
// Logging helpers
// ---------------------------------------------------------------------------

function appendStatus(html) {
    const el = document.getElementById('status-log');
    el.innerHTML += html + '<br>';
    el.scrollTop = el.scrollHeight;
}

function appendLog(html) {
    const el = document.getElementById('event-log');
    el.innerHTML += html + '<br>';
    el.scrollTop = el.scrollHeight;
}

function clearLogs() {
    document.getElementById('status-log').innerHTML = '';
    document.getElementById('event-log').innerHTML = '';
    document.getElementById('workflow-steps').innerHTML = '';
}

function statusBadge(status) {
    return `<span class="status-badge s-${status}">${status}</span>`;
}

function ts() {
    return `<span class="ts">[${new Date().toLocaleTimeString()}]</span>`;
}

// ---------------------------------------------------------------------------
// Workflow step tracker
// ---------------------------------------------------------------------------

const STEPS = ['copy', 'plugin', 'upload', 'delete', 'cleanup', 'completed'];

function updateStepTracker(currentStep, workflowStatus) {
    const el = document.getElementById('workflow-steps');
    if (!el.children.length) {
        el.innerHTML = STEPS.map((s, i) =>
            `<div class="step" id="step-${i}">${i + 1}. ${s}</div>`
        ).join('');
    }
    for (let i = 0; i < STEPS.length; i++) {
        const stepEl = document.getElementById(`step-${i}`);
        if (!stepEl) continue;
        if (STEPS[i] === currentStep) {
            stepEl.className = workflowStatus === 'failed' ? 'step error' : 'step active';
        } else if (i < STEPS.indexOf(currentStep)) {
            stepEl.className = 'step done';
        } else if (currentStep === 'completed') {
            stepEl.className = workflowStatus === 'failed' ? 'step error' : 'step done';
        } else {
            stepEl.className = 'step';
        }
    }
}

// ---------------------------------------------------------------------------
// SSE connection
// ---------------------------------------------------------------------------

function connectSSE() {
    const jid = document.getElementById('job-id').value.trim();
    disconnectSSE();

    const stateEl = document.getElementById('sse-state');

    // Status stream (includes historical replay)
    statusSource = new EventSource(`${SSE_BASE}/events/${jid}/status`);
    statusSource.addEventListener('status', (e) => {
        try {
            const data = JSON.parse(e.data);
            appendStatus(
                `${ts()} SSE ${statusBadge(data.status)} ` +
                `job_type=${data.job_type} exit=${data.exit_code ?? '-'}`
            );
        } catch { appendStatus(`${ts()} SSE raw: ${e.data}`); }
    });
    statusSource.onopen = () => {
        stateEl.textContent = 'connected';
        stateEl.style.color = 'var(--ok)';
    };
    statusSource.onerror = () => {
        stateEl.textContent = 'reconnecting...';
        stateEl.style.color = 'var(--err)';
    };

    // Log stream (includes historical replay)
    logSource = new EventSource(`${SSE_BASE}/events/${jid}/logs`);
    logSource.addEventListener('logs', (e) => {
        try {
            const data = JSON.parse(e.data);
            const streamClass = data.stream === 'stderr' ? 'stream-stderr' : '';
            appendLog(
                `<span class="log-line">` +
                `<span class="ts">[${data.timestamp?.substring(11, 23) || ''}]</span> ` +
                `<span class="${streamClass}">${escapeHtml(data.line)}</span>` +
                `</span>`
            );
        } catch { appendLog(`<span class="log-line">${escapeHtml(e.data)}</span>`); }
    });
}

function disconnectSSE() {
    if (statusSource) { statusSource.close(); statusSource = null; }
    if (logSource) { logSource.close(); logSource = null; }
    if (workflowPollInterval) { clearInterval(workflowPollInterval); workflowPollInterval = null; }
    document.getElementById('sse-state').textContent = 'disconnected';
    document.getElementById('sse-state').style.color = 'var(--muted)';
}

function escapeHtml(s) {
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
}

// ---------------------------------------------------------------------------
// Workflow submission and tracking
// ---------------------------------------------------------------------------

async function runFullWorkflow() {
    const btn = document.getElementById('btn-run');
    btn.disabled = true;

    const jid = document.getElementById('job-id').value.trim();
    const image = document.getElementById('plugin-image').value.trim();
    const type = document.getElementById('plugin-type').value;
    const cpuLimit = parseInt(document.getElementById('cpu-limit').value);
    const memLimit = parseInt(document.getElementById('mem-limit').value);
    const entrypoint = document.getElementById('entrypoint').value.split(',').map(s => s.trim());
    const args = document.getElementById('plugin-args').value.split(',').map(s => s.trim());
    const inputDirs = document.getElementById('input-dirs').value.trim();
    const outputDir = document.getElementById('output-dir').value.trim();

    try {
        // Connect SSE first to capture all events
        connectSSE();

        // Submit workflow to SSE service
        appendStatus(`${ts()} Submitting workflow...`);
        const resp = await fetch(`${SSE_BASE}/api/jobs/${jid}/run`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                image,
                entrypoint,
                args,
                type,
                cpu_limit: cpuLimit,
                memory_limit: memLimit,
                gpu_limit: 0,
                number_of_workers: 1,
                input_dirs: inputDirs,
                output_dir: outputDir,
            }),
        });

        if (resp.status === 202) {
            const data = await resp.json();
            appendStatus(`${ts()} Workflow submitted: ${statusBadge('started')}`);
            updateStepTracker('copy', 'running');

            // Poll workflow status for step tracker updates
            startWorkflowPolling(jid);
        } else {
            const text = await resp.text();
            appendStatus(
                `${ts()} <span style="color:var(--err)">` +
                `Submission failed (${resp.status}): ${text}</span>`
            );
        }
    } catch (err) {
        appendStatus(
            `${ts()} <span style="color:var(--err)">Error: ${err.message}</span>`
        );
    } finally {
        btn.disabled = false;
    }
}

function startWorkflowPolling(jid) {
    if (workflowPollInterval) clearInterval(workflowPollInterval);

    workflowPollInterval = setInterval(async () => {
        try {
            const resp = await fetch(`${SSE_BASE}/api/jobs/${jid}/workflow`);
            const data = await resp.json();

            if (data.status === 'not_found') return;

            updateStepTracker(data.current_step, data.status);

            if (data.current_step === 'completed' || data.status === 'failed') {
                clearInterval(workflowPollInterval);
                workflowPollInterval = null;

                const label = data.status === 'failed' ? 'failed' : 'completed';
                const color = data.status === 'failed' ? 'var(--err)' : 'var(--ok)';
                appendStatus(
                    `${ts()} <strong style="color:${color}">` +
                    `Workflow ${label}!</strong>`
                );
            }
        } catch { /* ignore poll errors */ }
    }, 2000);
}
