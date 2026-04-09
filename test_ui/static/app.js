// ============================================================================
// ChRIS Streaming Workers — Test UI
//
// Submits jobs to pfcon via the nginx proxy and streams status/log events
// from the SSE service.
// ============================================================================

const PFCON_BASE = '/pfcon/api/v1';
const SSE_BASE = '/sse';

let token = null;
let statusSource = null;
let logSource = null;

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

const STEPS = ['authenticate', 'copy', 'poll-copy', 'plugin', 'poll-plugin', 'file-list', 'upload', 'delete', 'poll-delete', 'cleanup'];
let currentStep = -1;

function updateStep(idx, state) {
    const el = document.getElementById('workflow-steps');
    if (!el.children.length) {
        el.innerHTML = STEPS.map((s, i) => `<div class="step" id="step-${i}">${i + 1}. ${s}</div>`).join('');
    }
    const stepEl = document.getElementById(`step-${idx}`);
    if (stepEl) stepEl.className = `step ${state}`;
}

function stepActive(idx) { currentStep = idx; updateStep(idx, 'active'); }
function stepDone(idx) { updateStep(idx, 'done'); }
function stepError(idx) { updateStep(idx, 'error'); }

// ---------------------------------------------------------------------------
// pfcon API calls
// ---------------------------------------------------------------------------

async function pfconPost(path, body, isJson = false) {
    const headers = {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    if (isJson) {
        headers['Content-Type'] = 'application/json';
    } else {
        headers['Content-Type'] = 'application/x-www-form-urlencoded';
    }
    const resp = await fetch(PFCON_BASE + path, { method: 'POST', headers, body });
    const text = await resp.text();
    try { return { status: resp.status, data: JSON.parse(text) }; }
    catch { return { status: resp.status, data: text }; }
}

async function pfconGet(path) {
    const headers = {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    const resp = await fetch(PFCON_BASE + path, { headers });
    return { status: resp.status, data: await resp.json() };
}

async function pfconDelete(path) {
    const headers = {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    const resp = await fetch(PFCON_BASE + path, { method: 'DELETE', headers });
    return { status: resp.status };
}

async function authenticate() {
    const body = JSON.stringify({ pfcon_user: 'pfcon', pfcon_password: 'pfcon1234' });
    const r = await pfconPost('/auth-token/', body, true);
    if (r.status === 200 && r.data.token) {
        token = r.data.token;
        appendStatus(`${ts()} Authenticated with pfcon`);
        return true;
    }
    appendStatus(`${ts()} <span style="color:var(--err)">Auth failed: ${JSON.stringify(r.data)}</span>`);
    return false;
}

async function pollUntilDone(path, label, stepIdx) {
    const maxPolls = 120;
    for (let i = 0; i < maxPolls; i++) {
        await sleep(2000);
        const r = await pfconGet(path);
        const status = r.data?.compute?.status;
        appendStatus(`${ts()} Poll ${label}: ${statusBadge(status)}`);
        if (status === 'finishedSuccessfully' || status === 'finishedWithError') {
            return status;
        }
        if (status === 'undefined') {
            return status;
        }
    }
    return 'timeout';
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function formEncode(obj) {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(obj)) {
        if (Array.isArray(v)) { v.forEach(item => params.append(k, item)); }
        else { params.append(k, v); }
    }
    return params.toString();
}

// ---------------------------------------------------------------------------
// Full workflow
// ---------------------------------------------------------------------------

async function runFullWorkflow() {
    const btn = document.getElementById('btn-run');
    btn.disabled = true;

    const jid = document.getElementById('job-id').value.trim();
    const image = document.getElementById('plugin-image').value.trim();
    const type = document.getElementById('plugin-type').value;
    const cpuLimit = document.getElementById('cpu-limit').value;
    const memLimit = document.getElementById('mem-limit').value;
    const entrypoint = document.getElementById('entrypoint').value.split(',').map(s => s.trim());
    const args = document.getElementById('plugin-args').value.split(',').map(s => s.trim());
    const inputDirs = document.getElementById('input-dirs').value.trim();
    const outputDir = document.getElementById('output-dir').value.trim();

    try {
        // Connect SSE before starting
        connectSSE();

        // 1. Authenticate
        stepActive(0);
        if (!await authenticate()) { stepError(0); return; }
        stepDone(0);

        // 2. Schedule copy
        stepActive(1);
        const copyBody = formEncode({ jid, input_dirs: inputDirs, output_dir: outputDir, cpu_limit: cpuLimit, memory_limit: memLimit });
        const copyResp = await pfconPost('/copyjobs/', copyBody);
        appendStatus(`${ts()} Copy scheduled: ${statusBadge(copyResp.data?.compute?.status || 'unknown')}`);
        if (copyResp.status !== 201) { stepError(1); appendStatus(`${ts()} Copy failed: ${JSON.stringify(copyResp.data)}`); return; }
        stepDone(1);

        // 3. Poll copy
        stepActive(2);
        const copySkipped = copyResp.data?.compute?.message === 'copySkipped' || copyResp.data?.compute?.status === 'finishedSuccessfully';
        if (!copySkipped) {
            const copyStatus = await pollUntilDone(`/copyjobs/${jid}/`, 'copy', 2);
            if (copyStatus !== 'finishedSuccessfully') { stepError(2); appendStatus(`${ts()} Copy ended: ${copyStatus}`); return; }
        }
        stepDone(2);

        // 4. Schedule plugin
        stepActive(3);
        const pluginParams = {
            jid, image, type,
            auid: 'cube',
            number_of_workers: '1',
            cpu_limit: cpuLimit,
            memory_limit: memLimit,
            gpu_limit: '0',
            entrypoint,
            args,
            input_dirs: inputDirs,
            output_dir: outputDir,
        };
        const pluginResp = await pfconPost('/pluginjobs/', formEncode(pluginParams));
        appendStatus(`${ts()} Plugin scheduled: ${statusBadge(pluginResp.data?.compute?.status || 'unknown')}`);
        if (pluginResp.status !== 201) { stepError(3); appendStatus(`${ts()} Plugin schedule failed: ${JSON.stringify(pluginResp.data)}`); return; }
        stepDone(3);

        // 5. Poll plugin
        stepActive(4);
        const pluginStatus = await pollUntilDone(`/pluginjobs/${jid}/`, 'plugin', 4);
        appendStatus(`${ts()} Plugin finished: ${statusBadge(pluginStatus)}`);
        stepDone(4);

        // 6. Get file list
        stepActive(5);
        const fileResp = await pfconGet(`/pluginjobs/${jid}/file/?job_output_path=${encodeURIComponent(outputDir)}`);
        if (fileResp.status === 200) {
            const files = fileResp.data?.rel_file_paths || [];
            appendStatus(`${ts()} Output files: ${files.join(', ') || '(none)'}`);
        }
        stepDone(5);

        // 7. Upload (no-op for fslink)
        stepActive(6);
        const uploadResp = await pfconPost('/uploadjobs/', formEncode({ jid, job_output_path: outputDir }));
        appendStatus(`${ts()} Upload: ${statusBadge(uploadResp.data?.compute?.status || 'unknown')} (${uploadResp.data?.compute?.message || ''})`);
        stepDone(6);

        // 8. Delete
        stepActive(7);
        const deleteResp = await pfconPost('/deletejobs/', formEncode({ jid }));
        appendStatus(`${ts()} Delete scheduled: ${statusBadge(deleteResp.data?.compute?.status || 'unknown')}`);
        stepDone(7);

        if (deleteResp.data?.compute?.status !== 'finishedSuccessfully') {
            stepActive(8);
            await pollUntilDone(`/deletejobs/${jid}/`, 'delete', 8);
            stepDone(8);
        } else {
            stepDone(8);
        }

        // 9. Cleanup containers
        stepActive(9);
        await pfconDelete(`/copyjobs/${jid}/`);
        await pfconDelete(`/pluginjobs/${jid}/`);
        await pfconDelete(`/uploadjobs/${jid}/`);
        await pfconDelete(`/deletejobs/${jid}/`);
        appendStatus(`${ts()} All containers removed`);
        stepDone(9);

        appendStatus(`${ts()} <strong style="color:var(--ok)">Workflow complete!</strong>`);

    } catch (err) {
        appendStatus(`${ts()} <span style="color:var(--err)">Error: ${err.message}</span>`);
        if (currentStep >= 0) stepError(currentStep);
    } finally {
        btn.disabled = false;
    }
}

// ---------------------------------------------------------------------------
// SSE connection
// ---------------------------------------------------------------------------

function connectSSE() {
    const jid = document.getElementById('job-id').value.trim();
    disconnectSSE();

    const stateEl = document.getElementById('sse-state');

    // Status stream
    statusSource = new EventSource(`${SSE_BASE}/events/${jid}/status`);
    statusSource.addEventListener('status', (e) => {
        try {
            const data = JSON.parse(e.data);
            appendStatus(`${ts()} SSE ${statusBadge(data.status)} job_type=${data.job_type} exit=${data.exit_code ?? '-'}`);
        } catch { appendStatus(`${ts()} SSE raw: ${e.data}`); }
    });
    statusSource.onopen = () => { stateEl.textContent = 'connected'; stateEl.style.color = 'var(--ok)'; };
    statusSource.onerror = () => { stateEl.textContent = 'reconnecting...'; stateEl.style.color = 'var(--err)'; };

    // Log stream
    logSource = new EventSource(`${SSE_BASE}/events/${jid}/logs`);
    logSource.addEventListener('logs', (e) => {
        try {
            const data = JSON.parse(e.data);
            const streamClass = data.stream === 'stderr' ? 'stream-stderr' : '';
            appendLog(`<span class="log-line"><span class="ts">[${data.timestamp?.substring(11, 23) || ''}]</span> <span class="${streamClass}">${escapeHtml(data.line)}</span></span>`);
        } catch { appendLog(`<span class="log-line">${escapeHtml(e.data)}</span>`); }
    });
}

function disconnectSSE() {
    if (statusSource) { statusSource.close(); statusSource = null; }
    if (logSource) { logSource.close(); logSource = null; }
    document.getElementById('sse-state').textContent = 'disconnected';
    document.getElementById('sse-state').style.color = 'var(--muted)';
}

function escapeHtml(s) {
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
}
