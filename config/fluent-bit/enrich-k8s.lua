-- ============================================================================
-- Lua filter for Fluent Bit on Kubernetes.
--
-- Consumes records enriched by the built-in kubernetes filter. Input record
-- shape (relevant keys):
--   record.log                    - the log line
--   record.stream                 - "stdout" | "stderr"
--   record.time                   - original timestamp (from cri parser)
--   record.kubernetes.labels      - pod labels dict
--   record.kubernetes.pod_name    - pod name (K8s Jobs generate pods as
--                                             <jobname>-<rand>, e.g.
--                                             "myjob-abc-copy-xyz12")
--
-- Keeps only records whose pod labels include
-- chrisproject.org/role=plugininstance. Extracts job_id/job_type from the
-- chrisproject.org/job-type label (preferred) and the owner Job name (parsed
-- for -copy/-upload/-delete suffixes).
-- ============================================================================

local ROLE_LABEL   = "chrisproject.org/role"
local ROLE_VALUE   = "plugininstance"
local JOB_TYPE_LBL = "chrisproject.org/job-type"

local SUFFIXES = {"-upload", "-delete", "-copy"}

-- Strip the "-<random>" suffix that K8s appends to Job pods to recover the
-- owner Job name. K8s pod names are <jobname>-<5-char-hash>, e.g.
--   "abc123-copy-hsk2p"  -> "abc123-copy"
local function pod_to_job_name(pod_name)
    if not pod_name then return nil end
    -- Trim trailing "-<hash>"
    local job = pod_name:match("^(.-)%-[a-z0-9]+$")
    return job or pod_name
end

-- Parse job_id/job_type from a Job name using the -copy/-upload/-delete suffix.
local function parse_job_name(name)
    for _, suffix in ipairs(SUFFIXES) do
        if name:sub(-#suffix) == suffix then
            return name:sub(1, -#suffix - 1), suffix:sub(2)
        end
    end
    return name, "plugin"
end

function enrich_log(tag, timestamp, record)
    local kube = record["kubernetes"]
    if type(kube) ~= "table" then
        return -1, 0, 0
    end

    local labels = kube["labels"] or {}
    if labels[ROLE_LABEL] ~= ROLE_VALUE then
        return -1, 0, 0
    end

    local pod_name = kube["pod_name"]
    local job_name = pod_to_job_name(pod_name)
    local job_id, job_type = parse_job_name(job_name or "")

    -- Label takes precedence over name-parsed job_type.
    if labels[JOB_TYPE_LBL] then
        job_type = labels[JOB_TYPE_LBL]
    end

    local new_record = {
        job_id         = job_id,
        job_type       = job_type,
        container_name = job_name or pod_name or "",
        line           = record["log"] or "",
        stream         = record["stream"] or "stdout",
        timestamp      = record["time"] or os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }

    return 1, timestamp, new_record
end
