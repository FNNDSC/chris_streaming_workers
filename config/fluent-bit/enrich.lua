-- ============================================================================
-- Lua filter for Fluent Bit: enriches Docker log records with ChRIS metadata.
--
-- Reads the Docker container config file to get container name and labels,
-- filters to only ChRIS job containers, and reshapes the record to match
-- the LogEvent Pydantic schema.
--
-- Container metadata is cached per container_id to avoid repeated disk reads.
-- ============================================================================

local CHRIS_LABEL = "org.chrisproject.miniChRIS"
local JOB_TYPE_LABEL = "org.chrisproject.job_type"

-- Suffixes used by pfcon to name job containers
local SUFFIXES = {"-upload", "-delete", "-copy"}

-- Cache: container_id -> {name, job_id, job_type, is_chris}
local cache = {}

-- Read and parse a JSON-ish config file for container metadata.
-- Docker stores config in /var/lib/docker/containers/<id>/config.v2.json
-- We do a simple string-based extraction to avoid needing a JSON library.
local function read_container_meta(container_id)
    if cache[container_id] then
        return cache[container_id]
    end

    local config_path = "/var/lib/docker/containers/" .. container_id .. "/config.v2.json"
    local f = io.open(config_path, "r")
    if not f then
        cache[container_id] = {is_chris = false}
        return cache[container_id]
    end

    local content = f:read("*a")
    f:close()

    -- Check for ChRIS label
    if not content:find(CHRIS_LABEL) then
        cache[container_id] = {is_chris = false}
        return cache[container_id]
    end

    -- Extract container name (format: "Name":"/container-name")
    local name = content:match('"Name":"/?([^"]+)"')
    if not name then
        cache[container_id] = {is_chris = false}
        return cache[container_id]
    end

    -- Determine job_type from label or name
    local job_type = content:match('"' .. JOB_TYPE_LABEL .. '":"([^"]+)"')
    local job_id = name

    if not job_type then
        -- Fall back to name parsing
        job_type = "plugin"
        for _, suffix in ipairs(SUFFIXES) do
            if name:sub(-#suffix) == suffix then
                job_id = name:sub(1, -#suffix - 1)
                job_type = suffix:sub(2)
                break
            end
        end
    else
        -- Still need to extract job_id from name
        for _, suffix in ipairs(SUFFIXES) do
            if name:sub(-#suffix) == suffix then
                job_id = name:sub(1, -#suffix - 1)
                break
            end
        end
    end

    local meta = {
        is_chris = true,
        name = name,
        job_id = job_id,
        job_type = job_type,
    }
    cache[container_id] = meta
    return meta
end

-- Extract container ID from the log file path
local function extract_container_id(log_path)
    if not log_path then return nil end
    -- Path format: /var/lib/docker/containers/<container_id>/<container_id>-json.log
    return log_path:match("/var/lib/docker/containers/([^/]+)/")
end

function enrich_log(tag, timestamp, record)
    -- Extract container ID from path or tag
    local container_id = extract_container_id(record["log_path"])
    if not container_id then
        -- Try from tag: docker.<container_id>
        container_id = tag:match("^docker%.(.+)$")
    end

    if not container_id then
        return -1, 0, 0  -- drop
    end

    local meta = read_container_meta(container_id)
    if not meta.is_chris then
        return -1, 0, 0  -- drop: not a ChRIS container
    end

    -- Build output record matching LogEvent schema
    local new_record = {
        job_id = meta.job_id,
        job_type = meta.job_type,
        container_name = meta.name,
        line = record["log"] or "",
        stream = record["stream"] or "stdout",
        timestamp = record["time"] or os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }

    return 1, timestamp, new_record
end
