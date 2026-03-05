-- tpcds: TPC-DS v4 benchmark extension for Apache Cloudberry
-- SQL-driven: gen_schema → gen_data → load_data → gen_query → bench

-- =============================================================================
-- Schemas
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS tpcds_ext;

-- =============================================================================
-- Config table (key-value store)
-- =============================================================================
CREATE TABLE tpcds.config (
    key   TEXT PRIMARY KEY,
    value TEXT
);

INSERT INTO tpcds.config (key, value) VALUES
    ('tpcds_dir', ''),
    ('data_dir', ''),
    ('query_dir', ''),
    ('results_dir', ''),
    ('tmp_dir', ''),
    ('use_partition', 'false'),
    ('storage_type', ''),
    ('scale_factor', ''),
    ('parallel', ''),
    ('workers', ''),
    ('gpfdist_base_port', ''),
    ('optimizer', ''),
    ('timeout_sec', '');

-- =============================================================================
-- Queries table — populated by gen_query()
-- =============================================================================
CREATE TABLE tpcds.query (
    query_id   INTEGER,
    query_text TEXT NOT NULL
) DISTRIBUTED REPLICATED;

-- =============================================================================
-- Benchmark results table (historical, appended each run)
-- =============================================================================
CREATE TABLE tpcds.bench_results (
    run_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    query_id      INTEGER NOT NULL,
    status        TEXT NOT NULL,
    duration_ms   NUMERIC NOT NULL,
    rows_returned BIGINT NOT NULL,
    optimizer     TEXT,
    scale_factor  INTEGER,
    storage_type  TEXT
) DISTRIBUTED REPLICATED;

-- =============================================================================
-- Benchmark summary table (latest run only, updated each bench())
-- =============================================================================
CREATE TABLE tpcds.bench_summary (
    query_id      INTEGER,
    status        TEXT NOT NULL,
    duration_ms   NUMERIC NOT NULL,
    rows_returned BIGINT NOT NULL,
    optimizer     TEXT,
    scale_factor  INTEGER,
    storage_type  TEXT,
    run_ts        TIMESTAMPTZ NOT NULL DEFAULT now()
) DISTRIBUTED REPLICATED;

-- =============================================================================
-- _get_config(key) — read config value, raise if not set
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._get_config(cfg_key TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _val TEXT;
BEGIN
    SELECT value INTO _val FROM tpcds.config WHERE key = cfg_key;
    IF _val IS NULL OR _val = '' THEN
        RAISE EXCEPTION 'tpcds.config key "%" is not set. Run: SELECT tpcds.config(''%'', ''...'');',
            cfg_key, cfg_key;
    END IF;
    RETURN _val;
END;
$func$;

-- =============================================================================
-- _resolve_dir(cfg_key, default_subdir) — config override or auto-detect
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._resolve_dir(cfg_key TEXT, default_subdir TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _val TEXT;
    _sharedir TEXT;
BEGIN
    SELECT value INTO _val FROM tpcds.config WHERE key = cfg_key;
    IF _val IS NOT NULL AND _val <> '' THEN
        RETURN _val;
    END IF;
    SELECT setting INTO _sharedir FROM pg_config() WHERE name = 'SHAREDIR';
    RETURN _sharedir || '/extension/' || default_subdir;
END;
$func$;

-- =============================================================================
-- _resolve_tmp_dir() — return coordinator-side temp directory
--   If tmp_dir config set → use it; otherwise → {PGDATA}/tpcds_tmp
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._resolve_tmp_dir()
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _val TEXT;
BEGIN
    SELECT value INTO _val FROM tpcds.config WHERE key = 'tmp_dir';
    IF _val IS NOT NULL AND _val <> '' THEN
        RETURN _val;
    END IF;
    RETURN current_setting('data_directory') || '/tpcds_tmp';
END;
$func$;

-- =============================================================================
-- _conf_path() — return path to tpcds.conf on disk
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._conf_path()
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sharedir TEXT;
BEGIN
    SELECT setting INTO _sharedir FROM pg_config() WHERE name = 'SHAREDIR';
    RETURN _sharedir || '/extension/tpcds.conf';
END;
$func$;

-- =============================================================================
-- _load_conf() — read tpcds.conf and populate config table
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._load_conf()
RETURNS void
LANGUAGE plpgsql
AS $func$
DECLARE
    _path TEXT;
    _lines TEXT[];
    _line TEXT;
    _key TEXT;
    _val TEXT;
    _m TEXT[];
BEGIN
    _path := tpcds._conf_path();

    -- Read file into temp table
    BEGIN
        CREATE TEMP TABLE _conf_tmp (line TEXT) ON COMMIT DROP DISTRIBUTED RANDOMLY;
        EXECUTE format('COPY _conf_tmp FROM %L', _path);
    EXCEPTION WHEN OTHERS THEN
        -- File doesn't exist or can't be read — no-op
        BEGIN DROP TABLE IF EXISTS _conf_tmp; EXCEPTION WHEN OTHERS THEN NULL; END;
        RETURN;
    END;

    -- Parse lines
    FOR _line IN SELECT t.line FROM _conf_tmp t LOOP
        -- Skip blank lines and pure comments
        IF _line IS NULL OR trim(_line) = '' OR trim(_line) LIKE '#%' THEN
            -- But check for commented-out key = value (# key = value) — skip those
            CONTINUE;
        END IF;

        -- Match: key = value (value may be empty)
        _m := regexp_match(trim(_line), '^(\w+)\s*=\s*(.*)$');
        IF _m IS NOT NULL THEN
            _key := _m[1];
            _val := trim(_m[2]);
            -- Upsert into config table
            UPDATE tpcds.config SET value = _val WHERE key = _key;
            IF NOT FOUND THEN
                INSERT INTO tpcds.config (key, value) VALUES (_key, _val);
            END IF;
        END IF;
    END LOOP;

    DROP TABLE IF EXISTS _conf_tmp;
END;
$func$;

-- =============================================================================
-- _save_conf() — write config table back to tpcds.conf, preserving comments
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._save_conf()
RETURNS void
LANGUAGE plpgsql
AS $func$
DECLARE
    _path TEXT;
    _lines TEXT[];
    _line TEXT;
    _m TEXT[];
    _key TEXT;
    _val TEXT;
    _found_keys TEXT[] := '{}';
    _i INTEGER;
    _output TEXT;
    _rec RECORD;
BEGIN
    _path := tpcds._conf_path();

    -- Read existing file lines
    BEGIN
        CREATE TEMP TABLE _conf_read (line_no SERIAL, line TEXT) DISTRIBUTED RANDOMLY;
        EXECUTE format('COPY _conf_read (line) FROM %L', _path);
    EXCEPTION WHEN OTHERS THEN
        BEGIN DROP TABLE IF EXISTS _conf_read; EXCEPTION WHEN OTHERS THEN NULL; END;
        -- No existing file — build from scratch
        _lines := '{}';
        FOR _rec IN SELECT c.key, c.value FROM tpcds.config c ORDER BY c.key LOOP
            IF _rec.value IS NOT NULL AND _rec.value <> '' THEN
                _lines := _lines || (_rec.key || ' = ' || _rec.value);
            ELSE
                _lines := _lines || ('# ' || _rec.key || ' =');
            END IF;
        END LOOP;
        CREATE TEMP TABLE _conf_write (line_no SERIAL, data TEXT) ON COMMIT DROP DISTRIBUTED RANDOMLY;
        INSERT INTO _conf_write (data) SELECT unnest(_lines);
        EXECUTE format('COPY (SELECT data FROM _conf_write ORDER BY line_no) TO PROGRAM %L', 'cat > ' || _path);
        DROP TABLE _conf_write;
        RETURN;
    END;

    -- Read lines into array
    SELECT array_agg(t.line ORDER BY t.line_no) INTO _lines FROM _conf_read t;
    DROP TABLE _conf_read;

    IF _lines IS NULL THEN
        _lines := '{}';
    END IF;

    -- Update existing lines in-place
    FOR _i IN 1..array_length(_lines, 1) LOOP
        _line := _lines[_i];

        -- Match active line: key = value
        _m := regexp_match(_line, '^\s*(\w+)\s*=');
        IF _m IS NOT NULL THEN
            _key := _m[1];
            SELECT c.value INTO _val FROM tpcds.config c WHERE c.key = _key;
            IF FOUND THEN
                _found_keys := _found_keys || _key;
                IF _val IS NOT NULL AND _val <> '' THEN
                    _lines[_i] := _key || ' = ' || _val;
                ELSE
                    _lines[_i] := '# ' || _key || ' =';
                END IF;
            END IF;
            CONTINUE;
        END IF;

        -- Match commented line: # key = [value]
        _m := regexp_match(_line, '^\s*#\s*(\w+)\s*=');
        IF _m IS NOT NULL THEN
            _key := _m[1];
            SELECT c.value INTO _val FROM tpcds.config c WHERE c.key = _key;
            IF FOUND THEN
                _found_keys := _found_keys || _key;
                IF _val IS NOT NULL AND _val <> '' THEN
                    _lines[_i] := _key || ' = ' || _val;
                ELSE
                    _lines[_i] := '# ' || _key || ' =';
                END IF;
            END IF;
        END IF;
    END LOOP;

    -- Append keys not found in file
    FOR _rec IN SELECT c.key, c.value FROM tpcds.config c
                WHERE NOT (c.key = ANY(_found_keys)) ORDER BY c.key LOOP
        IF _rec.value IS NOT NULL AND _rec.value <> '' THEN
            _lines := _lines || (_rec.key || ' = ' || _rec.value);
        ELSE
            _lines := _lines || ('# ' || _rec.key || ' =');
        END IF;
    END LOOP;

    -- Write back: one row per line so COPY outputs actual newlines
    CREATE TEMP TABLE _conf_write (line_no SERIAL, data TEXT) ON COMMIT DROP DISTRIBUTED RANDOMLY;
    INSERT INTO _conf_write (data) SELECT unnest(_lines);
    EXECUTE format('COPY (SELECT data FROM _conf_write ORDER BY line_no) TO PROGRAM %L', 'cat > ' || _path);
    DROP TABLE _conf_write;
END;
$func$;

-- =============================================================================
-- _host_tmp_dir(hostname) — return temp directory for a segment host
--   If tmp_dir config set → use it; otherwise → first segment datadir's tpcds_tmp
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._host_tmp_dir(p_hostname TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _val TEXT;
    _datadir TEXT;
BEGIN
    SELECT value INTO _val FROM tpcds.config WHERE key = 'tmp_dir';
    IF _val IS NOT NULL AND _val <> '' THEN
        RETURN _val;
    END IF;
    SELECT g.datadir INTO _datadir
    FROM gp_segment_configuration g
    WHERE g.content >= 0 AND g.role = 'p' AND g.hostname = p_hostname
    ORDER BY g.content
    LIMIT 1;
    RETURN _datadir || '/tpcds_tmp';
END;
$func$;

-- =============================================================================
-- config(key) — get config value
-- config(key, value) — set config value (upsert)
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.config(cfg_key TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _val TEXT;
BEGIN
    SELECT value INTO _val FROM tpcds.config WHERE key = cfg_key;
    RETURN _val;
END;
$func$;

CREATE OR REPLACE FUNCTION tpcds.config(cfg_key TEXT, cfg_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
BEGIN
    UPDATE tpcds.config SET value = cfg_value WHERE key = cfg_key;
    IF NOT FOUND THEN
        INSERT INTO tpcds.config (key, value) VALUES (cfg_key, cfg_value);
    END IF;
    -- Persist to tpcds.conf
    BEGIN
        PERFORM tpcds._save_conf();
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'tpcds: could not save to conf file: %', SQLERRM;
    END;
    RETURN cfg_value;
END;
$func$;

-- =============================================================================
-- _get_segment_info() — return segment host/datadir info
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._get_segment_info()
RETURNS TABLE(seg_id INTEGER, hostname TEXT, datadir TEXT)
LANGUAGE plpgsql
AS $func$
BEGIN
    RETURN QUERY
    SELECT g.content, g.hostname::TEXT, g.datadir::TEXT
    FROM gp_segment_configuration g
    WHERE g.content >= 0 AND g.role = 'p'
    ORDER BY g.content;
END;
$func$;

-- =============================================================================
-- info() — show resolved configuration and cluster info
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.info()
RETURNS TABLE(key TEXT, value TEXT)
LANGUAGE plpgsql
AS $func$
BEGIN
    RETURN QUERY
    SELECT 'conf_file'::TEXT,    tpcds._conf_path()
    UNION ALL
    SELECT 'tpcds_dir'::TEXT,    tpcds._resolve_dir('tpcds_dir', 'tpcds_dsgen')
    UNION ALL
    SELECT 'data_dir',
           COALESCE(NULLIF((SELECT c.value FROM tpcds.config c WHERE c.key = 'data_dir'), ''), '(auto: segment datadirs)')
    UNION ALL
    SELECT 'query_dir', tpcds._resolve_dir('query_dir', 'tpcds_query')
    UNION ALL
    SELECT 'results_dir', tpcds._resolve_dir('results_dir', 'tpcds_results')
    UNION ALL
    SELECT 'chart_tool', tpcds._resolve_dir('', 'tpcds_chart') || '/gen_chart.py'
    UNION ALL
    SELECT 'scale_factor',
           COALESCE((SELECT c.value FROM tpcds.config c WHERE c.key = 'scale_factor'), '(not set)')
    UNION ALL
    SELECT 'tmp_dir', tpcds._resolve_tmp_dir()
    UNION ALL
    SELECT 'use_partition',
           COALESCE(NULLIF((SELECT c.value FROM tpcds.config c WHERE c.key = 'use_partition'), ''), 'false')
    UNION ALL
    SELECT 'storage_type',
           COALESCE(NULLIF((SELECT c.value FROM tpcds.config c WHERE c.key = 'storage_type'), ''), '(not set)')
    UNION ALL
    SELECT 'parallel',
           COALESCE(NULLIF((SELECT c.value FROM tpcds.config c WHERE c.key = 'parallel'), ''), '(not set)')
    UNION ALL
    SELECT 'workers',
           COALESCE(NULLIF((SELECT c.value FROM tpcds.config c WHERE c.key = 'workers'), ''), '(not set)')
    UNION ALL
    SELECT 'optimizer',
           COALESCE(NULLIF((SELECT c.value FROM tpcds.config c WHERE c.key = 'optimizer'), ''), '(inherit)')
    UNION ALL
    SELECT 'timeout_sec',
           COALESCE(NULLIF((SELECT c.value FROM tpcds.config c WHERE c.key = 'timeout_sec'), ''), '(none)')
    UNION ALL
    SELECT 'num_segments',
           (SELECT count(*)::TEXT FROM gp_segment_configuration WHERE content >= 0 AND role = 'p')
    UNION ALL
    SELECT 'segment_hosts',
           (SELECT string_agg(DISTINCT hostname, ', ' ORDER BY hostname)
            FROM gp_segment_configuration WHERE content >= 0 AND role = 'p');
END;
$func$;

-- =============================================================================
-- check() — pre-benchmark cluster & OS health checks
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.check()
RETURNS TABLE(item TEXT, status TEXT, detail TEXT)
LANGUAGE plpgsql
AS $func$
DECLARE
    _total   INT;
    _up      INT;
    _has_mirrors BOOLEAN;
    _not_synced  INT;
    _hosts   TEXT[];
    _h       TEXT;
    _line    TEXT;
    _val     TEXT;
    _avail   INT;
    _datadir TEXT;
    _governor TEXT;
    _thp      TEXT;
    _low_hosts TEXT[];
    _bad_gov_hosts TEXT[];
    _thp_hosts TEXT[];
    _setting  TEXT;
    _max_cnt  NUMERIC;
    _avg_cnt  NUMERIC;
    _ratio    NUMERIC;
BEGIN
    -- 1. Segments: all primaries up?
    SELECT count(*), count(*) FILTER (WHERE g.status = 'u')
    INTO _total, _up
    FROM gp_segment_configuration g
    WHERE g.content >= 0 AND g.role = 'p';

    IF _up = _total THEN
        item := 'segments'; status := 'OK';
        detail := _up::TEXT || '/' || _total::TEXT || ' primaries up';
    ELSE
        item := 'segments'; status := 'FAIL';
        detail := _up::TEXT || '/' || _total::TEXT || ' primaries up — ' ||
            (_total - _up)::TEXT || ' DOWN';
    END IF;
    RETURN NEXT;

    -- 2. Mirrors: exist? synced?
    SELECT EXISTS(
        SELECT 1 FROM gp_segment_configuration g WHERE g.content >= 0 AND g.role = 'm'
    ) INTO _has_mirrors;

    item := 'mirrors';
    IF NOT _has_mirrors THEN
        status := 'OK'; detail := 'no mirrors configured';
    ELSE
        SELECT count(*) INTO _not_synced
        FROM gp_segment_configuration g
        WHERE g.content >= 0 AND g.role = 'm' AND g.mode <> 's';

        IF _not_synced = 0 THEN
            status := 'OK'; detail := 'all mirrors synced';
        ELSE
            status := 'WARN'; detail := _not_synced::TEXT || ' mirror(s) not synced';
        END IF;
    END IF;
    RETURN NEXT;

    -- Collect distinct segment hosts
    SELECT array_agg(DISTINCT g.hostname ORDER BY g.hostname)
    INTO _hosts
    FROM gp_segment_configuration g
    WHERE g.content >= 0 AND g.role = 'p';

    -- 3. Disk space: >=50 GB free on first segment datadir per host
    _low_hosts := '{}';
    FOREACH _h IN ARRAY _hosts LOOP
        SELECT g.datadir INTO _datadir
        FROM gp_segment_configuration g
        WHERE g.content >= 0 AND g.role = 'p' AND g.hostname = _h
        ORDER BY g.content LIMIT 1;

        BEGIN
            CREATE TEMP TABLE _chk_tmp (line TEXT) DISTRIBUTED RANDOMLY;
            EXECUTE format(
                'COPY _chk_tmp FROM PROGRAM ''ssh -o StrictHostKeyChecking=no %s df -BG %s | tail -1''',
                _h, replace(_datadir, '''', ''''''''));
            SELECT t.line INTO _line FROM _chk_tmp t LIMIT 1;
            DROP TABLE _chk_tmp;

            _val := (regexp_matches(_line, '(\d+)G\s+\d+%'))[1];
            IF _val IS NOT NULL THEN
                _avail := _val::INT;
                IF _avail < 50 THEN
                    _low_hosts := _low_hosts || _h;
                END IF;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            BEGIN DROP TABLE IF EXISTS _chk_tmp; EXCEPTION WHEN OTHERS THEN NULL; END;
            _low_hosts := _low_hosts || (_h || '(ssh fail)');
        END;
    END LOOP;

    item := 'disk_space';
    IF array_length(_low_hosts, 1) IS NULL THEN
        status := 'OK'; detail := '>=50 GB free on all hosts';
    ELSE
        status := 'WARN'; detail := '<50 GB free: ' || array_to_string(_low_hosts, ', ');
    END IF;
    RETURN NEXT;

    -- 4. CPU governor
    _bad_gov_hosts := '{}';
    FOREACH _h IN ARRAY _hosts LOOP
        BEGIN
            CREATE TEMP TABLE _chk_tmp (line TEXT) DISTRIBUTED RANDOMLY;
            EXECUTE format(
                'COPY _chk_tmp FROM PROGRAM ''ssh -o StrictHostKeyChecking=no %s cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo __MISSING__''',
                _h);
            SELECT trim(t.line) INTO _governor FROM _chk_tmp t LIMIT 1;
            DROP TABLE _chk_tmp;

            IF _governor = '__MISSING__' THEN
                NULL; -- VM or no sysfs, skip
            ELSIF _governor <> 'performance' THEN
                _bad_gov_hosts := _bad_gov_hosts || (_h || '=' || _governor);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            BEGIN DROP TABLE IF EXISTS _chk_tmp; EXCEPTION WHEN OTHERS THEN NULL; END;
        END;
    END LOOP;

    item := 'cpu_governor';
    IF array_length(_bad_gov_hosts, 1) IS NULL THEN
        status := 'OK'; detail := 'performance (or VM)';
    ELSE
        status := 'WARN'; detail := array_to_string(_bad_gov_hosts, ', ');
    END IF;
    RETURN NEXT;

    -- 5. Transparent hugepages
    _thp_hosts := '{}';
    FOREACH _h IN ARRAY _hosts LOOP
        BEGIN
            CREATE TEMP TABLE _chk_tmp (line TEXT) DISTRIBUTED RANDOMLY;
            EXECUTE format(
                'COPY _chk_tmp FROM PROGRAM ''ssh -o StrictHostKeyChecking=no %s cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo __MISSING__''',
                _h);
            SELECT trim(t.line) INTO _thp FROM _chk_tmp t LIMIT 1;
            DROP TABLE _chk_tmp;

            IF _thp = '__MISSING__' THEN
                NULL;
            ELSIF _thp LIKE '%[always]%' THEN
                _thp_hosts := _thp_hosts || _h;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            BEGIN DROP TABLE IF EXISTS _chk_tmp; EXCEPTION WHEN OTHERS THEN NULL; END;
        END;
    END LOOP;

    item := 'transparent_hugepages';
    IF array_length(_thp_hosts, 1) IS NULL THEN
        status := 'OK'; detail := 'never (or VM)';
    ELSE
        status := 'WARN'; detail := 'THP=always on: ' || array_to_string(_thp_hosts, ', ');
    END IF;
    RETURN NEXT;

    -- 6. ORCA optimizer
    _setting := current_setting('optimizer', true);
    item := 'optimizer';
    IF _setting = 'on' THEN
        status := 'OK'; detail := 'on (ORCA)';
    ELSE
        status := 'WARN'; detail := coalesce(_setting, 'unknown') || ' — ORCA recommended';
    END IF;
    RETURN NEXT;

    -- 7. gp_vmem_protect_limit
    _setting := current_setting('gp_vmem_protect_limit', true);
    item := 'vmem_protect_limit';
    IF _setting IS NOT NULL AND _setting::INT >= 4096 THEN
        status := 'OK'; detail := _setting || ' MB';
    ELSE
        status := 'WARN'; detail := coalesce(_setting, 'unknown') || ' MB — recommend >=4096';
    END IF;
    RETURN NEXT;

    -- 8. statement_mem (info only)
    _setting := current_setting('statement_mem', true);
    item := 'statement_mem'; status := 'OK'; detail := coalesce(_setting, 'unknown');
    RETURN NEXT;

    -- 9. resource_manager (info only)
    _setting := current_setting('gp_resource_manager', true);
    item := 'resource_manager'; status := 'OK'; detail := coalesce(_setting, 'unknown');
    RETURN NEXT;

    -- 10. Data skew on store_sales (only if table exists)
    IF EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'tpcds' AND c.relname = 'store_sales'
    ) THEN
        BEGIN
            EXECUTE '
                SELECT max(cnt), avg(cnt)
                FROM (SELECT gp_segment_id, count(*) AS cnt
                      FROM tpcds.store_sales GROUP BY gp_segment_id) sub'
            INTO _max_cnt, _avg_cnt;

            IF _avg_cnt IS NOT NULL AND _avg_cnt > 0 THEN
                _ratio := round(_max_cnt / _avg_cnt, 2);
                item := 'data_skew';
                IF _ratio < 1.2 THEN
                    status := 'OK'; detail := 'store_sales max/avg=' || _ratio::TEXT;
                ELSE
                    status := 'WARN';
                    detail := 'store_sales max/avg=' || _ratio::TEXT || ' — skew detected';
                END IF;
                RETURN NEXT;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            NULL; -- table empty or other issue, skip
        END;
    END IF;
END;
$func$;

-- =============================================================================
-- _fix_query(qid, sql) — apply compatibility fixes to dsqgen output
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._fix_query(qid INTEGER, raw_sql TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT := raw_sql;
BEGIN
    -- Fix 1: Date intervals — "+ 14 days" → "+ interval '14 days'"
    _sql := regexp_replace(_sql,
        '([+-])\s*(\d+)\s+(days?|months?|years?)',
        E'\\1 interval ''\\2 \\3''', 'gi');

    -- Fix 2: c_last_review_date (query 30) — dsqgen uses wrong column name
    IF qid = 30 THEN
        _sql := replace(_sql, 'c_last_review_date_sk', 'c_last_review_date');
    END IF;

    -- Fix 3: GROUPING alias (queries 36, 70, 86) — expand lochierarchy references
    IF qid IN (36, 86) THEN
        _sql := replace(_sql, 'as lochierarchy', 'as __LOCHIER__');
        _sql := replace(_sql, 'lochierarchy', 'grouping(i_category)+grouping(i_class)');
        _sql := replace(_sql, '__LOCHIER__', 'lochierarchy');
    END IF;
    IF qid = 70 THEN
        _sql := replace(_sql, 'as lochierarchy', 'as __LOCHIER__');
        _sql := replace(_sql, 'lochierarchy', 'grouping(s_state)+grouping(s_county)');
        _sql := replace(_sql, '__LOCHIER__', 'lochierarchy');
    END IF;

    -- Fix 4: Div-by-zero (query 90) — wrap pmc cast in nullif
    IF qid = 90 THEN
        _sql := regexp_replace(_sql,
            '/(cast\s*\(\s*pmc\s+as\s+decimal\s*\([^)]+\)\s*\))',
            '/nullif(\1,0)', 'i');
    END IF;

    -- Fix 5: Missing FROM subquery aliases (CBDB requires them, PG does not)
    -- Query 2: union subquery in wscs CTE
    IF qid = 2 THEN
        _sql := replace(_sql, 'from catalog_sales)),', 'from catalog_sales) _sub),');
    END IF;

    -- Query 14: intersect subquery in cross_items CTE (first statement only)
    IF qid = 14 THEN
        _sql := regexp_replace(_sql,
            E'(\\+ 2\\))(\\s+where\\s+i_brand_id)',
            E'\\1 x\\2');
    END IF;

    -- Query 23: three unaliased FROM subqueries
    IF qid = 23 THEN
        -- max_store_sales CTE: (select ... group by c_customer_sk))
        _sql := replace(_sql, 'c_customer_sk)),', 'c_customer_sk) _sub),');
        -- statement 1 main: ...best_ss_customer)) before limit
        _sql := regexp_replace(_sql,
            E'(best_ss_customer\\)\\))(\\s+limit)',
            E'\\1 _sub\\2');
        -- statement 2 main: ...c_first_name) before order by
        _sql := regexp_replace(_sql,
            E'(c_last_name,c_first_name\\))(\\s+order\\s+by\\s+c_last_name)',
            E'\\1 _sub\\2');
    END IF;

    -- Query 49: outer union subquery before order by
    IF qid = 49 THEN
        _sql := regexp_replace(_sql,
            E'(\\)\\s+\\))(\\s+order\\s+by\\s+1,4,5,2)',
            E'\\1 _sub\\2');
    END IF;

    RETURN _sql;
END;
$func$;

-- =============================================================================
-- gen_schema(storage_type) — create 25 TPC-DS tables
--   storage_type: 'heap' (default), 'ao', 'aocs'
--   Respects use_partition config for fact table partitioning
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.gen_schema(
    p_storage_type TEXT DEFAULT NULL,
    p_use_partition BOOLEAN DEFAULT NULL
)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _tbl TEXT;
    _tables TEXT[] := ARRAY[
        'dbgen_version','customer_address','customer_demographics','date_dim',
        'warehouse','ship_mode','time_dim','reason','income_band','item',
        'store','call_center','customer','web_site','store_returns',
        'household_demographics','web_page','promotion','catalog_page',
        'inventory','catalog_returns','web_returns','web_sales',
        'catalog_sales','store_sales'
    ];
    _storage_type TEXT;
    _with_clause TEXT := '';
    _use_partition BOOLEAN;
    _dist TEXT;
    _part TEXT;
    _ddl TEXT;
BEGIN
    -- Resolve storage type: param → config → 'heap'
    IF p_storage_type IS NOT NULL THEN
        _storage_type := lower(p_storage_type);
    ELSE
        SELECT NULLIF(value, '') INTO _storage_type FROM tpcds.config WHERE key = 'storage_type';
        _storage_type := lower(coalesce(_storage_type, 'heap'));
    END IF;
    IF _storage_type NOT IN ('heap', 'ao', 'aocs') THEN
        RAISE EXCEPTION 'Invalid storage_type "%". Use heap, ao, or aocs.', p_storage_type;
    END IF;

    IF _storage_type = 'ao' THEN
        _with_clause := ' WITH (appendoptimized=true, orientation=row, compresstype=zstd, compresslevel=5)';
    ELSIF _storage_type = 'aocs' THEN
        _with_clause := ' WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=5)';
    END IF;

    -- Save storage_type to config
    PERFORM tpcds.config('storage_type', _storage_type);

    -- Resolve use_partition: param → config → false
    IF p_use_partition IS NOT NULL THEN
        _use_partition := p_use_partition;
    ELSE
        SELECT COALESCE(NULLIF(value, ''), 'false')::BOOLEAN INTO _use_partition
        FROM tpcds.config WHERE key = 'use_partition';
        IF _use_partition IS NULL THEN
            _use_partition := false;
        END IF;
    END IF;
    PERFORM tpcds.config('use_partition', _use_partition::TEXT);

    SET LOCAL client_min_messages = warning;
    FOREACH _tbl IN ARRAY _tables LOOP
        EXECUTE format('DROP TABLE IF EXISTS tpcds.%I CASCADE', _tbl);
    END LOOP;
    RESET client_min_messages;

    -- =========================================================================
    -- dbgen_version (small utility table, heap always)
    -- =========================================================================
    EXECUTE $ddl$
CREATE TABLE tpcds.dbgen_version (
    dv_version                varchar(16),
    dv_create_date            date,
    dv_create_time            time,
    dv_cmdline_args           varchar(200)
) DISTRIBUTED REPLICATED
    $ddl$;

    -- =========================================================================
    -- customer_address — DISTRIBUTED BY (ca_address_sk)
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.customer_address (
    ca_address_sk             integer               not null,
    ca_address_id             char(16)              not null,
    ca_street_number          char(10),
    ca_street_name            varchar(60),
    ca_street_type            char(15),
    ca_suite_number           char(10),
    ca_city                   varchar(60),
    ca_county                 varchar(30),
    ca_state                  char(2),
    ca_zip                    char(10),
    ca_country                varchar(20),
    ca_gmt_offset             decimal(5,2),
    ca_location_type          char(20)
)$col$ || _with_clause || ' DISTRIBUTED BY (ca_address_sk)';
    EXECUTE _ddl;

    -- =========================================================================
    -- customer_demographics — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.customer_demographics (
    cd_demo_sk                integer               not null,
    cd_gender                 char(1),
    cd_marital_status         char(1),
    cd_education_status       char(20),
    cd_purchase_estimate      integer,
    cd_credit_rating          char(10),
    cd_dep_count              integer,
    cd_dep_employed_count     integer,
    cd_dep_college_count      integer
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- date_dim — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.date_dim (
    d_date_sk                 integer               not null,
    d_date_id                 char(16)              not null,
    d_date                    date                  not null,
    d_month_seq               integer,
    d_week_seq                integer,
    d_quarter_seq             integer,
    d_year                    integer,
    d_dow                     integer,
    d_moy                     integer,
    d_dom                     integer,
    d_qoy                     integer,
    d_fy_year                 integer,
    d_fy_quarter_seq          integer,
    d_fy_week_seq             integer,
    d_day_name                char(9),
    d_quarter_name            char(6),
    d_holiday                 char(1),
    d_weekend                 char(1),
    d_following_holiday       char(1),
    d_first_dom               integer,
    d_last_dom                integer,
    d_same_day_ly             integer,
    d_same_day_lq             integer,
    d_current_day             char(1),
    d_current_week            char(1),
    d_current_month           char(1),
    d_current_quarter         char(1),
    d_current_year            char(1)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- warehouse — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.warehouse (
    w_warehouse_sk            integer               not null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_name          varchar(20),
    w_warehouse_sq_ft         integer,
    w_street_number           char(10),
    w_street_name             varchar(60),
    w_street_type             char(15),
    w_suite_number            char(10),
    w_city                    varchar(60),
    w_county                  varchar(30),
    w_state                   char(2),
    w_zip                     char(10),
    w_country                 varchar(20),
    w_gmt_offset              decimal(5,2)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- ship_mode — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.ship_mode (
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30),
    sm_code                   char(10),
    sm_carrier                char(20),
    sm_contract               char(20)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- time_dim — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.time_dim (
    t_time_sk                 integer               not null,
    t_time_id                 char(16)              not null,
    t_time                    integer               not null,
    t_hour                    integer,
    t_minute                  integer,
    t_second                  integer,
    t_am_pm                   char(2),
    t_shift                   char(20),
    t_sub_shift               char(20),
    t_meal_time               char(20)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- reason — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.reason (
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- income_band — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.income_band (
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer,
    ib_upper_bound            integer
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- item — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.item (
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_rec_start_date          date,
    i_rec_end_date            date,
    i_item_desc               varchar(200),
    i_current_price           decimal(7,2),
    i_wholesale_cost          decimal(7,2),
    i_brand_id                integer,
    i_brand                   char(50),
    i_class_id                integer,
    i_class                   char(50),
    i_category_id             integer,
    i_category                char(50),
    i_manufact_id             integer,
    i_manufact                char(50),
    i_size                    char(20),
    i_formulation             char(20),
    i_color                   char(20),
    i_units                   char(10),
    i_container               char(10),
    i_manager_id              integer,
    i_product_name            char(50)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- store — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.store (
    s_store_sk                integer               not null,
    s_store_id                char(16)              not null,
    s_rec_start_date          date,
    s_rec_end_date            date,
    s_closed_date_sk          integer,
    s_store_name              varchar(50),
    s_number_employees        integer,
    s_floor_space             integer,
    s_hours                   char(20),
    s_manager                 varchar(40),
    s_market_id               integer,
    s_geography_class         varchar(100),
    s_market_desc             varchar(100),
    s_market_manager          varchar(40),
    s_division_id             integer,
    s_division_name           varchar(50),
    s_company_id              integer,
    s_company_name            varchar(50),
    s_street_number           varchar(10),
    s_street_name             varchar(60),
    s_street_type             char(15),
    s_suite_number            char(10),
    s_city                    varchar(60),
    s_county                  varchar(30),
    s_state                   char(2),
    s_zip                     char(10),
    s_country                 varchar(20),
    s_gmt_offset              decimal(5,2),
    s_tax_precentage          decimal(5,2)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- call_center — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.call_center (
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_rec_start_date         date,
    cc_rec_end_date           date,
    cc_closed_date_sk         integer,
    cc_open_date_sk           integer,
    cc_name                   varchar(50),
    cc_class                  varchar(50),
    cc_employees              integer,
    cc_sq_ft                  integer,
    cc_hours                  char(20),
    cc_manager                varchar(40),
    cc_mkt_id                 integer,
    cc_mkt_class              char(50),
    cc_mkt_desc               varchar(100),
    cc_market_manager         varchar(40),
    cc_division               integer,
    cc_division_name          varchar(50),
    cc_company                integer,
    cc_company_name           char(50),
    cc_street_number          char(10),
    cc_street_name            varchar(60),
    cc_street_type            char(15),
    cc_suite_number           char(10),
    cc_city                   varchar(60),
    cc_county                 varchar(30),
    cc_state                  char(2),
    cc_zip                    char(10),
    cc_country                varchar(20),
    cc_gmt_offset             decimal(5,2),
    cc_tax_percentage         decimal(5,2)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- customer — DISTRIBUTED BY (c_customer_sk)
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.customer (
    c_customer_sk             integer               not null,
    c_customer_id             char(16)              not null,
    c_current_cdemo_sk        integer,
    c_current_hdemo_sk        integer,
    c_current_addr_sk         integer,
    c_first_shipto_date_sk    integer,
    c_first_sales_date_sk     integer,
    c_salutation              char(10),
    c_first_name              char(20),
    c_last_name               char(30),
    c_preferred_cust_flag     char(1),
    c_birth_day               integer,
    c_birth_month             integer,
    c_birth_year              integer,
    c_birth_country           varchar(20),
    c_login                   char(13),
    c_email_address           char(50),
    c_last_review_date        char(10)
)$col$ || _with_clause || ' DISTRIBUTED BY (c_customer_sk)';
    EXECUTE _ddl;

    -- =========================================================================
    -- web_site — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.web_site (
    web_site_sk               integer               not null,
    web_site_id               char(16)              not null,
    web_rec_start_date        date,
    web_rec_end_date          date,
    web_name                  varchar(50),
    web_open_date_sk          integer,
    web_close_date_sk         integer,
    web_class                 varchar(50),
    web_manager               varchar(40),
    web_mkt_id                integer,
    web_mkt_class             varchar(50),
    web_mkt_desc              varchar(100),
    web_market_manager        varchar(40),
    web_company_id            integer,
    web_company_name          char(50),
    web_street_number         char(10),
    web_street_name           varchar(60),
    web_street_type           char(15),
    web_suite_number          char(10),
    web_city                  varchar(60),
    web_county                varchar(30),
    web_state                 char(2),
    web_zip                   char(10),
    web_country               varchar(20),
    web_gmt_offset            decimal(5,2),
    web_tax_percentage        decimal(5,2)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- store_returns — DISTRIBUTED BY (sr_item_sk)
    -- Optional: PARTITION BY RANGE(sr_returned_date_sk)
    -- =========================================================================
    _part := '';
    IF _use_partition::BOOLEAN THEN
        _part := ' PARTITION BY RANGE(sr_returned_date_sk) (start(2450815) INCLUSIVE end(2453005) INCLUSIVE every(100), default partition others)';
    END IF;
    _ddl := $col$
CREATE TABLE tpcds.store_returns (
    sr_returned_date_sk       integer,
    sr_return_time_sk         integer,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer,
    sr_cdemo_sk               integer,
    sr_hdemo_sk               integer,
    sr_addr_sk                integer,
    sr_store_sk               integer,
    sr_reason_sk              integer,
    sr_ticket_number          bigint                not null,
    sr_return_quantity        integer,
    sr_return_amt             decimal(7,2),
    sr_return_tax             decimal(7,2),
    sr_return_amt_inc_tax     decimal(7,2),
    sr_fee                    decimal(7,2),
    sr_return_ship_cost       decimal(7,2),
    sr_refunded_cash          decimal(7,2),
    sr_reversed_charge        decimal(7,2),
    sr_store_credit           decimal(7,2),
    sr_net_loss               decimal(7,2)
)$col$ || _with_clause || ' DISTRIBUTED BY (sr_item_sk)' || _part;
    EXECUTE _ddl;

    -- =========================================================================
    -- household_demographics — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.household_demographics (
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer,
    hd_buy_potential          char(15),
    hd_dep_count              integer,
    hd_vehicle_count          integer
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- web_page — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.web_page (
    wp_web_page_sk            integer               not null,
    wp_web_page_id            char(16)              not null,
    wp_rec_start_date         date,
    wp_rec_end_date           date,
    wp_creation_date_sk       integer,
    wp_access_date_sk         integer,
    wp_autogen_flag           char(1),
    wp_customer_sk            integer,
    wp_url                    varchar(100),
    wp_type                   char(50),
    wp_char_count             integer,
    wp_link_count             integer,
    wp_image_count            integer,
    wp_max_ad_count           integer
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- promotion — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.promotion (
    p_promo_sk                integer               not null,
    p_promo_id                char(16)              not null,
    p_start_date_sk           integer,
    p_end_date_sk             integer,
    p_item_sk                 integer,
    p_cost                    decimal(15,2),
    p_response_target         integer,
    p_promo_name              char(50),
    p_channel_dmail           char(1),
    p_channel_email           char(1),
    p_channel_catalog         char(1),
    p_channel_tv              char(1),
    p_channel_radio           char(1),
    p_channel_press           char(1),
    p_channel_event           char(1),
    p_channel_demo            char(1),
    p_channel_details         varchar(100),
    p_purpose                 char(15),
    p_discount_active         char(1)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- catalog_page — DISTRIBUTED REPLICATED
    -- =========================================================================
    _ddl := $col$
CREATE TABLE tpcds.catalog_page (
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        char(16)              not null,
    cp_start_date_sk          integer,
    cp_end_date_sk            integer,
    cp_department             varchar(50),
    cp_catalog_number         integer,
    cp_catalog_page_number    integer,
    cp_description            varchar(100),
    cp_type                   varchar(100)
)$col$ || _with_clause || ' DISTRIBUTED REPLICATED';
    EXECUTE _ddl;

    -- =========================================================================
    -- inventory — DISTRIBUTED BY (inv_item_sk)
    -- Optional: PARTITION BY RANGE(inv_date_sk)
    -- =========================================================================
    _part := '';
    IF _use_partition::BOOLEAN THEN
        _part := ' PARTITION BY RANGE(inv_date_sk) (start(2450815) INCLUSIVE end(2453005) INCLUSIVE every(100), default partition others)';
    END IF;
    _ddl := $col$
CREATE TABLE tpcds.inventory (
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer
)$col$ || _with_clause || ' DISTRIBUTED BY (inv_item_sk)' || _part;
    EXECUTE _ddl;

    -- =========================================================================
    -- catalog_returns — DISTRIBUTED BY (cr_item_sk)
    -- Optional: PARTITION BY RANGE(cr_returned_date_sk)
    -- =========================================================================
    _part := '';
    IF _use_partition::BOOLEAN THEN
        _part := ' PARTITION BY RANGE(cr_returned_date_sk) (start(2450815) INCLUSIVE end(2453005) INCLUSIVE every(8), default partition others)';
    END IF;
    _ddl := $col$
CREATE TABLE tpcds.catalog_returns (
    cr_returned_date_sk       integer,
    cr_returned_time_sk       integer,
    cr_item_sk                integer               not null,
    cr_refunded_customer_sk   integer,
    cr_refunded_cdemo_sk      integer,
    cr_refunded_hdemo_sk      integer,
    cr_refunded_addr_sk       integer,
    cr_returning_customer_sk  integer,
    cr_returning_cdemo_sk     integer,
    cr_returning_hdemo_sk     integer,
    cr_returning_addr_sk      integer,
    cr_call_center_sk         integer,
    cr_catalog_page_sk        integer,
    cr_ship_mode_sk           integer,
    cr_warehouse_sk           integer,
    cr_reason_sk              integer,
    cr_order_number           bigint                not null,
    cr_return_quantity        integer,
    cr_return_amount          decimal(7,2),
    cr_return_tax             decimal(7,2),
    cr_return_amt_inc_tax     decimal(7,2),
    cr_fee                    decimal(7,2),
    cr_return_ship_cost       decimal(7,2),
    cr_refunded_cash          decimal(7,2),
    cr_reversed_charge        decimal(7,2),
    cr_store_credit           decimal(7,2),
    cr_net_loss               decimal(7,2)
)$col$ || _with_clause || ' DISTRIBUTED BY (cr_item_sk)' || _part;
    EXECUTE _ddl;

    -- =========================================================================
    -- web_returns — DISTRIBUTED BY (wr_item_sk)
    -- Optional: PARTITION BY RANGE(wr_returned_date_sk)
    -- =========================================================================
    _part := '';
    IF _use_partition::BOOLEAN THEN
        _part := ' PARTITION BY RANGE(wr_returned_date_sk) (start(2450815) INCLUSIVE end(2453005) INCLUSIVE every(180), default partition others)';
    END IF;
    _ddl := $col$
CREATE TABLE tpcds.web_returns (
    wr_returned_date_sk       integer,
    wr_returned_time_sk       integer,
    wr_item_sk                integer               not null,
    wr_refunded_customer_sk   integer,
    wr_refunded_cdemo_sk      integer,
    wr_refunded_hdemo_sk      integer,
    wr_refunded_addr_sk       integer,
    wr_returning_customer_sk  integer,
    wr_returning_cdemo_sk     integer,
    wr_returning_hdemo_sk     integer,
    wr_returning_addr_sk      integer,
    wr_web_page_sk            integer,
    wr_reason_sk              integer,
    wr_order_number           bigint                not null,
    wr_return_quantity        integer,
    wr_return_amt             decimal(7,2),
    wr_return_tax             decimal(7,2),
    wr_return_amt_inc_tax     decimal(7,2),
    wr_fee                    decimal(7,2),
    wr_return_ship_cost       decimal(7,2),
    wr_refunded_cash          decimal(7,2),
    wr_reversed_charge        decimal(7,2),
    wr_account_credit         decimal(7,2),
    wr_net_loss               decimal(7,2)
)$col$ || _with_clause || ' DISTRIBUTED BY (wr_item_sk)' || _part;
    EXECUTE _ddl;

    -- =========================================================================
    -- web_sales — DISTRIBUTED BY (ws_item_sk)
    -- Optional: PARTITION BY RANGE(ws_sold_date_sk)
    -- =========================================================================
    _part := '';
    IF _use_partition::BOOLEAN THEN
        _part := ' PARTITION BY RANGE(ws_sold_date_sk) (start(2450815) INCLUSIVE end(2453005) INCLUSIVE every(40), default partition others)';
    END IF;
    _ddl := $col$
CREATE TABLE tpcds.web_sales (
    ws_sold_date_sk           integer,
    ws_sold_time_sk           integer,
    ws_ship_date_sk           integer,
    ws_item_sk                integer               not null,
    ws_bill_customer_sk       integer,
    ws_bill_cdemo_sk          integer,
    ws_bill_hdemo_sk          integer,
    ws_bill_addr_sk           integer,
    ws_ship_customer_sk       integer,
    ws_ship_cdemo_sk          integer,
    ws_ship_hdemo_sk          integer,
    ws_ship_addr_sk           integer,
    ws_web_page_sk            integer,
    ws_web_site_sk            integer,
    ws_ship_mode_sk           integer,
    ws_warehouse_sk           integer,
    ws_promo_sk               integer,
    ws_order_number           bigint                not null,
    ws_quantity               integer,
    ws_wholesale_cost         decimal(7,2),
    ws_list_price             decimal(7,2),
    ws_sales_price            decimal(7,2),
    ws_ext_discount_amt       decimal(7,2),
    ws_ext_sales_price        decimal(7,2),
    ws_ext_wholesale_cost     decimal(7,2),
    ws_ext_list_price         decimal(7,2),
    ws_ext_tax                decimal(7,2),
    ws_coupon_amt             decimal(7,2),
    ws_ext_ship_cost          decimal(7,2),
    ws_net_paid               decimal(7,2),
    ws_net_paid_inc_tax       decimal(7,2),
    ws_net_paid_inc_ship      decimal(7,2),
    ws_net_paid_inc_ship_tax  decimal(7,2),
    ws_net_profit             decimal(7,2)
)$col$ || _with_clause || ' DISTRIBUTED BY (ws_item_sk)' || _part;
    EXECUTE _ddl;

    -- =========================================================================
    -- catalog_sales — DISTRIBUTED BY (cs_item_sk)
    -- Optional: PARTITION BY RANGE(cs_sold_date_sk)
    -- =========================================================================
    _part := '';
    IF _use_partition::BOOLEAN THEN
        _part := ' PARTITION BY RANGE(cs_sold_date_sk) (start(2450815) INCLUSIVE end(2453005) INCLUSIVE every(28), default partition others)';
    END IF;
    _ddl := $col$
CREATE TABLE tpcds.catalog_sales (
    cs_sold_date_sk           integer,
    cs_sold_time_sk           integer,
    cs_ship_date_sk           integer,
    cs_bill_customer_sk       integer,
    cs_bill_cdemo_sk          integer,
    cs_bill_hdemo_sk          integer,
    cs_bill_addr_sk           integer,
    cs_ship_customer_sk       integer,
    cs_ship_cdemo_sk          integer,
    cs_ship_hdemo_sk          integer,
    cs_ship_addr_sk           integer,
    cs_call_center_sk         integer,
    cs_catalog_page_sk        integer,
    cs_ship_mode_sk           integer,
    cs_warehouse_sk           integer,
    cs_item_sk                integer               not null,
    cs_promo_sk               integer,
    cs_order_number           bigint                not null,
    cs_quantity               integer,
    cs_wholesale_cost         decimal(7,2),
    cs_list_price             decimal(7,2),
    cs_sales_price            decimal(7,2),
    cs_ext_discount_amt       decimal(7,2),
    cs_ext_sales_price        decimal(7,2),
    cs_ext_wholesale_cost     decimal(7,2),
    cs_ext_list_price         decimal(7,2),
    cs_ext_tax                decimal(7,2),
    cs_coupon_amt             decimal(7,2),
    cs_ext_ship_cost          decimal(7,2),
    cs_net_paid               decimal(7,2),
    cs_net_paid_inc_tax       decimal(7,2),
    cs_net_paid_inc_ship      decimal(7,2),
    cs_net_paid_inc_ship_tax  decimal(7,2),
    cs_net_profit             decimal(7,2)
)$col$ || _with_clause || ' DISTRIBUTED BY (cs_item_sk)' || _part;
    EXECUTE _ddl;

    -- =========================================================================
    -- store_sales — DISTRIBUTED BY (ss_item_sk)
    -- Optional: PARTITION BY RANGE(ss_sold_date_sk)
    -- =========================================================================
    _part := '';
    IF _use_partition::BOOLEAN THEN
        _part := ' PARTITION BY RANGE(ss_sold_date_sk) (start(2450815) INCLUSIVE end(2453005) INCLUSIVE every(10), default partition others)';
    END IF;
    _ddl := $col$
CREATE TABLE tpcds.store_sales (
    ss_sold_date_sk           integer,
    ss_sold_time_sk           integer,
    ss_item_sk                integer               not null,
    ss_customer_sk            integer,
    ss_cdemo_sk               integer,
    ss_hdemo_sk               integer,
    ss_addr_sk                integer,
    ss_store_sk               integer,
    ss_promo_sk               integer,
    ss_ticket_number          bigint                not null,
    ss_quantity               integer,
    ss_wholesale_cost         decimal(7,2),
    ss_list_price             decimal(7,2),
    ss_sales_price            decimal(7,2),
    ss_ext_discount_amt       decimal(7,2),
    ss_ext_sales_price        decimal(7,2),
    ss_ext_wholesale_cost     decimal(7,2),
    ss_ext_list_price         decimal(7,2),
    ss_ext_tax                decimal(7,2),
    ss_coupon_amt             decimal(7,2),
    ss_net_paid               decimal(7,2),
    ss_net_paid_inc_tax       decimal(7,2),
    ss_net_profit             decimal(7,2)
)$col$ || _with_clause || ' DISTRIBUTED BY (ss_item_sk)' || _part;
    EXECUTE _ddl;

    RETURN format('Created 25 TPC-DS tables in tpcds schema (storage=%s, partition=%s)',
                  _storage_type, _use_partition);
END;
$func$;

-- =============================================================================
-- gen_data(scale, parallel) — generate .dat files on segment hosts via SSH
--   Runs dsdgen in parallel across all segments.
--   TOTAL_PARALLEL = num_primary_segments × parallel
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.gen_data(scale_factor INTEGER DEFAULT NULL, parallel INTEGER DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _tpcds_dir    TEXT;
    _rec          RECORD;
    _num_segs     INTEGER;
    _total_par    INTEGER;
    _child        INTEGER := 1;
    _data_subdir  TEXT := 'dsgendata_tpcds';
    _start_ts     TIMESTAMPTZ;
    _dsdgen_bin   TEXT;
    _tpcds_idx    TEXT;
    _seg_hosts    TEXT[];
    _host         TEXT;
    _cmd          TEXT;
    _count        INTEGER;
    _poll_start   TIMESTAMPTZ;
    _tmp_dir      TEXT;
    _host_tmp     TEXT;
    _pid          TEXT;
    _script_lines TEXT[];
    _script_path  TEXT;
BEGIN
    -- Resolve from config if NULL
    IF scale_factor IS NULL THEN
        SELECT NULLIF(value, '')::INTEGER INTO scale_factor FROM tpcds.config WHERE key = 'scale_factor';
        scale_factor := coalesce(scale_factor, 1);
    END IF;
    IF parallel IS NULL THEN
        SELECT NULLIF(value, '')::INTEGER INTO parallel FROM tpcds.config WHERE key = 'parallel';
        parallel := coalesce(parallel, 2);
    END IF;

    IF scale_factor < 1 THEN
        RAISE EXCEPTION 'scale_factor must be >= 1';
    END IF;
    IF parallel < 1 THEN
        RAISE EXCEPTION 'parallel must be >= 1';
    END IF;

    _tpcds_dir := tpcds._resolve_dir('tpcds_dir', 'tpcds_dsgen');
    _dsdgen_bin := _tpcds_dir || '/tools/dsdgen';
    _tpcds_idx := _tpcds_dir || '/tools/tpcds.idx';
    _tmp_dir := tpcds._resolve_tmp_dir();
    _pid := pg_backend_pid()::TEXT;

    -- Get segment count
    SELECT count(*) INTO _num_segs
    FROM gp_segment_configuration
    WHERE content >= 0 AND role = 'p';

    _total_par := _num_segs * parallel;

    RAISE NOTICE 'gen_data: SF=%, segments=%, parallel_per_seg=%, total_parallel=%',
        scale_factor, _num_segs, parallel, _total_par;

    _start_ts := clock_timestamp();

    -- Ensure coordinator tmp_dir exists
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _tmp_dir);

    -- Get distinct segment hosts
    SELECT array_agg(DISTINCT hostname ORDER BY hostname) INTO _seg_hosts
    FROM gp_segment_configuration
    WHERE content >= 0 AND role = 'p';

    -- Copy dsdgen binary and tpcds.idx to each segment host
    FOREACH _host IN ARRAY _seg_hosts LOOP
        _host_tmp := tpcds._host_tmp_dir(_host);
        _cmd := format('ssh -o StrictHostKeyChecking=no %s "mkdir -p %s/tools"',
                       _host, _host_tmp);
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', _cmd);

        _cmd := format('scp -o StrictHostKeyChecking=no -q %s %s %s:%s/tools/',
                       _dsdgen_bin, _tpcds_idx, _host, _host_tmp);
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', _cmd);
    END LOOP;

    -- Build one launch script per host: create dirs + launch all dsdgen workers at once
    FOREACH _host IN ARRAY _seg_hosts LOOP
        _host_tmp := tpcds._host_tmp_dir(_host);
        _script_lines := ARRAY[
            '#!/bin/bash',
            'set -e'
        ];

        -- Create data directories for each segment on this host
        FOR _rec IN
            SELECT g.content AS seg_id, g.datadir
            FROM gp_segment_configuration g
            WHERE g.content >= 0 AND g.role = 'p' AND g.hostname = _host
            ORDER BY g.content
        LOOP
            _script_lines := _script_lines || format('mkdir -p %s/%s', _rec.datadir, _data_subdir);

            -- Launch parallel dsdgen workers for this segment
            FOR _child IN (_rec.seg_id * parallel + 1) .. ((_rec.seg_id + 1) * parallel) LOOP
                _script_lines := _script_lines || format(
                    'cd %s/tools && ./dsdgen -scale %s -dir %s/%s -parallel %s -child %s -RNGSEED 2016032410 -terminate n > /dev/null 2>&1 &',
                    _host_tmp, scale_factor,
                    _rec.datadir, _data_subdir,
                    _total_par, _child
                );
            END LOOP;
        END LOOP;

        _script_lines := _script_lines || ARRAY[
            'disown -a',
            format('echo "%s: launched dsdgen workers"', _host)
        ];

        -- Write script locally, SCP to host, execute via SSH
        _script_path := format('%s/tpcds_%s_dsdgen_%s.sh', _tmp_dir, _pid, _host);
        EXECUTE format(
            'COPY (SELECT line FROM unnest(%L::text[]) AS line) TO PROGRAM %L WITH (FORMAT text)',
            _script_lines, 'cat > ' || _script_path
        );

        -- Ensure remote tmp dir and copy script
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            format('ssh -o StrictHostKeyChecking=no %s "mkdir -p %s"', _host, _host_tmp));
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            format('scp -o StrictHostKeyChecking=no -q %s %s:%s/', _script_path, _host, _host_tmp));

        -- Execute the script (single SSH per host, all workers launched at once)
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            format('ssh -o StrictHostKeyChecking=no %s "bash %s/tpcds_%s_dsdgen_%s.sh"',
                   _host, _host_tmp, _pid, _host));

        RAISE NOTICE 'gen_data: launched dsdgen workers on %', _host;
    END LOOP;

    -- Poll for completion: check if any dsdgen processes are still running
    RAISE NOTICE 'gen_data: all dsdgen workers launched, waiting for completion...';
    _poll_start := clock_timestamp();
    LOOP
        PERFORM pg_sleep(5);
        _count := 0;
        FOREACH _host IN ARRAY _seg_hosts LOOP
            BEGIN
                _cmd := format(
                    'ssh -o StrictHostKeyChecking=no -n %s "pgrep -c dsdgen 2>/dev/null || echo 0"',
                    _host
                );
                CREATE TEMP TABLE IF NOT EXISTS _poll_out (line TEXT) ON COMMIT DROP;
                TRUNCATE _poll_out;
                EXECUTE format('COPY _poll_out FROM PROGRAM %L', _cmd);
                SELECT _count + COALESCE(btrim(line)::INTEGER, 0) INTO _count FROM _poll_out LIMIT 1;
            EXCEPTION WHEN OTHERS THEN
                NULL;
            END;
        END LOOP;

        IF _count = 0 THEN
            EXIT;
        END IF;

        RAISE NOTICE 'gen_data: % dsdgen processes still running (% sec elapsed)...',
            _count,
            round(extract(epoch from clock_timestamp() - _poll_start)::numeric, 0);
    END LOOP;

    -- Clean up temp table and scripts
    SET LOCAL client_min_messages = warning;
    DROP TABLE IF EXISTS _poll_out;
    RESET client_min_messages;

    BEGIN
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            'rm -f ' || _tmp_dir || '/tpcds_' || _pid || '_dsdgen_*.sh');
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    -- Save config
    PERFORM tpcds.config('scale_factor', scale_factor::TEXT);
    PERFORM tpcds.config('parallel', parallel::TEXT);
    PERFORM tpcds.config('total_parallel', _total_par::TEXT);
    PERFORM tpcds.config('data_subdir', _data_subdir);

    RAISE NOTICE 'gen_data: completed in % seconds',
        round(extract(epoch from clock_timestamp() - _start_ts)::numeric, 1);

    RETURN format('Generated TPC-DS data at SF=%s (total_parallel=%s, %s segments) in %s sec',
        scale_factor, _total_par, _num_segs,
        round(extract(epoch from clock_timestamp() - _start_ts)::numeric, 1));
END;
$func$;

-- =============================================================================
-- load_data(workers) — load data via gpfdist external tables
--   1. Start gpfdist on each segment host
--   2. Create external tables in tpcds_ext schema
--   3. INSERT INTO tpcds.table SELECT * FROM tpcds_ext.table (parallel)
--   4. Stop gpfdist, ANALYZE, clean up
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.load_data(workers INTEGER DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _tables TEXT[] := ARRAY[
        'dbgen_version','customer_address','customer_demographics','date_dim',
        'warehouse','ship_mode','time_dim','reason','income_band','item',
        'store','call_center','customer','web_site','store_returns',
        'household_demographics','web_page','promotion','catalog_page',
        'inventory','catalog_returns','web_returns','web_sales',
        'catalog_sales','store_sales'
    ];
    _data_subdir  TEXT;
    _base_port    INTEGER;
    _rec          RECORD;
    _seg_hosts    TEXT[];
    _host         TEXT;
    _tbl          TEXT;
    _cmd          TEXT;
    _locations    TEXT;
    _port         INTEGER;
    _start_ts     TIMESTAMPTZ;
    _pid          TEXT;
    _dbname       TEXT;
    _user         TEXT;
    _db_port      TEXT;
    _socket       TEXT;
    _psql_base    TEXT;
    _logfile      TEXT;
    _errfile      TEXT;
    _main_sh      TEXT;
    _setup_sql    TEXT;
    _script       TEXT[];
    _setup_lines  TEXT[];
    _total_rows   BIGINT;
    _env_file     TEXT;
    _gphome       TEXT;
    _gpfdist_lines  TEXT[];
    _gpfdist_sh     TEXT;
    _tmp_dir      TEXT;
    _host_tmp     TEXT;
    _host_idx     INTEGER;
BEGIN
    -- Resolve workers from config if NULL
    IF workers IS NULL THEN
        SELECT NULLIF(value, '')::INTEGER INTO workers FROM tpcds.config WHERE key = 'workers';
        workers := coalesce(workers, 4);
    END IF;

    _start_ts := clock_timestamp();

    -- Check for HTTP_PROXY — segments inherit env from postmaster; if proxy
    -- is set, gpfdist external table reads will fail with HTTP 502.
    DECLARE _proxy TEXT;
    BEGIN
        CREATE TEMP TABLE _proxy_chk (line TEXT) ON COMMIT DROP DISTRIBUTED RANDOMLY;
        EXECUTE 'COPY _proxy_chk FROM PROGRAM ''env | grep -i "^http_proxy=" || true''';
        SELECT line INTO _proxy FROM _proxy_chk WHERE line IS NOT NULL AND line <> '' LIMIT 1;
        DROP TABLE _proxy_chk;
        IF _proxy IS NOT NULL THEN
            RAISE EXCEPTION E'HTTP_PROXY is set in the database server environment (%).\n'
                'Segments will route gpfdist traffic through the proxy, causing 502 errors.\n'
                'Fix: restart the cluster with proxy unset:\n'
                '  unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY\n'
                '  gpstop -a && gpstart -a', _proxy;
        END IF;
    EXCEPTION
        WHEN raise_exception THEN RAISE;  -- re-raise our own exception
        WHEN OTHERS THEN NULL;  -- env check failed, proceed anyway
    END;

    -- Resolve config
    SELECT COALESCE(NULLIF(value, ''), 'dsgendata_tpcds') INTO _data_subdir
    FROM tpcds.config WHERE key = 'data_subdir';
    IF _data_subdir IS NULL THEN
        _data_subdir := 'dsgendata_tpcds';
    END IF;

    -- Auto-select base port: 20000 + (pid % 10000), or use config override
    SELECT NULLIF(value, '')::INTEGER INTO _base_port
    FROM tpcds.config WHERE key = 'gpfdist_base_port';
    IF _base_port IS NULL THEN
        _base_port := 20000 + (pg_backend_pid() % 10000);
    END IF;

    _tmp_dir := tpcds._resolve_tmp_dir();

    -- Get connection info
    _pid     := pg_backend_pid()::TEXT;
    _dbname  := current_database();
    _user    := current_user;
    _logfile := _tmp_dir || '/tpcds_load_' || _pid || '.log';
    _errfile := _tmp_dir || '/tpcds_load_' || _pid || '.err';
    SELECT setting INTO _db_port FROM pg_settings WHERE name = 'port';
    SELECT trim(split_part(setting, ',', 1)) INTO _socket
        FROM pg_settings WHERE name = 'unix_socket_directories';
    _psql_base := 'psql -h ' || _socket || ' -p ' || _db_port ||
                  ' -U ' || _user || ' -d ' || _dbname ||
                  ' -v ON_ERROR_STOP=1';

    -- Find GPHOME and env file
    SELECT setting INTO _gphome FROM pg_config() WHERE name = 'BINDIR';
    _gphome := regexp_replace(_gphome, '/bin$', '');

    -- Try to find the environment file
    _env_file := '';
    IF _env_file = '' THEN
        BEGIN
            EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
                format('test -f %s/greenplum_path.sh', _gphome));
            _env_file := _gphome || '/greenplum_path.sh';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END IF;
    IF _env_file = '' THEN
        BEGIN
            EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
                format('test -f %s/cloudberry_path.sh', _gphome));
            _env_file := _gphome || '/cloudberry_path.sh';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END IF;
    IF _env_file = '' THEN
        BEGIN
            EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
                format('test -f %s/cloudberry-env.sh', _gphome));
            _env_file := _gphome || '/cloudberry-env.sh';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END IF;

    -- Get distinct segment hosts
    SELECT array_agg(DISTINCT hostname ORDER BY hostname) INTO _seg_hosts
    FROM gp_segment_configuration
    WHERE content >= 0 AND role = 'p';

    -- Ensure coordinator tmp_dir exists
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _tmp_dir);

    -- =====================================================================
    -- Phase 1: Write per-host gpfdist startup scripts
    -- One gpfdist per segment, each serving its own datadir directly.
    -- gpfdist started from shell script (Phase 3), not PL/pgSQL.
    -- =====================================================================
    RAISE NOTICE 'load_data: preparing gpfdist startup scripts...';
    _port := _base_port;
    _locations := '';

    FOREACH _host IN ARRAY _seg_hosts LOOP
        _host_tmp := tpcds._host_tmp_dir(_host);

        _gpfdist_lines := ARRAY[
            '#!/bin/bash',
            'unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY'
        ];
        IF _env_file <> '' THEN
            _gpfdist_lines := _gpfdist_lines || ARRAY[format('source %s', _env_file)];
        END IF;

        -- Kill any existing gpfdist
        _gpfdist_lines := _gpfdist_lines || ARRAY['pkill -f "gpfdist.*tpcds" 2>/dev/null || true'];
        _gpfdist_lines := _gpfdist_lines || ARRAY['sleep 1'];
        _gpfdist_lines := _gpfdist_lines || ARRAY[format('mkdir -p %s', _host_tmp)];

        -- One gpfdist per segment: each serves its own datadir
        FOR _rec IN
            SELECT g.content AS seg_id, g.hostname, g.datadir
            FROM gp_segment_configuration g
            WHERE g.content >= 0 AND g.role = 'p' AND g.hostname = _host
            ORDER BY g.content
        LOOP
            -- Touch empty sentinel .dat for tables missing from this segment's datadir
            -- (small tables like reason, income_band only exist on certain segments)
            _gpfdist_lines := _gpfdist_lines || ARRAY[format(
                'for t in %s; do f=$(ls %s/%s/${t}_[0-9]*_[0-9]*.dat 2>/dev/null | head -1); [ -n "$f" ] || touch %s/%s/${t}_0_0.dat; done',
                array_to_string(_tables, ' '),
                _rec.datadir, _data_subdir,
                _rec.datadir, _data_subdir)];

            -- Auto-select port with retry
            _gpfdist_lines := _gpfdist_lines || ARRAY[
                format('PORT=%s', _port),
                'for attempt in $(seq 1 20); do',
                '    if ss -tln 2>/dev/null | grep -qw ":$PORT"; then',
                '        PORT=$((PORT + 1))',
                '    else',
                '        break',
                '    fi',
                'done',
                format('echo $PORT > %s/gpfdist_seg%s_%s.port', _host_tmp, _rec.seg_id, _pid),
                format('nohup gpfdist -d %s/%s -p $PORT </dev/null >%s/gpfdist_seg%s_%s.log 2>&1 &',
                       _rec.datadir, _data_subdir, _host_tmp, _rec.seg_id, _pid)
            ];

            -- LOCATION placeholder per segment
            IF _locations <> '' THEN
                _locations := _locations || ', ';
            END IF;
            _locations := _locations || format('''gpfdist://%s:__PORT_seg%s__/TABLENAME_[0-9]*_[0-9]*.dat''',
                                               _host, _rec.seg_id);
            _port := _port + 1;
        END LOOP;

        _gpfdist_lines := _gpfdist_lines || ARRAY['disown -a',
            format('echo "gpfdist: %s segments started on %s"',
                   (SELECT count(*) FROM gp_segment_configuration g
                    WHERE g.content >= 0 AND g.role = 'p' AND g.hostname = _host),
                   _host)];

        -- Write the per-host startup script locally, then SCP to remote host
        _gpfdist_sh := format('%s/tpcds_%s_gpfdist_%s.sh', _tmp_dir, _pid, _host);
        EXECUTE format(
            'COPY (SELECT line FROM unnest(%L::text[]) AS line) TO PROGRAM %L WITH (FORMAT text)',
            _gpfdist_lines,
            'cat > ' || _gpfdist_sh
        );
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            format('ssh -o StrictHostKeyChecking=no %s "mkdir -p %s"', _host, _host_tmp));
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            format('scp -o StrictHostKeyChecking=no -q %s %s:%s/', _gpfdist_sh, _host, _host_tmp));
    END LOOP;

    -- =====================================================================
    -- Phase 2: Write setup SQL file
    -- All DDL (TRUNCATE, CREATE EXT TABLE, DROP EXT TABLE) runs from the
    -- shell script in separate connections, so this function holds NO locks
    -- on tpcds tables and won't block parallel INSERT workers.
    -- =====================================================================
    RAISE NOTICE 'load_data: building setup SQL and load script...';
    _setup_sql := _tmp_dir || '/tpcds_' || _pid || '_setup.sql';

    -- Build setup SQL: drop old ext tables + create new ones + truncate target
    _setup_lines := ARRAY[
        '-- Auto-generated setup for TPC-DS load'
    ];

    -- Drop existing external tables
    FOREACH _tbl IN ARRAY _tables LOOP
        _setup_lines := _setup_lines || ARRAY[format('DROP EXTERNAL TABLE IF EXISTS tpcds_ext.%I CASCADE;', _tbl)];
    END LOOP;

    -- Truncate all target tables
    _setup_lines := _setup_lines || ARRAY[(
        'TRUNCATE ' || (
            SELECT string_agg('tpcds.' || quote_ident(t), ', ')
            FROM unnest(_tables) AS t
        ) || ' CASCADE;'
    )];

    -- Create external tables with explicit LIKE
    FOREACH _tbl IN ARRAY _tables LOOP
        _setup_lines := _setup_lines || ARRAY[format(
            'CREATE EXTERNAL TABLE tpcds_ext.%I (LIKE tpcds.%I) LOCATION (%s) FORMAT ''TEXT'' (DELIMITER ''|'' NULL AS '''' ESCAPE ''off'' );',
            _tbl, _tbl, replace(_locations, 'TABLENAME', _tbl)
        )];
    END LOOP;

    -- Write setup SQL file
    EXECUTE format(
        'COPY (SELECT line FROM unnest(%L::text[]) AS line) TO PROGRAM %L WITH (FORMAT text)',
        _setup_lines,
        'cat > ' || _setup_sql
    );

    -- =====================================================================
    -- Phase 3: Build and execute shell script for parallel load
    -- =====================================================================
    _main_sh := _tmp_dir || '/tpcds_' || _pid || '_load.sh';
    _script := ARRAY[
        '#!/bin/bash',
        '# Unset proxy to prevent interference with gpfdist connections',
        'unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY',
        'MAX=' || workers,
        'PSQL="' || _psql_base || '"',
        'LOG="' || _logfile || '"',
        'ERR="' || _errfile || '"',
        'rm -f "$ERR"',
        '',
        'run_load() {',
        '    local tbl=$1 t0=$(date +%s)',
        '    $PSQL -c "INSERT INTO tpcds.$tbl SELECT * FROM tpcds_ext.$tbl" >> "$LOG" 2>&1',
        '    local rc=$? e=$(( $(date +%s) - t0 ))',
        '    if [ $rc -ne 0 ]; then echo "$tbl" >> "$ERR"; fi',
        '    echo "  LOAD $tbl ${e}s $([ $rc -eq 0 ] && echo OK || echo FAILED)" | tee -a "$LOG"',
        '}',
        '',
        'echo "=== TPC-DS load started: workers=' || workers || ', $(date) ===" | tee "$LOG"'
    ];

    -- Start gpfdist on each host (from within the shell script, not PL/pgSQL)
    _script := _script || ARRAY['', '# Start gpfdist instances on each segment host'];
    FOREACH _host IN ARRAY _seg_hosts LOOP
        _host_tmp := tpcds._host_tmp_dir(_host);
        _script := _script || ARRAY[format(
            'ssh -o StrictHostKeyChecking=no %s bash %s/tpcds_%s_gpfdist_%s.sh',
            _host, _host_tmp, _pid, _host)];
    END LOOP;
    _script := _script || ARRAY['sleep 2', 'echo "gpfdist instances started" | tee -a "$LOG"'];

    -- Read actual ports from per-segment .port files and substitute into setup SQL
    _script := _script || ARRAY['', '# Read actual gpfdist ports and substitute into setup SQL'];
    FOR _rec IN
        SELECT g.content AS seg_id, g.hostname
        FROM gp_segment_configuration g
        WHERE g.content >= 0 AND g.role = 'p'
        ORDER BY g.content
    LOOP
        _host_tmp := tpcds._host_tmp_dir(_rec.hostname);
        _script := _script || ARRAY[format(
            'ACTUAL_PORT_seg%s=$(ssh -o StrictHostKeyChecking=no %s cat %s/gpfdist_seg%s_%s.port)',
            _rec.seg_id, _rec.hostname, _host_tmp, _rec.seg_id, _pid)];
        _script := _script || ARRAY[format(
            'sed -i "s/__PORT_seg%s__/$ACTUAL_PORT_seg%s/g" "%s"',
            _rec.seg_id, _rec.seg_id, _setup_sql)];
    END LOOP;
    _script := _script || ARRAY['echo "gpfdist ports resolved" | tee -a "$LOG"'];

    -- Setup SQL
    _script := _script || ARRAY[
        '',
        '# Setup: TRUNCATE target tables + create external tables (separate connection, no lock conflict)',
        '$PSQL -f "' || _setup_sql || '" >> "$LOG" 2>&1',
        'if [ $? -ne 0 ]; then echo "SETUP FAILED" | tee -a "$LOG"; exit 1; fi',
        'echo "Setup done (truncated + created external tables)" | tee -a "$LOG"'
    ];

    FOREACH _tbl IN ARRAY _tables LOOP
        _script := _script || ARRAY[(
            'while [ $(jobs -r 2>/dev/null | wc -l) -ge $MAX ]; do sleep 0.2; done' ||
            '; run_load ' || _tbl || ' &'
        )];
    END LOOP;

    _script := _script || ARRAY[
        'wait',
        'echo "LOAD done: $(date)" | tee -a "$LOG"',
        '',
        '# Drop external tables',
        '$PSQL -c "' || (
            SELECT string_agg(format('DROP EXTERNAL TABLE IF EXISTS tpcds_ext.%I CASCADE;', t), ' ')
            FROM unnest(_tables) AS t
        ) || '" >> "$LOG" 2>&1',
        '',
        '# ANALYZE all tables',
        'echo "Starting ANALYZE..." | tee -a "$LOG"'
    ];

    FOREACH _tbl IN ARRAY _tables LOOP
        _script := _script || ARRAY[(
            'while [ $(jobs -r 2>/dev/null | wc -l) -ge $MAX ]; do sleep 0.2; done' ||
            '; $PSQL -c "ANALYZE tpcds.' || _tbl || ';" >> "$LOG" 2>&1 && echo "  ANALYZE ' || _tbl || ' done" | tee -a "$LOG" &'
        )];
    END LOOP;

    _script := _script || ARRAY['wait'];

    -- Stop gpfdist and clean up
    _script := _script || ARRAY['', '# Stop gpfdist and clean up'];
    FOREACH _host IN ARRAY _seg_hosts LOOP
        _host_tmp := tpcds._host_tmp_dir(_host);
        _script := _script || ARRAY[format(
            'ssh -o StrictHostKeyChecking=no %s "pkill -f gpfdist.*tpcds || true; rm -f %s/gpfdist_seg*_%s.port %s/gpfdist_seg*_%s.log %s/tpcds_%s_gpfdist_%s.sh" 2>/dev/null || true',
            _host, _host_tmp, _pid, _host_tmp, _pid, _host_tmp, _pid, _host)];
    END LOOP;

    _script := _script || ARRAY[
        '',
        'echo "=== All done: $(date) ===" | tee -a "$LOG"',
        'if [ -f "$ERR" ]; then',
        '    echo "FAILED:" && cat "$ERR" && exit 1',
        'fi',
        'exit 0'
    ];

    -- Write and execute the script
    EXECUTE format(
        'COPY (SELECT line FROM unnest(%L::text[]) AS line) TO PROGRAM %L WITH (FORMAT text)',
        _script,
        'cat > ' || _main_sh
    );

    RAISE NOTICE 'load_data: launching % parallel workers, log: %', workers, _logfile;
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'bash ' || _main_sh || ' 2>>' || _logfile);

    -- =====================================================================
    -- Phase 4: Clean up temp files (gpfdist already stopped by shell script)
    -- =====================================================================
    BEGIN
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            'rm -f ' || _main_sh || ' ' || _setup_sql || ' ' || _tmp_dir || '/tpcds_' || _pid || '_gpfdist_*.sh');
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    -- Row count from pg_class after ANALYZE
    SELECT sum(reltuples::BIGINT) INTO _total_rows
    FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = 'tpcds' AND c.relname = ANY(_tables);

    RETURN format('Loaded ~%s rows in %s sec (workers=%s). Log: %s',
        _total_rows,
        round(extract(epoch from clock_timestamp() - _start_ts)::numeric, 1),
        workers, _logfile);
END;
$func$;

-- =============================================================================
-- gen_query(scale) — generate 99 queries via dsqgen, fix, store
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.gen_query(scale INTEGER DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _tpcds_dir TEXT;
    _query_dir TEXT;
    _scale INTEGER;
    _cmd TEXT;
    _raw TEXT;
    _fixed TEXT;
    _i INTEGER;
    _count INTEGER := 0;
    _pid TEXT;
    _tmp_dir TEXT;
BEGIN
    _pid := pg_backend_pid()::TEXT;
    _tpcds_dir := tpcds._resolve_dir('tpcds_dir', 'tpcds_dsgen');
    _query_dir := tpcds._resolve_dir('query_dir', 'tpcds_query');
    _tmp_dir := tpcds._resolve_tmp_dir();

    -- Ensure tmp_dir exists
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _tmp_dir);

    -- Use explicit scale if provided, otherwise read from config
    IF scale IS NOT NULL THEN
        _scale := scale;
    ELSE
        SELECT value::INTEGER INTO _scale FROM tpcds.config WHERE key = 'scale_factor';
        IF _scale IS NULL THEN
            _scale := 1;
        END IF;
    END IF;

    DELETE FROM tpcds.query;

    -- Create queries output directory
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _query_dir);

    FOR _i IN 1..99 LOOP
        _cmd := _tpcds_dir || '/tools/dsqgen'
            || ' -TEMPLATE query' || _i || '.tpl'
            || ' -DIRECTORY ' || _tpcds_dir || '/query_templates'
            || ' -DIALECT postgres'
            || ' -SCALE ' || _scale
            || ' -FILTER Y -QUIET Y'
            || ' -DISTRIBUTIONS ' || _tpcds_dir || '/tools/tpcds.idx'
            || ' > ' || _tmp_dir || '/tpcds_dsqgen_' || _pid || '.sql 2>&1';

        BEGIN
            EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', _cmd);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'dsqgen failed for query %: %', _i, SQLERRM;
            CONTINUE;
        END;

        _raw := pg_read_file(_tmp_dir || '/tpcds_dsqgen_' || _pid || '.sql');

        IF _raw IS NULL OR btrim(_raw) = '' THEN
            RAISE WARNING 'dsqgen produced no output for query %', _i;
            CONTINUE;
        END IF;

        _fixed := tpcds._fix_query(_i, _raw);
        INSERT INTO tpcds.query (query_id, query_text) VALUES (_i, _fixed);
        _count := _count + 1;

        -- Write query to file
        BEGIN
            EXECUTE format('COPY (SELECT %L) TO PROGRAM %L',
                _fixed,
                format('cat > %s/query%s.sql', _query_dir, _i));
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not write query%s.sql: %', _i, SQLERRM;
        END;
    END LOOP;

    -- Clean up temp file
    BEGIN
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            'rm -f ' || _tmp_dir || '/tpcds_dsqgen_' || _pid || '.sql');
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    RETURN format('Generated and stored %s queries (scale=%s)', _count, _scale);
END;
$func$;

-- =============================================================================
-- show(qid) — return query text
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.show(qid INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT;
BEGIN
    SELECT query_text INTO _sql FROM tpcds.query WHERE query_id = qid;
    IF _sql IS NULL THEN
        RAISE EXCEPTION 'Query % not found (valid: 1-99)', qid;
    END IF;
    RETURN _sql;
END;
$func$;

-- =============================================================================
-- exec(qid) — execute a single query, record results
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.exec(qid INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT;
    _stmts TEXT[];
    _stmt TEXT;
    _start_ts TIMESTAMPTZ;
    _dur NUMERIC;
    _rows BIGINT;
    _total_dur NUMERIC := 0;
    _total_rows BIGINT := 0;
    _status TEXT := 'OK';
    _saved_path TEXT;
BEGIN
    SELECT query_text INTO _sql FROM tpcds.query WHERE query_id = qid;
    IF _sql IS NULL THEN
        RAISE EXCEPTION 'Query % not found (valid: 1-99)', qid;
    END IF;

    _saved_path := current_setting('search_path');
    PERFORM set_config('search_path', 'tpcds, public', false);

    _sql := btrim(_sql, E' \t\n\r');
    _sql := rtrim(_sql, ';');
    _stmts := string_to_array(_sql, ';');

    FOREACH _stmt IN ARRAY _stmts LOOP
        _stmt := btrim(_stmt, E' \t\n\r');
        IF _stmt = '' OR _stmt IS NULL THEN
            CONTINUE;
        END IF;

        BEGIN
            _start_ts := clock_timestamp();
            EXECUTE _stmt;
            GET DIAGNOSTICS _rows = ROW_COUNT;
            _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
            _total_dur := _total_dur + _dur;
            _total_rows := _total_rows + _rows;
        EXCEPTION WHEN OTHERS THEN
            _status := 'ERROR: ' || SQLERRM;
            _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
            _total_dur := _total_dur + _dur;
        END;
    END LOOP;

    PERFORM set_config('search_path', _saved_path, false);

    INSERT INTO tpcds.bench_results (query_id, status, duration_ms, rows_returned, scale_factor, storage_type)
    VALUES (qid, _status, round(_total_dur, 2), _total_rows,
            (SELECT NULLIF(value, '')::INTEGER FROM tpcds.config WHERE key = 'scale_factor'),
            (SELECT NULLIF(value, '') FROM tpcds.config WHERE key = 'storage_type'));

    RETURN format('query %s: %s, %s ms, %s rows', qid, _status, round(_total_dur, 2), _total_rows);
END;
$func$;

-- =============================================================================
-- explain(qid, opts) — EXPLAIN a single query, return plan to client
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.explain(qid INTEGER, opts TEXT DEFAULT '')
RETURNS SETOF TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT;
    _stmts TEXT[];
    _stmt TEXT;
    _explain_sql TEXT;
    _line TEXT;
    _part INTEGER := 0;
    _num_stmts INTEGER;
    _saved_path TEXT;
BEGIN
    SELECT query_text INTO _sql FROM tpcds.query WHERE query_id = qid;
    IF _sql IS NULL THEN
        RAISE EXCEPTION 'Query % not found (valid: 1-99)', qid;
    END IF;

    _saved_path := current_setting('search_path');
    PERFORM set_config('search_path', 'tpcds, public', false);

    _sql := btrim(_sql, E' \t\n\r');
    _sql := rtrim(_sql, ';');
    _stmts := string_to_array(_sql, ';');

    SELECT count(*) INTO _num_stmts
    FROM unnest(_stmts) s WHERE btrim(s, E' \t\n\r') <> '';

    FOREACH _stmt IN ARRAY _stmts LOOP
        _stmt := btrim(_stmt, E' \t\n\r');
        IF _stmt = '' OR _stmt IS NULL THEN
            CONTINUE;
        END IF;
        _part := _part + 1;

        IF _num_stmts > 1 THEN
            RETURN NEXT format('-- Statement %s of %s', _part, _num_stmts);
        END IF;

        IF opts <> '' THEN
            _explain_sql := format('EXPLAIN (%s) %s', opts, _stmt);
        ELSE
            _explain_sql := 'EXPLAIN ' || _stmt;
        END IF;

        FOR _line IN EXECUTE _explain_sql LOOP
            RETURN NEXT _line;
        END LOOP;
    END LOOP;

    PERFORM set_config('search_path', _saved_path, false);
END;
$func$;

-- =============================================================================
-- bench(optimizer, timeout_sec, skip) — run all 99 queries
--   optimizer: 'on' (ORCA), 'off' (Planner), NULL (inherit current)
--   timeout_sec: per-query timeout in seconds
--   skip: array of query_ids to skip
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.bench(
    optimizer TEXT DEFAULT NULL,
    timeout_sec INTEGER DEFAULT NULL,
    skip INTEGER[] DEFAULT NULL
)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _qid INTEGER;
    _sql TEXT;
    _stmts TEXT[];
    _stmt TEXT;
    _start_ts TIMESTAMPTZ;
    _dur NUMERIC;
    _rows BIGINT;
    _total_dur NUMERIC;
    _total_rows BIGINT;
    _status TEXT;
    _all_lines TEXT;
    _part INTEGER;
    _num_stmts INTEGER;
    _results_dir TEXT;
    _bench_start TIMESTAMPTZ;
    _filename TEXT;
    _ok_count INTEGER := 0;
    _err_count INTEGER := 0;
    _skip_count INTEGER := 0;
    _timeout_count INTEGER := 0;
    _bench_dur NUMERIC;
    _scale_factor INTEGER;
    _storage_type TEXT;
    _saved_path TEXT;
    _timer_file TEXT;
    _my_pid INTEGER;
    _optimizer_label TEXT;
    _saved_optimizer TEXT;
BEGIN
    _bench_start := now();
    _saved_path := current_setting('search_path');
    PERFORM set_config('search_path', 'tpcds, public', false);
    _my_pid := pg_backend_pid();
    _timer_file := format('/tmp/.tpcds_timer_%s', _my_pid);

    -- Resolve from config if NULL
    IF optimizer IS NULL THEN
        SELECT NULLIF(value, '') INTO optimizer FROM tpcds.config WHERE key = 'optimizer';
    END IF;
    IF timeout_sec IS NULL THEN
        SELECT NULLIF(value, '')::INTEGER INTO timeout_sec FROM tpcds.config WHERE key = 'timeout_sec';
    END IF;

    -- Read scale_factor and storage_type for result metadata
    SELECT NULLIF(value, '')::INTEGER INTO _scale_factor FROM tpcds.config WHERE key = 'scale_factor';
    SELECT NULLIF(value, '') INTO _storage_type FROM tpcds.config WHERE key = 'storage_type';

    -- Set optimizer if requested
    IF optimizer IS NOT NULL THEN
        IF lower(optimizer) IN ('on', 'off') THEN
            _saved_optimizer := current_setting('optimizer', true);
            PERFORM set_config('optimizer', lower(optimizer), false);
            _optimizer_label := CASE WHEN lower(optimizer) = 'on' THEN 'ORCA' ELSE 'Planner' END;
            RAISE NOTICE 'bench: using optimizer=% (%)', optimizer, _optimizer_label;
        ELSE
            RAISE EXCEPTION 'optimizer must be ''on'' (ORCA), ''off'' (Planner), or NULL (inherit)';
        END IF;
    ELSE
        _optimizer_label := CASE WHEN current_setting('optimizer', true) = 'on' THEN 'ORCA' ELSE 'Planner' END;
    END IF;

    IF timeout_sec IS NOT NULL THEN
        RAISE NOTICE 'bench: per-query timeout: %s', timeout_sec;
    END IF;
    IF skip IS NOT NULL THEN
        RAISE NOTICE 'bench: skipping queries: %', skip;
    END IF;

    _results_dir := tpcds._resolve_dir('results_dir', 'tpcds_results');
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _results_dir);

    FOR _qid IN 1..99 LOOP
        -- Skip user-requested queries
        IF skip IS NOT NULL AND _qid = ANY(skip) THEN
            _skip_count := _skip_count + 1;
            RAISE NOTICE '[%/99] query %: SKIP (user-requested)  elapsed %s',
                _qid, _qid,
                round(extract(epoch from clock_timestamp() - _bench_start)::numeric, 1);
            CONTINUE;
        END IF;

        SELECT query_text INTO _sql FROM tpcds.query WHERE query_id = _qid;
        IF _sql IS NULL THEN
            _skip_count := _skip_count + 1;
            RAISE NOTICE '[%/99] query %: SKIP (not found)  elapsed %s',
                _qid, _qid,
                round(extract(epoch from clock_timestamp() - _bench_start)::numeric, 1);
            CONTINUE;
        END IF;

        -- Start background cancel timer for per-query timeout
        IF timeout_sec IS NOT NULL THEN
            BEGIN
                EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
                    format('bash -c ''(sleep %s && psql -qAtc "SELECT pg_cancel_backend(%s)") & echo $! > %s''',
                           timeout_sec, _my_pid, _timer_file));
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END IF;

        _sql := btrim(_sql, E' \t\n\r');
        _sql := rtrim(_sql, ';');
        _stmts := string_to_array(_sql, ';');
        _total_dur := 0;
        _total_rows := 0;
        _status := 'OK';
        _all_lines := '';
        _part := 0;

        SELECT count(*) INTO _num_stmts
        FROM unnest(_stmts) s WHERE btrim(s, E' \t\n\r') <> '';

        FOREACH _stmt IN ARRAY _stmts LOOP
            _stmt := btrim(_stmt, E' \t\n\r');
            IF _stmt = '' OR _stmt IS NULL THEN
                CONTINUE;
            END IF;
            _part := _part + 1;

            BEGIN
                _start_ts := clock_timestamp();
                EXECUTE _stmt;
                GET DIAGNOSTICS _rows = ROW_COUNT;
                _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                _total_dur := _total_dur + _dur;
                _total_rows := _total_rows + _rows;
                _all_lines := _all_lines
                    || format('Statement %s: %s rows, %s ms',
                              _part, _rows, round(_dur, 2)) || E'\n';
            EXCEPTION
                WHEN query_canceled THEN
                    _status := format('TIMEOUT (%ss)', timeout_sec);
                    _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                    _total_dur := _total_dur + _dur;
                    _all_lines := _all_lines || _status || E'\n';
                WHEN OTHERS THEN
                    _status := 'ERROR: ' || SQLERRM;
                    _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                    _total_dur := _total_dur + _dur;
                    _all_lines := _all_lines || 'ERROR: ' || SQLERRM || E'\n';
            END;
        END LOOP;

        -- Kill the background cancel timer
        IF timeout_sec IS NOT NULL THEN
            BEGIN
                EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
                    format('kill $(cat %s 2>/dev/null) 2>/dev/null; rm -f %s',
                           _timer_file, _timer_file));
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END IF;

        INSERT INTO tpcds.bench_results (query_id, status, duration_ms, rows_returned, optimizer, scale_factor, storage_type)
        VALUES (_qid, _status, round(_total_dur, 2), _total_rows, _optimizer_label, _scale_factor, _storage_type);

        _filename := format('query%s.out', _qid);
        BEGIN
            EXECUTE format('COPY (SELECT %L) TO PROGRAM %L',
                _all_lines,
                format('cat > %s/%s', _results_dir, _filename));
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not write %: %', _filename, SQLERRM;
        END;

        IF _status = 'OK' THEN
            _ok_count := _ok_count + 1;
        ELSIF _status LIKE 'TIMEOUT%' THEN
            _timeout_count := _timeout_count + 1;
        ELSE
            _err_count := _err_count + 1;
        END IF;

        RAISE NOTICE '[%/99] query %: % (% ms)  elapsed %s',
            _qid, _qid, _status, round(_total_dur),
            round(extract(epoch from clock_timestamp() - _bench_start)::numeric, 1);
    END LOOP;

    -- Restore optimizer setting
    IF optimizer IS NOT NULL AND _saved_optimizer IS NOT NULL THEN
        PERFORM set_config('optimizer', _saved_optimizer, false);
    END IF;

    -- Update bench_summary table with latest run
    TRUNCATE tpcds.bench_summary;
    INSERT INTO tpcds.bench_summary (query_id, status, duration_ms, rows_returned, optimizer, scale_factor, storage_type, run_ts)
    SELECT br.query_id, br.status, br.duration_ms, br.rows_returned, br.optimizer, br.scale_factor, br.storage_type, br.run_ts
    FROM tpcds.bench_results br
    WHERE br.run_ts >= _bench_start
    ORDER BY br.query_id;

    -- Write summary CSV
    BEGIN
        EXECUTE format(
            'COPY (SELECT query_id, status, duration_ms, rows_returned, optimizer, scale_factor, storage_type '
            'FROM tpcds.bench_summary ORDER BY query_id) '
            'TO PROGRAM %L WITH (FORMAT csv, HEADER)',
            format('cat > %s/summary.csv', _results_dir));
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Could not write summary.csv: %', SQLERRM;
    END;

    _bench_dur := round(extract(epoch from clock_timestamp() - _bench_start)::numeric, 1);

    PERFORM set_config('search_path', _saved_path, false);

    RETURN format('Completed (%s): %s OK, %s errors, %s timeouts, %s skipped in %s sec. Results: %s/summary.csv',
        _optimizer_label, _ok_count, _err_count, _timeout_count, _skip_count, _bench_dur, _results_dir);
END;
$func$;

-- =============================================================================
-- report() — text-based benchmark summary for psql
-- =============================================================================
-- =============================================================================
-- gen_chart(): Generate benchmark chart (PNG) and report files
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.gen_chart()
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _chart_script TEXT;
    _results_dir TEXT;
    _output TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM tpcds.bench_summary LIMIT 1) THEN
        RAISE EXCEPTION 'bench_summary is empty. Run SELECT tpcds.bench() first.';
    END IF;

    _chart_script := tpcds._resolve_dir('', 'tpcds_chart') || '/gen_chart.py';
    _results_dir := tpcds._resolve_dir('results_dir', 'tpcds_results');

    BEGIN
        EXECUTE format(
            'COPY (SELECT 1) TO PROGRAM %L',
            format('PGHOST=/tmp PGPORT=%s PGDATABASE=%s python3 %s 2>&1',
                current_setting('port'),
                current_database(),
                _chart_script)
        );
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'gen_chart failed: %. Ensure python3, psycopg2, and matplotlib are installed.', SQLERRM;
    END;

    RETURN format('Chart generated in %s/', _results_dir);
END;
$func$;

CREATE OR REPLACE FUNCTION tpcds.report()
RETURNS TABLE(item TEXT, value TEXT)
LANGUAGE plpgsql
AS $func$
DECLARE
    _run_ts TIMESTAMPTZ;
    _sf INTEGER;
    _st TEXT;
    _opt TEXT;
    _total NUMERIC;
    _ok INT;
    _err INT;
    _timeout INT;
    _skip INT;
    _cnt INT;
    _rec RECORD;
    _line TEXT;
BEGIN
    SELECT count(*) INTO _cnt FROM tpcds.bench_summary;
    IF _cnt = 0 THEN
        item := 'status'; value := 'No benchmark results. Run SELECT tpcds.bench() first.';
        RETURN NEXT;
        RETURN;
    END IF;

    SELECT bs.run_ts, bs.scale_factor, bs.storage_type, bs.optimizer
    INTO _run_ts, _sf, _st, _opt
    FROM tpcds.bench_summary bs LIMIT 1;

    SELECT count(*) FILTER (WHERE status = 'OK'),
           count(*) FILTER (WHERE status LIKE 'ERROR%'),
           count(*) FILTER (WHERE status LIKE 'TIMEOUT%'),
           count(*) FILTER (WHERE status = 'SKIP'),
           sum(duration_ms)
    INTO _ok, _err, _timeout, _skip, _total
    FROM tpcds.bench_summary;

    -- Header
    item := '=== TPC-DS Benchmark Report ==='; value := '';
    RETURN NEXT;
    item := 'run_ts'; value := to_char(_run_ts, 'YYYY-MM-DD HH24:MI:SS');
    RETURN NEXT;
    item := 'scale_factor'; value := coalesce(_sf::TEXT, '?');
    RETURN NEXT;
    item := 'storage_type'; value := coalesce(_st, '?');
    RETURN NEXT;
    item := 'optimizer'; value := coalesce(_opt, '?');
    RETURN NEXT;
    item := 'queries'; value := format('%s OK, %s errors, %s timeouts, %s skipped',
        _ok, _err, _timeout, _skip);
    RETURN NEXT;
    item := 'total_time'; value := format('%s sec', round(_total / 1000, 1));
    RETURN NEXT;
    item := 'avg_query'; value := format('%s sec', round(_total / NULLIF(_cnt, 0) / 1000, 1));
    RETURN NEXT;

    -- Top 10 slowest
    item := ''; value := '';
    RETURN NEXT;
    item := '--- Top 10 Slowest ---'; value := '';
    RETURN NEXT;

    FOR _rec IN
        SELECT bs.query_id, bs.status, round(bs.duration_ms / 1000, 1) AS sec
        FROM tpcds.bench_summary bs
        ORDER BY bs.duration_ms DESC
        LIMIT 10
    LOOP
        item := format('Q%s', _rec.query_id);
        value := format('%s sec  (%s)', _rec.sec, _rec.status);
        RETURN NEXT;
    END LOOP;

    -- Errors if any
    IF _err > 0 OR _timeout > 0 THEN
        item := ''; value := '';
        RETURN NEXT;
        item := '--- Failed Queries ---'; value := '';
        RETURN NEXT;
        FOR _rec IN
            SELECT bs.query_id, bs.status, round(bs.duration_ms / 1000, 1) AS sec
            FROM tpcds.bench_summary bs
            WHERE bs.status NOT IN ('OK', 'SKIP')
            ORDER BY bs.query_id
        LOOP
            item := format('Q%s', _rec.query_id);
            value := format('%s  (%s sec)', _rec.status, _rec.sec);
            RETURN NEXT;
        END LOOP;
    END IF;
END;
$func$;

-- =============================================================================
-- clean_data() — delete generated .dat files from segment hosts via SSH
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.clean_data()
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _data_subdir TEXT;
    _rec RECORD;
    _host TEXT;
    _seg_hosts TEXT[];
    _cmd TEXT;
BEGIN
    SELECT COALESCE(NULLIF(value, ''), 'dsgendata_tpcds') INTO _data_subdir
    FROM tpcds.config WHERE key = 'data_subdir';
    IF _data_subdir IS NULL THEN
        _data_subdir := 'dsgendata_tpcds';
    END IF;

    -- Get segment hosts
    SELECT array_agg(DISTINCT hostname ORDER BY hostname) INTO _seg_hosts
    FROM gp_segment_configuration
    WHERE content >= 0 AND role = 'p';

    -- Remove data directories on each segment
    FOR _rec IN
        SELECT g.hostname, g.datadir
        FROM gp_segment_configuration g
        WHERE g.content >= 0 AND g.role = 'p'
        ORDER BY g.content
    LOOP
        BEGIN
            _cmd := format('ssh -o StrictHostKeyChecking=no -n %s "rm -rf %s/%s"',
                          _rec.hostname, _rec.datadir, _data_subdir);
            EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', _cmd);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not clean data on %: %', _rec.hostname, SQLERRM;
        END;
    END LOOP;

    -- Also clean up dsdgen tools from segment hosts
    FOREACH _host IN ARRAY _seg_hosts LOOP
        BEGIN
            _cmd := format('ssh -o StrictHostKeyChecking=no -n %s "rm -rf %s/tools"',
                          _host, tpcds._host_tmp_dir(_host));
            EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', _cmd);
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    RETURN 'Cleaned up TPC-DS data files from all segment hosts';
END;
$func$;

-- =============================================================================
-- run(scale, parallel, storage_type) — full pipeline procedure
-- =============================================================================
CREATE OR REPLACE PROCEDURE tpcds.run(
    scale         INTEGER DEFAULT NULL,
    parallel      INTEGER DEFAULT NULL,
    storage_type  TEXT DEFAULT NULL,
    use_partition BOOLEAN DEFAULT NULL
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    _workers   INTEGER;
    _t0        TIMESTAMPTZ;
    _schema    TEXT;
    _gendata   TEXT;
    _load      TEXT;
    _genquery  TEXT;
    _bench     TEXT;
BEGIN
    -- Resolve from config if NULL
    IF scale IS NULL THEN
        SELECT NULLIF(value, '')::INTEGER INTO scale FROM tpcds.config WHERE key = 'scale_factor';
        scale := coalesce(scale, 1);
    END IF;
    IF parallel IS NULL THEN
        SELECT NULLIF(value, '')::INTEGER INTO parallel FROM tpcds.config WHERE key = 'parallel';
        parallel := coalesce(parallel, 2);
    END IF;
    IF storage_type IS NULL THEN
        SELECT NULLIF(value, '') INTO storage_type FROM tpcds.config WHERE key = 'storage_type';
        storage_type := coalesce(storage_type, 'aocs');
    END IF;

    _workers := LEAST(parallel * (
        SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 AND role = 'p'
    )::INTEGER, 16);
    _t0 := clock_timestamp();

    RAISE NOTICE 'run(): scale=%, parallel=%, storage_type=%, load_workers=%',
        scale, parallel, storage_type, _workers;

    /* 1. Schema */
    RAISE NOTICE 'run(): step 1/5 — gen_schema(%)' , storage_type;
    SELECT tpcds.gen_schema(storage_type, use_partition) INTO _schema;
    COMMIT;

    /* 2. Data generation */
    RAISE NOTICE 'run(): step 2/5 — gen_data(%, %)', scale, parallel;
    SELECT tpcds.gen_data(scale, parallel) INTO _gendata;
    COMMIT;

    /* 3. Load */
    RAISE NOTICE 'run(): step 3/5 — load_data(%)', _workers;
    SELECT tpcds.load_data(_workers) INTO _load;
    COMMIT;

    /* 4. Query generation */
    RAISE NOTICE 'run(): step 4/5 — gen_query(%)', scale;
    SELECT tpcds.gen_query(scale) INTO _genquery;
    COMMIT;

    /* 5. Benchmark */
    RAISE NOTICE 'run(): step 5/5 — bench()';
    SELECT tpcds.bench() INTO _bench;

    RAISE NOTICE E'=== TPC-DS run complete in % sec ===\ngen_schema : %\ngen_data   : %\nload_data  : %\ngen_query  : %\nbench      : %',
        round(extract(epoch from clock_timestamp() - _t0)::numeric, 1),
        _schema, _gendata, _load, _genquery, _bench;
END;
$proc$;

-- =============================================================================
-- Load persistent config from tpcds.conf
-- =============================================================================
DO $load_conf$
BEGIN
    PERFORM tpcds._load_conf();
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'tpcds: could not load conf file: %', SQLERRM;
END;
$load_conf$;

-- =============================================================================
-- Extension loaded notice
-- =============================================================================
DO $notice$
BEGIN
    RAISE WARNING E'\n'
        '  tpcds extension installed.\n'
        '  Quick start:  CALL tpcds.run(scale := 1, parallel := 2, storage_type := ''aocs'');\n'
        '  Step by step:\n'
        '    SELECT tpcds.gen_schema(''aocs'');\n'
        '    SELECT tpcds.gen_data(1, 2);\n'
        '    SELECT tpcds.load_data(4);\n'
        '    SELECT tpcds.gen_query(1);\n'
        '    SELECT tpcds.bench();\n'
        '  Config:        SELECT * FROM tpcds.info();\n'
        '  Clean up:      SELECT tpcds.clean_data();';
END;
$notice$;
