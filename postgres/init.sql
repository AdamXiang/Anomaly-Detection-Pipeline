-- ==============================================================================
-- 檔案：./postgres/init.sql
-- 用途：IoT 運動手錶遙測數據 (Telemetry) 資料庫初始化
-- PostgreSQL 版本：15+
-- ==============================================================================

-- 1. 建立專用 Schema
CREATE SCHEMA IF NOT EXISTS iot_data;

-- 2. 建立主表
-- 修改紀錄：
--   v2 - 新增 user_id（用戶識別）、mode（正常/爆量模式標記）
--      - blood_oxygen 改為 NUMERIC(4,1) 支援小數點（如 98.7%）
--      - step_count 改名為 steps（對應 Generator 輸出）
--      - speed_kmh 移除（Generator 未產生此欄位）
CREATE TABLE iot_data.athlete_telemetry (
    record_id       BIGINT GENERATED ALWAYS AS IDENTITY,
    device_id       VARCHAR(50)    NOT NULL,
    user_id         VARCHAR(50)    NOT NULL,               -- ← 新增：用戶識別
    event_time      TIMESTAMPTZ    NOT NULL,
    heart_rate      SMALLINT       CHECK (heart_rate BETWEEN 0 AND 300),
    blood_oxygen    NUMERIC(4, 1)  CHECK (blood_oxygen BETWEEN 0 AND 100), -- ← 改：支援小數
    steps           INTEGER        CHECK (steps >= 0),                     -- ← 改名：step_count → steps
    battery_level   SMALLINT       CHECK (battery_level BETWEEN 0 AND 100),
    latitude        NUMERIC(10, 7),
    longitude       NUMERIC(10, 7),
    mode            VARCHAR(10)    NOT NULL DEFAULT 'normal',              -- ← 新增：'normal' | 'burst'
    ingestion_time  TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (event_time);

-- 3. 建立每日分區
CREATE TABLE iot_data.athlete_telemetry_20260309
    PARTITION OF iot_data.athlete_telemetry
    FOR VALUES FROM ('2026-03-09 00:00:00+08') TO ('2026-03-10 00:00:00+08');

CREATE TABLE iot_data.athlete_telemetry_20260310
    PARTITION OF iot_data.athlete_telemetry
    FOR VALUES FROM ('2026-03-10 00:00:00+08') TO ('2026-03-11 00:00:00+08');

CREATE TABLE iot_data.athlete_telemetry_20260311
    PARTITION OF iot_data.athlete_telemetry
    FOR VALUES FROM ('2026-03-11 00:00:00+08') TO ('2026-03-12 00:00:00+08');

-- Default 分區
CREATE TABLE iot_data.athlete_telemetry_default
    PARTITION OF iot_data.athlete_telemetry DEFAULT;

-- 4. 索引策略
CREATE INDEX idx_telemetry_device_time
    ON iot_data.athlete_telemetry (device_id, event_time DESC);

CREATE INDEX idx_telemetry_event_time
    ON iot_data.athlete_telemetry (event_time DESC);

-- ← 新增：mode 索引（用於查詢「所有爆量事件」）
CREATE INDEX idx_telemetry_mode
    ON iot_data.athlete_telemetry (mode, event_time DESC);

-- 5. Ingestion Lag View
CREATE OR REPLACE VIEW iot_data.vw_ingestion_lag AS
SELECT
    device_id,
    user_id,
    event_time,
    ingestion_time,
    mode,
    ROUND(
        EXTRACT(EPOCH FROM (ingestion_time - event_time))::NUMERIC,
        3
    ) AS lag_seconds
FROM
    iot_data.athlete_telemetry;

-- 6. Materialized View（更新欄位）
CREATE MATERIALIZED VIEW iot_data.mv_telemetry_per_minute AS
SELECT
    device_id,
    DATE_TRUNC('minute', event_time)     AS minute_bucket,
    COUNT(*)                             AS record_count,
    ROUND(AVG(heart_rate)::NUMERIC, 1)   AS avg_heart_rate,
    ROUND(AVG(blood_oxygen)::NUMERIC, 1) AS avg_blood_oxygen,
    MAX(steps)                           AS max_steps,        -- ← 改：step_count → steps
    COUNT(*) FILTER (WHERE mode = 'burst') AS burst_count     -- ← 新增：每分鐘爆量次數
FROM
    iot_data.athlete_telemetry
GROUP BY
    device_id,
    DATE_TRUNC('minute', event_time)
WITH NO DATA;

CREATE INDEX idx_mv_device_minute
    ON iot_data.mv_telemetry_per_minute (device_id, minute_bucket DESC);

-- 7. Role 與權限
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'iot_consumer') THEN
        CREATE ROLE iot_consumer WITH LOGIN PASSWORD 'consumer_pass';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA iot_data TO iot_consumer;
GRANT INSERT, SELECT ON iot_data.athlete_telemetry TO iot_consumer;
GRANT SELECT ON iot_data.vw_ingestion_lag TO iot_consumer;
GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA iot_data TO iot_consumer;
