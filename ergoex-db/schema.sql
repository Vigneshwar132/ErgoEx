CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry_raw (
  time          TIMESTAMPTZ NOT NULL,
  org_id        TEXT NOT NULL,
  site_id       TEXT NOT NULL,
  worker_id     TEXT NOT NULL,
  role          TEXT NOT NULL,
  device_id     TEXT NOT NULL,
  ts_sec_ms     BIGINT,
  time_ok       BOOLEAN,
  pitch         DOUBLE PRECISION,
  roll          DOUBLE PRECISION,
  wz            DOUBLE PRECISION,
  amag          DOUBLE PRECISION,
  temp_c        DOUBLE PRECISION,
  hum           DOUBLE PRECISION,
  hr_raw        INTEGER,
  rx_ms         BIGINT
);

SELECT create_hypertable('telemetry_raw', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_tel_org_site_time ON telemetry_raw (org_id, site_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_tel_worker_time   ON telemetry_raw (worker_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_tel_device_time   ON telemetry_raw (device_id, time DESC);

CREATE TABLE IF NOT EXISTS worker_status (
  time          TIMESTAMPTZ NOT NULL,
  org_id        TEXT NOT NULL,
  site_id       TEXT NOT NULL,
  worker_id     TEXT NOT NULL,
  suit_state    TEXT NOT NULL,
  worker_risk   TEXT NOT NULL,
  trunk_online  BOOLEAN,
  knee_online   BOOLEAN,
  alignment_ok  BOOLEAN,
  ts_diff_ms    INTEGER,
  missing_roles TEXT,
  last_seen_ms  BIGINT
);

SELECT create_hypertable('worker_status', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_ws_org_site_time ON worker_status (org_id, site_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_ws_worker_time   ON worker_status (worker_id, time DESC);

CREATE TABLE IF NOT EXISTS events (
  time       TIMESTAMPTZ NOT NULL,
  org_id     TEXT NOT NULL,
  site_id    TEXT NOT NULL,
  worker_id  TEXT NOT NULL,
  severity   TEXT NOT NULL,
  event_type TEXT NOT NULL,
  details    JSONB
);

SELECT create_hypertable('events', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_ev_org_site_time ON events (org_id, site_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_ev_worker_time   ON events (worker_id, time DESC);
