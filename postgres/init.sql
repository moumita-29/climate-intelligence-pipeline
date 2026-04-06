-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Weather stations reference table ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_stations (
    station_id    VARCHAR(50) PRIMARY KEY,
    city          VARCHAR(100) NOT NULL,
    country       VARCHAR(10)  NOT NULL,
    latitude      DECIMAL(9,6) NOT NULL,
    longitude     DECIMAL(9,6) NOT NULL,
    source        VARCHAR(50)  NOT NULL,
    created_at    TIMESTAMPTZ  DEFAULT NOW()
);

INSERT INTO weather_stations (station_id, city, country, latitude, longitude, source) VALUES
    ('owm-london',    'London',    'GB',  51.5074,  -0.1278, 'openweathermap'),
    ('owm-new_york',  'New York',  'US',  40.7128, -74.0060, 'openweathermap'),
    ('owm-tokyo',     'Tokyo',     'JP',  35.6762, 139.6503, 'openweathermap'),
    ('owm-delhi',     'Delhi',     'IN',  28.6139,  77.2090, 'openweathermap'),
    ('owm-sydney',    'Sydney',    'AU', -33.8688, 151.2093, 'openweathermap'),
    ('owm-cairo',     'Cairo',     'EG',  30.0444,  31.2357, 'openweathermap'),
    ('owm-sao_paulo', 'São Paulo', 'BR', -23.5505, -46.6333, 'openweathermap'),
    ('owm-toronto',   'Toronto',   'CA',  43.6532, -79.3832, 'openweathermap')
ON CONFLICT DO NOTHING;

-- ── Core time-series table ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_readings (
    time              TIMESTAMPTZ  NOT NULL,
    station_id        VARCHAR(50)  NOT NULL REFERENCES weather_stations(station_id),
    temperature_c     DECIMAL(6,2),
    feels_like_c      DECIMAL(6,2),
    humidity_pct      SMALLINT,
    pressure_hpa      DECIMAL(8,2),
    wind_speed_ms     DECIMAL(6,2),
    wind_direction    SMALLINT,
    visibility_m      INT,
    cloud_cover_pct   SMALLINT,
    weather_condition VARCHAR(100),
    raw_payload       JSONB
);

SELECT create_hypertable('weather_readings', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_readings_station ON weather_readings (station_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_readings_temp    ON weather_readings (time DESC, temperature_c);

-- ── Alerts table ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_alerts (
    id              BIGSERIAL    PRIMARY KEY,
    time            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    station_id      VARCHAR(50)  NOT NULL REFERENCES weather_stations(station_id),
    alert_type      VARCHAR(50)  NOT NULL,
    severity        VARCHAR(20)  NOT NULL,
    metric          VARCHAR(50),
    actual_value    DECIMAL(10,3),
    threshold_value DECIMAL(10,3),
    message         TEXT
);

-- ── Continuous aggregate: hourly summaries ────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS weather_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    station_id,
    AVG(temperature_c)          AS avg_temp_c,
    MAX(temperature_c)          AS max_temp_c,
    MIN(temperature_c)          AS min_temp_c,
    AVG(humidity_pct)           AS avg_humidity,
    AVG(pressure_hpa)           AS avg_pressure,
    AVG(wind_speed_ms)          AS avg_wind_ms,
    COUNT(*)                    AS reading_count
FROM weather_readings
GROUP BY bucket, station_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('weather_hourly',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE
);

-- ── Continuous aggregate: daily summaries ────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS weather_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time)  AS bucket,
    station_id,
    AVG(temperature_c)          AS avg_temp_c,
    MAX(temperature_c)          AS max_temp_c,
    MIN(temperature_c)          AS min_temp_c,
    AVG(humidity_pct)           AS avg_humidity,
    AVG(wind_speed_ms)          AS avg_wind_ms,
    COUNT(*)                    AS reading_count
FROM weather_readings
GROUP BY bucket, station_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('weather_daily',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists     => TRUE
);

-- ── Retention: keep raw readings 90 days ─────────────────────────────────────
SELECT add_retention_policy('weather_readings',
    INTERVAL '90 days',
    if_not_exists => TRUE
);
