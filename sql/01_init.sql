-- Schema de inicialización para NYC Taxi ETL
-- Este archivo se ejecuta automáticamente al iniciar PostgreSQL

-- Crear schemas separados por capa
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Tabla de control de ejecuciones del pipeline
CREATE TABLE IF NOT EXISTS warehouse.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100)  NOT NULL,
    started_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(20)   NOT NULL DEFAULT 'running',
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,
    error_message   TEXT
);

-- Tabla principal de viajes
CREATE TABLE IF NOT EXISTS warehouse.taxi_trips (
    trip_id             BIGSERIAL PRIMARY KEY,
    vendor_id           SMALLINT,
    pickup_datetime     TIMESTAMPTZ  NOT NULL,
    dropoff_datetime    TIMESTAMPTZ  NOT NULL,
    passenger_count     SMALLINT,
    trip_distance_miles NUMERIC(8,2),
    pickup_location_id  INTEGER,
    dropoff_location_id INTEGER,
    payment_type        SMALLINT,
    fare_amount         NUMERIC(10,2),
    tip_amount          NUMERIC(10,2),
    total_amount        NUMERIC(10,2),
    source_file         VARCHAR(200),
    loaded_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Índices para queries analíticas frecuentes
CREATE INDEX IF NOT EXISTS idx_taxi_pickup_dt
    ON warehouse.taxi_trips (pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_taxi_payment
    ON warehouse.taxi_trips (payment_type);
