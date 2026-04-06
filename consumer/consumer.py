"""
Weather Data Consumer
──────────────────────
Consumes Kafka topics and persists to TimescaleDB:
  • weather-raw    → weather_readings table
  • weather-alerts → weather_alerts table

Includes statistical anomaly detection (z-score vs 7-day rolling mean).
"""

import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("consumer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME", "climate_db"),
    "user":     os.getenv("DB_USER", "climate_user"),
    "password": os.getenv("DB_PASSWORD", "climate_pass"),
}

TOPIC_RAW    = "weather-raw"
TOPIC_ALERTS = "weather-alerts"
GROUP_ID     = "weather-consumers"
BATCH_SIZE   = 50


def connect_db(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            log.info("Connected to TimescaleDB")
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("DB connection attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt == retries:
                raise
            time.sleep(delay)


def ensure_station(cur, reading: dict):
    cur.execute(
        """
        INSERT INTO weather_stations
            (station_id, city, country, latitude, longitude, source)
        VALUES
            (%(station_id)s, %(city)s, %(country)s,
             %(latitude)s,   %(longitude)s, %(source)s)
        ON CONFLICT (station_id) DO NOTHING
        """,
        {
            "station_id": reading["station_id"],
            "city":       reading["city"],
            "country":    reading["country"],
            "latitude":   reading.get("latitude", 0),
            "longitude":  reading.get("longitude", 0),
            "source":     reading["source"],
        },
    )


def insert_reading(cur, reading: dict):
    cur.execute(
        """
        INSERT INTO weather_readings (
            time, station_id, temperature_c, feels_like_c,
            humidity_pct, pressure_hpa, wind_speed_ms, wind_direction,
            visibility_m, cloud_cover_pct, weather_condition, raw_payload
        ) VALUES (
            NOW(), %(station_id)s, %(temperature_c)s, %(feels_like_c)s,
            %(humidity_pct)s, %(pressure_hpa)s, %(wind_speed_ms)s, %(wind_direction)s,
            %(visibility_m)s, %(cloud_cover_pct)s, %(weather_condition)s, %(raw_payload)s
        )
        """,
        {**reading, "raw_payload": json.dumps(reading.get("raw_payload", {}))},
    )


def insert_alert(cur, alert: dict):
    cur.execute(
        """
        INSERT INTO weather_alerts (
            time, station_id, alert_type, severity,
            metric, actual_value, threshold_value, message
        ) VALUES (
            %(timestamp)s, %(station_id)s, %(alert_type)s, %(severity)s,
            %(metric)s, %(actual_value)s, %(threshold_value)s, %(message)s
        )
        """,
        alert,
    )


def flag_statistical_anomaly(cur, reading: dict):
    """Flag readings that deviate more than 2 standard deviations from the 7-day mean."""
    cur.execute(
        """
        SELECT AVG(temperature_c), STDDEV(temperature_c)
        FROM weather_readings
        WHERE station_id = %s
          AND time > NOW() - INTERVAL '7 days'
          AND temperature_c IS NOT NULL
        """,
        (reading["station_id"],),
    )
    row = cur.fetchone()
    if not row or row[0] is None or row[1] is None or float(row[1]) == 0:
        return

    mean, stddev = float(row[0]), float(row[1])
    temp = reading.get("temperature_c")
    if temp is None:
        return

    z_score = (temp - mean) / stddev
    if abs(z_score) > 2:
        log.warning(
            "STATISTICAL ANOMALY | %s | temp=%.1f°C | mean=%.1f | z=%.2f",
            reading["station_id"], temp, mean, z_score,
        )
        cur.execute(
            """
            INSERT INTO weather_alerts (
                time, station_id, alert_type, severity,
                metric, actual_value, threshold_value, message
            ) VALUES (%s, %s, 'STATISTICAL_ANOMALY', 'WARNING',
                      'temperature_c', %s, %s, %s)
            """,
            (
                reading["timestamp"],
                reading["station_id"],
                temp,
                mean,
                f"Temp {temp:.1f}°C deviates {z_score:.1f}σ from 7-day mean {mean:.1f}°C",
            ),
        )


def run():
    conn = connect_db()
    cur  = conn.cursor()

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           GROUP_ID,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_RAW, TOPIC_ALERTS])
    log.info("Consumer started | topics=[%s, %s]", TOPIC_RAW, TOPIC_ALERTS)

    batch_count = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                topic   = msg.topic()

                if topic == TOPIC_RAW:
                    ensure_station(cur, payload)
                    insert_reading(cur, payload)
                    flag_statistical_anomaly(cur, payload)
                    log.info("Stored reading | %s | %.1f°C",
                             payload.get("station_id"), payload.get("temperature_c", 0))

                elif topic == TOPIC_ALERTS:
                    insert_alert(cur, payload)
                    log.warning("Stored alert | %s | %s",
                                payload.get("station_id"), payload.get("alert_type"))

                batch_count += 1
                if batch_count >= BATCH_SIZE:
                    conn.commit()
                    consumer.commit(asynchronous=False)
                    log.info("Committed batch of %d messages", batch_count)
                    batch_count = 0

            except (json.JSONDecodeError, KeyError) as exc:
                log.error("Malformed message skipped: %s", exc)
                conn.rollback()

            except psycopg2.Error as exc:
                log.error("DB error: %s", exc)
                conn.rollback()
                try:
                    conn = connect_db()
                    cur  = conn.cursor()
                except Exception:
                    time.sleep(5)

    except KeyboardInterrupt:
        log.info("Shutting down…")
    finally:
        if batch_count:
            conn.commit()
            consumer.commit(asynchronous=False)
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run()
