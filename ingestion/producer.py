"""
Weather Data Producer
─────────────────────
Polls OpenWeatherMap + Open-Meteo APIs and publishes to Kafka topics:
  • weather-raw    – every reading from every station
  • weather-alerts – extreme condition flags
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OWM_API_KEY     = os.getenv("OPENWEATHER_API_KEY", "demo")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

TOPIC_RAW    = "weather-raw"
TOPIC_ALERTS = "weather-alerts"

CITIES = [
    ("London,GB",    "London",    "GB"),
    ("New York,US",  "New York",  "US"),
    ("Tokyo,JP",     "Tokyo",     "JP"),
    ("Delhi,IN",     "Delhi",     "IN"),
    ("Sydney,AU",    "Sydney",    "AU"),
    ("Cairo,EG",     "Cairo",     "EG"),
    ("Sao Paulo,BR", "São Paulo", "BR"),
    ("Toronto,CA",   "Toronto",   "CA"),
]

CITY_COORDS = {
    "London":    (51.5074,  -0.1278),
    "New York":  (40.7128, -74.0060),
    "Tokyo":     (35.6762, 139.6503),
    "Delhi":     (28.6139,  77.2090),
    "Sydney":    (-33.8688, 151.2093),
    "Cairo":     (30.0444,  31.2357),
    "São Paulo": (-23.5505, -46.6333),
    "Toronto":   (43.6532, -79.3832),
}

THRESHOLDS = {
    "HEATWAVE":       {"metric": "temperature_c", "op": ">", "value": 40.0,  "severity": "CRITICAL"},
    "FREEZE":         {"metric": "temperature_c", "op": "<", "value": -10.0, "severity": "WARNING"},
    "HIGH_WIND":      {"metric": "wind_speed_ms", "op": ">", "value": 20.0,  "severity": "WARNING"},
    "LOW_VISIBILITY": {"metric": "visibility_m",  "op": "<", "value": 500,   "severity": "WARNING"},
    "STORM_PRESSURE": {"metric": "pressure_hpa",  "op": "<", "value": 980.0, "severity": "WARNING"},
}


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "weather-ingestion",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    })


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for %s: %s", msg.key(), err)
    else:
        log.debug("Delivered %s [partition %d offset %d]",
                  msg.topic(), msg.partition(), msg.offset())


def publish(producer: Producer, topic: str, key: str, payload: dict):
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload, default=str).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_owm(city_query: str) -> dict:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city_query, "appid": OWM_API_KEY, "units": "metric"}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def parse_owm(raw: dict, city: str, country: str) -> dict:
    station_id = f"owm-{city.lower().replace(' ', '_')}"
    return {
        "source":            "openweathermap",
        "station_id":        station_id,
        "city":              city,
        "country":           country,
        "timestamp":         datetime.fromtimestamp(raw["dt"], tz=timezone.utc).isoformat(),
        "temperature_c":     raw["main"]["temp"],
        "feels_like_c":      raw["main"]["feels_like"],
        "humidity_pct":      raw["main"]["humidity"],
        "pressure_hpa":      raw["main"]["pressure"],
        "wind_speed_ms":     raw["wind"]["speed"],
        "wind_direction":    raw["wind"].get("deg", 0),
        "visibility_m":      raw.get("visibility"),
        "cloud_cover_pct":   raw["clouds"]["all"],
        "weather_condition": raw["weather"][0]["description"],
        "latitude":          raw["coord"]["lat"],
        "longitude":         raw["coord"]["lon"],
        "raw_payload":       raw,
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_open_meteo(lat: float, lon: float) -> dict:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":        lat,
        "longitude":       lon,
        "current_weather": True,
        "hourly":          "relativehumidity_2m,surface_pressure,visibility",
        "forecast_days":   1,
        "timezone":        "UTC",
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def parse_open_meteo(raw: dict, city: str, country: str, lat: float, lon: float) -> dict:
    cw    = raw["current_weather"]
    hour0 = raw["hourly"]
    station_id = f"meteo-{city.lower().replace(' ', '_')}"
    return {
        "source":            "open_meteo",
        "station_id":        station_id,
        "city":              city,
        "country":           country,
        "timestamp":         cw["time"] + ":00+00:00",
        "temperature_c":     cw["temperature"],
        "feels_like_c":      None,
        "humidity_pct":      hour0["relativehumidity_2m"][0] if hour0.get("relativehumidity_2m") else None,
        "pressure_hpa":      hour0["surface_pressure"][0]    if hour0.get("surface_pressure")    else None,
        "wind_speed_ms":     cw["windspeed"] / 3.6,
        "wind_direction":    cw["winddirection"],
        "visibility_m":      hour0["visibility"][0]           if hour0.get("visibility")          else None,
        "cloud_cover_pct":   None,
        "weather_condition": f"WMO code {cw['weathercode']}",
        "latitude":          lat,
        "longitude":         lon,
        "raw_payload":       raw,
    }


def check_alerts(reading: dict) -> list:
    alerts = []
    for alert_type, cfg in THRESHOLDS.items():
        value = reading.get(cfg["metric"])
        if value is None:
            continue
        triggered = (
            (cfg["op"] == ">" and value > cfg["value"]) or
            (cfg["op"] == "<" and value < cfg["value"])
        )
        if triggered:
            alerts.append({
                "timestamp":       reading["timestamp"],
                "station_id":      reading["station_id"],
                "city":            reading["city"],
                "country":         reading["country"],
                "alert_type":      alert_type,
                "severity":        cfg["severity"],
                "metric":          cfg["metric"],
                "actual_value":    value,
                "threshold_value": cfg["value"],
                "message": (
                    f"{alert_type} in {reading['city']}: "
                    f"{cfg['metric']} = {value} "
                    f"(threshold {cfg['op']} {cfg['value']})"
                ),
            })
    return alerts


def run():
    log.info("Starting weather ingestion | interval=%ds | cities=%d",
             POLL_INTERVAL, len(CITIES))
    producer = make_producer()

    while True:
        batch_start = time.time()
        success, errors = 0, 0

        for city_query, city_name, country in CITIES:

            # OpenWeatherMap
            if OWM_API_KEY and OWM_API_KEY != "demo":
                try:
                    raw     = fetch_owm(city_query)
                    reading = parse_owm(raw, city_name, country)
                    publish(producer, TOPIC_RAW, reading["station_id"], reading)
                    for alert in check_alerts(reading):
                        publish(producer, TOPIC_ALERTS, alert["station_id"], alert)
                        log.warning("ALERT: %s", alert["message"])
                    log.info("OWM %-15s  temp=%.1f°C  humidity=%s%%",
                             city_name, reading["temperature_c"], reading["humidity_pct"])
                    success += 1
                except Exception as exc:
                    log.error("OWM fetch failed for %s: %s", city_name, exc)
                    errors += 1
            else:
                log.info("Skipping OWM for %s (no API key)", city_name)

            # Open-Meteo (always free, no key)
            try:
                lat, lon = CITY_COORDS.get(city_name, (0, 0))
                raw      = fetch_open_meteo(lat, lon)
                reading  = parse_open_meteo(raw, city_name, country, lat, lon)
                publish(producer, TOPIC_RAW, reading["station_id"], reading)
                for alert in check_alerts(reading):
                    publish(producer, TOPIC_ALERTS, alert["station_id"], alert)
                    log.warning("ALERT: %s", alert["message"])
                log.info("Meteo %-14s  temp=%.1f°C  wind=%.1fm/s",
                         city_name, reading["temperature_c"], reading["wind_speed_ms"])
                success += 1
            except Exception as exc:
                log.error("Open-Meteo fetch failed for %s: %s", city_name, exc)
                errors += 1

        producer.flush()
        elapsed = time.time() - batch_start
        log.info("Batch done | success=%d errors=%d elapsed=%.1fs | sleeping %ds",
                 success, errors, elapsed, POLL_INTERVAL)
        time.sleep(max(0, POLL_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
