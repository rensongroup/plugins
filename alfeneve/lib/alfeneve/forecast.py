"""Forecast.Solar public API adapter; production estimates, not household surplus."""

from datetime import datetime, timezone

import requests

from .control import finite_number


def parse_timestamp(value):
    # strptime keeps compatibility with gateways predating datetime.fromisoformat.
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z").astimezone(timezone.utc)


def parse_forecast(payload, inverter_kw):
    result = payload.get("result")
    if payload.get("message", {}).get("code") != 0 or not isinstance(result, dict):
        raise ValueError("Forecast.Solar returned an error")
    # Integrate the energy series, rather than assuming point power samples are
    # interval averages. Partial sunrise/sunset intervals must keep their duration.
    series = result.get("watt_hours_period")
    if not isinstance(series, dict) or len(series) < 2:
        raise ValueError("forecast has no interval energy series")
    points = sorted((parse_timestamp(key), finite_number(value)) for key, value in series.items())
    intervals = []
    for (start, _), (stop, wh) in zip(points, points[1:]):
        seconds = (stop - start).total_seconds()
        if wh < 0 or seconds <= 0:
            raise ValueError("invalid forecast interval")
        # A zero sunrise entry also spans the preceding night, deliberately.
        watts = min(wh * 3600.0 / seconds, finite_number(inverter_kw) * 1000)
        intervals.append((start, stop, watts))
    return intervals


def fetch_forecast(config):
    lat = finite_number(config["forecast_latitude"])
    lon = finite_number(config["forecast_longitude"])
    url = "https://api.forecast.solar/estimate/{:.5f}/{:.5f}/{}/{}/{}".format(
        lat, lon, config["forecast_tilt"], config["forecast_azimuth"], config["forecast_kwp"])
    response = requests.get(url, params={"inverter": config["forecast_inverter_kw"], "time": "utc"},
                            timeout=(3.05, config["request_timeout"]))
    response.raise_for_status()
    return parse_forecast(response.json(), config["forecast_inverter_kw"])