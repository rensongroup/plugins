"""Pure, single-phase charging policy. No gateway or network dependencies."""

import math
from datetime import timedelta

import pytz


DEFAULTS = {
    "timezone": "Europe/Brussels",
    "season": "auto",
    "min_current": 6,
    "max_current": 20,
    "voltage": 230,
    "summer_grid_current": 6,
    "winter_grid_current": 10,
    "night_start_hour": 22,
    "night_end_hour": 7,
    "deferral_end_hour": 16,
    "grid_import_limit_w": 6000,
    "safety_margin_w": 200,
    "household_forecast_w": 500,
    "forecast_lookahead_hours": 4,
    "forecast_min_surplus_wh": 2000,
    "forecast_min_window_minutes": 60,
    "forecast_max_age": 7200,
    "forecast_refresh_interval": 1800,
    "forecast_enabled": True,
    "forecast_latitude": "50.83713",
    "forecast_longitude": "3.47914",
    "forecast_tilt": 45,
    "forecast_azimuth": -23,
    "forecast_kwp": "6.4",
    "forecast_inverter_kw": "5",
    "solar_smoothing_percent": 35,
    "resume_margin_w": 200,
    "pause_hold_seconds": 120,
    "increase_hold_seconds": 30,
    "increase_step_tenths": 10,
}


def finite_number(value):
    if isinstance(value, bool):
        raise ValueError("boolean is not a measurement")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("value must be finite")
    return result


def useful_solar(config, now, intervals):
    """Integrate future usable surplus; timestamps describe interval bounds in UTC."""
    end = now + timedelta(hours=config["forecast_lookahead_hours"])
    energy = 0.0
    minutes = 0.0
    minimum = config["min_current"] * config["voltage"] + config["resume_margin_w"]
    for start, stop, watts in intervals:
        hours = (min(stop, end) - max(start, now)).total_seconds() / 3600.0
        if hours <= 0:
            continue
        surplus = max(0.0, watts - config["household_forecast_w"])
        if surplus >= minimum:
            energy += surplus * hours
            minutes += hours * 60
    return {
        "useful_surplus_wh": round(energy, 1),
        "useful_minutes": round(minutes, 1),
        "worth_waiting": energy >= config["forecast_min_surplus_wh"]
        and minutes >= config["forecast_min_window_minutes"],
    }


def decide(config, values, now, intervals=(), forecast_fetched_at=None):
    local = now.astimezone(pytz.timezone(config["timezone"]))
    season = config["season"]
    if season == "auto":
        season = "summer" if 5 <= local.month <= 9 else "winter"
    start, end = config["night_start_hour"], config["night_end_hour"]
    night = (local.hour >= start or local.hour < end) if start > end else start <= local.hour < end
    age = (now - forecast_fetched_at).total_seconds() if forecast_fetched_at else None
    forecast_valid = bool(
        config["forecast_enabled"] and age is not None and 0 <= age <= config["forecast_max_age"]
        and any(a <= now < b for a, b, _ in intervals)
        and max((b for _, b, _ in intervals), default=now) >= now + timedelta(hours=config["forecast_lookahead_hours"])
    )
    opportunity = useful_solar(config, now, intervals) if forecast_valid else {
        "useful_surplus_wh": 0.0, "useful_minutes": 0.0, "worth_waiting": False,
    }
    baseline = config[season + "_grid_current"] * config["voltage"]
    if night:
        mode, baseline = "night", config["max_current"] * config["voltage"]
    elif local.hour >= config["deferral_end_hour"]:
        mode = "deadline_baseline"
    elif opportunity["worth_waiting"]:
        mode, baseline = "solar_only", 0.0
    else:
        mode = "poor_forecast_baseline" if forecast_valid else "forecast_fallback_baseline"

    ev, grid, battery = (values[key] for key in ("ev_w", "grid_w", "battery_w"))
    solar = max(0.0, ev - grid + battery)
    grid_allowance = max(0.0, ev + config["grid_import_limit_w"] - grid - config["safety_margin_w"])
    target_w = min(solar + baseline, grid_allowance, config["max_current"] * config["voltage"])
    return dict(opportunity, mode=mode, season=season, forecast_valid=forecast_valid,
                forecast_age_seconds=age, solar_available_w=solar, baseline_w=baseline,
                grid_allowance_w=grid_allowance, target_w=target_w,
                household_w=values["solar_w"] + grid - battery - ev)


class CurrentLimiter:
    """Smooth increases only; raw solar drops and grid limits always win immediately."""

    def __init__(self):
        self.filtered_solar = None
        self.last_current = None
        self.last_write_at = None
        self.paused_at = None

    def target(self, config, decision, now):
        raw = decision["solar_available_w"]
        alpha = config["solar_smoothing_percent"] / 100.0
        self.filtered_solar = raw if self.filtered_solar is None else alpha * raw + (1 - alpha) * self.filtered_solar
        watts = min(min(raw, self.filtered_solar) + decision["baseline_w"],
                    decision["grid_allowance_w"], config["max_current"] * config["voltage"])
        minimum = config["min_current"]
        resuming = self.last_current is None or self.last_current == 0
        if watts < minimum * config["voltage"]:
            return 0.0
        if resuming:
            # Margin only for solar starts: do not prevent an exact-minimum grid baseline.
            if decision["baseline_w"] == 0 and watts < minimum * config["voltage"] + config["resume_margin_w"]:
                return 0.0
            if self.paused_at is not None and now - self.paused_at < config["pause_hold_seconds"]:
                return 0.0
        current = math.floor(watts / config["voltage"] * 10) / 10.0
        previous = self.last_current
        if previous is not None and current > previous:
            if self.last_write_at is not None and now - self.last_write_at < config["increase_hold_seconds"]:
                return previous
            current = min(current, max(minimum, previous + config["increase_step_tenths"] / 10.0))
        elif previous is None:
            current = minimum
        return current

    def applied(self, current, now):
        if current == 0 and self.last_current != 0:
            self.paused_at = now
        self.last_current = current
        self.last_write_at = now