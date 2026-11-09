"""Forecast-aware, single-phase Alfen Eve charging controller.

A zero policy target writes 0 A to the current property to pause charging.
"""

import copy
import json
import logging
import requests
import threading
import time
from datetime import datetime, timezone

import pytz
from plugins.base import OMPluginBase, PluginConfigChecker, background_task, om_expose

from .lib.alfeneve.control import DEFAULTS, CurrentLimiter, decide, finite_number
from .lib.alfeneve.discovery import discover_alfen
from .lib.alfeneve.forecast import fetch_forecast


logger = logging.getLogger(__name__)
POWER_SIGNALS = ("solar_w", "grid_w", "battery_w", "ev_w")


def as_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise ValueError("expected a boolean or true/false string")


class AlfenEveControl(OMPluginBase):
    name = "AlfenEveControl"
    version = "0.2.9"
    interfaces = [("config", "1.0")]

    default_config = dict(DEFAULTS, **{
        "config_schema_version": 6,
        "enabled": False,
        "dry_run": True,
        "endpoint": "",
        "charger_identity": "",
        "username": "admin",
        "password": "",
        "verify_ssl": False,
        "tls_legacy": False,
        "request_timeout": 10,
        "current_property": "OD_sysActiveLoadBalancingSafeCurrent",
        "battery_capacity_wh": 10000,
        "signals": [
            {"name": "solar_w", "measurement_counter_id": "PLUGIN/FoxEss/foxess/solar", "status_event_id": 525, "field": "value", "scale": -1000},
            {"name": "grid_w", "measurement_counter_id": "EAN.3153414731313035313134323631", "status_event_id": 526, "field": "value", "scale": 1},
            {"name": "battery_w", "measurement_counter_id": "PLUGIN/FoxEss/foxess/battery", "status_event_id": 524, "field": "value", "scale": 1000},
            {"name": "ev_w", "measurement_counter_id": "EY220722000169.3", "status_event_id": 513, "field": "value", "scale": 1},
        ],
        "deadband_tenths": 5,
        "update_interval": 5,
        "write_retries": 3,
        "write_retry_backoff": 2,
        "max_value_age": 120,
        "auxiliary_max_value_age": 900,
        "command_refresh_seconds": 60,
    })

    # String-valued decimals work with the gateway's int/str/bool config UI.
    config_description = [
        {"name": key, "type": "bool" if isinstance(value, bool) else "int" if isinstance(value, int) else "str",
         "description": key.replace("_", " ")}
        for key, value in default_config.items() if key not in ("signals", "config_schema_version")
    ] + [{
        "name": "signals", "type": "section", "repeat": True, "min": 4,
        "description": "Power in W; optional soc_percent maps stored energy in kWh, never realtime power",
        "content": [
            {"name": "name", "type": "enum", "choices": list(POWER_SIGNALS) + ["soc_percent"]},
            {"name": "measurement_counter_id", "type": "str"},
            {"name": "status_event_id", "type": "int", "description": "Numeric sensor ID from the gateway SENSOR_CHANGE event; 0 disables this alias"},
            {"name": "field", "type": "str", "description": "Sensor status events use value"},
            {"name": "scale", "type": "int", "description": "Multiply input; SOC expects kWh after scaling"},
        ],
    }]
    for field in config_description:
        if field["name"] == "endpoint":
            field["description"] = "Optional HTTPS IP override; blank resolves charger_identity over mDNS"
        elif field["name"] == "charger_identity":
            field["description"] = "Alfen mDNS TXT Identity; auto-learned only when exactly one charger is found"

    def __init__(self, webinterface, connector):
        super(AlfenEveControl, self).__init__(webinterface=webinterface, connector=connector)
        self._lock = threading.RLock()
        self._config_checker = PluginConfigChecker(self.config_description)
        self._config = self._normalize_config(self.read_config(copy.deepcopy(self.default_config)))
        self._discovery_event = threading.Event()
        self._config_error = None
        try:
            self._validate_config(self._config)
        except (ValueError, TypeError, KeyError) as exc:
            # Do not silently reinterpret legacy battery realtime as SOC or
            # silently replace an explicitly configured maximum current.
            self._config_error = str(exc)
            self._config["enabled"] = False
            logger.error("Control disabled: configuration requires migration: %s", exc)
        self._reset_state()
        self._rebuild_signal_map()
        self.connector.sensor.subscribe_status_event(self.handle_measurement_counter_status, version=2)
        logger.info("Started %s plugin v%s (dry_run=%s)", self.name, self.version, self._config["dry_run"])

    @classmethod
    def _normalize_config(cls, data):
        schema_version = data.get("config_schema_version", 0)
        config = copy.deepcopy(cls.default_config)
        config.update(data)
        for key, default in cls.default_config.items():
            if isinstance(default, bool):
                config[key] = as_bool(config[key])
        default_signals = {item["name"]: item for item in cls.default_config["signals"]}
        legacy_counter_ids = {"solar_w": 16, "grid_w": 17, "battery_w": 15, "ev_w": 5}
        normalized_signals = []
        for item in config["signals"]:
            signal = dict(item)
            defaults = default_signals.get(signal.get("name"), {})
            if "status_event_id" not in signal:
                previous_id = signal.pop("gateway_id", None)
                if schema_version < 4 and previous_id in (None, legacy_counter_ids.get(signal.get("name"))):
                    signal["status_event_id"] = defaults.get("status_event_id", 0)
                else:
                    signal["status_event_id"] = previous_id or defaults.get("status_event_id", 0)
            if signal.get("field") == "realtime":
                signal["field"] = "value"
            if schema_version < 2 and signal.get("scale") == 1 and defaults.get("scale", 1) != 1:
                signal["scale"] = defaults["scale"]
            normalized_signals.append(signal)
        config["signals"] = normalized_signals
        for key in ("pause_verified", "pause_property", "pause_value_json", "resume_value_json"):
            config.pop(key, None)
        config["endpoint"] = str(config.get("endpoint") or "").strip()
        config["charger_identity"] = str(config.get("charger_identity") or "").strip()
        config["config_schema_version"] = 6
        return config

    def _reset_state(self):
        self._values = dict.fromkeys(POWER_SIGNALS + ("soc_percent",))
        self._value_ts = {}
        self._last_seen = None
        self._limiter = CurrentLimiter()
        self._last_sent_current = None
        self._forecast = []
        self._forecast_fetched_at = None
        self._forecast_error = None
        self._forecast_next_run = 0
        self._last_error = None
        self._last_success_at = None
        self._decision = {}
        self._writes_succeeded = 0
        self._writes_failed = 0
        self._next_run = 0
        self._unknown_event_count = 0
        self._last_unknown_event = None
        self._measurement_event_count = 0
        self._matched_event_count = 0
        self._last_measurement_event = None
        self._resolved_endpoint = self._config["endpoint"]
        self._discovery_status = "manual_override" if self._resolved_endpoint else "waiting"
        self._discovery_error = None
        self._last_discovery_at = None

    def _rebuild_signal_map(self):
        self._signal_map = {}
        for item in self._config["signals"]:
            signal = dict(item)
            external_id = signal["measurement_counter_id"].strip()
            self._signal_map.setdefault(external_id, []).append(signal)
            status_event_id = signal.get("status_event_id", 0)
            if status_event_id:
                self._signal_map.setdefault(str(status_event_id), []).append(signal)

    def _validate_config(self, config):
        names = [s["name"] for s in config["signals"]]
        if not set(POWER_SIGNALS).issubset(names) or len(names) != len(set(names)) or set(names) - set(POWER_SIGNALS + ("soc_percent",)):
            raise ValueError("signals require solar_w, grid_w, battery_w, ev_w once each; soc_percent is optional")
        for signal in config["signals"]:
            if not signal["measurement_counter_id"].strip() or not signal["field"].strip():
                raise ValueError("signal ID and field must not be empty")
            finite_number(signal["scale"])
            status_event_id = signal.get("status_event_id", 0)
            if isinstance(status_event_id, bool) or not isinstance(status_event_id, int) or status_event_id < 0:
                raise ValueError("status_event_id must be a nonnegative integer")
            if signal["name"] == "soc_percent" and signal["field"] == "realtime":
                raise ValueError("SOC must use total stored energy, not realtime power")
        if config["endpoint"] and not config["endpoint"].startswith("https://"):
            raise ValueError("endpoint override must use HTTPS")
        positive = ("voltage", "battery_capacity_wh", "update_interval", "request_timeout", "max_value_age",
                "auxiliary_max_value_age",
                    "write_retries", "command_refresh_seconds", "grid_import_limit_w", "increase_step_tenths", "forecast_max_age",
                    "forecast_lookahead_hours", "forecast_min_surplus_wh", "forecast_min_window_minutes")
        nonnegative = ("safety_margin_w", "resume_margin_w", "pause_hold_seconds", "increase_hold_seconds",
                       "household_forecast_w", "write_retry_backoff", "deadband_tenths")
        for key in positive + nonnegative:
            value = finite_number(config[key])
            if value < 0 or (key in positive and value == 0):
                raise ValueError("invalid positive/nonnegative setting: " + key)
        # All int UI settings must remain ints for arithmetic and slicing.
        for key, default in self.default_config.items():
            if isinstance(default, int) and not isinstance(default, bool):
                if isinstance(config[key], bool) or not isinstance(config[key], int):
                    raise ValueError(key + " must be an integer")
        if not 6 <= config["min_current"] <= config["max_current"] <= 20:
            raise ValueError("current limits must satisfy 6 <= min_current <= max_current <= 20 A")
        for key in ("summer_grid_current", "winter_grid_current"):
            if not 0 <= config[key] <= config["max_current"]:
                raise ValueError(key + " must be between 0 and max_current")
        if not 0 <= config["safety_margin_w"] < config["grid_import_limit_w"]:
            raise ValueError("grid safety margin must be below import limit")
        if not 1 <= config["solar_smoothing_percent"] <= 100:
            raise ValueError("solar_smoothing_percent must be 1..100")
        if config["forecast_refresh_interval"] < 900:
            raise ValueError("forecast refresh must be at least 900 seconds")
        if config["forecast_max_age"] < config["forecast_refresh_interval"]:
            raise ValueError("forecast_max_age must cover the refresh interval")
        for key in ("night_start_hour", "night_end_hour", "deferral_end_hour"):
            if not 0 <= config[key] <= 23:
                raise ValueError(key + " must be 0..23")
        if not config["night_end_hour"] < config["deferral_end_hour"] < config["night_start_hour"]:
            raise ValueError("require night_end_hour < deferral_end_hour < night_start_hour")
        if config["season"] not in ("auto", "summer", "winter"):
            raise ValueError("unknown season")
        if config["timezone"] not in pytz.all_timezones_set:
            raise ValueError("unknown timezone")
        if config["forecast_enabled"]:
            if not -90 <= finite_number(config["forecast_latitude"]) <= 90 or not -180 <= finite_number(config["forecast_longitude"]) <= 180:
                raise ValueError("invalid forecast coordinates")
            if not 0 <= config["forecast_tilt"] <= 90 or not -180 <= config["forecast_azimuth"] <= 180:
                raise ValueError("invalid panel orientation")
            if finite_number(config["forecast_kwp"]) <= 0 or finite_number(config["forecast_inverter_kw"]) <= 0:
                raise ValueError("panel and inverter capacity must be positive")
        if config["enabled"] and not config["dry_run"]:
            if not config["password"] or not config["current_property"].strip():
                raise ValueError("live control requires password and current property")

    def _is_data_ready(self, now=None):
        now = time.time() if now is None else now
        return all(self._is_signal_fresh(key, now) for key in ("grid_w", "ev_w"))

    def _is_signal_fresh(self, name, now):
        timestamp = self._value_ts.get(name)
        max_age = self._config["max_value_age"] if name in ("grid_w", "ev_w") else self._config["auxiliary_max_value_age"]
        return (self._values.get(name) is not None and timestamp is not None
            and 0 <= now - timestamp <= max_age)

    def _should_send(self, target):
        previous = self._last_sent_current
        if previous is None:
            return True
        if self._limiter.last_write_at is None or time.time() - self._limiter.last_write_at >= self._config["command_refresh_seconds"]:
            return True  # Reassert after charger restart/external changes, including pause.
        if target == previous:
            return False
        # Never hide a safety reduction or pause/resume transition in deadband.
        return target < previous or previous == 0 or target - previous >= self._config["deadband_tenths"] / 10.0

    def _write_current(self, target):
        config = self._config
        if target != 0 and not config["min_current"] <= target <= config["max_current"]:
            raise ValueError("unsafe current target")
        if config["dry_run"]:
            logger.info("DRY-RUN %s %.1fA", "pause" if target == 0 else "charge", target)
        else:
            endpoint = config["endpoint"] or self._resolved_endpoint
            if not endpoint:
                self._last_error = "discovery: " + (self._discovery_error or "waiting for matching Alfen charger")
                return False
            # Validate at the write boundary too, not only via the UI.
            self._validate_config(dict(config, enabled=True))
            from .lib.alfeneve.alfen import Alfen
            for attempt in range(1, config["write_retries"] + 1):
                phase = "connect/login"
                try:
                    with Alfen(endpoint, (config["username"], config["password"]),
                               verify=config["verify_ssl"], tls_legacy=config["tls_legacy"],
                               timeout=(3.05, config["request_timeout"])) as eve:
                        # The user verified that zero pauses and a positive current
                        # resumes an active session through this property.
                        phase = "pause current" if target == 0 else "current limit"
                        eve.set_property(config["current_property"], float(target))
                        phase = "logout"
                    break
                except Exception as exc:
                    self._last_error = phase + ": " + type(exc).__name__
                    if not config["endpoint"] and isinstance(
                            exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
                        self._discovery_event.set()
                    if attempt == config["write_retries"]:
                        self._writes_failed += 1
                        # Some commands may have reached the charger. Forget the
                        # old acknowledged state so deadband cannot hide recovery.
                        self._last_sent_current = None
                        self._limiter.last_current = None
                        logger.error("Charger command failed after %d attempts (%s)", attempt, self._last_error)
                        return False
                    time.sleep(config["write_retry_backoff"] * attempt)
        self._last_sent_current = target
        self._limiter.applied(target, time.time())
        self._last_success_at = datetime.now(timezone.utc).isoformat()
        self._last_error = None
        self._writes_succeeded += 1
        return True

    def _apply_discovery_result(self, config_snapshot, services):
        with self._lock:
            if config_snapshot != self._config:
                return
            if config_snapshot["endpoint"]:
                self._resolved_endpoint = config_snapshot["endpoint"]
                self._discovery_status = "manual_override"
                self._discovery_error = None
                return

            identity = config_snapshot["charger_identity"].casefold()
            candidates = ([service for service in services
                           if service["identity"].casefold() == identity]
                          if identity else list(services))
            if not identity and len(candidates) == 1:
                updated_config = copy.deepcopy(self._config)
                updated_config["charger_identity"] = candidates[0]["identity"]
                self.write_config(updated_config)
                self._config = updated_config
                identity = updated_config["charger_identity"].casefold()

            if len(candidates) == 1:
                self._resolved_endpoint = candidates[0]["endpoint"]
                self._discovery_status = "resolved"
                self._discovery_error = None
            else:
                self._resolved_endpoint = ""
                if not candidates:
                    self._discovery_status = "not_found"
                    self._discovery_error = "no Alfen charger matched the configured identity"
                else:
                    self._discovery_status = "ambiguous"
                    self._discovery_error = "multiple Alfen chargers found; configure charger_identity"
            self._last_discovery_at = datetime.now(timezone.utc).isoformat()

    def _discover_once(self):
        with self._lock:
            config_snapshot = copy.deepcopy(self._config)
        if config_snapshot["endpoint"]:
            self._apply_discovery_result(config_snapshot, [])
            return
        try:
            services = discover_alfen(timeout=2.5)
            self._apply_discovery_result(config_snapshot, services)
        except Exception as exc:
            with self._lock:
                if config_snapshot == self._config:
                    self._resolved_endpoint = ""
                    self._discovery_status = "error"
                    self._discovery_error = type(exc).__name__
                    self._last_discovery_at = datetime.now(timezone.utc).isoformat()
            logger.warning("Alfen mDNS discovery failed (%s)", type(exc).__name__)

    @background_task
    def discovery_loop(self):
        while True:
            self._discover_once()
            self._discovery_event.wait(60)
            self._discovery_event.clear()

    def _control_once(self, now=None):
        now = datetime.now(timezone.utc) if now is None else now
        with self._lock:
            if not self._config["enabled"]:
                return
            if not self._is_data_ready(now.timestamp()):
                self._decision = {"mode": "stale_data_pause", "target_current": 0.0}
                target = 0.0
            else:
                control_values = dict(self._values)
                stale_signals = []
                for name in ("solar_w", "battery_w"):
                    if not self._is_signal_fresh(name, now.timestamp()):
                        stale_signals.append(name)
                        control_values[name] = 0.0
                self._decision = decide(self._config, control_values, now, self._forecast, self._forecast_fetched_at)
                if "battery_w" in stale_signals:
                    # Do not infer solar surplus from grid/EV values without a fresh battery flow.
                    self._decision["solar_available_w"] = 0.0
                    self._decision["target_w"] = min(
                        self._decision["baseline_w"],
                        self._decision["grid_allowance_w"],
                        self._config["max_current"] * self._config["voltage"],
                    )
                self._decision["stale_signals"] = stale_signals
                target = self._limiter.target(self._config, self._decision, now.timestamp())
                self._decision["target_current"] = target
            logger.debug("Charging decision: %s", self._decision)
            if self._should_send(target):
                self._write_current(target)

    def _refresh_forecast(self, now=None):
        now = datetime.now(timezone.utc) if now is None else now
        with self._lock:
            config = copy.deepcopy(self._config)
            if not config["enabled"] or not config["forecast_enabled"] or now.timestamp() < self._forecast_next_run:
                return
            self._forecast_next_run = now.timestamp() + config["forecast_refresh_interval"]
        # Network call is outside the lock and in a separate task: a weather
        # timeout must never delay import-cap or stale-data decisions.
        try:
            intervals = fetch_forecast(config)
            with self._lock:
                if config == self._config:
                    self._forecast = intervals
                    self._forecast_fetched_at = now
                    self._forecast_error = None
        except Exception as exc:
            with self._lock:
                if config == self._config:
                    self._forecast_error = type(exc).__name__
            # Keep cached data until max age, respect refresh interval even on 429.
            logger.warning("Solar forecast refresh failed (%s); cached/fallback policy applies", type(exc).__name__)

    @background_task
    def forecast_loop(self):
        while True:
            try:
                self._refresh_forecast()
            except Exception:
                logger.exception("Forecast task error")
            time.sleep(5)

    @background_task
    def loop(self):
        while True:
            try:
                if time.time() >= self._next_run:
                    self._next_run = time.time() + self._config["update_interval"]
                    self._control_once()
            except Exception as exc:
                self._last_error = type(exc).__name__
                logger.exception("Control loop error")
            time.sleep(1)

    def handle_measurement_counter_status(self, event):
        payload = event if isinstance(event, dict) else getattr(event, "data", None)
        if not isinstance(payload, dict):
            return
        with self._lock:
            self._measurement_event_count += 1
            event_id = payload.get("id")
            signals = self._signal_map.get(str(event_id), [])
            if signals:
                self._matched_event_count += 1
            if not signals:
                self._unknown_event_count += 1
            sample = {}
            for key in ("value", "realtime", "total_consumed", "total_injected"):
                raw_sample = payload.get(key)
                if isinstance(raw_sample, (int, float)) and not isinstance(raw_sample, bool):
                    try:
                        sample[key] = finite_number(raw_sample)
                    except ValueError:
                        pass
            self._last_measurement_event = {
                "id": event_id,
                "fields": sorted(payload.keys()),
                "matched_signals": [signal["name"] for signal in signals],
                "numeric_values": sample,
            }
            if not signals:
                self._last_unknown_event = self._last_measurement_event
                return
            for signal in signals:
                raw = payload.get(signal["field"])
                if raw is None and signal["field"] == "value":
                    raw = payload.get("realtime")
                if raw is None:
                    continue  # Omitted fields must not refresh another signal.
                try:
                    value = finite_number(raw) * finite_number(signal.get("scale", 1))
                    name = signal["name"]
                    if name == "soc_percent":
                        value *= 100000.0 / self._config["battery_capacity_wh"]
                    value = finite_number(value)
                    if name in ("solar_w", "ev_w") and value < 0 or name == "soc_percent" and not 0 <= value <= 100:
                        raise ValueError("measurement out of range")
                    self._values[name] = value
                    self._value_ts[name] = time.time()
                    self._last_seen = datetime.now(timezone.utc)
                except (ValueError, TypeError, OverflowError):
                    self._values[signal["name"]] = None
                    self._value_ts.pop(signal["name"], None)
                    logger.warning("Rejected invalid measurement for %s", signal["name"])

    @om_expose
    def get_config_description(self):
        return json.dumps(self.config_description)

    @om_expose
    def get_config(self):
        config = copy.deepcopy(self._config)
        config.pop("config_schema_version", None)
        return json.dumps(config)

    @om_expose
    def set_config(self, config):
        raw_config = json.loads(config)
        raw_config.setdefault("config_schema_version", self._config.get("config_schema_version", 0))
        data = self._normalize_config(raw_config)
        self._config_checker.check_config(data)
        self._validate_config(data)
        with self._lock:
            self.write_config(data)
            self._config = data
            self._config_error = None
            # Drop timestamps, simulated commands and forecasts after any config
            # change; never let a dry-run command suppress the first live write.
            self._reset_state()
            self._rebuild_signal_map()
            self._discovery_event.set()
        return json.dumps({"success": True})

    @om_expose
    def get_runtime_state(self):
        with self._lock:
            return json.dumps({
                "enabled": self._config["enabled"], "dry_run": self._config["dry_run"],
                "values": self._values,
                "signal_ages": {key: max(0, time.time() - ts) for key, ts in self._value_ts.items()},
                "last_seen": self._last_seen.isoformat() if self._last_seen else None,
                "last_sent_current": self._last_sent_current,
                "last_success_at": self._last_success_at,
                "last_error": self._last_error, "config_error": self._config_error,
                "forecast_error": self._forecast_error,
                "forecast_fetched_at": self._forecast_fetched_at.isoformat() if self._forecast_fetched_at else None,
                "decision": self._decision, "signal_map": self._signal_map,
                "writes_succeeded": self._writes_succeeded, "writes_failed": self._writes_failed,
                "unknown_event_count": self._unknown_event_count, "last_unknown_event": self._last_unknown_event,
                "measurement_event_count": self._measurement_event_count,
                "matched_event_count": self._matched_event_count,
                "last_measurement_event": self._last_measurement_event,
                "charger_identity": self._config["charger_identity"],
                "resolved_endpoint": self._resolved_endpoint,
                "discovery_status": self._discovery_status,
                "discovery_error": self._discovery_error,
                "last_discovery_at": self._last_discovery_at,
            }, allow_nan=False)