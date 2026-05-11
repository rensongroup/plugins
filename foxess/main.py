"""
FoxEss plugin
"""

import json
import logging
import os
import time
import sys
from pathlib import Path
import six
from unittest.mock import MagicMock


logger = logging.getLogger(__name__)
try:
    from . import openapi as f
except ImportError:
    # try relative
    root = Path(__file__).parent.resolve()
    sys.path.insert(0, root)
    import openapi as f

from plugins.base import (
    OMPluginBase,
    PluginConfigChecker,
    background_task,
    om_expose,
    sensor_status,
    measurement_counter_status,
)

TICK_INTERVAL = 5  # seconds - base loop interval
POLL_REAL_TICKS = 120 // TICK_INTERVAL  # poll API every 120s (24 ticks)
POLL_GENERATION_TICKS = (30 * 60) // TICK_INTERVAL  # poll generation every 30min (360 ticks)
FAILURE_NOTIFY_THRESHOLD = 10  # consecutive failures before notifying
FAILURE_NOTIFY_COOLDOWN = 24 * 60 * 60  # seconds between failure notifications

class FoxEss(OMPluginBase):
    """
    FoxEss plugin
    """

    name = "FoxEss"
    version = "0.1.7"
    interfaces = [("config", "1.0")]

    default_config = {}

    def __init__(self, webinterface, connector):
        super(FoxEss, self).__init__(webinterface=webinterface, connector=connector)
        logger.info("Starting FoxEss plugin {0}...".format(FoxEss.version))
        self.config_description = [
            {
                "name": "api_key",
                "type": "str",
                "description": "API key of foxesscloud.com",
            },
            {
                "name": "device_sn",
                "type": "str",
                "description": "Device serial of foxesscloud.com (leave empty to use default)",
            },
        ]
        self._config = self.read_config(FoxEss.default_config)
        self._config_checker = PluginConfigChecker(self.config_description)

        self._mc_battery = (
            self.connector.measurement_counter.register_counter_electricity_wh(
                external_id="foxess/battery",
                name="Battery",
                type=connector.measurement_counter.Enums.Types.BATTERY,
                has_realtime=True,
            )
        )

        self._mc_solar = (
            self.connector.measurement_counter.register_counter_electricity_wh(
                external_id="foxess/solar",
                name="Solar",
                type=connector.measurement_counter.Enums.Types.SOLAR,
                has_realtime=True,
            )
        )
        self._battery_full_sent = False
        self._tick_counter = 0
        self._generation = None
        self._solar_injection = 0.0
        self._solar_real = 0.0
        self._bat_energy = 0.0
        self._bat_real = 0.0
        self._last_reported = None
        self._consecutive_failures = 0
        self._last_failure_notification = 0.0
        logger.info("Started FoxEss plugin {0}".format(FoxEss.version))

    @om_expose
    def get_config_description(self):
        logger.info("Fetching config description")
        return json.dumps(self.config_description)

    @om_expose
    def get_config(self):
        logger.info("Fetching configuration")
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        logger.info("Saving configuration...")
        data = json.loads(config)
        self._save_config(data)
        logger.info("Saving configuration... Done")
        return json.dumps({"success": True})

    def _save_config(self, config):
        for key in config:
            if isinstance(config[key], six.string_types):
                config[key] = str(config[key])
        self._config_checker.check_config(config)
        self._config = config
        self.write_config(config)

    def get_timezone(self):
        try:
            return self.webinterface.get_timezone()["timezone"]
        except Exception:
            return "Europe/Brussels"

    @staticmethod
    def _get_variable(real, variable):
        """Extract a variable value from the real-time data list. Returns None if not found."""
        entry = next((v for v in real if v.get("variable") == variable), None)
        if entry is None:
            return None
        return entry.get("value")

    def _configure_api(self):
        """Configure the foxess API module and ensure connection is valid. Returns True on success."""
        f.api_key = self._config.get("api_key")
        if self._config.get("device_sn"):
            f.device_sn = self._config.get("device_sn")
        f.time_zone = self.get_timezone()
        f.residual_handling = 1
        site = f.get_site()
        if site is None:
            logger.error("Error calling Fox Ess API: do you have a valid token?")
            return False
        f.get_logger()
        f.get_device()
        return True

    def _poll_generation(self):
        """Fetch cumulative generation from the API and update cached solar injection. Returns True on success."""
        generation = f.get_generation()
        if generation is None:
            logger.warning("get_generation() returned None, using previous value")
            return False
        self._generation = generation
        cumulative = generation.get("cumulative")
        if cumulative is not None:
            self._solar_injection = cumulative
        else:
            logger.warning("get_generation() returned no 'cumulative' field")
        return True

    def _poll_real(self):
        """Fetch real-time inverter data and update cached battery/solar values. Returns True on success."""
        real = f.get_real()
        if real is None:
            logger.error("Unknown error calling Fox Ess API: no data received")
            return False

        bat_energy = self._get_variable(real, "ResidualEnergy")
        bat_charge = self._get_variable(real, "batChargePower")
        bat_discharge = self._get_variable(real, "batDischargePower")
        pv_power = self._get_variable(real, "pvPower")

        if bat_energy is not None:
            self._bat_energy = bat_energy
        else:
            logger.warning("Missing 'ResidualEnergy' in real-time data, using previous value")

        if bat_charge is not None and bat_discharge is not None:
            self._bat_real = bat_charge - bat_discharge
        else:
            logger.warning("Missing battery charge/discharge in real-time data, using previous value")

        if pv_power is not None:
            self._solar_real = -pv_power
        else:
            logger.warning("Missing 'pvPower' in real-time data, using previous value")

        self._check_battery_full(self._get_variable(real, "SoC"))
        return True

    def _check_battery_full(self, soc):
        """Send a one-shot notification when battery SoC crosses 100%."""
        if soc is not None and soc > 99:
            if not self._battery_full_sent:
                logger.info("Battery is full again")
                self.connector.notification.send(
                    topic="FoxEss",
                    message="Battery is full again",
                )
                self._battery_full_sent = True
        else:
            self._battery_full_sent = False

    def report_foxess(self):
        """Report cached values to the connector."""
        state = (self._solar_injection, self._solar_real, self._bat_energy, self._bat_real)
        if state != self._last_reported:
            logger.info(f"Solar: tot {self._solar_injection} kWh, real {self._solar_real} kW")
            logger.info(f"Battery: energy {self._bat_energy} kWh, real {self._bat_real} kW")
            self._last_reported = state
        self.report_mc_status(
            self._mc_battery, self._bat_energy * 1000, 0, self._bat_real * 1000
        )
        self.report_mc_status(
            self._mc_solar, 0, self._solar_injection * 1000, self._solar_real * 1000
        )

    def _track_poll_failure(self):
        """Track a consecutive poll failure and notify once per day after threshold."""
        self._consecutive_failures += 1
        if self._consecutive_failures >= FAILURE_NOTIFY_THRESHOLD:
            now = time.time()
            if now - self._last_failure_notification >= FAILURE_NOTIFY_COOLDOWN:
                logger.error(f"FoxEss polling failed {self._consecutive_failures} times in a row")
                self.connector.notification.send(
                    topic="FoxEss",
                    message=f"Polling failed {self._consecutive_failures} consecutive times",
                )
                self._last_failure_notification = now

    def _reset_poll_failures(self):
        """Reset failure counter after a successful poll."""
        self._consecutive_failures = 0

    @background_task
    def loop(self):
        while True:
            now = time.time()
            try:
                if self._tick_counter % POLL_REAL_TICKS == 0:
                    success = False
                    if self._configure_api():
                        gen_ok = True
                        if self._tick_counter % POLL_GENERATION_TICKS == 0:
                            gen_ok = self._poll_generation()
                        real_ok = self._poll_real()
                        success = gen_ok and real_ok
                    if success:
                        self._reset_poll_failures()
                    else:
                        self._track_poll_failure()
                self.report_foxess()
            except Exception:
                logger.exception("Error while polling Fox Ess")
                self._track_poll_failure()
            self._tick_counter += 1
            time.sleep(TICK_INTERVAL - (time.time() - now) % TICK_INTERVAL)

    # Measurement Counters
    def report_mc_status(self, mc_dto, total_consumed, total_injected, realtime):
        logger.debug(
            "publish measurementCounter value for {}: consumed = {}; injected = {}; realtime={}".format(
                mc_dto, total_consumed, total_injected, realtime
            )
        )
        self.connector.measurement_counter.report_counter_state(
            measurement_counter=mc_dto,
            total_consumed=total_consumed,
            total_injected=total_injected,
        )
        self.connector.measurement_counter.report_realtime_state(
            measurement_counter=mc_dto, value=realtime
        )


if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    # setup plumbing with the correct signatures
    from plugin_runtime.web import WebInterfaceDispatcher
    from plugin_runtime.connectors.connector import Connector
    from gateway.utilities.event_loop import EventLoop
    eventloop = EventLoop(name='plugin_event_loop')
    connector = Connector(forward_callback_actions=lambda *args, **kwargs: None, forward_status_event_subscriptions=lambda *args, **kwargs: None, event_loop=eventloop)
    web_dispatcher = WebInterfaceDispatcher("FoxEss")

    # replace by actual key for local development by env FOXESS_API_KEY
    import os
    FoxEss.read_config = MagicMock(
        return_value={"api_key": os.getenv("FOXESS_API_KEY", "")}
    )
    plugin = FoxEss(webinterface=web_dispatcher, connector=connector)
    if plugin._configure_api():
        plugin._poll_generation()
        plugin._poll_real()
        plugin.report_foxess()
