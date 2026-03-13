"""
Home Assistant MQTT Discovery plugin for OpenMotics.

Publishes Home Assistant MQTT discovery messages so that OpenMotics outputs,
inputs and sensors are automatically discovered by Home Assistant.

State updates and command handling are also done via MQTT, so Home Assistant
can both read and control the OpenMotics gateway in real time.

Discovery spec: https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery
"""

import json
import logging
import os
import random
import re
import string
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from plugins.base import (
    OMPluginBase,
    PluginConfigChecker,
    background_task,
    input_status,
    om_expose,
    output_status,
    receive_events,
    sensor_status,
    shutter_status,
)

try:
    this_dir = os.path.realpath(os.path.dirname(__file__))
    for lib in os.listdir(os.path.join(this_dir, "lib")):
        lib_path = os.path.join(this_dir, "lib", lib)
        sys.path.insert(0, lib_path)

    import paho.mqtt.client as mqtt
except Exception as ex:
    raise ImportError("Could not import paho-mqtt library: {0}".format(ex))

logger = logging.getLogger(__name__)

# Mapping from OpenMotics output type integer to HA component and optional device_class.
# component is one of "switch", "light", or "fan".
# device_class is the HA switch device_class (outlet, valve, …) or None for light.
OUTPUT_TYPE_MAP = {
    0: {"component": "switch", "device_class": "outlet"},
    1: {"component": "fan", "device_class": None},  # valve — percentage when dimmable
    2: {
        "component": "switch",
        "device_class": "switch",
    },  # alarm — no specific HA class
    3: {"component": "switch", "device_class": "switch"},  # appliance
    4: {"component": "switch", "device_class": "switch"},  # pump
    5: {"component": "switch", "device_class": "switch"},  # HVAC
    6: {"component": "switch", "device_class": "switch"},  # generic output
    7: {"component": "fan", "device_class": None},  # motor — percentage when dimmable
    8: {"component": "fan", "device_class": None},  # ventilation
    9: {"component": "switch", "device_class": "switch"},  # heater
    127: None,  # screen/shutter — handled by a dedicated shutter class, skip
    255: {"component": "light", "device_class": None},
}

OUTPUT_TYPE_NAMES = {
    0: "Outlet",
    1: "Valve",
    2: "Alarm",
    3: "Appliance",
    4: "Pump",
    5: "HVAC",
    6: "Output",
    7: "Motor",
    8: "Ventilation",
    9: "Heater",
    127: "Screen",
    255: "Light",
}

# Mapping from OpenMotics physical_quantity to Home Assistant device_class and unit
SENSOR_HA_MAP = {
    "temperature": {"device_class": "temperature", "unit": "°C", "component": "sensor"},
    "humidity": {"device_class": "humidity", "unit": "%", "component": "sensor"},
    "brightness": {"device_class": "illuminance", "unit": "lx", "component": "sensor"},
    "pressure": {"device_class": "pressure", "unit": "hPa", "component": "sensor"},
    "co2": {"device_class": "carbon_dioxide", "unit": "ppm", "component": "sensor"},
    "voc": {
        "device_class": "volatile_organic_compounds",
        "unit": "ppm",
        "component": "sensor",
    },
    "sound": {"device_class": None, "unit": "dB", "component": "sensor"},
    "dust": {"device_class": "pm10", "unit": "µg/m³", "component": "sensor"},
    "comfort_index": {"device_class": None, "unit": None, "component": "sensor"},
}


class HomeAssistant(OMPluginBase):
    """
    Home Assistant MQTT Discovery plugin for OpenMotics.

    Automatically publishes HA MQTT discovery payloads for:
      - Outputs  → switch or light (dimmer) entities
      - Inputs   → binary_sensor entities
      - Sensors  → sensor entities (temperature, humidity, …)

    State is published on every change and on a configurable heartbeat interval
    so that HA always has fresh data even after a restart.
    """

    name = "HomeAssistant"
    version = "1.0.67"
    interfaces = [("config", "1.0")]

    config_description = [
        # ── MQTT broker ───────────────────────────────────────────────────────
        {
            "name": "mqtt_hostname",
            "type": "str",
            "description": "Hostname or IP address of the MQTT broker (e.g. your Home Assistant host).",
        },
        {
            "name": "mqtt_port",
            "type": "int",
            "description": "MQTT broker port. Default: 1883",
        },
        {
            "name": "mqtt_username",
            "type": "str",
            "description": "MQTT broker username (leave empty for anonymous access).",
        },
        {
            "name": "mqtt_password",
            "type": "password",
            "description": "MQTT broker password.",
        },
        # ── HA discovery ──────────────────────────────────────────────────────
        {
            "name": "discovery_prefix",
            "type": "str",
            "description": "Home Assistant MQTT discovery prefix. Default: homeassistant",
        },
        {
            "name": "openmotics_device_id",
            "type": "str",
            "description": (
                "Unique identifier for this OpenMotics gateway used in discovery topics "
                "and as the HA device identifier. Leave empty to auto-derive from MAC address."
            ),
        },
        # ── entity toggles ────────────────────────────────────────────────────
        {
            "name": "publish_outputs",
            "type": "bool",
            "description": "Publish discovery messages for OpenMotics outputs (switches / lights).",
        },
        {
            "name": "publish_inputs",
            "type": "bool",
            "description": "Publish discovery messages for OpenMotics inputs as binary sensors.",
        },
        {
            "name": "publish_sensors",
            "type": "bool",
            "description": "Publish discovery messages for OpenMotics sensors.",
        },
        {
            "name": "publish_shutters",
            "type": "bool",
            "description": "Publish discovery messages for OpenMotics shutters as cover entities.",
        },
        # ── heartbeat ─────────────────────────────────────────────────────────
        {
            "name": "heartbeat_interval",
            "type": "int",
            "description": (
                "Interval in seconds for republishing current state to all entities. "
                "This keeps HA in sync after restarts. Default: 300, minimum: 30"
            ),
        },
        # ── sensor polling ────────────────────────────────────────────────────
        {
            "name": "sensor_poll_interval",
            "type": "int",
            "description": "Interval in seconds for polling sensor values. Default: 60, minimum: 10",
        },
        # ── config polling ────────────────────────────────────────────────────
        {
            "name": "config_poll_interval",
            "type": "int",
            "description": (
                "Interval in seconds for polling OpenMotics configuration for changes "
                "(e.g. renamed outputs). Re-publishes discovery when changes are detected. "
                "Default: 300, minimum: 60"
            ),
        },
    ]

    default_config = {
        "mqtt_port": 1883,
        "discovery_prefix": "homeassistant",
        "publish_outputs": True,
        "publish_inputs": True,
        "publish_sensors": True,
        "publish_shutters": True,
        "heartbeat_interval": 300,
        "sensor_poll_interval": 60,
        "config_poll_interval": 300,
    }

    def __init__(self, webinterface, connector):
        super(HomeAssistant, self).__init__(
            webinterface=webinterface, connector=connector
        )

        logger.info("Starting HomeAssistant plugin {0}…".format(HomeAssistant.version))

        self._config = self.read_config(HomeAssistant.default_config)
        self._config_checker = PluginConfigChecker(HomeAssistant.config_description)

        self._executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="ha-plugin"
        )
        self._client = None
        self._connected = False
        # Cache of last-published discovery payloads: topic → json string.
        # Used to skip republishing discovery configs that haven't changed.
        # Cleared on disconnect so the next reconnect always does a full publish.
        self._published_discovery = {}
        # Guard against treating a retained homeassistant/status: online message
        # (delivered immediately on every reconnect) as a genuine HA birth event.
        # Set to True only after the initial publish burst completes; reset to
        # False on disconnect so reconnects don't re-trigger a full republish
        # until the burst finishes again.
        self._initial_publish_done = False

        # Gateway version info (loaded at startup)
        self._gateway_version = ""  # e.g. "3.11.1"
        self._master_version = ""  # e.g. "1.0.169"

        # Local caches: id → config dict (with 'status', 'dimmer', … fields)
        self._rooms = {}  # room_id → name (255 = unassigned, excluded)
        self._outputs = {}  # output_id → {name, module_type, type, status, dimmer, room}
        self._inputs = {}  # input_id  → {name, status, room}
        self._sensors = {}  # sensor_id → {device_name, physical_quantity, unit, external_id, source, room}
        self._shutters = {}  # shutter_id → {name, has_position, state, position, room}
        # Pending travel timers: shutter_id → [cancelled_flag]
        # A ThreadPoolExecutor task polls the flag and fires after timer_up/timer_down seconds.
        # Set cancelled_flag[0] = True to abort before the delay expires.
        self._shutter_timers = {}  # type: dict

        # Cache of last-published state payloads: topic → string value.
        # Heartbeat skips republishing topics whose value hasn't changed.
        # Cleared on disconnect so the next reconnect always does a full publish.
        self._published_states = {}

        # Set of retained discovery topics seen on the broker (populated during cleanup scan)
        self._retained_discovery_topics = set()
        self._scanning_retained = False
        self._scanning_shutter_states = False
        # Cancellation flag for the in-flight _delayed_cleanup task.
        # Set to True when a new _on_connect fires so the previous task aborts.
        self._cleanup_cancelled = [False]

        self._read_config()
        # Kick off MQTT connection + configuration loading in the background so
        # __init__ returns immediately.  The plugin runner has a 5-second IPC
        # timeout for subscribe_status_event; blocking here on slow gateways
        # (many inputs/outputs) causes a PluginCommunicationTimeout crash.
        self._executor.submit(self._startup)

        logger.info("Started HomeAssistant plugin {0}".format(HomeAssistant.version))

    def _startup(self):
        """Background startup: connect to MQTT and load all configuration.
        Runs in the executor so __init__ returns immediately and the plugin
        runner IPC handshake can complete within its 5-second timeout."""
        try:
            self._connect()
            self._load_all_configuration()
        except Exception:
            logger.exception("Error during background startup")

    # ──────────────────────────────────────────────────────────────────────────
    # Configuration helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _read_config(self):
        self._mqtt_hostname = self._config.get("mqtt_hostname")
        self._mqtt_port = int(self._config.get("mqtt_port", 1883))
        self._mqtt_username = self._config.get("mqtt_username") or None
        self._mqtt_password = self._config.get("mqtt_password") or None
        self._discovery_prefix = self._config.get("discovery_prefix", "homeassistant")
        self._device_id = (
            self._config.get("openmotics_device_id") or self._mac_device_id()
        )
        self._publish_outputs = bool(self._config.get("publish_outputs", True))
        self._publish_inputs = bool(self._config.get("publish_inputs", True))
        self._publish_sensors = bool(self._config.get("publish_sensors", True))
        self._publish_shutters = bool(self._config.get("publish_shutters", True))
        self._heartbeat_interval = max(
            30, int(self._config.get("heartbeat_interval", 300))
        )
        self._sensor_poll_interval = max(
            10, int(self._config.get("sensor_poll_interval", 60))
        )
        self._config_poll_interval = max(
            60, int(self._config.get("config_poll_interval", 300))
        )
        self._enabled = bool(self._mqtt_hostname)
        logger.info("Device ID: {0}".format(self._device_id))
        logger.info(
            "HAMQTTDiscovery is {0}".format(
                "enabled" if self._enabled else "disabled (no broker hostname)"
            )
        )

    # ──────────────────────────────────────────────────────────────────────────
    # MQTT connection
    # ──────────────────────────────────────────────────────────────────────────

    def _connect(self):
        if not self._enabled:
            return
        try:
            if self._client is not None:
                try:
                    self._client.disconnect()
                except Exception:
                    pass
            # Include device_id for human readability in broker logs, plus a
            # per-process random suffix so two plugin instances on the same
            # gateway (e.g. during an upgrade transition) never share a
            # client_id and kick each other off the broker.
            _suffix = "".join(
                random.choices(string.ascii_lowercase + string.digits, k=6)
            )
            self._client = mqtt.Client(
                client_id="openmotics-ha-{0}-{1}".format(self._device_id, _suffix)
            )
            if self._mqtt_username:
                self._client.username_pw_set(self._mqtt_username, self._mqtt_password)
            self._client.on_connect = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.on_message = self._on_message
            self._client.connect(self._mqtt_hostname, self._mqtt_port, keepalive=60)
            self._client.loop_start()
        except Exception:
            logger.exception("Error connecting to MQTT broker")

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            logger.error("MQTT connection failed with rc={0}".format(rc))
            self._connected = False
            return
        logger.info(
            "Connected to MQTT broker {0}:{1}".format(
                self._mqtt_hostname, self._mqtt_port
            )
        )
        self._connected = True
        # Subscribe to command topics for outputs
        output_cmd_topic = "{prefix}/{device_id}/output/+/set".format(
            prefix=self._discovery_prefix, device_id=self._device_id
        )
        self._client.subscribe(output_cmd_topic)
        logger.info("Subscribed to {0}".format(output_cmd_topic))
        # Subscribe to brightness command topics for dimmable lights
        brightness_cmd_topic = "{prefix}/{device_id}/output/+/brightness/set".format(
            prefix=self._discovery_prefix, device_id=self._device_id
        )
        self._client.subscribe(brightness_cmd_topic)
        logger.info("Subscribed to {0}".format(brightness_cmd_topic))
        # Subscribe to percentage command topics for dimmable fans
        percentage_cmd_topic = "{prefix}/{device_id}/output/+/percentage/set".format(
            prefix=self._discovery_prefix, device_id=self._device_id
        )
        self._client.subscribe(percentage_cmd_topic)
        logger.info("Subscribed to {0}".format(percentage_cmd_topic))
        # Subscribe to shutter command topics
        shutter_cmd_topic = "{prefix}/{device_id}/shutter/+/set".format(
            prefix=self._discovery_prefix, device_id=self._device_id
        )
        self._client.subscribe(shutter_cmd_topic)
        logger.info("Subscribed to {0}".format(shutter_cmd_topic))
        # Subscribe to shutter position set topics
        shutter_pos_topic = "{prefix}/{device_id}/shutter/+/position/set".format(
            prefix=self._discovery_prefix, device_id=self._device_id
        )
        self._client.subscribe(shutter_pos_topic)
        logger.info("Subscribed to {0}".format(shutter_pos_topic))
        # Subscribe to Home Assistant birth messages
        ha_status_topic = "{prefix}/status".format(prefix=self._discovery_prefix)
        self._client.subscribe(ha_status_topic)
        logger.info("Subscribed to HA status: {0}".format(ha_status_topic))
        # Subscribe to existing discovery config topics so we can detect stale entries
        self._retained_discovery_topics = set()
        self._scanning_retained = True
        scan_topic = "{prefix}/+/{device_id}/+/config".format(
            prefix=self._discovery_prefix, device_id=self._device_id
        )
        self._client.subscribe(scan_topic)
        logger.info("Subscribed to retained discovery scan: {0}".format(scan_topic))
        shutter_state_scan_topic = "{prefix}/{device_id}/shutter/+/state".format(
            prefix=self._discovery_prefix, device_id=self._device_id
        )

        # Cancel any previously running _delayed_cleanup (e.g. from a prior
        # connect attempt that got disconnected before it finished).
        self._cleanup_cancelled[0] = True
        cancelled = [False]
        self._cleanup_cancelled = cancelled

        def _poll_sleep(seconds):
            """Sleep for `seconds` in 0.1 s increments, returning True if cancelled."""
            elapsed = 0.0
            while elapsed < seconds:
                time.sleep(0.1)
                elapsed += 0.1
                if cancelled[0]:
                    return True
            return False

        def _delayed_cleanup():
            # Wait for the broker to deliver retained messages
            if _poll_sleep(3):
                return
            self._scanning_retained = False
            self._client.unsubscribe(scan_topic)
            # Subscribe briefly to retained shutter state topics so we can restore
            # last_direction from whatever the broker has retained.
            self._scanning_shutter_states = True
            self._client.subscribe(shutter_state_scan_topic)
            logger.info(
                "Subscribed to retained shutter state scan: {0}".format(
                    shutter_state_scan_topic
                )
            )
            if _poll_sleep(1):
                self._scanning_shutter_states = False
                return
            self._scanning_shutter_states = False
            self._client.unsubscribe(shutter_state_scan_topic)
            self._cleanup_stale_discovery()
            self._publish_all_discovery()
            # Publish gateway diagnostic states once at startup (not via _publish_all_discovery
            # to avoid overwriting them on heartbeat/HA-birth republishes).
            self._publish_gateway_discovery(publish_state=True)
            self._publish_all_states()
            # Mark initial publish complete so HA birth messages can trigger republish
            self._initial_publish_done = True
            logger.info("Initial MQTT publish burst complete")

        self._executor.submit(_delayed_cleanup)

    def _on_disconnect(self, client, userdata, rc):
        self._connected = False
        self._initial_publish_done = False
        self._published_discovery = {}
        self._published_states = {}
        self._scanning_shutter_states = False
        # Abort any in-flight _delayed_cleanup so it doesn't hold a thread slot
        self._cleanup_cancelled[0] = True
        if rc != 0:
            logger.warning(
                "Unexpected MQTT disconnect (rc={0}). Will auto-reconnect.".format(rc)
            )

    def _on_message(self, client, userdata, msg):
        """Handle incoming MQTT messages (output set commands from HA)."""
        try:
            payload = msg.payload.decode("utf-8").strip()

            # Collect retained discovery config topics during the startup scan window
            if self._scanning_retained and msg.topic.endswith("/config") and payload:
                self._retained_discovery_topics.add(msg.topic)
                return

            # Restore last_direction from retained shutter state topics on startup
            if (
                self._scanning_shutter_states
                and msg.topic.endswith("/state")
                and payload
            ):
                shutter_state_match = re.match(r"^.+/shutter/(\d+)/state$", msg.topic)
                if shutter_state_match:
                    sid = int(shutter_state_match.group(1))
                    if (
                        sid in self._shutters
                        and self._shutters[sid].get("last_direction") is None
                    ):
                        # Restore enough state to know Close is safe to offer.
                        # Any non-None last_direction allows _publish_shutter_state
                        # to emit "open" on next stop, keeping Close available.
                        # We don't need the exact direction from the retained state —
                        # just mark direction as known so the "unknown" guard is bypassed.
                        if payload in ("open", "closed", "opening", "closing"):
                            self._shutters[sid]["last_direction"] = "down"
                        if self._shutters[sid]["last_direction"] is not None:
                            logger.info(
                                "Restored last_direction={0} for shutter {1} from retained state '{2}'".format(
                                    self._shutters[sid]["last_direction"], sid, payload
                                )
                            )
                    return

            # Home Assistant birth message: HA just (re)started, republish everything.
            # Guard: ignore the retained 'online' message delivered immediately on
            # (re)connect — only act on it once our own initial publish burst is done.
            ha_status_topic = "{prefix}/status".format(prefix=self._discovery_prefix)
            if msg.topic == ha_status_topic and payload == "online":
                if not self._initial_publish_done:
                    logger.info(
                        "HA status 'online' received before initial publish completed — ignoring"
                    )
                    return
                logger.info(
                    "Home Assistant came online — republishing discovery and state"
                )
                self._executor.submit(self._publish_all_discovery)
                self._executor.submit(self._publish_all_states)
                return

            # Brightness command: <prefix>/<device_id>/output/<id>/brightness/set
            brightness_match = re.match(r"^.+/output/(\d+)/brightness/set$", msg.topic)
            if brightness_match:
                output_id = int(brightness_match.group(1))
                if output_id not in self._outputs:
                    logger.warning(
                        "Received brightness command for unknown output {0}".format(
                            output_id
                        )
                    )
                    return
                output = self._outputs[output_id]
                logger.info(
                    "Received brightness command for output {0} ({1}): {2}".format(
                        output_id, output.get("name"), payload
                    )
                )
                try:
                    brightness = int(payload)
                    dimmer = max(0, min(100, int(round(brightness * 100.0 / 255.0))))
                    is_on = "true" if dimmer > 0 else "false"
                    result = json.loads(
                        self.webinterface.set_output(
                            id=output_id, is_on=is_on, dimmer=dimmer
                        )
                    )
                    if not result.get("success"):
                        logger.error(
                            "Failed to set brightness for output {0}: {1}".format(
                                output_id, result.get("msg")
                            )
                        )
                except ValueError:
                    logger.error("Invalid brightness payload: {0}".format(payload))
                return

            # Percentage command (dimmable fan): <prefix>/<device_id>/output/<id>/percentage/set
            percentage_match = re.match(r"^.+/output/(\d+)/percentage/set$", msg.topic)
            if percentage_match:
                output_id = int(percentage_match.group(1))
                if output_id not in self._outputs:
                    logger.warning(
                        "Received percentage command for unknown output {0}".format(
                            output_id
                        )
                    )
                    return
                output = self._outputs[output_id]
                logger.info(
                    "Received percentage command for output {0} ({1}): {2}".format(
                        output_id, output.get("name"), payload
                    )
                )
                try:
                    # HA fan percentage is already 0-100
                    dimmer = max(0, min(100, int(payload)))
                    is_on = "true" if dimmer > 0 else "false"
                    result = json.loads(
                        self.webinterface.set_output(
                            id=output_id, is_on=is_on, dimmer=dimmer
                        )
                    )
                    if not result.get("success"):
                        logger.error(
                            "Failed to set percentage for output {0}: {1}".format(
                                output_id, result.get("msg")
                            )
                        )
                except ValueError:
                    logger.error("Invalid percentage payload: {0}".format(payload))
                return

            # Shutter position set: <prefix>/<device_id>/shutter/<id>/position/set
            shutter_pos_match = re.match(r"^.+/shutter/(\d+)/position/set$", msg.topic)
            if shutter_pos_match:
                shutter_id = int(shutter_pos_match.group(1))
                if shutter_id not in self._shutters:
                    logger.warning(
                        "Received position command for unknown shutter {0}".format(
                            shutter_id
                        )
                    )
                    return
                shutter = self._shutters[shutter_id]
                try:
                    target = max(0, min(100, int(float(payload))))
                except ValueError:
                    logger.error("Invalid position payload: {0}".format(payload))
                    return
                current = shutter.get("sw_position")
                logger.info(
                    "Received position command for shutter {0}: target={1}, current={2}".format(
                        shutter_id, target, current
                    )
                )
                if shutter.get("has_position"):
                    # Hardware positioning — use do_shutter_goto
                    steps = shutter.get("steps", 200)
                    om_pos = int(round((100 - target) * steps / 100.0))
                    result = json.loads(
                        self.webinterface.do_shutter_goto(
                            id=shutter_id, position=om_pos
                        )
                    )
                    if not result.get("success"):
                        logger.error(
                            "Failed to goto position for shutter {0}: {1}".format(
                                shutter_id, result.get("msg")
                            )
                        )
                    return
                # Software positioning: determine direction and travel time
                if current is None:
                    # Unknown position — can't do partial travel; just go full open/close
                    if target >= 50:
                        result = json.loads(
                            self.webinterface.do_shutter_up(id=shutter_id)
                        )
                        if result.get("success"):
                            shutter["state"] = "going_up"
                            shutter["last_direction"] = "up"
                            shutter["travel_start_time"] = time.time()
                            self._mqtt_publish(
                                self._shutter_state_topic(shutter_id),
                                "opening",
                                retain=True,
                            )
                            self._schedule_shutter_arrival(
                                shutter_id, "open", shutter.get("timer_up", 30)
                            )
                    else:
                        result = json.loads(
                            self.webinterface.do_shutter_down(id=shutter_id)
                        )
                        if result.get("success"):
                            shutter["state"] = "going_down"
                            shutter["last_direction"] = "down"
                            shutter["travel_start_time"] = time.time()
                            self._mqtt_publish(
                                self._shutter_state_topic(shutter_id),
                                "closing",
                                retain=True,
                            )
                            self._schedule_shutter_arrival(
                                shutter_id, "closed", shutter.get("timer_down", 30)
                            )
                    return
                if target == current:
                    return
                if target > current:
                    # Need to open (go up)
                    travel_fraction = (target - current) / 100.0
                    delay = shutter.get("timer_up", 30) * travel_fraction
                    result = json.loads(self.webinterface.do_shutter_up(id=shutter_id))
                    if result.get("success"):
                        shutter["state"] = "going_up"
                        shutter["last_direction"] = "up"
                        shutter["travel_start_time"] = time.time()
                        self._mqtt_publish(
                            self._shutter_state_topic(shutter_id),
                            "opening",
                            retain=True,
                        )
                        self._schedule_shutter_arrival(
                            shutter_id, "open", delay, target_sw_position=target
                        )
                else:
                    # Need to close (go down)
                    travel_fraction = (current - target) / 100.0
                    delay = shutter.get("timer_down", 30) * travel_fraction
                    result = json.loads(
                        self.webinterface.do_shutter_down(id=shutter_id)
                    )
                    if result.get("success"):
                        shutter["state"] = "going_down"
                        shutter["last_direction"] = "down"
                        shutter["travel_start_time"] = time.time()
                        self._mqtt_publish(
                            self._shutter_state_topic(shutter_id),
                            "closing",
                            retain=True,
                        )
                        self._schedule_shutter_arrival(
                            shutter_id, "closed", delay, target_sw_position=target
                        )
                return

            # Shutter command: <prefix>/<device_id>/shutter/<id>/set  (OPEN/CLOSE/STOP)
            shutter_cmd_match = re.match(r"^.+/shutter/(\d+)/set$", msg.topic)
            if shutter_cmd_match:
                shutter_id = int(shutter_cmd_match.group(1))
                if shutter_id not in self._shutters:
                    logger.warning(
                        "Received command for unknown shutter {0}".format(shutter_id)
                    )
                    return
                shutter = self._shutters[shutter_id]
                logger.info(
                    "Received command for shutter {0} ({1}): {2}".format(
                        shutter_id, shutter.get("name"), payload
                    )
                )
                if payload == "OPEN":
                    result = json.loads(self.webinterface.do_shutter_up(id=shutter_id))
                    if result.get("success"):
                        shutter["state"] = "going_up"
                        shutter["last_direction"] = "up"
                        shutter["travel_start_time"] = time.time()
                        self._mqtt_publish(
                            self._shutter_state_topic(shutter_id),
                            "opening",
                            retain=True,
                        )
                        self._schedule_shutter_arrival(
                            shutter_id, "open", shutter.get("timer_up", 30)
                        )
                elif payload == "CLOSE":
                    result = json.loads(
                        self.webinterface.do_shutter_down(id=shutter_id)
                    )
                    if result.get("success"):
                        shutter["state"] = "going_down"
                        shutter["last_direction"] = "down"
                        shutter["travel_start_time"] = time.time()
                        self._mqtt_publish(
                            self._shutter_state_topic(shutter_id),
                            "closing",
                            retain=True,
                        )
                        self._schedule_shutter_arrival(
                            shutter_id, "closed", shutter.get("timer_down", 30)
                        )
                elif payload == "STOP":
                    result = json.loads(
                        self.webinterface.do_shutter_stop(id=shutter_id)
                    )
                    if result.get("success"):
                        self._cancel_shutter_timer(shutter_id)
                        shutter["state"] = "stopped"
                        # Compute sw_position from elapsed travel time
                        self._update_sw_position_on_stop(shutter_id)
                        # Publish state opposite to travel direction so that
                        # direction button is immediately available again
                        last = shutter.get("last_direction")
                        stop_state = (
                            "open"
                            if last == "down"
                            else "closed"
                            if last == "up"
                            else None
                        )
                        if stop_state:
                            self._mqtt_publish(
                                self._shutter_state_topic(shutter_id),
                                stop_state,
                                retain=True,
                            )
                        self._publish_shutter_position(shutter_id)
                else:
                    logger.warning("Unknown shutter payload: {0}".format(payload))
                    return
                if not result.get("success"):
                    logger.error(
                        "Failed to set shutter {0}: {1}".format(
                            shutter_id, result.get("msg")
                        )
                    )
                return

            # ON/OFF command: <prefix>/<device_id>/output/<id>/set
            cmd_match = re.match(r"^.+/output/(\d+)/set$", msg.topic)
            if not cmd_match:
                return
            output_id = int(cmd_match.group(1))
            if output_id not in self._outputs:
                logger.warning(
                    "Received command for unknown output {0}".format(output_id)
                )
                return
            output = self._outputs[output_id]
            logger.info(
                "Received command for output {0} ({1}): {2}".format(
                    output_id, output.get("name"), payload
                )
            )
            is_on = "true" if payload == "ON" else "false"
            result = json.loads(self.webinterface.set_output(id=output_id, is_on=is_on))
            if not result.get("success"):
                logger.error(
                    "Failed to set output {0}: {1}".format(output_id, result.get("msg"))
                )
        except Exception:
            logger.exception(
                "Error processing MQTT message on topic {0}".format(msg.topic)
            )

    # ──────────────────────────────────────────────────────────────────────────
    # Topic helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _output_state_topic(self, output_id):
        return "{prefix}/{device_id}/output/{id}/state".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=output_id
        )

    def _output_command_topic(self, output_id):
        return "{prefix}/{device_id}/output/{id}/set".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=output_id
        )

    def _output_brightness_state_topic(self, output_id):
        return "{prefix}/{device_id}/output/{id}/brightness/state".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=output_id
        )

    def _output_brightness_command_topic(self, output_id):
        return "{prefix}/{device_id}/output/{id}/brightness/set".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=output_id
        )

    def _output_percentage_state_topic(self, output_id):
        return "{prefix}/{device_id}/output/{id}/percentage/state".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=output_id
        )

    def _output_percentage_command_topic(self, output_id):
        return "{prefix}/{device_id}/output/{id}/percentage/set".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=output_id
        )

    def _shutter_state_topic(self, shutter_id):
        return "{prefix}/{device_id}/shutter/{id}/state".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=shutter_id
        )

    def _shutter_command_topic(self, shutter_id):
        return "{prefix}/{device_id}/shutter/{id}/set".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=shutter_id
        )

    def _shutter_position_state_topic(self, shutter_id):
        return "{prefix}/{device_id}/shutter/{id}/position/state".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=shutter_id
        )

    def _shutter_position_command_topic(self, shutter_id):
        return "{prefix}/{device_id}/shutter/{id}/position/set".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=shutter_id
        )

    def _input_state_topic(self, input_id):
        return "{prefix}/{device_id}/input/{id}/state".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=input_id
        )

    def _sensor_state_topic(self, sensor_id):
        return "{prefix}/{device_id}/sensor/{id}/state".format(
            prefix=self._discovery_prefix, device_id=self._device_id, id=sensor_id
        )

    def _discovery_topic(self, component, object_id):
        """Return the HA MQTT discovery config topic for the given component."""
        return "{prefix}/{component}/{device_id}/{object_id}/config".format(
            prefix=self._discovery_prefix,
            component=component,
            device_id=self._device_id,
            object_id=object_id,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # HA device block — one device per entity
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _mac_device_id():
        """Return a unique device ID based on the gateway's MAC address.

        Tries platform_utils first (available on the gateway), then falls back to
        reading /sys/class/net directly. Returns 'openmotics' if nothing works.
        """
        try:
            from platform_utils import Hardware  # type: ignore

            mac = Hardware.get_mac_address()
            logger.info("platform_utils MAC: {0}".format(mac))
            if mac:
                return "openmotics_" + mac.replace(":", "").lower()
        except Exception as e:
            logger.warning("platform_utils MAC failed: {0}".format(e))
        # Fallback: read the first non-loopback interface from /sys/class/net
        try:
            for iface in sorted(os.listdir("/sys/class/net")):
                if iface == "lo":
                    continue
                addr_path = "/sys/class/net/{0}/address".format(iface)
                if os.path.exists(addr_path):
                    with open(addr_path) as f:
                        mac = f.read().strip().replace(":", "").lower()
                    if mac and mac != "000000000000":
                        logger.info("sys/class/net MAC: {0}".format(mac))
                        return "openmotics_" + mac
        except Exception as e:
            logger.warning("sys/class/net MAC failed: {0}".format(e))
        logger.warning("MAC resolution failed, falling back to 'openmotics'")
        return "openmotics"

    def _room_name(self, room_id):
        """Return the room name for a given room id, or None if unassigned."""
        if room_id is None or room_id == 255:
            return None
        return self._rooms.get(room_id)

    def _sensor_device_block(self, sensor_device_id, device_name, room=None):
        """Return the HA device block for a grouped physical sensor device."""
        block = {
            "identifiers": [sensor_device_id],
            "name": device_name,
            "manufacturer": "OpenMotics",
            "model": "Sensor",
            "via_device": self._device_id,
        }
        area = self._room_name(room)
        if area:
            block["suggested_area"] = area
        return block

    def _device_info(self, entity_type, entity_id, name, model=None, room=None):
        """Return an HA device block that is unique per entity.

        Each output/input/sensor becomes its own device in Home Assistant,
        grouped under the OpenMotics gateway via `via_device`.
        """
        unique_id = "{device_id}_{type}_{id}".format(
            device_id=self._device_id, type=entity_type, id=entity_id
        )
        block = {
            "identifiers": [unique_id],
            "name": name,
            "manufacturer": "OpenMotics",
            "model": model or entity_type.capitalize(),
            "via_device": self._device_id,
        }
        area = self._room_name(room)
        if area:
            block["suggested_area"] = area
        return block

    # ──────────────────────────────────────────────────────────────────────────
    # Gateway data loading
    # ──────────────────────────────────────────────────────────────────────────

    def _load_all_configuration(self):
        """Load gateway versions, rooms, outputs, inputs, sensors and shutters from the gateway.
        Retries until all succeed (the gateway may not be ready immediately).
        """
        ver_ok = rooms_ok = outputs_ok = inputs_ok = sensors_ok = shutters_ok = False
        while not all(
            [ver_ok, rooms_ok, outputs_ok, inputs_ok, sensors_ok, shutters_ok]
        ):
            if not ver_ok:
                ver_ok = self._load_gateway_version()
            if not rooms_ok:
                rooms_ok = self._load_rooms()
            if not outputs_ok:
                outputs_ok = self._load_outputs()
            if not inputs_ok:
                inputs_ok = self._load_inputs()
            if not sensors_ok:
                sensors_ok = self._load_sensors()
            if not shutters_ok:
                shutters_ok = self._load_shutters()
            if not all(
                [ver_ok, rooms_ok, outputs_ok, inputs_ok, sensors_ok, shutters_ok]
            ):
                time.sleep(15)

    def _load_gateway_version(self):
        try:
            result = json.loads(self.webinterface.get_version())
            if not result.get("success"):
                logger.error("Failed to load gateway version")
                return False
            self._gateway_version = result.get("gateway", "")
            self._master_version = result.get("master", "")
            logger.info(
                "Gateway version: {0}, master: {1}".format(
                    self._gateway_version, self._master_version
                )
            )
            return True
        except Exception:
            logger.exception("Error loading gateway version")
            return False

    def _load_rooms(self):
        try:
            result = json.loads(self.webinterface.get_room_configurations())
            if not result.get("success"):
                logger.error("Failed to load room configurations")
                return False
            self._rooms = {}
            for cfg in result.get("config", []):
                rid = cfg.get("id")
                name = cfg.get("name", "").strip()
                if rid is None or rid == 255 or not name:
                    continue
                self._rooms[rid] = name
            logger.info("Loaded {0} rooms".format(len(self._rooms)))
            return True
        except Exception:
            logger.exception("Error loading room configurations")
            return False

    def _load_outputs(self):
        if not self._publish_outputs:
            return True
        try:
            result = json.loads(self.webinterface.get_output_configurations())
            if not result.get("success"):
                logger.error("Failed to load output configurations")
                return False
            self._outputs = {}
            for cfg in result.get("config", []):
                if cfg.get("module_type") not in ["o", "O", "d", "D"]:
                    continue
                if not cfg.get("in_use", True):
                    continue
                oid = cfg["id"]
                self._outputs[oid] = {
                    "name": cfg.get("name", ""),
                    "module_type": (
                        "dimmer" if cfg["module_type"] in ["d", "D"] else "output"
                    ),
                    "type": int(cfg.get("type", 0)),
                    "room": cfg.get("room"),
                    "status": 0,
                    "dimmer": 0,
                }
            # Fetch current status
            status_result = json.loads(self.webinterface.get_output_status())
            if status_result.get("success"):
                for entry in status_result.get("status", []):
                    oid = entry["id"]
                    if oid in self._outputs:
                        self._outputs[oid]["status"] = entry.get("status", 0)
                        self._outputs[oid]["dimmer"] = entry.get("dimmer", 0)
            logger.info("Loaded {0} outputs".format(len(self._outputs)))
            return True
        except Exception:
            logger.exception("Error loading output configurations")
            return False

    def _load_inputs(self):
        if not self._publish_inputs:
            return True
        try:
            result = json.loads(self.webinterface.get_input_configurations())
            if not result.get("success"):
                logger.error("Failed to load input configurations")
                return False
            self._inputs = {}
            for cfg in result.get("config", []):
                if not cfg.get("name", "").strip():
                    continue
                iid = cfg["id"]
                self._inputs[iid] = {
                    "name": cfg.get("name", ""),
                    "room": cfg.get("room"),
                    "status": 0,
                }
            # Fetch current status
            status_result = json.loads(self.webinterface.get_input_status())
            if status_result.get("success"):
                for entry in status_result.get("status", []):
                    iid = entry["id"]
                    if iid in self._inputs:
                        self._inputs[iid]["status"] = entry.get("status", 0)
            logger.info("Loaded {0} inputs".format(len(self._inputs)))
            return True
        except Exception:
            logger.exception("Error loading input configurations")
            return False

    def _load_sensors(self):
        if not self._publish_sensors:
            return True
        try:
            result = json.loads(self.webinterface.get_sensor_configurations())
            if not result.get("success"):
                logger.error("Failed to load sensor configurations")
                return False
            self._sensors = {}
            for cfg in result.get("config", []):
                if not cfg.get("in_use", False):
                    continue
                sid = cfg["id"]
                ext_id = str(cfg.get("external_id", sid))
                # device_name: the name of the physical sensor device (groups all quantities)
                device_name = cfg.get("name", "").strip() or "Sensor {0}".format(ext_id)
                self._sensors[sid] = {
                    "device_name": device_name,
                    "physical_quantity": str(cfg.get("physical_quantity", "")),
                    "unit": str(cfg.get("unit", "")),
                    "external_id": ext_id,
                    "room": cfg.get("room"),
                    "source": cfg.get("source"),
                    "value": None,
                }
            logger.info("Loaded {0} sensors".format(len(self._sensors)))
            return True
        except Exception:
            logger.exception("Error loading sensor configurations")
            return False

    def _load_shutters(self):
        if not self._publish_shutters:
            return True
        try:
            result = json.loads(self.webinterface.get_shutter_configurations())
            if not result.get("success"):
                logger.error("Failed to load shutter configurations")
                return False
            self._shutters = {}
            for cfg in result.get("config", []):
                if not cfg.get("in_use", True):
                    continue
                if not cfg.get("name", ""):
                    continue
                sid = cfg["id"]
                steps = cfg.get("steps", 65535)
                has_position = steps not in (0, 65535)
                self._shutters[sid] = {
                    "name": cfg["name"],
                    "has_position": has_position,
                    "steps": steps,
                    "timer_up": cfg.get("timer_up", 30),
                    "timer_down": cfg.get("timer_down", 30),
                    "room": cfg.get("room"),
                    "state": "stopped",
                    "last_direction": None,
                    "position": None,
                    # Software-tracked position: 0=closed, 100=open (HA convention).
                    # None = unknown (never moved since plugin start).
                    "sw_position": None,
                    "travel_start_time": None,
                }
            # Fetch current status
            status_result = json.loads(self.webinterface.get_shutter_status())
            if status_result.get("success"):
                detail = status_result.get("detail", {})
                for sid_str, info in detail.items():
                    sid = int(sid_str)
                    if sid in self._shutters:
                        self._shutters[sid]["state"] = info.get("state", "stopped")
                        self._shutters[sid]["position"] = info.get("actual_position")
            logger.info("Loaded {0} shutters".format(len(self._shutters)))
            return True
        except Exception:
            logger.exception("Error loading shutter configurations")
            return False

    # ──────────────────────────────────────────────────────────────────────────
    # HA MQTT discovery publishing
    # ──────────────────────────────────────────────────────────────────────────

    def _expected_discovery_topics(self):
        """Return the set of discovery config topics that should currently exist."""
        expected = set()
        # Gateway diagnostic sensors
        expected.add(self._discovery_topic("sensor", "gateway_version"))
        expected.add(self._discovery_topic("sensor", "gateway_sw_version"))
        expected.add(self._discovery_topic("sensor", "gateway_master_version"))
        if self._publish_outputs:
            for oid, output in list(self._outputs.items()):
                type_info = OUTPUT_TYPE_MAP.get(
                    output["type"], {"component": "switch", "device_class": "switch"}
                )
                if type_info is None:
                    continue  # skipped (e.g. SCREEN)
                component = type_info["component"]
                expected.add(self._discovery_topic(component, "output_{0}".format(oid)))
        if self._publish_inputs:
            for iid in list(self._inputs.keys()):
                expected.add(
                    self._discovery_topic("binary_sensor", "input_{0}".format(iid))
                )
        if self._publish_sensors:
            for sid in list(self._sensors.keys()):
                expected.add(self._discovery_topic("sensor", "sensor_{0}".format(sid)))
        if self._publish_shutters:
            for sid in list(self._shutters.keys()):
                expected.add(self._discovery_topic("cover", "shutter_{0}".format(sid)))
        return expected

    def _cleanup_stale_discovery(self):
        """Remove retained discovery topics from the broker that no longer have a matching entity."""
        if not self._retained_discovery_topics:
            return
        expected = self._expected_discovery_topics()
        stale = self._retained_discovery_topics - expected
        if not stale:
            logger.info("Stale discovery cleanup: nothing to remove")
            return
        logger.info("Stale discovery cleanup: removing {0} topic(s)".format(len(stale)))
        for topic in stale:
            logger.info("Removing stale discovery topic: {0}".format(topic))
            self._mqtt_publish(topic, "", retain=True)

    def _gateway_device_block(self):
        """Return the HA device block for the gateway itself."""
        return {
            "identifiers": [self._device_id],
            "name": "OpenMotics Gateway",
            "manufacturer": "OpenMotics",
            "model": "Gateway",
        }

    def _publish_gateway_discovery(self, publish_state=True):
        """Publish diagnostic sensor entities on the gateway device.

        publish_state=True  → also write the retained state values (startup only).
        publish_state=False → only republish the discovery config payloads, leaving
                              the retained state values untouched.  Used for heartbeat
                              and HA-birth republishes so an upgrading plugin instance
                              does not overwrite the new version's state values.
        """
        origin = {"name": "OpenMotics HA plugin", "sw_version": HomeAssistant.version}
        device = self._gateway_device_block()

        diagnostics = [
            {
                "object_id": "gateway_version",
                "state_key": "plugin_version",
                "name": "Plugin version",
                "icon": "mdi:puzzle",
                "value": HomeAssistant.version,
            },
            {
                "object_id": "gateway_sw_version",
                "state_key": "sw_version",
                "name": "Gateway version",
                "icon": "mdi:router-wireless",
                "value": self._gateway_version,
            },
            {
                "object_id": "gateway_master_version",
                "state_key": "master_version",
                "name": "Master version",
                "icon": "mdi:chip",
                "value": self._master_version,
            },
        ]

        for diag in diagnostics:
            topic = self._discovery_topic("sensor", diag["object_id"])
            state_topic = "{prefix}/{device_id}/gateway/{state_key}".format(
                prefix=self._discovery_prefix,
                device_id=self._device_id,
                state_key=diag["state_key"],
            )
            payload = {
                "name": diag["name"],
                "unique_id": "{device_id}_{object_id}".format(
                    device_id=self._device_id, object_id=diag["object_id"]
                ),
                "state_topic": state_topic,
                "icon": diag["icon"],
                "entity_category": "diagnostic",
                "retain": True,
                "device": device,
                "origin": origin,
            }
            published = self._mqtt_publish_discovery(topic, payload)
            if publish_state and diag["value"]:
                self._mqtt_publish(state_topic, diag["value"], retain=True)
            if published:
                logger.info(
                    "Published discovery for gateway diagnostic {0}".format(
                        diag["object_id"]
                    )
                )

    def _mqtt_publish_discovery(self, topic, payload_dict):
        """Publish a discovery config only if it has changed since last publish.

        payload_dict is serialised to a canonical JSON string (sorted keys) and
        compared against the cache.  Returns True if the message was published,
        False if it was skipped as unchanged.
        """
        payload_json = json.dumps(payload_dict, sort_keys=True)
        if self._published_discovery.get(topic) == payload_json:
            return False
        self._mqtt_publish(topic, payload_json, retain=True)
        self._published_discovery[topic] = payload_json
        return True

    def _publish_all_discovery(self):
        """Publish HA discovery config for every known entity."""
        if not self._connected:
            return
        # Never overwrite gateway diagnostic states here — those are written once
        # at startup. Heartbeat and HA-birth republishes only need the config topics.
        self._publish_gateway_discovery(publish_state=False)
        if self._publish_outputs:
            for oid in list(self._outputs.keys()):
                self._publish_output_discovery(oid)
        if self._publish_inputs:
            for iid in list(self._inputs.keys()):
                self._publish_input_discovery(iid)
        if self._publish_sensors:
            for sid in list(self._sensors.keys()):
                self._publish_sensor_discovery(sid)
        if self._publish_shutters:
            for sid in list(self._shutters.keys()):
                self._publish_shutter_discovery(sid)

    def _publish_output_discovery(self, output_id):
        output = self._outputs.get(output_id)
        if output is None:
            return
        name = output["name"] or "Output {0}".format(output_id)
        unique_id = "{device_id}_output_{id}".format(
            device_id=self._device_id, id=output_id
        )
        type_info = OUTPUT_TYPE_MAP.get(
            output["type"], {"component": "switch", "device_class": "switch"}
        )
        if type_info is None:
            logger.debug(
                "Skipping output {0} ({1}): type {2} is handled by another integration".format(
                    output_id, name, output["type"]
                )
            )
            return

        component = type_info["component"]
        is_dimmer = output["module_type"] == "dimmer"
        type_name = OUTPUT_TYPE_NAMES.get(output["type"], "Output")
        if is_dimmer:
            if component == "light":
                model = "Dimmable {0}".format(type_name)
            else:
                model = "Adjustable {0}".format(type_name)
        else:
            model = type_name

        # Base payload common to all components
        payload = {
            "name": None,  # entity IS the device — avoids "Name Name" doubling in HA
            "has_entity_name": True,
            "unique_id": unique_id,
            "state_topic": self._output_state_topic(output_id),
            "command_topic": self._output_command_topic(output_id),
            "payload_on": "ON",
            "payload_off": "OFF",
            "retain": True,
            "device": self._device_info(
                "output", output_id, name, model=model, room=output.get("room")
            ),
            "origin": {
                "name": "OpenMotics HA plugin",
                "sw_version": HomeAssistant.version,
            },
        }

        if component == "light" and is_dimmer:
            # Dimmable light: add brightness topics
            payload["brightness_state_topic"] = self._output_brightness_state_topic(
                output_id
            )
            payload["brightness_command_topic"] = self._output_brightness_command_topic(
                output_id
            )
            payload["brightness_scale"] = 255
        elif component == "fan" and is_dimmer:
            # Dimmable fan: add percentage topics
            payload["percentage_state_topic"] = self._output_percentage_state_topic(
                output_id
            )
            payload["percentage_command_topic"] = self._output_percentage_command_topic(
                output_id
            )
        elif component == "switch":
            if type_info["device_class"]:
                payload["device_class"] = type_info["device_class"]

        topic = self._discovery_topic(component, "output_{0}".format(output_id))
        if self._mqtt_publish_discovery(topic, payload):
            logger.info(
                "Published discovery for output {0} ({1}) as {2} (type={3})".format(
                    output_id, name, component, output["type"]
                )
            )

    def _publish_input_discovery(self, input_id):
        inp = self._inputs.get(input_id)
        if inp is None:
            return
        name = inp["name"] or "Input {0}".format(input_id)
        unique_id = "{device_id}_input_{id}".format(
            device_id=self._device_id, id=input_id
        )
        payload = {
            "name": None,  # entity IS the device — avoids "Name Name" doubling in HA
            "has_entity_name": True,
            "unique_id": unique_id,
            "state_topic": self._input_state_topic(input_id),
            "payload_on": "ON",
            "payload_off": "OFF",
            "retain": True,
            "device": self._device_info("input", input_id, name, room=inp.get("room")),
            "origin": {
                "name": "OpenMotics HA plugin",
                "sw_version": HomeAssistant.version,
            },
        }
        topic = self._discovery_topic("binary_sensor", "input_{0}".format(input_id))
        if self._mqtt_publish_discovery(topic, payload):
            logger.info(
                "Published discovery for input {0} ({1})".format(input_id, name)
            )

    def _publish_sensor_discovery(self, sensor_id):
        sensor = self._sensors.get(sensor_id)
        if sensor is None:
            return
        physical = sensor.get("physical_quantity", "")
        ext_id = sensor.get("external_id", str(sensor_id))
        device_name = sensor.get("device_name") or "Sensor {0}".format(ext_id)

        # Entity name is the physical quantity (e.g. "temperature") so HA shows
        # "MySensor • temperature" rather than one flat device per reading.
        entity_name = physical.capitalize() if physical else None

        # Device identifier groups all quantities of the same physical sensor together.
        sensor_device_id = "{device_id}_sensordevice_{ext_id}".format(
            device_id=self._device_id, ext_id=ext_id
        )

        unit = sensor.get("unit", "")
        ha_info = SENSOR_HA_MAP.get(physical, {})
        device_class = ha_info.get("device_class")
        # Always use HA-canonical unit when the physical quantity is known;
        # the gateway may return non-standard strings like "celcius" or "percent".
        if ha_info.get("unit") is not None:
            unit = ha_info["unit"]
        elif not unit:
            unit = ""

        unique_id = "{device_id}_sensor_{id}".format(
            device_id=self._device_id, id=sensor_id
        )
        payload = {
            "name": entity_name,
            "has_entity_name": True,
            "unique_id": unique_id,
            "state_topic": self._sensor_state_topic(sensor_id),
            "unit_of_measurement": unit,
            "retain": True,
            "device": self._sensor_device_block(
                sensor_device_id, device_name, sensor.get("room")
            ),
            "origin": {
                "name": "OpenMotics HA plugin",
                "sw_version": HomeAssistant.version,
            },
        }
        if device_class:
            payload["device_class"] = device_class
            payload["state_class"] = "measurement"
        topic = self._discovery_topic("sensor", "sensor_{0}".format(sensor_id))
        if self._mqtt_publish_discovery(topic, payload):
            logger.info(
                "Published discovery for sensor {0} ({1}, {2})".format(
                    sensor_id, device_name, physical
                )
            )

    def _publish_shutter_discovery(self, shutter_id):
        shutter = self._shutters.get(shutter_id)
        if shutter is None:
            return
        name = shutter["name"]
        unique_id = "{device_id}_shutter_{id}".format(
            device_id=self._device_id, id=shutter_id
        )
        payload = {
            "name": None,  # entity IS the device — avoids "Name Name" doubling in HA
            "has_entity_name": True,
            "unique_id": unique_id,
            "device_class": "shutter",
            "command_topic": self._shutter_command_topic(shutter_id),
            "state_topic": self._shutter_state_topic(shutter_id),
            "payload_open": "OPEN",
            "payload_close": "CLOSE",
            "payload_stop": "STOP",
            "state_open": "open",
            "state_opening": "opening",
            "state_closed": "closed",
            "state_closing": "closing",
            "retain": True,
            "device": self._device_info(
                "shutter",
                shutter_id,
                name,
                model="Shutter",
                room=shutter.get("room"),
            ),
            "origin": {
                "name": "OpenMotics HA plugin",
                "sw_version": HomeAssistant.version,
            },
        }
        # Always publish position — we track it in software using timer_up/timer_down
        payload["position_topic"] = self._shutter_position_state_topic(shutter_id)
        payload["set_position_topic"] = self._shutter_position_command_topic(shutter_id)
        # HA position: 0=closed, 100=open
        payload["position_open"] = 100
        payload["position_closed"] = 0
        topic = self._discovery_topic("cover", "shutter_{0}".format(shutter_id))
        if self._mqtt_publish_discovery(topic, payload):
            logger.info(
                "Published discovery for shutter {0} ({1}), has_position={2}".format(
                    shutter_id, name, shutter["has_position"]
                )
            )

    # ──────────────────────────────────────────────────────────────────────────
    # State publishing
    # ──────────────────────────────────────────────────────────────────────────

    def _publish_all_states(self, skip_unchanged=False):
        """Publish current state for all entities.

        When skip_unchanged=True (heartbeat calls) only entities whose state
        has changed since the last publish are sent to the broker.
        """
        if not self._connected:
            return
        if self._publish_outputs:
            for oid in list(self._outputs.keys()):
                self._publish_output_state(oid, skip_unchanged=skip_unchanged)
        if self._publish_inputs:
            for iid in list(self._inputs.keys()):
                self._publish_input_state(iid, skip_unchanged=skip_unchanged)
        if self._publish_sensors:
            for sid in list(self._sensors.keys()):
                sensor = self._sensors[sid]
                if sensor.get("value") is not None:
                    self._publish_sensor_state(
                        sid, sensor["value"], skip_unchanged=skip_unchanged
                    )
        if self._publish_shutters:
            for sid in list(self._shutters.keys()):
                self._publish_shutter_state(sid, skip_unchanged=skip_unchanged)

    def _publish_output_state(self, output_id, skip_unchanged=False):
        output = self._outputs.get(output_id)
        if output is None:
            return
        status = output.get("status", 0)
        dimmer = output.get("dimmer", 0)
        type_info = OUTPUT_TYPE_MAP.get(
            output["type"], {"component": "switch", "device_class": "switch"}
        )
        component = type_info["component"] if type_info else None
        is_dimmer = output["module_type"] == "dimmer"
        state = "ON" if status else "OFF"
        self._mqtt_publish(
            self._output_state_topic(output_id),
            state,
            retain=True,
            _skip_if_unchanged=skip_unchanged,
        )
        if is_dimmer and component == "fan":
            # percentage 0-100 (dimmer value maps directly)
            percentage = dimmer if status else 0
            self._mqtt_publish(
                self._output_percentage_state_topic(output_id),
                str(percentage),
                retain=True,
                _skip_if_unchanged=skip_unchanged,
            )
        elif is_dimmer and component == "light":
            # brightness 0-255
            brightness = int(round(dimmer * 255.0 / 100.0)) if status else 0
            self._mqtt_publish(
                self._output_brightness_state_topic(output_id),
                str(brightness),
                retain=True,
                _skip_if_unchanged=skip_unchanged,
            )

    def _publish_input_state(self, input_id, skip_unchanged=False):
        inp = self._inputs.get(input_id)
        if inp is None:
            return
        state = "ON" if inp.get("status", 0) else "OFF"
        self._mqtt_publish(
            self._input_state_topic(input_id),
            state,
            retain=True,
            _skip_if_unchanged=skip_unchanged,
        )

    def _publish_sensor_state(self, sensor_id, value, skip_unchanged=False):
        if value is None:
            return
        self._mqtt_publish(
            self._sensor_state_topic(sensor_id),
            str(round(float(value), 2)),
            retain=True,
            _skip_if_unchanged=skip_unchanged,
        )

    def _publish_shutter_state(self, shutter_id, skip_unchanged=False):
        shutter = self._shutters.get(shutter_id)
        if shutter is None:
            return
        # Map OM state strings to HA cover states.
        #
        # HA greys out the button matching the current state.
        # For end-stop states we publish the true position.
        # For mid-travel stops we always publish "open" so the Close button
        # stays available regardless of travel direction — the most useful
        # default for screens that are partially down.
        # If no direction is known yet we skip publishing (HA shows "unknown").
        om_state = shutter.get("state", "stopped").lower()
        if om_state in ("going_up", "up"):
            shutter["last_direction"] = "up"
        elif om_state in ("going_down", "down"):
            shutter["last_direction"] = "down"

        if om_state == "going_up":
            ha_state = "opening"
        elif om_state == "going_down":
            ha_state = "closing"
        elif om_state == "up":
            ha_state = "open"  # reached top end stop — truly open
        elif om_state == "down":
            ha_state = "closed"  # reached bottom end stop — truly closed
        elif om_state == "stopped" and shutter.get("last_direction") is not None:
            # Mid-travel stop: always publish "open" so the Close button stays
            # available regardless of which direction we were travelling.
            # This lets the user press Close immediately after any stop.
            ha_state = "open"
        else:
            ha_state = None  # no direction known yet — leave HA state alone

        if ha_state is None:
            return
        self._mqtt_publish(
            self._shutter_state_topic(shutter_id),
            ha_state,
            retain=True,
            _skip_if_unchanged=skip_unchanged,
        )
        self._publish_shutter_position(shutter_id, skip_unchanged=skip_unchanged)

    def _publish_shutter_position(self, shutter_id, skip_unchanged=False):
        """Publish the current software-tracked position (0=closed, 100=open)."""
        shutter = self._shutters.get(shutter_id)
        if shutter is None:
            return
        # Prefer hardware position if available
        if shutter["has_position"] and shutter.get("position") is not None:
            steps = shutter.get("steps", 200)
            om_pos = shutter["position"]
            ha_pos = int(round((steps - om_pos) * 100.0 / steps))
            ha_pos = max(0, min(100, ha_pos))
        elif shutter.get("sw_position") is not None:
            ha_pos = shutter["sw_position"]
        else:
            return  # position unknown — don't publish
        self._mqtt_publish(
            self._shutter_position_state_topic(shutter_id),
            str(ha_pos),
            retain=True,
            _skip_if_unchanged=skip_unchanged,
        )

    def _update_sw_position_on_stop(self, shutter_id):
        """Compute and store sw_position based on elapsed travel time since last command."""
        shutter = self._shutters.get(shutter_id)
        if shutter is None:
            return
        start = shutter.get("travel_start_time")
        if start is None:
            return
        elapsed = time.time() - start
        direction = shutter.get("last_direction")
        if direction == "down":
            timer = shutter.get("timer_down", 30)
            start_pos = (
                shutter.get("sw_position")
                if shutter.get("sw_position") is not None
                else 100
            )
            # Position decreases: 100→0 over timer_down seconds
            delta = (elapsed / timer) * 100.0
            new_pos = max(0, start_pos - delta)
        elif direction == "up":
            timer = shutter.get("timer_up", 30)
            start_pos = (
                shutter.get("sw_position")
                if shutter.get("sw_position") is not None
                else 0
            )
            # Position increases: 0→100 over timer_up seconds
            delta = (elapsed / timer) * 100.0
            new_pos = min(100, start_pos + delta)
        else:
            return
        shutter["sw_position"] = int(round(new_pos))
        shutter["travel_start_time"] = None
        logger.info(
            "Shutter {0} stopped after {1:.1f}s — sw_position={2}".format(
                shutter_id, elapsed, shutter["sw_position"]
            )
        )

    def _schedule_shutter_arrival(
        self, shutter_id, final_ha_state, delay, target_sw_position=None
    ):
        """Cancel any existing travel timer for this shutter and start a new one.

        After `delay` seconds, update the shutter state and publish final_ha_state,
        unless the timer was cancelled by a STOP command in the meantime.
        target_sw_position overrides the default 0/100 when targeting a mid-range position.
        """
        self._cancel_shutter_timer(shutter_id)

        cancelled = [False]
        self._shutter_timers[shutter_id] = cancelled

        def _on_arrival():
            # Poll in 0.1 s increments so a STOP can interrupt the sleep quickly
            elapsed = 0.0
            while elapsed < delay:
                time.sleep(0.1)
                elapsed += 0.1
                if cancelled[0]:
                    return
            self._shutter_timers.pop(shutter_id, None)
            shutter = self._shutters.get(shutter_id)
            if shutter is None:
                return
            logger.info(
                "Shutter {0} travel complete — publishing {1}".format(
                    shutter_id, final_ha_state
                )
            )
            shutter["state"] = "up" if final_ha_state == "open" else "down"
            shutter["travel_start_time"] = None
            if target_sw_position is not None:
                shutter["sw_position"] = target_sw_position
            else:
                shutter["sw_position"] = 100 if final_ha_state == "open" else 0
            # For partial travel, stop the motor now
            if target_sw_position is not None and target_sw_position not in (0, 100):
                json.loads(self.webinterface.do_shutter_stop(id=shutter_id))
                shutter["state"] = "stopped"
                last = shutter.get("last_direction")
                stop_state = (
                    "open" if last == "down" else "closed" if last == "up" else None
                )
                if stop_state:
                    self._mqtt_publish(
                        self._shutter_state_topic(shutter_id), stop_state, retain=True
                    )
                self._publish_shutter_position(shutter_id)
            else:
                self._publish_shutter_state(shutter_id)

        self._executor.submit(_on_arrival)

    def _cancel_shutter_timer(self, shutter_id):
        """Cancel any pending travel timer for this shutter (called on STOP)."""
        cancelled = self._shutter_timers.pop(shutter_id, None)
        if cancelled is not None:
            cancelled[0] = True

    def _mqtt_publish(
        self, topic, payload, retain=False, qos=0, _skip_if_unchanged=False
    ):
        """Publish to MQTT.

        If retain=True the value is tracked in _published_states.
        Pass _skip_if_unchanged=True (used by heartbeat state publishes) to
        suppress the publish when the retained value hasn't changed since last
        time we published it.
        """
        if self._client is None or not self._connected:
            return
        if retain:
            value = str(payload) if payload is not None else ""
            if _skip_if_unchanged and self._published_states.get(topic) == value:
                return
            self._published_states[topic] = value
        try:
            self._client.publish(topic, payload=payload, qos=qos, retain=retain)
        except Exception:
            logger.exception("Error publishing to MQTT topic {0}".format(topic))

    # ──────────────────────────────────────────────────────────────────────────
    # OpenMotics event hooks
    # ──────────────────────────────────────────────────────────────────────────

    @output_status
    def output_status(self, status):
        """Called by the gateway whenever any output changes state."""
        if not self._enabled or not self._publish_outputs:
            return
        try:
            new_status = {}
            for entry in status:
                new_status[entry[0]] = entry[1]  # id → dimmer level (0 = off)

            for output_id, output in list(self._outputs.items()):
                old_status = output.get("status", 0)
                old_dimmer = output.get("dimmer", 0)
                changed = False

                if output_id in new_status:
                    new_dimmer = new_status[output_id]
                    if old_status != 1:
                        self._outputs[output_id]["status"] = 1
                        changed = True
                    if old_dimmer != new_dimmer:
                        self._outputs[output_id]["dimmer"] = new_dimmer
                        changed = True
                else:
                    if old_status != 0:
                        self._outputs[output_id]["status"] = 0
                        changed = True

                if changed:
                    self._publish_output_state(output_id)
        except Exception:
            logger.exception("Error handling output status event")

    @input_status(version=2)
    def input_status(self, data):
        """Called by the gateway whenever an input changes state."""
        if not self._enabled or not self._publish_inputs:
            return
        try:
            input_id = data.get("input_id")
            pressed = bool(data.get("status"))
            if input_id in self._inputs:
                self._inputs[input_id]["status"] = 1 if pressed else 0
                self._publish_input_state(input_id)
            else:
                logger.debug("Got input event for unknown input {0}".format(input_id))
        except Exception:
            logger.exception("Error handling input status event")

    @shutter_status(version=2)
    def shutter_status(self, status, detail):
        """Called by the gateway whenever a shutter changes state (version 2: status + detail).

        For externally-triggered moves (wall switch, OM automation) there is no
        active travel timer.  In that case we mirror exactly what the MQTT command
        handler does: publish opening/closing, start a timer, or handle a stop.

        We guard against the BBB Classic quirk (all shutters reported on every
        event) by only acting when the reported state actually differs from what
        we already have cached.
        """
        if not self._enabled or not self._publish_shutters:
            return
        try:
            for sid_str, info in detail.items():
                shutter_id = int(sid_str)
                if shutter_id not in self._shutters:
                    continue
                shutter = self._shutters[shutter_id]
                state = info.get("state", "stopped").lower()
                actual = info.get("actual_position")
                if actual is not None:
                    shutter["position"] = actual

                has_timer = shutter_id in self._shutter_timers
                cached_state = shutter.get("state", "stopped").lower()

                # If a timer is already running we own this shutter's state —
                # don't interfere with it.
                if has_timer:
                    logger.debug(
                        "Shutter {0} state event: {1} (timer active, skipping)".format(
                            shutter_id, state
                        )
                    )
                    continue

                # BBB Classic fires events for all shutters on every event so
                # the reported state is usually the same as cached — skip those.
                if state == cached_state:
                    continue

                logger.info(
                    "Shutter {0} ({1}) external state change: {2} → {3}".format(
                        shutter_id, shutter.get("name"), cached_state, state
                    )
                )

                if state == "going_up":
                    shutter["state"] = "going_up"
                    shutter["last_direction"] = "up"
                    shutter["travel_start_time"] = time.time()
                    self._mqtt_publish(
                        self._shutter_state_topic(shutter_id), "opening", retain=True
                    )
                    self._schedule_shutter_arrival(
                        shutter_id, "open", shutter.get("timer_up", 30)
                    )
                elif state == "going_down":
                    shutter["state"] = "going_down"
                    shutter["last_direction"] = "down"
                    shutter["travel_start_time"] = time.time()
                    self._mqtt_publish(
                        self._shutter_state_topic(shutter_id), "closing", retain=True
                    )
                    self._schedule_shutter_arrival(
                        shutter_id, "closed", shutter.get("timer_down", 30)
                    )
                elif state == "stopped":
                    shutter["state"] = "stopped"
                    self._update_sw_position_on_stop(shutter_id)
                    last = shutter.get("last_direction")
                    stop_state = (
                        "open" if last == "down" else "closed" if last == "up" else None
                    )
                    if stop_state:
                        self._mqtt_publish(
                            self._shutter_state_topic(shutter_id),
                            stop_state,
                            retain=True,
                        )
                    self._publish_shutter_position(shutter_id)
        except Exception:
            logger.exception("Error handling shutter status event")

    @sensor_status
    def sensor_status(self, data):
        """Called by the gateway whenever a sensor value changes."""
        if not self._enabled or not self._publish_sensors:
            return
        try:
            sensor_id = data.get("id")
            value = data.get("value")
            if sensor_id is None or value is None:
                return
            if sensor_id not in self._sensors:
                return
            self._sensors[sensor_id]["value"] = value
            self._publish_sensor_state(sensor_id, value)
        except Exception:
            logger.exception("Error handling sensor status event")

    # ──────────────────────────────────────────────────────────────────────────
    # Background tasks
    # ──────────────────────────────────────────────────────────────────────────

    @background_task
    def background_sensor_poll(self):
        """Poll sensor values from the gateway and publish to MQTT."""
        while True:
            if self._enabled and self._publish_sensors and self._connected:
                try:
                    result = json.loads(self.webinterface.get_sensor_status())
                    if result.get("success"):
                        for entry in result.get("status", []):
                            sensor_id = entry.get("id")
                            value = entry.get("value")
                            if sensor_id is None or value is None:
                                continue
                            sensor = self._sensors.get(sensor_id)
                            if sensor is None:
                                continue
                            self._sensors[sensor_id]["value"] = value
                            self._publish_sensor_state(sensor_id, value)
                    else:
                        logger.error(
                            "Failed to get sensor status: {0}".format(result.get("msg"))
                        )
                except Exception:
                    logger.exception("Error polling sensor status")
            time.sleep(self._sensor_poll_interval)

    @background_task
    def background_heartbeat(self):
        """Periodically republish discovery + state for all entities.

        This ensures Home Assistant always has fresh data after a restart and
        catches any entities that missed a state update.
        """
        # Initial delay so the plugin has time to fully start up.
        time.sleep(30)
        while True:
            if self._enabled and self._connected:
                try:
                    logger.debug("Heartbeat: republishing discovery and changed states")
                    self._publish_all_discovery()
                    self._publish_all_states(skip_unchanged=True)
                except Exception:
                    logger.exception("Error during heartbeat publish")
            time.sleep(self._heartbeat_interval)

    @background_task
    def background_shutter_poll(self):
        """Poll shutter status while travel timers are active to detect external stops.

        On BBB Classic hardware the gateway does not reliably fire a @shutter_status
        'stopped' event when a physical wall button stops a shutter mid-travel.
        This task polls get_shutter_status() every second whenever at least one
        travel timer is active, and cancels the timer + publishes the correct
        state when the gateway reports 'stopped'.
        """
        while True:
            time.sleep(1)
            if not self._enabled or not self._publish_shutters or not self._connected:
                continue
            if not self._shutter_timers:
                continue
            try:
                result = json.loads(self.webinterface.get_shutter_status())
                if not result.get("success"):
                    continue
                detail = result.get("detail", {})
                for sid_str, info in detail.items():
                    shutter_id = int(sid_str)
                    if shutter_id not in self._shutter_timers:
                        continue
                    state = info.get("state", "stopped").lower()
                    if state != "stopped":
                        continue
                    # Gateway reports stopped while our timer is still running —
                    # the shutter was stopped externally (wall button, automation).
                    logger.info(
                        "Shutter {0} ({1}) stopped externally — cancelling timer".format(
                            shutter_id, self._shutters.get(shutter_id, {}).get("name")
                        )
                    )
                    self._cancel_shutter_timer(shutter_id)
                    shutter = self._shutters.get(shutter_id)
                    if shutter is None:
                        continue
                    actual = info.get("actual_position")
                    if actual is not None:
                        shutter["position"] = actual
                    shutter["state"] = "stopped"
                    self._update_sw_position_on_stop(shutter_id)
                    last = shutter.get("last_direction")
                    stop_state = (
                        "open" if last == "down" else "closed" if last == "up" else None
                    )
                    if stop_state:
                        self._mqtt_publish(
                            self._shutter_state_topic(shutter_id),
                            stop_state,
                            retain=True,
                        )
                    self._publish_shutter_position(shutter_id)
            except Exception:
                logger.exception("Error polling shutter status")

    @background_task
    def background_config_poll(self):
        """Periodically reload OpenMotics configuration and republish discovery for any changes."""
        time.sleep(60)  # Let the plugin fully start before the first poll
        while True:
            time.sleep(self._config_poll_interval)
            if not self._enabled or not self._connected:
                continue
            try:
                self._sync_rooms()
                self._sync_outputs()
                self._sync_inputs()
                self._sync_sensors()
                self._sync_shutters()
            except Exception:
                logger.exception("Error during config poll")

    def _sync_rooms(self):
        """Reload room names. If any changed, force a full discovery republish."""
        result = json.loads(self.webinterface.get_room_configurations())
        if not result.get("success"):
            return
        new_rooms = {}
        for cfg in result.get("config", []):
            rid = cfg.get("id")
            name = cfg.get("name", "").strip()
            if rid is None or rid == 255 or not name:
                continue
            new_rooms[rid] = name
        if new_rooms != self._rooms:
            logger.info("Room configuration changed, republishing all discovery")
            self._rooms = new_rooms
            self._publish_all_discovery()

    def _sync_outputs(self):
        if not self._publish_outputs:
            return
        result = json.loads(self.webinterface.get_output_configurations())
        if not result.get("success"):
            return
        current_ids = set(self._outputs.keys())
        seen_ids = set()
        for cfg in result.get("config", []):
            if cfg.get("module_type") not in ["o", "O", "d", "D"]:
                continue
            if not cfg.get("in_use", True):
                continue
            oid = cfg["id"]
            seen_ids.add(oid)
            new = {
                "name": cfg.get("name", ""),
                "module_type": "dimmer"
                if cfg["module_type"] in ["d", "D"]
                else "output",
                "type": int(cfg.get("type", 0)),
                "room": cfg.get("room"),
            }
            existing = self._outputs.get(oid)
            changed = existing is None or any(
                existing.get(k) != v for k, v in new.items()
            )
            if changed:
                if existing is None:
                    self._outputs[oid] = dict(new, status=0, dimmer=0)
                else:
                    self._outputs[oid].update(new)
                logger.info(
                    "Config change detected for output {0}, republishing discovery".format(
                        oid
                    )
                )
                self._publish_output_discovery(oid)
        # Remove outputs that disappeared
        for oid in current_ids - seen_ids:
            logger.info("Output {0} removed, clearing discovery".format(oid))
            type_info = OUTPUT_TYPE_MAP.get(
                self._outputs[oid]["type"],
                {"component": "switch", "device_class": "switch"},
            )
            if type_info:
                self._mqtt_publish(
                    self._discovery_topic(
                        type_info["component"], "output_{0}".format(oid)
                    ),
                    "",
                    retain=True,
                )
            del self._outputs[oid]

    def _sync_inputs(self):
        if not self._publish_inputs:
            return
        result = json.loads(self.webinterface.get_input_configurations())
        if not result.get("success"):
            return
        current_ids = set(self._inputs.keys())
        seen_ids = set()
        for cfg in result.get("config", []):
            if not cfg.get("name", "").strip():
                continue
            iid = cfg["id"]
            seen_ids.add(iid)
            new_name = cfg.get("name", "")
            new_room = cfg.get("room")
            existing = self._inputs.get(iid)
            if (
                existing is None
                or existing.get("name") != new_name
                or existing.get("room") != new_room
            ):
                if existing is None:
                    self._inputs[iid] = {
                        "name": new_name,
                        "room": new_room,
                        "status": 0,
                    }
                else:
                    self._inputs[iid]["name"] = new_name
                    self._inputs[iid]["room"] = new_room
                logger.info(
                    "Config change detected for input {0}, republishing discovery".format(
                        iid
                    )
                )
                self._publish_input_discovery(iid)
        for iid in current_ids - seen_ids:
            logger.info("Input {0} removed, clearing discovery".format(iid))
            self._mqtt_publish(
                self._discovery_topic("binary_sensor", "input_{0}".format(iid)),
                "",
                retain=True,
            )
            del self._inputs[iid]

    def _sync_sensors(self):
        if not self._publish_sensors:
            return
        result = json.loads(self.webinterface.get_sensor_configurations())
        if not result.get("success"):
            return
        current_ids = set(self._sensors.keys())
        seen_ids = set()
        for cfg in result.get("config", []):
            if not cfg.get("in_use", False):
                continue
            sid = cfg["id"]
            seen_ids.add(sid)
            ext_id = str(cfg.get("external_id", sid))
            device_name = cfg.get("name", "").strip() or "Sensor {0}".format(ext_id)
            new = {
                "device_name": device_name,
                "physical_quantity": str(cfg.get("physical_quantity", "")),
                "unit": str(cfg.get("unit", "")),
                "room": cfg.get("room"),
            }
            existing = self._sensors.get(sid)
            changed = existing is None or any(
                existing.get(k) != v for k, v in new.items()
            )
            if changed:
                if existing is None:
                    self._sensors[sid] = dict(
                        new,
                        external_id=ext_id,
                        source=cfg.get("source"),
                        value=None,
                    )
                else:
                    self._sensors[sid].update(new)
                logger.info(
                    "Config change detected for sensor {0}, republishing discovery".format(
                        sid
                    )
                )
                self._publish_sensor_discovery(sid)
        for sid in current_ids - seen_ids:
            logger.info("Sensor {0} removed, clearing discovery".format(sid))
            self._mqtt_publish(
                self._discovery_topic("sensor", "sensor_{0}".format(sid)),
                "",
                retain=True,
            )
            del self._sensors[sid]

    def _sync_shutters(self):
        if not self._publish_shutters:
            return
        result = json.loads(self.webinterface.get_shutter_configurations())
        if not result.get("success"):
            return
        current_ids = set(self._shutters.keys())
        seen_ids = set()
        for cfg in result.get("config", []):
            if not cfg.get("in_use", True) or not cfg.get("name", "").strip():
                continue
            sid = cfg["id"]
            seen_ids.add(sid)
            steps = cfg.get("steps", 65535)
            new = {
                "name": cfg["name"],
                "has_position": steps not in (0, 65535),
                "steps": steps,
                "timer_up": cfg.get("timer_up", 30),
                "timer_down": cfg.get("timer_down", 30),
                "room": cfg.get("room"),
            }
            existing = self._shutters.get(sid)
            changed = existing is None or any(
                existing.get(k) != v for k, v in new.items()
            )
            if changed:
                if existing is None:
                    self._shutters[sid] = dict(
                        new,
                        state="stopped",
                        last_direction=None,
                        position=None,
                        sw_position=None,
                        travel_start_time=None,
                    )
                else:
                    self._shutters[sid].update(new)
                logger.info(
                    "Config change detected for shutter {0}, republishing discovery".format(
                        sid
                    )
                )
                self._publish_shutter_discovery(sid)
        for sid in current_ids - seen_ids:
            logger.info("Shutter {0} removed, clearing discovery".format(sid))
            self._mqtt_publish(
                self._discovery_topic("cover", "shutter_{0}".format(sid)),
                "",
                retain=True,
            )
            del self._shutters[sid]

    @background_task
    def background_reconnect(self):
        """Watch the connection state and attempt to reconnect if lost."""
        time.sleep(60)  # Give initial connection time to establish
        while True:
            if self._enabled and not self._connected:
                logger.info("MQTT disconnected, attempting to reconnect…")
                try:
                    self._connect()
                except Exception:
                    logger.exception("Reconnect attempt failed")
            time.sleep(30)

    # ──────────────────────────────────────────────────────────────────────────
    # Config endpoints (required by the OMPluginBase 'config' interface)
    # ──────────────────────────────────────────────────────────────────────────

    @om_expose
    def get_config_description(self):
        return json.dumps(HomeAssistant.config_description)

    @om_expose
    def get_config(self):
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        try:
            config = json.loads(config)
            self._config_checker.check_config(config)
            self._config = config
            self._read_config()
            self.write_config(config)
            # Reconnect with new broker settings and reload entity configurations
            self._connect()
            self._executor.submit(self._load_all_configuration)
        except Exception:
            logger.exception("Error saving configuration")
            return json.dumps({"success": False})
        return json.dumps({"success": True})
