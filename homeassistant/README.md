# Home Assistant MQTT Discovery Plugin for OpenMotics

This plugin integrates an OpenMotics gateway with [Home Assistant](https://www.home-assistant.io/)
via an external MQTT broker using the
[HA MQTT Discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery) protocol.

Once installed and configured, Home Assistant will **automatically discover** all OpenMotics
entities — no manual YAML configuration required.

## Features

| OpenMotics entity | Home Assistant entity type |
|---|---|
| Output (relay) | `switch` |
| Output (light relay) | `light` |
| Output (dimmer) | `light` with brightness |
| Input | `binary_sensor` |
| Sensor (temperature, humidity, CO₂, …) | `sensor` with correct `device_class` |

- Real-time state updates on every change event from the gateway
- Bidirectional control: HA can switch outputs on/off and set dimmer brightness
- Periodic heartbeat republishes discovery + state so HA stays in sync after restarts
- Automatic reconnect if the broker connection is lost

## Prerequisites

1. A running MQTT broker reachable from both the OpenMotics gateway and Home Assistant.  
   The built-in [Mosquitto add-on](https://github.com/home-assistant/addons/tree/master/mosquitto)
   in Home Assistant works perfectly.
2. Home Assistant with the **MQTT integration** enabled and pointed at the same broker.

## Installation

Package and upload to your gateway the same way as any other plugin:

```bash
./package.sh ha-mqtt-discovery
./publish.sh ha-mqtt-discovery_1.0.0.tgz <gateway-ip> <admin-user>
```

## Configuration

| Field | Default | Description |
|---|---|---|
| `mqtt_hostname` | *(required)* | Hostname or IP of your MQTT broker |
| `mqtt_port` | `1883` | MQTT broker port |
| `mqtt_username` | *(optional)* | MQTT username |
| `mqtt_password` | *(optional)* | MQTT password |
| `discovery_prefix` | `homeassistant` | HA MQTT discovery prefix (must match HA setting) |
| `openmotics_device_id` | `openmotics` | Unique ID used in MQTT topics and as the HA device identifier. Change this if you have multiple gateways. |
| `publish_outputs` | `true` | Publish outputs as HA switches / lights |
| `publish_inputs` | `true` | Publish inputs as HA binary sensors |
| `publish_sensors` | `true` | Publish sensors as HA sensor entities |
| `heartbeat_interval` | `300` | Seconds between full state republish (min 30) |
| `sensor_poll_interval` | `60` | Seconds between sensor value polls (min 10) |

## MQTT Topic Layout

All topics are prefixed with `<discovery_prefix>/<openmotics_device_id>` (default: `homeassistant/openmotics`).

| Purpose | Topic |
|---|---|
| Output state | `homeassistant/openmotics/output/<id>/state` |
| Output command (HA → gateway) | `homeassistant/openmotics/output/<id>/set` |
| Output brightness state (dimmers) | `homeassistant/openmotics/output/<id>/brightness` |
| Input state | `homeassistant/openmotics/input/<id>/state` |
| Sensor value | `homeassistant/openmotics/sensor/<id>/state` |

### Output commands

- Regular switch/light: payload `ON` or `OFF`
- Dimmer: payload `ON`, `OFF`, or an integer brightness value `0-255`

## Sensor device classes

The plugin automatically maps OpenMotics `physical_quantity` values to HA `device_class`:

| physical_quantity | HA device_class |
|---|---|
| `temperature` | `temperature` |
| `humidity` | `humidity` |
| `brightness` | `illuminance` |
| `pressure` | `pressure` |
| `co2` | `carbon_dioxide` |
| `voc` | `volatile_organic_compounds` |
| `dust` | `pm10` |

## License

This plugin is provided under the Apache License 2.0.
