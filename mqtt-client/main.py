# coding=utf-8
"""
An MQTT client plugin for sending/receiving data to/from an MQTT broker.
For more info: https://github.com/openmotics/plugins/blob/master/mqtt-client/README.md
"""

import json
import logging
import os
import pytz
import six
import sys
import re
import time
from enums import HardwareType
from datetime import datetime, timezone
from threading import Thread
from .homeassistant import HomeAssistant

from plugins.base import (
    OMPluginBase,
    PluginConfigChecker,
    background_task,
    om_expose,
    input_status,
    output_status,
    shutter_status,
    receive_events,
    
)

try:
    this_dir = os.path.realpath(os.path.dirname(__file__))
    for lib in os.listdir(os.path.join(this_dir, 'lib')):
        lib_path = os.path.join(this_dir, 'lib', lib)
        sys.path.insert(0, lib_path)

    import paho.mqtt.client as client
except Exception as ex:
    raise ImportError(f"Could not import library: {ex}")

logger = logging.getLogger(__name__)

class MQTTClient(OMPluginBase):
    """
    An MQTT client plugin for sending/receiving data to/from an MQTT broker.
    For more info: https://github.com/openmotics/plugins/blob/master/mqtt-client/README.md
    """

    name = 'MQTTClient'
    version = '3.1.2'
    interfaces = [('config', '1.0')]

    energy_module_config = {
        1: 8,
        8: 8,
        12: 12
    }

    input_status_map = {'ON': 1,
                        'OFF': 0}    

    config_description = [
        {'name': 'hostname',
         'type': 'str',
         'description': 'MQTT broker hostname or IP address.'},
        {'name': 'port',
         'type': 'int',
         'description': 'MQTT broker port. Default: 1883'},
        {'name': 'username',
         'type': 'str',
         'description': 'MQTT broker username. Default: openmotics'},
        {'name': 'password',
         'type': 'password',
         'description': 'MQTT broker password'},
        # home assistant support
        {'name': 'homeassistant_discovery_enabled',
         'type': 'bool',
         'description': 'Enable HomeAssistant Components Discovery.'},
        {'name': 'homeassistant_discovery_prefix_topic',
         'type': 'str',
         'description': 'HomeAssistant topic prefix for MQTT Discovery. Default: homeassistant/'},
        {'name': 'homeassistant_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'Home Assistant message quality of service. Default: 0'},
        {'name': 'homeassistant_retain',
         'type': 'bool',
         'description': 'Home Assistant message retain.'},
        # input status
        {'name': 'input_status_enabled',
         'type': 'bool',
         'description': 'Enable input status publishing of messages.'},
        {'name': 'input_status_topic_format',
         'type': 'str',
         'description': 'Input status topic format. Default: openmotics/state/input/{id}'},
        {'name': 'input_status_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'Input status message quality of service. Default: 0'},
        {'name': 'input_status_retain',
         'type': 'bool',
         'description': 'Input status message retain.'},
        # output status
        {'name': 'output_status_enabled',
         'type': 'bool',
         'description': 'Enable output status publishing of messages.'},
        {'name': 'output_status_topic_format',
         'type': 'str',
         'description': 'Output status topic format. Default: openmotics/state/output/{id}'},
        {'name': 'output_status_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'Output status message quality of service. Default: 0'},
        {'name': 'output_status_retain',
         'type': 'bool',
         'description': 'Output status message retain.'},
        # shutter status
        {'name': 'shutter_status_enabled',
         'type': 'bool',
         'description': 'Enable shutter status publishing of messages.'},
        {'name': 'shutter_state_topic_format',
         'type': 'str',
         'description': 'Shutter state topic format. Default: openmotics/state/shutter/{id}'},
        {'name': 'shutter_position_topic_format',
         'type': 'str',
         'description': 'Shutter position topic format. Default: openmotics/state/shutter/{id}/position'},
        {'name': 'shutter_status_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'Shutter status message quality of service. Default: 0'},
        {'name': 'shutter_status_retain',
         'type': 'bool',
         'description': 'Shutter status message retain.'},
        # event status
        {'name': 'event_status_enabled',
         'type': 'bool',
         'description': 'Enable event status publishing of messages.'},
        {'name': 'event_status_topic_format',
         'type': 'str',
         'description': 'Event status topic format. Default: openmotics/state/event/{id}'},
        {'name': 'event_status_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'Event status message quality of service. Default: 0'},
        {'name': 'event_status_retain',
         'type': 'bool',
         'description': 'Event status message retain.'},
        # temperature status
        {'name': 'temperature_status_enabled',
         'type': 'bool',
         'description': 'Enable temperature status publishing of messages.'},
        {'name': 'temperature_status_topic_format',
         'type': 'str',
         'description': 'temperature status topic format. Default: openmotics/state/temperature/{id}'},
        {'name': 'temperature_status_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'temperature status message quality of service. Default: 0'},
        {'name': 'temperature_status_retain',
         'type': 'bool',
         'description': 'temperature status message retain.'},
        # humidity status
        {'name': 'humidity_status_enabled',
         'type': 'bool',
         'description': 'Enable humidity status publishing of messages.'},
        {'name': 'humidity_status_topic_format',
         'type': 'str',
         'description': 'humidity status topic format. Default: openmotics/state/humidity/{id}'},
        {'name': 'humidity_status_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'humidity status message quality of service. Default: 0'},
        {'name': 'humidity_status_retain',
         'type': 'bool',
         'description': 'humidity status message retain.'},
        # brightness status
        {'name': 'brightness_status_enabled',
         'type': 'bool',
         'description': 'Enable brightness status publishing of messages.'},
        {'name': 'brightness_status_topic_format',
         'type': 'str',
         'description': 'brightness status topic format. Default: openmotics/state/brightness/{id}'},
        {'name': 'brightness_status_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'brightness status message quality of service. Default: 0'},
        {'name': 'brightness_status_retain',
         'type': 'bool',
         'description': 'brightness status message retain.'},
        # power status
        {'name': 'power_status_enabled',
         'type': 'bool',
         'description': 'Enable power status publishing of messages.'},
        {'name': 'power_status_topic_format',
         'type': 'str',
         'description': 'Power status topic format. Default: openmotics/state/power/{id}'},
        {'name': 'power_status_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'Power status quality of Service. Default: 0'},
        {'name': 'power_status_retain',
         'type': 'bool',
         'description': 'Power status retain.'},
        # power details status
        {'name': 'power_details_status_enabled',
         'type': 'bool',
         'description': 'Enable power_details status publishing of messages.'},
        {'name': 'power_details_status_topic_format',
         'type': 'str',
         'description': 'power_details status topic format. Default: openmotics/state/power/{module_id}/{sensor_id}'},
        {'name': 'power_details_status_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'power_details status quality of Service. Default: 0'},
        {'name': 'power_details_status_retain',
         'type': 'bool',
         'description': 'power_details status retain.'},
        {'name': 'power_details_status_poll_frequency',
                'type': 'int',
                'description': 'Polling frequency for power_details status in seconds. Default: 3600 (1 hour), minimum: 10'},
        # energy status
        {'name': 'energy_status_enabled',
         'type': 'bool',
         'description': 'Enable energy status publishing of messages.'},
        {'name': 'energy_status_topic_format',
         'type': 'str',
         'description': 'Energy status topic format. Default: openmotics/state/energy/{module_id}/{sensor_id}'},
        {'name': 'energy_status_qos',
         'type': 'enum',
         'choices': ['0', '1', '2'],
         'description': 'Energy status quality of Service. Default: 0'},
        {'name': 'energy_status_retain',
         'type': 'bool',
         'description': 'Energy status retain.'},
        {'name': 'energy_status_poll_frequency',
        'type': 'int',
        'description': 'Polling frequency for energy status in seconds. Default: 3600 (1 hour), minimum: 10'},
        # output command
        {'name': 'output_command_topic',
         'type': 'str',
         'description': 'Topic to subscribe to for output command messages. Leave empty to turn off.'},
        # shutter command
        {'name': 'shutter_command_topic',
         'type': 'str',
         'description': 'Topic to subscribe to for shutter command messages. Leave empty to turn off.'},
        # shutter position command
        {'name': 'shutter_position_command_topic',
         'type': 'str',
         'description': 'Topic to subscribe to for shutter position command messages. Leave empty to turn off.'},
        # logging
        {'name': 'logging_topic',
         'type': 'str',
         'description': 'Topic for logging messages. Leave empty to turn off.'},
        # timestamp timezone
        {'name': 'timezone',
         'type': 'str',
         'description': 'Timezone. Default: UTC. Example: Europe/Brussels'}
    ]

    default_config = {
        'port': 1883,
        'username': 'openmotics',
        'homeassistant_discovery_prefix_topic': 'homeassistant/',
        'homeassistant_qos': 0,
        'input_status_topic_format': 'openmotics/state/input/{id}',
        'input_status_qos': 0,
        'output_status_topic_format': 'openmotics/state/output/{id}',
        'output_status_qos': 0,
        'shutter_state_topic_format': 'openmotics/state/shutter/{id}',
        'shutter_position_topic_format': 'openmotics/state/shutter/{id}/position',
        'shutter_status_qos': 0,
        'event_status_topic_format': 'openmotics/state/event/{id}',
        'event_status_qos': 0,
        'temperature_status_topic_format': 'openmotics/state/temperature/{id}',
        'temperature_status_qos': 0,
        'humidity_status_topic_format': 'openmotics/state/humidity/{id}',
        'humidity_status_qos': 0,
        'brightness_status_topic_format': 'openmotics/state/brightness/{id}',
        'brightness_status_qos': 0,
        'power_status_topic_format': 'openmotics/state/power/{id}',
        'power_status_qos': 0,
        'power_details_status_topic_format': 'openmotics/state/power/{module_id}/{sensor_id}',
        'power_details_status_qos': 0,
        'power_details_status_poll_frequency': 3600,
        'energy_status_topic_format': 'openmotics/state/energy/{module_id}/{sensor_id}',
        'energy_status_qos': 0,
        'energy_status_poll_frequency': 3600,
        'output_command_topic': 'openmotics/set/output/+',
        'shutter_command_topic': 'openmotics/set/shutter/+',
        'shutter_position_command_topic': 'openmotics/set/shutter/+/position',
        'logging_topic': 'openmotics/logging',
        'timezone': 'UTC'
    }

    def __init__(self, webinterface, connector):
        super(MQTTClient, self).__init__(webinterface=webinterface,
                                            connector=connector)

        logger.info('Starting MQTTClient plugin {0}...'.format(MQTTClient.version))

        self._config = self.read_config(MQTTClient.default_config)
        #logger.info("Default configuration '{0}'".format(self._config))
        self._config_checker = PluginConfigChecker(MQTTClient.config_description)

        if sys.version_info.major == 2:
            paho_mqtt_wheel = '/opt/openmotics/python/plugins/MQTTClient/paho_mqtt-1.5.0-py2-none-any.whl'
        else:
            paho_mqtt_wheel = '/opt/openmotics/python/plugins/MQTTClient/paho_mqtt-1.6.1-py3-none-any.whl'
            
        if paho_mqtt_wheel not in sys.path:
            sys.path.insert(0, paho_mqtt_wheel)

        self.client = None
        self._sensor_config = {}
        self._inputs = {}
        self._outputs = {}
        self._shutters = {}
        self._sensors = {}
        self._power_modules = {}
        self._rooms = {}

        self.connector.sensor.subscribe_status_event(
            self._handle_sensor_status, version=2
        )

        self._read_config()

        self._load_and_connect()

        logger.info('Started MQTTClient plugin {0}'.format(MQTTClient.version))

    def _read_config(self):
        # broker
        self._hostname = self._config.get('hostname')
        self._port     = self._config.get('port')
        self._username = self._config.get('username')
        self._password = self._config.get('password')
        # home assistant support
        self._homeassistant_discovery_enabled      = self._config.get('homeassistant_discovery_enabled')
        self._homeassistant_discovery_prefix_topic = self._config.get('homeassistant_discovery_prefix_topic')
        self._homeassistant_qos                    = int(self._config.get('homeassistant_qos'))
        self._homeassistant_retain                 = self._config.get('homeassistant_retain')
        # inputs
        self._input_enabled = self._config.get('input_status_enabled')
        self._input_topic   = self._config.get('input_status_topic_format')
        self._input_qos     = int(self._config.get('input_status_qos'))
        self._input_retain  = self._config.get('input_status_retain')
        # outputs
        self._output_enabled = self._config.get('output_status_enabled')
        self._output_topic   = self._config.get('output_status_topic_format')
        self._output_qos     = int(self._config.get('output_status_qos'))
        self._output_retain  = self._config.get('output_status_retain')
        # shutters
        self._shutter_enabled          = self._config.get('shutter_status_enabled')
        self._shutter_topic            = self._config.get('shutter_state_topic_format')
        self._shutter_position_topic   = self._config.get('shutter_position_topic_format')
        self._shutter_qos              = int(self._config.get('shutter_status_qos'))
        self._shutter_retain           = self._config.get('shutter_status_retain')
        # events
        self._event_enabled = self._config.get('event_status_enabled')
        self._event_topic   = self._config.get('event_status_topic_format')
        self._event_qos     = int(self._config.get('event_status_qos'))
        self._event_retain  = self._config.get('event_status_retain')
        # temperature sensors
        self._temperature_enabled = self._config.get('temperature_status_enabled')
        self._temperature_topic = self._config.get('temperature_status_topic_format')
        self._temperature_qos = int(self._config.get('temperature_status_qos'))
        self._temperature_retain = self._config.get('temperature_status_retain')
        # humidity sensors
        self._humidity_enabled = self._config.get('humidity_status_enabled')
        self._humidity_topic = self._config.get('humidity_status_topic_format')
        self._humidity_qos = int(self._config.get('humidity_status_qos'))
        self._humidity_retain = self._config.get('humidity_status_retain')
        # brightness sensors
        self._brightness_enabled = self._config.get('brightness_status_enabled')
        self._brightness_topic = self._config.get('brightness_status_topic_format')
        self._brightness_qos = int(self._config.get('brightness_status_qos'))
        self._brightness_retain = self._config.get('brightness_status_retain')
        # power sensors
        self._power_enabled = self._config.get('power_status_enabled')
        self._power_topic = self._config.get('power_status_topic_format')
        self._power_qos = int(self._config.get('power_status_qos'))
        self._power_retain = self._config.get('power_status_retain')
        # power details sensors
        self._power_details_enabled = self._config.get('power_details_status_enabled')
        self._power_details_topic = self._config.get('power_details_status_topic_format')
        self._power_details_qos = int(self._config.get('power_details_status_qos'))
        self._power_details_retain = self._config.get('power_details_status_retain')
        self._power_details_status_poll_frequency = self._config.get('power_details_status_poll_frequency')
        # energy sensors
        self._energy_enabled = self._config.get('energy_status_enabled')
        self._energy_topic = self._config.get('energy_status_topic_format')
        self._energy_qos = int(self._config.get('energy_status_qos'))
        self._energy_retain = self._config.get('energy_status_retain')
        self._energy_status_poll_frequency = self._config.get('energy_status_poll_frequency')
        # sensors config
        self._sensor_config = {
            'temperature': {
                'enabled':        self._config.get('temperature_status_enabled'),
                'topic':          self._config.get('temperature_status_topic_format'),
                'qos':            int(self._config.get('temperature_status_qos')),
                'retain':         self._config.get('temperature_status_retain'),
                'processor':      self._process_sensor_status,
            },
            'humidity': {
                'enabled':        self._config.get('humidity_status_enabled'),
                'topic':          self._config.get('humidity_status_topic_format'),
                'qos':            int(self._config.get('humidity_status_qos')),
                'retain':         self._config.get('humidity_status_retain'),
                'processor':      self._process_sensor_status,
            },
            'brightness': {
                'enabled':        self._config.get('brightness_status_enabled'),
                'topic':          self._config.get('brightness_status_topic_format'),
                'qos':            int(self._config.get('brightness_status_qos')),
                'retain':         self._config.get('brightness_status_retain'),
                'processor':      self._process_sensor_status,
            },
            'power': {
                'enabled':        self._config.get('power_status_enabled'),
                'topic':          self._config.get('power_status_topic_format'),
                'qos':            int(self._config.get('power_status_qos')),
                'retain':         self._config.get('power_status_retain'),
                'processor':      self._process_sensor_status,
            },
            'power_details': {
                'enabled':        self._config.get('power_details_status_enabled'),
                'topic':          self._config.get('power_details_status_topic_format'),
                'qos':            int(self._config.get('power_details_status_qos')),
                'retain':         self._config.get('power_details_status_retain'),
                'poll_frequency': self._config.get('power_details_status_poll_frequency'),
                'processor':      self._process_realtime_power,
            },
            'energy': {
                'enabled':        self._config.get('energy_status_enabled'),
                'topic':          self._config.get('energy_status_topic_format'),
                'qos':            int(self._config.get('energy_status_qos')),
                'retain':         self._config.get('energy_status_retain'),
                'poll_frequency': self._config.get('energy_status_poll_frequency'),
                'processor':      self._process_realtime_power,
            }
        }
        # output command
        self._output_command_topic = self._config.get('output_command_topic')
        # shutter command
        self._shutter_command_topic = self._config.get('shutter_command_topic')
        # shutter position command
        self._shutter_position_command_topic = self._config.get('shutter_position_command_topic')
        # logging topic
        self._logging_topic = self._config.get('logging_topic')
        # timezone
        self._timezone = self._config.get('timezone')
        self._enabled = self._hostname is not None and self._port is not None
        logger.info('MQTTClient is {0}'.format('enabled' if self._enabled else 'disabled'))

    def _load_configuration(self):
        if self._enabled is True:
            inputs_loaded = False
            outputs_loaded = False
            shutters_loaded = False
            sensors_loaded = False
            power_loaded = False
            rooms_loaded = False
            should_load = True

            while should_load:
                if not inputs_loaded:
                    inputs_loaded = self._load_input_configuration()
                if not outputs_loaded:
                    outputs_loaded = self._load_output_configuration()
                if not shutters_loaded:
                    shutters_loaded = self._load_shutter_configuration()
                if not sensors_loaded:
                    sensors_loaded = self._load_sensor_configuration()
                if not power_loaded:
                    power_loaded = self._load_power_configuration()
                if not rooms_loaded:
                    rooms_loaded = self._load_rooms_configuration()
                should_load = not all([inputs_loaded, outputs_loaded, shutters_loaded, sensors_loaded, power_loaded, rooms_loaded])
                if should_load:
                    time.sleep(15)

    def _load_and_connect(self):
        try:
            self._load_configuration()
            self._try_connect()
        except Exception as ex:
            logger.error("Error while loading config and trying to connect: {0}".format(ex))

    def _load_input_configuration(self):
        input_config_loaded = True
        if self._input_enabled:
            try:
                result = json.loads(self.webinterface.get_input_configurations())
                if result['success'] is False:
                    logger.error('Failed to load input configurations')
                    input_config_loaded = False
                else:
                    ids = []
                    for config in result['config']:
                        input_id = config['id']
                        if not config['in_use']:
                            continue
                        ids.append(input_id)
                        self._inputs[input_id] = config
                    for input_id in self._inputs.keys():
                        if input_id not in ids:
                            del self._inputs[input_id]
                    logger.info('Configuring {0} inputs'.format(len(ids)))
            except Exception as ex:
                logger.exception('Error while loading input configurations')
                input_config_loaded = False
            try:
                result = json.loads(self.webinterface.get_input_status())
                if result['success'] is False:
                    logger.error('Failed to get input status')
                    input_config_loaded = False
                else:
                    for input_data in result['status']:
                        input_id = input_data['id']
                        if input_id not in self._inputs:
                            continue
                        self._inputs[input_id]['status'] = input_data['status']
            except Exception as ex:
                logger.exception('Error getting input status')
                input_config_loaded = False
        return input_config_loaded

    def _load_output_configuration(self):
        output_config_loaded = True
        if self._output_enabled:
            try:
                result = json.loads(self.webinterface.get_output_configurations())
                if result['success'] is False:
                    output_config_loaded = False
                    logger.error('Failed to load output configurations')
                else:
                    ids = []
                    for config in result['config']:
                        if config['module_type'] not in ['o', 'O', 'd', 'D']:
                            continue
                        if not config['module']['hardware_type'] in [HardwareType.PHYSICAL, HardwareType.VIRTUAL]:
                            continue
                        if not config['in_use']:
                            continue
                        output_id = config['id']
                        ids.append(output_id)
                        self._outputs[output_id] = {'name': config['name'],
                                                    'hardware_type': config['module']['hardware_type'],
                                                    'module_type': {'o': 'output',
                                                                    'O': 'output',
                                                                    'd': 'dimmer',
                                                                    'D': 'dimmer'}[config['module_type']],
                                                    'room_id': config['room'],
                                                    'type': config['type']}
                    for output_id in self._outputs.keys():
                        if output_id not in ids:
                            del self._outputs[output_id]
                    logger.info('Configuring {0} outputs'.format(len(ids)))
            except Exception as ex:
                output_config_loaded = False
                logger.exception('Error while loading output configurations')
            try:
                result = json.loads(self.webinterface.get_output_status())
                if result['success'] is False:
                    output_config_loaded = False
                    logger.error('Failed to get output status')
                else:
                    for output in result['status']:
                        output_id = output['id']
                        if output_id not in self._outputs:
                            continue
                        self._outputs[output_id]['status'] = output['status']
                        self._outputs[output_id]['dimmer'] = output['dimmer']
            except Exception as ex:
                output_config_loaded = False
                logger.exception('Error getting output status')
        return output_config_loaded

    def _load_shutter_configuration(self):
        shutter_config_loaded = True
        if self._shutter_enabled:
            try:
                result = json.loads(self.webinterface.get_shutter_configurations())
                if result['success'] is False:
                    shutter_config_loaded = False
                    logger.error('Failed to load shutter configurations')
                else:
                    ids = []
                    for config in result['config']:
                        if not config['in_use']:
                            continue
                        if not config['module']['hardware_type'] == HardwareType.PHYSICAL:
                            continue
                        shutter_id = config['id']
                        ids.append(shutter_id)
                        self._shutters[shutter_id] = {'name': config['name'],
                                                     'module_id': config['module']['module_id'],
                                                     'type': config['module']['hardware_module'],
                                                     'timer_up': config['timer_up'],
                                                     'timer_down': config['timer_down'],
                                                     'steps': config['steps'],
                                                     'normal_direction': config['up_down_config'],
                                                     'room_id': config['room']
                                                    }
                    for shutter_id in self._shutters.keys():
                        if shutter_id not in ids:
                            del self._shutters[shutter_id]
                    logger.info('Configuring {0} shutters'.format(len(ids)))
            except Exception as ex:
                shutter_config_loaded = False
                logger.exception('Error while loading shutter configurations: {0}'.format(ex))
            try:
                result = json.loads(self.webinterface.get_shutter_status())
                if result['success'] is False:
                    shutter_config_loaded = False
                    logger.error('Failed to get shutter status')
                else:
                    for id in sorted(result['detail']):
                        shutter_id = int(id)
                        if shutter_id not in self._shutters:
                            continue
                        state = result['detail'][id]['state']
                        position = result['detail'][id]['actual_position']

                        logger.info('Shutter {0} state {1}'.format(shutter_id, state))

                        self._shutters[shutter_id]['state'] = state
                        if position is not None:
                            self._shutters[shutter_id]['position'] = position
            except Exception as ex:
                shutter_config_loaded = False
                logger.exception('Error getting shutter status: {0}'.format(ex))
        return shutter_config_loaded

    def _load_sensor_configuration(self):
        sensor_config_loaded = True
        if self._temperature_enabled or self._humidity_enabled or self._brightness_enabled or self._power_enabled:
            try:
                result = json.loads(self.webinterface.get_sensor_configurations())
                if result['success'] is False:
                    sensor_config_loaded = False
                    logger.error('Failed to load sensor configurations: {0}'.format(result.get('msg')))
                else:
                    ids = []
                    for config in result['config']:
                        sensor_id = config['id']

                        if not config['in_use']:
                            continue

                        ids.append(sensor_id)
                        self._sensors[sensor_id] = {'name': config['name'],
                                                    'external_id': str(config['external_id']),
                                                    'room_id': config['room'],
                                                    'physical_quantity': str(config['physical_quantity']),
                                                    'offset': config['offset'],
                                                    'source': config.get('source'),
                                                    'unit': config.get('unit')}
                    for sensor_id in self._sensors.keys():
                        if sensor_id not in ids:
                            del self._sensors[sensor_id]
                    logger.info('Configuring {0} sensors'.format(len(ids)))
            except Exception as ex:
                sensor_config_loaded = False
                logger.exception('Error while loading sensor configurations: {0}'.format(ex))
        return sensor_config_loaded

    def _load_power_configuration(self):
        power_config_loaded = True
        if self._power_enabled:
            try:
                result = json.loads(self.webinterface.get_power_modules())
                if result['success'] is False:
                    power_config_loaded = False
                    logger.error('Failed to load power configurations: {0}'.format(result.get('msg')))
                else:
                    ids = []
                    for module in result['modules']:
                        module_id = int(module['id'])
                        ids.append(module_id)
                        version = int(module['version'])
                        input_count = MQTTClient.energy_module_config.get(version, 0)
                        module_config = {}
                        if input_count == 0:
                            logger.warning('Warning: Skipping energy module {0}, version {1} is currently not supported by this plugin. Only versions: {2}'.format(
                                module_id,
                                version,
                                ', '.join(MQTTClient.energy_module_config.keys())))
                            continue
                        else:
    	                    logger.info('Configuring energy module {0} (version {1}) with {2} inputs'.format(module_id, version, input_count))
                        for input_id in range(0, input_count):
                            module_config[input_id] = {'name':     module['input{0}'.format(input_id)],
                                                        'sensor':   module['sensor{0}'.format(input_id)],
                                                        'times':    module['times{0}'.format(input_id)],
                                                        'inverted': module['inverted{0}'.format(input_id)]}
                        self._power_modules[module_id] = module_config
                    for module_id in self._power_modules.keys():
                        if module_id not in ids:
                            del self._power_modules[module_id]
            except Exception as ex:
                power_config_loaded = False
                logger.exception('Error while loading power configurations')
        return power_config_loaded

    def _load_rooms_configuration(self):
        room_config_loaded = True
        try:
            result = json.loads(self.webinterface.get_room_configurations())
            if result['success'] is False:
                logger.error('Failed to load room configurations')
                room_config_loaded = False
            else:
                ids = []
                for config in result['config']:
                    room_id = config['id']
                    if not config['name'].strip():
                        continue
                    ids.append(room_id)
                    self._rooms[room_id] = config
                logger.info('Configuring {0} rooms'.format(len(ids)))
        except Exception as ex:
            logger.exception('Error while loading rooms configurations')
            room_config_loaded = False
        return room_config_loaded

    def _try_connect(self):
        if self._enabled is True:
            try:
                import paho.mqtt.client as client
                self.client = client.Client()
                if self._username is not None:
                    logger.info("MQTTClient is using username '{0}' and password".format(self._username))
                    self.client.username_pw_set(self._username, self._password)
                self.client.on_message = self.on_message
                self.client.on_connect = self.on_connect
                self.client.connect(self._hostname, self._port, 5)
                self.client.loop_start()
            except Exception as ex:
                logger.exception('Error connecting to MQTT broker')

    def _log(self, info):
        # for log messages QoS = 0 and retain = False
        thread = Thread(target=self._send, args=(self._logging_topic, info, 0, False))
        thread.start()

    def _send(self, topic, data, qos, retain):
        try:
            self.client.publish(topic, payload=json.dumps(data), qos=qos, retain=retain)
        except Exception as ex:
            logger.exception('Error sending data to topic {0}'.format(topic))

    def _timestamp2isoformat(self, timestamp=None):
        # start with UTC
        dt = datetime.now(timezone.utc)
        if (timestamp is not None):
            dt.fromtimestamp(timezone.utc)(float(timestamp))
        # convert to timezone from configuration
        if self._timezone is not None and self._timezone is not 'UTC':
            dt = dt.astimezone(pytz.timezone(self._timezone))
        return dt.isoformat()

    @input_status(version=2)
    def input_status(self, data):
        if self._enabled and self._input_enabled:
            input_id = data.get('input_id')
            status = 'ON' if data.get('status') else 'OFF'
            try:
                if input_id in self._inputs:
                    name = self._inputs[input_id].get('name')
                    if self._inputs[input_id].get('status') != self.input_status_map[status]:
                        self._log('Input {0} ({1}) switched from {2} to {3}'.format(input_id, name,  self._inputs[input_id].get('status'), status))
                        logger.info('Input {0} ({1}) switched from {2} to {3}'.format(input_id, name,  self._inputs[input_id].get('status'), status))
                        data = {'id': input_id,
                                'name': name,
                                'status': status,
                                'timestamp': self._timestamp2isoformat()}
                        thread = Thread(
                            target=self._send,
                            args=(self._input_topic.format(id=input_id), data, self._input_qos, self._input_retain)
                        )
                        thread.start()
                else:
                    logger.error('Got event for unknown input {0}'.format(input_id))
            except Exception as ex:
                logger.exception('Error processing input {0}'.format(input_id))

    @output_status(version = 2)
    def output_status(self, event_data):
        if self._enabled and self._output_enabled:
            try:
                output_id = event_data['id']
                current_output_status = self._outputs
                if output_id in current_output_status:
                    status = current_output_status[output_id].get('status')
                    dimmer = current_output_status[output_id].get('dimmer')
                    if status is None or dimmer is None:
                        return
             
                    # set vars for control flow 
                    event_status = 1 if event_data['status']['on'] is True else 0
                    event_dimmer = event_data['status'].get('value')
    
                    self._process_output_event(output_id, event_status, event_dimmer)
            except Exception as ex:
                logger.exception('Error processing outputs: {0}'.format(ex))

    def _process_output_event(self, output_id, event_status, event_dimmer, force_change=False):
        current_output_status = self._outputs
        name = current_output_status[output_id].get('name')
        status = current_output_status[output_id].get('status')
        dimmer = current_output_status[output_id].get('dimmer')

        change = False
        if event_status != status:
            change = True
            current_output_status[output_id]['status'] = event_status
        if event_dimmer is not None and dimmer != event_dimmer:
            change = True
            current_output_status[output_id]['dimmer'] = event_dimmer

        if change is True or force_change is True:
            level = event_status * 100

            if current_output_status[output_id]['module_type'] != 'output':
                # if there's no change to dimmer, keep old value
                level = event_dimmer or dimmer

            if not force_change:
                self._log('Output {0} ({1}) changed from {2} to {3}'.format(output_id, name, status, event_status))
                logger.info('Output {0} ({1}) changed from {2} to {3}'.format(output_id, name, status, event_status))
            data = {'id': output_id,
                    'name': name,
                    'value': level,
                    'timestamp': self._timestamp2isoformat()}
            thread = Thread(
                target=self._send,
                args=(self._output_topic.format(id=output_id), data, self._output_qos, self._output_retain)
            )
            thread.start()

    @shutter_status(version = 2)
    def shutter_status(self, status, detail):
        if self._enabled and self._shutter_enabled:
            try:
                new_shutter_status = {}
                for id, state in enumerate(status):
                    new_shutter_status[id] = {}
                    new_shutter_status[id]['state'] = state
                    new_shutter_status[id]['position'] = detail[id]['actual_position']

                current_shutter_status = self._shutters
                for shutter_id in current_shutter_status:
                    if shutter_id in new_shutter_status:
                        new_state = new_shutter_status[shutter_id]['state']
                        new_position = new_shutter_status[shutter_id]['position']
                        if new_state is None and new_position is None:
                            continue
                        self._process_shutter_event(shutter_id, new_shutter_status[shutter_id])

            except Exception as ex:
                logger.exception('Error processing shutters: {0}'.format(ex))

    def _process_shutter_event(self, shutter_id, new_shutter_status, force_change=False):
        current_shutter_status = self._shutters
        name = current_shutter_status[shutter_id].get('name')
        state = current_shutter_status[shutter_id].get('state')
        position = current_shutter_status[shutter_id].get('position', None)

        if (state != new_shutter_status['state']) or (force_change is True):
            if new_shutter_status['state'] is not None:
                current_shutter_status[shutter_id]['state'] = new_shutter_status['state']
                thread = Thread(
                    target=self._send,
                    args=(self._shutter_topic.format(id=shutter_id), new_shutter_status['state'], self._shutter_qos, self._shutter_retain)
                )
                thread.start()
                if not force_change:
                    self._log('Shutter {0} ({1}) changed from {2} to {3}'.format(shutter_id, name, state, new_shutter_status['state']))
                    logger.info('Shutter {0} ({1}) changed from {2} to {3}'.format(shutter_id, name, state, new_shutter_status['state']))

        if (position != new_shutter_status.get('position')) or (force_change is True):
            if new_shutter_status.get('position') is not None:
                current_shutter_status[shutter_id]['position'] = new_shutter_status.get('position')

                thread = Thread(
                    target=self._send,
                    args=(self._shutter_position_topic.format(id=shutter_id), new_shutter_status.get('position'), self._shutter_qos, self._shutter_retain)
                )
                thread.start()
                if not force_change:
                    self._log('Shutter {0} ({1}) changed from {2} to {3}'.format(shutter_id, name, position, new_shutter_status.get('position')))
                    logger.info('Shutter {0} ({1}) changed from {2} to {3}'.format(shutter_id, name, position, new_shutter_status.get('position')))

    @receive_events
    def receive_events(self, event_id):
        if self._enabled and self._event_enabled:
            try:
                self._log('Got event {0}'.format(event_id))
                logger.info('Got event {0}'.format(event_id))
                data = {'id': event_id,
                        'timestamp': self._timestamp2isoformat()}
                thread = Thread(
                    target=self._send,
                    args=(self._event_topic.format(id=event_id), data, self._event_qos, self._event_retain)
                )
                thread.start()
            except Exception as ex:
                logger.exception('Error processing event')

    @background_task
    def background_task_realtime_power(self):
        self._create_background_task(
            'power_details',
            self.webinterface.get_realtime_power,
            self._process_realtime_power
        )()

    @background_task
    def background_task_total_energy(self):
        self._create_background_task(
            'energy',
            self.webinterface.get_total_energy,
            self._process_total_energy
        )()

    def _handle_sensor_status(self, event):
        try:
            logger.debug(
                "Received sensor status from gateway: {0} {1}".format(
                    event.data["id"], event.data["value"]
                )
            )
            sensor_id = event.data["id"]
            sensor = self._sensors.get(sensor_id)
            if sensor:
                sensor_type = sensor.get('physical_quantity')
                sensor_config = self._sensor_config.get(sensor_type)
                sensor_topic = sensor_config.get('topic')
                sensor_process = sensor_config.get('processor')

                if sensor_config.get('enabled'):
                    mqtt_message = sensor_process(sensor_topic, event)
                    thread = Thread(target=self._send,
                                    args=(mqtt_message.get('topic'),
                                            mqtt_message.get('message'),
                                            sensor_config.get('qos'),
                                            sensor_config.get('retain')))
                    thread.start()
        except Exception as ex:
            logger.exception('Error processing {0} status {1}'.format(sensor_type, ex))        

    def _parse_sensor_value(self, value):
        if value is None:
            return value
        if type(value) == float:
            return value
        return float(value)

    def _process_sensor_status(self, topic, event):
        mqtt_message = {}
        sensor_id = event.data["id"]
        sensor_value = event.data["value"]
        sensor = self._sensors.get(sensor_id)
        if sensor:
            sensor_data = {'id': sensor_id,
                            'source': sensor.get('source'),
                            'external_id': sensor.get('external_id'),
                            'physical_quantity': sensor.get('physical_quantity'),
                            'offset': sensor.get('offset'),
                            'unit': sensor.get('unit'),
                            'name': sensor.get('name'),
                            'value': self._parse_sensor_value(sensor_value),
                            'timestamp': self._timestamp2isoformat()}
            mqtt_message = {'topic': topic.format(id=sensor_id),
                                    'message': sensor_data}
        return mqtt_message

    def _process_power_sensor_status(self, topic, event):
        mqtt_message = {}
        sensor_id = event.data["id"]
        sensor_value = event.data["value"]
        sensor = self._sensors.get(sensor_id)
        if sensor:
            sensor_data = {'id': sensor_id,
                            'source': sensor.get('source'),
                            'external_id': sensor.get('external_id'),
                            'physical_quantity': sensor.get('physical_quantity'),
                            'offset': sensor.get('offset'),
                            'unit': sensor.get('unit'),
                            'name': sensor.get('name'),
                            'value': self._parse_sensor_value(sensor_value),
                            'timestamp': self._timestamp2isoformat()}
            mqtt_message = {'topic': topic.format(id=sensor_id),
                                    'message': sensor_data}
        return mqtt_message

    def _process_realtime_power(self, sensor_config, json_data):
        mqtt_messages = []
        json_data.pop('success')
        for module_id, values in json_data.items():
            module = self._power_modules.get(int(module_id))
            if module:
                for input_id, sensor_values in enumerate(values):
                    power_input = module.get(int(input_id))
                    if power_input:
                        sensor_data = {'sensor_id': input_id,
                                       'module_id': module_id,
                                       'name': power_input.get('name'),
                                       'voltage': sensor_values[0],
                                       'frequency': sensor_values[1],
                                       'current': sensor_values[2],
                                       'power': sensor_values[3],
                                       'timestamp': self._timestamp2isoformat()}
                        mqtt_messages.append({'topic': sensor_config.get('topic').format(module_id=module_id, sensor_id=input_id),
                                              'message': sensor_data })
        return mqtt_messages

    def _process_total_energy(self, sensor_config, json_data):
        mqtt_messages = []
        json_data.pop('success')
        for module_id, values in json_data.items():
            module = self._power_modules.get(int(module_id))
            if module:
                for input_id, sensor_values in enumerate(values):
                    power_input = module.get(int(input_id))
                    if power_input:
                        sensor_data = {'sensor_id': input_id,
                                       'module_id': module_id,
                                       'name': power_input.get('name'),
                                       'day': sensor_values[0],
                                       'night': sensor_values[1],
                                       'timestamp': self._timestamp2isoformat()}
                    mqtt_messages.append({'topic': sensor_config.get('topic').format(module_id=module_id, sensor_id=input_id),
                                          'message': sensor_data})
        return mqtt_messages

    def _create_background_task(self, sensor_type, data_retriever, data_processor):
        def background_function():
            while True:
                if self._enabled:
                    sensor_config = self._sensor_config.get(sensor_type)
                    frequency = sensor_config.get('poll_frequency')
                    logger.info('Background task to retrieve {0} sensor data started, will run every {1} seconds.'.format(sensor_type, frequency))
                    # highest frequency is every 10s
                    while frequency >= 10:
                        start = time.time()
                        try:
                            if sensor_config.get('enabled'):
                                result = json.loads(data_retriever())
                                if result['success'] is False:
                                    logger.error('Failed to load {0} sensor data: {1}'.format(sensor_type, result.get('msg')))
                                else:
                                    mqtt_messages = data_processor(sensor_config, result)
                                    for mqtt_message in mqtt_messages:
                                        thread = Thread(target=self._send,
                                                        args=(mqtt_message.get('topic'),
                                                              mqtt_message.get('message'),
                                                              sensor_config.get('qos'),
                                                              sensor_config.get('retain')))
                                        thread.start()
                        except Exception as ex:
                            logger.exception('Error processing {0} sensor status {1}'.format(sensor_type, ex))
                        # This loop will run approx. every 'frequency' seconds
                        sleep = frequency - (time.time() - start)
                        if sleep < 0:
                            sleep = 1
                        time.sleep(sleep)
                else:
                    time.sleep(15)
        return background_function

    def on_connect(self, client, userdata, flags, rc):
        try:
            if rc != 0:
                logger.error('Error connecting: rc={0}', rc)
                return

            logger.info('Connected to MQTT broker {0}:{1}'.format(self._hostname, self._port))

            # load initial output status
            current_output_status = self._outputs
            for output_id in current_output_status:
                event_status = current_output_status[output_id].get('status')
                event_dimmer = current_output_status[output_id].get('dimmer')
                if event_status is None or event_dimmer is None:
                    continue

                self._process_output_event(output_id, event_status, event_dimmer, force_change=True)

            # load initial shutters status
            current_shutter_status = self._shutters
            for shutter_id in current_shutter_status:
                new_state = current_shutter_status[shutter_id].get('state')
                new_position = current_shutter_status[shutter_id].get('position', None)
                if new_state is None and new_position is None:
                    continue

                self._process_shutter_event(shutter_id, current_shutter_status[shutter_id], force_change=True)

            # load home assistant discovery
            homeassistant = HomeAssistant(
                client,
                self._config,
                self._outputs,
                self._shutters,
                self._power_modules,
                self._sensors,
                self._rooms)
            homeassistant.start_discovery()

            # subscribe to output command topic if provided
            if self._output_command_topic:
                try:
                    self.client.subscribe(self._output_command_topic)
                    logger.info('Subscribed to {0}'.format(self._output_command_topic))
                except Exception as ex:
                    logger.exception('Could not subscribe to {0}: {1}'.format(self._output_command_topic, ex))

            # subscribe to shutter command topic if provided
            if self._shutter_command_topic:
                try:
                    self.client.subscribe(self._shutter_command_topic)
                    logger.info('Subscribed to {0}'.format(self._shutter_command_topic))
                except Exception as ex:
                    logger.exception('Could not subscribe to {0}: {1}'.format(self._shutter_command_topic, ex))
        except Exception as ex:
            logger.exception('Error when handling connection: {0}'.format(ex))

        # subscribe to shutter position command topic if provided
        if self._shutter_position_command_topic:
            try:
                self.client.subscribe(self._shutter_position_command_topic)
                logger.info('Subscribed to {0}'.format(self._shutter_position_command_topic))
            except Exception as ex:
                logger.exception('Could not subscribe to {0}: {1}'.format(self._shutter_position_command_topic, ex))

    def on_message(self, client, userdata, msg):
        output_regexp = "^{0}$".format(self._output_command_topic.replace('+', '(\d+)'))
        shutter_regexp = "^{0}$".format(self._shutter_command_topic.replace('+', '(\d+)'))
        shutter_position_regexp = "^{0}$".format(self._shutter_position_command_topic.replace('+', '(\d+)'))

        if re.search(output_regexp, msg.topic) is not None:
            # the output_id is the first match of the regular expression
            output_id = int(re.findall(output_regexp, msg.topic)[0])
            self._output_command(output_id, msg)
        elif re.search(shutter_regexp, msg.topic) is not None:
            # the shutter_id is the first match of the regular expression
            shutter_id = int(re.findall(shutter_regexp, msg.topic)[0])
            self._shutter_command(shutter_id, msg)
        elif re.search(shutter_position_regexp, msg.topic) is not None:
            # the shutter_id is the first match of the regular expression
            shutter_id = int(re.findall(shutter_position_regexp, msg.topic)[0])
            self._shutter_position_command(shutter_id, msg)
        else:
            self._log('Message with topic {0} ignored'.format(msg.topic))
            logger.info('Message with topic {0} ignored'.format(msg.topic))

    def _output_command(self, output_id, msg):
        try:
            if output_id in self._outputs:
                output = self._outputs[output_id]
                value = int(msg.payload)
                if value > 0:
                    is_on = 'true'
                else:
                    is_on = 'false'
                dimmer = None
                if output['module_type'] == 'dimmer':
                    dimmer = None if value == 0 else max(0, min(100, value))
                    if value > 0:
                        log_value = '{0} ON ({1}%)'.format(output_id, value)
                result = json.loads(self.webinterface.set_output(id=output_id, is_on=is_on, dimmer=dimmer))
                if result['success'] is False:
                    log_message = 'Failed to set output {0} to {1}: {2}'.format(output_id, value, result.get('msg', 'Unknown error'))
                    self._log(log_message)
                    logger.error(log_message)
                else:
                    log_message = 'Message for output {0} with payload {1}'.format(output_id, value)
                    self._log(log_message)
                    logger.info(log_message)
            else:
                self._log('Unknown output: {0}'.format(output_id))
        except Exception as ex:
            self._log('Failed to process message')

    def _shutter_command(self, shutter_id, msg):
        try:
            if shutter_id in self._shutters:
                shutter = self._shutters[shutter_id]
                value = str(msg.payload, "utf-8")
                if value.lower() in ['up', 'down', 'stop']:
                    thread = Thread(
                        target=self._execute_shutter_command,
                        args=(
                            shutter_id,
                            value.lower()
                        )
                    )
                    thread.start()
                else:
                    log_message = 'Failed to set shutter {0} to {1}'.format(shutter_id, value)
                    self._log(log_message)
                    logger.error(log_message)
            else:
                self._log('Unknown shutter: {0}'.format(shutter_id))
        except Exception as ex:
            self._log('Failed to process message: {0}'.format(ex))

    def _execute_shutter_command(self, shutter_id, command):
        try:
            if command == 'up':
                result = json.loads(self.webinterface.do_shutter_up(id=shutter_id))
            elif command == 'down':
                result = json.loads(self.webinterface.do_shutter_down(id=shutter_id))
            else:
                result = json.loads(self.webinterface.do_shutter_stop(id=shutter_id))

            if result['success'] is False:
                logger.error('Failed to set shutter {0} to up: {1}'.format(shutter_id, result.get('msg', 'Unknown error')))
        except Exception as ex:
            logger.error('Error calling shutter up web service: {0}'.format(ex))

    def _shutter_position_command(self, shutter_id, msg):
        try:
            log_message = 'Execute shutter position command on shutter {0}'.format(shutter_id)
            self._log(log_message)
            logger.info(log_message)

            if shutter_id in self._shutters:
                shutter = self._shutters[shutter_id]
                value = int(msg.payload)
                if value >= 0 and value <=99:
                    thread = Thread(
                        target=self._execute_shutter_position_command,
                        args=(
                            shutter_id,
                            value
                        )
                    )
                    thread.start()
                else:
                    log_message = 'Failed to set shutter {0} to {1}'.format(shutter_id, value)
                    self._log(log_message)
                    logger.error(log_message)
            else:
                self._log('Unknown shutter: {0}'.format(shutter_id))
        except Exception as ex:
            self._log('Failed to process message: {0}'.format(ex))

    def _execute_shutter_position_command(self, shutter_id, position):
        try:
            result = json.loads(self.webinterface.do_shutter_goto(id=shutter_id, position=position))

            if result['success'] is False:
                log_message = 'Failed to set shutter {0} to position {1}: {2}'.format(shutter_id, position, result.get('msg', 'Unknown error'))
                self._log(log_message)
                logger.error(log_message)
            else:
                log_message = 'Message for shutter {0} with payload {1}'.format(shutter_id, position)
                self._log(log_message)
                logger.info(log_message)
        except Exception as ex:
            logger.exception('Error calling shutter position web service: {0}'.format(ex))

    @om_expose
    def get_config_description(self):
        return json.dumps(MQTTClient.config_description)

    @om_expose
    def get_config(self):
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        try:
            config = json.loads(config)
            for key in config:
                if isinstance(config[key], six.string_types):
                    config[key] = str(config[key])
            self._config_checker.check_config(config)
            self._config = config
            self._read_config()
            self.write_config(config)
            thread = Thread(target=self._load_and_connect)
            thread.start()
        except Exception as ex:
            logger.exception('Error saving configuration: {0}'.format(ex))

        return json.dumps({'success': True})
