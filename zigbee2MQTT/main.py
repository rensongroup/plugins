import json
import logging
import random
import time
import six
import sys
from collections import deque

from plugins.base import OMPluginBase, PluginConfigChecker, background_task, \
    om_expose, output_status, om_metric_receive, om_metric_data

logger = logging.getLogger(__name__)

class zigbee2MQTT(OMPluginBase):

    """
    A plugin to communicate with zigbee devices exposed via zigbee2mqtt
    """

    name = 'zigbee2MQTT'
    version = '0.0.27'
    interfaces = [('config', '1.0'),
                  ('metrics', '1.0')]

    metric_definitions = []
    #TODO: add luminance (  LUX = 'lux'?? ) as sensor device type
    sensor_device_types = ['temperature', 'humidity', 'co2', 'energy(kwh)', 'brightness']
    actuator_device_types = ['light', 'outlet', 'dimmer']

    config_description = [{'name': 'broker_ip',
                           'type': 'str',
                           'description': 'IP or hostname of the MQTT broker.'},
                          {'name': 'broker_port',
                           'type': 'int',
                           'description': 'Port of the MQTT broker. Default: 1883'},
                           {'name': 'broker_username',
                           'type': 'str',
                           'description': 'optional username'},
                          {'name': 'broker_password',
                           'type': 'str',
                           'description': 'optional password'},
                          {'name': 'zigbee2mqtt_broker_topic',
                           'type': 'str',
                           'description': 'Broker topic to communicate with zigbee2mqtt'},
                          {'name': 'sensor_mapping',
                          'type': 'section',
                          'description': 'Sensor mapping',
                          'repeat': True,
                          'min': 0,
                          'content': [{'name': 'mqtt_friendly_name', 'type': 'str'},
                                      {'name': 'om_friendly_name', 'type': 'str'},
                                      {'name': 'device_type',
                                          'type': 'enum',
                                          'description': 'Device type',
                                          'choices': sensor_device_types}]},
                          {'name': 'actuator_mapping',
                          'type': 'section',
                          'description': 'Actuator mapping',
                          'repeat': True,
                          'min': 0,
                          'content': [{'name': 'mqtt_friendly_name', 'type': 'str'},
                                      {'name': 'om_output_id', 'type': 'int'}]},
                          {'name': 'input_mapping',
                          'type': 'section',
                          'description': 'Input mapping',
                          'repeat': True,
                          'min': 0,
                          'content': [{'name': 'mqtt_friendly_name', 'type': 'str'},
                                      {'name': 'om_input_id', 'type': 'int'}]}
                          ]

    default_config = {'broker_ip': '',
                      'broker_port': 1883,
                      'broker_username': '',
                      'broker_password': '',
                      'zigbee2mqtt_broker_topic': 'zigbee2mqtt',
                      'sensor_mapping': [],
                      'actuator_mapping': [],
                      'input_mapping': []
                      }

    def __init__(self, webinterface, connector):
        """
        @param webinterface : Interface to call local gateway APIs, called on runtime
        @param logger : A logger helper, called on runtime
        """
        super(zigbee2MQTT, self).__init__(webinterface=webinterface,
                                    connector=connector)
        logger.info('Starting plugin...')

        self._config = self.read_config(zigbee2MQTT.default_config)
        self._config_checker = PluginConfigChecker(zigbee2MQTT.config_description)

        self.last_data_save_reading = 0

        self._sensor_dto = {}
        self._metrics_queue = deque()

        eggs = '/opt/openmotics/python/eggs'
        if eggs not in sys.path:
            sys.path.insert(0, eggs)

        self.client = None
        self._read_config()

        self._list_of_sensors = self._list_sensors()
        self._dict_of_actuators = self._dict_of_actuators()
        self._dict_of_inputs = self._dict_of_inputs()

        logger.info("Register sensors")
        self._register_sensors()
        logger.info("Started plugin")

    def _dict_of_actuators(self):
        actuator_dict = {}
        for actuator_config in self._config.get('actuator_mapping'):
            actuator_dict[actuator_config['om_output_id']] = (actuator_config['mqtt_friendly_name'])
        return actuator_dict

    def _dict_of_inputs(self):
        input_dict = {}
        for input_config in self._config.get('input_mapping'):
            input_dict[input_config['om_input_id']] = (input_config['mqtt_friendly_name'])
        return input_dict

    def _list_sensors(self):
        sensor_list = []
        for sensor_config in self._config.get('sensor_mapping'):
            sensor_list.append(sensor_config['mqtt_friendly_name'])
        return sensor_list

    def _read_config(self):
        self._broker_ip = self._config.get('broker_ip', zigbee2MQTT.default_config['broker_ip'])
        self._broker_port = self._config.get('broker_port', zigbee2MQTT.default_config['broker_port'])
        self._broker_username = self._config.get('broker_username', zigbee2MQTT.default_config['broker_username'])
        self._broker_password = self._config.get('broker_password', zigbee2MQTT.default_config['broker_password'])
        self._zigbee2mqtt_broker_topic = self._config.get('zigbee2mqtt_broker_topic', zigbee2MQTT.default_config['zigbee2mqtt_broker_topic'])
        self._sensor_mapping = self._config.get('sensor_mapping', zigbee2MQTT.default_config['sensor_mapping'])
        self._actuator_mapping = self._config.get('actuator_mapping', zigbee2MQTT.default_config['actuator_mapping'])
        self._input_mapping = self._config.get('input_mapping', zigbee2MQTT.default_config['input_mapping'])

        self._connected = False
        self._mqtt_enabled = self._broker_ip is not None and self._broker_port is not None
        logger.info("zigbee2mqtt is {0}".format("enabled" if self._mqtt_enabled else "disabled"))

    def _enqueue_metrics(self, tags, values):
        """
        Add a received metric to _metrics_queue, so this can be sent to cloud/
        database in batch later.
        @param tags: hold metric tags, e.g. {'id': sensor_id, 'name': sensor_name}
        @type tags: dict
        @param values: holds metric values, e.g. {'value_1': float(value_1),'value_2': float(value_2),
         'value_3': int(value_3)}
        @type values : dict
        """
        try:
            now = time.time()
            self._metrics_queue.appendleft({'type': 'zigbee2mqtt',
                                            'timestamp': int(now),
                                            'tags': tags,
                                            'values': values})
        except Exception as ex:
            logger.info('Got unexpected error while enqueing metrics: {0}'.format(ex))

    @om_metric_data(interval=60)
    def collect_metrics(self):
        # Yield all metrics in the Queue
        try:
            if self._metrics_queue:
                logger.info("Sending data to cloud: {0}".format(self._metrics_queue))
                while True:
                    yield self._metrics_queue.pop()
            else:
                logger.info("No data to send to cloud")
        except IndexError:
            pass
        except Exception as ex:
            logger.info('Unexpected error while sending data to cloud: {0}'.format(ex))

    def _register_sensors(self):
        if self._config.get('sensor_mapping', False):
            logger.info('Registering sensors...')
            try:
                #Grouping in the frontend is done using the external id
                for sensor_config in self._config.get('sensor_mapping'):
                    external_id = sensor_config['om_friendly_name']
                    dto_name = '{0}-{1}'.format(sensor_config['device_type'], sensor_config['om_friendly_name'])
                    if sensor_config['device_type'] == 'temperature':
                        sensor = self.connector.sensor.register_temperature_celcius(external_id=external_id, name=sensor_config['om_friendly_name'])
                    elif sensor_config['device_type'] == 'humidity':
                        sensor = self.connector.sensor.register_humidity_percent(external_id=external_id, name=sensor_config['om_friendly_name'])
                    elif sensor_config['device_type'] == 'co2':
                        sensor = self.connector.sensor.register_co2_ppm(external_id=external_id, name=sensor_config['om_friendly_name'])
                    elif sensor_config['device_type'] == 'energy(kwh)':
                        sensor = self.connector.sensor.register_energy_kwh(external_id=external_id, name=sensor_config['om_friendly_name'])
                    elif sensor_config['device_type'] == 'brightness':
                        sensor = self.connector.sensor.register(external_id=external_id, physical_quantity='brightness', unit='lux', name=sensor_config['om_friendly_name'])
                    self._sensor_dto[dto_name] = sensor
            except Exception:
                logger.exception('Error registering sensor')
                #self._sensor_dto = None
        else:
            self._sensor_dto = None


    def _on_connect(self, client, userdata, flags, rc):
        """
        The callback for when the client receives a CONNACK response from the server.
        Subscribing to a topic in on_connect() means that if we lose the connection and reconnect then subscriptions
        will be renewed.
        """
        _ = userdata, flags
        if rc != 0:
            logger.info("Error connecting: rc={0}", rc)
            return

        logger.info("Connected to MQTT broker ({0}:{1})".format(self._broker_ip, self._broker_port))
        self._connected = True
        try:
            client.subscribe(self._zigbee2mqtt_broker_topic + "/#")
            logger.info("Gateway subscribed to {0}/#".format(self._zigbee2mqtt_broker_topic))
        except Exception as ex:
            logger.info("Gateway could not subscribe to {0}/#: {1}".format(self._zigbee2mqtt_broker_topic, ex))


    def _on_message_reading(self, client, userdata, msg):
        """
        When topic data is received from the broker, queue it and send to the cloud in batches with collect_metrics
        Notes
        -----
        If for some reason (e.g. an error), this function cannot be executed, paho-mqtt falls back on the default
        on_message call.
        """
        try:
            _ = client, userdata
            mqtt_friendly_name = msg.topic.split('/')[1]
            payload = json.loads(msg.payload)
            if mqtt_friendly_name in self._list_of_sensors:
                self._report_sensor_state(mqtt_friendly_name, payload)
            #TODO: check value since friendly name is the value not the key
            if mqtt_friendly_name in self._dict_of_actuators.values():
                self._report_actuator_state(mqtt_friendly_name, payload)
            if mqtt_friendly_name in self._dict_of_inputs.values():
                self._report_input_state(mqtt_friendly_name, payload)
        except Exception as ex:
            logger.info("Something went wrong in the _on_message_reading: {0}".format(ex))

    def _get_actuator_id(self, val):
        for key, value in self._dict_of_actuators.items():
            if val == value:
                return key

    def _get_input_id(self, val):
        for key, value in self._dict_of_inputs.items():
            if val == value:
                return key

    # Fetch all sensor data and update the value accordingly
    def _report_sensor_state(self, mqtt_friendly_name, payload):
        try:
            for sensor_config in self._sensor_mapping:
                if sensor_config['mqtt_friendly_name'] == mqtt_friendly_name:
                    dto_name = '{0}-{1}'.format(sensor_config['device_type'], sensor_config['om_friendly_name'])
                    dto = self._sensor_dto[dto_name]
                    sensor_type = sensor_config['device_type']
                    #Temp hack
                    if sensor_type == 'brightness':
                        sensor_type = 'illuminance'
                    value = payload[sensor_type]
                    self.connector.sensor.report_status(sensor=dto, value=value)
        except Exception as ex:
            logger.info('Failed to set the sensor state for {0} with type {1} with exception {2} '.format(mqtt_friendly_name, sensor_type, ex))

    # Fetch all actuator data and update the value accordingly
    def _report_actuator_state(self, mqtt_friendly_name, payload):
        try:
            actuator_id = self._get_actuator_id(mqtt_friendly_name)
            value = payload["state"]
            logger.info('Friendly name {0} has Value in report actuator state {1}'.format(mqtt_friendly_name, value))
            if value == "ON":
                #TODO: shouldn't this be boolean instead of string?
                result = json.loads(self.webinterface.set_output(None, actuator_id, 'true'))
            elif value == "OFF":
                result = json.loads(self.webinterface.set_output(None, actuator_id, 'false'))
            else:
                logger.info("State {0} not implemented yet".format(value))
        except Exception as ex:
            logger.info('Failed to set the actuator state for {0} with exception {2} '.format(mqtt_friendly_name, ex))

    # Fetch all input data and update the value accordingly
    def _report_input_state(self, mqtt_friendly_name, payload):
        try:
            input_id = self._get_input_id(mqtt_friendly_name)
            value = payload["occupancy"]
            result = json.loads(self.webinterface.set_input(None, input_id, value))
        except Exception as ex:
            logger.info('Failed to set the input state for {0} with with exception {1}'.format(mqtt_friendly_name, ex))

    # Check if the output is configured in the zigbee2mqtt plugin and if so send the appropriate command
    @output_status(version=2)
    def _handle_output_status(self, event):
        if event['id'] in self._dict_of_actuators:
            #publish mqtt message with the state
            try:
                topic = '{0}/{1}/set'.format(self._zigbee2mqtt_broker_topic, self._dict_of_actuators[event['id']])
                state_message = {"state": "{0}".format("ON" if event['status']['on'] else "OFF")}
                self.client.publish(topic, json.dumps(state_message))
            except Exception as ex:
                logger.info('Failed to publish actuator state on the mqtt for id {0} with exception {1}'.format(event['id'], ex))


    @background_task
    def read_zigbee2mqtt(self):
        """
        1. Connect to zigbee2mqtt broker
        2. Subscribe to reading topic and send data from zigbee2mqtt to cloud/InfluxDB on connection
        3. Publish data to mqtt broker
        """

        if self._mqtt_enabled:
            if not self._connected:
                try:
                    import paho.mqtt.client as mqtt
                    self.client = mqtt.Client()
                    # TODO: merge these lists to optimize the loops here.
                    for sensor in self._config.get('sensor_mapping'):
                        self.client.message_callback_add("{0}/{1}".format(self._zigbee2mqtt_broker_topic, sensor['mqtt_friendly_name']) , self._on_message_reading)
                    for actuator in self._config.get('actuator_mapping'):
                        self.client.message_callback_add("{0}/{1}".format(self._zigbee2mqtt_broker_topic, actuator['mqtt_friendly_name']) , self._on_message_reading)
                    for input in self._config.get('input_mapping'):
                        self.client.message_callback_add("{0}/{1}".format(self._zigbee2mqtt_broker_topic, input['mqtt_friendly_name']) , self._on_message_reading)
                    self.client.on_connect = self._on_connect
                    if self._broker_username and self._broker_password:
                        self.client.username_pw_set(self._broker_username, self._broker_password)
                    self.client.connect(self._broker_ip, self._broker_port, 5)
                    self.client.loop_start()
                except Exception as ex:
                    logger.info("Error connecting to MQTT broker ({0}:{1}): {2}".format(self._broker_ip, self._broker_port, ex))
                    time.sleep(3)
                    return

    @om_expose
    def get_config_description(self):
        return json.dumps(zigbee2MQTT.config_description)

    @om_expose
    def get_config(self):
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        logger.info("Applying new configuration...")
        if self._connected:
            try:
                self.client.loop_stop()
                logger.info('Stopping client connection...')
            except:
                pass
        config = json.loads(config)
        for key in config:
            if isinstance(config[key], six.string_types):
                config[key] = str(config[key])
        self._config_checker.check_config(config)
        self._config = config
        self._read_config()
        self.write_config(config)
        return json.dumps({'success': True})

