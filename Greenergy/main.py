import json
import logging
import random
import time
import six
import sys
from typing import List
from collections import deque
from dataclasses import dataclass

from plugins.base import om_expose, output_status, background_task, \
    OMPluginBase, PluginConfigChecker, om_metric_data

logger = logging.getLogger(__name__)

class NoConsumptionException(Exception):
    pass

@dataclass
class Sensor:
    name: str
    description: str
    physical_quantity: str
    unit: str
    value: float

class Greenergy(OMPluginBase):

    """
    A plugin to publish grid consumption from energy module over MQTT
    Info returned from the BMS
    {
    "state":{
        "reported":{
            "highCell":30945,
            "lowCell":30583,
            "Pbatt":1018,
            "Pgrid":-102,
            "Psolar":205,
            "SOC":1,
            "Phouse":-915,
            "Whcount":14992.8,
            "CellTemp":22.55749,
            "5minGridKwh":0,
            "5minBattKwh":0,
            "invPinAc": 0,
            "invPoutAc": '0'
            'isHealthy': True,
            'isCANRunning': True,
            'isInverterRunning': True,
            'isP1Connected': True,
            'ExtPVPower': 0
        }
    }
    }
    """
    name = 'Greenergy'
    version = '0.0.21'
    interfaces = [('config', '1.0'),
                  ('metrics', '1.0')]

    metric_definitions = [{'type': 'battery',
                           'tags': ['device'],
                           'metrics': [{'name': 'gridPower',
                                        'description': 'Grid Power',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'batteryPower',
                                        'description': 'Battery Power',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'solarPower',
                                        'description': 'Solar Power',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'housePower',
                                        'description': 'House Power',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': '5minGridKwh',
                                        'description': '5minGridKwh',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': '5minBattKwh',
                                        'description': '5minBattKwh',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'CellTemp',
                                        'description': 'CellTemperature',
                                        'type': 'gauge', 'unit': 'degreesC'},
                                       {'name': 'stateOfCharge',
                                        'description': 'State of charge',
                                        'type': 'gauge', 'unit': '-'},
                                       {'name': 'energyCounter',
                                        'description': 'Energy counter',
                                        'type': 'counter', 'unit': '-'},
                                       {'name': 'highCell',
                                        'description': 'High Cell voltage',
                                        'type': 'counter', 'unit': 'V'},
                                       {'name': 'lowCell',
                                        'description': 'Low Cell voltage',
                                        'type': 'counter', 'unit': 'V'},
                                       {'name': 'invPinAc',
                                        'description': 'Invertor AC power in',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'invPoutAc',
                                        'description': 'Invertor AC power out',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'powerDelivered',
                                        'description': 'Grid power delivered',
                                        'type': 'gauge', 'unit': 'W'},
                                       {'name': 'powerReturned',
                                        'description': 'Grid power returned',
                                        'type': 'gauge', 'unit': 'W'},
                                       {'name': 'pGridSet',
                                        'description': 'Grid capacity setting',
                                        'type': 'gauge', 'unit': 'W'},
                                       {'name': 'isHealthy',
                                        'description': 'Battery healthy',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'isCANRunning',
                                        'description': 'CAN running',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'isInverterRunning',
                                        'description': 'Inverter running',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'isP1Connected',
                                        'description': 'P1 connected',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'ExtPVPower',
                                        'description': 'Externel PV power',
                                        'type': 'gauge', 'unit': 'W'}
                                       ]}]

    metric_mappings = {'Pbatt': 'batteryPower',
                       'Pgrid': 'gridPower',
                       'highCell': 'highCell',
                       'SOC': 'stateOfCharge',
                       'lowCell': 'lowCell',
                       '5minGridKwh': '5minGridKwh',
                       'CellTemp': 'CellTemp',
                       '5minBattKwh': '5minBattKwh',
                       'Phouse': 'housePower',
                       'Whcount': 'energyCounter',
                       'Psolar': 'solarPower',
                       'invPinAc': 'invPinAc',
                       'invPoutAc': 'invPoutAc',
                       'isHealthy': 'isHealthy',
                       'isCANRunning': 'isCANRunning',
                       'isInverterRunning': 'isInverterRunning',
                       'isP1Connected': 'isP1Connected',
                       'ExtPVPower': 'ExtPVPower'
                       }


    #Dict containing values to register as a sensor with their physical quantity
    sensors_to_register = {'Pbatt': 'power',
                           '5minGridKwh': 'energy',
                           '5minBattKwh': 'energy',
                           'invPinAc': 'power',
                           'invPoutAc': 'power',
                           }

    config_description = [{'name': 'broker_ip',
                           'type': 'str',
                           'description': 'IP or hostname of the MQTT broker.'},
                          {'name': 'broker_port',
                           'type': 'int',
                           'description': 'Port of the MQTT broker. Default: 1883'},
                          {'name': 'serial_number',
                           'type': 'str',
                           'description': 'Greenergy serial number. eg. SNxxxx'},
                          {'name': 'update_bms_frequency',
                           'type': 'int',
                           'description': 'Frequency with which to push power date to BMS[sec]'},
                          {'name': 'data_frequency',
                           'type': 'int',
                           'description': 'Frequency with which to push power datato the cloud [sec]'},
                          {'name': 'net_consumption_formula',
                           'type': 'str',
                           'description': 'Formula of Ct defining the net consumption. eg. 2.4 + 1.5 ( mind the spaces next to the operator)'},
                          {'name': 'grid_capacity',
                           'type': 'int',
                           'description': 'Capacity of the grid(in W) to correct the battery operation with'},
                          {'name': 'grid_capacitor',
                           'type': 'int',
                           'description': 'Id of the 0/1-10V Control to balance the grid capacity(in W)'},
                          {'name': 'battery_soc',
                           'type': 'int',
                           'description': 'HACK: Id of the 0/1-10V Control to visualise the State of charge of the battery'},
                          ]

    default_config = {'broker_ip': '',
                      'broker_port': 1883,
                      'serial_number': '',
                      'update_bms_frequency': 2,
                      'data_frequency': 120,
                      'grid_capacity': 0,
                      'grid_capacitor': 998,
                      'battery_soc': ''}


    def __init__(self, webinterface, connector):
        """
        @param webinterface : Interface to call local gateway APIs, called on runtime
        @param logger : A logger helper, called on runtime
        """
        super(Greenergy, self).__init__(webinterface=webinterface, connector=connector)
        logger.info('Starting plugin...')

        self._config = self.read_config(Greenergy.default_config)
        self._config_checker = PluginConfigChecker(Greenergy.config_description)

        self._read_config()

        self.last_data_save_reading = 0
        self._metrics_queue = deque()
        eggs = '/opt/openmotics/python/eggs'
        if eggs not in sys.path:
            sys.path.insert(0, eggs)

        self._writing_topic_power_delivered = 'DSMR-API/power_delivered'
        self._writing_topic_power_returned = 'DSMR-API/power_returned'
        self._reading_topic_bms = 'LF2/{0}/actual'.format(self._serial_number)
        self._writing_topic_pgridset = '$aws/things/LF2{0}/shadow/name/EMS/update'.format(self._serial_number)

        self.client = None
        self._sensor_dtos = {}

        logger.info(f"Started {self.name} plugin")

    def _read_config(self):
        self._broker_ip = self._config.get('broker_ip', Greenergy.default_config['broker_ip'])
        self._broker_port = self._config.get('broker_port', Greenergy.default_config['broker_port'])
        self._serial_number = self._config.get('serial_number', Greenergy.default_config['serial_number'])
        self._update_bms_frequency = self._config.get('update_bms_frequency', Greenergy.default_config['update_bms_frequency'])
        self._data_frequency = self._config.get('data_frequency', Greenergy.default_config['data_frequency'])
        self._grid_capacity = self._config.get('grid_capacity', Greenergy.default_config['grid_capacity'])
        self._capacitor_output = self._config.get('grid_capacitor', Greenergy.default_config['grid_capacitor'])
        self._soc_output = self._config.get('battery_soc', Greenergy.default_config['battery_soc'])
        self._capacitor_status = 0
        self._PGridSet = 0

        self._net_consumption_formula = []
        for item in self._config.get('net_consumption_formula', '').split(' '):
            if item in ['+', '-']:
                self._net_consumption_formula.append(item)
            elif '.' in item:
                module_index, input_index = item.split('.')
                self._net_consumption_formula.append([module_index, int(input_index)])
                # Should end up being like [['5', 1], '+', ['5', 5], '+', ['5', 2], '-', ['3', 11]]

        self._connected = False
        self._mqtt_enabled = self._broker_ip is not None and self._broker_port is not None and bool(self._serial_number) and len(self._net_consumption_formula) > 0
        logger.info("MQTTClient is {0}".format("enabled" if self._mqtt_enabled else "disabled"))

        logger.info("Saving a data point every {} seconds".format(self._data_frequency))

    def _calculate_net_consumption(self):
        realtime_power = json.loads(self.webinterface.get_realtime_power())
        if realtime_power.get('success') is not True:
            raise NoConsumptionException(realtime_power.get('msg', 'Unknown error'))
        operator = None
        net_consumption_value = 0.0
        net_consumption = {"power_delivered":[{"value":0,"unit":"kW"}],
                           "power_returned":[{"value":0,"unit":"kW"}]}
        for entry in self._net_consumption_formula:
            if isinstance(entry, list):
                value = realtime_power[entry[0]][entry[1]][3]  # 3 = power
                if operator is None or operator == '+':
                    net_consumption_value += value
                else:
                    net_consumption_value -= value
            else:
                operator = entry
        #logger.info('Net consumption: {0:.3f} kW'.format(net_consumption_value / 1000.0))

        # return dict with either power_delivered or power_returned
        if net_consumption_value > 0:
            net_consumption['power_delivered'][0]['value'] = round((net_consumption_value / 1000.0), 3)
        else:
            net_consumption['power_returned'][0]['value'] = round((-net_consumption_value / 1000.0), 3)
        self._PGridSet = self._grid_capacity*self._capacitor_status/100
        #logger.info("Grid Capacity: {0}, Capacitor status: {1} results in PGridSet {2}".format(self._grid_capacity, self._capacitor_status, self._PGridSet))
        net_capacity = {"desired": {"PGridSet": self._PGridSet}}

        return net_consumption, net_capacity


    @output_status
    def grid_status(self, status, version=2):
        if self._mqtt_enabled is True:
            logger.info(status)
            #consumer_next = self._consumers[self._modulation] if self._modulation < len(self._consumers) else None
            # boost
            self._capacitor_status = 0
            for output in status:
                if output[0] == self._capacitor_output:
                    self._capacitor_status = output[1]
                    logger.info("Grid capacitor set to {0}.".format(self._capacitor_status))

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
            self._metrics_queue.appendleft({'type': 'battery',
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
                logger.debug("Sending data to cloud: {0}".format(self._metrics_queue))
                while True:
                    yield self._metrics_queue.pop()
            else:
                logger.debug("No data to send to cloud")
        except IndexError:
            pass
        except Exception as ex:
            logger.info('Unexpected error while sending data to cloud: {0}'.format(ex))

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

        logger.info("Connected to MQTT broker battery ({0}:{1})".format(self._broker_ip, self._broker_port))
        self._connected = True
        try:
            client.subscribe(self._reading_topic_bms)
            logger.info("Gateway subscribed to {}".format(self._reading_topic_bms))
        except Exception as ex:
            logger.info("Gateway could not subscribe to {0}: {1}".format(self._reading_topic_bms, ex))

    def _on_message_reading(self, client, userdata, msg):
        """
        When topic data is received from the broker, queue it and send to the cloud in batches with collect_metrics
        Notes
        -----
        If for some reason (e.g. an error), this function cannot be executed, paho-mqtt falls back on the default
        on_message call.
        """
        _ = client, userdata
        if self.last_data_save_reading + self._data_frequency < int(time.time()):
            battery_data_payload = json.loads(msg.payload)
            try:
                battery_data = battery_data_payload['state']['reported']
                logger.debug('battery_data is: {0}'.format(battery_data))
                #Update the SOC output if configured
                if self._soc_output and 'SOC' in battery_data:
                    self._update_battery_soc_output(battery_data['SOC'])
                #update keys
                battery_data = {self.metric_mappings.get(k): float(v) for k, v in battery_data.items()}
                #Remove battery values which are stored in sensors
                self._enqueue_metrics(tags={'device': 'greenergy'}, values=battery_data)
                # Create and/or update sensors
                sensors = self._get_sensors(battery_data)
                self._populate_sensors(sensors)
            except Exception as ex:
                logger.info('Error queuing metrics (reading topic): {0}'.format(ex))

            self.last_data_save_reading = int(time.time())

    #Temp hack...
    quantity_unit_hack = {'energy': 'kilo_watt_hour',
                          'power': 'watt'
                          }

    def _get_sensors(self, battery_data):
        sensors = []
        metrics_definitions = self.metric_definitions[0]['metrics']
        for metric, name in self.metric_mappings.items():
            if metric in self.sensors_to_register.keys():
                for definition in metrics_definitions:
                    if definition['name'] == metric:
                        sensors.append(Sensor(name=definition['name'],
                                            description=definition['description'],
                                            physical_quantity=self.sensors_to_register[metric],
                                            unit=definition['unit'],
                                            value=battery_data[metric]))
        return sensors

    def _populate_sensors(self, sensors: List[Sensor]):
        for sensor in sensors:
            external_id = f'greenergysensor_{sensor.name}'
            if external_id not in self._sensor_dtos and sensor.value is not None:
                try:
                    # Register the sensor on the gateway
                    name = f'{sensor.description} (greenergy {sensor.description})'
                    sensor_dto = self.connector.sensor.register(external_id=external_id,
                                                                name=name,
                                                                physical_quantity=sensor.physical_quantity,
                                                                unit=self.quantity_unit_hack[sensor.physical_quantity])
                    logger.info('Registered %s' % sensor)
                    self._sensor_dtos[external_id] = sensor_dto
                except Exception:
                    logger.exception('Error registering sensor %s' % sensor)
            try:
                sensor_dto = self._sensor_dtos.get(external_id)
                # only update sensor value if the sensor is known on the gateway
                if sensor_dto is not None:
                    value = round(sensor.value, 2) if sensor.value is not None else None
                    self.connector.sensor.report_status(sensor=sensor_dto,
                                                       value=value)
            except Exception:
                logger.exception('Error while reporting sensor state')

    def _update_battery_soc_output(self, soc):
        try:
            result = json.loads(self.webinterface.set_output(id=self._soc_output, is_on=True,dimmer=soc))
            if not result.get('success', False):
                logger.error('Could not update dimmer {0} to {1}: {2}'.format(self._soc_output,
                                                                               soc,
                                                                               result.get('msg', 'Unknown')))
        except Exception as ex:
            logger.exception('Unexpected exception setting dimmer {0} to {1}'.format(self._soc_output,
                                                                                     soc))

    @background_task
    def control_battery(self):
        """
        1. Connect to battery mqtt broker
        2. Subscribe to reading topic and send data from bms to cloud/InfluxDB on connection
        3. Publish data to mqtt broker
        """
        awsLastTime = 0
        while True:
            if self._mqtt_enabled:
                if not self._connected:
                    try:
                        import paho.mqtt.client as mqtt
                        self.client = mqtt.Client()
                        self.client.message_callback_add(self._reading_topic_bms, self._on_message_reading)
                        self.client.on_connect = self._on_connect
                        self.client.connect(self._broker_ip, self._broker_port, 5)
                        self.client.loop_start()
                    except Exception as ex:
                        logger.info("Error connecting to MQTT broker ({0}:{1}): {2}".format(self._broker_ip, self._broker_port, ex))
                        time.sleep(3)
                        return
                # Get actual consumption
                try:
                    net_consumption, net_capacity = self._calculate_net_consumption()
                except Exception as ex:
                    logger.info("Error fetching energy data: {0}".format(ex))
                    time.sleep(3)
                    return
                # Publish to broker
                try:
                    self.client.publish(self._writing_topic_power_delivered, json.dumps({"power_delivered":net_consumption['power_delivered']}), retain=False)
                    self.client.publish(self._writing_topic_power_returned, json.dumps({"power_returned":net_consumption['power_returned']}), retain=False)
                    if time.time() - awsLastTime > 120:
                        self.client.publish(self._writing_topic_pgridset, json.dumps({"state":net_capacity}), retain=False)
                        logger.info("Sending {1} to BMS {0}".format(self._writing_topic_pgridset, net_capacity))
                        awsLastTime = time.time()
                    # Additional custom metrics
                    calculated_battery_data = {'powerDelivered': float(net_consumption['power_delivered'][0]['value']),
                                               'powerReturned': float(net_consumption['power_returned'][0]['value']),
                                               'pGridSet': float(self._PGridSet)}
                    self._enqueue_metrics(tags={'device': 'greenergy'}, values=calculated_battery_data)

                except Exception as ex:
                    logger.info("Error sending data to broker: {0}".format(ex))

            time.sleep(self._update_bms_frequency)

    @om_expose
    def get_config_description(self):
        return json.dumps(Greenergy.config_description)

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
