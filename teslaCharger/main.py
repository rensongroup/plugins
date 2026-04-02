import json
import logging
import random
import time
import six
import sys
import requests
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

class TeslaCharger(OMPluginBase):

    """
    A plugin to publish Tesla Wall Charger info from Vitals:

    {'contactor_closed': False,
    'vehicle_connected': False,
    'session_s': 0,
    'grid_v': 231.6,
    'grid_hz': 49.815,
    'vehicle_current_a': 0.7,
    'currentA_a': 0.4,
    'currentB_a': 0.4,
    'currentC_a': 0.4,
    'currentN_a': 0.0,
    'voltageA_v': 0.0,
    'voltageB_v': 6.2,
    'voltageC_v': 0.0,
    'relay_coil_v': 11.9,
    'pcba_temp_c': 17.2,
    'handle_temp_c': 13.9,
    'mcu_temp_c': 23.9,
    'uptime_s': 15411,
    'input_thermopile_uv': -220,
    'prox_v': 0.0,
    'pilot_high_v': 11.9,
    'pilot_low_v': 11.9,
    'session_energy_wh': 0.0,
    'config_status': 5,
    'evse_state': 1,
    'current_alerts': []}

    From Lifetime:

    {'contactor_cycles': 29,
    'contactor_cycles_loaded': 1,
    'alert_count': 4074,
    'thermal_foldbacks': 0,
    'avg_startup_temp': 30.0,
    'charge_starts': 29,
    'energy_wh': 80,
    'connector_cycles': 2,
    'uptime_s': 3659417,
    'charging_time_s': 1618}
    """

    name = 'TeslaCharger'
    version = '0.0.5'
    interfaces = [('config', '1.0'),
                  ('metrics', '1.0')]

    metric_definitions = [{'type': 'ev',
                           'tags': ['device'],
                           'metrics': [{'name': 'contactorClosed',
                                        'description': 'Contactor State',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'vehicleConnected',
                                        'description': 'Battery Power',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'session_s',
                                        'description': 'Number of seconds the vehicle is currently being charged',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'vehicle_current_a',
                                        'description': 'Current amps to vehicle',
                                        'type': 'gauge', 'unit': 'A'},
                                       {'name': 'currentA_a',
                                        'description': 'Current amps phase 1',
                                        'type': 'gauge', 'unit': 'A'},
                                       {'name': 'currentB_a',
                                        'description': 'Current amps phase 2',
                                        'type': 'gauge', 'unit': 'A'},
                                       {'name': 'currentC_a',
                                        'description': 'Current amps phase 3',
                                        'type': 'gauge', 'unit': 'A'},
                                       {'name': 'currentN_a',
                                        'description': 'Current amps Neutral',
                                        'type': 'gauge', 'unit': 'A'},
                                       {'name': 'voltageA_v',
                                        'description': 'Current voltage phase 1',
                                        'type': 'gauge', 'unit': 'V'},
                                       {'name': 'voltageB_v',
                                        'description': 'Current voltage phase 2',
                                        'type': 'gauge', 'unit': 'V'},
                                       {'name': 'voltageC_v',
                                        'description': 'Current voltage phase 3',
                                        'type': 'gauge', 'unit': 'V'},
                                       {'name': 'session_energy_wh',
                                        'description': 'Used energy of this charging session',
                                        'type': 'gauge', 'unit': 'Wh'},
                                       {'name': 'energy_wh',
                                        'description': 'Alltime Charged wh',
                                        'type': 'gauge', 'unit': 'Wh'},
                                       {'name': 'alert_count',
                                        'description': 'Alltime alerts counter',
                                        'type': 'gauge', 'unit': ''},
                                       ]}]

    config_description = [{'name': 'tesla_wall_charger_ip',
                           'type': 'str',
                           'description': 'IP or hostname of Tesla Wall Charger.'},
                          ]

    default_config = {'tesla_wall_charger_ip': ''}


    def __init__(self, webinterface, connector):
        """
        @param webinterface : Interface to call local gateway APIs, called on runtime
        @param logger : A logger helper, called on runtime
        """
        super(TeslaCharger, self).__init__(webinterface=webinterface, connector=connector)
        logger.info('Starting plugin...')

        self._config = self.read_config(TeslaCharger.default_config)
        self._config_checker = PluginConfigChecker(TeslaCharger.config_description)

        self.last_data_save_reading = 0
        self._metrics_queue = deque()

        self._polling_frequency = 10

        self._sensor_dtos = {}
        self._read_config()

        self._lifetime_url = "http://{0}/api/1/lifetime".format(self._tesla_wall_charger_ip)
        self._vitals_url = "http://{0}/api/1/vitals".format(self._tesla_wall_charger_ip)

        logger.info(f"Started {self.name} plugin")

    def _read_config(self):
        self._tesla_wall_charger_ip = self._config.get('tesla_wall_charger_ip', TeslaCharger.default_config['tesla_wall_charger_ip'])

        self._enabled = self._tesla_wall_charger_ip != ''
        logger.info('Tesla Wall Chargers is {0}'.format('enabled' if self._enabled else 'disabled'))

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
            self._metrics_queue.appendleft({'type': 'ev',
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

    #Temp hack...
    quantity_unit_hack = {'energy': 'kilo_watt_hour',
                          'power': 'watt'
                          }

    @background_task
    def read_wallcharger(self):
        """
        1. Execute 2 api calls to the charing pole
        2. Update sensors
        3. Send metrics to the cloud
        """
        while self._enabled:
            # Fetch Data from the wall charger
            try:
                # Get Lifetime data
                lifetime = json.loads(requests.get(self._lifetime_url).content.decode('UTF-8'))
                # Get Vitals data
                vitals = json.loads(requests.get(self._vitals_url).content.decode('UTF-8'))
            except Exception as ex:
                logger.info("Error fetching data from wall charging unit: {0}".format(ex))
            # Update sensors
            try:
                pass
            except Exception as ex:
                 logger.info("Error updating sensors: {0}".format(ex))
            # Push metrics
            try:
                # Additional custom metrics
                charger_data = {
                                'contactorClosed': vitals['contactor_closed'],
                                'vehicleConnected': vitals['vehicle_connected'],
                                'session_s': vitals['session_s'],
                                'vehicle_current_a': vitals['vehicle_current_a'],
                                'currentA_a': vitals['currentA_a'],
                                'currentB_a': vitals['currentB_a'],
                                'currentC_a': vitals['currentC_a'],
                                'currentN_a': vitals['currentN_a'],
                                'voltageA_v': vitals['voltageA_v'],
                                'voltageB_v': vitals['voltageB_v'],
                                'voltageC_v': vitals['voltageC_v'],
                                'session_energy_wh': vitals['session_energy_wh'],
                                'energy_wh': lifetime['energy_wh'],
                                'alert_count': lifetime['alert_count']
                                }
                self._enqueue_metrics(tags={'device': 'teslacharger'}, values=charger_data)
            except Exception as ex:
                logger.info("Error enqueing metrics to the cloud: {0}".format(ex))
            time.sleep(self._polling_frequency)


    @om_expose
    def get_config_description(self):
        return json.dumps(TeslaCharger.config_description)

    @om_expose
    def get_config(self):
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        config = json.loads(config)
        for key in config:
            if isinstance(config[key], six.string_types):
                config[key] = str(config[key])
        self._config_checker.check_config(config)
        self._config = config
        self._read_config()
        self.write_config(config)
        return json.dumps({'success': True})
