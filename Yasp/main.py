import json
import logging
import time

from plugins.base import (OMPluginBase, PluginConfigChecker, background_task,
                          om_expose)

from .client import DummyWebConnect, WebConnect, Value

logger = logging.getLogger(__name__)


class Yasp(OMPluginBase):
    """
    Integrate an SMA inverter using it's WebConnect api
    """
    name = 'SMA'
    version = '0.0.1'
    interfaces = [('config', '1.0')]

    default_config = {
        'dummy': False,  # useful for local development
        'sample_rate': 60,
    }

    config_description = [
        {'name': 'sample_rate',
         'type': 'int',
         'description': 'How frequent (every x seconds) to fetch the sensor data, Default: 30'},
        {'name': 'devices',
         'type': 'section',
         'description': 'List of all SMA devices.',
         'repeat': True,
         'min': 1,
         'content': [{'name': 'sma_inverter_ip',
                      'type': 'str',
                      'description': 'IP or hostname of the SMA inverter including the scheme (e.g. http:// or https://).'},
                     {'name': 'password',
                      'type': 'str',
                      'description': 'The password of the `User` account'}]}
    ]

    def __init__(self, webinterface, connector):
        super().__init__(webinterface=webinterface, connector=connector)

        self._config = self.read_config(self.default_config)
        self._config_checker = PluginConfigChecker(self.config_description)
        self._read_config()
        self._counters = {}

    def _read_config(self):
        self._sample_rate = self._config['sample_rate']
        if self._config.get('dummy'):
            self._sma_devices = [DummyWebConnect('data/single_phase.json'),
                                 DummyWebConnect('data/three_phase.json')]
        else:
            self._sma_devices = [WebConnect(x['sma_inverter_ip'], x['password']) for x in self._config.get('devices', [])]

    @om_expose
    def get_config_description(self):
        return json.dumps(self.config_description)

    @om_expose
    def get_config(self):
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        config = json.loads(config)
        self._config_checker.check_config(config)
        self.write_config(config)
        self._config = config
        self._read_config()
        return json.dumps({'success': True})

    @background_task
    def sample_measurements(self):
        while True:
            try:
                for client in self._sma_devices:
                    for data in client.fetch_values(Value.TOTAL_YIELD, Value.PV_POWER):
                        if data['type'] == Value.TOTAL_YIELD:
                            counter = self._get_counter(data['external_id'])
                            self.connector.measurement_counter.report_counter_state(counter, total_consumed=0, total_injected=data['value'])
            except Exception:
                logger.exception('Failed to report measurements state')
            time.sleep(self._sample_rate)

    @background_task
    def sample_realtime(self):
        while True:
            try:
                for client in self._sma_devices:
                    for data in client.fetch_values(Value.PV_POWER):
                        counter = self._counters.get(data['external_id'])
                        if counter:
                            self.connector.measurement_counter.report_realtime_state(counter, -data['value'])
            except Exception:
                logger.exception('Failed to report realtime state')
            time.sleep(5)

    def _get_counter(self, external_id):
        counter_type = self.connector.measurement_counter.Enums.Types.SOLAR
        if external_id not in self._counters:
            self._counters[external_id] = self.connector.measurement_counter.register_counter_electricity_wh(external_id, counter_type, 'SMA Inverter', has_realtime=True)
        return self._counters[external_id]
