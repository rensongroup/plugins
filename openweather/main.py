# Copyright (C) 2020 OpenMotics BV
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either versio 3 of the
# License, or (at your option) any later versio.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import time
import requests
import simplejson as json
from plugins.base import om_expose, OMPluginBase, PluginConfigChecker, background_task, om_metric_data
from collections import deque


class OpenWeather(OMPluginBase):
    """
    Reads out the OpenWeather API to fetch outside temperature, humidity and AQI for a certain location
    """

    name = 'OpenWeather'
    version = '1.0.15'
    interfaces = [('config', '1.0')]

    config_description = [{'name': 'api_key',
                           'type': 'str',
                           'description': 'The API key from OpenWeather'},
                           {'name': 'latitude',
                           'type': 'int',
                           'description': 'Your latitude'},
                           {'name': 'longitude',
                           'type': 'int',
                           'description': 'Your longitude'},
                           {'name': 'sensor_prefix',
                           'type': 'str',
                           'description': 'The OpenMotics sensor prefix'},
                          {'name': 'sample_rate',
                           'type': 'int',
                           'description': 'How frequent (every x seconds) to fetch the sensor data, Default: 120'},
                          {'name': 'debug',
                           'type': 'bool',
                           'description': 'Indicate whether debug logging should be enabled'}]

    default_config = {}

    def _log_debug(self, message):
        if self._debug:
            self.logger(message)

    def __init__(self, webinterface, logger):
        super(OpenWeather, self).__init__(webinterface, logger)
        self.logger('Starting OpenWeather plugin...')
        self._config = self.read_config(OpenWeather.default_config)
        self._config_checker = PluginConfigChecker(OpenWeather.config_description)
        self._enabled = False
        self._sensor_prefix = ''
        self._sample_rate = 120
        self._read_config()
        # Fetch the GW sensors or create if not present
        self._sensorconfig = [{"source":{"type": "plugin","name": self.name},"external_id":"temp","physical_quantity": "temperature","unit": "celcius","name":"{0}temp".format(self._sensor_prefix)},
                              {"source":{"type": "plugin","name": self.name},"external_id":"hum","physical_quantity": "humidity","unit": "percent","name":"{0}hum".format(self._sensor_prefix)},
                              {"source":{"type": "plugin","name": self.name},"external_id":"aqi","physical_quantity": "aqi","unit": "none","name":"{0}aqi".format(self._sensor_prefix)}]

        self.webinterface.set_sensor_configurations(config=json.dumps(self._sensorconfig))

        # TODO: extra call until bug OM-2318 is fixed
        response_sensors = json.loads(self.webinterface.get_sensor_configurations())
        self._log_debug('response sensors are {0}'.format(response_sensors))
        self._sensors_ids = {i['external_id']: i['id']  for i in response_sensors["config"]}
        self._log_debug('Sensors ids are {0}'.format(self._sensors_ids))

        self.logger("Started OpenWeather plugin")

    def _read_config(self):
        self.logger('start read config')
        self._enabled = False
        self._api_key = str(self._config.get('api_key', ''))
        self._latitude = int(self._config.get('latitude', 0))
        self._longitude = int(self._config.get('longitude', 0))
        self._sensor_prefix = str(self._config.get('sensor_prefix', ''))
        self._sample_rate = int(self._config.get('sample_rate', 120))
        self._debug = bool(self._config.get('debug', False))

        self._enabled = self._api_key and self._latitude and self._longitude
        self.logger('OpenWeather is {0}'.format('enabled' if self._enabled else 'disabled'))



    @background_task
    def run(self):
        while True:
            if not self._enabled:
                time.sleep(5)
                continue
            try:
                sensors = self._read_data()
            except Exception as ex:
                self.logger('Could not read OpenWeather values: {0}'.format(ex))
            try:
                self._write_sensor_values(sensors)
            except Exception as ex:
                self.logger('Could not write OpenMotics sensors: {0}'.format(ex))
            time.sleep(self._sample_rate)

    def _read_data(self):
        endpoint_temphum = 'http://api.openweathermap.org/data/2.5/weather?lat={0}&lon={1}&appid={2}&units=metric'.format(self._latitude, self._longitude, self._api_key)
        endpoint_aqi = 'http://api.openweathermap.org/data/2.5/air_pollution?lat={0}&lon={1}&appid={2}'.format(self._latitude, self._longitude, self._api_key)
        response_temphum = requests.get(endpoint_temphum).json()
        response_aqi = requests.get(endpoint_aqi).json()

        self._log_debug('The temphum sensors are: {0}'.format(response_temphum))
        self._log_debug('The aqi sensors are: {0}'.format(response_aqi))

        if 'main' not in response_temphum or len(response_temphum['main']) < 1:
            raise RuntimeError('Unexpected response: {0}'.format(response_temphum))
        if 'list' not in response_aqi or len(response_aqi['list'][0]['main']) < 1:
            raise RuntimeError('Unexpected response: {0}'.format(response_aqi))

        sensors = {
            'temp': response_temphum['main']['temp'],
            'hum': response_temphum['main']['humidity'],
            'aqi': response_aqi['list'][0]['main']['aqi'],
        }
        self._log_debug('Sensors are {0}'.format(sensors))
        return sensors

    def _write_sensor_values(self, sensors):
        # Write temp sensor
        result = json.loads(self.webinterface.set_sensor_status(status=json.dumps({'id':self._sensors_ids['temp'], 'value':sensors['temp']})))
        if result['success'] is False:
           self._log_debug('Error when updating virtual sensor {0}: {1}'.format('temp', result))

        # Write hum sensor
        result = json.loads(self.webinterface.set_sensor_status(status=json.dumps({'id':self._sensors_ids['hum'], 'value':sensors['hum']})))
        if result['success'] is False:
           self._log_debug('Error when updating virtual sensor {0}: {1}'.format('hum', result))

        # Write aqi senson
        result = json.loads(self.webinterface.set_sensor_status(status=json.dumps({'id':self._sensors_ids['aqi'], 'value':sensors['aqi']})))
        if result['success'] is False:
           self._log_debug('Error when updating virtual sensor {0}: {1}'.format('aqi', result))

    @om_expose
    def get_config_description(self):
        return json.dumps(OpenWeather.config_description)

    @om_expose
    def get_config(self):
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        config = json.loads(config)
        for key in config:
            if isinstance(config[key], basestring):
                config[key] = str(config[key])
        self._config_checker.check_config(config)
        self.write_config(config)
        self._config = config
        self._read_config()
        return json.dumps({'success': True})
