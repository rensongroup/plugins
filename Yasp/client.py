import enum
import json
import logging
import os

import requests
# Disable HTTPS warnings becasue of self-signed HTTPS certificate on the SMA inverter
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logger = logging.getLogger(__name__)


class Value(enum.Enum):
    PV_POWER = '6100_0046C200'
    PV_CURRENT = '6380_40452100'
    PV_VOLTAGE = '6380_40451F00'
    TOTAL_YIELD = '6400_00260100'
    DAILY_YIELD = '6400_00262200'


class Client:
    def _fetch(self, keys):
        raise NotImplementedError()

    def fetch_values(self, *keys):
        for device_serial, data in self._fetch(keys).items():
            for key, value in _extract(keys, data):
                yield {'external_id': device_serial, 'type': key, 'value': value}


def _extract(keys, data):
    for key in keys:
        values = data[key.value]
        if '1' in values:
            values = values['1']
        elif len(values) == 1:
            values = next(iter(values.values()))
        else:
            continue
        yield (key, values[0]['val'])


class WebConnect(Client):
    def __init__(self, url, password):
        self._url = url
        self._password = password
        self._session_id = ''

    def _fetch(self, keys):
        while True:
            endpoint = '{0}/dyn/getValues.json?sid={1}'.format(self._url, self._session_id)
            response = requests.post(endpoint,
                                     json={'destDev': [], 'keys': [k.value for k in keys]},
                                     timeout=10,
                                     verify=False).json()
            if response.get('err') == 401:
                self._login()
                continue
            break
        if 'result' not in response or len(response['result']) != 1:
            raise RuntimeError('Unexpected response: {0}'.format(response))
        return response['result']

    def _login(self):
        endpoint = '{0}/dyn/login.json'.format(self._url)
        response = requests.post(endpoint,
                                 json={'right': 'usr',
                                       'pass': self._password},
                                 verify=False).json()
        if 'result' in response and 'sid' in response['result']:
            self._session_id = response['result']['sid']
        else:
            error_code = response.get('err', 'unknown')
            if error_code == 503:
                raise RuntimeError('Maximum amount of sessions')
            raise RuntimeError('Could not login: {0}'.format(error_code))
        return response


class DummyWebConnect(Client):
    def __init__(self, path):
        with open(os.path.abspath(os.path.join(__file__, '..', path))) as fd:
            self._data = json.load(fd)

    def _fetch(self, keys):
        return self._data['result']


if __name__ == '__main__':
    for path in ('data/single_phase.json', 'data/three_phase.json'):
        client = DummyWebConnect(path)
        for x in client.fetch_values(Value.TOTAL_YIELD):
            print(x)
