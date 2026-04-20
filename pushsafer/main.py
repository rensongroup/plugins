"""
A Pushsafer (http://www.pushsafer.com) plugin for pushing events through Pushsafer
"""

import six
import collections
import time
import requests
import json
from threading import Thread
from plugins.base import om_expose, input_status, OMPluginBase, PluginConfigChecker
import logging

logger = logging.getLogger(__name__)


class Pushsafer(OMPluginBase):
    """
    A Pushsafer (http://www.pushsafer.com) plugin for pushing events through Pushsafer
    """

    name = 'Pushsafer'
    version = '2.1.4'
    interfaces = [('config', '1.0')]

    config_description = [{'name': 'privatekey',
                           'type': 'str',
                           'description': 'Your Private or Alias key.'},
                          {'name': 'input_mapping',
                           'type': 'section',
                           'description': 'The mapping between input_id and a given Pushsafer settings',
                           'repeat': True,
                           'min': 1,
                           'content': [{'name': 'input_id',
                                        'type': 'int',
                                        'description': 'The ID of the (virtual) input that will trigger the event.'},
                                       {'name': 'message',
                                        'type': 'str',
                                        'description': 'The message to be send.'},
                                       {'name': 'title',
                                        'type': 'str',
                                        'description': 'The title of message to be send.'},
                                       {'name': 'device',
                                        'type': 'str',
                                        'description': 'The device or device group id where the message to be send.'},
                                       {'name': 'icon',
                                        'type': 'str',
                                        'description': 'The icon which is displayed with the message (a number 1-181).'},
                                       {'name': 'iconcolor',
                                        'type': 'str',
                                        'description': 'The icons color e.g. #FF0000.'},
                                       {'name': 'sound',
                                        'type': 'int',
                                        'description': 'The notification sound of message (a number 0-62 or empty).'},
                                       {'name': 'vibration',
                                        'type': 'str',
                                        'description': 'How often the device should vibrate (a number 0-3 or empty).'},
                                       {'name': 'url',
                                        'type': 'str',
                                        'description': 'A URL or URL scheme: https://www.pushsafer.com/en/url_schemes'},
                                       {'name': 'urltitle',
                                        'type': 'str',
                                        'description': 'the URLs title'},
                                       {'name': 'time2live',
                                        'type': 'str',
                                        'description': 'Integer number 0-43200: Time in minutes after which message automatically gets purged.'}]},
                                       {'name': 'priority',
                                        'type': 'str',
                                        'description': 'Integer number: -2 = lowest priority, -1 = lower priority, 0 = normal priority, 1 = high priority, 2 = highest priority (Critical Alerts).'}]},
                                       {'name': 'retry',
                                        'type': 'str',
                                        'description': 'Integer number 60-10800: Time in seconds, after a message should resend.'}]}
                                       {'name': 'expire',
                                        'type': 'str',
                                        'description': 'Integer number 60-10800: Time in seconds, after the retry/resend should stop.'}]}
                                       {'name': 'confirm',
                                        'type': 'str',
                                        'description': 'Integer number 10-10800: Time in seconds after which a message should be sent again before it is confirmed.'}]}
                                       {'name': 'answer',
                                        'type': 'str',
                                        'description': 'Integer number: 1 = Answer is possible or 0 = Answer is not possible.'}]}
                                       {'name': 'answeroptions',
                                        'type': 'str',
                                        'description': 'predefined answer options divided by a pipe character e.g. Yes|No|Maybe'}]}
                                       {'name': 'answerforce',
                                        'type': 'str',
                                        'description': 'Integer number: Force Answer 1 = yes or 0 = no'}]}]

    default_config = {'privatekey': '', 'input_id': -1, 'message': '', 'title': 'OpenMotics', 'device': '', 'icon': '1', 'iconcolor': '', 'sound': '', 'vibration': '',
                      'url': '', 'urltitle': '', 'time2live': '', 'priority': '0', 'retry': '', 'expire': '', 'confirm': '', 'answer': '0', 'answeroptions': '', 'answerforce': '0'}

    def __init__(self, webinterface, connector):
        super(Pushsafer, self).__init__(webinterface=webinterface,
                                    connector=connector)

        logger.info('Starting Pushsafer plugin...')

        self._config = self.read_config(Pushsafer.default_config)
        self._config_checker = PluginConfigChecker(Pushsafer.config_description)

        self._cooldown = {}
        self._read_config()

        logger.info("Started Pushsafer plugin")

    def _read_config(self):
        self._privatekey = self._config['privatekey']
        self._mapping = self._config.get('input_mapping', [])

        self._endpoint = 'https://www.pushsafer.com/api'
        self._headers = {'Content-type': 'application/x-www-form-urlencoded',
                         'X-Requested-With': 'OpenMotics plugin: Pushsafer'}

        self._enabled = self._privatekey != '' and len(self._mapping) > 0
        logger.info('Pushsafer is {0}'.format('enabled' if self._enabled else 'disabled'))

    def convert(self, data):
        if isinstance(data, six.string_types):
            return str(data)
        elif isinstance(data, collections.Mapping):
            return dict(map(self.convert, data.items()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(self.convert, data))
        else:
            return data

    @input_status
    def input_status(self, status):
        now = time.time()
        if self._enabled is True:
            input_id = status[0]
            if self._cooldown.get(input_id, 0) > now - 10:
                logger.warning('Ignored duplicate Input in 10 seconds.')
                return
            data_send = False
            for mapping in self._mapping:
                if input_id == mapping['input_id']:
                    data = {'k': self._privatekey,
                            'm': mapping['message'],
                            't': mapping['title'],
                            'd': mapping['device'],
                            'i': mapping['icon'],
                            'c': mapping['iconcolor'],
                            's': mapping['sound'],
                            'v': mapping['vibration'],
                            'u': mapping['url'],
                            'ut': mapping['urltitle'],
                            'l': mapping['time2live'],
                            'pr': mapping['priority'],
                            're': mapping['retry'],
                            'ex': mapping['expire'],
                            'cr': mapping['confirm'],
                            'a': mapping['answer'],
                            'ao': mapping['answeroptions'],
                            'af': mapping['answerforce']}
                    thread = Thread(target=self._send_data, args=(data,))
                    thread.start()
                    data_send = True
            if data_send is True:
                self._cooldown[input_id] = now

    def _send_data(self, data):
        try:
            logger.info('Sending data')
            response = requests.post(url=self._endpoint,
                                     data=data,
                                     headers=self._headers,
                                     verify=False)
            if response.status_code != 200:
                logger.error('Got error response: {0} ({1})'.format(response.text, response.status_code))
            else:
                result = json.loads(response.text)
                if result['status'] != 1:
                    logger.error('Got error response: {0}'.format(result['error']))
                else:
                    logger.info('Got reply: {0}'.format(result['success']))
                    quotas = []
                    for data in result['available'].values():
                        device = data.keys()[0]
                        quotas.append('{0}: {1}'.format(device, data[device]))
                    logger.info('Remaining quotas: {0}'.format(', '.join(quotas)))
        except Exception as ex:
            logger.exception('Error sending')

    @om_expose
    def get_config_description(self):
        return json.dumps(Pushsafer.config_description)

    @om_expose
    def get_config(self):
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        config = json.loads(config)
        config = self.convert(config)
        self._config_checker.check_config(config)
        self._config = config
        self._read_config()
        self.write_config(config)
        return json.dumps({'success': True})
