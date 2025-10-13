"""
An astronomical plugin, for providing the system with astronomical data (e.g. whether it's day or not, based on the sun's location)
"""

from __future__ import annotations

import six
import time
import requests
import logging
import json
from enum import Enum
from threading import Event
from datetime import datetime, timedelta
from .configuration import ConfigurationFactory
from plugins.base import om_expose, background_task, OMPluginBase, PluginConfigChecker
from collections import defaultdict, deque

import typing
if typing.TYPE_CHECKING:
    from typing import Deque


logger = logging.getLogger(__name__)

config_factory = ConfigurationFactory()

class AstroAction:
    """
    Represents an action that can be executed based on astronomical data.
    This class is used to encapsulate the details of the action, such as its type and parameters.
    """

    class ActionType(Enum):
        bit_clear = 'bit_clear'
        bit_set = 'bit_set'
        group_action = 'group_action'

    def __init__(self,
                 timestamp:datetime,
                 sun_location:str,
                 action_type:ActionType,
                 action_number: int):
        self.timestamp = timestamp  # The time when the action should be executed
        self.sun_location = sun_location  # The location of the sun (e.g., sunrise, sunset, etc.)
        self.action_type = action_type  # The type of action (e.g., group action, validation bit)
        self.action_number = action_number  # The identifier for the action

    def action_string(self, include_timestamp:bool=True):
        """
        Returns a string representation of the action, including its type and number.
        """
        timestamp_str = self.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        action_str = None
        if self.action_type == AstroAction.ActionType.bit_set:
            action_str =  f"Set Validation Bit {self.action_number}"
        elif self.action_type == AstroAction.ActionType.bit_clear:
            action_str =  f"Clear Validation Bit {self.action_number}"
        elif self.action_type == AstroAction.ActionType.group_action:
            action_str =  f"Execute Group Action {self.action_number}"

        if action_str:
            if include_timestamp:
                return f"{timestamp_str}: {action_str}"
            else:
                return action_str
        else:
            raise ValueError(f"Unknown action type: {self.action_type}")

    def __repr__(self):
        return f"AstroAction(timestamp={self.timestamp}, sun_location={self.sun_location}, " \
               f"action_type={self.action_type}, action_number={self.action_number})"

    def __str__(self):
        return self.__repr__()


class Astro(OMPluginBase):
    """
    An astronomical plugin, for providing the system with astronomical data (e.g. whether it's day or not, based on the sun's location)
    """

    name = 'Astro'
    version = '1.1.3'
    interfaces = [('config', '1.0')]

    config_description = config_factory.get_config_description()

    default_config = config_factory.get_default_configuration()

    def __init__(self, webinterface, connector):
        super(Astro, self).__init__(webinterface=webinterface, connector=connector)
        logger.info('Starting Astro plugin...')

        self._config = self.read_config(Astro.default_config)
        self._config_checker = PluginConfigChecker(Astro.config_description)
        self._parsed_config = None

        self._latitude = None
        self._longitude = None

        self._group_actions = {}
        self._bits = {}

        self._trigger_event = Event()
        self._action_queue: Deque[AstroAction] = deque()  # Queue for actions to be executed
        self._last_request_date = None
        self._last_api_response = None

        self._read_config()

        logger.info("Started Astro plugin")

    def _read_config(self):
        logger.info('Reading configuration...')
        self._parsed_config = config_factory.parse_configuration(self._config)

        try:
            import pytz
            from pytz import reference
            enabled = True
        except ImportError:
            logger.error('Could not import pytz')
            enabled = False

        if enabled:
            # Parse coordinates
            coordinates = self._parsed_config.coordinates
            self._latitude = coordinates.latitude
            self._longitude = coordinates.longitude
            self._print_coordinate_time()


        if enabled:
            group_actions = {}
            for entry in self._parsed_config.group_action_jobs:
                sun_location = entry.sun_location.value
                if not sun_location:
                    continue
                try:
                    group_action_id = int(entry.group_action_id)
                except ValueError:
                    continue
                try:
                    offset = int(entry.offset or 0)
                except ValueError:
                    offset = 0
                actions = group_actions.setdefault(sun_location, [])
                actions.append({'group_action_id': group_action_id,
                                'offset': offset})
                self._group_actions = group_actions

            bits = {}
            for entry in self._parsed_config.validation_jobs:
                sun_location = entry.sun_location.value
                if not sun_location:
                    continue
                action = entry.action.value or 'clear'
                try:
                    bit_id = int(entry.bit_id)
                except ValueError:
                    continue
                try:
                    offset = int(entry.offset or 0)
                except ValueError:
                    offset = 0
                actions = bits.setdefault(sun_location, [])
                actions.append({'bit_id': bit_id,
                                'action': action,
                                'offset': offset})
                self._bits = bits

        self._print_actions()
        self._enabled = enabled and (self._group_actions or self._bits)
        self._last_request_date = None
        logger.info('Astro is {0}abled'.format('en' if self._enabled else 'dis'))
        self._trigger_event.set()

    @staticmethod
    def _format_date(date, timezone=None):
        from pytz import reference

        if timezone is None:
            timezone = reference.LocalTimezone()
        if date.tzinfo is None:
            date = date.replace(tzinfo=reference.LocalTimezone())
        return date.astimezone(timezone).strftime('%Y-%m-%d %H:%M')

    @staticmethod
    def _format_offset(offset):
        return ' with a {0} min offset'.format(
            '+{0}'.format(offset) if offset > 0 else offset
        ) if offset else ''

    def _print_coordinate_time(self):
        import pytz

        now = datetime.now()
        logger.info('Location:')
        logger.info('* Latitude: {0} - Longitude: {1}'.format(self._latitude, self._longitude))
        logger.info('* Time: {0} local time, {1} UTC'.format(Astro._format_date(now),
                                                             Astro._format_date(now, timezone=pytz.UTC)))

    def _print_actions(self):
        sun_locations = set(self._group_actions.keys()) | set(self._bits.keys())
        if sun_locations:
            logger.info('Configured actions:')
        for sun_location in sun_locations:
            group_actions = self._group_actions.get(sun_location, [])
            bits = self._bits.get(sun_location, [])
            for entry in bits:
                logger.info('* At {0}{1}: {2} Validation Bit {3}'.format(
                    sun_location,
                    Astro._format_offset(entry['offset']),
                    entry['action'].capitalize(),
                    entry['bit_id']
                ))
            for entry in group_actions:
                logger.info('* At {0}{1}: Execute Automation {2}'.format(
                    sun_location,
                    Astro._format_offset(entry['offset']),
                    entry['group_action_id']
                ))

    def _convert(self, dt_string):
        import pytz

        if dt_string is None:
            return None
        try:
            date = datetime.strptime(dt_string, '%Y-%m-%dT%H:%M:%S+00:00')
            date = pytz.utc.localize(date)
            if date.year == 1970:
                return None
            return date
        except Exception as ex:
            logger.exception('Could not parse date {0}: {1}'.format(dt_string, ex))
            return None

    def _get_queue_string(self):
        """
        Returns a string representation of the action queue, showing the actions that are scheduled to be executed.
        """
        if not self._action_queue:
            return 'No actions in queue'
        actions_str =  ', '.join(['{}'.format(action.action_string()) for action in self._action_queue])
        return 'Actions in queue: {}'.format(actions_str)


    def _execute_action(self, action:AstroAction) -> None:
        if action.action_type == AstroAction.ActionType.group_action:
            group_action_id = action.action_number
            try:
                result = json.loads(self.webinterface.do_basic_action(action_type=2,
                                                                      action_number=group_action_id))
                if not result.get('success'):
                    raise RuntimeError(result.get('msg', 'Unknown error'))
                logger.info('* Executing Automation {0}: Done'.format(group_action_id))
            except Exception as ex:
                logger.error('* Executing Automation {0} failed: {1}'.format(group_action_id, ex))
        elif action.action_type in (AstroAction.ActionType.bit_set, AstroAction.ActionType.bit_clear):
            bit_id = action.action_number
            action_words = 'Setting' if action.action_type == AstroAction.ActionType.bit_set else 'Clearing'
            ba_action = 237 if action.action_type == AstroAction.ActionType.bit_set else 238
            try:
                result = json.loads(self.webinterface.do_basic_action(action_type=ba_action,
                                                                      action_number=bit_id))
                if not result.get('success'):
                    raise RuntimeError(result.get('msg', 'Unknown error'))
                logger.info('* {0} Validation Bit {1}: Done'.format(action_words, bit_id))
            except Exception as ex:
                logger.error('* {0} Validation Bit {1} failed: {2}'.format(action_words, bit_id, ex))


    def _build_execution_plan(self) -> None:
        """
        This method builds the execution plan based on the astronomical data and the configured actions.
        It checks the sunrise, sunset, and twilight times to determine when actions should be executed.

        First it requests the sunrise-sunset API for the current timestamps needed to trigger.
        Then it processes the data to create a plan of actions that will be executed at the appropriate times.
        """
        from pytz import reference
        now = datetime.now(reference.LocalTimezone())

        action_plan = defaultdict(list)

        # do not request data if we already have it for today
        # and rate limit the times tried to fetch data to max once per hour
        # if there are still actions in the queue, also do not request data
        if (self._last_request_date is not None and now - self._last_request_date < timedelta(hours=1)) or \
                self._action_queue:
            logger.debug('Data already fetched for today, skipping request')
            return

        data = None
        for _ in range(3):
            try:
                req = requests.get('http://api.sunrise-sunset.org/json?lat={0}&lng={1}&date={2}&formatted=0'.format(
                    self._latitude, self._longitude, now.strftime('%Y-%m-%d')
                ))
                if req.status_code != 200:
                    raise RuntimeError("Invalid request response")
                data = req.json()
                if data['status'] != 'OK':
                    raise RuntimeError(data['status'])
                break
            except RuntimeError as ex:
                logger.exception('Could not fetch or load data: {0}'.format(ex))
                continue
            except Exception as ex:
                logger.exception('Could not fetch or load data: {0}'.format(ex))

            time.sleep(10)

        # # Left in here for testing purposes, to simulate the API returning data
        # if data is not None:
        #     logger.info('Fetched data from sunrise-sunset API: {0}'.format(data))
        #     now_1_minute = now + timedelta(seconds=10) - timedelta(hours=2)
        #     now_2_minute = now + timedelta(seconds=20) - timedelta(hours=2)
        #     now_3_minute = now + timedelta(seconds=30) - timedelta(hours=2)
        #     data['results']['civil_twilight_end'] = now_1_minute.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        #     data['results']['nautical_twilight_end'] = now_2_minute.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        #     data['results']['astronomical_twilight_end'] = now_3_minute.strftime('%Y-%m-%dT%H:%M:%S+00:00')


        self._last_request_date = now
        self._last_api_response = data

        if self._last_api_response is None:
            logger.error('No data received from the sunrise-sunset API.')
            return

        try:
            field_map = {
                    'sunrise': 'sunrise',
                    'civil dawn': 'civil_twilight_begin',
                    'nautical dawn': 'nautical_twilight_begin',
                    'astronomical dawn': 'astronomical_twilight_begin',
                    'astronomical dusk': 'astronomical_twilight_end',
                    'nautical dusk': 'nautical_twilight_end',
                    'civil dusk': 'civil_twilight_end',
                    'sunset': 'sunset',
                    'solar noon': 'solar_noon',
                }
            for sun_location in set(self._group_actions.keys()) | set(self._bits.keys()):
                group_actions = self._group_actions.get(sun_location, [])
                bits = self._bits.get(sun_location, [])

                if not group_actions and not bits:
                    continue
                date = self._convert(self._last_api_response['results'].get(field_map.get(sun_location, 'x')))
                if date is None:
                    continue
                date = date.astimezone(reference.LocalTimezone())

                for entry in bits:
                    entry_date = date + timedelta(minutes=entry['offset'])
                    if entry_date < now:
                        continue
                    action = AstroAction.ActionType.bit_set if entry['action'] == 'set' else AstroAction.ActionType.bit_clear
                    task = AstroAction(
                            timestamp=entry_date,
                            sun_location=sun_location,
                            action_type=action,
                            action_number=entry['bit_id'])
                    self._action_queue.append(task)
                    action_plan[entry_date].append(task)

                for entry in group_actions:
                    entry_date = date + timedelta(minutes=entry['offset'])
                    if entry_date < now:
                        continue
                    task = AstroAction(
                            timestamp=entry_date,
                            sun_location=sun_location,
                            action_type=AstroAction.ActionType.group_action,
                            action_number=entry['group_action_id'])
                    action_plan[entry_date].append(task)
                    self._action_queue.append(task)

        except Exception as ex:
            logger.exception('Could not fetch or load data: {0}'.format(ex))
            logger.info("sleeping 5 seconds and retrying...")
            time.sleep(5)


        # sort the action queue by timestamp
        self._action_queue = deque(sorted(self._action_queue, key=lambda x: x.timestamp))

        logger.info('Execution plan built for {0}:'.format(self._last_request_date.strftime('%Y-%m-%d')))
        for timestamp, actions in sorted(action_plan.items(), key=lambda x: x[0]):
            act_str = ', '.join([action.action_string(include_timestamp=False) for action in actions])
            logger.info(f" * {timestamp.strftime('%Y-%m-%dT%H:%M:%S')}: {act_str}")



    def _run_actions(self):
        """
        This method checks if there are any actions scheduled based on the astronomical data
        and executes them accordingly.
        """

        if not self._last_request_date:
            self._action_queue.clear()
            self._build_execution_plan()

        if not self._action_queue:
            self._build_execution_plan()

        # check the same condition again, as the build_execution_plan might have added actions
        if not self._action_queue:
            return

        from pytz import reference
        now = datetime.now(reference.LocalTimezone())

        while True:
            if not self._action_queue:
                logger.info('No actions left in queue. Re-building execution plan.')
                break
            last_action = self._action_queue[0]
            if last_action.timestamp < now + timedelta(seconds=5):  # give it a 5 second buffer
                self._execute_action(last_action)
                self._action_queue.popleft()
            else:
                break


    @background_task
    def run(self):
        """
        The main loop of the plugin, which runs in a background thread.
        It checks if there are any pending actions based on the astronomical data
        and executes actions accordingly.
        """
        logger.info('Astro plugin run started')
        while True:
            self._trigger_event.wait(timeout=10)
            if self._trigger_event.is_set():
                self._trigger_event.clear()

            try:
                self._run_actions()
            except Exception as ex:
                logger.exception('Error in Astro plugin run loop: {0}'.format(ex))

    @om_expose
    def get_config_description(self):
        return json.dumps(Astro.config_description)

    @om_expose
    def get_config(self):
        return json.dumps(config_factory.get_json_configuration(self._parsed_config))

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
