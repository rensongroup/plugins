"""
A plugin to let two Gateways work together
"""

import six
import copy
import time
import requests
import json
from plugins.base import om_expose, output_status, OMPluginBase, PluginConfigChecker, background_task
from threading import Thread
import logging

logger = logging.getLogger(__name__)


class Syncer(OMPluginBase):
    """
    A syncer plugin to let a GW work together with n other GWs. Depending on the config, this GW will follow/set values
    (shutters, outputs, sensors) of other linked GWs.
    """

    name = 'Syncer'
    version = '0.0.10'
    interfaces = [('config', '1.0')]

    config_description = [{
        'name': 'local_name',
        'type': 'str',
        'description': 'Optional name field of the local GW.'
    },
        {
            'name': 'polling_interval',
            'type': 'int',
            'description': 'Interval (in seconds) to check remote sensor changes. (default 60)'
        },
        {
            'name': 'gateways',
            'type': 'section',
            'description': 'Other Gateways to sync.',
            'repeat': True,
            'min': 1,
            'content': [{
                'name': 'gateway_ip',
                'type': 'str',
                'description': 'The IP address of the other Gateway.'
            },
                {
                    'name': 'remote_name',
                    'type': 'str',
                    'description': 'Optional name field of the remote GW.'
                },
                {
                    'name': 'username',
                    'type': 'str',
                    'description': 'The (local) username for the other Gateway.'
                },
                {
                    'name': 'password',
                    'type': 'str',
                    'description': 'The (local) password for the other Gateway.'
                },
                {
                    'name': 'mappings',
                    'type': 'section',
                    'repeat': True,
                    'min': 1,
                    'content': [{
                        'name': 'type',
                        'type': 'nested_enum',
                        'choices': [{
                            'value': 'sensor',
                            'content': [{
                                'name': 'remote_sensor_id',
                                'description': "Mapping between local sensors and remote sensors. \nThe "
                                               "remote sensor should exist.\n Remote updates local.",
                                'type': 'int'
                            }]
                        },
                            {
                                'value': 'input',
                                'content': [{

                                    'name': 'local_input_id',
                                    'type': 'int'
                                },
                                    {
                                        'name': 'remote_input_id',
                                        'description': 'Mapping between local inputs and remote inputs.\nLocal updates remote.',
                                        'type': 'int'
                                    }]
                            },
                            {
                                'value': 'output',
                                'content': [{
                                    'name': 'local_output_id',
                                    'type': 'int'
                                },
                                    {
                                        'name': 'remote_output_id',
                                        'description': 'Mapping between local outputs and remote outputs.\nLocal updates remote.',
                                        'type': 'int'
                                    }]
                            },
                            {
                                'value': 'shutter',
                                'content': [{
                                    'name': 'local_shutter_id',
                                    'type': 'int'
                                },
                                    {
                                        'name': 'remote_shutter_id',
                                        'description': 'Mapping between local shutters and remote shutter. \nLocal updates remote.',
                                        'type': 'int'
                                    },
                                    {
                                        'name': 'is_shutter_group',
                                        'type': 'bool',
                                        'description': 'True => remote_shutter_id refers to the id of a shutter group\nFalse => remote_shutter_id refers to the id of a shutter (default False)'
                                    },
                                    {
                                        'name': 'reversed',
                                        'type': 'bool',
                                        'description': 'True => up=up, down=down\nFalse => up=down, down=up (default True)'
                                    }]
                            }]
                    }]
                }]
        }]

    default_config = {
        "polling_interval": 60
    }

    def __init__(self, webinterface, connector):
        super(Syncer, self).__init__(webinterface=webinterface, connector=connector)
        logger.info('Starting Syncer plugin...')

        self._config = self.read_config(Syncer.default_config)
        self._config_checker = PluginConfigChecker(Syncer.config_description)

        self._polling_interval = 60
        self._gateways = {}
        self._name = ''
        self._mappings = {}
        self._local_confs = {}
        self._enabled = False
        self._old_conf_deleted = True
        thread = Thread(target=self._process_config)
        thread.start()

        self.connector.input.subscribe_status_event(handler=self.handle_input_status, version=2)
        self.connector.output.subscribe_status_event(handler=self.handle_output_status, version=2)
        self.connector.shutter.subscribe_status_event(handler=self.handle_shutter_status, version=2)

        logger.info("Started Syncer plugin")

    def _process_config(self):
        while True:
            if not self._old_conf_deleted:
                time.sleep(5)
                continue
            self._enabled = False
            self._polling_interval = self._config.get('polling_interval', 60)
            self._name = self._config.get('local_name', '')
            self._gateways = {}
            self._mappings = {
                "output": {},
                "input": {},
                "shutter": {}
                }

            for obj_type in ["input", "output", "shutter"]:
                method = f"get_{obj_type}_configurations"
                local_conf = json.loads(getattr(self.webinterface, method)()).get("config")
                short_local_conf = []
                for obj in local_conf:
                    short_obj = {
                        "id": obj.get("id"),
                        "name": obj.get("name")
                        }
                    if obj_type == "output":
                        short_obj["type"] = obj.get("type")
                    short_local_conf.append(short_obj)
                self._local_confs[obj_type] = short_local_conf

            for gateway in self._config.get('gateways', ''):
                self.process_gw_config(gateway)
                self.process_mapping_config(gateway)

            for obj_type in ["input", "output", "shutter"]:
                logger.info(f"{obj_type.capitalize()}  mapping: {self._mappings.get(obj_type)}")

            _gateways = copy.deepcopy(self._gateways)
            for gateway in _gateways.values():
                gateway.pop("headers")
                gateway.pop("remote_confs")
                if gateway.get("enabled"):
                    self._enabled = True
            logger.info(f"Gateways: {_gateways}")
            del _gateways

            logger.info('Syncer is {0}'.format('enabled' if self._enabled else 'disabled'))
            break

    def process_gw_config(self, gw):
        ip = gw.get('gateway_ip', '')
        username = gw.get('username', '')
        password = gw.get('password', '')

        gw_name = gw.get('remote_name')

        if gw_name in ['', None]:
            gw_name = ip

        headers = {
            'X-Requested-With': 'OpenMotics plugin: Syncer'
        }
        endpoint = 'https://{0}'.format(ip)

        enabled = '' not in [ip, username, password]

        self._gateways[ip] = {
            'ip': ip,
            'name': gw_name,
            'username': username,
            'password': password,
            'headers': headers,
            'endpoint': endpoint,
            'enabled': enabled
        }

        try:
            self._call_remote("get_status", self._gateways[ip])
        except Exception as ex:
            logger.info(f"Could not connect to GW {ip}: {ex}")

    def process_mapping_config(self, gw):
        ip = gw.get('gateway_ip')
        remote_confs = {}
        for obj_type in ["shutter", "sensor", "output", "input"]:
            remote_conf = self._call_remote(f'get_{obj_type}_configurations', self._gateways[ip]).get('config')
            short_remote_conf = []
            for obj in remote_conf:
                short_obj = {"id": obj.get("id"),
                             "initial_name": obj.get("name")}
                if obj_type == "sensor":
                    short_obj.update({"external_id": obj.get("external_id"),
                                      "physical_quantity": obj.get("physical_quantity"),
                                      "unit": obj.get("unit"),
                                      "name": obj.get("name")})
                elif obj_type == "shutter":
                    short_obj.update({"group_1": obj.get("group_1"),
                                      "group_2": obj.get("group_2")})
                elif obj_type == "output":
                    short_obj["type"] = obj.get("type")
                short_remote_conf.append(short_obj)
            remote_confs[obj_type] = short_remote_conf
        self._gateways[ip]["remote_confs"] = remote_confs

        sensor_mapping = {}
        for entry in gw.get("mappings", []):
            try:
                obj_type = entry.get("type")[0]
                pi_conf = entry.get("type")[1]
                if obj_type == "sensor":
                    sensor_mapping = self.process_sensor_config(pi_conf, ip, sensor_mapping)
                elif obj_type in [
                    "output",
                    "input",
                    "shutter"
                                  ]:
                    method = f"process_{obj_type}_config"
                    getattr(self, method)(pi_conf, ip)

            except Exception as ex:
                logger.info(ex)

        self._gateways[ip]["sensor_mapping"] = sensor_mapping

    def process_sensor_config(self, pi_sensor_conf, ip, current_mapping):
        # Specific for each remote GW as this is remote 2 local. A loop over every remote GW will occur and this
        # will give our local sensor_dto if a value has changed.
        gw_name = self._gateways.get(ip).get('name')
        remote_sensor_conf = self._gateways.get(ip).get('remote_confs').get('sensor')

        try:
            remote_sensor_id = pi_sensor_conf.get('remote_sensor_id', -1)
            if remote_sensor_id < 0:
                raise RuntimeError("Please give valid sensor ids")
            for remote_sensor in remote_sensor_conf:
                if remote_sensor.get('id') == remote_sensor_id:
                    external_id = f"syncer/{ip}/{remote_sensor.get('external_id')}"
                    sensor = self.connector.sensor.register(external_id=external_id,
                                                            physical_quantity=remote_sensor.get(
                                                                'physical_quantity'),
                                                            unit=remote_sensor.get('unit'),
                                                            name=f"{gw_name}/{remote_sensor.get('name')}")
                    current_mapping[remote_sensor_id] = {"sensor_dto": sensor}
        except Exception as ex:
            logger.exception(f'Could not load sensor mapping for GW with ip {ip}: {ex}')
        return current_mapping

    def process_output_config(self, pi_output_conf, ip):
        self.process_io_config(pi_output_conf, ip, 'output')

    def process_input_config(self, pi_input_conf, ip):
        self.process_io_config(pi_input_conf, ip, 'input')

    def process_io_config(self, pi_io_conf, ip, obj_type):
        # Global output_mapping variable because this is local 2 remote, thus if local output changes -> mapping to
        # remote outputs will happen. One specific local output changes to different remote output changes
        local_io_conf = self._local_confs.get(obj_type)
        mapping = self._mappings.get(obj_type)

        # Organise local output data and their mapping with one or more remote outputs by making use of a dict
        try:
            local_id = int(pi_io_conf.get(f'local_{obj_type}_id', -1))
            remote_id = int(pi_io_conf.get(f'remote_{obj_type}_id', -1))
            if local_id < 0 or remote_id < 0:
                raise RuntimeError(f"Please give valid {obj_type} ids")
            if local_io_conf[local_id].get("type", "") == 127 and obj_type == "output":
                raise RuntimeError(
                    f"Skipped because {local_id} is a shutter, please include it in the shutter config instead")
            method = f"get_{obj_type}_status"
            state = json.loads(getattr(self.webinterface, method)()).get("status")[local_id]
            config = local_io_conf[local_id]
            if local_id not in mapping.keys():
                mapping[local_id] = {
                    'remotes': [],
                    'config': config,
                    'state': state
                }

            local_name = config.get('name') if config.get("name") != "" else local_id

            mapping[local_id]['remotes'].append({
                'remote': remote_id,
                'gw': ip,
                'name': f"{self._name}/{local_name}"
            })

            self._mappings[obj_type] = mapping

        except Exception as ex:
            raise RuntimeError(f'Could not load {obj_type} mapping for GW with ip {ip}: {ex}')

        try:
            self.update_remote_config(obj_type=obj_type, ip=ip, remote_id=remote_id, local_name=local_name)
        except Exception as ex:
            raise RuntimeError(f"Error while updating remote {obj_type} config: {ex}")

        try:
            self.update_remote_io_state(obj_type=obj_type, ip=ip, remote_id=remote_id, state=state)
        except Exception as ex:
            raise RuntimeError(f"Error while updating remote {obj_type} state: {ex}")

    def process_shutter_config(self, pi_shutter_conf, ip):
        # Global shutter mapping
        local_shutter_confs = self._local_confs.get("shutter")
        local_output_conf = self._local_confs.get("output")
        mapping = self._mappings.get("shutter")

        # Organise local shutter data and their mapping with one or more remote shutters by making use of a dict
        try:
            local_id = int(pi_shutter_conf.get('local_shutter_id', -1))
            remote_id = int(pi_shutter_conf.get('remote_shutter_id', -1))
            is_shutter_group = bool(pi_shutter_conf.get('is_shutter_group', False))
            reverse = bool(pi_shutter_conf.get("reversed", False))
            if local_id < 0 or remote_id < 0:
                raise RuntimeError("Please give valid shutter ids")
            if local_output_conf[local_id * 2].get("type") != 127 or local_output_conf[local_id * 2 + 1].get(
                    "type") != 127:
                raise RuntimeError(
                    f"Skipped because {local_id} is not a shutter, please include it in the output config instead")

            config = local_shutter_confs[local_id]
            local_state = json.loads(self.webinterface.get_shutter_status()).get("status")[local_id]
            if local_id not in mapping.keys():
                mapping[local_id] = {
                    'remotes': [],
                    'config': config,
                    'state': local_state
                }

            local_name = config.get("name") if config.get("name") not in ["", None] else local_id

            mapping[local_id]['remotes'].append({
                'remote': remote_id,
                'gw': ip,
                'name': f"{self._name}/{local_name}",
                'reverse': reverse,
                'is_shutter_group': is_shutter_group
            })
            self._mappings["shutter"] = mapping
        except Exception as ex:
            raise RuntimeError(f'Could not load shutter mapping for GW with ip {ip}: {ex}')

        try:
            self.update_remote_config(obj_type=f"shutter{'group' if is_shutter_group else ''}",
                                      ip=ip, remote_id=remote_id, local_name=local_name)
        except Exception as ex:
            raise RuntimeError(f"Error while updating remote shutter config: {ex}")

        try:
            self.update_remote_shutter_state(ip=ip, remote_id=remote_id, state=local_state, reverse=reverse,
                                             is_shutter_group=is_shutter_group)
        except Exception as ex:
            raise RuntimeError(f"Error while updating remote shutter state: {ex}")

    def update_remote_config(self, obj_type, ip, remote_id, local_name=None, restore=False):
        configs_to_set = []
        if obj_type == "shuttergroup":
            obj_type = "shutter"
            remote_conf = self._gateways.get(ip).get('remote_confs').get(obj_type)
            for shutter in remote_conf:
                if shutter.get("group_1") == remote_id or shutter.get("group_2") == remote_id:
                    configs_to_set.append(shutter)
        else:
            remote_conf = self._gateways.get(ip).get('remote_confs').get(obj_type)
            configs_to_set = [remote_conf[remote_id]]

        logger.debug(f"Confs_to_set: {configs_to_set}")

        for conf in configs_to_set:
            remote_id = conf.get("id")
            try:
                old_name = conf.get('initial_name')
                initial_name = old_name
                if ' (also controlled by syncer plugin ' in old_name:
                    initial_name = old_name.split(' (also controlled by syncer plugin ')[0]

                if not restore:
                    name_to_set = f"{initial_name} (also controlled by syncer plugin {self._name}/{local_name})"

                else:
                    name_to_set = initial_name

                remote_conf[remote_id]['name'] = name_to_set
                config = {'id': remote_id, 'name': name_to_set}
                if obj_type == "shutter":
                    config.update({"group_1": conf.get("group_1"), "group_2": conf.get("group_2")})
                self._call_remote(f"set_{obj_type}_configuration?config={json.dumps(config)}", gateway=self._gateways[ip])
                logger.debug(f"Set name of remote {obj_type} {ip}/{remote_id} from {old_name} to {name_to_set}")
            except Exception as ex:
                raise RuntimeError(f'Could not set configuration of remote {obj_type} {ip}/{remote_id}: {ex}')
        self._gateways[ip]['remote_confs'][obj_type] = remote_conf

    def update_remote_io_state(self, obj_type, ip, remote_id, state):
        params = {
            "id": remote_id,
            "is_on": state.get('status')
            }

        if obj_type == "output":
            params.update({
                "dimmer": state.get('dimmer', ""),
            })

        try:
            self._call_remote(api_call=f"set_{obj_type}", params=params, gateway=self._gateways[ip])
            logger.info(
                f"Updated remote {obj_type} {remote_id} on GW {self._gateways[ip].get('name')} to {'on' if state.get('status') in [1, True] else 'off'} {'' if state.get('dimmer') in ['', None] else '(' + str(state.get('dimmer')) + '%)'}")
        except Exception as ex:
            raise RuntimeError(
                f"Could not set state of remote {obj_type} {self._gateways[ip].get('name')}/{remote_id}: {ex}")

    def update_remote_shutter_state(self, ip, remote_id, state, reverse, is_shutter_group):
        try:
            do_call = True
            print_state = ""
            if state == "stopped":
                remote_state = "stop"
                print_state = "stopping"
            elif state == "going_up":
                remote_state = "up" if not reverse else "down"
            elif state == "going_down":
                remote_state = "up" if reverse else "down"
            else:
                remote_state = ""
                do_call = False

            if remote_state in ["up", "down"]:
                print_state = f"going {remote_state}"

            if is_shutter_group:
                group = "group_"
            else:
                group = ""

            if do_call:
                self._call_remote(api_call=f"do_shutter_{group}{remote_state}", params={
                    "id": remote_id
                }, gateway=self._gateways[ip])

                logger.info(
                    f"Updated remote shutter{group[:-1]} {remote_id} on GW {self._gateways[ip].get('name')} to {print_state}")
        except Exception as ex:
            raise RuntimeError(
                f"Could not set state of remote shutter{'group' if is_shutter_group else ''} {self._gateways[ip].get('name')}/{remote_id}: {ex}")

    @background_task
    def run(self):
        while True:
            if not self._enabled:
                time.sleep(10)
                continue
            try:
                # Sync sensor values:
                for gateway in self._gateways.values():
                    sensor_mapping = gateway.get('sensor_mapping')
                    if sensor_mapping == {}:
                        continue
                    sensor_status = self._call_remote('get_sensor_status', gateway).get('status')

                    for sensor in sensor_status:
                        local_sensor = sensor_mapping.get(sensor.get('id'), None)
                        if local_sensor is None:
                            logger.debug(f"Did not update sensor value because there is remote sensor coupled to this sensor {sensor}")
                            continue
                        if sensor.get('value', None) == local_sensor.get('value'):
                            logger.debug(f"Did not update sensor value because there is no temperature change")
                            continue
                        self.connector.sensor.report_status(sensor=local_sensor.get('sensor_dto'),
                                                           value=sensor.get('value'))
                        sensor_mapping[sensor['id']]['value'] = sensor.get('value')
                        logger.info(
                            f"Updated {local_sensor.get('sensor_dto').name} with value {sensor.get('value')} from remote sensor ({gateway.get('ip')}) with id {sensor.get('id')}")
            except Exception as ex:
                logger.exception('Error while syncing sensors: {0}'.format(ex))
            time.sleep(self._polling_interval)

    def handle_shutter_status(self, minimal_event, _):
        if not self._enabled:
            return
        shutter_mapping = self._mappings.get("shutter")
        for key, value in shutter_mapping.items():
            if value.get('state') != minimal_event[key]:
                state = minimal_event[key]
                shutter_mapping[key]['state'] = state
                logger.info(f"Shutter change detected: {key} {state}")
                logger.info(f"Checking if remote shutters should be updated...")
                try:
                    for remote_shutter in value.get('remotes'):
                        ip = remote_shutter.get('gw')
                        remote_id = remote_shutter.get('remote')
                        reverse = remote_shutter.get('reverse')
                        is_shutter_group = remote_shutter.get('is_shutter_group')
                        self.update_remote_shutter_state(ip=ip, remote_id=remote_id, state=state, reverse=reverse,
                                                         is_shutter_group=is_shutter_group)
                except Exception as ex:
                    logger.exception(f"Error processing shutter event {state} of shutter {key}: {ex}")

    def handle_output_status(self, event):
        new_event = {'id': event.get('id'),
                     'status': bool(event.get('status').get('on')),
                     'dimmer': event.get('status').get('value', "")}
        self.handle_io_status(obj_type="output", event=new_event)

    def handle_input_status(self, event):
        new_event = {'id': event.get('input_id'),
                     'status': bool(event.get('status'))}
        self.handle_io_status(obj_type="input", event=new_event)

    def handle_io_status(self, obj_type, event):
        if not self._enabled or event.get('id') not in self._mappings.get(obj_type):
            return
        local_id = event.get('id')
        event.pop("id")

        self._mappings[obj_type][local_id]["state"] = event
        remote_ios = self._mappings.get(obj_type).get(local_id).get('remotes')
        logger.info(
            f"{obj_type.capitalize()} change detected: {local_id} {'on' if event.get('status') in [1, True] else 'off'} {'' if event.get('dimmer') in ['', None] else '(' + str(event.get('dimmer')) + '%)'}")
        logger.info(f"Updating remote {obj_type}s: {remote_ios}")

        try:
            for remote_io in remote_ios:
                ip = remote_io.get('gw')
                remote_id = remote_io.get('remote')
                self.update_remote_io_state(obj_type=obj_type, ip=ip, remote_id=remote_id, state=event)
        except Exception as ex:
            logger.exception(f"Error processing output event {event}: {ex}")

    def _call_remote(self, api_call, gateway, params=None, method="GET"):
        # TODO: If there's an invalid_token error, call self._login() and try this call again
        retries = 0
        while retries < 3:
            try:
                if gateway.get("headers", {}).get("Authorization") is None:
                    self._login(gateway)
                response = requests.request(method=method,
                                            url=f"{gateway.get('endpoint')}/{api_call}",
                                            params=params,
                                            verify=False,
                                            headers=gateway.get('headers'))
                response_data = json.loads(response.text)
                if response_data.get('success', False) is False:
                    if "Invalid session token is passed" in response_data.get('msg') or "invalid_token" in response_data.get('msg'):
                        logger.info('Token expired')
                        gateway["headers"]["Authorization"] = None
                        retries += 1
                        continue
                    else:
                        raise RuntimeError(
                            'Could not execute API call {0}: {1}'.format(api_call, response_data.get('msg',
                                                                                                     'Unknown error')))
                return response_data
            except Exception as ex:
                logger.exception('Unexpected error during API call {0}: {1}'.format(api_call, ex))
                return None

    def _login(self, gateway):
        try:
            response = requests.get(f"{gateway.get('endpoint')}/login",
                                    params={
                                        'username': gateway.get("username"),
                                        'password': gateway.get("password"),
                                        'accept_terms': '1',
                                        'timeout': 60 * 60 * 24 * 30
                                    },
                                    verify=False,
                                    headers=gateway.get("headers"))
            response_data = json.loads(response.text)
            if response_data.get('success', False) is False:
                logger.error(f"Could not login to {gateway.get('ip')}: {response_data.get('msg', 'Unknown error')}")
            else:
                token = response_data.get('token')
                gateway["headers"]['Authorization'] = 'Bearer {0}'.format(token)
        except Exception as ex:
            logger.exception('Unexpected error during login: {0}'.format(ex))

    @om_expose
    def get_config_description(self):
        return json.dumps(Syncer.config_description)

    @om_expose
    def get_config(self):
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        logger.info("Saving configuration")
        thread = Thread(target=self._delete_old_config)
        thread.start()
        config = json.loads(config)
        for key in config:
            if isinstance(config[key], six.string_types):
                config[key] = str(config[key])
        self._config_checker.check_config(config)
        self._config = config
        thread = Thread(target=self._process_config)
        thread.start()
        self.write_config(config)
        return json.dumps({
            'success': True
        })

    def on_remove(self):
        self._delete_old_config()

    def _delete_old_config(self):
        self._old_conf_deleted = False
        for obj_type, values in self._mappings.items():
            for local_id, remotes in values.items():
                for remote in remotes.get("remotes"):
                    self.update_remote_config(obj_type=f"{obj_type}{'group' if bool(remote.get('is_shutter_group', False)) else ''}",
                                              ip=remote.get("gw"), remote_id=remote.get("remote"), restore=True)
        for ip, gw in self._gateways.items():
            for _, sensor_dto in gw.get('sensor_mapping', {}).items():
                """
                TODO: remove sensor_dto, this todo is blocked because removal of connectors is not yet implemented in BE.
                When removing the plugin, connectors will be deleted in the background, so not necessary in that case.
                """
                continue

        self._old_conf_deleted = True
