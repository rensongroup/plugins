"""
Dummy Error plugin
"""

import json
import logging
import six
from collections import deque

from plugins.base import (
    OMPluginBase,
    PluginConfigChecker,
    background_task,
    om_expose,
)

logger = logging.getLogger(__name__)


class DummyError(OMPluginBase):
    """
    Dummy Error plugin
    """

    name = "DummyError"
    version = "0.0.1"
    interfaces = [("config", "1.0")]

    default_config = {}

    def __init__(self, webinterface, connector):
        super(DummyError, self).__init__(webinterface=webinterface, connector=connector)
        logger.info("Starting Dummy plugin {0}...".format(DummyError.version))
        self.config_description = [
            {
                "name": "thermostat_config",
                "type": "section",
                "description": "Thermostat config for error simulation",
                "repeat": True,
                "min": 0,
                "content": [
                    {
                        "name": "thermostat_id",
                        "type": "int",
                        "description": "The id of the thermostat to simulate an error for",
                    },
                    {
                        "name": "errors",
                        "type": "section",
                        "repeat": True,
                        "min": 1,
                        "content": [
                            {
                                "name": "error_code",
                                "type": "str",
                            },
                            {
                                "name": "error_description",
                                "type": "str",
                            },
                            {
                                "name": "severity",
                                "type": "enum",
                                "choices": [
                                    "WARNING",
                                    "ERROR",
                                    "CRITICAL",
                                ],
                                "description": "The severity of the error",
                            },
                            {
                                "name": "error_specific_info",
                                "type": "str",
                                "description": "Additional information about the error in JSON format",
                            },
                        ],
                    },
                ],
            },
            {
                "name": "hotwater_config",
                "type": "section",
                "description": "Hotwater config for error simulation",
                "repeat": True,
                "min": 0,
                "content": [
                    {
                        "name": "hotwater_id",
                        "type": "int",
                        "description": "The id of the hot water to simulate an error for",
                    },
                    {
                        "name": "errors",
                        "type": "section",
                        "repeat": True,
                        "min": 1,
                        "content": [
                            {
                                "name": "error_code",
                                "type": "str",
                            },
                            {
                                "name": "error_description",
                                "type": "str",
                            },
                            {
                                "name": "severity",
                                "type": "enum",
                                "choices": [
                                    "WARNING",
                                    "ERROR",
                                    "CRITICAL",
                                ],
                                "description": "The severity of the error",
                            },
                            {
                                "name": "error_specific_info",
                                "type": "str",
                                "description": "Additional information about the error in JSON format",
                            },
                        ],
                    },
                ],
            },
            {
                "name": "report_errors",
                "type": "bool",
                "default": True,
                "description": "Whether to report errors to the gateway",
            },
            {
                "name": "clear_errors",
                "type": "bool",
                "default": False,
                "description": "Whether to clear errors on the gateway",
            }
        ]
        self._config = self.read_config(DummyError.default_config)
        self._config_checker = PluginConfigChecker(self.config_description)

        self._metrics_queue = deque()
        self._wants_registration = True

        self.connector.hot_water.subscribe_status_event(
            self.handle_hot_water_status, version=1
        )
        self.connector.thermostat.subscribe_status_event(
            self.handle_thermostat_status, version=2
        )

        self._hot_water_dtos = {} # "hotwater_id": List[error]
        self._thermostat_dtos = {} # "thermostat_id": List[error]

        logger.info("Started Dummy Error plugin {0}".format(DummyError.version))

    @om_expose
    def get_config_description(self):
        logger.info("Fetching config description")
        return json.dumps(self.config_description)

    @om_expose
    def get_config(self):
        logger.info("Fetching configuration")
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        logger.info("Saving configuration...")
        data = json.loads(config)
        self._save_config(data)
        self._wants_registration = True
        logger.info("Saving configuration... Done")
        return json.dumps({"success": True})

    def _save_config(self, config):
        for key in config:
            if isinstance(config[key], six.string_types):
                config[key] = str(config[key])
        self._config_checker.check_config(config)
        self._config = config
        self.write_config(config)

    @background_task
    def loop(self):
        while True:
            try:
                if self._config.get("report_errors", False):
                    logger.info("Reporting errors...")
                    self._register_entities()
                    self.report_hotwater_errors()
                    self.report_thermostat_errors()
                    self._config["report_errors"] = False  # Only send errors once
                    self.write_config(self._config)
            except Exception as e:
                logger.exception(f"Error while reporting errors: {e}")
            try:
                if self._config.get("clear_errors", False):
                    logger.info("Clearing all errors...")
                    self.clear_all_errors()
                    self._config["clear_errors"] = False  # Only clear errors once
                    self.write_config(self._config)
            except Exception as e:
                logger.exception(f"Error while clearing errors: {e}")

    def _register_entities(self):
        # register a hot waters
        hotwater_error_configs = self._config.get("hotwater_config", [])
        for hotwater_config in hotwater_error_configs:
            logger.info("Registering hot water errors...")
            hotwater_id = hotwater_config.get("hotwater_id")
            if hotwater_id is None:
                logger.error("Hot water id is missing")
                continue
            try:
                errors = hotwater_config.get("errors", [])
                self._hot_water_dtos[hotwater_id] = errors
            except Exception:
                logger.exception(f"Error registering hot_water: {hotwater_id}")

        # load thermostat
        thermostat_error_configs = self._config.get("thermostat_config", [])
        logger.info("Registering thermostat errors...")
        for thermostat_error_config in thermostat_error_configs:
            thermostat_id = thermostat_error_config.get("thermostat_id")
            if thermostat_id is None:
                logger.error("Thermostat id is missing")
                continue
            try:
                thermostat_errors = thermostat_error_config.get("errors", [])
                self._thermostat_dtos[thermostat_id] = thermostat_errors
            except Exception:
                logger.exception(f"Error registering thermostat {thermostat_id}")

    
    def report_thermostat_errors(self):
        for thermostat_id, errors in self._thermostat_dtos.items():
            for error in errors:
                try:
                    code = error.get("error_code", "UNKNOWN")
                    description = error.get("error_description")
                    severity = error.get("severity", "WARNING")
                    # error_specific_info = json.loads(error.get("error_specific_info", '{"test":"value"}'))
                    self.connector.thermostat.report_error(
                        id=thermostat_id,
                        code=code,
                        description=description,
                        severity=severity,
                        reporter="PLUGIN",
                        error_specific_info={}
                    )
                except Exception as e:
                    logger.exception(f"Error reporting thermostat {thermostat_id} error: {error} exception: {e}")

    def report_hotwater_errors(self):
        for hotwater_id, errors in self._hot_water_dtos.items():
            for error in errors:
                try:
                    code = error.get("error_code", "UNKNOWN")
                    description = error.get("error_description")
                    severity = error.get("severity", "WARNING")
                    # error_specific_info = json.loads(error.get("error_specific_info", '{"test":"value"}'))
                    self.connector.hot_water.report_error(
                        id=hotwater_id,
                        code=code,
                        description=description,
                        severity=severity,
                        reporter="PLUGIN",
                        error_specific_info={}
                    )
                except Exception as e:
                    logger.exception(f"Error reporting hot water {hotwater_id} error: {error} exception: {e}")


    def clear_all_errors(self):
        for hotwater_id in self._hot_water_dtos.keys():
            try:
                self.connector.hot_water.clear_errors_all(hotwater_id)
            except Exception as e:
                logger.exception(f"Error clearing hot water {hotwater_id} errors: {e}")
        for thermostat_id in self._thermostat_dtos.keys():
            try:
                self.connector.thermostat.clear_errors_all(thermostat_id)
            except Exception as e:
                logger.exception(f"Error clearing thermostat {thermostat_id} errors: {e}")

    def fetch_all_errors(self):
        all_errors = {}
        for hotwater_id in self._hot_water_dtos.keys():
            try:
                errors = self.connector.hot_water.get_errors(hotwater_id)
                all_errors[f"hotwater_{hotwater_id}"] = [error.json() for error in errors]
            except Exception as e:
                logger.exception(f"Error fetching hot water {hotwater_id} errors: {e}")
        for thermostat_id in self._thermostat_dtos.keys():
            try:
                errors = self.connector.thermostat.get_errors(thermostat_id)
                all_errors[f"thermostat_{thermostat_id}"] = [error.json() for error in errors]
            except Exception as e:
                logger.exception(f"Error fetching thermostat {thermostat_id} errors: {e}")
        
        logger.info(f"Fetched all errors: {all_errors}")

    
    @staticmethod
    def handle_hot_water_status(event):
        logger.info(
            "Received hot_water status from gateway: {0} {1}".format(
                event.data["id"],
                event.data["errors"],
            )
        )

    @staticmethod
    def handle_thermostat_status(event):
        logger.info(
            "Received thermostat status from gateway: {0} {1}".format(
                event.data["id"],
                event.data["errors"],
            )
        )