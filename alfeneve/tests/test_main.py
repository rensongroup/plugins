import copy
import json
import sys
import threading
import requests
from unittest import TestCase
from unittest.mock import MagicMock, patch


def _get_plugin_class():
    plugin_base = MagicMock()
    plugin_base.OMPluginBase = object
    plugin_base.PluginConfigChecker = MagicMock
    plugin_base.background_task = lambda function: function
    plugin_base.om_expose = lambda *args, **kwargs: (
        args[0] if args and callable(args[0]) else (lambda function: function)
    )
    modules = {"plugins": MagicMock(), "plugins.base": plugin_base}
    with patch.dict(sys.modules, modules):
        from ..main import AlfenEveControl
    return AlfenEveControl


def _service(identity, address, port=443):
    return {
        "identity": identity,
        "hostname": "{}.local".format(identity.lower()),
        "address": address,
        "port": port,
        "endpoint": "https://{}:{}".format(address, port),
        "content_type": "alfen-ace-v1/json",
        "firmware_version": "6.4.0-4210",
    }


def _make_instance(overrides=None):
    plugin_class = _get_plugin_class()
    instance = plugin_class.__new__(plugin_class)
    config = copy.deepcopy(plugin_class.default_config)
    config.update(overrides or {})
    instance._config = plugin_class._normalize_config(config)
    instance._lock = threading.RLock()
    instance._discovery_event = threading.Event()
    instance.write_config = MagicMock()
    instance._reset_state()
    return instance


class ChargerDiscoveryConfigTest(TestCase):
    def test_new_config_uses_discovery_without_ip_override(self):
        instance = _make_instance()
        self.assertEqual(instance._config["endpoint"], "")
        self.assertEqual(instance._config["charger_identity"], "")
        instance._validate_config(dict(instance._config, enabled=True, dry_run=False, password="secret"))

    def test_legacy_endpoint_is_preserved_as_manual_override(self):
        plugin_class = _get_plugin_class()
        migrated = plugin_class._normalize_config({
            "config_schema_version": 5,
            "endpoint": "https://192.168.0.10",
        })
        self.assertEqual(migrated["endpoint"], "https://192.168.0.10")
        self.assertEqual(migrated["charger_identity"], "")
        self.assertEqual(migrated["config_schema_version"], 6)

    def test_only_unique_unconfigured_charger_is_auto_learned(self):
        instance = _make_instance()
        config_snapshot = copy.deepcopy(instance._config)
        service = _service("DIE_16628", "192.168.0.146")

        instance._apply_discovery_result(config_snapshot, [service])

        self.assertEqual(instance._config["charger_identity"], "DIE_16628")
        self.assertEqual(instance._config["endpoint"], "")
        self.assertEqual(instance._resolved_endpoint, "https://192.168.0.146:443")
        self.assertEqual(instance._discovery_status, "resolved")
        instance.write_config.assert_called_once_with(instance._config)

    def test_configured_identity_selects_matching_charger(self):
        instance = _make_instance({"charger_identity": "DIE_2"})
        config_snapshot = copy.deepcopy(instance._config)
        services = [_service("DIE_1", "192.168.0.10"), _service("DIE_2", "192.168.0.20")]

        instance._apply_discovery_result(config_snapshot, services)

        self.assertEqual(instance._resolved_endpoint, "https://192.168.0.20:443")
        self.assertEqual(instance._discovery_status, "resolved")
        instance.write_config.assert_not_called()

    def test_ambiguous_initial_scan_does_not_choose_or_save(self):
        instance = _make_instance()
        config_snapshot = copy.deepcopy(instance._config)

        instance._apply_discovery_result(config_snapshot, [
            _service("DIE_1", "192.168.0.10"),
            _service("DIE_2", "192.168.0.20"),
        ])

        self.assertEqual(instance._resolved_endpoint, "")
        self.assertEqual(instance._discovery_status, "ambiguous")
        self.assertEqual(instance._config["charger_identity"], "")
        instance.write_config.assert_not_called()

    def test_missing_configured_identity_never_substitutes_another_charger(self):
        instance = _make_instance({"charger_identity": "DIE_EXPECTED"})

        instance._apply_discovery_result(copy.deepcopy(instance._config), [
            _service("DIE_OTHER", "192.168.0.10"),
        ])

        self.assertEqual(instance._resolved_endpoint, "")
        self.assertEqual(instance._discovery_status, "not_found")
        self.assertIn("configured identity", instance._discovery_error)
        instance.write_config.assert_not_called()

    def test_manual_endpoint_override_wins_over_discovery(self):
        instance = _make_instance({"endpoint": "https://192.168.0.50"})

        instance._apply_discovery_result(copy.deepcopy(instance._config), [
            _service("DIE_1", "192.168.0.10"),
        ])

        self.assertEqual(instance._resolved_endpoint, "https://192.168.0.50")
        self.assertEqual(instance._discovery_status, "manual_override")
        instance.write_config.assert_not_called()

    def test_config_ui_keys_have_descriptions_and_hide_schema_version(self):
        plugin_class = _get_plugin_class()
        instance = _make_instance()
        config = json.loads(instance.get_config())
        description_keys = {item["name"] for item in plugin_class.config_description}

        self.assertEqual(set(config), description_keys)
        self.assertNotIn("config_schema_version", config)
        self.assertIn("charger_identity", config)

    def test_connection_failure_requests_rediscovery(self):
        instance = _make_instance({
            "enabled": True,
            "dry_run": False,
            "password": "test-password",
            "write_retries": 1,
            "write_retry_backoff": 0,
        })
        instance._resolved_endpoint = "https://192.168.0.146:443"

        with patch("alfeneve.lib.alfeneve.alfen.Alfen") as alfen:
            alfen.return_value.__enter__.side_effect = requests.exceptions.ConnectionError()
            self.assertFalse(instance._write_current(6))

        self.assertTrue(instance._discovery_event.is_set())
        self.assertEqual(instance._discovery_status, "waiting")

    def test_authentication_http_error_does_not_request_rediscovery(self):
        instance = _make_instance({
            "enabled": True,
            "dry_run": False,
            "password": "test-password",
            "write_retries": 1,
            "write_retry_backoff": 0,
        })
        instance._resolved_endpoint = "https://192.168.0.146:443"

        with patch("alfeneve.lib.alfeneve.alfen.Alfen") as alfen:
            alfen.return_value.__enter__.side_effect = requests.exceptions.HTTPError()
            self.assertFalse(instance._write_current(6))

        self.assertFalse(instance._discovery_event.is_set())
