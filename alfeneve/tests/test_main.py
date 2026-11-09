import sys
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch


def _import_alfen_eve_control():
    plugins_base_mock = MagicMock()
    plugins_base_mock.OMPluginBase = object
    plugins_base_mock.PluginConfigChecker = MagicMock()
    plugins_base_mock.background_task = lambda f: f
    plugins_base_mock.om_expose = lambda *a, **kw: (a[0] if a and callable(a[0]) else (lambda f: f))
    mocks = {
        "plugins": MagicMock(),
        "plugins.base": plugins_base_mock,
    }
    with patch.dict(sys.modules, mocks):
        from ..main import AlfenEveControl
    return AlfenEveControl


def _make_instance(config_overrides=None):
    AlfenEveControl = _import_alfen_eve_control()
    instance = AlfenEveControl.__new__(AlfenEveControl)
    instance._config = dict(AlfenEveControl.default_config)
    if config_overrides:
        instance._config.update(config_overrides)
    instance._signal_map = {}
    instance._values = {"solar_w": None, "grid_w": None, "soc_percent": None}
    instance._value_ts = {}
    instance._last_seen = None
    instance._last_sent_current = None
    instance._rebuild_signal_map()
    return instance


class CalculateTargetCurrentTest(TestCase):
    def test_summer_exports_solar_scales_with_export(self):
        instance = _make_instance({"season": "summer", "min_current": 6, "max_current": 16})
        instance._values.update({"grid_w": -1150.0, "soc_percent": 50.0})  # exporting 1150W
        self.assertEqual(instance._calculate_target_current(), 5.0)

    def test_summer_importing_falls_back_to_min_current(self):
        instance = _make_instance({"season": "summer", "min_current": 6, "max_current": 16})
        instance._values.update({"grid_w": 500.0, "soc_percent": 50.0})  # importing, no excess
        self.assertEqual(instance._calculate_target_current(), 6.0)

    def test_summer_clamps_to_max_current(self):
        instance = _make_instance({"season": "summer", "min_current": 6, "max_current": 16})
        instance._values.update({"grid_w": -10000.0, "soc_percent": 50.0})
        self.assertEqual(instance._calculate_target_current(), 16.0)

    def test_winter_high_soc_uses_high_current(self):
        instance = _make_instance({
            "season": "winter",
            "winter_high_soc_threshold": 30,
            "winter_high_soc_current": 8,
            "winter_low_soc_current": 4,
        })
        instance._values.update({"grid_w": 0.0, "soc_percent": 50.0})
        self.assertEqual(instance._calculate_target_current(), 8.0)

    def test_winter_low_soc_uses_low_current(self):
        instance = _make_instance({
            "season": "winter",
            "winter_high_soc_threshold": 30,
            "winter_high_soc_current": 8,
            "winter_low_soc_current": 4,
        })
        instance._values.update({"grid_w": 0.0, "soc_percent": 10.0})
        self.assertEqual(instance._calculate_target_current(), 4.0)


class ShouldSendTest(TestCase):
    def test_first_send_always_true(self):
        instance = _make_instance()
        self.assertTrue(instance._should_send(6.0))

    def test_within_deadband_is_false(self):
        instance = _make_instance({"deadband_tenths": 5})
        instance._last_sent_current = 6.0
        self.assertFalse(instance._should_send(6.3))

    def test_outside_deadband_is_true(self):
        instance = _make_instance({"deadband_tenths": 5})
        instance._last_sent_current = 6.0
        self.assertTrue(instance._should_send(6.6))


class ValidateConfigTest(TestCase):
    def _valid_config(self):
        return {
            "signals": [
                {"name": "solar_w"},
                {"name": "grid_w"},
                {"name": "soc_percent"},
            ],
            "min_current": 6,
            "max_current": 16,
            "update_interval": 30,
        }

    def test_valid_config_does_not_raise(self):
        instance = _make_instance()
        instance._validate_config(self._valid_config())

    def test_missing_signal_raises(self):
        instance = _make_instance()
        config = self._valid_config()
        config["signals"] = [{"name": "solar_w"}, {"name": "grid_w"}]
        with self.assertRaises(ValueError):
            instance._validate_config(config)

    def test_max_below_min_raises(self):
        instance = _make_instance()
        config = self._valid_config()
        config["max_current"] = 4
        with self.assertRaises(ValueError):
            instance._validate_config(config)

    def test_update_interval_zero_raises(self):
        instance = _make_instance()
        config = self._valid_config()
        config["update_interval"] = 0
        with self.assertRaises(ValueError):
            instance._validate_config(config)


class HandleMeasurementCounterStatusTest(TestCase):
    def test_soc_percent_converted_from_kwh(self):
        instance = _make_instance({"battery_capacity_wh": 10000})
        instance._signal_map = {"battery-id": {"name": "soc_percent", "field": "realtime", "scale": 1}}
        event = MagicMock()
        event.data = {"id": "battery-id", "realtime": 7.2}
        instance.handle_measurement_counter_status(event)
        self.assertAlmostEqual(instance._values["soc_percent"], 72.0)
        self.assertIn("soc_percent", instance._value_ts)

    def test_solar_w_passed_through_unconverted(self):
        instance = _make_instance()
        instance._signal_map = {"solar-id": {"name": "solar_w", "field": "realtime", "scale": 1}}
        event = MagicMock()
        event.data = {"id": "solar-id", "realtime": 1500}
        instance.handle_measurement_counter_status(event)
        self.assertEqual(instance._values["solar_w"], 1500.0)

    def test_unknown_measurement_counter_id_ignored(self):
        instance = _make_instance()
        instance._signal_map = {"known-id": {"name": "solar_w", "field": "realtime", "scale": 1}}
        event = MagicMock()
        event.data = {"id": "unknown-id", "realtime": 1500}
        instance.handle_measurement_counter_status(event)
        self.assertIsNone(instance._values["solar_w"])


class IsDataReadyTest(TestCase):
    def test_ready_when_all_present_and_fresh(self):
        instance = _make_instance({"max_value_age": 120})
        instance._values.update({"solar_w": 100.0, "grid_w": 50.0, "soc_percent": 60.0})
        now = time.time()
        instance._value_ts = {"solar_w": now, "grid_w": now, "soc_percent": now}
        self.assertTrue(instance._is_data_ready())

    def test_not_ready_when_missing_value(self):
        instance = _make_instance()
        instance._values.update({"solar_w": 100.0, "grid_w": 50.0, "soc_percent": None})
        self.assertFalse(instance._is_data_ready())

    def test_not_ready_when_stale(self):
        instance = _make_instance({"max_value_age": 60})
        instance._values.update({"solar_w": 100.0, "grid_w": 50.0, "soc_percent": 60.0})
        stale = time.time() - 3600
        instance._value_ts = {"solar_w": stale, "grid_w": stale, "soc_percent": stale}
        self.assertFalse(instance._is_data_ready())


class WriteCurrentRetryTest(TestCase):
    def _run_write_current(self, instance, alfen_cls):
        fake_lib = MagicMock()
        fake_lib_alfeneve = MagicMock()
        fake_lib_alfeneve_alfen = MagicMock()
        fake_lib_alfeneve_alfen.Alfen = alfen_cls
        mocks = {
            "lib": fake_lib,
            "lib.alfeneve": fake_lib_alfeneve,
            "lib.alfeneve.alfen": fake_lib_alfeneve_alfen,
        }
        with patch.dict(sys.modules, mocks), patch("time.sleep"):
            instance._write_current(7.5)

    def test_succeeds_on_first_attempt(self):
        instance = _make_instance({"dry_run": False, "write_retries": 3, "write_retry_backoff": 0})
        context = MagicMock()
        alfen_cls = MagicMock(return_value=context)
        context.__enter__ = MagicMock(return_value=context)
        context.__exit__ = MagicMock(return_value=False)

        self._run_write_current(instance, alfen_cls)

        context.set_property.assert_called_once()
        self.assertEqual(instance._last_sent_current, 7.5)

    def test_retries_then_succeeds(self):
        instance = _make_instance({"dry_run": False, "write_retries": 3, "write_retry_backoff": 0})

        attempts = {"count": 0}

        class FlakyAlfen:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                attempts["count"] += 1
                if attempts["count"] < 2:
                    raise RuntimeError("boom")
                return self

            def __exit__(self, *args):
                return False

            def set_property(self, *args, **kwargs):
                pass

        self._run_write_current(instance, FlakyAlfen)

        self.assertEqual(attempts["count"], 2)
        self.assertEqual(instance._last_sent_current, 7.5)

    def test_gives_up_after_max_retries(self):
        instance = _make_instance({"dry_run": False, "write_retries": 2, "write_retry_backoff": 0})

        class AlwaysFailAlfen:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                raise RuntimeError("boom")

            def __exit__(self, *args):
                return False

        self._run_write_current(instance, AlwaysFailAlfen)

        self.assertIsNone(instance._last_sent_current)

    def test_dry_run_does_not_call_alfen(self):
        instance = _make_instance({"dry_run": True})
        instance._write_current(6.0)
        self.assertEqual(instance._last_sent_current, 6.0)
