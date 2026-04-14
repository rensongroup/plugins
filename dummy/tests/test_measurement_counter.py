from unittest import TestCase
from unittest.mock import MagicMock, patch


class MeasurementCounterDummyTest(TestCase):
    def _make_dto(self, category="electric", mc_type="grid"):
        dto = MagicMock()
        dto.category.value = category
        dto.type = mc_type
        return dto

    def _make_dummy(self, category="electric", mc_type="grid", update_interval=30, mode="random", offset=0):
        from ..measurement_counter import MeasurementCounterDummy

        dto = self._make_dto(category=category, mc_type=mc_type)
        report_status = MagicMock()
        return MeasurementCounterDummy(dto, report_status=report_status, update_interval=update_interval, mode=mode, offset=offset)

    def test_init_random_mode_defaults(self):
        dummy = self._make_dummy()
        self.assertEqual(dummy.mode, "random")
        self.assertEqual(dummy.offset, 0)

    def test_init_constant_mode(self):
        dummy = self._make_dummy(mode="constant", offset=42)
        self.assertEqual(dummy.mode, "constant")
        self.assertEqual(dummy.offset, 42)

    def test_update_values_constant_mode_adds_fixed_offset(self):
        dummy = self._make_dummy(category="electric", mode="constant", offset=10)
        dummy.update_values()
        expected = 10 * 30 / 3600
        self.assertAlmostEqual(dummy.values["total_consumed"], expected)
        self.assertAlmostEqual(dummy.values["total_injected"], expected)
        self.assertEqual(dummy.values["realtime"], 10)

    def test_update_values_constant_mode_accumulates_counter_values(self):
        dummy = self._make_dummy(category="electric", mode="constant", offset=5)
        dummy.update_values()
        dummy.update_values()
        # Counter values (total_consumed, total_injected) accumulate in Wh
        expected = 2 * 5 * 30 / 3600
        self.assertAlmostEqual(dummy.values["total_consumed"], expected)
        self.assertAlmostEqual(dummy.values["total_injected"], expected)
        # Realtime value does not accumulate - it is set to the offset each time
        self.assertEqual(dummy.values["realtime"], 5)

    def test_update_values_constant_mode_zero_offset(self):
        dummy = self._make_dummy(category="water", mode="constant", offset=0)
        dummy.update_values()
        dummy.update_values()
        self.assertEqual(dummy.values["total_consumed"], 0)
        self.assertEqual(dummy.values["realtime"], 0)

    def test_update_values_random_mode_uses_random(self):
        dummy = self._make_dummy(category="electric", mode="random")
        with patch("random.randint", return_value=50) as mock_randint:
            dummy.update_values()
            mock_randint.assert_called()
        # Values should have been updated using random
        self.assertEqual(dummy.values["total_consumed"], 50)
        self.assertEqual(dummy.values["total_injected"], 50)
        self.assertEqual(dummy.values["realtime"], 50)

    def test_update_values_non_electric_category_constant(self):
        dummy = self._make_dummy(category="water", mode="constant", offset=7)
        dummy.update_values()
        self.assertAlmostEqual(dummy.values["total_consumed"], 7 * 30 / 3600)
        self.assertEqual(dummy.values["realtime"], 7)
        self.assertNotIn("total_injected", dummy.values)

    def test_update_values_returns_true(self):
        dummy = self._make_dummy(mode="constant", offset=1)
        result = dummy.update_values()
        self.assertTrue(result)

    def test_update_values_constant_mode_negative_offset(self):
        dummy = self._make_dummy(category="electric", mode="constant", offset=-5)
        dummy.update_values()
        expected = -5 * 30 / 3600
        self.assertAlmostEqual(dummy.values["total_consumed"], expected)
        self.assertAlmostEqual(dummy.values["total_injected"], expected)
        self.assertEqual(dummy.values["realtime"], -5)

    def test_update_values_constant_mode_wh_conversion(self):
        # 1W over 3600s interval = 1 Wh per tick
        dummy = self._make_dummy(category="electric", mode="constant", offset=1, update_interval=3600)
        dummy.update_values()
        self.assertAlmostEqual(dummy.values["total_consumed"], 1.0)
        self.assertAlmostEqual(dummy.values["total_injected"], 1.0)

    def test_simulation_positive_offset_reports_consumed_not_injected(self):
        """A positive offset accumulates in total_consumed; injected is clamped to zero."""

        dummy = self._make_dummy(category="electric", mode="constant", offset=100)
        # Run one update cycle directly so values accumulate
        dummy.update_values()
        consumed = max(0, dummy.values.get("total_consumed", 0))
        injected = max(0, -1 * dummy.values.get("total_injected", 0))
        self.assertGreater(consumed, 0)
        self.assertEqual(injected, 0)

    def test_simulation_negative_offset_reports_injected_not_consumed(self):
        """A negative offset accumulates in total_injected; consumed is clamped to zero."""
        dummy = self._make_dummy(category="electric", mode="constant", offset=-100)
        dummy.update_values()
        consumed = max(0, dummy.values.get("total_consumed", 0))
        injected = max(0, -1 * dummy.values.get("total_injected", 0))
        self.assertEqual(consumed, 0)
        self.assertGreater(injected, 0)

    def test_simulation_calls_report_status_with_sign_corrected_values(self):
        """simulation() passes max(0, consumed) and max(0, -injected) to report_status."""
        from unittest.mock import patch

        dummy = self._make_dummy(category="electric", mode="constant", offset=-2300, update_interval=30)

        call_args = []

        def fake_report(dto, consumed, injected, realtime):
            call_args.append((consumed, injected, realtime))

        dummy.report_status = fake_report

        # Patch time.sleep and stop the loop after one iteration
        _iteration = [0]

        def fake_sleep(_):
            dummy._running = False

        with patch("time.sleep", side_effect=fake_sleep):
            dummy._running = True
            dummy.simulation()

        self.assertEqual(len(call_args), 1)
        consumed, injected, realtime = call_args[0]
        self.assertEqual(consumed, 0, "consumed should be 0 for a negative offset")
        self.assertGreater(injected, 0, "injected should be positive for a negative offset")
        self.assertEqual(realtime, -2300)


class AddDefaultsTest(TestCase):
    def _add_defaults(self, config):
        import sys

        plugins_base_mock = MagicMock()
        plugins_base_mock.OMPluginBase = object
        mocks = {
            "six": MagicMock(),
            "plugins": MagicMock(),
            "plugins.base": plugins_base_mock,
        }
        with patch.dict(sys.modules, mocks):
            from ..main import Dummy

            Dummy._add_defaults_to_optional_fields(None, config)

    def test_random_mode_sets_offset_to_zero_when_missing(self):
        mc = {"name": "test", "offset_mode": "random"}
        self._add_defaults({"measurement_counters": [mc]})
        self.assertEqual(mc["offset"], 0)

    def test_random_mode_does_not_overwrite_existing_offset(self):
        mc = {"name": "test", "offset_mode": "random", "offset": 99}
        self._add_defaults({"measurement_counters": [mc]})
        self.assertEqual(mc["offset"], 99)


class CheckDummyConfigTest(TestCase):
    def _check_dummy_config(self, config):
        import sys

        plugins_base_mock = MagicMock()
        plugins_base_mock.OMPluginBase = object
        mocks = {
            "six": MagicMock(),
            "plugins": MagicMock(),
            "plugins.base": plugins_base_mock,
        }
        with patch.dict(sys.modules, mocks):
            from ..main import Dummy

            Dummy._check_dummy_config(None, config)

    def test_constant_mode_with_offset_passes(self):
        mc = {"name": "test", "offset_mode": "constant", "offset": 5}
        self._check_dummy_config({"measurement_counters": [mc]})  # should not raise

    def test_constant_mode_without_offset_raises(self):
        mc = {"name": "test", "offset_mode": "constant"}
        with self.assertRaises(ValueError):
            self._check_dummy_config({"measurement_counters": [mc]})
