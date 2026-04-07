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
        return MeasurementCounterDummy(dto, report_status=report_status, update_interval=30, mode=mode, offset=offset)

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
        self.assertEqual(dummy.values["total_consumed"], 10)
        self.assertEqual(dummy.values["total_injected"], 10)
        self.assertEqual(dummy.values["realtime"], 10)

    def test_update_values_constant_mode_accumulates_counter_values(self):
        dummy = self._make_dummy(category="electric", mode="constant", offset=5)
        dummy.update_values()
        dummy.update_values()
        # Counter values (total_consumed, total_injected) accumulate
        self.assertEqual(dummy.values["total_consumed"], 10)
        self.assertEqual(dummy.values["total_injected"], 10)
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
        self.assertEqual(dummy.values["total_consumed"], 7)
        self.assertEqual(dummy.values["realtime"], 7)
        self.assertNotIn("total_injected", dummy.values)

    def test_update_values_returns_true(self):
        dummy = self._make_dummy(mode="constant", offset=1)
        result = dummy.update_values()
        self.assertTrue(result)

    def test_update_values_constant_mode_negative_offset(self):
        dummy = self._make_dummy(category="electric", mode="constant", offset=-5)
        dummy.update_values()
        self.assertEqual(dummy.values["total_consumed"], -5)
        self.assertEqual(dummy.values["total_injected"], -5)
        self.assertEqual(dummy.values["realtime"], -5)


class CheckDummyConfigTest(TestCase):
    def _check_dummy_config(self, config):
        import sys
        plugins_base_mock = MagicMock()
        plugins_base_mock.OMPluginBase = object
        mocks = {
            'six': MagicMock(),
            'plugins': MagicMock(),
            'plugins.base': plugins_base_mock,
        }
        with patch.dict(sys.modules, mocks):
            from ..main import Dummy
            Dummy._check_dummy_config(None, config)

    def test_random_mode_sets_offset_to_zero(self):
        mc = {"name": "test", "offset_mode": "random"}
        self._check_dummy_config({"measurement_counters": [mc]})
        self.assertEqual(mc["offset"], 0)

    def test_random_mode_overwrites_existing_offset(self):
        mc = {"name": "test", "offset_mode": "random", "offset": 99}
        self._check_dummy_config({"measurement_counters": [mc]})
        self.assertEqual(mc["offset"], 0)

    def test_constant_mode_with_offset_passes(self):
        mc = {"name": "test", "offset_mode": "constant", "offset": 5}
        self._check_dummy_config({"measurement_counters": [mc]})  # should not raise

    def test_constant_mode_without_offset_raises(self):
        mc = {"name": "test", "offset_mode": "constant"}
        with self.assertRaises(ValueError):
            self._check_dummy_config({"measurement_counters": [mc]})
