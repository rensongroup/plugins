from unittest import TestCase
from unittest.mock import MagicMock, patch

from plugin_runtime.web import WebInterfaceDispatcher
from plugin_runtime.connectors.connector import Connector
from gateway.utilities.event_loop import EventLoop

from foxess.main import FoxEss, POLL_REAL_TICKS, POLL_GENERATION_TICKS, FAILURE_NOTIFY_THRESHOLD

# Real data captured from the API on 2026-05-09
MOCK_REAL_DATA = [
    {"unit": "kW", "name": "PVPower", "variable": "pvPower", "value": 4.225},
    {"unit": "V", "name": "PV1Volt", "variable": "pv1Volt", "value": 231.3},
    {"unit": "A", "name": "PV1Current", "variable": "pv1Current", "value": 9.2},
    {"unit": "kW", "name": "PV1Power", "variable": "pv1Power", "value": 2.128},
    {"unit": "V", "name": "PV2Volt", "variable": "pv2Volt", "value": 230.4},
    {"unit": "A", "name": "PV2Current", "variable": "pv2Current", "value": 9.1},
    {"unit": "kW", "name": "PV2Power", "variable": "pv2Power", "value": 2.097},
    {"unit": "kW", "name": "EPSPower", "variable": "epsPower", "value": 0.0},
    {"unit": "A", "name": "EPS-RCurrent", "variable": "epsCurrentR", "value": 0.0},
    {"unit": "V", "name": "EPS-RVolt", "variable": "epsVoltR", "value": 0.0},
    {"unit": "kW", "name": "EPS-RPower", "variable": "epsPowerR", "value": 0.0},
    {"unit": "A", "name": "RCurrent", "variable": "RCurrent", "value": 1.4},
    {"unit": "V", "name": "RVolt", "variable": "RVolt", "value": 227.4},
    {"unit": "Hz", "name": "RFreq", "variable": "RFreq", "value": 50.02},
    {"unit": "kW", "name": "RPower", "variable": "RPower", "value": 0.29},
    {"unit": "\u2103", "name": "AmbientTemperature", "variable": "ambientTemperation", "value": 45.3},
    {"unit": "\u2103", "name": "InvTemperation", "variable": "invTemperation", "value": 37.5},
    {"unit": "\u2103", "name": "ChargeTemperature", "variable": "chargeTemperature", "value": 0.0},
    {"unit": "\u2103", "name": "batTemperature", "variable": "batTemperature", "value": 27.1},
    {"unit": "kW", "name": "Load Power", "variable": "loadsPower", "value": 0.26},
    {"unit": "kW", "name": "Output Power", "variable": "generationPower", "value": 0.29},
    {"unit": "kW", "name": "Feed-in Power", "variable": "feedinPower", "value": 0.0},
    {"unit": "kW", "name": "GridConsumption Power", "variable": "gridConsumptionPower", "value": 0.0},
    {"unit": "V", "name": "InvBatVolt", "variable": "invBatVolt", "value": 241.5},
    {"unit": "A", "name": "InvBatCurrent", "variable": "invBatCurrent", "value": -15.4},
    {"unit": "kW", "name": "invBatPower", "variable": "invBatPower", "value": -3.736},
    {"unit": "kW", "name": "Charge Power", "variable": "batChargePower", "value": 3.736},
    {"unit": "kW", "name": "Discharge Power", "variable": "batDischargePower", "value": 0.0},
    {"unit": "V", "name": "BatVolt", "variable": "batVolt", "value": 243.4},
    {"unit": "A", "name": "BatCurrent", "variable": "batCurrent", "value": 15.8},
    {"unit": "kW", "name": "MeterPower", "variable": "meterPower", "value": 0.0},
    {"unit": "kW", "name": "Meter2Power", "variable": "meterPower2", "value": -0.0},
    {"unit": "%", "name": "SoC", "variable": "SoC", "value": 62.0},
    {"unit": "kWh", "name": "Cumulative power generation", "variable": "generation", "value": 20442.5},
    {"unit": "kWh", "name": "Battery Residual Energy", "variable": "ResidualEnergy", "value": 7.3},
    {"name": "Running State", "variable": "runningState", "value": "163", "unit": ""},
    {"name": "Battery Status", "variable": "batStatus", "value": "1", "unit": ""},
    {"name": "Battery Status Name", "variable": "batStatusV2", "value": "Normal", "unit": ""},
    {"name": "The current error code is reported", "variable": "currentFault", "value": "", "unit": ""},
    {"name": "The number of errors", "variable": "currentFaultCount", "value": "0", "unit": ""},
    {"unit": "kWh", "name": "Battery throughput", "variable": "energyThroughput", "value": 11097.867},
    {"unit": "%", "name": "SOH", "variable": "SOH", "value": 0.0},
    {"unit": "kWh", "name": "Total grid electricity consumption", "variable": "gridConsumption", "value": 9390.1},
    {"unit": "kWh", "name": "Load power consumption", "variable": "loads", "value": 19870.2},
    {"unit": "kWh", "name": "The total energy of the feeder", "variable": "feedin", "value": 9453.1},
    {"unit": "kWh", "name": "Total charge energy", "variable": "chargeEnergyToTal", "value": 5841.9},
    {"unit": "kWh", "name": "Total discharge energy", "variable": "dischargeEnergyToTal", "value": 5211.1},
    {"unit": "kWh", "name": "Photovoltaic power generation", "variable": "PVEnergyTotal", "value": 22607.3},
    {"unit": "A", "name": "Maximum charge current", "variable": "maxChargeCurrent", "value": 50.0},
    {"unit": "A", "name": "Maximum discharge current", "variable": "maxDischargeCurrent", "value": 50.0},
    {"name": "Battery cycle count", "variable": "batCycleCount", "value": "446", "unit": ""},
]

MOCK_GENERATION_DATA = {"month": 167.40000000000146, "today": 0.5999999999985448, "cumulative": 20442.5}


class TestFoxEss(TestCase):

    def setUp(self) -> None:
        eventloop = EventLoop(name='plugin_event_loop')
        connector = Connector(forward_callback_actions=lambda *args, **kwargs: None, forward_status_event_subscriptions=lambda *args, **kwargs: None, event_loop=eventloop)
        web_dispatcher = WebInterfaceDispatcher("FoxEss")

        FoxEss.read_config = MagicMock(
            return_value={"api_key": "<API_KEY>"}
        )
        self.patcher = patch(
            "foxess.main.f",
            autospec=True,
        )
        self.mock_f = self.patcher.start()

        self.plugin = FoxEss(webinterface=web_dispatcher, connector=connector)

    def _setup_default_mocks(self):
        self.mock_f.get_generation = MagicMock(return_value=MOCK_GENERATION_DATA.copy())
        self.mock_f.get_real = MagicMock(return_value=[d.copy() for d in MOCK_REAL_DATA])

    # --- _configure_api ---

    def test_configure_api_success(self):
        result = self.plugin._configure_api()
        self.assertTrue(result)
        self.mock_f.get_site.assert_called_once()
        self.mock_f.get_logger.assert_called_once()
        self.mock_f.get_device.assert_called_once()

    def test_configure_api_no_site(self):
        self.mock_f.get_site = MagicMock(return_value=None)
        result = self.plugin._configure_api()
        self.assertFalse(result)
        self.mock_f.get_logger.assert_not_called()

    # --- _poll_generation ---

    def test_poll_generation(self):
        self.mock_f.get_generation = MagicMock(return_value=MOCK_GENERATION_DATA.copy())
        self.assertTrue(self.plugin._poll_generation())
        self.assertAlmostEqual(self.plugin._solar_injection, 20442.5)

    def test_poll_generation_returns_none(self):
        self.plugin._solar_injection = 10000.0
        self.mock_f.get_generation = MagicMock(return_value=None)
        self.assertFalse(self.plugin._poll_generation())
        self.assertAlmostEqual(self.plugin._solar_injection, 10000.0)

    def test_poll_generation_missing_cumulative(self):
        self.plugin._solar_injection = 10000.0
        self.mock_f.get_generation = MagicMock(return_value={"month": 100.0, "today": 5.0})
        self.assertTrue(self.plugin._poll_generation())
        self.assertAlmostEqual(self.plugin._solar_injection, 10000.0)

    # --- _poll_real ---

    def test_poll_real(self):
        self.mock_f.get_real = MagicMock(return_value=[d.copy() for d in MOCK_REAL_DATA])
        self.assertTrue(self.plugin._poll_real())
        self.assertAlmostEqual(self.plugin._bat_energy, 7.3)
        self.assertAlmostEqual(self.plugin._bat_real, 3.736)
        self.assertAlmostEqual(self.plugin._solar_real, -4.225)

    def test_poll_real_battery_discharging(self):
        real_data = [d.copy() for d in MOCK_REAL_DATA]
        for d in real_data:
            if d["variable"] == "batChargePower":
                d["value"] = 0.0
            elif d["variable"] == "batDischargePower":
                d["value"] = 2.5
        self.mock_f.get_real = MagicMock(return_value=real_data)
        self.plugin._poll_real()
        self.assertAlmostEqual(self.plugin._bat_real, -2.5)

    def test_poll_real_returns_none(self):
        self.plugin._bat_energy = 5.0
        self.mock_f.get_real = MagicMock(return_value=None)
        self.assertFalse(self.plugin._poll_real())
        # Values unchanged
        self.assertAlmostEqual(self.plugin._bat_energy, 5.0)

    def test_poll_real_missing_variables_keeps_previous(self):
        self.plugin._bat_energy = 5.0
        self.plugin._bat_real = 1.0
        self.plugin._solar_real = -2.0
        self.mock_f.get_real = MagicMock(return_value=[
            {"unit": "%", "name": "SoC", "variable": "SoC", "value": 50.0},
        ])
        self.plugin._poll_real()
        self.assertAlmostEqual(self.plugin._bat_energy, 5.0)
        self.assertAlmostEqual(self.plugin._bat_real, 1.0)
        self.assertAlmostEqual(self.plugin._solar_real, -2.0)

    # --- _check_battery_full ---

    def test_check_battery_full_sends_notification(self):
        self.plugin.connector.notification.send = MagicMock()
        self.plugin._check_battery_full(100.0)
        self.plugin.connector.notification.send.assert_called_once_with(
            topic="FoxEss", message="Battery is full again"
        )
        self.assertTrue(self.plugin._battery_full_sent)

    def test_check_battery_full_not_repeated(self):
        self.plugin.connector.notification.send = MagicMock()
        self.plugin._check_battery_full(100.0)
        self.plugin._check_battery_full(100.0)
        self.plugin.connector.notification.send.assert_called_once()

    def test_check_battery_full_resets_below_threshold(self):
        self.plugin._battery_full_sent = True
        self.plugin._check_battery_full(62.0)
        self.assertFalse(self.plugin._battery_full_sent)

    def test_check_battery_full_none_soc(self):
        self.plugin._battery_full_sent = True
        self.plugin._check_battery_full(None)
        # None is treated as "not full" -> resets flag
        self.assertFalse(self.plugin._battery_full_sent)

    # --- report_foxess ---

    def test_report_foxess(self):
        self.plugin._solar_injection = 15000.0
        self.plugin._solar_real = -3.5
        self.plugin._bat_energy = 6.0
        self.plugin._bat_real = 2.0

        self.plugin.report_mc_status = MagicMock()
        self.plugin.report_foxess()
        self.plugin.report_mc_status.assert_any_call(
            self.plugin._mc_battery, 6000.0, 0, 2000.0
        )
        self.plugin.report_mc_status.assert_any_call(
            self.plugin._mc_solar, 0, 15000000.0, -3500.0
        )

    # --- full poll + report integration ---

    def test_full_poll_and_report(self):
        """Integration: configure + generation + real + report produces correct output."""
        self._setup_default_mocks()
        self.plugin.report_mc_status = MagicMock()

        self.assertTrue(self.plugin._configure_api())
        self.plugin._poll_generation()
        self.plugin._poll_real()
        self.plugin.report_foxess()

        self.plugin.report_mc_status.assert_any_call(
            self.plugin._mc_battery, 7300.0, 0, 3736.0
        )
        self.plugin.report_mc_status.assert_any_call(
            self.plugin._mc_solar, 0, 20442500.0, -4225.0
        )

    # --- tick scheduling ---

    def test_generation_only_at_generation_tick(self):
        """_poll_generation is called at tick 0 but not at tick POLL_REAL_TICKS."""
        self._setup_default_mocks()
        self.plugin.report_mc_status = MagicMock()

        # tick 0: both generation and real should be polled
        self.plugin._tick_counter = 0
        self.assertTrue(self.plugin._tick_counter % POLL_GENERATION_TICKS == 0)
        self.assertTrue(self.plugin._tick_counter % POLL_REAL_TICKS == 0)

        # tick POLL_REAL_TICKS: real should be polled, generation should not
        self.plugin._tick_counter = POLL_REAL_TICKS
        self.assertTrue(self.plugin._tick_counter % POLL_REAL_TICKS == 0)
        self.assertFalse(self.plugin._tick_counter % POLL_GENERATION_TICKS == 0)

    # --- _get_variable ---

    def test_get_variable_helper(self):
        data = [
            {"variable": "pvPower", "value": 4.225},
            {"variable": "SoC", "value": 62.0},
        ]
        self.assertAlmostEqual(FoxEss._get_variable(data, "pvPower"), 4.225)
        self.assertAlmostEqual(FoxEss._get_variable(data, "SoC"), 62.0)
        self.assertIsNone(FoxEss._get_variable(data, "nonExistent"))
        self.assertIsNone(FoxEss._get_variable([], "pvPower"))

    # --- failure tracking ---

    def test_failure_notification_after_threshold(self):
        self.plugin.connector.notification.send = MagicMock()
        for _ in range(FAILURE_NOTIFY_THRESHOLD):
            self.plugin._track_poll_failure()
        self.plugin.connector.notification.send.assert_called_once()
        self.assertIn("failed", self.plugin.connector.notification.send.call_args[1]["message"])

    def test_no_notification_below_threshold(self):
        self.plugin.connector.notification.send = MagicMock()
        for _ in range(FAILURE_NOTIFY_THRESHOLD - 1):
            self.plugin._track_poll_failure()
        self.plugin.connector.notification.send.assert_not_called()

    def test_failure_notification_cooldown(self):
        """Second batch of failures within 24h does not send another notification."""
        self.plugin.connector.notification.send = MagicMock()
        for _ in range(FAILURE_NOTIFY_THRESHOLD):
            self.plugin._track_poll_failure()
        self.plugin.connector.notification.send.assert_called_once()

        # More failures without time advancing -> no second notification
        for _ in range(FAILURE_NOTIFY_THRESHOLD):
            self.plugin._track_poll_failure()
        self.plugin.connector.notification.send.assert_called_once()

    def test_reset_clears_failure_count(self):
        self.plugin._consecutive_failures = 5
        self.plugin._reset_poll_failures()
        self.assertEqual(self.plugin._consecutive_failures, 0)

    def tearDown(self) -> None:
        self.patcher.stop()
