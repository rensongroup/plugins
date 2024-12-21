from unittest import TestCase
from unittest.mock import MagicMock, patch

from plugin_runtime.web import WebInterfaceDispatcher
from plugin_runtime.connectors.connector import Connector
from gateway.utilities.event_loop import EventLoop

from foxess.main import FoxEss

import logging

logger = logging.getLogger(__name__)

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

    def test_poll_foxess(self):
        self.mock_f.get_generation = MagicMock(return_value={'month': 56.20000000000073, 'today': 0.8000000000010914, 'cumulative': 11641.1})
        self.mock_f.get_real = MagicMock(
            return_value=[
                {"unit": "kW", "name": "PVPower", "variable": "pvPower", "value": 0.0},
                {
                    "unit": "kW",
                    "name": "Load Power",
                    "variable": "loadsPower",
                    "value": 0.35,
                },
                {
                    "unit": "kW",
                    "name": "Output Power",
                    "variable": "generationPower",
                    "value": -0.031,
                },
                {
                    "unit": "kW",
                    "name": "Charge Power",
                    "variable": "batChargePower",
                    "value": 0.0,
                },
                {
                    "unit": "kW",
                    "name": "Discharge Power",
                    "variable": "batDischargePower",
                    "value": 0.0,
                },
                {"unit": "%", "name": "SoC", "variable": "SoC", "value": 17.0},
                {
                    "unit": "kWh",
                    "name": "Cumulative power generation",
                    "variable": "generation",
                    "value": 11641.1,
                },
                {
                    "unit": "kWh",
                    "name": "Battery Residual Energy",
                    "variable": "ResidualEnergy",
                    "value": 1.8900000000000001,
                },
            ]
        )
        self.plugin.report_mc_status = MagicMock()
        self.plugin.poll_foxess()
        self.plugin.report_mc_status.assert_any_call(self.plugin._mc_battery, 0, 11641100.0, -0.0)
        self.plugin.report_mc_status.assert_any_call(self.plugin._mc_solar, 1890.0000000000002, 0, 0.0)


    def tearDown(self) -> None:
        self.patcher.stop()
