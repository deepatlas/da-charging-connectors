import logging
import os
from numbers import Number
from typing import Dict, List
from charging_stations.connectors import Config, Connector, OCMConnector
from .connector_helper import connector_process, connector_load

log = logging.getLogger(os.path.basename(__file__))


class TestConnectorOCM:
    base_path: str = os.path.realpath(
        os.path.join(os.path.dirname(__file__), "../data")
    )
    connector: Connector = OCMConnector(base_path=base_path, **Config.OCM)

    def test_get_data(self):
        """
        Not really necessary, since we essentially just return the response.

        :return:
        """
        self.connector.get_data(to_disk=False)
        raw_data: List[Dict] = self.connector.raw_data
        assert len(raw_data) >= 1
        raw_data_point: Dict
        for raw_data_point in raw_data:
            address_info: Dict = raw_data_point.get("AddressInfo")
            assert address_info is not None
            latitude: float = address_info.get("Latitude")
            assert isinstance(latitude, Number) & (latitude == latitude)
            longitude: float = address_info.get("Longitude")
            assert isinstance(longitude, Number) & (longitude == longitude)

    def test_process(self):
        connector_process(connector=self.connector)

    def test_load(self):
        connector_load(connector=self.connector)
