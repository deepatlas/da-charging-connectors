import logging
import os
from numbers import Number
from typing import List, Dict
from charging_stations.connectors import BNAConnector, Connector, Config
from .connector_helper import connector_process

log = logging.getLogger(os.path.basename(__file__))


class TestConnectorBNA:
    base_path: str = os.path.realpath(
        os.path.join(os.path.dirname(__file__), "../data")
    )
    connector: Connector = BNAConnector(base_path=base_path, **Config.BNA)

    def test_get_data(self):
        """
        Necessary, since we use a webscraper to download a excel file and transform it.

        :return:
        """
        self.connector.get_data(to_disk=False)
        raw_data: List[Dict] = self.connector.raw_data
        assert len(raw_data) >= 1
        raw_data_point: Dict
        for raw_data_point in raw_data:
            latitude: float = raw_data_point.get("Breitengrad [DG]")
            assert isinstance(latitude, Number) & (latitude == latitude)
            longitude: float = raw_data_point.get("Längengrad [DG]")
            assert isinstance(longitude, Number) & (longitude == longitude)

    def test_process(self):
        # TODO: Add option to first download new data before run process data
        connector_process(connector=self.connector)
