import os
from numbers import Number
from typing import List, Dict
from charging_stations.connectors import Config, Connector, OSMConnector
from .connector_helper import connector_process


class TestConnectorOSM:
    base_path: str = os.path.realpath(
        os.path.join(os.path.dirname(__file__), "../data")
    )
    connector: Connector = OSMConnector(base_path=base_path, **Config.OSM)

    def test_get_data(self):
        """
        Not really necessary, since we essentially just return the response.

        :return:
        """
        self.connector.get_data(to_disk=True)
        raw_data: List[Dict] = self.connector.raw_data
        assert len(raw_data) >= 1
        raw_data_point: Dict
        for raw_data_point in raw_data:
            latitude: float = raw_data_point.get("lat")
            assert isinstance(latitude, Number) & (latitude == latitude)
            longitude: float = raw_data_point.get("lon")
            assert isinstance(longitude, Number) & (longitude == longitude)

    def test__string_to_number_list(self):
        some_string: str = "12 V;55BC ,33/12A"
        expected_list: List[int] = [12, 55, 33, 12]
        seperator_list: List[str] = [";", ",", "/"]
        actual_list: List[int] = self.connector._string_to_number_list(
            list_string=some_string, transform_fn=int, separator_list=seperator_list
        )
        assert expected_list == actual_list

    def test_process(self):
        connector_process(connector=self.connector)
