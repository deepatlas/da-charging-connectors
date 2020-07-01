import json
import os
from typing import List, Dict
from charging_stations.connectors import Connector
from charging_stations.helpers import object_hook


def connector_process(connector: Connector):
    connector.load(is_processed=False, is_test=True)
    connector.process(to_disk=False)
    processed_data: List[Dict] = connector.processed_data
    processed_data_point: Dict
    for processed_data_point in processed_data:
        # TODO: add more assertions as we change attribute format
        assert processed_data_point.get("charging") is not None
        assert isinstance(processed_data_point.get("id"), bytes)
        charging: Dict = processed_data_point.get("charging")
        if charging.get("max_kw") is not None:
            assert charging.get("max_kw") == max(charging.get("kw_list"))
        if len(charging.get("socket_type_list")) > 0 | charging.get("dc_support"):
            assert any(
                ["DC" in s for s in charging.get("socket_type_list")]
            ) == charging.get("dc_support")
        capacity: int = charging.get("capacity")
        assert isinstance(capacity, int) | isinstance(capacity, type(None))

        assert processed_data_point.get("address") is not None
        address: Dict = processed_data_point.get("address")
        if address.get("postcode") is not None:
            assert (len(address.get("postcode")) == 5) & (
                all([s.isdigit() for s in address.get("postcode")])
            )
        if address.get("country") is not None:
            assert all([not s.isdigit() for s in address.get("country")])
        if address.get("state") is not None:
            assert all([not s.isdigit() for s in address.get("state")])
        if address.get("town") is not None:
            assert all([not s.isdigit() for s in address.get("town")])

        coordinates: str = processed_data_point.get("coordinates")
        assert (
            isinstance(coordinates, str)
            & coordinates.startswith("POINT(")
            & (" " in coordinates)
            & coordinates.endswith(")")
        )


def connector_load(connector: Connector):
    base_path: str = os.path.realpath(
        os.path.join(os.path.dirname(__file__), "../data")
    )
    file_path: str = os.path.join(
        base_path, f"test_{connector.__data_source__}__raw.json"
    )
    expected_ocm_raw: Dict
    with open(file_path, "r") as f:
        expected_ocm_raw = json.load(f)
    connector.load(is_processed=False, is_test=True)
    assert expected_ocm_raw == connector.raw_data
    expected_ocm_processed: Dict
    with open(file_path.replace("__raw", "__processed"), "r") as f:
        expected_ocm_processed = json.load(f, object_hook=object_hook)
    connector.load(is_processed=True, is_test=True)
    assert expected_ocm_processed == connector.processed_data
