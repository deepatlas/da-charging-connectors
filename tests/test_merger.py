import json
import logging
import os
import pandas as pd
import geopandas as gpd
from typing import List, Dict
from charging_stations.connectors import Merger

log = logging.getLogger(os.path.basename(__file__))


class TestMerger:
    base_path: str = os.path.realpath(
        os.path.join(os.path.dirname(__file__), "../data")
    )
    merger: Merger = Merger(base_path=base_path)

    def test__load_data(self):
        self.merger._load_data(is_test=True)
        basepath: str = os.path.realpath(
            os.path.join(os.path.dirname(__file__), "../data")
        )
        actual_data_sources: List[Dict] = self.merger.data_sources
        expected_data_sources = self._load_data(basepath)
        assert actual_data_sources == expected_data_sources

    def _load_data(self, basepath: str):
        from charging_stations.connectors._connector import object_hook

        expected_data_sources: List[Dict] = []
        for file in os.listdir(basepath):
            if file.startswith("test_") & file.endswith("__processed.json"):
                file_path = os.path.join(basepath, file)
                with open(file_path, "r", encoding="utf-8") as f:
                    expected_data_sources += json.load(f, object_hook=object_hook)
        return expected_data_sources

    def test_haversine_distance(self):
        coords: pd.DataFrame = pd.DataFrame(
            [
                [-77.037852, 38.898556, -77.043934, 38.897147],
                [10.944427, 48.402489, 10.940854, 48.397232],
            ],
            columns=["lon1", "lat1", "lon2", "lat2"],
        )
        coords["actual_distance"]: pd.Series = self.merger.haversine_distance(
            coords=coords
        )
        coords["expected_distance"]: pd.Series = pd.Series(
            [549.1557912048178, 641.3109178030164]
        )
        assert coords["actual_distance"].equals(coords["expected_distance"])

    def test__prepare_geodataframe(self):
        self.merger._load_data(is_test=True)
        geodataframe: gpd.GeoDataFrame = self.merger._load_data()._prepare_geodataframe()
        # TODO: add some type checks & maybe distance checks
        assert (
            pd.concat([geodataframe.isnull().sum(), geodataframe.isna().sum()], axis=1)
            .loc[["geometry", "is_duplicate", "merged_attributes"]]
            .sum()
            .sum()
            == 0
        )

    def test__determine_duplicates(self):
        # TODO
        assert False

    def test_merge(self):
        # TODO
        assert False
