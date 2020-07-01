import json
import os
import geopandas as gpd
import libpysal
import numpy as np
import pandas as pd
from libpysal.weights.distance import KNN
from shapely import wkt
from typing import List, Dict, Union
from shapely.geometry.base import BaseGeometry
from tqdm import tqdm
from ..helpers import get_logger, object_hook
from difflib import SequenceMatcher

log = get_logger(os.path.basename(__file__))


class Merger(object):
    def __init__(
        self,
        base_path: str = os.path.realpath(
            os.path.join(os.path.dirname(__file__), "../../../data")
        ),
    ):
        self.base_path: str = base_path
        self.data_sources: List[Dict] = []
        self.stations_gdf: Union[gpd.GeoDataFrame, None] = None
        self.knn3: Union[KNN, None] = None
        self.merged_stations_gdf: Union[gpd.GeoDataFrame, None] = None

    def _load_data(self, is_test: bool = False) -> "Merger":
        """
        Loads all files in data folder which end with "__processed.json" into a list of dictionaries,
        which can be thought of as Stations.

        :param is_test: If true, files for running unit test specifically are loaded
        :return: Merger object
        """
        for file in os.listdir(self.base_path):
            startswith: bool = file.startswith("test_")
            endswith: bool = file.endswith("__processed.json")
            file_path: str = os.path.join(self.base_path, file)
            if is_test:
                if (not startswith) | (not endswith):
                    continue
            else:
                if (not file.endswith("__processed.json")) | file.startswith("test_"):
                    continue
            with open(file_path, "r", encoding="utf-8") as f:
                self.data_sources += json.load(f, object_hook=object_hook)
        if len(self.data_sources) < 1:
            raise RuntimeError("Could not read any json files!")
        return self

    @staticmethod
    def haversine_distance(
        coords: pd.DataFrame, to_radians: bool = True, earth_radius: int = 6371
    ) -> pd.Series:
        """
        Vectorized haversine distance computation between two points.

        :param coords: pd.DataFrame with ordered columns ["lon1", "lat1", "lon2", "lat2"]
        :param to_radians: If True, coordinates are converted to radiants
        :param earth_radius: Earth radius
        :return: pd.Series containing haversine distances
        """
        if to_radians:
            coords = coords.apply(np.radians)
        coords["delta_lat_sin_squared"] = (
            np.sin((coords["lat2"] - coords["lat1"]) / 2) ** 2
        )
        coords["delta_lon_sin_squared"] = (
            np.sin((coords["lon2"] - coords["lon1"]) / 2) ** 2
        )
        coords["a"] = (
            coords["delta_lat_sin_squared"]
            + np.cos(coords["lat1"])
            * np.cos(coords["lat2"])
            * coords["delta_lon_sin_squared"]
        )
        return earth_radius * 2 * np.arcsin(np.sqrt(coords["a"])) * 1000

    def _prepare_geodataframe(self) -> gpd.GeoDataFrame:
        """
        Turns list of json objects (Stations) into GeoDataFrame. Mainly by transforming coordinates to wkt and
        flattening address & charging object.

        :return: gpd.GeoDataFrame
        """

        def get_wkt(x: str) -> Union[BaseGeometry, None]:
            try:
                return wkt.loads(x)
            except:
                log.debug(f"Could not convert string to wkt: {x}")
                return None

        stations_df = pd.json_normalize(data=self.data_sources, max_level=1).drop(
            ["address.station_id", "charging.station_id"], axis=1
        )
        stations_df.columns = [c.split(".")[-1] for c in stations_df.columns]
        assert len(set(stations_df.columns)) == stations_df.shape[1]
        original_no_rows: int = stations_df.shape[0]
        stations_df["coordinates"] = stations_df["coordinates"].apply(get_wkt)
        stations_df.dropna(subset=["coordinates"], inplace=True)
        log.debug(
            f"Dropped {original_no_rows - stations_df.shape[0]} rows after loading wkt coordinates!"
        )
        stations_df.rename(columns={"coordinates": "geometry"}, inplace=True)
        stations_gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(
            stations_df, geometry="geometry", crs={"init": "epsg:5243"}
        )
        stations_gdf["is_duplicate"] = False
        stations_gdf["merged_attributes"] = False
        stations_gdf.drop_duplicates(subset=["id"], inplace=True)
        return (
            stations_gdf.loc[
                (stations_gdf.operator.notna() & stations_gdf.socket_type_list.notna()),
                :,
            ]
            .reset_index(drop=True)
            .sample(frac=1.0)
        )

    def _get_duplicate_candidates(
        self, current_station: pd.Series, max_distance: int = 100
    ) -> pd.DataFrame:
        """
        Determines relevant neighboring stations based on haversine distance to current_station.

        :param current_station: pd.Series containing some Station
        :param max_distance: Maximum distance for determining if neighboring distance is a potential duplicate candidate
        :return: pd.DataFrame containing potential duplicate candidates
        """
        current_station_id = current_station.name
        knn_neighbors = self.knn3.neighbors[current_station_id]
        neighbors = self.stations_gdf.loc[
            self.stations_gdf.index.isin(knn_neighbors)
            & (self.stations_gdf["is_duplicate"] != True),
            :,
        ]
        neighbor_distances = neighbors.geometry.bounds.iloc[:, :2].rename(
            columns={"minx": "lon1", "miny": "lat1"}
        )
        (
            neighbor_distances["lon2"],
            neighbor_distances["lat2"],
            _,
            _,
        ) = current_station.geometry.bounds
        neighbor_distances["distance_meter"] = self.haversine_distance(
            neighbor_distances
        )
        relevant_neighbors = neighbor_distances.loc[
            neighbor_distances["distance_meter"] < max_distance, :
        ]
        if relevant_neighbors.empty:
            return pd.DataFrame()

        return pd.merge(
            relevant_neighbors.loc[:, "distance_meter"],
            self.stations_gdf,
            how="left",
            left_index=True,
            right_index=True,
        )

    def _determine_duplicates(
        self,
        current_station: pd.Series,
        duplicate_candidates: pd.DataFrame,
        score_threshold: float = 0.49,
        max_distance: int = 100,
        score_weights: Union[Dict[str, float], None] = None,
    ) -> pd.DataFrame:
        score_weights = (
            score_weights
            if score_weights
            else dict(operator=0.2, address=0.1, distance=0.7)
        )

        duplicate_candidates["operator_match"] = duplicate_candidates.operator.apply(
            lambda x: SequenceMatcher(None, current_station.operator, str(x)).ratio()
            if (current_station.operator is not None) & (x is not None)
            else 0.0
        )

        current_station_address = f"{current_station['street']}{current_station['postcode']}{current_station['town']}"
        duplicate_candidates["address"] = duplicate_candidates[
            ["street", "postcode", "town"]
        ].apply(lambda x: f"{x['street']}{x['postcode']}{x['town']}", axis=1)
        duplicate_candidates["address_match"] = duplicate_candidates.address.apply(
            lambda x: SequenceMatcher(None, current_station_address, x).ratio()
            if (current_station_address != "NoneNoneNone") & (x != "NoneNoneNone")
            else 0.0,
        )

        operator_score = (
            score_weights["operator"] * duplicate_candidates["operator_match"]
        )
        address_score = score_weights["address"] * duplicate_candidates["address_match"]
        distance_score = score_weights["distance"] * (
            1 - duplicate_candidates["distance_meter"] / max_distance
        )
        duplicate_candidates["matching_score"] = (
            operator_score + address_score + distance_score
        )
        duplicate_candidates.loc[
            (duplicate_candidates.matching_score > score_threshold), "is_duplicate"
        ] = True
        return duplicate_candidates.loc[duplicate_candidates.is_duplicate, :]

    def _merge_duplicates(
        self, current_station: pd.Series, duplicates: pd.DataFrame
    ) -> "Merger":
        """
        Simple procedure, which follows BNA > OCM > OSM.

        :param current_station: pd.Series contianing current station.
        :param duplicates: pd.DataFrame containing all duplicates.
        :return:
        """

        def merge_attributes(station: pd.Series, duplicates_to_merge: pd.DataFrame):
            """
            Might be used in the future.

            :param station:
            :param duplicates_to_merge:
            :return:
            """
            for att_name in [
                "amperage",
                "operator",
                "payment",
                "socket_type",
                "authentication",
                "capacity",
                "voltage",
            ]:
                att_values = duplicates_to_merge[att_name].dropna().unique().tolist()
                att_values = [str(x) for x in att_values if len(str(x)) > 0]
                if att_name in station.dropna():
                    att_value = str(station[att_name])
                    att_values += (
                        [att_value] if ";" not in att_value else att_value.split(";")
                    )
                att_values = set(att_values)
                new_value = (
                    ";".join([str(x) for x in att_values]) if att_values else None
                )
                station.at[att_name] = new_value
            station.at["merged_attributes"] = True
            return

        if current_station["data_source"] == "BNA":
            # in case of BNA vs OCM / OSM we always stick with BNA (we do not need to do anything)
            # TODO: check for missing attributes and merge in smart way
            return self
        data_sources = ["BNA", "OCM", "OSM"]
        # generate data source pairs following preference ordering: BNA > OCM > OSM
        ordered_data_source_pairs = [
            (x, y) for x in data_sources for y in data_sources if x != "BNA"
        ]
        for (current_data_source, duplicate_data_source) in ordered_data_source_pairs:
            if current_station["data_source"] != current_data_source:
                continue
            mergeable_stations = duplicates.loc[
                duplicates["data_source"] == duplicate_data_source
            ]
            if mergeable_stations.empty:
                continue

            selected_station = mergeable_stations.iloc[0][current_station.index].copy()
            selected_station.loc[["is_duplicate", "merged_attributes"]] = False, True
            self.stations_gdf.loc[current_station.name] = selected_station

            break
        return self

    def merge(
        self,
        stations_list: List[Dict] = None,
        score_threshold: float = 0.49,
        max_distance: int = 100,
        score_weights: Union[Dict, None] = None,
    ) -> "Merger":
        score_weights = (
            score_weights
            if score_weights
            else dict(operator=0.2, address=0.1, distance=0.7)
        )

        if stations_list is not None:
            if len(stations_list) < 1:
                raise RuntimeError("Your provided list of stations is empty!")
            self.data_sources = stations_list
        if (stations_list is None) & (len(self.data_sources) < 1):
            try:
                self._load_data()
            except Exception as anyErr:
                log.error(f"Could not load the processed station files! {anyErr}")

        self.stations_gdf = self._prepare_geodataframe()
        self.knn3 = libpysal.weights.KNN.from_dataframe(self.stations_gdf, k=40)

        for idx in tqdm(range(self.stations_gdf.shape[0])):
            current_station: pd.Series = self.stations_gdf.iloc[idx]
            if current_station["is_duplicate"]:
                continue
            duplicate_candidates: pd.DataFrame = self._get_duplicate_candidates(
                current_station=current_station, max_distance=max_distance
            )
            if duplicate_candidates.empty:
                continue

            duplicates: pd.DataFrame = self._determine_duplicates(
                current_station=current_station,
                duplicate_candidates=duplicate_candidates,
                score_threshold=score_threshold,
                max_distance=max_distance,
                score_weights=score_weights,
            )

            self.stations_gdf.loc[
                self.stations_gdf.index.isin(duplicates.index), "is_duplicate",
            ] = True

            self._merge_duplicates(current_station, duplicates)

        self.merged_stations_gdf: gpd.GeoDataFrame = self.stations_gdf.loc[
            ~self.stations_gdf["is_duplicate"], :
        ]

        return self


if __name__ == "__main__":
    import matplotlib.pyplot as plt

    merger: Merger = Merger()
    merger.merge()
    # stations: gpd.GeoDataFrame = merger.merged_stations_gdf
    # stations.to_pickle("../../data/stations__merged.pkl")
    stations = gpd.GeoDataFrame(
        pd.read_pickle("../../../data/stations__merged.pkl"),
        geometry="geometry",
        crs={"init": "epsg:5243"},
    )

    stations["lat"] = None
    stations["lon"] = None
    stations[["lat", "lon"]] = stations.geometry.apply(lambda x: pd.Series([x.y, x.x]))
    stations.to_csv("../../data/kepler_charging_map.csv")

    attribute_missing_statistics = pd.DataFrame()
    station_group_by_source = stations.groupby("data_source")
    for (data_source, data) in station_group_by_source:
        statistics_df = data[
            [
                "operator",
                "payment",
                "authentication",
                "kw_list",
                "ampere_list",
                "volt_list",
                "socket_type_list",
                "dc_support",
                "total_kw",
                "max_kw",
                "street",
                "town",
                "postcode",
                "district",
                "state",
                "country",
            ]
        ].isnull().sum() / len(data)
        statistics_df = statistics_df.append(
            pd.Series(
                dict(samples=len(data), merged_samples=data.merged_attributes.sum())
            )
        )
        attribute_missing_statistics = attribute_missing_statistics.assign(
            **{f"{data_source}": statistics_df}
        )

    log.debug(
        f"\nMissing Attribute Statistics by Data Source:\n{attribute_missing_statistics}"
    )

    # # distance = distance(stations.geometry, Point(49.0811, 9.19813))
    # # print(distance.head())
    states = gpd.read_file("../../../data/shapefiles/DEU_adm0.shp")
    roads = gpd.read_file("../../../data/shapefiles/DEU_roads.shp")
    railroads = gpd.read_file("../../../data/shapefiles/DEU_rails.shp")

    ax = states.plot(color="gray", edgecolor="black", label="borders")
    roads.plot(color="blue", edgecolor="black", ax=ax, label="roads")
    railroads.plot(color="green", edgecolor="black", ax=ax, label="rails")
    stations.plot(ax=ax, color="red", label="charging stations")

    plt.legend(loc="upper right")
    plt.show()

    print("done")
