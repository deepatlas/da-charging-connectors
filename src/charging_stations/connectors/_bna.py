import hashlib
import io
import json
import math
import os
import pandas as pd
import requests
import yarl
from typing import Dict, List, Union
from numbers import Number
from bs4 import BeautifulSoup, ResultSet
from ._ocm import OCMConnector
from ..helpers import get_logger
from ..helpers import default

log = get_logger(os.path.basename(__file__))


class BNAConnector(OCMConnector):
    __data_source__: str = "BNA"

    def get_data(self, to_disk: bool = False):
        """
        Retrieves charging station data in excel format from Bundesnetzagentur and converts it first to a pd.DataFrame
        (for some easy plausibility checks and transformations) and then to dictionary for further processing.

        :param to_disk: If true, will save data to file.
        :return:
        """
        headers = {"User-Agent": "Mozilla/5.0"}
        response: requests.Response = self.http_method_fn(
            self.url, params=self.query_params, headers=headers
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to get data! Status Code: {response.status_code}"
            )
        result: ResultSet = BeautifulSoup(response.content, "html.parser").find_all(
            "a", class_="downloadLink Publication " + "FTxlsx"
        )
        if len(result) != 1:
            raise RuntimeError("Could not identify link!")
        rel_link: str = result[0].get("href", None)
        if rel_link is None:
            raise RuntimeError("Could not retrieve href from link!")
        url: yarl.URL = yarl.URL(self.url)
        xlsx_file: requests.Response = self.http_method_fn(
            f"{url.scheme}://{url.host}{rel_link}",
            params=self.query_params,
            headers=headers,
        )
        xlsx_pd: pd.DataFrame = pd.read_excel(
            io.BytesIO(xlsx_file.content), engine="xlrd"
        )
        data_columns: pd.DataFrame = xlsx_pd.loc[
            xlsx_pd[xlsx_pd.columns[0]] == "Betreiber"
        ]
        if data_columns.shape[0] != 1:
            raise RuntimeError("Could not find start of data!")

        bna_data: pd.DataFrame = pd.DataFrame(
            xlsx_pd.iloc[data_columns.index[0] + 1 :, : data_columns.shape[1]].values,
            columns=list(data_columns.values[0]),
        )
        self.raw_data: List[Dict] = bna_data.to_dict(orient="record")
        if to_disk:
            file_path: str = os.path.join(
                self.base_path, f"{self.__data_source__}__raw.json"
            )
            self._save(file_path=file_path, content_list=self.raw_data)

    def process(self, to_disk: bool = False):
        if not self.raw_data:
            raise RuntimeError("Load or get raw data first!")
        station_raw: Dict
        for station_raw in self.raw_data:
            raw_data: str = json.dumps(
                station_raw, sort_keys=True, ensure_ascii=True, default=default
            )
            id_hash: hashlib._Hash = hashlib.sha256(
                f"{station_raw['Längengrad [DG]']}{station_raw['Breitengrad [DG]']}".encode(
                    "utf8"
                )
            )
            identifier: bytes = id_hash.hexdigest().encode("utf8")
            try:
                address: Dict = self._create_address(identifier, station_raw)
            except Exception as addressConversionErr:
                log.error(
                    f"Failed to create address object: {addressConversionErr}! Will skip this station!"
                )
                continue
            try:
                charging: Dict = self._create_charging(identifier, station_raw)
            except Exception as chargingConversionErr:
                log.error(
                    f"Failed to create charging object: {chargingConversionErr}! Will skip this station!"
                )
                continue

            try:
                station: Dict = self._create_station(
                    address, charging, identifier, raw_data, station_raw
                )
            except Exception as stationConversionErr:
                log.error(
                    f"Failed to create station object: {stationConversionErr}! Will skip this station!"
                )
                continue

            self.processed_data += [station]

        if to_disk:
            file_path: str = os.path.join(
                self.base_path, f"{self.__data_source__}__processed.json"
            )
            self._save(file_path=file_path, content_list=self.processed_data)

    def _create_station(
        self,
        address: Dict,
        charging: Dict,
        identifier: bytes,
        raw_data: str,
        station_raw: Dict,
    ) -> Dict:
        payment = None
        authentication = None
        latitude: float = self.check_coordinates(station_raw["Breitengrad [DG]"])
        longitude: float = self.check_coordinates(station_raw["Längengrad [DG]"])
        coordinates: str = f"POINT({longitude} {latitude})"
        station: Dict = dict(
            id=identifier,
            data_source=self.__data_source__,
            address=address,
            charging=charging,
            operator=station_raw["Betreiber"],
            payment=payment,
            authentication=authentication,
            coordinates=coordinates,
            raw_data=raw_data,
        )
        return station

    def _create_charging(self, identifier: bytes, station_raw: Dict) -> Dict:
        total_kw: Union[float, None] = station_raw.get("Anschlussleistung [kW]", None)
        if isinstance(total_kw, str):
            try:
                total_kw = float(total_kw.replace(",", "."))
                log.debug(f"Converting total_kw from string {total_kw} to int!")
            except Exception as conversionErr:
                log.warning(
                    f"Failed to convert string {total_kw} to Number! Will set total_kw to None! {conversionErr}"
                )
                total_kw = None
        if isinstance(total_kw, Number):
            if math.isnan(total_kw):
                log.warn("Found nan in total_kw! Will set total_kw to None!")
                total_kw = None
        if not isinstance(total_kw, Number):
            log.warn(
                f"Cannot process total_kw {total_kw} with type {type(total_kw)}! Will set total_kw to None!"
            )
            total_kw = None

        # kw_list
        kw_list: List[float] = []
        for k, v in station_raw.items():
            if not (("P" in k) & ("[kW]" in k)):
                continue
            if pd.isnull(v) | pd.isna(v):
                continue
            if isinstance(v, str):
                if "," in v:
                    v: str = v.replace(",", ".")
                    log.debug(
                        "Replaced coma with point for string to float conversion of kw!"
                    )
                try:
                    float_kw: float = float(v)
                    kw_list += [float_kw]
                except:
                    log.warn(
                        f"Failed to convert kw string {v} to float! Will not add this kw entry to list!"
                    )
            if isinstance(v, Number):
                kw_list += [v]

        capacity: Union[int, None] = station_raw.get("Anzahl Ladepunkte")
        if len(kw_list) != station_raw.get("Anzahl Ladepunkte"):
            log.warning(f"kw_list {kw_list} length does not equal capacity {capacity}!")

        # ampere_list not available
        # volt_list not available
        # socket_type_list
        socket_types_infos: List[str] = [
            v
            for k, v in station_raw.items()
            if ("Steckertypen" in k) & (isinstance(v, str)) & (not pd.isnull(v))
        ]
        socket_type_list: List[str] = []
        dc_support: bool = False
        for socket_types_info in socket_types_infos:
            tmp_socket_info: List[str] = socket_types_info.split(",")
            if (not dc_support) & (
                any(["DC" in s for s in tmp_socket_info])
            ):  # TODO: find more reliable way!
                dc_support = True
            socket_type_list += socket_types_info.split(",")
        kw_list_len: int = len(kw_list)
        if len(kw_list) != capacity:
            log.warning(
                f"Difference between length of kw_list {kw_list_len} and capacity {capacity}!"
            )
        charging: Dict = dict(
            station_id=identifier,
            capacity=capacity,
            kw_list=kw_list,
            ampere_list=None,
            volt_list=None,
            socket_type_list=socket_type_list,
            dc_support=dc_support,
            total_kw=total_kw,
            max_kw=max(kw_list) if kw_list else None,
        )
        return charging

    def _create_address(self, identifier: bytes, station_raw: Dict) -> Dict:
        postcode: Union[str, None]
        town: Union[str, None]
        state: str
        country: str
        street: str = station_raw.get("Adresse")
        postcode_town: str = station_raw.get("Postleitzahl Ort") if (
            station_raw.get("Postleitzahl Ort") is not None
        ) & (isinstance(station_raw.get("Postleitzahl Ort"), str)) else None
        postcode = ""
        town = ""
        for s in postcode_town:
            if s.isspace():
                continue
            if s.isdigit():
                postcode += s
                continue
            town += s
        if len(postcode) != 5:
            log.warning(
                f"Failed to process postcode {postcode} from {postcode_town}! Will set postcode to None!"
            )
            postcode = None
        if len(town) < 2:
            log.warning(
                f"Failed to process town {town} from {postcode_town}! Will set town to None!"
            )
            town = None
        address: Dict = dict(
            station_id=identifier,
            street=street,
            town=town,
            postcode=postcode,
            district=None,
            state=station_raw.get("Bundesland"),
            country="DE",
        )
        return address


if __name__ == "__main__":
    print("done")
