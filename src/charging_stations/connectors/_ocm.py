import hashlib
import json
import os
import requests
from numbers import Number
from typing import Callable, Dict, List, Optional
from ..helpers import get_logger, default
from ._connector import Connector

log = get_logger(os.path.basename(__file__))


class OCMConnector(Connector):
    __data_source__ = "OCM"
    raw_data: List[Dict] = None
    processed_data: List[Dict] = None

    def __init__(
        self,
        url: str,
        http_method_fn: Callable,
        base_path: str,
        query_params: Dict[str, any] = None,
    ):
        self.url: str = url
        self.http_method_fn: Callable = http_method_fn
        self.raw_data: List[any] = []
        self.processed_data: List[Dict] = []
        if not os.path.exists(base_path):
            os.makedirs(base_path)
        self.base_path: str = base_path
        self.query_params: Dict[str, any] = query_params

    def get_data(self, to_disk: bool = False):
        response: requests.Response = self.http_method_fn(
            self.url, params=self.query_params
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to get data! Status Code: {response.status_code}"
            )
        self.raw_data: Dict[str, any] = response.json()
        if to_disk:
            file_path: str = os.path.join(
                self.base_path, f"{self.__data_source__}__raw.json"
            )
            self._save(file_path=file_path, content_list=self.raw_data)

    def load(self, is_processed: bool = False, is_test: bool = False):
        file_name: str = f"{self.__data_source__}__{'processed' if is_processed else 'raw'}.json"
        if is_test:
            file_name = f"test_{file_name}"
        file_path: str = os.path.join(self.base_path, file_name)
        content: List[Dict] = self._load(file_path=file_path)

        if is_processed:
            self.processed_data = content
        else:
            self.raw_data: Dict[str, any] = content

    def process(self, to_disk: bool = False):
        if not self.raw_data:
            raise RuntimeError("Load or get raw data first!")

        for station_raw in self.raw_data:
            addressInfo: Optional[Dict] = station_raw.get("AddressInfo")
            raw_data: str = json.dumps(
                station_raw, sort_keys=True, ensure_ascii=True, default=default
            )
            ocm_id: Optional[int] = addressInfo.get("ID")
            id_hash: hashlib._Hash = hashlib.sha256(
                str(ocm_id).encode("utf8")
                if ocm_id is not None
                else f"{station_raw['AddressInfo']['Longitude']}{station_raw['AddressInfo']['Latitude']})".encode(
                    "utf8"
                )
            )
            identifier: bytes = id_hash.hexdigest().encode("utf8")

            try:
                address: Dict = self._create_address(
                    addressInfo, identifier, station_raw
                )
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

    @staticmethod
    def check_coordinates(coords: float) -> float:
        if isinstance(coords, str):
            log.warn(f"Coords are string: {coords} will be transformed!")
            coords = float(
                "".join(
                    [s for s in coords.replace(",", ".") if (s.isdigit()) | (s == ".")]
                )
            )
        if not isinstance(coords, Number):
            raise ValueError("Coordinatess could not be read propery!")
        return coords

    def _create_station(
        self,
        address: Dict,
        charging: Dict,
        identifier: bytes,
        raw_data: str,
        station_raw: Dict,
    ) -> Dict:
        latitude: float = self.check_coordinates(station_raw["AddressInfo"]["Latitude"])
        longitude: float = self.check_coordinates(
            station_raw["AddressInfo"]["Longitude"]
        )
        coordinates: str = f"POINT({longitude} {latitude})"
        authentication: str = ";".join(
            [f"{k}:{v}" for k, v in station_raw["UsageType"].items()]
        ) if isinstance(station_raw["UsageType"], dict) else None
        operator: Optional[str] = station_raw["OperatorInfo"].get(
            "Title", None
        ) if isinstance(station_raw["OperatorInfo"], dict) else None
        station: Dict = dict(
            id=identifier,
            data_source=self.__data_source__,
            address=address,
            charging=charging,
            operator=operator,
            payment=station_raw.get("UsageCost"),
            authentication=authentication,
            coordinates=coordinates,
            raw_data=raw_data,
        )
        return station

    def _create_charging(self, identifier: bytes, station_raw: Dict) -> Dict:
        # TODO: compute kW if missing and possible
        connections: List[Dict] = station_raw.get("Connections")
        capacity: int = station_raw.get("NumberOfPoints")
        kw_list: List[float] = []
        ampere_list: List[float] = []
        volt_list: List[float] = []
        socket_type_list: List[str] = []
        if connections is not None:
            for connection in connections:
                currentType: Dict = connection.get("CurrentType")
                if currentType is not None:
                    socket_title: Optional[str] = currentType.get("Title")
                    if socket_title is not None:
                        socket_type_list += [socket_title]
                kw: Optional[float] = connection.get("PowerKW")
                ampere: Optional[float] = connection.get("Amps")
                volt: Optional[float] = connection.get("Voltage")
                quantity: Optional[int] = connection.get("Quantity") if connection.get(
                    "Quantity"
                ) is not None else 1
                metric_list: List[List]
                metric: float
                for metric_list, metric in zip(
                    [kw_list, ampere_list, volt_list], [kw, ampere, volt]
                ):
                    if metric is None:
                        continue
                    metric_list += [metric] * quantity
        dc_support: bool = any(["DC" in s for s in socket_type_list])
        charging: Dict = dict(
            station_id=identifier,
            capacity=capacity,
            kw_list=kw_list if kw_list else None,
            ampere_list=None,
            volt_list=None,
            socket_type_list=socket_type_list,
            dc_support=dc_support,
            total_kw=sum(kw_list),
            max_kw=max(kw_list) if kw_list else None,
        )
        return charging

    def _create_address(
        self, addressInfo: Dict, identifier: bytes, station_raw: Dict
    ) -> Dict:
        country: Optional[Dict] = addressInfo.get("Country")
        postcode: Optional[str] = addressInfo.get(
            "Postcode",
        ) if addressInfo is not None else None
        postcode = (
            "".join([s for s in postcode if s.isdigit()])
            if postcode is not None
            else ""
        )
        town: Optional[str] = addressInfo.get(
            "Town",
        ) if addressInfo is not None else None
        if town is None:
            town = ""
        state: Optional[str] = addressInfo.get(
            "StateOrProvince"
        ) if addressInfo is not None else None
        if state is None:
            state = ""
        country: Optional[str] = country.get("ISOCode") if country is not None else None
        street: Optional[str] = addressInfo.get(
            "street"
        ) if addressInfo is not None else None
        if len(postcode) != 5:
            log.warning(
                f"Postcode {postcode} of town {town} is not of length 5! Will set postcode to None!"
            )
            postcode = None
        if (len(town) < 2) | (not all(not s.isdigit() for s in town)):
            log.warning(
                f"Town {town} has less than 2 chars or contains digits! Will set town to None!"
            )
            town = None
        if (not all(not s.isdigit() for s in state)) | (len(state) < 2):
            log.warning(
                f"State {state} contains digits or has less than 2 chars! Will set state to None!"
            )
            state = None
        address: Dict = dict(
            station_id=identifier,
            street=street,
            town=town,
            postcode=postcode,
            district=None,
            state=state,
            country=country,
        )
        return address


if __name__ == "__main__":
    print("done")
