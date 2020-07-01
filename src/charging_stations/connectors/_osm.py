import hashlib
import json
import os
import string
import requests
from numbers import Number
from typing import Dict, List, Union, Callable
from charging_stations.connectors._ocm import OCMConnector
from charging_stations.helpers import get_logger, default

log = get_logger(os.path.basename(__file__))


class OSMConnector(OCMConnector):
    __data_source__ = "OSM"

    def get_data(self, to_disk: bool = False):
        response: requests.Response = self.http_method_fn(
            self.url, params=self.query_params
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to get data! Status Code: {response.status_code}"
            )
        self.raw_data: List[Dict] = response.json()["elements"]
        if to_disk:
            file_path: str = os.path.join(
                self.base_path, f"{self.__data_source__}__raw.json"
            )
            self._save(file_path=file_path, content_list=self.raw_data)

    @staticmethod
    def _string_to_number_list(
        list_string: str,
        transform_fn: Callable = int,
        separator_list: List[str] = [":", ",", "/"],
    ) -> Union[List[Number], None]:
        """
        Turns a string, which contains seperated digits e.g. "12;3,5;" with several seperators",
        into a list of e.g. int or floats - depending on transform_fn.

        :param list_string: string e.g. "12;3,5;"
        :param transform_fn: callable e.g. int or float. The transformation is expected to yield Number type!
        :param separator_list: list of seperators e.g. [";", ","]
        :return:
        """
        # TODO: Maybe, add special case for voltage e.g. 250 - 1000V!
        if not isinstance(transform_fn("12"), Number):
            raise RuntimeError(
                "Parameter transform_fn must be a callable function that returns type Number!"
            )
        if isinstance(list_string, type(None)):
            return list_string
        if isinstance(list_string, Number):
            log.debug(
                f"Unexpected type! {list_string} should be a string but is {type(list_string)}. Will return as list!"
            )
            return [list_string]
        if not isinstance(list_string, str):
            log.warning(
                f"Unexpected type! {list_string} should be a string but is {type(list_string)}. Skipping Entry!"
            )
            return None
        excluding_punctuation: set[str] = set(string.punctuation) - set(
            separator_list + [";"]
        )
        if any([s in excluding_punctuation for s in list_string]):
            log.warning(
                f"Cannot handle punctuation inside of {list_string}! Will return None!"
            )
            return None
        for sep in separator_list:
            if sep != ";":
                list_string = list_string.replace(sep, ";")

        clean_string_list: List[str] = [
            "".join([s for s in substring if s.isdigit()])
            for substring in list_string.split(";")
        ]

        clean_numbers_list: List[any] = [
            transform_fn(s) for s in clean_string_list if len(s) > 0
        ]
        return clean_numbers_list if clean_string_list else None

    def process(self, to_disk: bool = False):
        if not self.raw_data:
            raise RuntimeError("Load or get raw data first!")
        for station_raw in self.raw_data:
            raw_data: str = json.dumps(
                station_raw, sort_keys=True, ensure_ascii=True, default=default
            )

            osm_id: Union[int, None] = station_raw.get("id")
            id_hash: hashlib._Hash = hashlib.sha256(
                str(osm_id).encode("utf8")
                if osm_id is not None
                else f"{station_raw['lon']}{station_raw['lat']})".encode("utf8")
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
        latitude: float = self.check_coordinates(station_raw["lat"])
        longitude: float = self.check_coordinates(station_raw["lon"])
        coordinates: str = f"POINT({longitude} {latitude})"
        tags: Union[Dict, None] = station_raw.get("tags")
        authentication: Union[str, None] = None
        operator: Union[str, None] = None
        payment: Union[str, None] = None
        if tags is not None:
            auth_key_value_strings: List[str] = [
                f"{k.replace('authentication:','')}:{v}"
                for k, v in tags.items()
                if "auth" in k
            ]
            authentication = (
                ";".join(auth_key_value_strings) if auth_key_value_strings else None
            )
            operator = tags.get("operator")
            payment_key_value_strings: List[str] = [
                f"{k.replace('payment:','')}:{v}"
                for k, v in tags.items()
                if "payment" in k
            ]
            payment = (
                ";".join(payment_key_value_strings) if auth_key_value_strings else None
            )
        station: Dict = dict(
            id=identifier,
            data_source=self.__data_source__,
            address=address,
            charging=charging,
            operator=operator,
            payment=payment,
            authentication=authentication,
            coordinates=coordinates,
            raw_data=raw_data,
        )
        return station

    def _create_charging(self, identifier: bytes, station_raw: Dict) -> Dict:
        # TODO: kw_list computation, dc_support
        tags: Union[Dict, None] = station_raw.get("tags")
        capacity: Union[int, None] = None
        kw_list: List[float] = []
        ampere_list: Union[List[float], None] = []
        volt_list: Union[List[float], None] = []
        socket_type_list: List[str] = []
        dc_support: bool = False
        if tags is not None:
            capacity = tags.get("capacity")
            if not isinstance(capacity, int):
                try:
                    capacity = int(capacity)
                except:
                    log.warning(
                        f"Failed to convert capacity {capacity} to int! Will set to None!"
                    )
                    capacity = None
            amperage_string: str = tags.get("amperage")
            ampere_list = (
                self._string_to_number_list(
                    list_string=amperage_string, transform_fn=float
                )
                if amperage_string is not None
                else None
            )
            voltage_string: str = tags.get("voltage")
            volt_list = (
                self._string_to_number_list(
                    list_string=voltage_string, transform_fn=float
                )
                if voltage_string is not None
                else None
            )
            socket_type_list = [
                k.replace("socket:", "") for k, v in tags.items() if "socket:" in k
            ]

        charging: Dict = dict(
            station_id=identifier,
            capacity=capacity,
            kw_list=kw_list,
            ampere_list=ampere_list,
            volt_list=volt_list,
            socket_type_list=socket_type_list,
            dc_support=dc_support,
            total_kw=None,
            max_kw=max(kw_list) if kw_list else None,
        )

        return charging

    def _create_address(self, identifier: bytes, station_raw: Dict) -> Dict:
        tags: Dict = station_raw.get("tags", None)
        country: Union[str, None] = None
        street: Union[str, None] = None
        postcode: Union[str, None] = None
        town: Union[str, None] = None
        state: Union[str, None] = None
        house_number: Union[str, None] = None
        if tags is not None:
            country: Union[str, None] = tags.get(
                "addr:country", "DE"
            )  # TODO: check this one
            street: Union[str, None] = tags.get("addr:street")
            postcode: Union[str, None] = tags.get("addr:postcode")
            town: Union[str, None] = tags.get("addr:city", "")
            state: Union[str, None] = tags.get("addr:state", "")  # TODO: check this one
            house_number = tags.get("addr:housenumber") if tags is not None else None
        if town is None:
            town = ""
        if state is None:
            state = ""
        postcode = (
            "".join([s for s in postcode if s.isdigit()])
            if postcode is not None
            else ""
        )
        if len(postcode) != 5:
            log.warning(
                f"Postcode {postcode} of town {town} is not of length 5! Will set postcode to None!"
            )
            postcode = None
        if (street is not None) & (house_number is not None):
            street += f" {house_number}"
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
