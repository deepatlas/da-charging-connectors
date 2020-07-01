import requests

OCM = {
    "url": "https://api.openchargemap.io/v3/poi/",
    "http_method_fn": requests.get,
    "query_params": {
        "opendata": True,
        "output": "json",
        "countrycode": "DE",
        "compact": False,
        "maxresults": int(1e5),
    },
}

OSM = {
    "url": "http://overpass-api.de/api/interpreter",
    "http_method_fn": requests.get,
    "query_params": {
        "data": f"""
    [out:json];
area[name="Deutschland"];
// gather results
(
  // query part for: “"charging station"”
  node["amenity"="charging_station"](area);
  way["amenity"="charging_station"](area);
  rel["amenity"="charging_station"](area);

);
    out;
    """
    },
}

BNA = {
    "url": "https://www.bundesnetzagentur.de/DE/Sachgebiete/ElektrizitaetundGas/Unternehmen_Institutionen"
    + "/HandelundVertrieb/Ladesaeulenkarte/Ladesaeulenkarte_node.html",
    "http_method_fn": requests.get,
    "query_params": None,
}

CONNECTOR_CONFIGS = {"OCM": OCM, "OSM": OSM, "BNA": BNA}
