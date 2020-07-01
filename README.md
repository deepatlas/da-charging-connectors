da-charging-connectors
============
[![GitHub Stars](https://img.shields.io/github/stars/deepatlas/da-charging-connectors?style=social)](https://github.com/deepatlas/da-charging-connectors/stargazers) [![GitHub Issues](https://img.shields.io/github/issues-raw/deepatlas/da-charging-connectors)](https://github.com/deepatlas/da-charging-connects/issues) [![GitHub Pulls](https://img.shields.io/github/issues-pr/deepatlas/da-charging-connectors)](https://github.com/deepatlas/da-charging-connects/pulls) [![Current Version](https://img.shields.io/badge/version-1.0.0-green.svg)](https://github.com/deepatlas/da-charging-data) [![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)

Collects, cleans and structures charging station information in and around Germany from various sources.
## Installation
```bash
pip install git+https://github.com/deepatlas/da-charging-connectors.git
```
## Usage
```python
import os
from charging_stations.connectors import BNAConnector, OCMConnector, OSMConnector, Merger, Config

CONNECTOR_CONFIGS = {"OCM": Config.OCM, "OSM": Config.OSM, "BNA": Config.BNA}

connectors = {
    c.__data_source__: c(
        base_path=os.path.realpath(os.path.join(os.path.dirname(__file__), "data")),
        **CONNECTOR_CONFIGS[c.__data_source__],
    )
    for c in [BNAConnector, OCMConnector, OSMConnector]
}

stations_list = []

for data_source, connector in connectors.items():
    connector.get_data(to_disk=False)
    connector.process(to_disk=False)

    stations_list += connector.processed_data

merger = Merger(
    base_path=os.path.realpath(os.path.join(os.path.dirname(__file__), "data"))
)

merger.merge(stations_list=stations_list)
stations = merger.merged_stations_gdf
```
## Development
Set src/ as Source Root!
### Testing
Based on pytest. Start from Project Root:
```bash
pytest
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
### Package
[MIT](https://choosealicense.com/licenses/mit/)

### Data Sources and Licenses
[Bundesnetzagentur](https://www.bundesnetzagentur.de):
- [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.en)
- Changes as described at [DeepAtlas](https://www.deepatlas.io/emobility/ladestationen-fur-elektroautos)

[OpenChargeMap](https://openchargemap.org):
- [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/)
- Changes as described at [DeepAtlas](https://www.deepatlas.io/emobility/ladestationen-fur-elektroautos)

[OpenStreetMap](https://www.openstreetmap.org):
- [ODbL](http://opendatacommons.org/licenses/odbl/1.0/)
