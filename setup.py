import codecs
import os
from setuptools import setup, find_packages


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


# Get the long description from the README file
name = "charging_stations"  # TODO: rename properly!
version = get_version("src/charging_stations/__init__.py")
description = "Pull, clean and merge charging station information from various sources."
long_description = read("README.md")
long_description_content_type = "text/markdown"
url = "https://github.com/deepatlas/da-charging-connectors"
author = "Markus Steger"
author_email = "m.steger@reply.de"
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "Topic :: Software Development",
    "Operating System :: OS Independent",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3 :: Only",
]
keywords = "electric vehicle, charging stations, germany"
packages = find_packages(where="src")
package_dir = {"": "src"}
python_requires = ">=3.5, <4"
install_requirements = read("requirements.txt")
package_data = {
    "data": [
        "data/test_BNA__processed.json",
        "data/test_BNA__raw.json",
        "data/test_OCM__processed.json",
        "data/test_OCM__raw.json",
        "data/test_OSM__processed.json",
        "data/test_OSM__raw.json",
    ],
}
project_urls = {  # Optional
    "Bug Reports": "https://github.com/deepatlas/da-charging-connectors/issues",
    "Funding": "https://donate.pypi.org",
    "Say Thanks!": "https://www.deeptlas.io",
    "Source": "https://github.com/deepatlas/da-charging-connectors",
}

setup(
    name=name,  # Required
    version=version,  # Required
    description=description,  # Optional
    long_description=long_description,  # Optional
    long_description_content_type=long_description_content_type,  # Optional (see note above)
    url=url,  # Optional
    author=author,  # Optional
    author_email=author_email,  # Optional
    classifiers=classifiers,  # Optional
    keywords=keywords,  # Optional
    packages=packages,  # Required
    package_dir=package_dir,
    python_requires=python_requires,
    install_requires=install_requirements,  # Optional
    package_data=package_data,  # Optional
    project_urls=project_urls,
)
