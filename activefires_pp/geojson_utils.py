#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 - 2026 Adam Dybbroe

# Author(s):

#   Adam Dybbroe <Firstname.Lastname@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Geojson utilities."""

import os
import pathlib
import pyproj
import geojson
from geojson import Feature, Point, FeatureCollection, dump
import json
import logging
from trollsift import Parser, globify
import pytz
from datetime import datetime

import numpy as np
from activefires_pp.utils import json_serial

logger = logging.getLogger(__name__)

PROPERTY_MAP = {
    "power": "power",
    "tb": "tb",
    "confidence": lambda row: int(row.conf),
    "observation_time": lambda row: json_serial(
        row.starttime.to_pydatetime()
        + (row.endtime.to_pydatetime() - row.starttime.to_pydatetime()) / 2
    ),
}

OPTIONAL_PROPERTY_MAP = {
    "anomaly": lambda row: int(row.anomaly),
    "tb_celcius": "tb_celcius",
    "id": "detection_id",
}


def read_geojson_data(filename):
    """Read Geo json data from file."""
    if str(filename).endswith('.geojson') and filename.exists():
        # Read the file:
        try:
            with open(filename, "r") as fpt:
                return geojson.load(fpt)
        except json.decoder.JSONDecodeError:
            logger.exception("Geojson file invalid and cannot be read: %s", str(filename))
    else:
        logger.error("No valid filename to read: %s", str(filename))


def get_geojson_files_in_observation_time_order(path, pattern, time_interval):
    """Get all geojson files with filtered active fire detections (=triggered alarms) since *dtime*."""
    dtime_start = time_interval[0]
    dtime_end = time_interval[1]
    if not dtime_end:
        dtime_end = datetime.utcnow()

    p__ = Parser(pattern)
    files = path.glob(globify(pattern))
    dtimes = []
    fnames = []
    for gjson_file in files:
        fname = gjson_file.name
        res = p__.parse(fname)
        if res['start_time'] > dtime_start and res['start_time'] < dtime_end:
            dtimes.append(res['start_time'])
            fnames.append(fname)

    dtimes = np.array(dtimes)
    fnames = np.array(fnames)

    idx = dtimes.argsort()
    files = np.take(fnames, idx)
    return files.tolist()


def _resolve_property(row, source):
    """Resolve one property from a row.

    source may be:
      - a column/attribute name (str)
      - a callable taking row and returning a value
    """
    if callable(source):
        return source(row)
    return getattr(row, source)


def geojson_feature_collection_from_detections(
    detections,
    property_map,
    optional_property_map=None,
    lon_col="longitude",
    lat_col="latitude",
    platform_name=None
):
    """Create a GeoJSON FeatureCollection from a dataframe."""
    if detections.empty:
        raise ValueError("No detections to save!")

    optional_property_map = optional_property_map or {}
    features = []

    for row in detections.itertuples(index=False):
        props = {}

        # Required properties
        for out_name, source in property_map.items():
            props[out_name] = _resolve_property(row, source)

        # Optional properties
        for out_name, source in optional_property_map.items():
            try:
                props[out_name] = _resolve_property(row, source)
            except AttributeError:
                if logger:
                    logger.debug("Optional property '%s' not available", out_name)
            except Exception as exc:
                if logger:
                    logger.debug(
                        "Failed computing optional property '%s': %s",
                        out_name, exc
                    )

        # Static optional property
        if platform_name is not None:
            props["platform_name"] = platform_name
        elif logger:
            logger.debug("No platform name specified for output")

        features.append(
            Feature(
                geometry=Point((
                    float(getattr(row, lon_col)),
                    float(getattr(row, lat_col)),
                )),
                properties=props,
            )
        )

    return FeatureCollection(features)


def map_coordinates_in_feature_collection(feature_collection, epsg_str):
    """Map the Point coordinates of all data in Feature Collection."""
    outp = pyproj.Proj(epsg_str)

    mapped_features = []
    # Iterate through each feature of the feature collection
    for feature in feature_collection['features']:
        lon, lat = feature['geometry']['coordinates']
        prop = feature['properties']
        feature_out = Feature(geometry=Point(map(float, [lon, lat])), properties=prop)
        # Project/transform coordinate pairs of each Point
        result = outp(lon, lat)
        feature_out['geometry']['coordinates'] = [result[0], result[1]]
        mapped_features.append(feature_out)

    return FeatureCollection(mapped_features)


def store_geojson_alarm(fires_alarms_dir, file_parser, idx, alarm):
    """Store the fire alarm to a geojson file."""
    utc = pytz.timezone('utc')
    start_time = datetime.fromisoformat(alarm["features"]["properties"]["observation_time"])
    platform_name = alarm["features"]["properties"]["platform_name"]
    start_time = start_time.astimezone(utc).replace(tzinfo=None)
    fname = file_parser.compose({'start_time': start_time, 'id': idx,
                                 'platform_name': platform_name})
    output_filename = fires_alarms_dir / fname
    with open(output_filename, 'w') as fpt:
        dump(alarm, fpt)

    return output_filename


def store_geojson(output_filename, feature_collection):
    """Store the Geojson feature collection of fire detections on disk."""
    if isinstance(output_filename, str):
        output_filename = pathlib.Path(output_filename)
    elif isinstance(output_filename, pathlib.PosixPath):
        pass

    path = output_filename.parent
    if not os.path.exists(path):
        logger.info("Create directory: %s", path)
        os.makedirs(path)

    with open(output_filename, 'w') as fpt:
        dump(feature_collection, fpt)
