#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

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

"""Fixtures for unittests."""

import pytest


TEST_YAML_CONFIG_CONTENT = """# Publish/subscribe
subscribe_topics: /VIIRS/L2/Fires/PP/National
publish_topic: /VIIRS/L2/Fires/PP/SOSAlarm

geojson_file_pattern_alarms: sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson

fire_alarms_dir: /path/where/the/filtered/alarms/will/be/stored

restapi_url: "https://xxx.smhi.se:xxxx"

time_and_space_thresholds:
  hour_threshold: 6
  long_fires_threshold_km: 1.2
  spatial_threshold_km: 0.8
"""

TEST_POST_PROCESSING_YAML_CONFIG_CONTENT = """# Publish/subscribe
publish_topic: /VIIRS/L2/Fires/PP
subscribe_topics: VIIRS/L2/AFI

af_pattern_ibands: AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_e{end_hour:%H%M%S%f}_b{orbit:s}_c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt

geojson_file_pattern_national: AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}.geojson
geojson_file_pattern_regional: AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}_{region_name:s}.geojson

regional_shapefiles_format: omr_{region_code:s}_Buffer.{ext:s}

output_dir: /path/where/the/filtered/results/will/be/stored

timezone: Europe/Stockholm
"""  # noqa

TEST_YAML_TOKENS = """xauth_tokens:
  x-auth-satellite-alarm : 'my-token'
"""

# AFIMG_NOAA-20_20210619_005803_sweden.geojson
TEST_GEOJSON_FILE_CONTENT_MONSTERAS = """{"type": "FeatureCollection", "features":
[{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.240452, 57.17329]},
"properties": {"power": 4.19946575, "tb": 336.38024902, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.247334, 57.172443]},
"properties": {"power": 5.85325146, "tb": 339.84768677, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.242519, 57.17498]},
"properties": {"power": 3.34151864, "tb": 316.57772827, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.249384, 57.174122]},
"properties": {"power": 3.34151864, "tb": 310.37808228, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.241102, 57.171574]},
"properties": {"power": 3.34151864, "tb": 339.86465454, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.247967, 57.170712]},
"properties": {"power": 3.34151864, "tb": 335.95074463, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.246538, 57.167309]},
"properties": {"power": 3.10640526, "tb": 337.62503052, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.239674, 57.168167]},
"properties": {"power": 3.10640526, "tb": 305.36495972, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.245104, 57.163902]},
"properties": {"power": 3.10640526, "tb": 336.21279907, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.251965, 57.16304]},
"properties": {"power": 2.40693879, "tb": 306.66555786, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.250517, 57.159637]},
"properties": {"power": 2.23312426, "tb": 325.92211914, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.24366, 57.160496]},
"properties": {"power": 1.51176202, "tb": 317.16009521, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.242212, 57.157097]},
"properties": {"power": 1.51176202, "tb": 303.77804565, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.249069, 57.156235]},
"properties": {"power": 2.23312426, "tb": 310.37322998, "confidence": 8,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}]}"""

TEST_GEOJSON_FILE_CONTENT = """{"type": "FeatureCollection", "features":
[{"type": "Feature", "geometry": {"type": "Point", "coordinates": [23.562864, 67.341919]},
"properties": {"power": 1.62920368, "tb": 325.2354126, "confidence": 8,
"observation_time": "2022-06-29T14:01:08.850000", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [23.56245, 67.347328]},
"properties": {"power": 3.40044808, "tb": 329.46963501, "confidence": 8,
"observation_time": "2022-06-29T14:01:08.850000", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [23.555086, 67.343231]},
"properties": {"power": 6.81757641, "tb": 334.62347412, "confidence": 8,
"observation_time": "2022-06-29T14:01:08.850000", "platform_name": "NOAA-20"}}]}"""

# Past alarms:
PAST_ALARMS_MONSTERAS1 = """{"type": "FeatureCollection", "features":
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.246222, 57.175987]},
"properties": {"power": 1.83814871, "tb": 302.3949585, "confidence": 8,
"observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP", "related_detection": true}}}"""

PAST_ALARMS_MONSTERAS2 = """{"type": "FeatureCollection", "features":
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.245516, 57.1651]},
"properties": {"power": 2.94999027, "tb": 324.5098877, "confidence": 8,
"observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP", "related_detection": true}}}"""

PAST_ALARMS_MONSTERAS3 = """{"features": {"geometry": {"coordinates": [16.252192, 57.15242], "type": "Point"},
"properties": {"confidence": 8, "observation_time": "2021-06-18T14:49:01.750000+02:00",
"platform_name": "NOAA-20", "related_detection": false, "power": 2.87395763, "tb": 330.10293579},
"type": "Feature"}, "type": "FeatureCollection"}"""


@pytest.fixture
def fake_token_file(tmp_path):
    """Write fake token file."""
    file_path = tmp_path / '.sometokenfile.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_TOKENS)

    yield file_path


@pytest.fixture
def fake_yamlconfig_file(tmp_path):
    """Write fake yaml config file."""
    file_path = tmp_path / 'test_alarm_filtering_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT)

    yield file_path


@pytest.fixture
def fake_yamlconfig_file_post_processing(tmp_path):
    """Write fake yaml config file."""
    file_path = tmp_path / 'test_af_post_processing_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_POST_PROCESSING_YAML_CONFIG_CONTENT)

    yield file_path


@pytest.fixture
def fake_geojson_file_many_detections(tmp_path):
    """Write fake geojson file with many close detections."""
    file_path = tmp_path / 'test_afimg_NOAA-20_20210619_005803_sweden.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_GEOJSON_FILE_CONTENT_MONSTERAS)

    yield file_path


@pytest.fixture
def fake_geojson_file(tmp_path):
    """Write fake geojson file."""
    file_path = tmp_path / 'test_afimg_20220629_120026.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_GEOJSON_FILE_CONTENT)

    yield file_path


@pytest.fixture
def fake_past_detections_dir(tmp_path):
    """Create fake directory with past detections."""
    past_detections_dir = tmp_path / 'past_detections'
    past_detections_dir.mkdir()
    file_path = past_detections_dir / 'sos_20210619_000651_0.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(PAST_ALARMS_MONSTERAS1)

    file_path = past_detections_dir / 'sos_20210619_000651_1.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(PAST_ALARMS_MONSTERAS2)

    file_path = past_detections_dir / 'sos_20210618_124819_0.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(PAST_ALARMS_MONSTERAS2)

    yield file_path.parent


@pytest.fixture
def fake_national_borders_shapefile(tmp_path):
    """Write fake national borders shape file."""
    file_path = tmp_path / 'some_national_borders_shape.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write('')

    yield file_path
