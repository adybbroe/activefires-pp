#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Adam Dybbroe

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

"""Testing the spatio-temporal alarm filtering
"""

import pytest
import logging
from unittest.mock import patch
import pathlib
from geojson import FeatureCollection
from geojson import dump
import json
from activefires_pp.geojson_utils import read_geojson_data
from activefires_pp.spatiotemporal_alarm_filtering import create_alarms_from_fire_detections
from activefires_pp.spatiotemporal_alarm_filtering import join_fire_detections
from activefires_pp.spatiotemporal_alarm_filtering import split_large_fire_clusters
from activefires_pp.spatiotemporal_alarm_filtering import create_one_detection_from_collection
from activefires_pp.spatiotemporal_alarm_filtering import create_single_point_alarms_from_collections
from activefires_pp.spatiotemporal_alarm_filtering import AlarmFilterRunner
from activefires_pp.api_posting import post_alarm


TEST_GEOJSON_FILE_CONTENT = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [23.562864, 67.341919]}, "properties": {"power": 1.62920368, "tb": 325.2354126, "confidence": 8, "observation_time": "2022-06-29T14:01:08.850000", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [23.56245, 67.347328]}, "properties": {"power": 3.40044808, "tb": 329.46963501, "confidence": 8, "observation_time": "2022-06-29T14:01:08.850000", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [23.555086, 67.343231]}, "properties": {"power": 6.81757641, "tb": 334.62347412, "confidence": 8, "observation_time": "2022-06-29T14:01:08.850000", "platform_name": "NOAA-20"}}]}"""

# afimg_20220629_110913.geojson
TEST_GEOJSON_FILE_CONTENT2 = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [20.629259, 65.48558]}, "properties": {"power": 21.38915062, "tb": 343.66696167, "confidence": 8, "observation_time": "2022-06-29T13:09:55.350000", "platform_name": "Suomi-NPP"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [20.627005, 65.489014]}, "properties": {"power": 26.42259789, "tb": 346.634552, "confidence": 8, "observation_time": "2022-06-29T13:09:55.350000", "platform_name": "Suomi-NPP"}}]}"""

# afimg_20220629_110748.geojson
TEST_GEOJSON_FILE_CONTENT3 = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [15.262635, 59.30434]}, "properties": {"power": 4.96109343, "tb": 337.80944824, "confidence": 8, "observation_time": "2022-06-29T13:08:30.150000", "platform_name": "Suomi-NPP"}}]}"""

# afimg_20220629_101926.geojson
TEST_GEOJSON_FILE_CONTENT4 = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [20.659672, 65.496849]}, "properties": {"power": 4.94186735, "tb": 334.15759277, "confidence": 8, "observation_time": "2022-06-29T12:20:08.800000", "platform_name": "NOAA-20"}}]}"""

# afimg_20220629_092938.geojson
TEST_GEOJSON_FILE_CONTENT5 = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [20.657578, 65.493927]}, "properties": {"power": 9.46942616, "tb": 341.33267212, "confidence": 8, "observation_time": "2022-06-29T11:30:20.950000", "platform_name": "Suomi-NPP"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [20.650656, 65.497543]}, "properties": {"power": 9.46942616, "tb": 348.74557495, "confidence": 8, "observation_time": "2022-06-29T11:30:20.950000", "platform_name": "Suomi-NPP"}}]}"""

# AFIMG_NOAA-20_20210619_005803_sweden.geojson
TEST_GEOJSON_FILE_CONTENT_MONSTERAS = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.240452, 57.17329]}, "properties": {"power": 4.19946575, "tb": 336.38024902, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.247334, 57.172443]}, "properties": {"power": 5.85325146, "tb": 339.84768677, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.242519, 57.17498]}, "properties": {"power": 3.34151864, "tb": 316.57772827, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.249384, 57.174122]}, "properties": {"power": 3.34151864, "tb": 310.37808228, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.241102, 57.171574]}, "properties": {"power": 3.34151864, "tb": 339.86465454, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.247967, 57.170712]}, "properties": {"power": 3.34151864, "tb": 335.95074463, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.246538, 57.167309]}, "properties": {"power": 3.10640526, "tb": 337.62503052, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.239674, 57.168167]}, "properties": {"power": 3.10640526, "tb": 305.36495972, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.245104, 57.163902]}, "properties": {"power": 3.10640526, "tb": 336.21279907, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.251965, 57.16304]}, "properties": {"power": 2.40693879, "tb": 306.66555786, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.250517, 57.159637]}, "properties": {"power": 2.23312426, "tb": 325.92211914, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.24366, 57.160496]}, "properties": {"power": 1.51176202, "tb": 317.16009521, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.242212, 57.157097]}, "properties": {"power": 1.51176202, "tb": 303.77804565, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.249069, 57.156235]}, "properties": {"power": 2.23312426, "tb": 310.37322998, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}]}"""

TEST_MONSTERAS_FIRST_COLLECTION = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.240452, 57.17329]}, "properties": {"power": 4.19946575, "tb": 336.38024902, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.247334, 57.172443]}, "properties": {"power": 5.85325146, "tb": 339.84768677, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.242519, 57.17498]}, "properties": {"power": 3.34151864, "tb": 316.57772827, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.249384, 57.174122]}, "properties": {"power": 3.34151864, "tb": 310.37808228, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.241102, 57.171574]}, "properties": {"power": 3.34151864, "tb": 339.86465454, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.247967, 57.170712]}, "properties": {"power": 3.34151864, "tb": 335.95074463, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.246538, 57.167309]}, "properties": {"power": 3.10640526, "tb": 337.62503052, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.239674, 57.168167]}, "properties": {"power": 3.10640526, "tb": 305.36495972, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}]}"""

TEST_MONSTERAS_SECOND_COLLECTION = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.245104, 57.163902]}, "properties": {"power": 3.10640526, "tb": 336.21279907, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.251965, 57.16304]}, "properties": {"power": 2.40693879, "tb": 306.66555786, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.250517, 57.159637]}, "properties": {"power": 2.23312426, "tb": 325.92211914, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.24366, 57.160496]}, "properties": {"power": 1.51176202, "tb": 317.16009521, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.242212, 57.157097]}, "properties": {"power": 1.51176202, "tb": 303.77804565, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}]}"""

TEST_MONSTERAS_THIRD_COLLECTION = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.249069, 57.156235]}, "properties": {"power": 2.23312426, "tb": 310.37322998, "confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}]}"""


# AFIMG_Suomi-NPP_20210619_000651_sweden.geojson
TEST_MONSTERAS_PREVIOUS1_COLLECTION = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.246222, 57.175987]}, "properties": {"power": 1.83814871, "tb": 302.3949585, "confidence": 8, "observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.245924, 57.17054]}, "properties": {"power": 1.83814871, "tb": 338.78729248, "confidence": 8, "observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.239649, 57.17067]}, "properties": {"power": 1.83814871, "tb": 301.75921631, "confidence": 8, "observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.245516, 57.1651]}, "properties": {"power": 2.94999027, "tb": 324.5098877, "confidence": 8, "observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.251759, 57.164974]}, "properties": {"power": 1.55109835, "tb": 308.91491699, "confidence": 8, "observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.245028, 57.15966]}, "properties": {"power": 2.94999027, "tb": 313.83581543, "confidence": 8, "observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP"}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.251282, 57.159531]}, "properties": {"power": 1.55109835, "tb": 310.77600098, "confidence": 8, "observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP"}}]}"""

# AFIMG_NOAA-20_20210618_124819_sweden.geojson
TEST_MONSTERAS_PREVIOUS2_COLLECTION = """{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.252192, 57.15242]}, "properties": {"power": 2.87395763, "tb": 330.10293579, "confidence": 8, "observation_time": "2021-06-18T14:49:01.750000+02:00", "platform_name": "NOAA-20"}}]}"""

# Past alarms:
PAST_ALARMS_MONSTERAS1 = """{"type": "FeatureCollection", "features": {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.246222, 57.175987]}, "properties": {"power": 1.83814871, "tb": 302.3949585, "confidence": 8, "observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP", "related_detection": true}}}"""

PAST_ALARMS_MONSTERAS2 = """{"type": "FeatureCollection", "features": {"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.245516, 57.1651]}, "properties": {"power": 2.94999027, "tb": 324.5098877, "confidence": 8, "observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP", "related_detection": true}}}"""

PAST_ALARMS_MONSTERAS3 = """{"features": {"geometry": {"coordinates": [16.252192, 57.15242], "type": "Point"}, "properties": {"confidence": 8, "observation_time": "2021-06-18T14:49:01.750000+02:00", "platform_name": "NOAA-20", "related_detection": false, "power": 2.87395763, "tb": 330.10293579}, "type": "Feature"}, "type": "FeatureCollection"}"""


CONFIG_EXAMPLE = {'subscribe_topics': '/VIIRS/L2/Fires/PP/National',
                  'publish_topic': '/VIIRS/L2/Fires/PP/SOSAlarm',
                  'geojson_file_pattern_alarms': 'sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson',
                  'fire_alarms_dir': '/path/where/the/filtered/alarms/will/be/stored',
                  'restapi_url': 'https://xxx.smhi.se:xxxx'}


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


def test_join_fire_detections_large_fire(fake_geojson_file_many_detections):
    """Test create alarm from set of fire detections."""
    ffdata = read_geojson_data(fake_geojson_file_many_detections)

    joined_detections = join_fire_detections(ffdata)

    for key in joined_detections.keys():
        assert key in ['1624045_5717329', '1624510_5716390', '1624906_5715623']

    json_test_data = json.loads(TEST_MONSTERAS_FIRST_COLLECTION)
    collection_id = '1624045_5717329'
    assert len(joined_detections[collection_id]) == 8
    for idx in range(8):
        assert joined_detections[collection_id][idx]['geometry']['coordinates'] == json_test_data['features'][idx]['geometry']['coordinates']
        assert joined_detections[collection_id][idx]['properties'] == json_test_data['features'][idx]['properties']

    json_test_data = json.loads(TEST_MONSTERAS_SECOND_COLLECTION)
    collection_id = '1624510_5716390'
    assert len(joined_detections[collection_id]) == 5
    for idx in range(5):
        assert joined_detections[collection_id][idx]['geometry']['coordinates'] == json_test_data['features'][idx]['geometry']['coordinates']
        assert joined_detections[collection_id][idx]['properties'] == json_test_data['features'][idx]['properties']

    json_test_data = json.loads(TEST_MONSTERAS_THIRD_COLLECTION)
    collection_id = '1624906_5715623'
    assert len(joined_detections[collection_id]) == 1
    for idx in range(1):
        assert joined_detections[collection_id][idx]['geometry']['coordinates'] == json_test_data['features'][idx]['geometry']['coordinates']
        assert joined_detections[collection_id][idx]['properties'] == json_test_data['features'][idx]['properties']


def test_split_large_fire_clusters():
    """Test the splitting of large fire clusters."""
    json_test_data = json.loads(TEST_MONSTERAS_FIRST_COLLECTION)

    fcolls = split_large_fire_clusters(json_test_data['features'], 1.2)
    assert 'only-one-cluster' in fcolls

    fcolls = split_large_fire_clusters(json_test_data['features'], 0.6)

    for key in fcolls.keys():
        assert key in ['1624251_5717498', '1624653_5716730']

    assert len(fcolls['1624251_5717498']) == 6
    assert len(fcolls['1624653_5716730']) == 2
    assert fcolls['1624653_5716730'][0]['geometry']['coordinates'] == [16.246538, 57.167309]
    assert fcolls['1624653_5716730'][1]['geometry']['coordinates'] == [16.239674, 57.168167]

    # Only one detection:
    json_test_data = json.loads(TEST_MONSTERAS_THIRD_COLLECTION)

    fcolls = split_large_fire_clusters(json_test_data['features'], 1.2)
    assert json_test_data['features'][0] == fcolls['only-one-cluster'][0]


def test_create_one_detection_from_collection():
    """Test create one detection (alarm) from a collection."""
    json_test_data = json.loads(TEST_MONSTERAS_FIRST_COLLECTION)

    features = json_test_data['features']
    alarm_feature = create_one_detection_from_collection(features)

    assert alarm_feature['properties']['power'] == 5.85325146
    assert alarm_feature['geometry']['coordinates'] == [16.247334, 57.172443]

    test_two_points_fire_same_power = [{'type': 'Feature',
                                        'geometry': {'type': 'Point', 'coordinates': [16.246538, 57.167309]},
                                        'properties': {'power': 3.10640526, 'tb': 337.62503052, 'confidence': 8,
                                                       'observation_time': '2021-06-19T02:58:45.700000+02:00',
                                                       'platform_name': 'NOAA-20'}},
                                       {'type': 'Feature',
                                        'geometry': {'type': 'Point', 'coordinates': [16.239674, 57.168167]},
                                        'properties': {'power': 3.10640526, 'tb': 305.36495972, 'confidence': 8,
                                                       'observation_time': '2021-06-19T02:58:45.700000+02:00',
                                                       'platform_name': 'NOAA-20'}}]

    alarm_feature = create_one_detection_from_collection(test_two_points_fire_same_power)
    assert alarm_feature['properties']['power'] == 3.10640526
    assert alarm_feature['geometry']['coordinates'] == [16.246538, 57.167309]


def test_create_single_point_alarms_from_collections():
    """Test create for each collection of fire detections one single alarm."""
    json_test_data = json.loads(TEST_MONSTERAS_PREVIOUS1_COLLECTION)
    fcolls = split_large_fire_clusters(json_test_data['features'], 1.2)

    alarms = create_single_point_alarms_from_collections(fcolls)

    assert len(alarms) == 2
    assert alarms[0]['features']['geometry']['coordinates'] == [16.246222, 57.175987]
    assert alarms[0]['features']['properties']['power'] == 1.83814871
    assert alarms[1]['features']['geometry']['coordinates'] == [16.245516, 57.1651]
    assert alarms[1]['features']['properties']['power'] == 2.94999027


def test_create_alarms_from_fire_detections(fake_past_detections_dir):
    """Test create alarm from set of fire detections."""
    json_test_data = json.loads(TEST_GEOJSON_FILE_CONTENT_MONSTERAS)

    pattern = "sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson"
    # Default distance threshold but disregarding past alarms:
    alarms = create_alarms_from_fire_detections(json_test_data, fake_past_detections_dir,
                                                pattern, 1.2, 0.0)
    assert len(alarms) == 3
    assert alarms[0]['features']['geometry']['coordinates'] == [16.247334, 57.172443]
    assert alarms[0]['features']['properties']['power'] == 5.85325146
    assert alarms[0]['features']['properties']['related_detection'] is True
    assert alarms[0]['features']['properties']['platform_name'] == 'NOAA-20'
    assert alarms[0]['features']['properties']['tb'] == 339.84768677
    assert alarms[0]['features']['properties']['observation_time'] == '2021-06-19T02:58:45.700000+02:00'

    assert alarms[1]['features']['geometry']['coordinates'] == [16.245104, 57.163902]
    assert alarms[1]['features']['properties']['power'] == 3.10640526
    assert alarms[1]['features']['properties']['related_detection'] is True
    assert alarms[1]['features']['properties']['platform_name'] == 'NOAA-20'
    assert alarms[1]['features']['properties']['tb'] == 336.21279907
    assert alarms[1]['features']['properties']['observation_time'] == '2021-06-19T02:58:45.700000+02:00'

    assert alarms[2]['features']['geometry']['coordinates'] == [16.249069, 57.156235]
    assert alarms[2]['features']['properties']['power'] == 2.23312426
    assert alarms[2]['features']['properties']['related_detection'] is False
    assert alarms[2]['features']['properties']['platform_name'] == 'NOAA-20'
    assert alarms[2]['features']['properties']['tb'] == 310.37322998
    assert alarms[2]['features']['properties']['observation_time'] == '2021-06-19T02:58:45.700000+02:00'

    # Default thresholds (distance=1.2km, time-interval=6hours):
    alarms = create_alarms_from_fire_detections(json_test_data, fake_past_detections_dir,
                                                pattern)

    assert len(alarms) == 1
    assert alarms[0]['features']['geometry']['coordinates'] == [16.249069, 57.156235]
    assert alarms[0]['features']['properties']['power'] == 2.23312426
    assert alarms[0]['features']['properties']['related_detection'] is False
    assert alarms[0]['features']['properties']['platform_name'] == 'NOAA-20'
    assert alarms[0]['features']['properties']['tb'] == 310.37322998
    assert alarms[0]['features']['properties']['observation_time'] == '2021-06-19T02:58:45.700000+02:00'

    # Default time-interval(=6hours) threshold but smaller distance threshold
    # (the threshold used to split larger fires):
    alarms = create_alarms_from_fire_detections(json_test_data, fake_past_detections_dir,
                                                pattern, 0.6)

    assert len(alarms) == 2
    assert alarms[0]['features']['geometry']['coordinates'] == [16.242212, 57.157097]
    assert alarms[0]['features']['properties']['power'] == 1.51176202
    assert alarms[0]['features']['properties']['related_detection'] is False
    assert alarms[0]['features']['properties']['platform_name'] == 'NOAA-20'
    assert alarms[0]['features']['properties']['tb'] == 303.77804565
    assert alarms[0]['features']['properties']['observation_time'] == '2021-06-19T02:58:45.700000+02:00'

    assert alarms[1]['features']['geometry']['coordinates'] == [16.249069, 57.156235]
    assert alarms[1]['features']['properties']['power'] == 2.23312426
    assert alarms[1]['features']['properties']['related_detection'] is False
    assert alarms[1]['features']['properties']['platform_name'] == 'NOAA-20'
    assert alarms[1]['features']['properties']['tb'] == 310.37322998
    assert alarms[1]['features']['properties']['observation_time'] == '2021-06-19T02:58:45.700000+02:00'

    alarms = create_alarms_from_fire_detections(json_test_data, fake_past_detections_dir,
                                                pattern, 1.2, 16.0)

    assert len(alarms) == 0
    assert alarms[0]['features']['geometry']['coordinates'] == [16.249069, 57.156235]
    assert alarms[0]['features']['properties']['power'] == 2.23312426
    assert alarms[0]['features']['properties']['related_detection'] is False
    assert alarms[0]['features']['properties']['platform_name'] == 'NOAA-20'
    assert alarms[0]['features']['properties']['tb'] == 310.37322998
    assert alarms[0]['features']['properties']['observation_time'] == '2021-06-19T02:58:45.700000+02:00'

    alarms = create_alarms_from_fire_detections(json_test_data, fake_past_detections_dir,
                                                pattern, 1.2)

    assert len(alarms) == 1
    assert alarms[0]['features']['geometry']['coordinates'] == [16.249069, 57.156235]
    assert alarms[0]['features']['properties']['power'] == 2.23312426
    assert alarms[0]['features']['properties']['related_detection'] is False
    assert alarms[0]['features']['properties']['platform_name'] == 'NOAA-20'
    assert alarms[0]['features']['properties']['tb'] == 310.37322998
    assert alarms[0]['features']['properties']['observation_time'] == '2021-06-19T02:58:45.700000+02:00'


@patch('activefires_pp.spatiotemporal_alarm_filtering.get_xauth_environment_variable')
@patch('activefires_pp.spatiotemporal_alarm_filtering.read_config')
@patch('activefires_pp.spatiotemporal_alarm_filtering.AlarmFilterRunner._setup_and_start_communication')
def test_alarm_filter_runner_init(setup_comm, get_config, get_xauth):
    """Test initialize the AlarmFilterRunner class."""
    get_config.return_value = CONFIG_EXAMPLE
    get_xauth.return_value = 'some-token'

    myconfigfile = "/my/config/file/path"

    alarm_runner = AlarmFilterRunner(myconfigfile)

    assert alarm_runner.configfile == myconfigfile
    assert type(alarm_runner.fire_alarms_dir) is pathlib.PosixPath
    assert str(alarm_runner.fire_alarms_dir) == "/path/where/the/filtered/alarms/will/be/stored"
    assert alarm_runner.input_topic == '/VIIRS/L2/Fires/PP/National'
    assert alarm_runner.output_topic == '/VIIRS/L2/Fires/PP/SOSAlarm'
    assert alarm_runner.listener is None
    assert alarm_runner.publisher is None
    assert alarm_runner.loop is False
    assert alarm_runner.sos_alarms_file_pattern == 'sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson'
    assert alarm_runner.restapi_url == 'https://xxx.smhi.se:xxxx'
    assert alarm_runner.options == {'subscribe_topics': ['/VIIRS/L2/Fires/PP/National'],
                                    'publish_topic': '/VIIRS/L2/Fires/PP/SOSAlarm',
                                    'geojson_file_pattern_alarms': 'sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson',
                                    'fire_alarms_dir': '/path/where/the/filtered/alarms/will/be/stored',
                                    'restapi_url': 'https://xxx.smhi.se:xxxx'}


@patch('activefires_pp.spatiotemporal_alarm_filtering.get_xauth_environment_variable')
@patch('activefires_pp.spatiotemporal_alarm_filtering.read_config')
@patch('activefires_pp.spatiotemporal_alarm_filtering.AlarmFilterRunner._setup_and_start_communication')
def test_alarm_filter_runner_init_no_env(setup_comm, get_config, get_xauth):
    """Test initialize the AlarmFilterRunner class."""
    get_config.return_value = CONFIG_EXAMPLE
    get_xauth.return_value = None

    myconfigfile = "/my/config/file/path"

    with pytest.raises(OSError) as exec_info:
        _ = AlarmFilterRunner(myconfigfile)

    expected = "Environment variable XAUTH_SMHI_FIREALARMS_REST_API not set!"
    assert str(exec_info.value) == expected


@patch('activefires_pp.spatiotemporal_alarm_filtering.read_config')
@patch('activefires_pp.spatiotemporal_alarm_filtering.AlarmFilterRunner._setup_and_start_communication')
@patch('activefires_pp.spatiotemporal_alarm_filtering.get_filename_from_posttroll_message')
@patch('activefires_pp.spatiotemporal_alarm_filtering.read_geojson_data')
@patch('activefires_pp.spatiotemporal_alarm_filtering.create_alarms_from_fire_detections')
@patch('activefires_pp.spatiotemporal_alarm_filtering.AlarmFilterRunner.send_alarms')
def test_alarm_filter_runner_call_spatio_temporal_alarm_filtering_has_alarms(send_alarms, create_alarms, read_geojson,
                                                                             get_filename_from_pmsg, setup_comm, get_config):
    """Test run the spatio_temporal_alarm_filtering method of the AlarmFilterRunner class."""
    get_config.return_value = CONFIG_EXAMPLE
    json_test_data = json.loads(TEST_MONSTERAS_FIRST_COLLECTION)
    read_geojson.return_value = json_test_data

    alarm = {"features": {"geometry": {"coordinates": [16.249069, 57.156235], "type": "Point"},
                          "properties": {"confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00",
                                         "platform_name": "NOAA-20",
                                         "power": 2.23312426,
                                         "related_detection": False,
                                         "tb": 310.37322998}, "type": "Feature"},
             "type": "FeatureCollection"}
    create_alarms.return_value = [alarm]

    myconfigfile = "/my/config/file/path"

    alarm_runner = AlarmFilterRunner(myconfigfile)

    dummy_msg = None
    result = alarm_runner.spatio_temporal_alarm_filtering(dummy_msg)
    assert len(result) == 1
    assert result[0] == alarm


@patch('activefires_pp.spatiotemporal_alarm_filtering.read_config')
@patch('activefires_pp.spatiotemporal_alarm_filtering.AlarmFilterRunner._setup_and_start_communication')
@patch('activefires_pp.spatiotemporal_alarm_filtering.get_filename_from_posttroll_message')
@patch('activefires_pp.spatiotemporal_alarm_filtering.read_geojson_data')
def test_alarm_filter_runner_call_spatio_temporal_alarm_filtering_no_firedata(read_geojson,
                                                                              get_filename_from_pmsg, setup_comm, get_config):
    """Test run the spatio_temporal_alarm_filtering method of the AlarmFilterRunner class - no fires."""
    get_config.return_value = CONFIG_EXAMPLE
    read_geojson.return_value = None

    myconfigfile = "/my/config/file/path"

    alarm_runner = AlarmFilterRunner(myconfigfile)

    dummy_msg = None
    result = alarm_runner.spatio_temporal_alarm_filtering(dummy_msg)
    assert result is None


@patch('activefires_pp.spatiotemporal_alarm_filtering.read_config')
@patch('activefires_pp.spatiotemporal_alarm_filtering.AlarmFilterRunner._setup_and_start_communication')
@patch('activefires_pp.spatiotemporal_alarm_filtering.get_filename_from_posttroll_message')
@patch('activefires_pp.spatiotemporal_alarm_filtering.read_geojson_data')
@patch('activefires_pp.spatiotemporal_alarm_filtering.create_alarms_from_fire_detections')
def test_alarm_filter_runner_call_spatio_temporal_alarm_filtering_no_alarms(create_alarms, read_geojson,
                                                                            get_filename_from_pmsg, setup_comm, get_config):
    """Test run the spatio_temporal_alarm_filtering method of the AlarmFilterRunner class - no alarms."""
    get_config.return_value = CONFIG_EXAMPLE
    json_test_data = json.loads(TEST_MONSTERAS_FIRST_COLLECTION)
    read_geojson.return_value = json_test_data

    create_alarms.return_value = []

    myconfigfile = "/my/config/file/path"

    alarm_runner = AlarmFilterRunner(myconfigfile)

    dummy_msg = None
    result = alarm_runner.spatio_temporal_alarm_filtering(dummy_msg)
    assert result is None


def test_send_alarm_post_ok(fake_past_detections_dir):
    """Test send alarm."""

    alarm = json.loads(PAST_ALARMS_MONSTERAS3)
    restapi_url = "https://httpbin.org/post"

    retv = post_alarm(alarm, restapi_url)
    assert retv is True


def test_send_alarm_post_log_messages(caplog, fake_past_detections_dir):
    """Test send alarm."""

    alarm = json.loads(PAST_ALARMS_MONSTERAS3)
    restapi_url = "https://httpbin.org/post"

    with caplog.at_level(logging.INFO):
        retv = post_alarm(alarm, restapi_url)

    log_output = "Alarm posted: Response = <Response [200]>"
    assert log_output in caplog.text
