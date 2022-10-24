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

"""Test the Geojson utillities."""

from activefires_pp.geojson_utils import read_geojson_data
from activefires_pp.geojson_utils import get_geojson_files_in_observation_time_order
from activefires_pp.geojson_utils import store_geojson_alarm
from trollsift import Parser

from datetime import datetime
import pytest
import logging

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


@pytest.fixture
def fake_geojson_file(tmp_path):
    """Write fake geojson file."""
    file_path = tmp_path / 'test_afimg_20220629_120026.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_GEOJSON_FILE_CONTENT)

    yield file_path


@pytest.fixture
def fake_empty_geojson_file(tmp_path):
    """Write fake empty geojson file."""
    file_path = tmp_path / 'test_afimg_20220629_110913.geojson'
    file_path.touch()
    yield file_path


@pytest.fixture
def fake_nonexisting_geojson_file(tmp_path):
    """Write non existing (geojson) file."""
    file_path = tmp_path / 'test_afimg_20220629_110913.geojson'
    yield file_path


@pytest.fixture
def fake_past_detections_dir(tmp_path):
    """Create fake directory with past detections."""
    past_detections_dir = tmp_path / 'past_detections'
    past_detections_dir.mkdir()
    file_path = past_detections_dir / 'sos_20210619_005803_0.geojson'
    file_path.touch()
    file_path = past_detections_dir / 'sos_20210619_000651_0.geojson'
    file_path.touch()
    file_path = past_detections_dir / 'sos_20210619_000651_1.geojson'
    file_path.touch()
    file_path = past_detections_dir / 'sos_20210618_124819_0.geojson'
    file_path.touch()
    file_path = past_detections_dir / 'sos_20210618_110719_0.geojson'
    file_path.touch()

    yield file_path.parent


def test_read_and_get_geojson_data_from_file(fake_geojson_file):
    """Test reading a geojson file and return the content."""
    ffdata = read_geojson_data(fake_geojson_file)

    assert len(ffdata) == 2
    assert ffdata['type'] == 'FeatureCollection'
    assert len(ffdata['features']) == 3
    feature1 = ffdata['features'][0]
    assert 'type' in feature1.keys()
    assert 'geometry' in feature1.keys()
    assert 'properties' in feature1.keys()
    assert feature1['type'] == 'Feature'
    assert feature1['geometry'] == {"coordinates": [23.562864, 67.341919], "type": "Point"}
    assert feature1['properties'] == {"confidence": 8, "observation_time": "2022-06-29T14:01:08.850000",
                                      "platform_name": "NOAA-20", "power": 1.62920368, "tb": 325.2354126}


def test_read_and_get_geojson_data_from_nonexisting_file(caplog, fake_nonexisting_geojson_file):
    """Test reading a geojson file when file is not there."""
    with caplog.at_level(logging.ERROR):
        ffdata = read_geojson_data(fake_nonexisting_geojson_file)

    assert ffdata is None
    log_output = "No valid filename to read: {filename}".format(filename=fake_nonexisting_geojson_file)
    assert log_output in caplog.text


def test_read_and_get_geojson_data_from_empty_file(caplog, fake_empty_geojson_file):
    """Test reading an empty geojson file and return the content."""
    with caplog.at_level(logging.WARNING):
        ffdata = read_geojson_data(fake_empty_geojson_file)

    assert ffdata is None
    log_output = "Geojson file invalid and cannot be read: {filename}".format(filename=fake_empty_geojson_file)
    assert log_output in caplog.text


def test_get_geojson_files_in_observation_time_order(fake_past_detections_dir):
    """Test getting the list of geojson files ordered by observation time - most recent file first."""
    starttime = datetime(2021, 6, 18, 12, 0)
    endtime = datetime(2021, 6, 19, 0, 30)
    # geojson_file_pattern_alarms: sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson
    pattern = "sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson"

    recent = get_geojson_files_in_observation_time_order(fake_past_detections_dir, pattern, (starttime, endtime))
    assert set(recent) == {'sos_20210618_124819_0.geojson', 'sos_20210619_000651_1.geojson',
                           'sos_20210619_000651_0.geojson'}

    starttime = datetime(2021, 6, 18, 12, 0)
    endtime = datetime(2021, 6, 19, 12, 0)
    recent = get_geojson_files_in_observation_time_order(fake_past_detections_dir, pattern, (starttime, endtime))

    assert set(recent) == {'sos_20210618_124819_0.geojson', 'sos_20210619_000651_1.geojson',
                           'sos_20210619_000651_0.geojson', 'sos_20210619_005803_0.geojson'}

    starttime = datetime(2022, 6, 18, 12, 0)
    endtime = datetime(2022, 6, 19, 12, 0)
    recent = get_geojson_files_in_observation_time_order(fake_past_detections_dir, pattern, (starttime, endtime))
    assert len(recent) == 0


def test_store_geojson_alarm(fake_past_detections_dir):
    """Test storing the geojson alarm object."""
    sos_alarms_file_pattern = 'sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson'
    file_parser = Parser(sos_alarms_file_pattern)
    idx = 0
    alarm = {"features": {"geometry": {"coordinates": [16.249069, 57.156235], "type": "Point"},
                          "properties": {"confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00",
                                         "platform_name": "NOAA-20",
                                         "power": 2.23312426,
                                         "related_detection": False,
                                         "tb": 310.37322998}, "type": "Feature"},
             "type": "FeatureCollection"}

    result_filename = store_geojson_alarm(fake_past_detections_dir, file_parser, idx, alarm)
    assert result_filename.exists() is True
    assert result_filename.name == 'sos_20210619_005845_0.geojson'

    json_test_data = read_geojson_data(result_filename)

    assert json_test_data['features']['geometry']['coordinates'] == [16.249069, 57.156235]
    assert json_test_data['features']['properties']['confidence'] == 8
    assert json_test_data['features']['properties']['observation_time'] == "2021-06-19T02:58:45.700000+02:00"
    assert json_test_data['features']['properties']['platform_name'] == "NOAA-20"
    assert json_test_data['features']['properties']['power'] == 2.23312426
    assert json_test_data['features']['properties']['related_detection'] is False
    assert json_test_data['features']['properties']['tb'] == 310.37322998
