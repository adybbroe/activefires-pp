#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023, 2024 Adam.Dybbroe

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

"""Test operations on the fire detection id."""

from unittest.mock import patch
from datetime import datetime
from freezegun import freeze_time

from activefires_pp.post_processing import ActiveFiresShapefileFiltering
from activefires_pp.post_processing import ActiveFiresPostprocessing
from activefires_pp.tests.test_utils import AF_FILE_PATTERN


@freeze_time('2023-06-16 11:24:00')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_add_unique_day_id_to_detections_sameday(setup_comm, gethostname,
                                                 fake_active_fires_ascii_file2,
                                                 fake_yamlconfig_file_post_processing):
    """Test adding unique id's to the fire detection data."""
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)

    this = ActiveFiresShapefileFiltering(filepath=fake_active_fires_ascii_file2, timezone='GMT')
    afdata = this.get_af_data(filepattern=AF_FILE_PATTERN, localtime=False)

    assert afpp._fire_detection_id == {'date': datetime.utcnow(), 'counter': 0}

    # 4 fire detections, so (current) ID should be raised by 4
    afdata = afpp.add_unique_day_id(afdata)
    assert 'detection_id' in afdata
    assert afdata['detection_id'].values.tolist() == ['20230616-1', '20230616-2',
                                                      '20230616-3', '20230616-4']
    assert afpp._fire_detection_id == {'date': datetime.utcnow(), 'counter': 4}


@freeze_time('2023-06-17 11:55:00')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_add_unique_day_id_to_detections_24hours_plus(setup_comm, gethostname,
                                                      fake_active_fires_ascii_file3,
                                                      fake_yamlconfig_file_post_processing):
    """Test adding unique id's to the fire detection data."""
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afpp._fire_detection_id = {'date': datetime(2023, 6, 16, 11, 24, 0), 'counter': 4}

    this = ActiveFiresShapefileFiltering(filepath=fake_active_fires_ascii_file3, timezone='GMT')
    afdata = this.get_af_data(filepattern=AF_FILE_PATTERN, localtime=False)

    assert afpp._fire_detection_id == {'date': datetime(2023, 6, 16, 11, 24, 0), 'counter': 4}

    # 1 new fire detection, so (current) ID should be raised - a new day, so id
    # starting over from 0, and a new date!
    afdata = afpp.add_unique_day_id(afdata)
    assert 'detection_id' in afdata
    assert afdata['detection_id'].values.tolist() == ['20230617-1']
    assert afpp._fire_detection_id, {'date': datetime(2023, 6, 17, 11, 55, 0), 'counter': 1}


@freeze_time('2023-06-18 09:56:00')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_add_unique_day_id_to_detections_newday_from_cache(setup_comm, gethostname,
                                                           fake_active_fires_ascii_file4,
                                                           fake_yamlconfig_file_post_processing_with_id_cache):
    """Test adding unique id's to the fire detection data."""
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing_with_id_cache,
                                     myborders_file, mymask_file)

    this = ActiveFiresShapefileFiltering(filepath=fake_active_fires_ascii_file4,
                                         timezone='GMT')
    afdata = this.get_af_data(filepattern=AF_FILE_PATTERN, localtime=False)

    assert afpp._fire_detection_id == {'date': datetime(2023, 5, 1, 0, 0), 'counter': 1}
    # 2 new fire detections, so (current) ID should be raised - a new day, so id
    # starting over from 0, and a new date!
    afdata = afpp.add_unique_day_id(afdata)
    assert 'detection_id' in afdata
    assert afdata['detection_id'].values.tolist() == ['20230618-1', '20230618-2']
    assert afpp._fire_detection_id == {'date': datetime(2023, 6, 18, 9, 56, 0), 'counter': 2}


@freeze_time('2023-06-18 09:56:00')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_add_unique_day_id_to_detections_newday_no_cache(setup_comm, gethostname,
                                                         fake_active_fires_ascii_file4,
                                                         fake_yamlconfig_file_post_processing):
    """Test adding unique id's to the fire detection data."""
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afpp._fire_detection_id = {'date': datetime(2023, 6, 17, 23, 55, 0), 'counter': 1}

    this = ActiveFiresShapefileFiltering(filepath=fake_active_fires_ascii_file4,
                                         timezone='GMT')
    afdata = this.get_af_data(filepattern=AF_FILE_PATTERN, localtime=False)

    assert afpp._fire_detection_id == {'date': datetime(2023, 6, 17, 23, 55), 'counter': 1}
    # 2 new fire detections, so (current) ID should be raised - a new day, so id
    # starting over from 0, and a new date!
    afdata = afpp.add_unique_day_id(afdata)
    assert 'detection_id' in afdata
    assert afdata['detection_id'].values.tolist() == ['20230618-1', '20230618-2']
    assert afpp._fire_detection_id == {'date': datetime(2023, 6, 18, 9, 56, 0), 'counter': 2}


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.read_cspp_output_data')
def test_store_fire_detection_id_on_disk(readdata, setup_comm, gethostname, tmp_path,
                                         fake_yamlconfig_file_post_processing):
    """Test store the latest/current detection id to a file."""
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afpp._fire_detection_id = {'date': datetime(2023, 6, 17, 11, 55, 0), 'counter': 1}

    detection_id_cache = tmp_path / 'detection_id_cache.txt'
    afpp.filepath_detection_id_cache = str(detection_id_cache)
    afpp.save_id_to_file()

    with open(afpp.filepath_detection_id_cache) as fpt:
        result = fpt.read()

    assert result == '20230617-1'


@freeze_time('2023-06-18 12:00:00')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.read_cspp_output_data')
def test_initialize_fire_detection_id_nofile(readdata, setup_comm, gethostname, tmp_path,
                                             fake_yamlconfig_file_post_processing):
    """Test initialize the fire detection id with no cache on disk."""
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)

    expected = {'date': datetime(2023, 6, 18, 12, 0, 0), 'counter': 0}

    afpp._initialize_fire_detection_id()
    assert afpp._fire_detection_id == expected


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.read_cspp_output_data')
def test_get_fire_detection_id_from_file(readdata, setup_comm, gethostname, tmp_path,
                                         fake_yamlconfig_file_post_processing):
    """Test rtrieve the detection id from file."""
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afpp._fire_detection_id = {'date': datetime(2023, 6, 17, 11, 55, 0), 'counter': 1}

    detection_id_cache = tmp_path / 'detection_id_cache.txt'
    afpp.filepath_detection_id_cache = str(detection_id_cache)
    afpp.save_id_to_file()
    result = afpp.get_id_from_file()
    expected = {'date': datetime(2023, 6, 17), 'counter': 1}
    assert result == expected

    afpp._initialize_fire_detection_id()
    assert afpp._fire_detection_id == expected
