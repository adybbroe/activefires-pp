#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 - 2023 Adam Dybbroe

# Author(s):

#   Adam Dybbroe <Firstname.Lastname at smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this 2program.  If not, see <http://www.gnu.org/licenses/>.

"""Test the Fires filtering functionality."""

from activefires_pp.tests.test_utils import MY_FILE_PATTERN
import pytest
from unittest.mock import patch

import pandas as pd
import numpy as np
from datetime import datetime

from activefires_pp.post_processing import ActiveFiresShapefileFiltering
from activefires_pp.post_processing import ActiveFiresPostprocessing
from activefires_pp.post_processing import COL_NAMES


TEST_REGIONAL_MASK = {}
TEST_REGIONAL_MASK['Bergslagen (RRB)'] = {'mask': np.array([False, False, False, False, False,
                                                            False, False, False, False,
                                                            False, False, False, False,  True,
                                                            False, False, False, False]),
                                          'attributes': {'Join_Count': 2, 'TARGET_FID': 142,
                                                         'Kod_omr': '1438', 'KNNAMN': 'Dals-Ed',
                                                         'LANDAREAKM': 728.0, 'KNBEF96': 5287.0,
                                                         'OBJECTID': 1080804, 'Datum_Tid': '2016-06-13',
                                                         'Testomr': 'Bergslagen (RRB)',
                                                         'Shape_Leng': 2131994.36042, 'Shape_Area': 53512139344.2},
                                          'all_inside_test_area': False, 'some_inside_test_area': True}
TEST_REGIONAL_MASK['Västerviks kommun'] = {'mask': np.array([False, False, False, False, False,
                                                             False, False, False, False,
                                                             False, False, False, False, False,
                                                             False, False, False, False]),
                                           'attributes': {'Join_Count': 30, 'TARGET_FID': 85,
                                                          'Kod_omr': '0883', 'KNNAMN': 'Västervik',
                                                          'LANDAREAKM': 1870.5, 'KNBEF96': 39579.0,
                                                          'OBJECTID': 1079223, 'Datum_Tid': '2016-06-13',
                                                          'Testomr': 'Västerviks kommun',
                                                          'Shape_Leng': 251653.298274,
                                                          'Shape_Area': 2040770168.02},
                                           'all_inside_test_area': False, 'some_inside_test_area': False}

FAKE_MASK1 = np.array([False, False, False, False, False, False, False, False,  True,
                       False, False,  True, False,  True, False, False, False, False])
FAKE_MASK2 = np.array([True, False,  True])


@patch('activefires_pp.post_processing._read_data')
def test_add_start_and_end_time_to_active_fires_data_utc(readdata, fake_active_fires_file_data):
    """Test adding start and end times to the active fires data."""
    open_fstream, myfilepath = fake_active_fires_file_data
    afdata = pd.read_csv(open_fstream, index_col=None, header=None, comment='#', names=COL_NAMES)

    readdata.return_value = afdata

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='GMT')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=False)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata
    assert this._afdata['starttime'].shape == (18,)

    assert str(this._afdata['starttime'][0]) == '2021-04-14 11:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 11:28:08'


@patch('activefires_pp.post_processing._read_data')
def test_add_start_and_end_time_to_active_fires_data_localtime(readdata, fake_active_fires_file_data):
    """Test adding start and end times to the active fires data."""
    open_fstream, myfilepath = fake_active_fires_file_data

    afdata = pd.read_csv(open_fstream, index_col=None, header=None, comment='#', names=COL_NAMES)
    readdata.return_value = afdata

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Europe/Stockholm')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=True)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata

    assert str(this._afdata['starttime'][0]) == '2021-04-14 13:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 13:28:08'

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Iceland')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=True)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata

    assert str(this._afdata['starttime'][0]) == '2021-04-14 11:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 11:28:08'

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Europe/Helsinki')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=True)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata

    assert str(this._afdata['starttime'][0]) == '2021-04-14 14:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 14:28:08'

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Europe/Lisbon')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=True)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata

    assert str(this._afdata['starttime'][0]) == '2021-04-14 12:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 12:28:08'


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_regional_fires_filtering(setup_comm, get_config, gethostname,
                                  fake_active_fires_file_data, fake_config_data):
    """Test the regional fires filtering."""
    # FIXME! This test is to big/broad. Need for refactoring!
    open_fstream, _ = fake_active_fires_file_data
    get_config.return_value = fake_config_data

    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)
    afpp._initialize_fire_detection_id()

    afdata = pd.read_csv(open_fstream, index_col=None, header=None, comment='#', names=COL_NAMES)

    starttime = datetime.fromisoformat('2021-04-14 11:26:43.900')
    endtime = datetime.fromisoformat('2021-04-14 11:28:08')

    afdata['starttime'] = np.repeat(starttime, len(afdata)).astype(np.datetime64)
    afdata['endtime'] = np.repeat(endtime, len(afdata)).astype(np.datetime64)

    afdata = afpp.add_unique_day_id(afdata)

    # Add metadata to the pandas dataframe:
    fake_metadata = {'platform': 'j01',
                     'start_time': datetime(2021, 4, 14, 11, 26, 43, 900000),
                     'end_hour': datetime(1900, 1, 1, 11, 28, 8, 400000),
                     'orbit': '17637',
                     'processing_time': datetime(2021, 4, 14, 11, 41, 30, 392094),
                     'end_time': datetime(2021, 4, 14, 11, 28, 8)}
    afdata.attrs = fake_metadata

    af_shapeff = ActiveFiresShapefileFiltering(afdata=afdata, platform_name='NOAA-20')
    regional_fmask = TEST_REGIONAL_MASK

    mymsg = "Fake message"
    with patch('activefires_pp.post_processing.store_geojson') as store_geojson:
        with patch('activefires_pp.post_processing.ActiveFiresPostprocessing._generate_output_message') as generate_msg:
            generate_msg.return_value = "my fake output message"
            result = afpp.regional_fires_filtering_and_publishing(mymsg, regional_fmask, af_shapeff)

    store_geojson.assert_called_once()
    generate_msg.assert_called_once()
    assert len(result) == 1
    assert result[0] == "my fake output message"


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.get_global_mask_from_shapefile', side_effect=[FAKE_MASK1, FAKE_MASK2])
def test_general_national_fires_filtering(get_global_mask, setup_comm,
                                          get_config, gethostname, fake_active_fires_file_data,
                                          fake_config_data):
    """Test the general/basic national fires filtering."""
    open_fstream, _ = fake_active_fires_file_data

    get_config.return_value = fake_config_data
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)
    afdata = pd.read_csv(open_fstream, index_col=None, header=None, comment='#', names=COL_NAMES)

    # Add metadata to the pandas dataframe:
    fake_metadata = {'platform': 'j01',
                     'start_time': datetime(2021, 4, 14, 11, 26, 43, 900000),
                     'end_hour': datetime(1900, 1, 1, 11, 28, 8, 400000),
                     'orbit': '17637',
                     'processing_time': datetime(2021, 4, 14, 11, 41, 30, 392094),
                     'end_time': datetime(2021, 4, 14, 11, 28, 8)}
    afdata.attrs = fake_metadata

    af_shapeff = ActiveFiresShapefileFiltering(afdata=afdata, platform_name='NOAA-20')
    afdata = af_shapeff.get_af_data(MY_FILE_PATTERN)

    mymsg = "Fake message"
    result = afpp.fires_filtering(mymsg, af_shapeff)

    assert get_global_mask.call_count == 2

    assert isinstance(result, pd.core.frame.DataFrame)
    assert len(result) == 1
    np.testing.assert_almost_equal(result.iloc[0]['latitude'], 59.52483368)
    np.testing.assert_almost_equal(result.iloc[0]['longitude'], 17.1681633)


@pytest.mark.usefixtures("fake_national_borders_shapefile")
@pytest.mark.usefixtures("fake_yamlconfig_file_post_processing")
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_checking_national_borders_shapefile_file_exists(setup_comm, gethostname,
                                                         fake_yamlconfig_file_post_processing,
                                                         fake_national_borders_shapefile):
    """Test the checking of the national borders shapefile - borders shapefile exists."""
    gethostname.return_value = "my.host.name"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(str(fake_yamlconfig_file_post_processing),
                                     fake_national_borders_shapefile, mymask_file)
    afpp._check_borders_shapes_exists()

    assert afpp.shp_borders.name == 'some_national_borders_shape.yaml'
    assert afpp.shp_borders.is_file()


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_checking_national_borders_shapefile_file_nonexisting(setup_comm, get_config, gethostname,
                                                              fake_config_data):
    """Test the checking of the national borders shapefile - borders shapefile does not exist."""
    get_config.return_value = fake_config_data
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)
    with pytest.raises(OSError) as exec_info:
        afpp._check_borders_shapes_exists()

    expected = "Shape file does not exist! Filename = /my/shape/file/with/country/borders"
    assert str(exec_info.value) == expected
