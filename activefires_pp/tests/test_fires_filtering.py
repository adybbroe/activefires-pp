#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam Dybbroe

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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
"""

import pytest
import unittest
import pandas as pd
from unittest.mock import patch
from unittest.mock import mock_open
import io

from datetime import datetime
import pycrs
import cartopy.io.shapereader

from activefires_pp.post_processing import ShapeGeometry
from activefires_pp.post_processing import ActiveFiresShapefileFiltering
from activefires_pp.post_processing import COL_NAMES


SHP_BOARDERS = "/home/a000680/data/shapes/Sverige/Sverige.shp"

TEST_ACTIVE_FIRES_FILEPATH = "./AFIMG_j01_d20210414_t1126439_e1128084_b17637_c20210414114130392094_cspp_dev.txt"

TEST_ACTIVE_FIRES_FILE_DATA = """
# Active Fires I-band EDR
#
# source: AFIMG_j01_d20210414_t1126439_e1128084_b17637_c20210414114130392094_cspp_dev.nc
# version: CSPP Active Fires version: cspp-active-fire-noaa_1.1.0
#
# column 1: latitude of fire pixel (degrees)
# column 2: longitude of fire pixel (degrees)
# column 3: I04 brightness temperature of fire pixel (K)
# column 4: Along-scan fire pixel resolution (km)
# column 5: Along-track fire pixel resolution (km)
# column 6: detection confidence ([7,8,9]->[lo,med,hi])
# column 7: fire radiative power (MW)
#
# number of fire pixels: 18
#
  59.14783859,   37.85886765,  331.58309937,  0.375,  0.375,    8,    4.67825794
  59.05127335,   28.15227890,  349.83993530,  0.375,  0.375,    8,    7.10289335
  59.05587006,   28.15146446,  326.76165771,  0.375,  0.375,    8,    7.10289335
  59.46587372,   29.04332352,  327.60366821,  0.375,  0.375,    8,    5.01662874
  59.59255981,   28.77226448,  345.88961792,  0.375,  0.375,    8,   13.13724804
  59.58853149,   28.77531433,  339.56134033,  0.375,  0.375,    8,    8.76600266
  59.59326553,   28.77456856,  352.21545410,  0.375,  0.375,    8,    8.76600266
  59.59757233,   28.76391029,  328.43835449,  0.375,  0.375,    8,    5.08633661
  58.35777283,   12.37761784,  327.17175293,  0.375,  0.375,    8,   17.58141518
  60.30867004,   25.53105164,  349.98794556,  0.375,  0.375,    8,    6.93412018
  55.01095581,   -2.28794742,  335.89736938,  0.375,  0.375,    8,    4.39908028
  59.52483368,   17.16816330,  336.57437134,  0.375,  0.375,    8,   14.13167953
  55.00822449,   -2.28098702,  344.50894165,  0.375,  0.375,    8,    4.16644764
  60.13325882,   16.18420029,  329.47689819,  0.375,  0.375,    8,    5.32859230
  61.30901337,   21.98561668,  341.69180298,  0.375,  0.375,    8,    8.87900448
  58.29126740,    0.20132475,  331.47875977,  0.375,  0.375,    8,    3.64687872
  57.42922211,   -3.47403550,  336.02111816,  0.375,  0.375,    8,    8.39092922
  57.42747116,   -3.47912717,  353.80722046,  0.375,  0.375,    8,   12.13035393
"""


OPEN_FSTREAM = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA)


MY_FILE_PATTERN = ("AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_e{end_hour:%H%M%S%f}_" +
                   "b{orbit:s}_c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt")

TEST_CRS_PROJ = ('+proj=utm +ellps=GRS80 +a=6378137.0 +rf=298.257222101 +pm=0 +x_0=500000.0 ' +
                 '+y_0=0.0 +lon_0=15.0 +lat_0=0.0 +units=m +axis=enu +no_defs')


class MyMockCrs(object):
    def __init__(self):
        self._crs_proj = TEST_CRS_PROJ

    def to_proj4(self):
        return self._crs_proj


class fake_geometry_records(object):
    def __init__(self):
        self.geometry = 'Some geom'
        self.attributes = {'attr1': 1, 'attr2': 'myname'}


def fake_get_records():
    """Fake retrieving the generator class of the Geometry records in a shapefile."""
    num = 0
    while num < 10:
        yield fake_geometry_records()
        num = num + 1


@patch('pycrs.load.from_file')
def test_shape_geometry_init(load_from_file):
    """Test creating the ShapeGeometry object."""

    load_from_file.return_value = MyMockCrs()
    shpgeom = ShapeGeometry('/my/shape/file/path/myshapefile.sph')

    assert shpgeom.proj4str == TEST_CRS_PROJ


@patch('pycrs.load.from_file')
def test_shape_geometry_loading(load_from_file):
    """Test loading the geometries and attributes from the shapefile."""

    load_from_file.return_value = MyMockCrs()
    shpgeom = ShapeGeometry('/my/shape/file/path/myshapefile.sph')
    #shpgeom = ShapeGeometry(SHP_BOARDERS)

    with patch('cartopy.io.shapereader.Reader') as MockShpReader:
        MockShpReader.return_value.records.return_value = fake_get_records()
        shpgeom.load()

    assert shpgeom.geometries is not None
    assert shpgeom.attributes is not None

    assert len(shpgeom.geometries) == 10
    assert len(shpgeom.attributes) == 10


@patch('activefires_pp.post_processing._read_data')
def test_add_start_and_end_time_to_active_fires_data(readdata):
    """Test adding start and end times to the active fires data."""

    myfilepath = TEST_ACTIVE_FIRES_FILEPATH

    fstream = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA)
    afdata = pd.read_csv(fstream, index_col=None, header=None, comment='#', names=COL_NAMES)
    readdata.return_value = afdata

    this = ActiveFiresShapefileFiltering(filepath=myfilepath)
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=False)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata
    assert this._afdata['starttime'].shape == (18,)

    assert str(this._afdata['starttime'][0]) == '2021-04-14 11:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 11:28:08'

    this = ActiveFiresShapefileFiltering(filepath=myfilepath)
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=True)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata

    # FIXME! How to check that a time zone localisation has been done?
    assert str(this._afdata['starttime'][0]) == '2021-04-14 11:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 11:28:08'
