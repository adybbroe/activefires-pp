#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021, 2022 Adam.Dybbroe

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

"""Unit testing the loading of the polygon geometries from the shapefile
"""

import os
from unittest.mock import patch

from activefires_pp.geometries_from_shapefiles import ShapeGeometry

TEST_CRS_PROJ = ('+proj=utm +ellps=GRS80 +a=6378137.0 +rf=298.257222101 +pm=0 +x_0=500000.0 ' +
                 '+y_0=0.0 +lon_0=15.0 +lat_0=0.0 +units=m +axis=enu +no_defs')
TEST_CRS_PROJ_ZONE33 = ('+proj=utm +ellps=GRS80 +a=6378137.0 +rf=298.257222101 +pm=0 +x_0=500000.0 ' +
                        '+y_0=0.0 +lon_0=15.0 +lat_0=0.0 +units=m +axis=enu +no_defs +zone=33')


def fake_shapefiles_glob(dirname):
    list_of_files = []
    for idx in range(4):
        list_of_files.append(os.path.join(dirname, 'some_shape_file_name_%d.shp' % idx))

    return list_of_files


class MyMockProjName(object):
    def __init__(self):
        self.proj4 = 'utm'


class MyMockProj(object):
    def __init__(self):
        self.name = MyMockProjName()


class MyMockCrs(object):
    def __init__(self, crs_name='myname'):
        self._crs_proj = TEST_CRS_PROJ
        self.name = crs_name
        self.proj = MyMockProj()

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


@patch('activefires_pp.geometries_from_shapefiles._get_shapefile_paths')
@patch('activefires_pp.geometries_from_shapefiles.pycrs.load.from_file')
def test_shape_geometry_init_single_shapefile_path(load_from_file, get_shapefile_paths):
    """Test creating the ShapeGeometry object from a path to a single shapefile."""

    mypath = '/my/shape/file/path/myshapefile.sph'

    get_shapefile_paths.return_value = [mypath]
    load_from_file.return_value = MyMockCrs('SWEREF99_TM')
    shpgeom = ShapeGeometry(mypath)

    assert shpgeom.proj4str == TEST_CRS_PROJ_ZONE33


@patch('activefires_pp.geometries_from_shapefiles._get_shapefile_paths')
@patch('activefires_pp.geometries_from_shapefiles.pycrs.load.from_file')
def test_shape_geometry_init_dirpath(load_from_file, get_shapefile_paths):
    """Test creating the ShapeGeometry object from a path to a directory with shapefiles."""

    mypath = '/my/shape/file/path/myshapefile.sph'
    get_shapefile_paths.return_value = fake_shapefiles_glob(mypath)

    load_from_file.return_value = MyMockCrs()
    shpgeom = ShapeGeometry(mypath)

    assert shpgeom.proj4str == TEST_CRS_PROJ


@patch('activefires_pp.geometries_from_shapefiles._get_shapefile_paths')
@patch('activefires_pp.geometries_from_shapefiles.pycrs.load.from_file')
def test_shape_geometry_loading(load_from_file, get_shapefile_paths):
    """Test loading the geometries and attributes from the shapefile."""

    mypath = '/my/shape/file/path/myshapefile.sph'
    get_shapefile_paths.return_value = [mypath]

    load_from_file.return_value = MyMockCrs()
    shpgeom = ShapeGeometry(mypath)

    with patch('activefires_pp.geometries_from_shapefiles.shpreader.Reader') as MockShpReader:
        MockShpReader.return_value.records.return_value = fake_get_records()
        shpgeom.load()

    assert shpgeom.geometries is not None
    assert shpgeom.attributes is not None

    assert len(shpgeom.geometries) == 10
    assert len(shpgeom.attributes) == 10
