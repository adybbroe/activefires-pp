#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021-2023 Adam.Dybbroe

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

"""Unit testing the loading of the polygon geometries from the shapefile."""

import os
from unittest.mock import patch
import pytest
from shapely.geometry import MultiPolygon, Polygon
import numpy as np
import geopandas as gpd
import pandas as pd
import pyproj

from activefires_pp.geometries_from_shapefiles import ShapeGeometry
from activefires_pp.geometries_from_shapefiles import _get_proj_filename_from_shapefile
from activefires_pp.post_processing import get_mask_from_multipolygon

TEST_CRS_PROJ = ('+proj=utm +ellps=GRS80 +a=6378137.0 +rf=298.257222101 +pm=0 +x_0=500000.0 ' +
                 '+y_0=0.0 +lon_0=15.0 +lat_0=0.0 +units=m +axis=enu +no_defs')
TEST_CRS_PROJ_ZONE33 = ('+proj=utm +ellps=GRS80 +a=6378137.0 +rf=298.257222101 +pm=0 +x_0=500000.0 ' +
                        '+y_0=0.0 +lon_0=15.0 +lat_0=0.0 +units=m +axis=enu +no_defs +zone=33')


def fake_shapefiles_glob(dirname):
    """Fake returning a glob list of shapefiles."""
    list_of_files = []
    for idx in range(4):
        list_of_files.append(os.path.join(dirname, 'some_shape_file_name_%d.shp' % idx))

    return list_of_files


class MyMockProjName(object):
    """Mock a Proj.4 projection name."""

    def __init__(self):
        """Initialize the object."""
        self.proj4 = 'utm'


class MyMockProj(object):
    """Mock a Proj.4 object."""

    def __init__(self):
        """Initialize the object."""
        self.name = MyMockProjName()


class MyMockCrs(object):
    """Mock a CRS."""

    def __init__(self, crs_name='myname'):
        """Initialize the object."""
        self._crs_proj = TEST_CRS_PROJ
        self.name = crs_name
        self.proj = MyMockProj()

    def to_proj4(self):
        """CRS to Proj4."""
        return self._crs_proj


class _fake_geometry_records(object):
    def __init__(self):
        """Initialize the object."""
        self.geometry = 'Some geom'
        self.attributes = {'attr1': 1, 'attr2': 'myname'}


def fake_get_records():
    """Fake retrieving the generator class of the Geometry records in a shapefile."""
    num = 0
    while num < 10:
        yield _fake_geometry_records()
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


def test_get_proj_filename_from_shapefile():
    """Test get the filename of the prj file from the shapefile."""
    test_shape_filepath = '/path/to/my/shapefile/test_shapes.shp'
    retv = _get_proj_filename_from_shapefile(test_shape_filepath)
    assert retv == "/path/to/my/shapefile/test_shapes.prj"


# Prepare test shape file(s)
@pytest.fixture
def multipolygon_shapefile(tmp_path):
    """Return a file path to a shapefile with two simple polygons in a shapely multipolygon object."""
    shape_path = tmp_path / 'shapes1'

    polygon1 = ((16.087539330808468, 58.534966647195134),
                (14.578204884662805, 56.87163800385355),
                (13.289768863572561, 57.972381043637576),
                (16.087539330808468, 58.534966647195134))

    polygon2 = [(18.5070156681231, 57.77101070717485),
                (18.752558538975304, 57.38408540625345),
                (18.391771372137708, 57.62493709920018),
                (18.391771372137708, 57.62493709920018),
                (18.5070156681231, 57.77101070717485)]

    poly1 = Polygon(polygon1)
    poly2 = Polygon(polygon2)

    multip = MultiPolygon([poly1, poly2])

    wgs84 = pyproj.CRS('EPSG:4326')
    gpd.GeoDataFrame(pd.DataFrame(['p1'], columns=['geom']),
                     crs=wgs84,
                     geometry=[multip]).to_file(shape_path)

    yield shape_path


@pytest.mark.parametrize("lonlats, expected_ll",
                         [(np.array([[15.2, 15.2],
                                     [58.3, 58.7]]),
                           np.array([True, False])),
                          (np.array([[18.4112, 18.45, 18.5],
                                     [57.6317, 57.643, 57.643]]),
                           np.array([True, True, True])),
                          ]
                         )
def test_get_global_mask_from_shapefile(multipolygon_shapefile, lonlats, expected_ll):
    """From a shapefile test get a mask defining which points are inside the geometries."""
    from activefires_pp.post_processing import get_global_mask_from_shapefile

    retv_mask = get_global_mask_from_shapefile(multipolygon_shapefile,
                                               (lonlats[0], lonlats[1]))

    np.testing.assert_equal(retv_mask, expected_ll)


def test_get_mask_from_multipolygon(multipolygon_shapefile):
    """Test from a set of geo-points and a shapely geometry get a mask defining which points are inside."""
    points = np.array([[15.2, 58.3], [15.2, 58.7]])
    expected = np.array([True, False])

    globstr = os.path.dirname(multipolygon_shapefile) + '/shapes1/shapes1*shp'
    shape_geom = ShapeGeometry(multipolygon_shapefile, globstr)
    shape_geom.load()

    geometry = shape_geom.geometries[0]
    retv_mask = get_mask_from_multipolygon(points, geometry)

    np.testing.assert_equal(retv_mask, expected)
