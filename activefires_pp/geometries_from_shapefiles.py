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

"""Loading shapefile geometries from files."""

from glob import glob
import os
import cartopy.io.shapereader as shpreader
import pycrs


class ShapeGeometry(object):
    """Geometry from a shape file."""

    def __init__(self, shapefilepath, globstr='*.shp'):
        """Initialize the ShapeGeometry class."""
        self.filepaths = _get_shapefile_paths(shapefilepath, globstr)
        self.geometries = None
        self.attributes = None
        self._get_proj()

    def load(self):
        """Load the geometries and the associated attributes."""
        records = []
        for filepath in self.filepaths:
            records = records + [r for r in shpreader.Reader(filepath).records()]

        self._records = records
        self._load_member_from_records('geometries', 'geometry')
        self._load_member_from_records('attributes', 'attributes')

    def _get_proj(self):
        """Get and return the Proj.4 string."""
        self.proj4str = []
        for filepath in self.filepaths:
            prj_filename = filepath.strip('.shp') + '.prj'
            crs = pycrs.load.from_file(prj_filename)
            if crs.name == 'SWEREF99_TM' and crs.proj.name.proj4 == 'utm':
                utm_zone_proj4 = ' +zone=33'
                proj4str = crs.to_proj4() + utm_zone_proj4
            else:
                proj4str = crs.to_proj4()
            self.proj4str.append(proj4str)

        first_proj4_str = self.proj4str[0]
        for proj4_str in self.proj4str[1:]:
            if proj4_str != first_proj4_str:
                return

        self.proj4str = first_proj4_str

    def _load_member_from_records(self, class_member, record_type):
        """Load a member of the shapely.geometry object and set the corresponding class member."""
        setattr(self, class_member, [getattr(rec, record_type) for rec in self._records])


def _get_shapefile_paths(path, globstr='*.shp'):
    """Get full filepaths for all shapefiles in directory or simply return the paths as a list.

    From a path to a directory with shapefiles or a full file path,
    return list of file paths for all shapefiles.
    """
    if os.path.isfile(path):
        return [path]

    shapefile_paths = glob(os.path.join(path, globstr))
    if len(shapefile_paths) == 0:
        raise OSError('No matching shapefiles found on disk. Path = %s, glob-string = %s', path, globstr)
    return shapefile_paths
