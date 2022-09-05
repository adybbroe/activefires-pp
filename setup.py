#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021, 2022 Adam Dybbroe

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

"""Set up the activefires-postprocessing package."""

from setuptools import setup
from setuptools import find_packages

try:
    # HACK: https://github.com/pypa/setuptools_scm/issues/190#issuecomment-351181286
    # Stop setuptools_scm from including all repository files
    import setuptools_scm.integration
    setuptools_scm.integration.find_files = lambda _: []
except ImportError:
    pass

with open('./README.md', 'r') as fd:
    long_description = fd.read()

description = 'Post-processing of and notifications on Satellite active fire detections'

requires = ['posttroll', 'netifaces', 'trollsift', 'setuptools_scm', 'pycrs',
            'shapely', 'cartopy', 'pandas', 'geojson', 'fiona', 'geopy', 'matplotlib',
            'requests']
test_requires = ['mock', 'posttroll', 'trollsift', 'pycrs',
                 'shapely', 'cartopy', 'pandas', 'geojson', 'fiona',
                 'freezegun', 'responses']

setup(name="activefires-pp",
      description=description,
      author='Adam Dybroe',
      author_email='adam.dybroe@smhi.se',
      classifiers=["Development Status :: 3 - Alpha",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="https://github.com/adybbroe/activefires-pp",
      packages=find_packages(),
      long_description=long_description,
      license='GPLv3',
      scripts=['bin/active_fires_postprocessing.py',
               'bin/active_fires_notifier.py',
               'bin/active_fires_spatiotemporal_alarm_filtering.py', ],
      data_files=[],
      zip_safe=False,
      install_requires=requires,
      tests_require=test_requires,
      python_requires='>=3.9',
      use_scm_version=True
      )
