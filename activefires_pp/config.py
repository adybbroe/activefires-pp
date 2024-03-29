#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Adam Dybbroe

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

"""Handling the yaml configurations."""

import yaml
from yaml import UnsafeLoader, Loader


def read_config(config_filepath):
    """Read and extract config information."""
    with open(config_filepath, 'r') as fp_:
        config = yaml.load(fp_, Loader=UnsafeLoader)

    return config


def get_xauthentication_token(xauth_filepath):
    """Get the X-Authentication-token needed for posting to the API."""
    with open(xauth_filepath, 'r') as fp_:
        tokens = yaml.load(fp_, Loader=Loader)

    return tokens['xauth_tokens']['x-auth-satellite-alarm']
