#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 - 2024 Adam.Dybbroe

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

"""The activefires_pp package initialization."""

try:
    from activefires_pp.version import version as __version__  # noqa
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "No module named activefires_pp.version. This could mean "
        "you didn't install 'activefires_pp' properly. Try reinstalling ('pip "
        "install activefires-pp').")
