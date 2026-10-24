#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021-2026 Adam Dybbroe

"""Core active-fire reading and filtering independent of runner orchestration."""

from dataclasses import dataclass, field
import datetime as dt
import logging
import os
import time
from typing import Any

import numpy as np
import pandas as pd
import pyproj
import shapely
from trollsift import Parser

from activefires_pp.geometries_from_shapefiles import ShapeGeometry
from activefires_pp.sanity_check_detections import remove_spurious_detections
from activefires_pp.utils import datetime_utc2local


logger = logging.getLogger(__name__)
logging.getLogger("fiona").setLevel(logging.WARNING)


COL_NAMES = ["latitude", "longitude", "tb", "along_scan_res", "along_track_res", "conf", "power"]
COL_NAMES_CSPP21 = [
    "latitude", "longitude", "tb", "along_scan_res", "along_track_res", "conf", "power", "anomaly"
]


class CSPP_ASCII_FILE_FORMAT_ERROR(Exception):
    """Exception to catch CSPP ascii files of unexpected format."""


@dataclass(frozen=True)
class ProcessingOptions:
    """Configuration controlling the reusable active-fire processing pipeline."""

    national_boundary: str | os.PathLike[str]
    filter_mask: str | os.PathLike[str]
    infile_pattern: str | None
    timezone: str = "GMT"
    remove_spurious: bool = True

    @classmethod
    def from_config(cls, config: dict[str, Any], national_boundary, filter_mask):
        """Create processing options from the operational runner configuration."""
        return cls(
            national_boundary=national_boundary,
            filter_mask=filter_mask,
            infile_pattern=config.get("af_pattern_ibands"),
            timezone=config.get("timezone", "GMT"),
        )


@dataclass
class ActiveFiresProcessingResult:
    """Result returned by :class:`ActiveFiresProcessor`."""

    detections: pd.DataFrame
    platform_name: str | None
    start_time: dt.datetime | None
    end_time: dt.datetime | None
    metadata: dict[str, Any] = field(default_factory=dict)
    input_detection_count: int = 0


class ActiveFiresProcessor:
    """Read and filter active-fire detections without runner side effects."""

    def __init__(self, options: ProcessingOptions):
        """Initialize a processor with reusable processing options."""
        self.options = options

    @classmethod
    def from_config(cls, config: dict[str, Any], national_boundary, filter_mask):
        """Create a processor directly from the operational configuration."""
        return cls(ProcessingOptions.from_config(config, national_boundary, filter_mask))

    def filter_detections(self, active_fires):
        """Apply the configured filtering pipeline to loaded detections."""
        detections = active_fires.get_af_data()

        if detections is None or len(detections) == 0:
            return detections

        active_fires.fires_shapefile_filtering(
            self.options.national_boundary
        )
        detections = active_fires.get_af_data()

        if self.options.remove_spurious:
            detections = remove_spurious_detections(detections)
            active_fires.afdata = detections

        if len(detections) > 0:
            active_fires.fires_shapefile_filtering(
                self.options.filter_mask,
                start_geometries_index=0,
                inside=False,
            )
            detections = active_fires.get_af_data()

        return detections

    def process(self, filename, platform_name=None):
        """Read and filter one active-fire input file."""
        active_fires = ActiveFiresShapefileFiltering(
            filename,
            platform_name=platform_name,
            timezone=self.options.timezone,
        )
        detections = active_fires.get_af_data(self.options.infile_pattern)
        input_detection_count = 0 if detections is None else len(detections)

        if detections is None:
            detections = pd.DataFrame()
        elif len(detections) > 0:
            detections = self.filter_detections(active_fires)

        metadata = dict(active_fires.metadata)
        detections.attrs.update(metadata)
        return ActiveFiresProcessingResult(
            detections=detections,
            platform_name=active_fires.platform_name,
            start_time=metadata.get("start_time"),
            end_time=metadata.get("end_time"),
            metadata=metadata,
            input_detection_count=input_detection_count,
        )


class ActiveFiresShapefileFiltering(object):
    """Reading and spatial filtering of active-fire detections."""

    def __init__(self, filepath=None, afdata=None, platform_name=None, timezone='GMT'):
        """Initialize the ActiveFiresShapefileFiltering class."""
        self.input_filepath = filepath
        self.afdata = afdata
        if afdata is None:
            self.metadata = {}
        else:
            self.metadata = afdata.attrs

        self.timezone = timezone
        self.platform_name = platform_name

    def get_af_data(self, filepattern=None, localtime=True):
        """Read the Active Fire results from file - ascii formatted output from CSPP VIIRS-AF."""
        if self.afdata is not None:
            self.afdata.attrs.update(self.metadata)
            return self.afdata

        if not self.input_filepath or not os.path.exists(self.input_filepath):
            return self.afdata

        if not filepattern:
            raise AttributeError("file pattern must be provided in order to be able to read from file!")

        self.metadata = self._get_metadata_from_filename(filepattern)
        self.afdata = read_cspp_output_data(self.input_filepath)
        self._add_start_and_end_time_to_active_fires_data(localtime)

        return self.afdata

    def _get_metadata_from_filename(self, infile_pattern):
        """From the filename retrieve the metadata such as satellite and sensing time."""
        return get_metadata_from_filename(infile_pattern, self.input_filepath)

    def _add_start_and_end_time_to_active_fires_data(self, localtime):
        """Add start and end time to active fires data."""
        if localtime:
            logger.info("Convert to local time zone!")
            starttime = datetime_utc2local(self.metadata['start_time'], self.timezone)
            endtime = datetime_utc2local(self.metadata['end_time'], self.timezone)
        else:
            starttime = datetime_utc2local(self.metadata['start_time'], 'GMT')
            endtime = datetime_utc2local(self.metadata['end_time'], 'GMT')

        self.afdata['starttime'] = starttime
        self.afdata['endtime'] = endtime

        logger.info('Start and end times: %s %s',
                    str(self.afdata['starttime'][0]),
                    str(self.afdata['endtime'][0]))

    def fires_shapefile_filtering(self, shapefile, start_geometries_index=1, inside=True):
        """Remove fires outside borders or inside a false-alarm mask."""
        detections = self.afdata

        lons = detections.longitude.values
        lats = detections.latitude.values

        toc = time.time()
        points_inside = get_global_mask_from_shapefile(shapefile, (lons, lats), start_geometries_index)
        logger.debug("Time used checking inside polygon - mpl path method: %f", time.time() - toc)
        self.afdata = detections[points_inside == inside]

        if len(self.afdata) == 0:
            logger.debug("No fires after filtering on Polygon...")
        else:
            logger.debug("Number of detections after filtering on Polygon: %d", len(self.afdata))

    def get_regional_filtermasks(self, shapefile, globstr):
        """Get the regional filter masks from the shapefile."""
        detections = self.afdata

        lons = detections.longitude.values
        lats = detections.latitude.values

        logger.debug("Before ShapeGeometry instance - shapefile name = %s" % str(shapefile))
        logger.debug("Shape file glob-string = %s" % str(globstr))
        shape_geom = ShapeGeometry(shapefile, globstr)
        shape_geom.load()

        p__ = pyproj.Proj(shape_geom.proj4str)
        metersx, metersy = p__(lons, lats)
        points = np.vstack([metersx, metersy]).T

        regional_masks = {}

        for attr, geometry in zip(shape_geom.attributes, shape_geom.geometries):
            test_omr = attr['Testomr']
            all_inside_test_omr = False
            some_inside_test_omr = False
            logger.debug(u'Test area: {}'.format(str(test_omr)))

            regional_masks[test_omr] = {'mask': None, 'attributes': attr}

            regional_masks[test_omr]['mask'] = get_mask_from_multipolygon(points, geometry)

            if sum(regional_masks[test_omr]['mask']) == len(points):
                all_inside_test_omr = True
                some_inside_test_omr = True
                logger.debug("All points inside test area!")
            elif sum(regional_masks[test_omr]['mask']) > 0:
                some_inside_test_omr = True
                logger.debug("Some points inside test area!")

            regional_masks[test_omr]['all_inside_test_area'] = all_inside_test_omr
            regional_masks[test_omr]['some_inside_test_area'] = some_inside_test_omr

        return regional_masks


def read_cspp_output_data(filepath):
    """Read the CSPP AF output data - ascii format."""
    column_names = _check_ascii_fileformat(filepath)
    with open(filepath, 'r') as fpt:
        return pd.read_csv(fpt, index_col=None, header=None, comment='#', names=column_names)


def _check_ascii_fileformat(filepath):
    """Open ascii file and check the number of columns to decide the format."""
    with open(filepath, 'r') as fpt:
        data = pd.read_csv(fpt, index_col=None, header=None, comment='#', nrows=1)

    if len(data.columns) == 8:
        return COL_NAMES_CSPP21
    elif len(data.columns) == 7:
        return COL_NAMES
    raise CSPP_ASCII_FILE_FORMAT_ERROR(
        'Unexpected number of data columns in file! '
        f'{len(data.columns)} (should be either 7 or 8)'
    )


def get_metadata_from_filename(infile_pattern, filepath):
    """From the filename and its pattern get basic metadata of the satellite observations."""
    p__ = Parser(infile_pattern)
    fname = os.path.basename(filepath)
    try:
        res = p__.parse(fname)
    except ValueError:
        return None

    endtime = dt.datetime(res['start_time'].year, res['start_time'].month,
                          res['start_time'].day, res['end_hour'].hour, res['end_hour'].minute,
                          res['end_hour'].second)
    if endtime < res['start_time']:
        endtime = endtime + dt.timedelta(days=1)

    res['end_time'] = endtime
    return res


def store(output_filename, detections):
    """Store the filtered AF detections on disk."""
    if len(detections) > 0:
        detections.to_csv(output_filename, index=False)
        return output_filename
    logger.debug("No detections to save!")
    return None


def _get_mask_from_polygon(points, geometry):
    """Return mask for points covered by a Polygon.

    ``covers`` treats the polygon boundary as inside while respecting interior
    rings, so points strictly inside holes are excluded.
    """
    return np.array([geometry.covers(shapely.geometry.Point(point[0], point[1])) for point in points], dtype=bool)


def get_mask_from_multipolygon(points, geometry, start_idx=1):
    """Get mask for points covered by a Polygon or MultiPolygon.

    Polygon boundaries are considered inside. Interior rings (holes) are
    respected. ``start_idx`` retains the historical iteration behaviour for
    MultiPolygons.
    """
    if isinstance(geometry, shapely.geometry.polygon.Polygon):
        return _get_mask_from_polygon(points, geometry)

    shape = geometry.geoms[0]
    mask = _get_mask_from_polygon(points, shape)

    if sum(mask) == len(points):
        return mask

    constituent_part = geometry.geoms[start_idx:]
    for shape in constituent_part.geoms:
        mask = np.logical_or(mask, _get_mask_from_polygon(points, shape))
        if sum(mask) == len(points):
            break

    return mask


def get_global_mask_from_shapefile(shapefile, lonlats, start_geom_index=0):
    """Given geographical (lon,lat) points get a mask to apply when filtering."""
    lons, lats = lonlats
    logger.debug("Getting the global mask from file: shapefile file path = %s", str(shapefile))
    shape_geom = ShapeGeometry(shapefile)
    shape_geom.load()

    p__ = pyproj.Proj(shape_geom.proj4str)
    geometry = shape_geom.geometries[0]

    metersx, metersy = p__(lons, lats)
    points = np.vstack([metersx, metersy]).T

    return get_mask_from_multipolygon(points, geometry, start_geom_index)
