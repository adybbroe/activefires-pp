#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 - 2023 Adam.Dybbro

# Author(s):

#   Adam Dybbroe <Firstname.Lastname@smhi.se>

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

"""Post processing on the Active Fire detections."""

import socket
from trollsift import Parser, globify
import time
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import os
from six.moves.urllib.parse import urlparse
from geojson import Feature, Point, FeatureCollection, dump
import logging
import signal
from queue import Empty
from threading import Thread
from posttroll.listener import ListenerContainer
from posttroll.message import Message
from posttroll.publisher import NoisyPublisher
import pyproj
from matplotlib.path import Path
import shapely

from activefires_pp.utils import datetime_utc2local
from activefires_pp.utils import get_local_timezone_offset
from activefires_pp.utils import json_serial
from activefires_pp.config import read_config
from activefires_pp.geometries_from_shapefiles import ShapeGeometry

# M-band output:
# column 1: latitude of fire pixel (degrees)
# column 2: longitude of fire pixel (degrees)
# column 3: M13 brightness temperature of fire pixel (K)
# column 4: Along-scan fire pixel resolution (km)
# column 5: Along-track fire pixel resolution (km)
# column 6: detection confidence (%)
#           7=low, 8=nominal, 9=high (Imager Resolution)
#           0-100 % (Moderate Resolution)
# column 7: fire radiative power (MW)
# I-band output:
# column 1: latitude of fire pixel (degrees)
# column 2: longitude of fire pixel (degrees)
# column 3: I04 brightness temperature of fire pixel (K)
# column 4: Along-scan fire pixel resolution (km)
# column 5: Along-track fire pixel resolution (km)
# column 6: detection confidence ([7,8,9]->[lo,med,hi])
# column 7: fire radiative power (MW)
#
COL_NAMES = ["latitude", "longitude", "tb", "along_scan_res", "along_track_res", "conf", "power"]

NO_FIRES_TEXT = 'No fire detections for this granule'


logger = logging.getLogger(__name__)
logging.getLogger("fiona").setLevel(logging.WARNING)


class ActiveFiresShapefileFiltering(object):
    """Reading, filtering and writing Active Fire detections.

    Reading either the CSPP VIIRS AF output (txt files) or the Geojson formatted files.
    Filtering for static and false alarms, and/or simply on geographical regions.
    Data is stored in geojson format.
    """

    def __init__(self, filepath=None, afdata=None, platform_name=None, timezone='GMT'):
        """Initialize the ActiveFiresShapefileFiltering class."""
        self.input_filepath = filepath
        self._afdata = afdata
        if afdata is None:
            self.metadata = {}
        else:
            self.metadata = afdata.attrs

        self.timezone = timezone
        self.platform_name = platform_name

    def get_af_data(self, filepattern=None, localtime=True):
        """Read the Active Fire results from file - ascii formatted output from CSPP VIIRS-AF."""
        if self._afdata is not None:
            # Make sure the attrs are populated with metadata instance attribute
            self._afdata.attrs.update(self.metadata)
            return self._afdata

        if not self.input_filepath or not os.path.exists(self.input_filepath):
            # FIXME! Better to raise an exception!?
            return self._afdata

        if not filepattern:
            raise AttributeError("file pattern must be provided in order to be able to read from file!")

        self.metadata = self._get_metadata_from_filename(filepattern)
        self._afdata = _read_data(self.input_filepath)
        self._add_start_and_end_time_to_active_fires_data(localtime)

        return self._afdata

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

        starttime = starttime.replace(tzinfo=None)
        endtime = endtime.replace(tzinfo=None)

        self._afdata['starttime'] = np.repeat(starttime, len(self._afdata)).astype(np.datetime64)
        self._afdata['endtime'] = np.repeat(endtime, len(self._afdata)).astype(np.datetime64)

        logger.info('Start and end times: %s %s',
                    str(self._afdata['starttime'][0]),
                    str(self._afdata['endtime'][0]))

    def _apply_timezone_offset(self, obstime):
        """Apply the time zone offset to the datetime objects."""
        obstime_offset = get_local_timezone_offset(self.timezone)
        return np.repeat(obstime.replace(tzinfo=None) + obstime_offset,
                         len(self._afdata)).astype(np.datetime64)

    def fires_filtering(self, shapefile, start_geometries_index=1, inside=True):
        """Remove fires outside National borders or filter out potential false detections.

        If *inside* is True the filtering will keep those detections that are inside the polygon.
        If *inside* is False the filtering will disregard the detections that are inside the polygon.
        """
        detections = self._afdata

        lons = detections.longitude.values
        lats = detections.latitude.values

        toc = time.time()
        points_inside = get_global_mask_from_shapefile(shapefile, (lons, lats), start_geometries_index)
        logger.debug("Time used checking inside polygon - mpl path method: %f", time.time() - toc)

        self._afdata = detections[points_inside == inside]

        if len(self._afdata) == 0:
            logger.debug("No fires after filtering on Polygon...")
        else:
            logger.debug("Number of detections after filtering on Polygon: %d", len(self._afdata))

    def get_regional_filtermasks(self, shapefile, globstr):
        """Get the regional filter masks from the shapefile."""
        detections = self._afdata

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

            if isinstance(geometry, shapely.geometry.multipolygon.MultiPolygon):
                regional_masks[test_omr]['mask'] = get_mask_from_multipolygon(points, geometry)
            else:
                shape = geometry
                pth = Path(shape.exterior.coords)
                regional_masks[test_omr]['mask'] = pth.contains_points(points)

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


def _read_data(filepath):
    """Read the AF data."""
    with open(filepath, 'r') as fpt:
        return pd.read_csv(fpt, index_col=None, header=None, comment='#', names=COL_NAMES)


def get_metadata_from_filename(infile_pattern, filepath):
    """From the filename and its pattern get basic metadata of the satellite observations."""
    p__ = Parser(infile_pattern)
    fname = os.path.basename(filepath)
    try:
        res = p__.parse(fname)
    except ValueError:
        # Do something!
        return None

    # Fix the end time:
    endtime = datetime(res['start_time'].year, res['start_time'].month,
                       res['start_time'].day, res['end_hour'].hour, res['end_hour'].minute,
                       res['end_hour'].second)
    if endtime < res['start_time']:
        endtime = endtime + timedelta(days=1)

    res['end_time'] = endtime

    return res


def store(output_filename, detections):
    """Store the filtered AF detections on disk."""
    if len(detections) > 0:
        detections.to_csv(output_filename, index=False)
        return output_filename
    else:
        logger.debug("No detections to save!")
        return None


def geojson_feature_collection_from_detections(detections, platform_name=None):
    """Create the Geojson feature collection from fire detection data."""
    if len(detections) == 0:
        logger.debug("No detections to save!")
        return None

    # Convert points to GeoJSON
    features = []
    for idx in range(len(detections)):
        starttime = detections.iloc[idx].starttime
        endtime = detections.iloc[idx].endtime
        mean_granule_time = starttime.to_pydatetime() + (endtime.to_pydatetime() -
                                                         starttime.to_pydatetime()) / 2.

        prop = {'power': detections.iloc[idx].power,
                'tb': detections.iloc[idx].tb,
                'confidence': int(detections.iloc[idx].conf),
                'id': detections.iloc[idx].detection_id,
                'observation_time': json_serial(mean_granule_time)
                }
        if platform_name:
            prop['platform_name'] = platform_name
        else:
            logger.debug("No platform name specified for output")

        feat = Feature(
            geometry=Point(map(float, [detections.iloc[idx].longitude, detections.iloc[idx].latitude])),
            properties=prop)
        features.append(feat)

    return FeatureCollection(features)


def store_geojson(output_filename, feature_collection):
    """Store the Geojson feature collection of fire detections on disk."""
    path = os.path.dirname(output_filename)
    if not os.path.exists(path):
        logger.info("Create directory: %s", path)
        os.makedirs(path)

    with open(output_filename, 'w') as fpt:
        dump(feature_collection, fpt)


def get_mask_from_multipolygon(points, geometry, start_idx=1):
    """Get mask for points from a shapely Multipolygon."""
    shape = geometry.geoms[0]
    pth = Path(shape.exterior.coords)
    mask = pth.contains_points(points)

    if sum(mask) == len(points):
        return mask

    constituent_part = geometry.geoms[start_idx:]
    for shape in constituent_part.geoms:
        pth = Path(shape.exterior.coords)
        mask = np.logical_or(mask, pth.contains_points(points))
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

    # There is only one geometry/multi-polygon!
    geometry = shape_geom.geometries[0]

    metersx, metersy = p__(lons, lats)
    points = np.vstack([metersx, metersy]).T

    return get_mask_from_multipolygon(points, geometry, start_geom_index)


class ActiveFiresPostprocessing(Thread):
    """The active fires post processor."""

    def __init__(self, configfile, shp_borders, shp_mask, regional_filtermask=None):
        """Initialize the active fires post processor class."""
        super().__init__()
        self.shp_borders = shp_borders
        self.shp_filtermask = shp_mask

        self.regional_filtermask = regional_filtermask
        self.configfile = configfile
        self.options = {}

        config = read_config(self.configfile)
        self._set_options_from_config(config)

        self.host = socket.gethostname()
        self.timezone = self.options.get('timezone', 'GMT')

        self.input_topic = self.options['subscribe_topics'][0]
        self.output_topic = self.options['publish_topic']
        self.infile_pattern = self.options.get('af_pattern_ibands')
        self.outfile_pattern_national = self.options.get('geojson_file_pattern_national')
        self.outfile_pattern_regional = self.options.get('geojson_file_pattern_regional')
        self.output_dir = self.options.get('output_dir', '/tmp')
        self.filepath_detection_id_cache = self.options.get('filepath_detection_id_cache')

        frmt = self.options['regional_shapefiles_format']
        self.regional_shapefiles_globstr = globify(frmt)

        self._fire_detection_id = None
        self._initialize_fire_detection_id()

        self.listener = None
        self.publisher = None
        self.loop = False
        self._setup_and_start_communication()

    def _setup_and_start_communication(self):
        """Set up the Posttroll communication and start the publisher."""
        logger.debug("Starting up... Input topic: %s", self.input_topic)
        now = datetime_utc2local(datetime.now(), self.timezone)
        logger.debug("Output times for timezone: {zone} Now = {time}".format(zone=str(self.timezone), time=now))

        self._check_borders_shapes_exists()

        self.listener = ListenerContainer(topics=[self.input_topic])
        self.publisher = NoisyPublisher("active_fires_postprocessing")
        self.publisher.start()
        self.loop = True
        signal.signal(signal.SIGTERM, self.signal_shutdown)

    def _set_options_from_config(self, config):
        """From the configuration on disk set the option dictionary, holding all metadata for processing."""
        for item in config:
            if not isinstance(config[item], dict):
                self.options[item] = config[item]

        if isinstance(self.options.get('subscribe_topics'), str):
            subscribe_topics = self.options.get('subscribe_topics').split(',')
            for item in subscribe_topics:
                if len(item) == 0:
                    subscribe_topics.remove(item)
            self.options['subscribe_topics'] = subscribe_topics

        if isinstance(self.options.get('publish_topics'), str):
            publish_topics = self.options.get('publish_topics').split(',')
            for item in publish_topics:
                if len(item) == 0:
                    publish_topics.remove(item)
            self.options['publish_topics'] = publish_topics

    def signal_shutdown(self, *args, **kwargs):
        """Shutdown the Active Fires postprocessing."""
        self.close()

    def check_incoming_message_and_get_filename(self, msg):
        """Check the message content and return filename if okay."""
        if msg.type not in ['file', 'collection', 'dataset']:
            logger.debug("Message type not supported: %s", str(msg.type))
            return None

        filename = get_filename_from_uri(msg.data.get('uri'))
        if not os.path.exists(filename):
            logger.warning("File does not exist: %s", filename)
            return None

        file_ok = check_file_type_okay(msg.data.get('type'))
        if not file_ok:
            output_messages = self._generate_no_fires_messages(msg, NO_FIRES_TEXT)
            for output_msg in output_messages:
                logger.debug("Sending message: %s", str(output_msg))
                self.publisher.send(str(output_msg))
            return None

        return filename

    def do_postprocessing_on_message(self, msg, filename):
        """Do the fires post processing on a message."""
        platform_name = msg.data.get('platform_name')
        af_shapeff = ActiveFiresShapefileFiltering(filename, platform_name=platform_name,
                                                   timezone=self.timezone)
        afdata = af_shapeff.get_af_data(self.infile_pattern)
        if len(afdata) == 0:
            output_messages = self._generate_no_fires_messages(msg, NO_FIRES_TEXT)
            for output_msg in output_messages:
                logger.debug("Sending message: %s", str(output_msg))
                self.publisher.send(str(output_msg))
            return

        afdata = self.fires_filtering(msg, af_shapeff)
        logger.debug("After fires_filtering...: Number of fire detections = %d", len(afdata))
        if len(afdata) == 0:
            logger.debug("No fires - so no regional filtering to be done!")
            return

        # It is here that we should add a uniue day-ID to each of the detections!
        afdata = self.add_unique_day_id(afdata)
        self.save_id_to_file()

        # 1) Create geojson feature collection
        # 2) Dump geojson data to disk
        feature_collection = geojson_feature_collection_from_detections(afdata,
                                                                        platform_name=af_shapeff.platform_name)

        fmda = af_shapeff.metadata
        pout = Parser(self.outfile_pattern_national)
        out_filepath = os.path.join(self.output_dir, pout.compose(fmda))
        logger.debug("Output file path = %s", out_filepath)

        if feature_collection is None:
            logger.info("No geojson file created, number of fires after filtering = %d", len(afdata))
            output_messages = self._generate_no_fires_messages(msg,
                                                               'No true fire detections inside National borders')  # noqa
        else:
            store_geojson(out_filepath, feature_collection)
            output_messages = self.get_output_messages(out_filepath, msg, len(afdata))

        for output_msg in output_messages:
            if output_msg:
                logger.debug("Sending message: %s", str(output_msg))
                self.publisher.send(str(output_msg))

        # Do the regional filtering now:
        if not self.regional_filtermask:
            logger.info("No regional filtering is attempted.")
            return

        # FIXME! If afdata is empty (len=0) then it seems all data are inside all regions!
        af_shapeff = ActiveFiresShapefileFiltering(afdata=afdata, platform_name=platform_name,
                                                   timezone=self.timezone)
        regional_fmask = af_shapeff.get_regional_filtermasks(self.regional_filtermask,
                                                             globstr=self.regional_shapefiles_globstr)
        regional_messages = self.regional_fires_filtering_and_publishing(msg, regional_fmask, af_shapeff)
        for region_msg in regional_messages:
            logger.debug("Sending message: %s", str(region_msg))
            self.publisher.send(str(region_msg))

    def run(self):
        """Run the AF post processing."""
        while self.loop:
            try:
                msg = self.listener.output_queue.get(timeout=1)
                logger.debug("Message: %s", str(msg.data))
            except Empty:
                continue
            else:
                filename = self.check_incoming_message_and_get_filename(msg)
                if not filename:
                    continue

                self.do_postprocessing_on_message(msg, filename)

    def regional_fires_filtering_and_publishing(self, msg, regional_fmask, afsff_obj):
        """From the regional-fires-filter-mask and the fire detection data send regional messages."""
        logger.debug("Perform regional masking on VIIRS AF detections and publish accordingly.")

        afdata = afsff_obj.get_af_data()
        fmda = afsff_obj.metadata

        fmda['platform'] = afsff_obj.platform_name

        pout = Parser(self.outfile_pattern_regional)

        output_messages = []
        regions_with_detections = 0
        for region_name in regional_fmask:
            if not regional_fmask[region_name]['some_inside_test_area']:
                continue

            regions_with_detections = regions_with_detections + 1
            fmda['region_name'] = regional_fmask[region_name]['attributes']['Kod_omr']

            out_filepath = os.path.join(self.output_dir, pout.compose(fmda))
            logger.debug("Output file path = %s", out_filepath)
            data_in_region = afdata[regional_fmask[region_name]['mask']]

            # filepath = store_geojson(out_filepath, data_in_region, platform_name=fmda['platform'])
            feature_collection = geojson_feature_collection_from_detections(data_in_region,
                                                                            platform_name=fmda['platform'])
            if feature_collection is None:
                logger.warning("Something wrong happended storing regional " +
                               "data to Geojson - area: {name}".format(name=str(region_name)))
                continue

            store_geojson(out_filepath, feature_collection)

            outmsg = self._generate_output_message(out_filepath, msg, regional_fmask[region_name])
            output_messages.append(outmsg)
            logger.info("Geojson file created! Number of fires in region = %d", len(data_in_region))

        logger.debug("Regional masking done. Number of regions with fire " +
                     "detections on this granule: %s" % str(regions_with_detections))
        return output_messages

    def fires_filtering(self, msg, af_shapeff):
        """Read Active Fire data and perform spatial filtering removing false detections.

        Do the national filtering first, and then filter out potential false
        detections by the special mask for that.

        """
        logger.debug("Read VIIRS AF detections and perform quality control and spatial filtering")

        fmda = af_shapeff.metadata
        # metdata contains time and everything but it is not being transfered to the dataframe.attrs

        pout = Parser(self.outfile_pattern_national)
        out_filepath = os.path.join(self.output_dir, pout.compose(fmda))
        logger.debug("Output file path = %s", out_filepath)

        # National filtering:
        af_shapeff.fires_filtering(self.shp_borders)

        # Metadata should be transfered here!
        afdata_ff = af_shapeff.get_af_data()

        if len(afdata_ff) > 0:
            logger.debug("Doing the fires filtering: shapefile-mask = %s", str(self.shp_filtermask))
            af_shapeff.fires_filtering(self.shp_filtermask, start_geometries_index=0, inside=False)
            afdata_ff = af_shapeff.get_af_data()
            logger.debug("After fires_filtering: Number of fire detections left: %d", len(afdata_ff))

        return afdata_ff

    def get_output_messages(self, filepath, msg, number_of_data):
        """Generate the adequate output message(s) depending on if an output file was created or not."""
        logger.info("Geojson file created! Number of fires = %d", number_of_data)
        return [self._generate_output_message(filepath, msg)]

    def _generate_output_message(self, filepath, input_msg, region=None):
        """Create the output message to publish."""
        output_topic = generate_posttroll_topic(self.output_topic, region)
        to_send = prepare_posttroll_message(input_msg, region)
        to_send['uri'] = ('ssh://%s/%s' % (self.host, filepath))
        to_send['uid'] = os.path.basename(filepath)
        to_send['type'] = 'GEOJSON-filtered'
        to_send['format'] = 'geojson'
        to_send['product'] = 'afimg'
        pubmsg = Message(output_topic, 'file', to_send)
        return pubmsg

    def _generate_no_fires_messages(self, input_msg, msg_string):
        """Create the output messages to publish."""
        to_send = prepare_posttroll_message(input_msg)
        to_send['info'] = msg_string
        publish_messages = []
        for ext in ['National', 'Regional']:
            topic = self.output_topic + '/' + ext
            publish_messages.append(Message(topic, 'info', to_send))

        return publish_messages

    def _check_borders_shapes_exists(self):
        """Check that the national borders shapefile exists on disk."""
        if not os.path.exists(self.shp_borders):
            raise OSError("Shape file does not exist! Filename = %s" % self.shp_borders)

    def _initialize_fire_detection_id(self):
        """Initialize the fire detection ID."""
        if self.filepath_detection_id_cache and os.path.exists(self.filepath_detection_id_cache):
            self._fire_detection_id = self.get_id_from_file()
        else:
            self._fire_detection_id = {'date': datetime.utcnow(), 'counter': 0}

    def update_fire_detection_id(self):
        """Update the fire detection ID registry."""
        now = datetime.utcnow()
        tdelta = now - self._fire_detection_id['date']
        if tdelta.total_seconds() > 24*3600:
            self._initialize_fire_detection_id()
        elif tdelta.total_seconds() > 0 and self._fire_detection_id['date'].day != now.day:
            self._initialize_fire_detection_id()

        self._fire_detection_id['counter'] = self._fire_detection_id['counter'] + 1

    def save_id_to_file(self):
        """Save the (current) detection id on disk.

        It is assumed that the user permissions are so that a file can actually
        be written to disk here!
        """
        with open(self.filepath_detection_id_cache, 'w') as fpt:
            id_ = self._create_id_string()
            fpt.write(id_)

    def get_id_from_file(self):
        """Read the latest stored detection id string from disk and convert to internal format."""
        with open(self.filepath_detection_id_cache, 'r') as fpt:
            idstr = fpt.read()

        return self._get_id_from_string(idstr)

    def _get_id_from_string(self, idstr):
        """Get the detection id from string."""
        datestr, counter = idstr.split('-')
        return {'date': datetime.strptime(datestr, '%Y%m%d'),
                'counter': int(counter)}

    def _create_id_string(self):
        """From the internal fire detection id create the id string to be exposed to the user."""
        return (self._fire_detection_id['date'].strftime('%Y%m%d') +
                '-' + str(self._fire_detection_id['counter']))

    def add_unique_day_id(self, afdata):
        """Add a unique detection id - date + a running number for the day."""
        # Add id's to the detections:
        id_list = []
        for _i in range(len(afdata)):
            self.update_fire_detection_id()
            id_ = self._create_id_string()
            id_list.append(id_)

        afdata['detection_id'] = id_list
        return afdata

    def close(self):
        """Shutdown the Active Fires postprocessing."""
        logger.info('Terminating Active Fires post processing.')
        self.loop = False
        logger.info('Dumping the latest detection id to disk: %s', str(self.filepath_detection_id_cache))
        self.save_id_to_file()
        try:
            self.listener.stop()
        except Exception:
            logger.exception("Couldn't stop listener.")
        if self.publisher:
            try:
                self.publisher.stop()
            except Exception:
                logger.exception("Couldn't stop publisher.")


def check_file_type_okay(file_type):
    """Check if the file is of the correct type."""
    if file_type not in ['txt', 'TXT']:
        logger.info('File type not txt: %s', str(file_type))
        return False
    return True


def get_filename_from_uri(uri):
    """Get the file name from the uri given."""
    logger.info('File uri: %s', str(uri))
    url = urlparse(uri)
    return url.path


def generate_posttroll_topic(output_topic, region=None):
    """Create the topic for the posttroll message to publish."""
    if region:
        output_topic = output_topic + '/Regional/' + region['attributes']['Kod_omr']
    else:
        output_topic = output_topic + '/National'

    return output_topic


def prepare_posttroll_message(input_msg, region=None):
    """Create the basic posttroll-message fields and return."""
    to_send = input_msg.data.copy()
    to_send.pop('dataset', None)
    to_send.pop('collection', None)
    to_send.pop('uri', None)
    to_send.pop('uid', None)
    to_send.pop('format', None)
    to_send.pop('type', None)
    # FIXME! Check that the region_name is stored as a unicode string!
    if region:
        to_send['region_name'] = region['attributes']['Testomr']
        to_send['region_code'] = region['attributes']['Kod_omr']

    return to_send
