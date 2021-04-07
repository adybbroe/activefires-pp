#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

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


import socket
import yaml
from yaml import UnsafeLoader
from trollsift import Parser
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
import time

from activefires_pp.utils import get_geometry_from_shapefile
from activefires_pp.utils import datetime_from_utc_to_local
from activefires_pp.utils import json_serial

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


LOG_FORMAT = "[%(asctime)s %(levelname)-8s] %(message)s"
logger = logging.getLogger(__name__)


class FiresShapefileFilter(object):
    def __init__(self, dataframe, outputfile, shp_boarders, shp_mask, platform_name=None):
        self.shp_boarders = shp_boarders
        self.shp_filtermask = shp_mask
        self.data = dataframe
        self.output_filename = outputfile
        self.platform_name = platform_name

    def fires_inside_sweden_mpl(self, lons, lats):
        """For an array of geographical positions (lon,lat) return a mask with points inside Sweden."""
        geometries = get_geometry_from_shapefile(self.shp_boarders)
        # Proj def hardcoded - FIXME!
        proj_def = "+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
        p__ = pyproj.Proj(proj_def)

        geometry = geometries[0]

        metersx, metersy = p__(lons, lats)
        points = np.vstack([metersx, metersy]).T

        shape = geometry.geoms[0]
        pth = Path(shape.exterior.coords)
        mask = pth.contains_points(points)
        for shape in geometry.geoms[1:]:
            pth = Path(shape.exterior.coords)
            mask = np.logical_or(mask, pth.contains_points(points))

        return mask

    def fires_inside_populated_areas_mpl(self, lons, lats):
        """Use a shapefile containing industries and populated areas and check if the fires are inside those areas."""
        # swereff99 TM
        # https://spatialreference.org/ref/epsg/sweref99-tm/proj4/
        # Proj def hardcoded - FIXME!
        proj_def = "+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
        p__ = pyproj.Proj(proj_def)

        geometries = get_geometry_from_shapefile(self.shp_filtermask)

        metersx, metersy = p__(lons, lats)
        points = np.vstack([metersx, metersy]).T

        shape = geometries[0].geoms[0]
        pth = Path(shape.exterior.coords)
        mask = pth.contains_points(points)
        # Counting the first shape in the first geometry twice...
        # FIXME!
        for geometry in geometries:
            for shape in geometry.geoms:
                pth = Path(shape.exterior.coords)
                mask = np.logical_or(mask, pth.contains_points(points))

        return mask

    def fires_filtering(self, detections):
        """Remove fires outside Sweden and keep only those outside populated areas and away from industries."""
        lons = detections.longitude.values
        lats = detections.latitude.values

        toc = time.time()
        insides = self.fires_inside_sweden_mpl(lons, lats)
        logger.debug("Time used checking inside Sweden - mpl path method: %f", time.time() - toc)

        retv = detections[insides]

        if len(retv) == 0:
            logger.debug("No fires in Sweden in this file...")
            return retv
        else:
            logger.debug("Number of detections inside Sweden: %d", len(retv))

        lons = retv.longitude.values
        lats = retv.latitude.values

        # Filter for populated areas and industries:
        toc = time.time()
        populated = self.fires_inside_populated_areas_mpl(lons, lats)
        logger.debug("Time used checking if fire is in a populated area or near an industry: %f", time.time() - toc)

        return retv[populated == False]

    def store(self, detections):
        """Store the filtered AF detections on disk."""
        if len(detections) > 0:
            detections.to_csv(self.output_filename, index=False)
            return self.output_filename
        else:
            logger.debug("No detections to save!")
            return None

    def store_geojson(self, detections):
        """Store the filtered AF detections on disk."""
        if len(detections) > 0:
            # Convert points to GeoJSON
            features = []
            for idx in range(len(detections)):
                starttime = detections.iloc[idx].starttime
                endtime = detections.iloc[idx].endtime
                mean_granule_time = starttime.to_pydatetime() + (endtime.to_pydatetime() -
                                                                 starttime.to_pydatetime()) / 2.
                prop = {'power': detections.iloc[idx].power,
                        'tb': detections.iloc[idx].tb,
                        'observation_time': json_serial(mean_granule_time)
                        }
                if self.platform_name:
                    prop['platform_name'] = self.platform_name
                else:
                    logger.debug("No platform name specified for output")

                feat = Feature(
                    geometry=Point(map(float, [detections.iloc[idx].longitude, detections.iloc[idx].latitude])),
                    properties=prop)
                features.append(feat)

            feature_collection = FeatureCollection(features)
            path = os.path.dirname(self.output_filename)
            if not os.path.exists(path):
                logger.info("Create directory: %s", path)
                os.makedirs(path)

            with open(self.output_filename, 'w') as f:
                dump(feature_collection, f)

            return self.output_filename
        else:
            logger.debug("No detections to save!")
            return None


class ActiveFiresPostprocessing(Thread):
    """The active fires post processor."""

    def __init__(self, configfile, shp_boarders, shp_mask):
        """Initialize the active fires post processor class."""
        super().__init__()
        self.shp_boarders = shp_boarders
        self.shp_filtermask = shp_mask
        self.configfile = configfile
        self.options = None

        self.get_config()

        self.input_topic = self.options['subscribe_topics'][0]
        self.output_topic = self.options['publish_topic']
        self.infile_pattern = self.options.get('af_pattern_ibands')
        self.outfile_pattern = self.options.get('geojson_file_pattern')
        self.host = socket.gethostname()
        self.output_dir = self.options.get('output_dir', '/tmp')

        logger.debug("Input topic: %s", self.input_topic)

        self.listener = ListenerContainer(topics=[self.input_topic])
        self.publisher = NoisyPublisher("active_fires_postprocessing")
        self.publisher.start()
        self.loop = True
        signal.signal(signal.SIGTERM, self.signal_shutdown)

    def get_config(self):
        """Read and extract config information."""
        with open(self.configfile, 'r') as fp_:
            config = yaml.load(fp_, Loader=UnsafeLoader)

            self.options = {}
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

    def run(self):
        """Run the AF post processing."""
        while self.loop:
            try:
                msg = self.listener.output_queue.get(timeout=1)
                logger.debug("Message: %s", str(msg.data))
            except Empty:
                continue
            else:
                if msg.type not in ['file', 'collection', 'dataset']:
                    logger.debug("Message type not supported: %s", str(msg.type))
                    continue
                output_msg = self.fires_filtering(msg)
                if output_msg:
                    logger.debug("Sending message: %s", str(output_msg))
                    self.publisher.send(str(output_msg))

    def fires_filtering(self, msg):
        """Read Active Fire data and perform spatial filtering removing false detections."""
        logger.debug("Read VIIRS AF detections and perform quality control and spatial filtering")

        outmsg = self.generate_no_fires_message(msg, 'No fire detections for this granule')
        file_type = msg.data.get('type')
        if not file_type in ['txt', 'TXT']:
            logger.info('File type not txt: %s', str(file_type))
            return outmsg

        uri = msg.data.get('uri')
        logger.info('File uri: %s', str(uri))
        url = urlparse(uri)
        filename = url.path
        fmda = self.get_metadata_from_filename(filename)
        platform_name = msg.data.get('platform_name')
        afdata = self.read_af_data(filename, fmda, localtime=True)
        if len(afdata) > 0:
            pout = Parser(self.outfile_pattern)
            out_filepath = os.path.join(self.output_dir, pout.compose(fmda))
            logger.debug("Output file path = %s", out_filepath)
            fshf = FiresShapefileFilter(afdata, out_filepath, self.shp_boarders, self.shp_filtermask,
                                        platform_name=platform_name)
            afdata_ff = fshf.fires_filtering(afdata)
            filepath = fshf.store_geojson(afdata_ff)
            if filepath:
                outmsg = self.generate_output_message(filepath, msg)
                logger.info("geojson file created! Number of fires after filtering = %d", len(afdata_ff))
            else:
                logger.info("No geojson file created, number of fires after filtering = %d", len(afdata_ff))
                outmsg = self.generate_no_fires_message(msg, 'No true fire detections inside Sweden')

        return outmsg

    def generate_output_message(self, filepath, input_msg):
        """Create the output message to publish."""

        to_send = input_msg.data.copy()
        to_send.pop('dataset', None)
        to_send.pop('collection', None)
        to_send['uri'] = ('ssh://%s/%s' % (self.host, filepath))
        to_send['uid'] = os.path.basename(filepath)
        to_send['type'] = 'GEOJSON-filtered'
        to_send['format'] = 'geojson'
        to_send['product'] = 'afimg'
        pubmsg = Message(self.output_topic, 'file', to_send)
        return pubmsg

    def generate_no_fires_message(self, input_msg, msg_string):
        """Create the output message to publish."""

        to_send = input_msg.data.copy()
        to_send.pop('dataset', None)
        to_send.pop('collection', None)
        to_send.pop('uri', None)
        to_send.pop('uid', None)
        to_send.pop('format', None)
        to_send.pop('type', None)
        to_send['info'] = msg_string
        pubmsg = Message(self.output_topic, 'info', to_send)
        return pubmsg

    def get_metadata_from_filename(self, filepath):
        """From the filename retrieve the metadata such as satellite and sensing time."""
        if not self.infile_pattern:
            raise IOError("No file pattern provided: In order to read all " +
                          "meta data in the file (and from the filename) the file pattern is required!")

        p__ = Parser(self.infile_pattern)
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

    def read_af_data(self, filepath, file_mda, localtime=True):
        """Read the Active Fire results from file."""

        df = pd.read_csv(filepath, index_col=None, header=None, comment='#', names=COL_NAMES)
        # Add start and end times:
        if localtime:
            logger.info("Convert to local time zone!")
            starttime = datetime_from_utc_to_local(file_mda['start_time'])
            endtime = datetime_from_utc_to_local(file_mda['end_time'])
        else:
            starttime = file_mda['start_time']
            endtime = file_mda['end_time']

        logger.info('Start and end times: %s %s', str(starttime), str(endtime))

        # Apply timezone offset:
        df['starttime'] = np.repeat(starttime + starttime.utcoffset(), len(df)).astype(np.datetime64)
        df['endtime'] = np.repeat(endtime + endtime.utcoffset(), len(df)).astype(np.datetime64)

        return df

    def close(self):
        """Shutdown the Active Fires postprocessing."""
        logger.info('Terminating Active Fires post processing.')
        self.loop = False
        try:
            self.listener.stop()
        except Exception:
            logger.exception("Couldn't stop listener.")
        if self.publisher:
            try:
                self.publisher.stop()
            except Exception:
                logger.exception("Couldn't stop publisher.")
