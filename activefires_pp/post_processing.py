#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021-2026 Adam Dybbroe

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

"""Post processing on the Active Fire detections."""

import socket
from contextlib import closing
from trollsift import Parser, globify
import time
import datetime as dt
import os
from six.moves.urllib.parse import urlparse

import logging
import signal
from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from posttroll.subscriber import create_subscriber_from_dict_config

from activefires_pp.utils import datetime_utc2local
from activefires_pp.utils import UnitConverter
from activefires_pp.config import read_config
from activefires_pp.processing import ActiveFiresProcessor
from activefires_pp.processing import ActiveFiresShapefileFiltering

from activefires_pp.geojson_utils import PROPERTY_MAP
from activefires_pp.geojson_utils import OPTIONAL_PROPERTY_MAP as OPT_PROPERTY_MAP
from activefires_pp.geojson_utils import store_geojson
from activefires_pp.geojson_utils import geojson_feature_collection_from_detections
from activefires_pp.geojson_utils import map_coordinates_in_feature_collection

logger = logging.getLogger(__name__)


NO_FIRES_TEXT = 'No fire detections for this granule'


class ActiveFiresPostprocessing():
    """The active fires post processor."""

    def __init__(self, configfile, shp_borders, shp_mask, regional_filtermask=None):
        """Initialize the active fires post processor class."""
        self.shp_borders = shp_borders
        self.shp_filtermask = shp_mask

        self.regional_filtermask = regional_filtermask
        self.configfile = configfile

        self.config = read_config(self.configfile)

        self.subscriber_config = self.config['subscriber_config']
        self.publisher_config = self.config['publisher_config']
        self.input_topics = self.subscriber_config['topics']
        self.output_topic = self.publisher_config['topic']

        self.host = socket.gethostname()
        self.timezone = self.config.get('timezone', 'GMT')

        self.infile_pattern = self.config.get('af_pattern_ibands')

        self.processor = ActiveFiresProcessor.from_config(
            self.config, self.shp_borders, self.shp_filtermask
        )

        self.outfile_patterns_national = self.config.get('output').get('national')
        self.outfile_patterns_regional = self.config.get('output').get('regional')

        self.output_dir = self.config.get('output_dir', '/tmp')

        self.filepath_detection_id_cache = self.config.get('filepath_detection_id_cache')
        self.regional_shapefiles_globstr = globify(self.config['regional_shapefiles_format'])

        self._fire_detection_id = None
        self._initialize_fire_detection_id()

        self._init_unit_converter()
        self._check_borders_shapes_exists()

        self.subscriber = None
        self.publisher = None
        self.loop = False
        self._start_communication()

    def _init_unit_converter(self):
        """Initialize the unit converter.."""
        now = datetime_utc2local(dt.datetime.now(), self.timezone)
        logger.debug("Output times for timezone: {zone} Now = {time}".format(zone=str(self.timezone), time=now))

        tic = time.time()
        units = {'temperature': 'degC'}
        self.unit_converter = UnitConverter(units)
        logger.debug("Unit conversion initialization with Pint took %f seconds.", time.time() - tic)

    def _start_communication(self):
        """Set up the Posttroll communication and start the publisher."""
        for topic in self.input_topics:
            logger.debug(f'Starting up... Input topic: {str(topic)}')
        self.loop = True
        signal.signal(signal.SIGTERM, self.signal_shutdown)

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

    def get_output_filepath_from_projname(self, projname, metadata):
        """From the projection-name (given in the config file) retrieve the output file path."""
        try:
            pout = Parser(self.outfile_patterns_national[projname]['geojson_file_pattern'])
        except KeyError:
            raise KeyError('Projection name %s not supported in configuration!' % projname)

        return os.path.join(self.output_dir, pout.compose(metadata))

    def _national_save_and_publish(self, feature_collection, ndata, metadata, msg, projname='default'):
        """Take the fearure collection and store the results in a Geojson file and publish."""
        if feature_collection is None:
            logger.info("No geojson file created, number of fires after filtering = %d", ndata)
            output_messages = self._generate_no_fires_messages(msg,
                                                               'No true fire detections inside National borders')  # noqa
            return

        out_filepath = self.get_output_filepath_from_projname(projname, metadata)
        logger.debug("Output file path = %s", out_filepath)

        store_geojson(out_filepath, feature_collection)
        output_messages = self.get_output_messages(out_filepath, msg, ndata, proj_name=projname)

        for output_msg in output_messages:
            if output_msg:
                logger.debug("Sending message: %s", str(output_msg))
                self.publisher.send(str(output_msg))

    def postprocess_from_message(self, msg, filename):
        """Do the fires post processing on a message."""
        platform_name = msg.data.get('platform_name')

        processing_result = self.processor.process(filename, platform_name=platform_name)
        afdata = processing_result.detections
        if processing_result.input_detection_count == 0:
            output_messages = self._generate_no_fires_messages(msg, NO_FIRES_TEXT)
            for output_msg in output_messages:
                logger.debug("Sending message: %s", str(output_msg))
                self.publisher.send(str(output_msg))
            return

        logger.debug("After processing: Number of fire detections = %d", len(afdata))
        if len(afdata) == 0:
            logger.debug("No fires - so no regional filtering to be done!")
            return

        afdata = self.add_unique_day_id(afdata)
        self.save_id_to_file()
        afdata = self.add_tb_celsius(afdata)

        # 1) Create geojson feature collection
        # 2) Dump geojson data to disk

        feature_collection = geojson_feature_collection_from_detections(afdata,
                                                                        PROPERTY_MAP,
                                                                        optional_property_map=OPT_PROPERTY_MAP,
                                                                        platform_name=processing_result.platform_name)

        for proj_name in self.outfile_patterns_national:
            if proj_name == "default":
                self._national_save_and_publish(feature_collection, len(afdata), processing_result.metadata, msg)
            else:
                epsg_str = self.outfile_patterns_national[proj_name].get('projection')
                other_fc = map_coordinates_in_feature_collection(feature_collection, epsg_str)
                self._national_save_and_publish(other_fc, len(afdata), processing_result.metadata, msg, proj_name)

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

    def run_and_publish(self):
        """Run the AF post processing and start subcriber and publisher."""
        with closing(create_publisher_from_dict_config(self.publisher_config["publisher_settings"])) as self.publisher:
            self.publisher.start()
            with closing(create_subscriber_from_dict_config(self.subscriber_config)) as self.subscriber:
                for msg in self.subscriber.recv():
                    if not self.loop:
                        break

                    filename = self.check_incoming_message_and_get_filename(msg)
                    if not filename:
                        continue

                    self.postprocess_from_message(msg, filename)

    def regional_fires_filtering_and_publishing(self, msg, regional_fmask, afsff_obj):
        """From the regional-fires-filter-mask and the fire detection data send regional messages."""
        logger.debug("Perform regional masking on VIIRS AF detections and publish accordingly.")

        afdata = afsff_obj.get_af_data()
        fmda = afsff_obj.metadata

        fmda['platform'] = afsff_obj.platform_name

        pout = Parser(self.outfile_patterns_regional['default']['geojson_file_pattern'])

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

            try:
                feature_collection = geojson_feature_collection_from_detections(data_in_region,
                                                                                PROPERTY_MAP,
                                                                                optional_property_map=OPT_PROPERTY_MAP,
                                                                                platform_name=fmda['platform'])
            except ValueError:
                logger.warning("Something wrong happended storing regional " +
                               "data to Geojson - area: {name}".format(name=str(region_name)))
                continue

            store_geojson(out_filepath, feature_collection)

            outmsg = self._generate_output_message(out_filepath, msg, 'default',
                                                   regional_fmask[region_name])
            output_messages.append(outmsg)
            logger.info("Geojson file created! Number of fires in region = %d", len(data_in_region))

        logger.debug("Regional masking done. Number of regions with fire " +
                     "detections on this granule: %s" % str(regions_with_detections))
        return output_messages

    def fires_filtering(self, msg, af_shapeff):
        """Apply the shared active-fire filtering pipeline.

        Kept for backwards compatibility with callers using the former
        post-processing interface.
        """
        return self.processor.filter_detections(af_shapeff)

    def get_output_messages(self, filepath, msg, number_of_data, proj_name='default'):
        """Generate the adequate output message(s) depending on if an output file was created or not."""
        logger.info("Geojson file created! Number of fires = %d", number_of_data)
        return [self._generate_output_message(filepath, msg, proj_name)]

    def _generate_output_message(self, filepath, input_msg, proj_name, region=None):
        """Create the output message to publish."""
        output_topic = generate_posttroll_topic(self.output_topic, region)
        to_send = prepare_posttroll_message(input_msg, region)
        to_send['uri'] = str(filepath)
        to_send['uid'] = os.path.basename(filepath)
        to_send['type'] = 'GEOJSON-filtered'
        to_send['format'] = 'geojson'
        if proj_name == 'default':
            to_send['product'] = 'afimg'
        else:
            to_send['product'] = 'afimg_{proj_name}'.format(proj_name=proj_name)

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
            self._fire_detection_id = {'date': dt.datetime.now(dt.timezone.utc), 'counter': 0}

    def update_fire_detection_id(self):
        """Update the fire detection ID registry."""
        now = dt.datetime.now(dt.timezone.utc)
        if self._fire_detection_id['date'].date() < now.date():
            self._fire_detection_id = {'date': dt.datetime.now(dt.timezone.utc), 'counter': 0}

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
        return {'date': dt.datetime.strptime(datestr, '%Y%m%d'),
                'counter': int(counter)}

    def _create_id_string(self):
        """From the internal fire detection id create the id string to be exposed to the user."""
        return (self._fire_detection_id['date'].strftime('%Y%m%d') +
                '-' + str(self._fire_detection_id['counter']))

    def add_unique_day_id(self, data_frame):
        """Add a unique detection id - date + a running number for the day."""
        # Add id's to the detections:
        id_list = []
        for _i in range(len(data_frame)):
            self.update_fire_detection_id()
            id_ = self._create_id_string()
            id_list.append(id_)

        col = len(data_frame.columns)
        data_frame.insert(col, 'detection_id', id_list)
        return data_frame

    def add_tb_celsius(self, data_frame):
        """Add a column with TB in Celsius to the fire detection data frame."""
        def kelvin2celsius(x):
            tb_c = self.unit_converter.convert('temperature', x)
            return tb_c.magnitude

        col = data_frame.columns.get_loc('tb')
        data_frame.insert(2, "tb_celsius", data_frame.iloc[:, col].apply(kelvin2celsius))
        return data_frame

    def close(self):
        """Shutdown the Active Fires postprocessing."""
        logger.info('Terminating Active Fires post processing.')
        self.subscriber.close()
        self.loop = False
        logger.info('Dumping the latest detection id to disk: %s', str(self.filepath_detection_id_cache))
        self.save_id_to_file()


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
