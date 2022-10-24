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

"""Spatial and temporal filtering of fire alarms.

The aim of this filtering is to avoid multiple alarms/alerts being forwarded to
the end user(s) of the 'same' fire. A vegetation fire may give rise to more
than one VIIRS pixel detection in space, and if it does not move new detections
at the same location may be triggered at the following overpasses. This
filtering will also avoid repetative alerts of a sustainable fire. We will use
thresholds in space and time to define when a fire is a new fire and when a
pixel-detection is not associated with the same fire. Currently the thresholds
are:

 * Time threshold: 6 hours
 * Spatial threshold: 800 meters
 * Threshold defining the longest possible fire before it will be split into smaller fires: 1.2 km

"""

import os
import logging
import signal
from queue import Empty
from threading import Thread
from requests.exceptions import HTTPError, ConnectionError
from posttroll.listener import ListenerContainer
from posttroll.message import Message
from posttroll.publisher import NoisyPublisher

import pytz
from datetime import datetime, timedelta

from geopy import distance
from geojson import FeatureCollection, dump
from itertools import combinations
from pathlib import Path

from activefires_pp.utils import get_filename_from_posttroll_message
from activefires_pp.config import read_config
from activefires_pp.config import get_xauthentication_token

from activefires_pp.geojson_utils import read_geojson_data
from activefires_pp.geojson_utils import get_geojson_files_in_observation_time_order
from activefires_pp.geojson_utils import store_geojson_alarm
from activefires_pp.api_posting import post_alarm

from trollsift import Parser

LOG = logging.getLogger(__name__)

DIR_SPATIAL_FILTER = '/tmp'


class AlarmFilterRunner(Thread):
    """The Alarm-Filter runner class."""

    def __init__(self, configfile):
        """Initialize the AlarmFilterRunner class."""
        super().__init__()
        self.configfile = configfile
        self.options = {}
        self.time_and_space_thresholds = {}

        self._read_and_check_configuration()

        self.input_topic = self.options['subscribe_topics'][0]
        LOG.debug("Input topic: %s", self.input_topic)
        self.output_topic = self.options['publish_topic']

        self.sos_alarms_file_pattern = self.options['geojson_file_pattern_alarms']
        self.restapi_url = self.options['restapi_url']
        _xauth_filepath = get_xauthentication_filepath_from_environment()
        self._xauth_token = get_xauthentication_token(_xauth_filepath)

        self.fire_alarms_dir = Path(self.options['fire_alarms_dir'])

        self.listener = None
        self.publisher = None
        self.loop = False
        self._setup_and_start_communication()

    def _setup_and_start_communication(self):
        """Set up the Posttroll communication and start the publisher."""
        LOG.debug("Input topic: %s", self.input_topic)
        self.listener = ListenerContainer(topics=[self.input_topic])
        self.publisher = NoisyPublisher("spatiotemporal_alarm_filtering")
        self.publisher.start()
        self.loop = True
        signal.signal(signal.SIGTERM, self.signal_shutdown)

    def _read_and_check_configuration(self):
        """Read and check the configuration parameters."""
        config = read_config(self.configfile)
        self._check_and_set_thresholds_from_config(config)
        self._set_options_from_config(config)

    def _check_and_set_thresholds_from_config(self, config):
        """Check that adequate thresholds are provided in config file."""
        if 'time_and_space_thresholds' not in config:
            raise OSError('Missing time and space thresholds from configuration!')

        self._log_message_per_threshold = {
            'hour_threshold': ("Threshold defining when a new detection should trigger an " +
                               "alarm on the same spot (in hours): ",
                               "Threshold in time is missing!"),
            'long_fires_threshold_km': ("Threshold defining the maximum extention of a fire before " +
                                        "dividing it in smaller pieces (in km): %3.1f",
                                        "Threshold for splitting long fires is missing!"),
            "spatial_threshold_km": ("Threshold defining the maximum distance between two detections " +
                                     "beyond which they must trigger more than one alarm (km): %3.1f",
                                     "A spatial threshold is missing!")
        }
        for thr_type in self._log_message_per_threshold:
            self._check_and_set_threshold(thr_type, config)

    def _check_and_set_threshold(self, thr_type, config):
        """Check if threshold exists in config and set it accordingly or raise an error."""
        if thr_type in config['time_and_space_thresholds']:
            self.time_and_space_thresholds[thr_type] = config['time_and_space_thresholds'][thr_type]
            LOG.info(self._log_message_per_threshold[thr_type][0], self.time_and_space_thresholds[thr_type])
        else:
            raise OSError(self._log_message_per_threshold[thr_type][1])

    def _set_options_from_config(self, config):
        """From the configuration on disk set the option dictionary with all metadata for real-time processing."""
        for item in config:
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
        """Shutdown the Notifier process."""
        self.close()

    def run(self):
        """Run the spatiotemporal alarm filtering."""
        while self.loop:
            try:
                msg = self.listener.output_queue.get(timeout=1)
                LOG.debug("Message: %s", str(msg.data))
            except Empty:
                continue
            else:
                if msg.type in ['info', ]:
                    # No fires detected - no notification to send:
                    LOG.info("Message type info: No fires detected - no alarm to generate.")
                    continue
                elif msg.type not in ['file', 'collection', 'dataset']:
                    LOG.debug("Message type not supported: %s", str(msg.type))
                    continue

                generated_alarms = self.spatio_temporal_alarm_filtering(msg)
                if generated_alarms:
                    LOG.debug("Number of generated alarms: %d", len(generated_alarms))
                else:
                    LOG.debug("No alarms generated!")

    def spatio_temporal_alarm_filtering(self, msg):
        """Spatial and temporal filtering of the fire detections and create and post the alarm.

        Perform the spatial and temporal filtering of the current detections
        against a catalogue of past detections and generate an alarm point by
        creating a geoJSON file and posting to a ReST-API.

        """
        LOG.debug("Start the spatio-temporal alarm filtering.")

        filename = get_filename_from_posttroll_message(msg)
        ffdata = read_geojson_data(filename)
        if not ffdata:
            return None

        geojson_alarms = create_alarms_from_fire_detections(ffdata,
                                                            self.fire_alarms_dir, self.sos_alarms_file_pattern,
                                                            self.time_and_space_thresholds)

        if len(geojson_alarms) == 0:
            LOG.info("No alarms to be triggered!")
            return None

        self.send_alarms(geojson_alarms, msg)
        return geojson_alarms

    def send_alarms(self, geojson_alarms, msg):
        """Send the alarms: Create geojson file with alarm data, post it and publish message."""
        p__ = Parser(self.sos_alarms_file_pattern)
        for idx, alarm in enumerate(geojson_alarms):
            # Write alarm to a geojson file in the fire_alarms_dir destination:
            # 1) Create the filename
            # 2) Wite to a file
            output_filename = store_geojson_alarm(self.fire_alarms_dir, p__, idx, alarm)
            try:
                post_alarm(alarm['features'], self.restapi_url, self._xauth_token)
                LOG.info('Alarm sent - status OK')
            except (HTTPError, ConnectionError):
                LOG.exception('Failed sending alarm!')
                LOG.error('Data: %s', str(alarm['features']))

            output_message = _create_output_message(msg, self.output_topic, alarm, output_filename)
            LOG.debug("Sending message: %s", str(output_message))
            self.publisher.send(str(output_message))

    def close(self):
        """Shutdown the AlarmFilterRunner process."""
        LOG.info('Terminating the Alarm Filter Runner process.')
        self.loop = False
        try:
            self.listener.stop()
        except Exception:
            LOG.exception("Couldn't stop listener.")
        if self.publisher:
            try:
                self.publisher.stop()
            except Exception:
                LOG.exception("Couldn't stop publisher.")


def get_xauthentication_filepath_from_environment():
    """Get the filename with the X-Authentication-token from environment."""
    xauth_filepath = os.environ.get('FIREALARMS_XAUTH_FILEPATH')
    if xauth_filepath is None:
        raise OSError("Environment variable FIREALARMS_XAUTH_FILEPATH not set!")

    return xauth_filepath


def dump_collection(idx, features):
    """Dump the list of features as a Geojson Feature Collection."""
    tmpdir = Path(DIR_SPATIAL_FILTER)
    fname = 'sos_alarm_{index}.geojson'.format(index=idx)
    output_filename = tmpdir / fname
    feature_collection = FeatureCollection(features)
    with open(output_filename, 'w') as fpt:
        dump(feature_collection, fpt)


def create_alarms_from_fire_detections(fire_data, past_detections_dir, sos_alarms_file_pattern,
                                       time_space_thresholds):
    """Create alarm(s) from a set of detections."""
    long_fires_threshold = time_space_thresholds.get('long_fires_threshold_km', 1.2)
    gathered_fires = join_fire_detections(fire_data)

    # Now go through the gathered fires and split long/large clusters of
    # detections in smaller parts, and create potential alarms:

    alarms_list = []
    for key in gathered_fires:
        LOG.debug("Key: %s" % key)
        fire_alarms = get_single_point_fires_as_collections(gathered_fires[key], long_fires_threshold)

        alarm_should_be_triggered = False
        for fire_alarm in fire_alarms:
            # Check against the most recent alarms:
            alarm_should_be_triggered = check_if_fire_should_trigger_alarm(fire_alarm, past_detections_dir,
                                                                           sos_alarms_file_pattern,
                                                                           time_space_thresholds)
            if alarm_should_be_triggered:
                lonlat = fire_alarm['features']['geometry']['coordinates']
                power = fire_alarm['features']['properties']['power']
                LOG.info("Alarm should be triggered: Power=%f Location=(%f,%f)", power, lonlat[0], lonlat[1])
                alarms_list.append(fire_alarm)

    return alarms_list


def find_neighbours(feature, other_features, thr_dist=0.8):
    """For a given feature (fire detection) find its close neighbours."""
    lon0, lat0 = feature['geometry']['coordinates']
    idx = []
    for key, feat in other_features.items():
        geom = feat['geometry']
        lon, lat = geom['coordinates']
        km_dist = distance.distance((lat0, lon0), (lat, lon)).kilometers
        if km_dist < thr_dist:
            idx.append(key)

    return idx


def gather_neighbours_to_new_collection(start_id, features, feature_collections, thr_dist=None):
    """Go through all features and gather into groups of neighbouring detections."""
    first_point = features[start_id]
    features.pop(start_id)
    neighbour_ids = find_neighbours(first_point, features)

    lon1, lat1 = first_point['geometry']['coordinates']
    collection_id = "%d_%d" % (lon1*100000, lat1*100000)
    feature_collections[collection_id] = [first_point]
    for i in neighbour_ids:
        if thr_dist:
            lon, lat = features[i]['geometry']['coordinates']
            km_dist = distance.distance((lat1, lon1), (lat, lon)).kilometers
            if km_dist > thr_dist:
                continue

        feature_collections[collection_id].append(features[i])
        features.pop(i)

    return features


def join_fire_detections(gdata):
    """Go through detections and gather detections that are close.

    1) Take the first feature (fire detection)
    2) Identify its neighbours (close fires, with a distance less than some
       threshold, say 800 meters)
    3) Gather those features into the first collection of neighbours
    4) Go through the rest and see if any are close to the collection,
       if there is one, add it to the collection, and repeat.
    5) If there are any features left in the list go to step 1 above, and
       repeat.

    """
    num_features = len(gdata['features'])
    features = dict(zip(range(num_features), gdata['features']))

    feature_collections = {}
    start_index = 0
    while num_features > 0:
        features = gather_neighbours_to_new_collection(start_index, features, feature_collections)
        if len(features.keys()) == 0:
            break
        start_index = min(features.keys())
        num_features = len(features.keys())

    return feature_collections


def split_large_fire_clusters(features, large_fires_threshold):
    """Take a list of fire detection features and split in smaller clusters/chains."""
    num_features = len(features)
    LOG.debug("Split large fire clusters - Number of features: %d" % num_features)
    features = dict(zip(range(num_features), features))

    # Find the two features that are farthest away from each other:
    # First find all 2-combinations of the set:
    two_combs = list(combinations(range(num_features), 2))
    max_distance = 0.0
    max_2comb = (0, 0)
    for tup2 in two_combs:
        lon1, lat1 = features[tup2[0]]['geometry']['coordinates']
        lon2, lat2 = features[tup2[1]]['geometry']['coordinates']

        km_dist = distance.distance((lat1, lon1), (lat2, lon2)).kilometers
        if km_dist > max_distance:
            max_distance = km_dist
            max_2comb = tup2

    if max_distance < large_fires_threshold:
        LOG.debug("Only one cluster - (max_distance, threshold) = (%f, %f)" % (max_distance, large_fires_threshold))
        return {'only-one-cluster': list(features.values())}

    # Now we have located the two detections in the collection that are
    # farthest apart! Then from one of those points gather neighbours to a new
    # collection within threshold distance:
    feature_collections = {}
    start_index = max_2comb[0]
    while num_features > 0:
        features = gather_neighbours_to_new_collection(start_index, features, feature_collections,
                                                       large_fires_threshold)
        if not features:
            break
        start_index = min(features.keys())
        num_features = len(features.keys())

    return feature_collections


def create_one_detection_from_collection(features):
    """From a collection of close fire detections create one detection using the maximum FRP as locator."""
    max_frp = 0.0
    for feat in features:
        if feat["properties"]["power"] > max_frp:
            max_frp = feat["properties"]["power"]
            feature2save = feat

    lonlat = feature2save["geometry"]["coordinates"]
    LOG.info("Fire detection with maxmimum FRP in the cluster found: power = %f location = (%f, %f)",
             feature2save["properties"]["power"], lonlat[0], lonlat[1])
    # Record if there were more than one detection in the collection:
    related_detection = False
    if len(features) > 1:
        related_detection = True

    LOG.info("Related detection: %s" % str(related_detection))
    feature2save["properties"]['related_detection'] = related_detection
    return feature2save


def create_single_point_alarms_from_collections(features):
    """Go through list of features and create a single fire-detection/alarm for each and return as collection."""
    fcollection = []
    for key in features:
        # Make one alarm from each feature-collection (one fire with neighbouring detections):
        single_detection = create_one_detection_from_collection(features[key])

        # Maybe we shouldn't actually store it as a collection now that it is only one detection!?
        fcollection.append(FeatureCollection(single_detection))

    return fcollection


def get_single_point_fires_as_collections(fires, long_fires_threshold):
    """Split larger fires into smaller parts and make a single fire-detection out of each and return as collection."""
    features = split_large_fire_clusters(fires, long_fires_threshold)
    return create_single_point_alarms_from_collections(features)


def check_if_fire_should_trigger_alarm(gjson_data, past_alarms_dir, sos_alarms_file_pattern,
                                       time_space_thresholds):
    """Check if fire point should trigger an alarm.

    The fire point is a GeoJSON object. A search back in time X hours (e.g. X=16) is
    done, and a check for previous fire alarms on that location is done. Only
    if no previous fire alarm at the same position (determined by a distance
    threshold) is found, an alarm should be issued.
    """
    utc = pytz.timezone('utc')
    end_time = datetime.fromisoformat(gjson_data["properties"]["observation_time"])
    end_time = end_time.astimezone(utc).replace(tzinfo=None)

    hour_thr = time_space_thresholds.get('hour_threshold', 16)
    km_threshold = time_space_thresholds.get('spatial_threshold_km', 0.8)

    LOG.debug("Check if fire detection should trigger alarm. Thresholds = (%3.1f h, %3.1f km)",
              hour_thr, km_threshold)

    start_time = end_time - timedelta(hours=hour_thr)
    files_in_observation_time_order = get_geojson_files_in_observation_time_order(past_alarms_dir,
                                                                                  sos_alarms_file_pattern,
                                                                                  (start_time, end_time))

    # If directory is empty there is no history present and this alarm should be triggered:
    if len(files_in_observation_time_order) == 0:
        LOG.info("Directory empty - no history present - alarm should be triggered!")
        return True

    lonlat_current_position = gjson_data["geometry"]["coordinates"]
    shall_trigger_alarm = True
    for filename in reversed(files_in_observation_time_order):
        obstime, dist = distance_and_time_from_geojson_position(lonlat_current_position,
                                                                (past_alarms_dir / filename))
        if dist < km_threshold:
            LOG.debug("Recent alarm file: %s", str(filename))
            shall_trigger_alarm = False

            time_diff_in_minutes = (end_time - obstime).total_seconds()/60.
            LOG.info("There was a recent alarm on this location! No new alarm generated.")
            LOG.info("Distance = %f, Time distance = %f minutes" % (dist, time_diff_in_minutes))
            break

    return shall_trigger_alarm


def distance_and_time_from_geojson_position(position, filepath):
    """Read the geojson data and get the observation time and the distance to a position given as input."""
    lon0, lat0 = position
    gjdata = read_geojson_data(filepath)
    lon, lat = gjdata["geometry"]["coordinates"]
    # Get distance to this fire point:
    dist = distance.distance((lat0, lon0), (lat, lon)).kilometers

    obstime = datetime.fromisoformat(gjdata["properties"]["observation_time"])
    utc = pytz.timezone('utc')
    obstime = obstime.astimezone(utc).replace(tzinfo=None)

    return obstime, dist


def _create_output_message(msg, topic, geojson, filename):
    """Create the output message from the input message and the geojson payload."""
    to_send = msg.data.copy()
    to_send.pop('type', None)
    to_send['related_detection'] = geojson['features']['properties']['related_detection']
    to_send['power'] = geojson['features']['properties']['power']
    to_send['tb'] = geojson['features']['properties']['tb']
    to_send['platform_name'] = geojson['features']['properties']['platform_name']
    to_send['coordinates'] = geojson['features']['geometry']['coordinates']
    to_send['file'] = filename.name
    to_send['format'] = 'geojson'
    to_send['uri'] = str(filename)
    to_send.pop('uid', None)

    return Message(topic, 'file', to_send)
