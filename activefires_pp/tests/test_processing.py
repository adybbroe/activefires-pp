#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2026 Adam.Dybbroe

"""Tests for the reusable active-fire processing API."""

import pytest
import datetime as dt

from activefires_pp.processing import ActiveFiresProcessor, ProcessingOptions


EDRFILE_PATTERN = (
    'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_'
    'e{end_hour:%H%M%S%f}_b{orbit:s}_'
    'c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt'
)


def test_processing_options_from_config():
    """Operational configuration should map cleanly to processing options."""
    config = {
        'af_pattern_ibands': EDRFILE_PATTERN,
        'timezone': 'Europe/Stockholm',
    }

    options = ProcessingOptions.from_config(config, 'borders.shp', 'mask.shp')

    assert options.national_boundary == 'borders.shp'
    assert options.filter_mask == 'mask.shp'
    assert options.infile_pattern == EDRFILE_PATTERN
    assert options.timezone == 'Europe/Stockholm'
    assert options.remove_spurious is True


def test_active_fires_processor_filters_system_test_case(
        fake_active_fires_ascii_file_system_test_case,
        sample_filtermask,
        sample_sweden):
    """The shared processor should reproduce the accepted filtered detection."""
    options = ProcessingOptions(
        national_boundary=sample_sweden,
        filter_mask=sample_filtermask,
        infile_pattern=EDRFILE_PATTERN,
    )
    processor = ActiveFiresProcessor(options)

    result = processor.process(
        str(fake_active_fires_ascii_file_system_test_case),
        platform_name='NOAA-20',
    )

    assert result.platform_name == 'NOAA-20'
    assert result.start_time == dt.datetime(2021, 4, 14, 11, 26, 43, 900000)
    assert result.end_time == dt.datetime(2021, 4, 14, 11, 28, 8)
    assert result.input_detection_count > 0
    assert len(result.detections) == 1

    detection = result.detections.iloc[0]
    assert detection.longitude == pytest.approx(17.168163, abs=1e-6)
    assert detection.latitude == pytest.approx(59.524834, abs=1e-6)
    assert detection.power == pytest.approx(14.13167953, abs=1e-8)
    assert detection.tb == pytest.approx(336.57437134, abs=1e-8)
    assert int(detection.conf) == 8
