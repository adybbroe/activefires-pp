activefires-pp
==============

[![Build status](https://github.com/adybbroe/activefires-pp/workflows/CI/badge.svg?branch=main)](https://github.com/adybbroe/activefires-pp/workflows/CI/badge.svg?branch=main)
[![Coverage Status](https://coveralls.io/repos/github/adybbroe/activefires-pp/badge.svg)](https://coveralls.io/github/adybbroe/activefires-pp)

Post-processing (including regional filtering) of Satellite Active Fires and notify end-users
Supports reading and processing VIIRS Active Fires EDR. There is support for filtering out fire
detections using three different levels:

  * National filtering, where all detections outside boarders as given by a shapefile are left out

  * Filtering to exclude fire detections inside a set of polygons (meant to
    define populated areas and local industries known to be heat sources that
    can turn up as detections) from a shapefile

  * Regional filtering, where detections are localisized in regions and output
    messages are treated accordingly.
