# Overview

Small self contained system test for the activefires_pp tool.

# Content

├── activefires_pp_test_mask.dbf
├── activefires_pp_test_mask.prj
├── activefires_pp_test_mask.shp
├── activefires_pp_test_mask.shx
├── AFIMG_j01_d20210414_t1126439_e1128084_b17637_c20210414114130392094_cspp_dev.txt
├── expected_result.geojson
├── qgis_screen_capture.png
├── README.md
├── Sverige.cpg
├── Sverige.dbf
├── Sverige.prj
├── Sverige.sbn
├── Sverige.sbx
├── Sverige.shp
├── Sverige.shp.xml
├── Sverige.shx
├── systemtest_read_and_filter.py


# How to run it

python systemtest_read_and_filter.py -f ./activefires_pp_test_mask.shp -b ./Sverige.shp


# What it does
The script takes two shapefiles as input. One file identifying the boarders of Sweden (Sverige.shp) and the other one is a (dummy) mask used to mask out hotspots inside Sweden. The test reads a VIIRS EDR fire hotspot file in ascii format (AFIMG_j01_d20210414_t1126439_e1128084_b17637_c20210414114130392094_cspp_dev.txt). There are three hotspots inside Sweden. The mask is supposed filter out two of those.  

A screenshot of qgis with both shapefiles included and the three hotspots is included: qgis_screen_capture.png 

The script will produce a test_output.geojson file with one fire/hotspot. This file should be identical with expected_result.geojson


Adam, Ilona and Jekaterina, Iceland, 2026-04-15


