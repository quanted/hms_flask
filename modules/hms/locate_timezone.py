"""
HMS Locate Timezone from lat/lon coordinates
Using GeoPandas
"""

import os
import json
import geopandas as gp
import pandas as pd
import logging
from shapely.geometry import Point

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

tz_df = None
if os.path.exists(r"/src/hms_flask/modules/hms/tz_files/tzinfo_updated.csv"):
    tz_df = pd.read_csv(r"/src/hms_flask/modules/hms/tz_files/tzinfo_updated.csv", names=("TZID", "TZName", "TZOffset", "TZOffsetDST"))
else:
    tz_list = os.path.join(os.path.realpath(__file__), "..", "tz_files", "tzinfo_updated.csv")
    tz_df = pd.read_csv(tz_list, names=("TZID", "TZName", "TZOffset", "TZOffsetDST"))
if os.path.exists(r"/src/hms_flask/modules/hms/tz_files/tz_world.dbf"):
    tz_gdf = gp.read_file(r"/src/hms_flask/modules/hms/tz_files/tz_world.dbf")
else:
    tz_files = os.path.join(os.path.realpath(__file__), "..", "tz_files", "tz_world.dbf")
    tz_gdf = gp.read_file(tz_files)


def get_timezone(latitude, longitude):
    tz_point = Point(float(longitude), float(latitude))
    gdf = tz_gdf[tz_gdf.contains(tz_point)]
    if gdf.empty:
        return json.dumps({"error": "No timezone found for the given coordinates"})
    tz_name = gdf['TZID'].values[0]
    tz_offset = tz_df[tz_df["TZName"] == tz_name]["TZOffset"]
    tz_offset = tz_offset.values[0].split(":")[0]
    timezone_details = {"latitude": float(latitude), "longitude": float(longitude), "tzName": str(tz_name), "tzOffset": int(tz_offset)}
    logger.info(f"Timezone details: {timezone_details}")
    return timezone_details
