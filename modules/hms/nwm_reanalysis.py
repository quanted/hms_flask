import os
import json

import pandas as pd
import requests
import fsspec
import geopandas as gp
import numpy as np
import xarray as xr
import dask
import boto3
import logging
from dask.distributed import Client, LocalCluster
import datetime
import warnings
try:
    from .timeseries import TimeSeriesOutput
except ImportError:
    from timeseries import TimeSeriesOutput

warnings.filterwarnings("ignore", category=ResourceWarning)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
boto3.set_stream_logger('botocore', logging.INFO)
boto3.set_stream_logger('s3fs', logging.INFO)

epa_waters_url = "https://watersgeo.epa.gov/arcgis/rest/services/NHDPlus_NP21/Catchments_NP21_Simplified/MapServer/0/query?"
nwm_url = "s3://noaa-nwm-retro-v2-zarr-pds"
all_variables = ["elevation", "order", "qBtmVertRunoff", "qBucket", "qSfcLatRunoff", "q_lateral", "streamflow", "velocity"]
variables = ["streamflow", "velocity"]
missing_value = -9999


class NWM:

    def __init__(self, start_date: str, end_date: str, comids: list, timestep: str = None):

        self.start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d") + datetime.timedelta(days=1)
        self.comids = [int(c) for c in comids]
        self.timestep = timestep
        self.catchment = None
        self.geometry = None
        self.output = TimeSeriesOutput(source="nwm", dataset="streamflow")
        self.data = None

    def get_geometry(self):
        # NOT REQUIRED: direct query using COMID, has not been updated to handle more than 1 comid
        request_url = epa_waters_url + f"where=FEATUREID={self.comids[0]}&text=&objectIds=&time=&geometry=&" \
                                       f"geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&" \
                                       f"relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&" \
                                       f"maxAllowableOffset=&geometryPrecision=&" \
                                       f"outSR=%7B%22wkt%22+%3A+%22GEOGCS%5B%5C%22GCS_WGS_1984%5C%22%2CDATUM%5B%5C" \
                                       f"%22D_WGS_1984%5C%22%2C+SPHEROID%5B%5C%22WGS_1984%5C%22%2C6378137%" \
                                       f"2C298.257223563%5D%5D%2CPRIMEM%5B%5C%22Greenwich%5C%22%2C0%5D%2C+" \
                                       f"UNIT%5B%5C%22Degree%5C%22%2C0.017453292519943295%5D%5D%22%7D&" \
                                       f"returnIdsOnly=false&returnCountOnly=false&orderByFields=&" \
                                       f"groupByFieldsForStatistics=&outStatistics=&returnZ=false&" \
                                       f"returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&" \
                                       f"resultRecordCount=&queryByDistance=&returnExtentsOnly=false&" \
                                       f"datumTransformation=&parameterValues=&rangeValues=&f=geojson"
        self.catchment = json.loads(requests.get(request_url).text)
        self.geometry = gp.GeoDataFrame.from_features(self.catchment)
        for k, v in self.catchment["features"][0]["properties"].items():
            self.output.add_metadata(k, v)

    def request_timeseries(self):
        warnings.filterwarnings("ignore", category=ResourceWarning)
        scheduler = os.getenv('DASK_SCHEDULER', "127.0.0.1:8786")
        # scheduler = LocalCluster()
        client = Client(scheduler)
        logging.info(f"Request zarr data from: {nwm_url}")
        ds = xr.open_zarr(fsspec.get_mapper(nwm_url, anon=True), consolidated=True)
        comid_check = []
        missing_comids = []
        for c in self.comids:
            try:
                test = ds["streamflow"].sel(feature_id=c).sel(time=slice("2010-01-01", "2010-01-01"))
                comid_check.append(c)
            except KeyError:
                missing_comids.append(c)
        if len(missing_comids) > 0:
            self.output.add_metadata("missing_comids", ", ".join(missing_comids))

        with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            ds_streamflow = ds[variables].sel(feature_id=self.comids).sel(time=slice(
                f"{self.start_date.year}-{self.start_date.month}-{self.start_date.day}",
                f"{self.end_date.year}-{self.end_date.month}-{self.end_date.day}"
            )).load()

        self.data = ds_streamflow
        self.output.add_metadata("retrieval_timestamp", datetime.datetime.now().isoformat())
        self.output.add_metadata("source_url", nwm_url)
        self.output.add_metadata("variables", ", ".join(variables))
        # scheduler.close()
        # client.close()

    def set_output(self):
        if self.data is None:
            return
        for k, v in self.data.attrs.items():
            self.output.add_metadata(k, v)
        j = 1
        for c in self.comids:
            for v in variables:
                self.output.metadata[f"column_{j}_units"] = str(self.data.data_vars[v].attrs["units"])
                j += 1
        timeseries = self.data.to_dataframe()
        for v in variables:
            nan_count = timeseries[v].isna().sum()
            if nan_count > 0:
                self.output.add_metadata(f"{v}_missing_value_count", str(nan_count))
                self.output.add_metadata(f"{v}_missing_value_flag", str(missing_value))
                timeseries[v] = timeseries[v].replace(np.nan, missing_value)
        i = 1
        first = True
        for idx, catchment in timeseries.groupby("feature_id"):
            i_meta = True
            for date, row in catchment.iterrows():
                d = date[1].strftime('%Y-%m-%d %H')
                if first:
                    self.output.data[d] = [r for r in row[variables]]
                else:
                    for r in row[variables]:
                        self.output.data[d].append(r)
                if i_meta:
                    for v in variables:
                        self.output.metadata[f"column_{i}"] = f"{v}-{idx}"
                        i += 1
                    i_meta = False
            first = False
