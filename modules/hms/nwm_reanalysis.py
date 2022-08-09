import os
import json
import copy
import pandas as pd
import requests
import fsspec
import geopandas as gp
import numpy as np
import xarray as xr
import dask
import boto3
import s3fs
import zarr
import logging
from dask.distributed import Client, LocalCluster
import datetime
import warnings
try:
    from .timeseries import TimeSeriesOutput
except ImportError:
    from timeseries import TimeSeriesOutput
import multiprocessing as mp

warnings.filterwarnings("ignore", category=ResourceWarning)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
boto3.set_stream_logger('botocore', logging.INFO)
boto3.set_stream_logger('s3fs', logging.INFO)

epa_waters_url = "https://watersgeo.epa.gov/arcgis/rest/services/NHDPlus_NP21/Catchments_NP21_Simplified/MapServer/0/query?"
nwm_url = "s3://noaa-nwm-retro-v2-zarr-pds"
nwm_21_url = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr"
all_variables = ["elevation", "order", "qBtmVertRunoff", "qBucket", "qSfcLatRunoff", "q_lateral", "streamflow", "velocity"]
variables = ["streamflow", "velocity"]

nwm_21_wb_url = "s3://noaa-nwm-retrospective-2-1-zarr-pds/lakeout.zarr"
wb_variables = ["inflow", "outflow", "water_sfc_elev"]

lakeparm_path = os.path.join("/src","hms-data", "LAKEPARM_CONUS.nwm.v2.1.nc")

missing_value = -9999


class NWM:

    def __init__(self, start_date: str, end_date: str, comids: list, timestep: str = None, waterbody: bool = False):

        self.start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d") + datetime.timedelta(days=1)
        self.comids = [int(c) for c in comids]
        self.timestep = timestep
        self.catchment = None
        self.geometry = None
        self.output = TimeSeriesOutput(source="nwm", dataset="streamflow")
        self.data = None
        self.waterbody = waterbody

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

    def request_timeseries(self, scheduler=None):
        warnings.filterwarnings("ignore", category=ResourceWarning)
        if not scheduler:
            scheduler = os.getenv('DASK_SCHEDULER', "127.0.0.1:8786")
        # scheduler = LocalCluster()
        client = Client(scheduler)
        request_url = nwm_21_url
        request_variables = copy.copy(variables)

        if self.waterbody:
            logging.info("Requesting NWM waterbody data")
            request_url = nwm_21_wb_url
            request_variables = copy.copy(wb_variables)
        logging.info(f"Using NWM 2.1 URL: {request_url}")
        logging.info(f"Request data for COMIDS: {self.comids}")
        logging.info("Executing optimized nwm data call")
        s3 = s3fs.S3FileSystem(anon=True)
        store = s3fs.S3Map(root=request_url, s3=s3, check=False)

        ds = xr.open_zarr(store=store, consolidated=True)

        with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            ds_streamflow = ds[request_variables].sel(feature_id=self.comids).sel(time=slice(
                f"{self.start_date.year}-{self.start_date.month}-{self.start_date.day}",
                f"{self.end_date.year}-{self.end_date.month}-{self.end_date.day}"
            )).load(optimize_graph=True, traverse=False)

        self.data = ds_streamflow
        self.output.add_metadata("retrieval_timestamp", datetime.datetime.now().isoformat())
        self.output.add_metadata("source_url", nwm_url)
        self.output.add_metadata("variables", ", ".join(request_variables))
        # scheduler.close()
        # client.close()

    def request_timeseries_parallel(self, scheduler=None):
        warnings.filterwarnings("ignore", category=ResourceWarning)
        if not scheduler:
            scheduler = os.getenv('DASK_SCHEDULER', "127.0.0.1:8786")
        client = Client(scheduler)

        request_url = nwm_21_url
        request_variables = copy.copy(variables)
        n_days = 365

        # s3 = s3fs.S3FileSystem(anon=True)
        # s3.connect_timeout = 60 * 60 * 1
        # s3.read_timeout = 60 * 60 * 1

        if self.waterbody:
            logging.info("Requesting NWM waterbody data")
            request_url = nwm_21_wb_url
            request_variables = copy.copy(wb_variables)
            request_inputs = [[copy.copy(self.start_date), copy.copy(self.start_date), request_variables, request_url]]
        else:
            request_inputs = []
            i_date = copy.copy(self.start_date)
            j_date = copy.copy(self.start_date) + datetime.timedelta(days=n_days)
            e_date = copy.copy(j_date)

            while e_date < self.end_date:
                if j_date >= self.end_date:
                    e_date = copy.copy(self.end_date)
                else:
                    e_date = copy.copy(j_date)
                request_inputs.append([copy.copy(i_date), copy.copy(e_date), request_variables, request_url])
                i_date = copy.copy(j_date)
                j_date = copy.copy(e_date) + datetime.timedelta(days=n_days)

        logging.info(f"Using NWM 2.1 URL: {request_url}")
        logging.info(f"Request data for COMIDS: {self.comids}")
        logging.info("Executing optimized nwm data call")
        cpu_count = os.getenv('PARALLEL_PROCESSES', mp.cpu_count())
        # cpu_count = cpu_count if cpu_count <= len(request_inputs) else len(request_inputs)
        # cpu_count = 4
        pool = mp.Pool(cpu_count)
        data_results = []
        for r_inputs in request_inputs:
            data_results.append(self.request_timeseries_i(*r_inputs))
        # data_results = pool.starmap_async(self.request_timeseries_i, request_inputs).get()
        # pool.close()
        # pool.join()
        ds_streamflow = xr.merge(data_results)

        self.data = ds_streamflow
        self.output.add_metadata("retrieval_timestamp", datetime.datetime.now().isoformat())
        self.output.add_metadata("source_url", nwm_url)
        self.output.add_metadata("variables", ", ".join(request_variables))

    def request_timeseries_i(self, start_date, end_date, request_variables, request_url):
        s3 = s3fs.S3FileSystem(anon=True)
        s3.connect_timeout = 60 * 60 * 1
        s3.read_timeout = 60 * 60 * 1
        store = s3fs.S3Map(root=request_url, s3=s3, check=False)

        ds = xr.open_zarr(store=store, consolidated=True)

        with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            ds_streamflow = ds[request_variables].sel(feature_id=self.comids).sel(time=slice(
                f"{start_date.year}-{start_date.month}-{start_date.day}",
                f"{end_date.year}-{end_date.month}-{end_date.day}"
            )).load(optimize_graph=True, traverse=False)
        return ds_streamflow

    def _load_lakeparm(self):
        lakenc = xr.open_dataset(lakeparm_path)
        lake_df = lakenc[["lake_id", "LkArea", "OrificeE"]].to_dataframe()
        lake_data = lake_df.loc[lake_df['lake_id'].isin(self.comids)]
        return lake_data

    def set_output(self, return_dataframe: bool=False):
        if self.data is None:
            return
        for k, v in self.data.attrs.items():
            self.output.add_metadata(k, v)
        j = 1
        vars = copy.copy(wb_variables) if self.waterbody else copy.copy(variables)
        for c in self.comids:
            for v in vars:
                self.output.metadata[f"column_{j}_units"] = str(self.data.data_vars[v].attrs["units"])
                j += 1
        timeseries = self.data.to_dataframe()
        for v in vars:
            nan_count = timeseries[v].isna().sum()
            if nan_count > 0:
                self.output.add_metadata(f"{v}_missing_value_count", str(nan_count))
                self.output.add_metadata(f"{v}_missing_value_flag", str(missing_value))
                timeseries[v] = timeseries[v].replace(np.nan, missing_value)
        if return_dataframe:
            return timeseries
        i = 1
        first = True
        if self.waterbody:
            vars.append("volume")
            lake_data = self._load_lakeparm()
            timeseries["volume"] = (float(lake_data["LkArea"]) * 1000000.0) * (timeseries["water_sfc_elev"] - float(lake_data["OrificeE"]))

        for idx, catchment in timeseries.groupby("feature_id"):
            i_meta = True
            for date, row in catchment.iterrows():
                d = date[0].strftime('%Y-%m-%d %H')
                if first:
                    self.output.data[d] = [r for r in row[vars]]
                else:
                    for r in row[vars]:
                        self.output.data[d].append(r)
                if i_meta:
                    for v in vars:
                        self.output.metadata[f"column_{i}"] = f"{v}-{idx}"
                        i += 1
                    i_meta = False
            first = False
        if self.waterbody:
            self.output.metadata[f"column_{i-1}_units"] = "m3"


if __name__ == "__main__":
    import time

    start_date = "2000-01-01"
    end_date = "2002-12-31"
    comids = [2043493]
    # comids = [6277975, 6278087]
    # comids = [6277975, 6278087]

    t0 = time.time()
    nwm = NWM(start_date=start_date, end_date=end_date, comids=comids)
    scheduler = LocalCluster(n_workers=10, threads_per_worker=2, processes=False)
    nwm.request_timeseries(scheduler=scheduler)
    # nwm.request_timeseries_parallel(scheduler=scheduler)
    t1 = time.time()
    print(f"Request time: {round(t1-t0, 4) / 60} min(s)")
    df = nwm.set_output(return_dataframe=True)
    print(df.shape)
    t2 = time.time()
    print(f"Set output time: {round(t2-t1, 4)} sec")
    print(f"Total Runtime: {round(t2-t0, 4)/60} min(s)")

# (4, 10, opt: True nwm_2: True) = 3.84757 min
