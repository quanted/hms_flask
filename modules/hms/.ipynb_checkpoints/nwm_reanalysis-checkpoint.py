import json
import requests
import fsspec
import geopandas as gp
import s3fs
import xarray as xr
import numpy as np
from dask.distributed import Client
from datetime import datetime
from timeseries import TimeSeriesOutput


epa_waters_url = "https://watersgeo.epa.gov/arcgis/rest/services/NHDPlus_NP21/Catchments_NP21_Simplified/MapServer/0/query?"
nwm_url = "s3://noaa-nwm-retro-v2-zarr-pds"


class NWM:

    def __init__(self, start_date: str, end_date: str, comid: str):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.comid = comid
        self.catchment = None
        self.geometry = None
        self.output = TimeSeriesOutput(source="nwm", dataset="streamflow")
        self.data = None

    def get_geometry(self):
        request_url = epa_waters_url + f"where=FEATUREID={self.comid}&text=&objectIds=&time=&geometry=&" \
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
        client = Client()
        sig_figs = 4
        bounds = self.geometry.bounds
        bounds = [round(float(bounds["miny"]), sig_figs), round(float(bounds["maxy"]), sig_figs), round(float(bounds["minx"]), sig_figs), round(float(bounds["maxx"]), sig_figs)]
        variable = 'streamflow'
        fs_mapper = fsspec.get_mapper(nwm_url, anon=True)
        ds = xr.open_zarr(store=fs_mapper, consolidated=True)
        ds_streamflow = ds[variable].sel(
            time=slice(
                f"{self.start_date.year}-{self.start_date.month}-{self.start_date.day} 00:00",
                f"{self.end_date.year}-{self.end_date.month}-{self.end_date.day} 00:00"
            ))

        bounds_idx = (ds_streamflow.latitude > bounds[0]) & (ds_streamflow.latitude < bounds[1]) & (
                    ds_streamflow.longitude > bounds[2]) & (ds_streamflow.longitude < bounds[3])
        # bounds_idx = (ds_streamflow.latitude > 41.0) & (ds_streamflow.latitude < 44.0) & (ds_streamflow.longitude > -75.0) & (ds_streamflow.longitude < -72.0)

        streamflow_all = ds_streamflow[:, bounds_idx]
        self.data = streamflow_all.mean(dim='feature_id').compute().to_pandas()

    def set_output(self):
        for k, v in self.data.attrs.items():
            self.output.add_metadata(k, v)
        dims = self.data.dims
        coords = self.data.coords
        values = self.data.values

        test = 1

