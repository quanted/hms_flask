"""
file: nwm_forecast_data.py
date: 12/17/2019
description: Downloads NWM short term forecast data for channel routing (default), and assembles data into a timeseries
for a specified NHDPlus catchment COMID.
"""
import netCDF4
import numpy as np
import requests
import json
import time
import copy
from datetime import datetime, timedelta


class NWMForecastData:
    """
    Class to determine latest NWM forecast directory and time.
    """
    def __init__(self, dt=None, dataset="channel_rt", dtype="short_range"):
        self.base_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm."
        self.sim_date = datetime.now() if dt is None else datetime.strptime(dt, "%Y-%m-%d %H")
        self.set_time = False if dt is None else True
        self.dataset = dataset
        self.dtype = dtype
        self.date_counter = 0
        self.current_datetime = copy.copy(self.sim_date)
        self.datestamp = None
        self.check_count = 0
        self.check_count_threshold = 5
        self.data = {}
        self.t = 0
        self.timeseries_data = {}
        self.status = ""
        self.timerstart = None
        self.timerstop = None

    def download_data(self):
        """
        Attempts to download the latest NWM forecast data, with recursive check for up to 5 days before present day.
        :return:
        """
        self.timerstart = time.time()
        datestamp = str(self.current_datetime.year) + str(self.current_datetime.month) + str(self.current_datetime.day)
        check_dir = False
        while not check_dir and self.check_count < self.check_count_threshold:
            check_dir = self.check_directory(datestamp)
            if check_dir:
                self.datestamp = datestamp
            else:
                self.check_count += 1
                temp_date = self.current_datetime + timedelta(days=-1)
                datestamp = str(temp_date.year) + str(temp_date.month) + str(temp_date.day)
        if not check_dir:
            self.status = "ERROR: Error locating nwm data directory."
        else:
            t = self.sim_date.hour if self.set_time else 23
            check_hour = False
            while not check_hour and t >= 0:
                check_hour = self.get_file(t, 1)
                if check_hour:
                    self.t = t
                else:
                    t -= 1

            if not check_hour:
                self.status = "ERROR: Error locating nwm data files in directory: {}".format(self.datestamp)
                if self.date_counter < 5:
                    self.current_datetime = self.current_datetime.day + timedelta(days=-1)
                    self.date_counter += 1
                    self.download_data()
            else:
                f = 2
                error_count = 0
                # netCDF4 Dataset is not picklable, unable to parallelize directly through multiprocessing
                # Converting each Dataset to a DaskArray may allow for parallel execution of the data retrieval
                while f <= 18 and error_count < 5:
                    got_file = self.get_file(self.t, f)
                    if got_file:
                        f += 1
                    else:
                        error_count += 1
                if error_count >= 5:
                    self.status = "ERROR: Error downloading nwm data. Directory: {}, Time: {}, F: {}".format(self.datestamp, self.t, f)
                else:
                    self.status = "Successfully downloaded all files."

    def check_directory(self, datestamp):
        """
        Determines if a NWM forecast directory is present.
        :param datestamp: Desired directory timestamp
        :return: True if timestep is present, otherwise False
        """
        request_url = self.base_url + datestamp + "/" + self.dtype + "/"
        dir_check = requests.get(request_url)
        if dir_check.status_code == 200:
            return True
        else:
            return False

    def get_file(self, t, f):
        """
        Determines if a NWM timed file is present in the datestamp directory.
        :param t: Timestep value (valid values 0 - 23)
        :param f: Forecast value (valid values 1 - 18)
        :return: True if data successfully downloaded, otherwise False
        """
        t = str(t) if t >= 10 else "0{}".format(t)
        f = str(f) if f >= 10 else "0{}".format(f)
        request_url = self.base_url + self.datestamp + "/" + self.dtype + "/nwm.t{}z.short_range.".format(t) + self.dataset + ".f0{}.conus.nc".format(f)
        file_check = requests.get(request_url)
        if file_check.status_code == 200:
            self.data[f] = netCDF4.Dataset("inmemory.nc", mode='r', diskless=True, memory=bytes(file_check.content))
            return True
        else:
            return False

    def generate_timeseries(self, comid):
        """
        Generates a timeseries from the NWM data collected in download_data
        :param comid: COMID for timeseries
        :return: JSON string of the timeseries object, containing the data and metadata dictionaries
        """
        if comid is None:
            return "ERROR: Invalid comid {}".format(comid)
        if self.status != "Successfully downloaded all files.":
            return "ERROR: Required data files not present."

        timestep = 1
        timeseries = {}
        while timestep <= len(self.data):
            t = str(timestep) if timestep >= 10 else "0{}".format(timestep)
            feature_ids = np.array(self.data[t].variables['feature_id'][:])
            comid_i = np.where(feature_ids == int(comid))[0]
            if comid_i is None:
                return "ERROR: Unable to find comid, comid: {}".format(comid)
            # date = datetime.fromtimestamp((self.data[t].variables['time'][:])[0] * 60)
            date = datetime.strptime(self.data[t].model_output_valid_time, "%Y-%m-%d_%H:%M:%S")
            streamflow = np.array(self.data[t].variables['streamflow'][:])[comid_i][0]
            velocity = np.array(self.data[t].variables["velocity"][:])[comid_i][0]
            tr_runoff = np.array(self.data[t].variables["qSfcLatRunoff"][:])[comid_i][0]
            bucket = np.array(self.data[t].variables["qBucket"][:])[comid_i][0]
            bt_runoff = np.array(self.data[t].variables["qBtmVertRunoff"][:])[comid_i][0]
            timeseries[date.strftime("%Y-%m-%d %H")] = [streamflow, velocity, tr_runoff, bucket, bt_runoff]
            timestep += 1

        # date_details = self.data["01"].variables['time'][:]
        date_ref = self.data["01"].model_output_valid_time
        # ldate_details = self.data[str(len(self.data))].variables['time'][:]
        date_ref2 = self.data[str(len(self.data))].model_output_valid_time
        streamflow_details = self.data["01"].variables['streamflow']
        velocity_details = self.data["01"].variables['velocity']
        tr_runoff_details = self.data["01"].variables["qSfcLatRunoff"]
        bucket_details = self.data["01"].variables["qBucket"]
        bt_runoff_details = self.data["01"].variables["qBtmVertRunoff"]

        date_start = datetime.strptime(date_ref, "%Y-%m-%d_%H:%M:%S")
        date_end = datetime.strptime(date_ref2, "%Y-%m-%d_%H:%M:%S")

        metadata = {
            # "startdate": datetime.fromtimestamp(date_details[0] * 60).strftime("%Y-%m-%d %H"),
            # "enddate": datetime.fromtimestamp(ldate_details[0] * 60).strftime("%Y-%m-%d %H"),
            "startdate": date_start.strftime("%Y-%m-%d %H"),
            "enddate": date_end.strftime("%Y-%m-%d %H"),
            "timezone": "UTC",
            "comid": str(comid),
            "column_1": streamflow_details.name,
            "column_1_name": streamflow_details.long_name,
            "column_1_units": streamflow_details.units,
            "column_2": velocity_details.name,
            "column_2_name": velocity_details.long_name,
            "column_2_units": velocity_details.units,
            "column_3": tr_runoff_details.name,
            "column_3_name": tr_runoff_details.long_name,
            "column_3_units": tr_runoff_details.units,
            "column_4": bucket_details.name,
            "column_4_name": bucket_details.long_name,
            "column_4_units": bucket_details.units,
            "column_5": bt_runoff_details.name,
            "column_5_name": bt_runoff_details.long_name,
            "column_5_units": bt_runoff_details.units,
            "retrieval_time": datetime.now().strftime("%Y/%m/%d %H:%M:%S") + " secs"
        }
        self.timerstop = time.time()
        metadata["retrieval_time_elapse"] = str(self.timerstop - self.timerstart)
        self.timeseries_data = {"data": timeseries, "metadata": metadata}
        return json.dumps(self.timeseries_data)
