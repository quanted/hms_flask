from flask_restful import Resource, reqparse
from xml.etree import ElementTree as ET
from flask import Response
from netCDF4 import Dataset
from pyproj import Proj
from dateutil import tz
import numpy as np
import requests
import logging
import json
import datetime
import tempfile
import io


class DataTable():
    def __init__(self):
        self.data = {}  # Dictionary of catchments, where the key is the catchment ID
        self.metadata = {}  # and the value is a Catchment

class NWMData:

	@staticmethod
	def JSONData(dataset, comid, startDate, endDate, lat, long, NWM_TASK_COUNT):
		#https://hs-apps.hydroshare.org/apps/nwm-forecasts/api/GetWaterML/?config=analysis_assim&geom=channel_rt&variable=streamflow&COMID=11359107&startDate=2019-04-11&endDate=2019-04-12
		# http://localhost:7777/hms/nwm/data?dataset=streamflow&comid=11359107&startDate=2019-05-11&endDate=2019-05-12
		startTime = datetime.datetime.strptime(startDate, "%Y-%m-%d")
		endTime = datetime.datetime.strptime(endDate, "%Y-%m-%d")
		fortyday = datetime.date.today() + datetime.timedelta(days=-40)
		# Check that start and end dates are within available 40 day range
		if (endTime.date() > datetime.date.today() or startTime.date() < fortyday):
			return json.dumps({ "ERROR:" : "Date range must occur between " + fortyday.strftime(
				"%Y-%m-%d") + " and " + datetime.date.today().strftime("%Y-%m-%d")})
		url = "https://hs-apps.hydroshare.org/apps/nwm-forecasts/api/GetWaterML/"
		if(dataset == "streamflow"):
			url += "?config=analysis_assim&geom=channel_rt&variable=" + dataset + "&COMID=" + comid + "&startDate=" + startDate + "&endDate=" + endDate
		elif(dataset == "precipitation"):
			#Convert lat long to lcc grid cell
			p = Proj(proj='lcc', lat_0=40, lat_1=30, lat_2=60, lon_0=-97, x_0=0, y_0=0, a=6370000, b=6370000)
			x, y = p(long, lat)
			x = int(x/1000)
			y = int(y/1000)
			url += "?archive=rolling&config=analysis_assim&geom=forcing&variable=RAINRATE&COMID=" + str(x) + "," + str(y) + "&startDate=" + startDate + "&endDate=" + endDate
		header = {'Authorization': 'Token 2bc568b8f1b5f49153df7f691cfbebc1d7d740b3'}
		#if (NWM_TASK_COUNT % 2):	#Switch between different tokens based on how many requests have been made.
		#	header = ''#Replace with a different valid token
		logging.info(url)
		xml = requests.get(url, headers=header)
		print("NWM Data: " + xml.text)
		try:
			root = ET.fromstring(xml.text)
		except ET.ParseError as e:
			return json.dumps({ "ERROR:" : "Data could not be retrieved: " + str(e.text)})
		#xml = requests.get(url, headers=header)

		from_zone = tz.tzutc()
		to_zone = tz.tzlocal()
		#namespaces = {'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance', 'xmlns': 'http://www.cuahsi.org/waterML/1.1/'}
		jsondata = DataTable()
		for item in root[1][2]:
			if ("dateTime" in item.attrib.keys()):
				date = item.attrib["dateTime"]
				jsondata.data[date] = [item.text]
				# CONVERT FROM GMT TO LOCAL TIME
				#utc = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
				#utc = utc.replace(tzinfo=from_zone)
				#converted = utc.astimezone(to_zone).strftime("%Y-%m-%dT%H:%M:%S")
				#jsondata[converted] = item.text
		# Append metadata?
		jsondata.metadata['nwm_date_of_creation'] = root[0][0].text
		jsondata.metadata['nwm_dataset_info'] = root[1][0][0].text
		jsondata.metadata['nwm_units'] = root[1][1][6][1].text + ', ' + root[1][1][6][2].text
		return json.dumps(jsondata.__dict__)

	@staticmethod
	def NetCDFData(dataset, comid, startDate):
		# NET CDF TOO BIG TO STORE IN MONGODB
		year = startDate[0:4]
		month = startDate[5:7]
		day = startDate[8:10]
		url = "https://nwm-archive.s3.amazonaws.com/" + year + "/" + year + month + day + "1200.CHRTOUT_DOMAIN1.comp"
		r = requests.get(url, params=None)  # , headers=headers)
		temp = tempfile.NamedTemporaryFile(delete=False)
		temp.write(r.content)
		temp.seek(0)
		ds = Dataset(temp.name)
		# ds = {}
		temp.close()
		id = np.array(ds.variables["feature_id"])
		ts = np.array(ds.variables["time"])
		sf = np.array(ds.variables["streamflow"])
		i = 0
		ts_v = datetime.datetime.utcfromtimestamp(ts[i] * 60)
		table = {}
		for i in range(0, id.size):
			table[i] = ("{}, {}, {}, {}\n".format(i, id[i], ts_v.strftime("%Y-%m-%d %H:%M:%S"), sf[i]))
		ds.close()
		#with tempfile.TemporaryFile() as file:
		#	file.write(b"i, COMID, Date/Time, Streamflow\n")
		#	while i < id.size:
		#		file.write(("{}, {}, {}, {}\n".format(i, id[i], ts_v.strftime("%Y-%m-%d %H:%M:%S"), sf[i])).encode())
		#		i += 1
		#	file.seek(0)
		#	ds.close()
		#	return file.read()
		return table.__dict__