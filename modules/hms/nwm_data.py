from flask_restful import Resource, reqparse
from xml.etree import ElementTree as ET
from flask import Response
from netCDF4 import Dataset
from dateutil import tz
import numpy as np
import requests
import json
import datetime
import tempfile
import io

class NWMData:

	@staticmethod
	def JSONData(dataset, comid, startDate, endDate):
		#https://hs-apps.hydroshare.org/apps/nwm-forecasts/api/GetWaterML/?config=analysis_assim&geom=channel_rt&variable=streamflow&COMID=11359107&startDate=2019-04-11&endDate=2019-04-12
		# http://localhost:7777/hms/nwm/data?dataset=streamflow&comid=11359107&startDate=2019-04-11&endDate=2019-04-12
		startTime = datetime.datetime.strptime(startDate, "%Y-%m-%d")
		endTime = datetime.datetime.strptime(endDate, "%Y-%m-%d")
		fortyday = datetime.date.today() + datetime.timedelta(days=-40)
		# Check that start and end dates are within available 40 day range
		if (endTime.date() > datetime.date.today() or startTime.date() < fortyday):
			return json.dumps({ "ERROR:" : "Date range must occur between " + fortyday.strftime(
				"%Y-%m-%d") + " and " + datetime.date.today().strftime("%Y-%m-%d")})
		url = "https://hs-apps.hydroshare.org/apps/nwm-forecasts/api/GetWaterML/?config=analysis_assim&geom=channel_rt&variable=" + dataset + "&COMID=" + comid + "&startDate=" + startDate + "&endDate=" + endDate
		header = {'Authorization': '2bc568b8f1b5f49153df7f691cfbebc1d7d740b3'}#*Replace this token? Using multiple?
		xml = requests.get(url, headers=header)
		root = ET.fromstring(xml.text)
		from_zone = tz.tzutc()
		to_zone = tz.tzlocal()
		#namespaces = {'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance', 'xmlns': 'http://www.cuahsi.org/waterML/1.1/'}
		jsondata = {}
		for item in root[1][2]:
			if ("dateTime" in item.attrib.keys()):
				date = item.attrib["dateTime"]
				# CONVERT FROM GMT TO LOCAL TIME
				utc = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
				utc = utc.replace(tzinfo=from_zone)
				converted = utc.astimezone(to_zone).strftime("%Y-%m-%dT%H:%M:%S")
				jsondata[converted] = item.text
		# Append metadata?
		return json.dumps(jsondata)

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