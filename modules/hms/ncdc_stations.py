from shapely.geometry import Point, shape
from fiona.crs import from_epsg
import geopandas as geo
import logging
import json
import requests


class NCDCStations:
    @staticmethod
    def findStationsInGeoJson(geojson, startDate=None, endDate=None, crs=None):
        logging.info("HMS Celery task: searching for NCDC Stations with geojson bounds. process starting...")
        geometry = geo.GeoDataFrame.from_features(geojson)
        if crs is not None and crs is not "4326":
            geometry.crs = from_epsg(crs)
            geometry = geometry.to_crs({'init': 'epsg:326'})
            geojson = json.loads(geometry.to_json())
            extent = geometry.total_bounds
        else:
            geometry.crs = {'init': '+proj=longlat +datum=WGS84 +no_defs'}
            extent = geometry.total_bounds
        stations = getStations(extent, startDate, endDate)
        try:
            intersect_stations = stationsInGeometry(geojson['features'], stations)
        except:
            return "{'station collection error': 'Error attempting to collect stations from NCDC.'}"
        return json.dumps(intersect_stations)


def isExtentValid(bounds):
    return bounds[0] > 90 or bounds[0] < -90 or bounds[1] > 180 or bounds[1] < 180 or bounds[2] > 90 or bounds[2] < -90 or bounds[3] > 180 or bounds[3] < -180


def getStations(bounds, startDate, endDate):
    token = "RUYNSTvfSvtosAoakBSpgxcHASBxazzP"
    base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations"
    start_date = "startdate=" + startDate
    end_date = "enddate=" + endDate
    data_category = "datacategoryid=PRCP"
    extent = "extent=" + str(bounds[1]) + "," + str(bounds[0]) + "," + str(bounds[3]) + "," + str(bounds[2])
    request_url = base_url + "?" + start_date + "&" + end_date + "&" + extent + "&" + data_category + "&" + "limit=1000"
    headers = {'token': token}
    stations = requests.get(request_url, params=None, headers=headers)
    return json.loads(stations.text)


def stationsInGeometry(geometry, stations):
    intersect_stations = []
    station_index = 0
    intersect_stations.append(["ID", "NAME", "LONG", "LAT", "ELEVATION", "STATIONID"])
    geometry = shape(geometry[0]['geometry'])
    print("Number of stations: " + str(len(stations["results"])))
    for station in stations["results"]:
        point = Point(station["longitude"], station["latitude"])
        point.crs = {'init': '+proj=longlat +datum=WGS84 +no_defs'}
        if geometry.contains(point):
            station_index += 1
            add_station = [station_index, station["name"], station["longitude"], station["latitude"], station["elevation"], station["id"]]
            intersect_stations.append(add_station)
    print("Number of stations in geometry:" + str(station_index))
    return intersect_stations
