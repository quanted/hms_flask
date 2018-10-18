from shapely.geometry import Point, shape
from fiona.crs import from_epsg
import geopandas as geo
import math
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

        try:
            stations = getStations(extent, startDate, endDate)
            intersect_stations = stationsInGeometry(geojson['features'], stations)
            return json.dumps(intersect_stations)
        except Exception as ex:
            return "{'station collection error': 'Error attempting to collect stations from NCDC.'}"

    @staticmethod
    def findStationFromPoint(lat, lng, startDate=None, endDate=None):
        logging.info("HMS Celery task: finding NCDC Station closest to POINT({} {})".format(lng, lat))
        lat = float(lat)
        lng = float(lng)
        df = geo.GeoDataFrame()
        df['geometry'] = Point(lng, lat)
        df.crs = from_epsg("4326")
        bounds_list = []
        i = 1
        i_max = 10
        while i < i_max:
            # 1 arc-minutes ~= 1.852km
            initial_bounds = [lng - ((1/30)*i), lat - ((1/30)*i), lng + ((1/30)*i), lat + ((1/30)*i)]
            bounds_list.append(initial_bounds)
            stations = getStations(initial_bounds, startDate, endDate)
            if len(stations) == 0:
                i = i + 1
            elif len(stations["results"]) >= 1:
                stations_list = orderStations(stations, lat, lng)
                return json.dumps(stations_list)
            else:
                i = i + 1
        return "{'stationNotFoundError': 'No stations were found within one deg of Point({} {})'}".format(lng, lat)


def isExtentValid(bounds):
    return bounds[0] > 90 or bounds[0] < -90 or bounds[1] > 180 or bounds[1] < 180 or bounds[2] > 90 or bounds[2] < -90 or bounds[3] > 180 or bounds[3] < -180

def getStations(bounds, startDate, endDate):
    '''
    Gets the stations within the coordinate bounds provided that have data for the specified time period.
    :param bounds: Array of coordinates [lower left lat, lower left lng, upper right lat, upper right lng]
    :param startDate:
    :param endDate:
    :return:
    '''
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

def orderStations(stations, lat, lng):
    station_list = []
    for station in stations["results"]:
        d = round(math.sqrt(math.pow(float(station["latitude"]) - float(lat), 2) + math.pow(float(station["longitude"]) - float(lng),2)) * 111, 4)
        _s = {
            "id": station["id"],
            "distance": d,
            "data": station,
            "metadata": {
                "distance_units": "(km)"
            }
        }
        station_list.append(_s)
    station_list.sort(key=lambda x: x["distance"])
    return station_list
