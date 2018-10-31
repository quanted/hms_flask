# from flask import Response, Flask, request, jsonify
# from flask_restful import Resource, reqparse, abort
# from werkzeug.datastructures import FileStorage
import multiprocessing as mp
from osgeo import ogr
from osgeo import osr
import urllib.request
from zipfile import *
import shapefile
import datetime
import requests
import shutil
import json
import time
import io
import re
import os


# parser = reqparse.RequestParser()
# parser.add_argument('huc_8_num')
# parser.add_argument('huc_12_num')
# parser.add_argument('lat_long_x')
# parser.add_argument('lat_long_y')
# parser.add_argument('filename', location='files', type=FileStorage)


class CatchmentGrid:
    """
    User sends Catchment ID (CommID) and HUC8 number to get % area of each NLDAS/GLDAS cell covered by the catchment
    Sample usage:
    1) http://127.0.0.1:5000/gis/rest/hms/percentage/?huc_12_num=020100050107
    2) http://127.0.0.1:5000/gis/rest/hms/percentage/?huc_8_num=01060002
    3) http://127.0.0.1:5000/gis/rest/hms/percentage/?huc_8_num=01060002&com_id_num=9311911
    6/7) http://127.0.0.1:5000/gis/rest/hms/percentage/?lat_long_x=-83.5&lat_long_y=33.5
    """

    @staticmethod
    def getIntersectCellsInCatchment(huc_id, grid_source):
        table = process_huc_8(huc_id, grid_source)
        return json.dumps(table, indent=4, sort_keys=True, default=str)

    @staticmethod
    def getIntersectCellsInHuc12(huc_id, grid_source):
        table = process_huc_12(huc_id, grid_source)
        return json.dumps(table, indent=4, sort_keys=True, default=str)

    @staticmethod
    def getIntersectCellsInComlist(comlist, grid_source):
        table = process_com_list(comlist, grid_source)
        return json.dumps(table, indent=4, sort_keys=True, default=str)


# class getPercentArea(Resource):
#     '''
# 	User sends Catchment ID (CommID) and HUC8 number to get % area of each NLDAS/GLDAS cell covered by the catchment
# 	Sample usage:
# 	1) http://127.0.0.1:5000/gis/rest/hms/percentage/?huc_12_num=020100050107
# 	2) http://127.0.0.1:5000/gis/rest/hms/percentage/?huc_8_num=01060002
# 	3) http://127.0.0.1:5000/gis/rest/hms/percentage/?huc_8_num=01060002&com_id_num=9311911
# 	6/7) http://127.0.0.1:5000/gis/rest/hms/percentage/?lat_long_x=-83.5&lat_long_y=33.5
# 	'''
#
#     def get(self):
#         args = parser.parse_args()
#         huc_8_num = args.huc_8_num
#         huc_12_num = args.huc_12_num
#         com_id_num = args.com_id_num
#         latlongx = args.lat_long_x
#         latlongy = args.lat_long_y
#         if (huc_8_num):
#             tab = process_huc_8(huc_8_num, com_id_num)
#         elif (huc_12_num):
#             tab = process_huc_12(huc_12_num[0:8])  # Huc12s are catalogued on ftp server by huc8 numbers
#         elif (latlongx and latlongy):
#             coord = '(' + latlongx + '+' + latlongy + ')'
#             tab = process_lat_long(coord)
#         return jsonify(tab)
#
#     '''
# 	User uploads geojson of a catchment or NHDPlus
# 	Sample usage:
# 	4/5) curl -X POST -F 'filename=@file.geojson' http://127.0.0.1:5000/gis/rest/hms/percentage/
# 	'''
#
#     def post(self):
#         args = parser.parse_args()
#         if (args.filename is not None):  # Using curl
#             tab = process_geojson(args.filename.read())
#         else:
#             return Response("{'posting error': 'POST operation failed'}")
#         return jsonify(tab)


class GeometryTable():
    def __init__(self):
        self.geometry = {}  # Dictionary of catchments, where the key is the catchment ID
        self.metadata = {}  # and the value is a Catchment


class Catchment():
    def __init__(self):
        self.points = []  # An array of CatchmentPoint objects


class CatchmentPoint():
    def __init__(self, cellArea, containedArea, latitude, longitude, percentArea):
        self.cellArea = cellArea
        self.containedArea = containedArea
        self.latitude = latitude
        self.longitude = longitude
        self.percentArea = percentArea


def process_huc_8(huc_8_num, grid_source):
    url = 'ftp://newftp.epa.gov/exposure/BasinsData/NHDPlus21/NHDPlus' + str(huc_8_num) + '.zip'
    req = urllib.request.urlopen(url)
    shzip = ZipFile(io.BytesIO(req.read()))
    mshp = shzip.open('NHDPlus' + str(huc_8_num) + '/Drainage/Catchment.shp')
    mdbf = shzip.open('NHDPlus' + str(huc_8_num) + '/Drainage/Catchment.dbf')
    sfile = shp_to_geojson(mshp, mdbf)
    gridfile = get_grid(grid_source)
    result_table = readHucGeometry(sfile, gridfile, url, None)
    return result_table


def process_huc_12(huc_12_num, grid_source):
    start = time.time()
    table = GeometryTable()
    url = 'https://watersgeo.epa.gov/arcgis/rest/services/NHDPlus_NP21/Catchments_NP21_Simplified/MapServer/0/query?where=WBD_HUC12+LIKE+%28%27' + str(huc_12_num) + '%27%29&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=FEATUREID&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=geojson'
    filestr = urllib.request.urlopen(url).read()
    sfile = re.search(r'(?={"type").*(]})', str(filestr)).group(0)
    gridfile = get_grid(grid_source)
    result_table = readHucGeometry(sfile, gridfile, url, None)
    return result_table

def get_grid(grid_source):
    shape = None
    if(grid_source == 'nldas'):
        return '/NLDAS.geojson'
    elif (grid_source == 'gldas'):
        return '/GLDAS.geojson'
    elif (grid_source == 'prism'):
        return '/PRISM.bil'
    elif (grid_source == 'daymet'):
        return '/DAYMET.geojson'


def shp_to_geojson(mshp, mdbf):
    # read the shapefile
    reader = shapefile.Reader(shp=mshp, dbf=mdbf)
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    buffer = []
    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        if ('WBD_Date' in field_names):  # New NHDPlus files use non-serializable datetime objects, must convert to str
            atr['WBD_Date'] = str(atr['WBD_Date'])
        geom = sr.shape.__geo_interface__
        buffer.append(dict(type="Feature", geometry=geom, properties=atr))
    # write the GeoJSON file
    geojson = json.dumps({"type": "FeatureCollection", "features": buffer}, indent=2) + "\n"
    return geojson


def process_com_list(comlist, grid_source):
	start = time.time()
	table = GeometryTable()
	multi, coms = [], []
	url = ''
	for comid in comlist.split(','):
		sfile = urllib.request.urlopen(
			'https://ofmpub.epa.gov/waters10/NavigationDelineation.Service?pNavigationType=PP&pStartComid=' + str(
				comid) + '&pStopComid=' + str(comid)).read()
		com = re.search(r'([0-9]{2,})', str(sfile))
		geojson = re.search(r'(?={"type").*(]})', str(sfile))
		if (geojson != None):
			coms.append(comid)
			geo = ogr.CreateGeometryFromJson(geojson.group(0))
			multi.append(geo)
	table = readComs(multi, coms)
	table['metadata']['execution time'] = time.time() - start
	return table


def process_lat_long(coordinate):
    start = time.time()
    table = GeometryTable()
    url = 'https://ofmpub.epa.gov/waters10/SpatialAssignment.Service?pGeometry=POINT' + coordinate + '&pLayer=NHDPLUS_CATCHMENT&pSpatialSnap=TRUE&pReturnGeometry=TRUE'
    req = urllib.request.urlopen(url)
    sfile = req.read()
    com = re.search(r'("[0-9]{2,}")', str(sfile))
    geojson = re.search(r'(?={"type").*(]})', str(sfile))
    comid = com.group(0).replace('"', '')
    result_table = readGeometry(geojson.group(0), url, comid)
    table.geometry[comid] = result_table
    table.metadata['request date'] = datetime.datetime.now()
    table.metadata['shapefile source'] = url
    table.metadata['nldas source'] = 'https://ldas.gsfc.nasa.gov/nldas/gis/NLDAS_Grid_Reference.zip'
    table.metadata['execution time'] = time.time() - start
    return table.__dict__


def readComs(geometries, comlist):
	pa = os.path.dirname(__file__)
	nldas = ogr.Open(pa + '/NLDAS.geojson')  # To Do: Change to filepath on server
	nldasLayer = nldas.GetLayer()
	overlap, newpolygons = [], []
	totalPoly = ogr.Geometry(ogr.wkbMultiPolygon)
	# Treat all polygons as one larger one since we are just finding overlapping cells
	for polygon in geometries:
		totalPoly.AddGeometry(polygon)
		newpolygons.append(polygon.ExportToJson())
	totalPoly = totalPoly.UnionCascaded()
	if (totalPoly == None):
		totalPoly = geometries[0].GetGeometryRef()  # latLong
	# Calculate cells that contain polygons ahead of time to make intersections faster
	text_file = open("Output.geojson", "w")
	text_file.write(totalPoly.ExportToJson())
	text_file.close()
	for feature in nldasLayer:
		cell = feature.GetGeometryRef()
		if (totalPoly.Intersects(cell)):
			overlap.append(cell.ExportToJson())
	table = GeometryTable()
	i = 0;
	num_points = 0
	for polygon in newpolygons:
		obj, np = calculations(polygon, overlap)
		table.geometry[comlist[i]] = obj.__dict__
		num_points += np
		i += 1
	'''
	pool = mp.Pool(30)  # 30 = Number of cores used to parallelize
	args = [(polygon, overlap) for polygon in newpolygons]
	results = pool.map_async(calculations, args)
	pool.close()
	pool.join()
	for obj, np in results.get():
		table.geometry[comlist[i]] = obj.__dict__
		#table = obj
		num_points += np
		i += 1'''
	table.metadata['request date'] = datetime.datetime.now()
	table.metadata['number of points'] = num_points
	table.metadata['shapefile source'] = 'https://ofmpub.epa.gov/waters10/NavigationDelineation.Service'
	table.metadata['nldas source'] = 'https://ldas.gsfc.nasa.gov/nldas/gis/NLDAS_Grid_Reference.zip'
	#table.metadata['execution time'] = time.time() - start
	# De-reference shapefiles
	shape = None
	nldas = None
	return table.__dict__


def readGeometry(sfile, url, com):
	#start = time.time()
	shapeFiles = []
	nldasFiles = []
	colNames = []
	shape = ogr.Open(sfile)
	pa = os.path.dirname(__file__)
	nldas = ogr.Open(pa + '/NLDAS.geojson')  # To Do: Change to filepath on server
	shapeLayer = shape.GetLayer()
	nldasLayer = nldas.GetLayer()
	# Getting features from shapefile
	colLayer = shape.GetLayer(0).GetLayerDefn()
	for i in range(colLayer.GetFieldCount()):
		colNames.append(colLayer.GetFieldDefn(i).GetName())
	coms = []
	overlap, polygons, newpolygons = [], [], []
	if ('COMID' in colNames):
		for feature in shapeLayer:
			if(com):
				if (feature.GetField('COMID') == int(com)):  # Only focusing on the given catchment argument
					polygons.append(feature)
					coms.append(feature.GetField('COMID'))
			else:
				polygons.append(feature)
				coms.append(feature.GetField('COMID'))
		if(len(polygons) <= 0):
			return 'The COMID was not found in the specified HUC8.'  # If user enters comid not in huc
	elif ('HUC_8' in colNames):
		for feature in shapeLayer:
			polygons.append(feature)
			try:
				coms.append(feature.GetField('OBJECTID'))# Object IDs specify each shape (treat like catchment)
			except:
				coms = [None] * len(shapeLayer)
	else:
		coms = [None] * len(shapeLayer)
		if(len(com) > 1):
			coms = com
		else:
			coms[0] = com  #Lat/long case specifies one COMID
		for feature in shapeLayer:
			polygons.append(feature)
	# Reproject geometries from shapefile
	totalPoly = ogr.Geometry(ogr.wkbMultiPolygon)
	# Treat all polygons as one larger one since we are just finding overlapping cells
	for polygon in polygons:
		poly = polygon.GetGeometryRef()
		totalPoly.AddGeometry(poly)
		newpolygons.append(poly.ExportToJson())
	totalPoly = totalPoly.UnionCascaded()
	if (totalPoly == None):
		totalPoly = polygons[0].GetGeometryRef()  # latLong
	# Calculate cells that contain polygons ahead of time to make intersections faster
	for feature in nldasLayer:
		cell = feature.GetGeometryRef()
		if (totalPoly.Intersects(cell)):
			overlap.append(cell.ExportToJson())
	table = GeometryTable()
	i = 0;
	num_points = 0
	for polygon in newpolygons:
		obj, np = calculations(polygon, overlap)
		#table.geometry[coms[i]] = obj.__dict__
		table = obj
		num_points += np
		i += 1
	'''
	pool = mp.Pool(30)	#30 = Number of cores used to parallelize
	args = [(polygon, overlap) for polygon in newpolygons]
	results = pool.map_async(calculations, args)
	pool.close()
	pool.join()
	for obj, np in results.get():
		#table.geometry[coms[i]] = obj.__dict__
		table = obj
		num_points += np
		i += 1'''
	table.metadata['request date'] = datetime.datetime.now()
	table.metadata['number of points'] = num_points
	table.metadata['shapefile source'] = url
	table.metadata['nldas source'] = 'https://ldas.gsfc.nasa.gov/nldas/gis/NLDAS_Grid_Reference.zip'
	table.metadata['execution time'] = time.time() - start
	# De-reference shapefiles
	shape = None
	nldas = None
	return table.__dict__


def calculations(polygon, overlap):
	num_points = 0
	poly = ogr.CreateGeometryFromJson(polygon)
	huc12table = Catchment()
	for feature in overlap:
		cell = ogr.CreateGeometryFromJson(feature)
		interArea = 0
		squareArea = cell.Area()
		if (poly.Intersects(cell)):
			inter = poly.Intersection(cell)
			if inter == None:
				interArea += 0
			else:
				interArea += inter.Area()
			percentArea = (interArea / squareArea) * 100
			catchtable = CatchmentPoint(squareArea, interArea, cell.Centroid().GetY(), cell.Centroid().GetX(),
										percentArea)
			huc12table.points.append(catchtable.__dict__)
			num_points += 1
	return huc12table, num_points


'''
HUC 8 and HUC 12
'''


def readHucGeometry(sfile, gridfile, url, com):
	start = time.time()
	colNames = []
	shape = ogr.Open(sfile)
	#nldas = ogr.Open('NLDAS.geojson')  # To Do: Change to filepath on server
	shapeLayer = shape.GetLayer()
	#nldasLayer = nldas.GetLayer()
	pa = os.path.dirname(__file__)
	grid = ogr.Open(pa + gridfile)  # To Do: Change to filepath on server
	gridLayer = grid.GetLayer()
	# Getting features from shapefile
	colLayer = shape.GetLayer(0).GetLayerDefn()
	for i in range(colLayer.GetFieldCount()):
		colNames.append(colLayer.GetFieldDefn(i).GetName())
	coms = []
	overlap, polygons, newpolygons = [], [], []
	if ('FEATUREID' in colNames):   #'FEATUREID' was 'COMID'
		for feature in shapeLayer:
			if (com):
				if (feature.GetField('FEATUREID') == int(com)):  # Only focusing on the given catchment argument
					polygons.append(feature)
					coms.append(feature.GetField('FEATUREID'))
			else:
				polygons.append(feature)
				coms.append(feature.GetField('FEATUREID'))
		if (len(polygons) <= 0):
			return 'The COMID was not found in the specified HUC.'  # If user enters comid not in huc
	elif ('COMID' in colNames):   #'FEATUREID' was 'COMID'
		for feature in shapeLayer:
			if (com):
				if (feature.GetField('COMID') == int(com)):  # Only focusing on the given catchment argument
					polygons.append(feature)
					coms.append(feature.GetField('COMID'))
			else:
				polygons.append(feature)
				coms.append(feature.GetField('COMID'))
		if (len(polygons) <= 0):
			return 'The COMID was not found in the specified HUC.'  # If user enters comid not in huc
	elif ('HUC_8' in colNames):
		for feature in shapeLayer:
			polygons.append(feature)
			try:
				coms.append(feature.GetField('OBJECTID'))  # Object IDs specify each shape (treat like catchment)
			except:
				coms = [None] * len(shapeLayer)
	else:
		coms = [None] * len(shapeLayer)
		if (com):
			coms[0] = com  # Lat/long case specifies one COMID
		for feature in shapeLayer:
			polygons.append(feature)
	# Reproject geometries from shapefile
	totalPoly = ogr.Geometry(ogr.wkbMultiPolygon)
	# Treat all polygons as one larger one since we are just finding overlapping cells
	for polygon in polygons:
		poly = polygon.GetGeometryRef()
		totalPoly.AddGeometry(poly)
		newpolygons.append(poly.ExportToJson())
	totalPoly = totalPoly.UnionCascaded()
	if (totalPoly == None):
		totalPoly = polygons[0].GetGeometryRef()  # latLong
	# Calculate cells that contain polygons ahead of time to make intersections faster
	for feature in gridLayer:
		cell = feature.GetGeometryRef()
		if (totalPoly.Intersects(cell)):
			overlap.append(cell.ExportToJson())
	table = GeometryTable()
	i = 0;
	num_points = 0
	for polygon in newpolygons:
		obj, np = calculations(polygon, overlap)
		table.geometry[coms[i]] = obj.__dict__
		num_points += np
		i += 1
	'''
	pool = mp.Pool(30)  # 30 = Number of cores used to parallelize
	args = [(polygon, overlap) for polygon in newpolygons]
	results = pool.map_async(calculations, args)
	pool.close()
	pool.join()
	for obj, np in results.get():
		table.geometry[coms[i]] = obj.__dict__
		num_points += np
		i += 1'''
	table.metadata['request date'] = datetime.datetime.now()
	table.metadata['number of points'] = num_points
	table.metadata['shapefile source'] = url
	table.metadata['nldas source'] = 'https://ldas.gsfc.nasa.gov/nldas/gis/NLDAS_Grid_Reference.zip'
	table.metadata['execution time'] = time.time() - start
	# De-reference shapefiles
	shape = None
	nldas = None
	return table.__dict__