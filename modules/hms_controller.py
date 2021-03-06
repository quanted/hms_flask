from flask import Response
from flask_restful import Resource, reqparse, request
import pymongo as pymongo
from datetime import datetime
import uuid
import requests
import logging
import json
import os

from celery_cgi import celery

# HMS Modules import
from .hms.ncdc_stations import NCDCStations
from .hms.percent_area import CatchmentGrid
from .hms.hydrodynamics import FlowRouting
from .hms.nwm_data import NWMData
from .hms.nwm_forecast import NWMForecastData

IN_DOCKER = os.environ.get("IN_DOCKER")
NWM_TASK_COUNT = 0


def connect_to_mongoDB(database=None):
    if database is None:
        database = 'flask_hms'
    if IN_DOCKER == "False":
        # Dev env mongoDB
        logging.info("Connecting to mongoDB at: mongodb://localhost:27017/0")
        mongo = pymongo.MongoClient(host='mongodb://localhost:27017/0')
    else:
        # Production env mongoDB
        logging.info("Connecting to mongoDB at: mongodb://mongodb:27017/0")
        mongo = pymongo.MongoClient(host='mongodb://mongodb:27017/0')
    mongo_db = mongo[database]
    if database is 'flask_hms':
        mongo.flask_hms.Collection.create_index([("date", pymongo.DESCENDING)], expireAfterSeconds=86400)
        # ALL entries into mongo.flask_hms must have datetime.utcnow() timestamp, which is used to delete the record after 86400 seconds, 24 hours.
    elif database is 'nwm_data':
        mongo.nwm_data.Collection.create_index([("date", pymongo.DESCENDING)], expireAfterSeconds=64800)    # 18 hr
    else:
        mongo[database].Collection.create_index([("date", pymongo.DESCENDING)], expireAfterSeconds=604800)
        # Set expiration to be 1 week
    return mongo_db


parser_base = reqparse.RequestParser()


class HMSTaskData(Resource):
    """
    Controller class to retrieve data from the mongoDB database and/or checking status of a task
    """
    parser = parser_base.copy()
    parser.add_argument('job_id')

    def get(self):
        args = self.parser.parse_args()
        task_id = args.job_id
        if task_id is not None:
            task = celery.AsyncResult(task_id)
            if task.status == "SUCCESS":
                mongo_db = connect_to_mongoDB("hms")
                posts = mongo_db["data"]
                posts_data = posts.find_one({'_id': task_id})
                if posts_data is None:
                    data = None
                    print("No data for mongodb: hms, posts: data, id: {}".format(task_id))
                else:
                    data = json.loads(json.dumps(posts_data['data']))
                return Response(json.dumps({'id': task.id, 'status': task.status, 'data': data}))
            else:
                try:
                    mongo_db = connect_to_mongoDB("hms")
                    db = mongo_db["data"]
                    posts_data = db.find_one({'_id': task_id})
                    data = json.loads(posts_data['data'])
                    if data is not None:
                        status = "SUCCESS"
                    else:
                        status = task.status
                    return Response(json.dumps({'id': task.id, 'status': status, 'data': data}))
                except Exception as ex:
                    return Response(json.dumps({'id': task.id, 'status': task.status}))
        else:
            return Response(json.dumps({'error': 'id provided is invalid.'}))


class HMSFlaskTest(Resource):

    def get(self):
        test_id = self.run_test.apply_async(queue="qed")
        logging.info("Sending test task to Celery...")
        return Response(json.dumps({'job_id': test_id.id}))

    @celery.task(name="hms_flask_test", bind=True)
    def run_test(self):
        task_id = celery.current_task.request.id
        mongo_db = connect_to_mongoDB()
        posts = mongo_db.posts
        time_stamp = datetime.utcnow()
        data_value = {"request_time": str(time_stamp)}
        data = {'_id': task_id, 'date': time_stamp, 'data': json.dumps(data_value)}
        posts.insert_one(data)


class NCDCStationSearch(Resource):
    """
    Controller class for getting all ncdc stations within a provided geometry, as geojson, and a date range.
    """
    parser = parser_base.copy()
    parser.add_argument('latitude')
    parser.add_argument('longitude')
    parser.add_argument('comid')
    parser.add_argument('geometry')
    parser.add_argument('startDate')
    parser.add_argument('endDate')
    parser.add_argument('crs')

    def post(self):
        args = self.parser.parse_args()
        if args.startDate is None or args.endDate is None:
            return Response("{'input error':'Arguments startDate and endDate are required.")
        geojson = json.loads(args.geometry)
        job_id = self.start_async.apply_async(args=(geojson, args.startDate, args.endDate, args.crs, args.latitude, args.longitude, args.comid), queue="qed")
        # job_id = self.start_async(args=(geojson, args.startDate, args.endDate, args.crs, args.latitude, args.longitude), queue="qed")
        return Response(json.dumps({'job_id': job_id.id}))

    def get(self):
        args = self.parser.parse_args()
        if args.startDate is None or args.endDate is None:
            return Response("{'input error':'Arguments startDate and endDate are required.")
        job_id = self.start_async.apply_async(args=(None, args.startDate, args.endDate, args.crs, args.latitude, args.longitude, args.comid), queue="qed")
        # job_id = self.start_async(None, args.startDate, args.endDate, args.crs, args.latitude, args.longitude, args.comid)
        return Response(json.dumps({'job_id': job_id.id}))

    @celery.task(name='hms_ncdc_stations', bind=True)
    def start_async(self, geojson, start_date, end_date, crs, latitude=None, longitude=None, comid=None):
        task_id = celery.current_task.request.id
        logging.info("task_id: {}".format(task_id))
        logging.info("hms_controller.NCDCStationSearch starting search...")
        if geojson:
            stations = NCDCStations.findStationsInGeoJson(geojson, start_date, end_date, crs)
        elif comid:
            stations = NCDCStations.findStationFromComid(comid, start_date, end_date)
        else:
            stations = NCDCStations.findStationFromPoint(latitude, longitude, start_date, end_date)
        logging.info("hms_controller.NCDCStationSearch search completed.")
        logging.info("Adding data to mongoDB...")
        logging.info("Search data: {}".format(stations))
        mongo_db = connect_to_mongoDB("hms")
        posts = mongo_db["data"]
        time_stamp = datetime.utcnow()
        data = {'_id': task_id, 'date': str(time_stamp), 'data': stations}
        posts.insert_one(data)


class NWMDownload(Resource):
    """
    Json or NetCDF Download
    http://localhost:7777/hms/nwm/data/?dataset=streamflow&comid=6411690&startDate=2010-01-01&endDate=2010-12-31
    """
    parser = parser_base.copy()
    parser.add_argument('dataset')
    parser.add_argument('comid')
    parser.add_argument('startDate')
    parser.add_argument('endDate')
    parser.add_argument('long')
    parser.add_argument('lat')

    def get(self):
        args = self.parser.parse_args()
        task_id = self.start_async.apply_async(
            args=(args.dataset, args.comid, args.startDate, args.endDate, args.lat, args.long), queue="qed")
        return Response(json.dumps({'job_id': task_id.id}))

    @celery.task(name='hms_nwm_data', bind=True)
    def start_async(self, dataset, comid, startDate, endDate, lat, long):
        task_id = celery.current_task.request.id
        logging.info("task_id: {}".format(task_id))
        logging.info("hms_controller.NWMDownload starting calculation...")
        global NWM_TASK_COUNT
        NWM_TASK_COUNT += 1
        logging.info("NWM TASK COUNT: " + str(NWM_TASK_COUNT))
        logging.info("inputs id: {}, {}, {}, {}".format(dataset, comid, startDate, endDate, NWM_TASK_COUNT))
        if(endDate):
            nwm_data = NWMData.JSONData(dataset, comid, startDate, endDate, lat, long, NWM_TASK_COUNT)
        else:
            nwm_data = NWMData.NetCDFData(dataset, comid, startDate)
        logging.info("hms_controller.NWMDownload calcuation completed.")
        logging.info("Adding data to mongoDB...")
        mongo_db = connect_to_mongoDB("hms")
        posts = mongo_db["data"]
        time_stamp = datetime.utcnow()
        data = {'_id': task_id, 'date': time_stamp, 'data': nwm_data}
        posts.insert_one(data)


class NLDASGridCells(Resource):
    """

    """
    parser = parser_base.copy()
    parser.add_argument('huc_8_num')
    parser.add_argument('huc_12_num')
    parser.add_argument('com_id_list')
    parser.add_argument('grid_source')

    def get(self):
        args = self.parser.parse_args()
        task_id = self.start_async.apply_async(
            args=(args.huc_8_num, args.huc_12_num, args.com_id_list, args.grid_source), queue="qed")
        # task_id = self.start_async(args.huc_8_num, args.huc_12_num, args.com_id_num, args.com_id_list)
        return Response(json.dumps({'job_id': task_id.id}))

    @celery.task(name='hms_nldas_grid', bind=True)
    def start_async(self, huc_8_id, huc_12_id, com_id_list, grid_source):
        task_id = celery.current_task.request.id
        logging.info("task_id: {}".format(task_id))
        logging.info("hms_controller.NLDASGridCells starting calculation...")
        logging.info("inputs id: {}, {}, {}, {}".format(huc_8_id, huc_12_id, com_id_list, grid_source))
        if huc_8_id:
            catchment_cells = CatchmentGrid.getIntersectCellsInHuc8(huc_8_id, grid_source)
        elif huc_12_id:
            catchment_cells = CatchmentGrid.getIntersectCellsInHuc12(huc_12_id, grid_source)
        elif com_id_list:
            catchment_cells = CatchmentGrid.getIntersectCellsInComlist(com_id_list, grid_source)
        else:
            catchment_cells = {}
        logging.info("hms_controller.NLDASGridCells calcuation completed.")
        logging.info("Adding data to mongoDB...")
        mongo_db = connect_to_mongoDB("hms")
        posts = mongo_db["data"]
        time_stamp = datetime.utcnow()
        data = {'_id': task_id, 'date': time_stamp, 'data': catchment_cells}
        posts.insert_one(data)


class Hydrodynamics(Resource):
    """

    """

    parser = parser_base.copy()
    parser.add_argument('submodel')  # add to django request
    parser.add_argument('startDate')
    parser.add_argument('endDate')
    parser.add_argument('timestep')
    parser.add_argument('segments')
    parser.add_argument('boundary_flow')

    def post(self):
        use_celery = False
        args = self.parser.parse_args()
        if args.startDate is None or args.endDate is None:
            return Response("{'input error':'Arguments startDate and endDate are required.")
        if use_celery:
            if args.submodel is 'constant_volume':
                job_id = self.CV_start_async.apply_async(
                    args=(args.startDate, args.endDate, args.timestep, args.boundary_flow, args.segments),
                    queue="qed")  # DO STUFF with args, validation
                return Response(json.dumps({'job_id': job_id.id}))  # return task_id
        else:
            data = FlowRouting(startDate=args.startDate, endDate=args.endDate, timestep=args.timestep,
                               boundary_flow=args.boundary_flow, segments=args.segments)
            if args.submodel == 'constant_volume':
                result = data.constant_volume()
            # elif args.submodel == 'changing_volume':
            #     result = data.changing_volume()
            # elif args.submodel == 'kinematic_wave':
            #     result = data.kinematic_wave()
            else:
                return Response(
                    "{'input error':'invalid model type. Must be constant_volume, changing_volume, or kinematic_wave.'}")
            return Response(json.dumps({"data": result}))  # return task_id

        @celery.task(name='hms_constant_volume', bind=True)
        def CV_start_async(self, startDate, endDate, timestep, boundary_flow, segments):
            task_id = celery.current_task.request.id
            logging.info("task_id: {}".format(task_id))
            logging.info("hms_controller.Hydrodynamics starting model...")
            cv = FlowRouting(startDate, endDate, timestep, boundary_flow, segments)
            result = cv.constant_volume()  # one for each alg
            logging.info("Adding data to mongoDB...")
            mongo_db = connect_to_mongoDB()
            posts = mongo_db.posts
            time_stamp = datetime.utcnow()
            data = {'_id': task_id, 'date': time_stamp, 'data': result}
            posts.insert_one(data)


class ProxyDNC2(Resource):
    """

    """
    def post(self, model=None):
        request_url = model + "/"
        request_body = request.json
        logging.info("Sending task to celery for: {}".format(request_url))
        job_id = self.request_to_service.apply_async(args=(request_url, request_body), queue="qed")
        # job_id = self.request_to_service(args=(request_url, request_body), queue="qed")
        return Response(json.dumps({'job_id': job_id.id}))

    @celery.task(name='hms_dotnetcore2_request', bind=True)
    def request_to_service(self, request_url, request_body):
        task_id = celery.current_task.request.id
        if task_id is None:
            task_id = uuid.uuid4()
        logging.info("task_id: {}".format(task_id))
        request_body["taskID"] = task_id
        logging.info("HMS_LOCAL: {}".format(os.environ['HMS_LOCAL']))
        if os.environ['IN_DOCKER'] == "False":
            #proxy_url = "http://localhost:5000/api/" + request_url
            proxy_url = "http://localhost:60050/api/" + request_url
        else:
            proxy_url = str(os.environ.get('HMS_BACKEND_SERVER_INTERNAL')) + "/api/" + request_url
        logging.info("Proxy sending request to dotnetcore2 container. Request url: {}".format(proxy_url))
        logging.info("Request body: {}".format(json.dumps(request_body)))
        request_data = requests.post(proxy_url, json=request_body)
        logging.info("Proxy data recieved from dotnetcore2 container.")
        json_data = json.loads(request_data.text)
        # json_data["metaData"]["job_id"] = task_id
        mongo_db = connect_to_mongoDB("hms")
        db = mongo_db["data"]
        time_stamp = datetime.utcnow()
        data = {'_id': task_id, 'date': time_stamp, 'data': json.dumps(json_data)}
        db.insert_one(data)


class NWMDataShortTerm(Resource):
    """
    NWM Data retriever
    """
    parser = parser_base.copy()
    parser.add_argument('comid')
    parser.add_argument('simdate')

    def get(self):
        args = self.parser.parse_args()
        comid = [int(c) for c in str(args.comid).split(",")]
        if comid is None or len(comid) == 0:
            return Response(json.dumps({'ERROR': 'comid value does not exist or is invalid. COMID: {}'.format(comid)}))
        dt_format = "%Y-%m-%d:%H"
        dt = args.simdate
        if dt is None:
            dt = datetime.now().strftime("%Y-%m-%d %H")
        else:
            dt = datetime.strptime(dt, dt_format).strftime("%Y-%m-%d %H")
        job_id = self.get_data.apply_async(args=(comid, dt,), queue="qed")
        return Response(json.dumps({'job_id': job_id.id}))

    @celery.task(name="nwm data - short term request", bind=True)
    def get_data(self, comid, dt):
        job_id = celery.current_task.request.id
        if job_id is None:
            job_id = uuid.uuid4()
        logging.info("NWM short term forecast data call started. ID: {}".format(job_id))
        nwm = NWMForecastData(dt=dt)
        nwm.download_data()
        comid_data = {}
        for c in comid:
            json_data = nwm.generate_timeseries(c)
            comid_data[str(c)] = json_data
        mongo_db = connect_to_mongoDB("hms")
        db = mongo_db["data"]
        time_stamp = datetime.utcnow()
        data = {'_id': job_id, 'date': time_stamp, 'data': comid_data}
        db.insert_one(data)
        logging.info("NWM short term forecast data call completed. ID: {}".format(job_id))
