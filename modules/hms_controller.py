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

IN_DOCKER = os.environ.get("IN_DOCKER")


def connect_to_mongoDB():
    if IN_DOCKER == "False":
        # Dev env mongoDB
        logging.info("Connecting to mongoDB at: mongodb://localhost:27017/0")
        mongo = pymongo.MongoClient(host='mongodb://localhost:27017/0')
    else:
        # Production env mongoDB
        logging.info("Connecting to mongoDB at: mongodb://mongodb:27017/0")
        mongo = pymongo.MongoClient(host='mongodb://mongodb:27017/0')
    mongo_db = mongo['flask_hms']
    mongo.flask_hms.Collection.create_index([("date", pymongo.DESCENDING)], expireAfterSeconds=86400)
    # ALL entries into mongo.flask_hms must have datetime.utcnow() timestamp, which is used to delete the record after 86400
    # seconds, 24 hours.
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
                mongo_db = connect_to_mongoDB()
                posts = mongo_db.posts
                posts_data = posts.find_one({'_id': task_id})['data']
                data = json.loads(posts_data)
                return Response(json.dumps({'id': task.id, 'status': task.status, 'data': data}))
            else:
                return Response(json.dumps({'id': task.id, 'status': task.status}))
        else:
            return Response(json.dumps({'error': 'id provided is invalid.'}))


class HMSFlaskTest(Resource):

    def get(self):
        test_id = self.run_test.apply_async(queue="qed")
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


class NCDCStationsInGeojson(Resource):
    """
    Controller class for getting all ncdc stations within a provided geometry, as geojson, and a date range.
    """
    parser = parser_base.copy()
    parser.add_argument('geometry')
    parser.add_argument('startDate')
    parser.add_argument('endDate')
    parser.add_argument('crs')

    def post(self):
        args = self.parser.parse_args()
        if args.startDate is None or args.endDate is None:
            return Response("{'input error':'Arguments startDate and endDate are required.")
        geojson = json.loads(args.geometry)
        job_id = self.start_async.apply_async(args=(geojson, args.startDate, args.endDate, args.crs), queue="qed")
        return Response(json.dumps({'job_id': job_id.id}))

    @celery.task(name='hms_ncdc_stations', bind=True)
    def start_async(self, geojson, start_date, end_date, crs):
        task_id = celery.current_task.request.id
        logging.info("task_id: {}".format(task_id))
        logging.info("hms_controller.NCDCStationsInGeojson starting search...")
        stations = NCDCStations.findStationsInGeoJson(geojson, start_date, end_date, crs)
        logging.info("hms_controller.NCDCStationsInGeojson search completed.")
        logging.info("Adding data to mongoDB...")
        mongo_db = connect_to_mongoDB()
        posts = mongo_db.posts
        time_stamp = datetime.utcnow()
        data = {'_id': task_id, 'date': time_stamp, 'data': stations}
        posts.insert_one(data)


class NLDASGridCells(Resource):
    """

    """
    parser = parser_base.copy()
    parser.add_argument('huc_8_num')
    parser.add_argument('huc_12_num')
    parser.add_argument('com_id_num')

    def get(self):
        args = self.parser.parse_args()
        task_id = self.start_async.apply_async(args=(args.huc_8_num, args.huc_12_num, args.com_id_num), queue="qed")
        #task_id = self.start_async(args.huc_8_num, args.huc_12_num, args.com_id_num)
        return Response(json.dumps({'job_id': task_id.id}))

    @celery.task(name='hms_nldas_grid', bind=True)
    def start_async(self, huc_8_id, huc_12_id, com_id):
        task_id = celery.current_task.request.id
        logging.info("task_id: {}".format(task_id))
        logging.info("hms_controller.NLDASGridCells starting calculation...")
        if huc_8_id and com_id:
            catchment_cells = CatchmentGrid.getIntersectCellsInCatchment(huc_8_id, com_id)
        elif huc_12_id:
            catchment_cells = CatchmentGrid.getIntersectCellsInHuc12(huc_12_id)
        else:
            catchment_cells = {}
        logging.info("hms_controller.NLDASGridCells calcuation completed.")
        logging.info("Adding data to mongoDB...")
        mongo_db = connect_to_mongoDB()
        posts = mongo_db.posts
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
            elif args.submodel == 'changing_volume':
                result = data.changing_volume()
            elif args.submodel == 'kinematic_wave':
                result = data.kinematic_wave()
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
        job_id = self.request_to_service.apply_async(args=(request_url, request_body), queue="qed")
        return Response(json.dumps({'job_id': job_id.id}))

    @celery.task(name='hms_dotnetcore2_request', bind=True)
    def request_to_service(self, request_url, request_body):
        task_id = celery.current_task.request.id
        if task_id is None:
            task_id = uuid.uuid4()
        logging.info("task_id: {}".format(task_id))
        if os.environ['HMS_LOCAL'] == "True":
            proxy_url = "http://localhost:60050/api/" + request_url
        else:
            proxy_url = str(os.environ.get('HMS_BACKEND_SERVER_INTERNAL')) + "/api/" + request_url
        logging.info("Proxy sending request to dotnetcore2 container. Request url: {}".format(proxy_url))
        request_data = requests.post(proxy_url, json=request_body)
        logging.info("Proxy data recieved from dotnetcore2 container.")
        mongo_db = connect_to_mongoDB()
        posts = mongo_db.posts
        time_stamp = datetime.utcnow()
        data = {'_id': task_id, 'date': time_stamp, 'data': request_data.text}
        posts.insert_one(data)
