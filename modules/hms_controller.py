from flask import Response
from flask_restful import Resource, reqparse
from celery import Celery
from pymongo import MongoClient
import redis
import logging
import json
import os

# HMS Modules import
from .hms.ncdc_stations import NCDCStations
from .hms.percent_area import CatchmentGrid

# envi setup
from temp_config.set_environment import DeployEnv
runtime_env = DeployEnv()
runtime_env.load_deployment_environment()

redis_hostname = os.environ.get('REDIS_HOSTNAME')
redis_port = os.environ.get('REDIS_PORT')
REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME')
redis_conn = redis.StrictRedis(host=REDIS_HOSTNAME, port=6379, db=0)

if not os.environ.get('REDIS_HOSTNAME'):
    os.environ.setdefault('REDIS_HOSTNAME', 'redis')
    REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME')

logging.info("REDIS HOSTNAME: {}".format(REDIS_HOSTNAME))

celery = Celery('flask_qed', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0', include=['flask_qed.hms_flask'])
# celery = Celery('flask_qed', broker='redis://redis:6379/0', backend='redis://redis:6379/0', include=['flask_qed.hms_flask'])

mongo = MongoClient('localhost', 27017)
mongo_db = mongo['flask_hms']

parser = reqparse.RequestParser()


class NCDCStationsInGeojson(Resource):
    """
    Controller class for getting all ncdc stations within a provided geometry, as geojson, and a date range.
    """
    parser.add_argument('geometry')
    parser.add_argument('startDate')
    parser.add_argument('endDate')
    parser.add_argument('crs')

    def post(self):
        args = parser.parse_args()
        if args.startDate is None or args.endDate is None:
            return Response("{'input error':'Arguments startDate and endDate are required.")
        geojson = json.loads(args.geometry)
        job_id = self.start_async.apply_async(args=(geojson, args.startDate, args.endDate, args.crs), queue="qed")
        return Response(json.dumps({'job_id': job_id.id}))

    parser.add_argument('job_id')

    def get(self):
        args = parser.parse_args()
        task_id = args.job_id
        if task_id is not None:
            task = celery.AsyncResult(task_id)
            if task.status == "SUCCESS":
                posts = mongo_db.posts
                stations = json.loads(posts.find_one({'_id': task_id})['data'])
                return Response(json.dumps({'id': task.id, 'status': task.status, 'data': stations}))
            else:
                return Response(json.dumps({'id': task.id, 'status': task.status}))
        else:
            return Response(json.dumps({'error': 'id provided is invalid.'}))

    @celery.task(name='hms_tasks', bind=True, ignore_result=False)
    def start_async(self, geojson, start_date, end_date, crs):
        task_id = celery.current_task.request.id
        logging.info("task_id: {}".format(task_id))
        logging.info("hms_controller.NCDCStationsInGeojson starting search...")
        stations = NCDCStations.findStationsInGeoJson(geojson, start_date, end_date, crs)
        logging.info("hms_controller.NCDCStationsInGeojson search completed.")
        logging.info("Adding data to mongoDB...")
        posts = mongo_db.posts
        data = {'_id': task_id, 'data': stations}
        posts.insert_one(data)


class NLDASGridCells(Resource):
    """

    """
    parser.add_argument('huc_8_num')
    parser.add_argument('huc_12_num')
    parser.add_argument('com_id_num')

    def get(self):
        args = parser.parse_args()
        huc8 = args.huc_8_num
        huc12 = args.huc_12_num
        comid = args.com_id_num
        if huc8 and comid:
            catchment_cells = CatchmentGrid.getIntersectCellsInCatchment(huc8, comid)
            return Response(catchment_cells)
        elif huc12:
            catchment_cells = CatchmentGrid.getIntersectCellsInHuc12(huc12)
            return Response(catchment_cells)
