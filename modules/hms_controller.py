from flask import Response
from flask_restful import Resource, reqparse
import pymongo as pymongo
from datetime import datetime
import logging
import json
import os

from celery_cgi import celery

# HMS Modules import
from .hms.ncdc_stations import NCDCStations
from .hms.percent_area import CatchmentGrid

IN_DOCKER = os.environ.get("IN_DOCKER")


def connect_to_mongoDB():
    if IN_DOCKER == "False":
        # Dev env mongoDB
        mongo = pymongo.MongoClient(host='mongodb://localhost:27017/0')
    else:
        # Production env mongoDB
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
                data = json.loads(posts.find_one({'_id': task_id})['data'])
                return Response(json.dumps({'id': task.id, 'status': task.status, 'data': data}))
            else:
                return Response(json.dumps({'id': task.id, 'status': task.status}))
        else:
            return Response(json.dumps({'error': 'id provided is invalid.'}))


class HMSFlaskTest(Resource):

    def get(self):
        test_id = self.run_test.apply_async(queue="qed")
        return Response(json.dumps({'job_id': test_id.id}))

    @celery.task(name="hms_flask_test", bind=True, ignore_result=False)
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

    @celery.task(name='hms_ncdc_stations', bind=True, ignore_result=False)
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
        return Response(json.dumps({'job_id': task_id.id}))

    @celery.task(name='hms_nldas_grid', bind=True, ignore_result=False)
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
