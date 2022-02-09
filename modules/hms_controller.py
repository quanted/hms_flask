import io

from flask import Response, send_file
from flask_restful import Resource, reqparse, request, Api
import pymongo as pymongo
from datetime import datetime
from io import BytesIO
import time
import uuid
import requests
import logging
import json
import os
import zipfile
import hashlib

import dask
from dask.distributed import Client, LocalCluster, fire_and_forget


# HMS Modules import
from .hms.ncdc_stations import NCDCStations
from .hms.percent_area import CatchmentGrid
from .hms.hydrodynamics import FlowRouting
from .hms.nwm_reanalysis import NWM
from .hms.nwm_data import NWMData
from .hms.nwm_forecast import NWMForecastData
from .hms.workflow_manager import WorkflowManager, MongoWorkflow


IN_DOCKER = os.environ.get("IN_DOCKER")
USE_DASK = os.getenv("DASK", "True") == "True"
NWM_TASK_COUNT = 0


def connect_to_mongoDB(database=None):
    mongodb_host = os.getenv("MONGODB", "mongodb://localhost:27017/0")
    if database is None:
        database = 'flask_hms'
    if IN_DOCKER == "False":
        # Dev env mongoDB
        logging.info(f"Connecting to mongoDB at: {mongodb_host}")
        mongo = pymongo.MongoClient(host=mongodb_host)
    else:
        # Production env mongoDB
        logging.info(f"Connecting to mongoDB at: {mongodb_host}")
        mongo = pymongo.MongoClient(host=mongodb_host)
    mongo_db = mongo[database]
    if database == 'flask_hms':
        mongo.flask_hms.Collection.create_index([("date", pymongo.DESCENDING)], expireAfterSeconds=86400)
        # ALL entries into mongo.flask_hms must have datetime.utcnow() timestamp, which is used to delete the record after 86400 seconds, 24 hours.
    elif database == 'nwm_data':
        mongo.nwm_data.Collection.create_index([("date", pymongo.DESCENDING)], expireAfterSeconds=64800)    # 18 hr
    else:
        mongo[database].Collection.create_index([("date", pymongo.DESCENDING)], expireAfterSeconds=604800)
        # Set expiration to be 1 week
    return mongo_db


def save_status(task_id, status, message=None, data=None, hash=None):
    mongo_db = connect_to_mongoDB("hms")
    db = mongo_db["data"]
    posts = db.posts
    db_record = dict(posts.find_one({"_id": task_id}))
    time_stamp = datetime.utcnow()
    if len(db_record):          # existing entry, update
        db_record["status"] = status
        db_record["date"] = str(time_stamp)
        if message is not None:
            db_record["message"] = message
        if data is not None:
            db_record["data"] = json.dumps(data)
        if hash is not None:
            db_record["hash"] = hash
        posts.replace_one({"_id": task_id}, db_record)
        logging.info(f"MongoDB - Updated record, ID: {task_id}, status: {status}")
    else:                       # new entry
        new_record = {"_id": task_id, "status": status, "date": str(time_stamp)}
        if message is not None:
            new_record["message"] = message
        if data is not None:
            new_record["data"] = json.dumps(data)
        if hash is not None:
            new_record["hash"] = hash
        posts.insert_one(new_record)
        logging.info(f"MongoDB - New record created, ID: {task_id}, status: {status}")


def get_dask_client():
    if not USE_DASK:
        return None
    if IN_DOCKER == "True":
        scheduler_name = os.getenv('DASK_SCHEDULER', "hms-dask-scheduler:8786")
        logging.info(f"Dask Scheduler: {scheduler_name}")
        dask_client = Client(scheduler_name)
    else:
        logging.info("Dask Scheduler: Local Cluster")
        scheduler = LocalCluster(processes=False)
        dask_client = Client(scheduler)
    return dask_client


def task_status(task_id):
    mongo_db = connect_to_mongoDB("hms")
    db = mongo_db["data"]
    posts = db.posts
    db_record = dict(posts.find_one({"_id": task_id}))
    if len(db_record) == 0:
        return {"job_id": task_id, "status": "NA", "data": f"No task found for id: {task_id}"}
    message = db_record["message"] if "message" in db_record else ""
    if db_record["status"] == "SUCCESS":
        data = json.loads(db_record.get("data", ""))
        return {"job_id": task_id, "status": "SUCCESS", "data": data, "message": message}
    else:
        return {"job_id": task_id, "status": db_record["status"], "message": message}


def check_hash(hash):
    mongo_db = connect_to_mongoDB("hms")
    db = mongo_db["data"]
    posts = db.posts
    db_record = dict(posts.find_one({"hash": hash}))
    if len(db_record) == 0:
        return None
    else:
        return db_record["_id"]


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
            task_data = task_status(task_id=task_id)
            return Response(json.dumps(task_data))
        else:
            return Response(json.dumps({'error': 'No id provided.'}))


class HMSFlaskTest(Resource):

    def get(self):
        task_id = uuid.uuid4()
        save_status(task_id=task_id, status="PENDING", message="Flask-Dask Test")
        dask_client = get_dask_client()
        test_task = dask_client.submit(HMSFlaskTest.run_test, task_id, key=task_id)
        fire_and_forget(test_task)
        logging.info("Sending test task to Dask, ID: {task_id}")
        return Response({'job_id': task_id})

    @staticmethod
    def run_test(task_id):
        save_status(task_id=task_id, status="STARTED")
        logging.info(f"Starting test task on Dask, ID: {task_id}")
        time_stamp = datetime.utcnow()
        data = {"request_time": str(time_stamp)}
        save_status(task_id=task_id, status="SUCCESS", data=data)
        logging.info("Completed test task on Dask, ID: {task_id}")


class HMSRevokeTask(Resource):
    parser = parser_base.copy()
    parser.add_argument('task_id')

    def get(self):
        args = self.parser.parse_args()
        task_id = args.task_id
        task_data = task_status(task_id=task_id)
        message = ""
        if task_data["status"] == "NA":
            message = f"Error no task found for ID: {task_id}"
            logging.info(message)
            try:
                dask_client = get_dask_client()
                dask_task = dask.distributed.Future(key=task_id, client=dask_client)
                dask_task.cancel()
                message = f"Successfully cancelled task: {task_id}"
                save_status(task_id=task_id, status="CANCELLED", message=message)
            except Exception as e:
                message = f"Error attempting to cancel task: {task_id}, message: {e}"
            logging.info(message)
        return Response(json.dumps({'job_id': task_id, 'data': message}))


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

        task_id = uuid.uuid4()
        save_status(task_id=task_id, status="PENDING", message="ncdc-station-search")
        dask_client = get_dask_client()
        test_task = dask_client.submit(NCDCStationSearch.station_search, task_id,
                                       geojson, args.startDate, args.endDate, args.crs, args.latitude, args.longitude, args.comid,
                                       key=task_id)
        fire_and_forget(test_task)
        return Response(json.dumps({'job_id': task_id}))

    def get(self):
        args = self.parser.parse_args()
        if args.startDate is None or args.endDate is None:
            return Response("{'input error':'Arguments startDate and endDate are required.")

        task_id = uuid.uuid4()
        save_status(task_id=task_id, status="PENDING", message="ncdc-station-search")
        dask_client = get_dask_client()
        test_task = dask_client.submit(NCDCStationSearch.station_search, task_id,
                                       None, args.startDate, args.endDate, args.crs, args.latitude, args.longitude, args.comid,
                                       key=task_id)
        fire_and_forget(test_task)
        return Response(json.dumps({'job_id': task_id}))

    @staticmethod
    def station_search(task_id, geojson, start_date, end_date, crs, latitude=None, longitude=None, comid=None):
        save_status(task_id=task_id, status="STARTED")
        logging.info(f"Starting NCDC station search task, ID: {task_id}")
        if geojson:
            stations = NCDCStations.findStationsInGeoJson(geojson, start_date, end_date, crs)
        elif comid:
            stations = NCDCStations.findStationFromComid(comid, start_date, end_date)
        else:
            stations = NCDCStations.findStationFromPoint(latitude, longitude, start_date, end_date)
        save_status(task_id=task_id, status="SUCCESS", data=stations)
        logging.info(f"Completed NCDC station search task, ID: {task_id}")


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
    parser.add_argument('timestep')

    def get(self):
        args = self.parser.parse_args()

        hash = hashlib.md5(json.dumps(
            {'dataset': args.dataset, 'comids': args.comid, 'startDate': args.startDate, 'endDate': args.endDate},
            sort_keys=True).encode()).hexdigest()
        exists = check_hash(hash=hash)
        if exists:
            if len(exists["data"]["data"]) > 0:
                return Response(json.dumps({'job_id': exists["_id"]}))

        task_id = str(uuid.uuid4())
        save_status(task_id=task_id, status="PENDING", message="nwm data download")
        dask_client = get_dask_client()
        test_task = dask_client.submit(NWMDownload.get_data, task_id, args.dataset, args.comid, args.startDate, args.endDate,
                                       args.timestep, key=task_id)
        fire_and_forget(test_task)
        return Response(json.dumps({'job_id': task_id}))

    @staticmethod
    def get_data(task_id, dataset, comid, startDate, endDate):
        comids = comid.split(",")
        save_status(task_id=task_id, status="STARTED")
        logging.info(f"Starting NWM download task, ID: {task_id}")
        time0 = time.time()
        try:
            nwm = NWM(start_date=startDate, end_date=endDate, comids=comids)
            nwm.request_timeseries()
            nwm.set_output()
        except Exception as e:
            logging.warning(f"Error attempting to retrieve NWM data, ID: {task_id}, error: {e}")
            save_status(task_id=task_id, status="FAILURE", message=str(e))
            return
        time1 = time.time()
        hash = hashlib.md5(json.dumps(
            {'dataset': dataset, 'comids': comid, 'startDate': startDate, 'endDate': endDate},
            sort_keys=True).encode()).hexdigest()
        save_status(task_id=task_id, status="SUCCESS", data=nwm.output.to_dict(), hash=hash)
        logging.info(f"Completed NWM download task, ID: {task_id}, runtime: {round(time1-time0, 4)}")


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

        task_id = str(uuid.uuid4())
        save_status(task_id=task_id, status="PENDING", message="nldas grid cells task")
        dask_client = get_dask_client()
        test_task = dask_client.submit(NLDASGridCells.get_data,
                                       task_id, args.huc_8_num, args.huc_12_num, args.com_id_list, args.grid_source,
                                       key=task_id)
        fire_and_forget(test_task)
        return Response(json.dumps({'job_id': task_id}))

    @staticmethod
    def get_data(task_id, huc_8_id, huc_12_id, com_id_list, grid_source):
        save_status(task_id=task_id, status="STARTED")
        logging.info(f"Started NLDAS grid cells task, ID: {task_id}")
        if huc_8_id:
            catchment_cells = CatchmentGrid.getIntersectCellsInHuc8(huc_8_id, grid_source)
        elif huc_12_id:
            catchment_cells = CatchmentGrid.getIntersectCellsInHuc12(huc_12_id, grid_source)
        elif com_id_list:
            catchment_cells = CatchmentGrid.getIntersectCellsInComlist(com_id_list, grid_source)
        else:
            catchment_cells = {}
        save_status(task_id=task_id, status="SUCCESS", data=catchment_cells)
        logging.info(f"Completed NLDAS grid cells task, ID: {task_id}")


class ProxyDNC2(Resource):
    """
    Pass through proxy to the hms_dotnetcore REST api, used to store response data in mongodb
    """
    def post(self, model=None):
        request_url = model + "/"
        request_body = request.json

        task_id = str(uuid.uuid4())
        save_status(task_id=task_id, status="PENDING", message="HMS REST API proxy request task")
        dask_client = get_dask_client()
        test_task = dask_client.submit(ProxyDNC2.proxy_request,
                                       task_id, request_url, request_body,
                                       key=task_id)
        fire_and_forget(test_task)
        return Response(json.dumps({'job_id': task_id}))

    @staticmethod
    def proxy_request(task_id, request_url, request_body):
        save_status(task_id=task_id, status="STARTED")
        logging.info(f"Started HMS REST proxy task, ID: {task_id}")
        if os.environ['IN_DOCKER'] == "False":
            proxy_url = "http://localhost:60050/api/" + request_url
        else:
            proxy_url = os.getenv('HMS_BACKEND', "hms_dotnetcore:80") + "/api/" + request_url
        request_data = requests.post(proxy_url, json=request_body)
        json_data = json.loads(request_data.text)
        save_status(task_id=task_id, status="SUCCESS", data=json_data)
        logging.info(f"Completed HMS REST proxy task, ID: {task_id}")


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

        task_id = str(uuid.uuid4())
        save_status(task_id=task_id, status="PENDING", message="NWM Short-term forecast task")
        dask_client = get_dask_client()
        test_task = dask_client.submit(NWMDataShortTerm.get_data,
                                       task_id, comid, dt,
                                       key=task_id)
        fire_and_forget(test_task)
        return Response(json.dumps({'job_id': task_id}))

    @staticmethod
    def get_data(task_id, comid, dt):
        logging.info(f"Started NWM short term forecast data task. ID: {task_id}")
        save_status(task_id=task_id, status="STARTED")
        nwm = NWMForecastData(dt=dt)
        nwm.download_data()
        comid_data = {}
        for c in comid:
            json_data = nwm.generate_timeseries(c)
            comid_data[str(c)] = json_data
        save_status(task_id=task_id, status="SUCCESS", data=comid_data)
        logging.info(f"Completed NWM short term forecast data task. ID: {task_id}")


class HMSWorkflow(Resource):
    """
    HMS WorkflowManager
    """
    # parser = parser_base.copy()
    # parser.add_argument('sim_input', type=dict)
    # parser.add_argument('comid_inputs', type=dict)
    # parser.add_argument('network', type=dict)
    # parser.add_argument("simulation_dependencies")
    # parser.add_argument("catchment_dependencies", type=dict)

    class Simulation(Resource):
        sim_parser = parser_base.copy()
        sim_parser.add_argument('sim_id', type=str)
        sim_parser.add_argument('comid_input', type=dict)
        sim_parser.add_argument('network', type=dict)
        sim_parser.add_argument("simulation_dependencies", type=list)
        sim_parser.add_argument("catchment_dependencies", type=list)

        def post(self):
            args = request.get_json(force=True)
            new_sim = False
            if 'sim_id' in args.keys():
                sim_id = args['sim_id']
            else:
                new_sim = True
                sim_id = str(uuid.uuid4())
            if 'network' in args.keys():
                sim_deps = None
                if 'simulation_dependencies' in args.keys():
                    sim_deps = args['simulation_dependencies']
                if not new_sim:
                    check_sim = MongoWorkflow.get_entry(task_id=sim_id)
                    if check_sim["status"] == "IN-PROGRESS":
                        return Response(json.dumps({"error": "Unable to modify a currently running simulation."}))
                WorkflowManager.create_simulation(
                    sim_taskid=sim_id,
                    simulation_dependencies=sim_deps,
                    network=args['network'])
                logging.info(f"New simulation created with id: {sim_id}")
            elif new_sim:
                return Response(json.dumps({"error": "A network must be provided with a new simulation."}))
            valid = 1
            cat_id = None
            if 'comid_input' in args:
                cat_deps = None
                if 'catchment_dependencies' in args:
                    cat_deps = args['catchment_dependencies']
                valid, cat_id = WorkflowManager.create_catchment(
                    sim_taskid=sim_id,
                    catchment_input=args['comid_input']["input"],
                    comid=args['comid_input']["comid"],
                    dependencies=cat_deps
                )
                logging.info(f"New catchment added to simulation: {sim_id}, COMID: {args['comid_input']['comid']}")
            if valid == 1:
                return MongoWorkflow.get_status(task_id=sim_id)
            else:
                return Response(json.dumps({"error": cat_id}))

        def get(self):
            args = self.sim_parser.parse_args()
            if args.sim_id:
                sim_id = str(args.sim_id)
                try:
                    valid, msg = MongoWorkflow.simulation_run_ready(task_id=sim_id)
                except Exception as e:
                    logging.warning(f"No simulation found for execution with taskID: {sim_id}")
                    return Response(json.dumps({"error": f"No simulation found with taskID:{sim_id}"}))
                if valid == 0:
                    return Response(json.dumps({"error": str(msg)}))
                else:
                    logging.info(f"Executing HMS workflow simulation with ID: {sim_id}")
                    MongoWorkflow.set_sim_status(task_id=sim_id, status="PENDING")
                    dask_client = get_dask_client()
                    test_task = dask_client.submit(HMSWorkflow.Simulation.execute_sim_workflow, sim_id)
                    fire_and_forget(test_task)
                    return MongoWorkflow.get_status(task_id=sim_id)
            else:
                return Response(json.dumps({"error": "No simulation taskID provided. Requires argument 'sim_id'"}))

        def delete(self):
            args = self.sim_parser.parse_args()
            if args.sim_id:
                sim_id = str(args.sim_id)
                check_sim = MongoWorkflow.get_entry(task_id=sim_id)
                if check_sim:
                    if check_sim["type"] == "workflow" and check_sim["status"] in ("PENDING", "IN-PROGRESS",):
                        dask_client = get_dask_client()
                        test_task = dask_client.submit(HMSWorkflow.Simulation.cancel_workflow, sim_id)
                        fire_and_forget(test_task)
                return Response(json.dumps({"task_id": sim_id, "message": "Cancel request submitted."}))
            else:
                return Response(json.dumps({"error": "No simulation taskid provided. Requires argument 'sim_id'"}))

        @staticmethod
        def cancel_workflow(task_id):
            logging.info(f"HMS workflow cancel request for: {task_id}")
            MongoWorkflow.kill_simulation(sim_id=task_id)
            logging.info(f"HMS workflow cancellation completed for: {task_id}")

        @staticmethod
        def execute_sim_workflow(task_id):
            logging.info("Starting HMS Workflow simulation. ID: {}".format(task_id))
            valid, workflow = WorkflowManager.load(sim_taskid=task_id)
            if valid == 0:
                MongoWorkflow.update_simulation_entry(simulation_id=task_id, status="FAILED", message=workflow)
            else:
                simulation_entry = MongoWorkflow.get_entry(task_id=task_id)
                simulation_dependencies = simulation_entry["dependencies"]
                workflow.define_presim_dependencies(simulation_dependencies)
                workflow.construct_from_db(catchment_ids=simulation_entry["catchments"])
                workflow.compute()
                logging.info(f"HMS Workflow Manager simulation completed. ID: {task_id}")

    class Status(Resource):
        parser = parser_base.copy()
        parser.add_argument("task_id", location='args')

        def get(self):
            args = self.parser.parse_args()
            status = MongoWorkflow.get_status(task_id=args.task_id)
            return status

    class Data(Resource):
        parser = parser_base.copy()
        parser.add_argument("task_id", location='args')
        parser.add_argument("input", type=bool, default=False)
        parser.add_argument("output", type=bool, default=False)

        def get(self):
            args = self.parser.parse_args()
            data = MongoWorkflow.get_data(task_id=args.task_id)
            if data["type"] != "workflow":
                if not args.input:
                    del data["input"]
                if not args.output:
                    del data["output"]
            return data

    class Download(Resource):
        parser = parser_base.copy()
        parser.add_argument("task_id", location='args', required=True)

        def get(self):
            args = self.parser.parse_args()
            wk_entry = MongoWorkflow.get_data(task_id=args.task_id)
            if wk_entry:
                file_out = io.BytesIO()
                file_name = None
                if wk_entry["type"] == "workflow":
                    file_name = f"workflow_{args.task_id}.zip"
                    with zipfile.ZipFile(file_out, "w", compression=zipfile.ZIP_DEFLATED) as wk_zip:
                        wk_file_name = "workflow_details.json"
                        wk_file_data = MongoWorkflow.get_status(task_id=args.task_id)
                        wk_zip.writestr(wk_file_name, data=json.dumps(wk_file_data))
                        for comid, catchment_id in wk_entry["catchments"].items():
                            cat_entry = MongoWorkflow.get_data(task_id=catchment_id)
                            wk_zip.writestr(f"{comid}_input.json", data=json.dumps(cat_entry["input"]))
                            if cat_entry["output"]:
                                wk_zip.writestr(f"{comid}_output.json", data=json.dumps(cat_entry["output"]))
                            if type(cat_entry["dependencies"]) == dict:
                                for dep, dep_id in cat_entry["dependencies"].items():
                                    dep_name = f"{comid}-{dep}"
                                    dep_entry = MongoWorkflow.get_data(task_id=dep_id)
                                    wk_zip.writestr(f"{dep_name}_input.json", data=json.dumps(dep_entry["input"]))
                                    if dep_entry["output"]:
                                        wk_zip.writestr(f"{dep_name}_output.json", data=json.dumps(dep_entry["output"]))
                        wk_data = MongoWorkflow.get_data(task_id=args.task_id)
                        if type(wk_data["dependencies"]) == dict:
                            for dep_name, dep_id in wk_data["dependencies"].items():
                                dep_entry = MongoWorkflow.get_data(task_id=dep_id)
                                dep_name = f"workflow-{dep_entry['name']}"
                                wk_zip.writestr(f"{dep_name}_input.json", data=json.dumps(dep_entry["input"]))
                                if dep_entry["output"]:
                                    wk_zip.writestr(f"{dep_name}_output.json", data=json.dumps(dep_entry["output"]))
                elif wk_entry["type"] == "catchment":
                    cat_entry = wk_entry
                    sim_entry = MongoWorkflow.get_data(task_id=cat_entry["sim_id"])
                    comid = None
                    for c, catchment_id in sim_entry["catchments"].items():
                        if catchment_id == args.task_id:
                            comid = c
                            break
                    file_name = f"{comid}_{args.task_id}.zip"
                    with zipfile.ZipFile(file_out, "w", compression=zipfile.ZIP_DEFLATED) as wk_zip:
                        wk_zip.writestr(f"{comid}_input.json", data=json.dumps(cat_entry["input"]))
                        if cat_entry["output"]:
                            wk_zip.writestr(f"{comid}_output.json", data=json.dumps(cat_entry["output"]))
                        for dep, dep_id in cat_entry["dependencies"].items():
                            dep_name = f"{comid}-{dep}"
                            dep_entry = MongoWorkflow.get_data(task_id=dep_id)
                            wk_zip.writestr(f"{dep_name}_input.json", data=json.dumps(dep_entry["input"]))
                            if dep_entry["output"]:
                                wk_zip.writestr(f"{dep_name}_output.json", data=json.dumps(dep_entry["output"]))
                else:       # dependency
                    file_name = f"{wk_entry['name']}_{args.task_id}.zip"
                    with zipfile.ZipFile(file_out, "w", compression=zipfile.ZIP_DEFLATED) as wk_zip:
                        wk_zip.writestr(f"{wk_entry['name']}_input.json", data=json.dumps(wk_entry["input"]))
                        if wk_entry["output"]:
                            wk_zip.writestr(f"{wk_entry['name']}_output.json", data=json.dumps(wk_entry["output"]))
                file_out.seek(0)
                response = send_file(
                    BytesIO(file_out.read()),
                    mimetype='application/zip',
                    as_attachment=True,
                    download_name=file_name
                )
                response.headers["x-suggested-filename"] = file_name
                return response
            return Response(json.dumps({"error": f"No object found for task_id: {args.task_id}"}))
