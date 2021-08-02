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

from celery_cgi import celery

# HMS Modules import
from .hms.ncdc_stations import NCDCStations
from .hms.percent_area import CatchmentGrid
from .hms.hydrodynamics import FlowRouting
from .hms.nwm_reanalysis import NWM
from .hms.nwm_data import NWMData
from .hms.nwm_forecast import NWMForecastData
from .hms.workflow_manager import WorkflowManager, MongoWorkflow


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
    if database == 'flask_hms':
        mongo.flask_hms.Collection.create_index([("date", pymongo.DESCENDING)], expireAfterSeconds=86400)
        # ALL entries into mongo.flask_hms must have datetime.utcnow() timestamp, which is used to delete the record after 86400 seconds, 24 hours.
    elif database == 'nwm_data':
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
                    if isinstance(posts_data['data'], str):
                        data = json.loads(posts_data['data'])
                    elif isinstance(posts_data['data'], dict):
                        data = posts_data['data']
                    else:
                        data = None
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


class HMSRevokeTask(Resource):
    parser = parser_base.copy()
    parser.add_argument('task_id')

    def get(self):
        args = self.parser.parse_args()
        task_id = args.task_id
        try:
            celery.control.revoke(task_id, terminate=True, signal='SIGKILL')
            message = f"Successfully cancelled task: {task_id}"
        except Exception as e:
            message = f"Error attempting to cancel task: {task_id}, message: {e}"
        logging.info(message)
        return Response(json.dumps({'task_id': task_id, 'result': message}))


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
    parser.add_argument('timestep')

    def get(self):
        args = self.parser.parse_args()
        task_id = self.start_async.apply_async(
            args=(args.dataset, args.comid, args.startDate, args.endDate, args.timestep), queue="qed")
        return Response(json.dumps({'job_id': task_id.id}))

        # task_id = str(uuid.uuid4())
        # self.start_async(args.dataset, args.comid, args.startDate, args.endDate, uuid=task_id)
        # return Response(json.dumps({'job_id': task_id}))

    @celery.task(name='hms_nwm_data', bind=True)
    def start_async(self, dataset, comid, startDate, endDate, uuid=None):
        if uuid:
            task_id = uuid
        else:
            task_id = celery.current_task.request.id
        comids = comid.split(",")
        logging.info("task_id: {}".format(task_id))
        logging.info("hms_controller.NWM download starting...")
        logging.info("inputs id: {}, {}, {}, {}".format(dataset, comids, startDate, endDate))
        time0 = time.time()
        try:
            nwm = NWM(start_date=startDate, end_date=endDate, comids=comids)
            nwm.request_timeseries()
            nwm.set_output()
        except Exception as e:
            logging.warning(f"Error attempting to retrieve NWM data: {e}")
            return
        time1 = time.time()
        logging.info("NWM timeseries runtime: {} sec ".format((round(time1-time0, 4))))
        logging.info("hms_controller.NWM download completed.")
        logging.info("Adding data to mongoDB...")
        mongo_db = connect_to_mongoDB("hms")
        posts = mongo_db["data"]
        time_stamp = datetime.utcnow()
        data = {'_id': task_id, 'date': time_stamp, 'data': nwm.output.to_dict()}
        query = {'_id': task_id}
        exists = posts.find_one(query)
        if exists:
            posts.replace(query, data)
        else:
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
            if args.submodel == 'constant_volume':
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


class HMSWorkflow(Resource):
    """
    HMS WorkflowManager
    """
    parser = parser_base.copy()
    parser.add_argument('sim_input', type=dict)
    parser.add_argument('comid_inputs', type=dict)
    parser.add_argument('network', type=dict)
    parser.add_argument("simulation_dependencies")
    parser.add_argument("catchment_dependencies", type=dict)

    # def post(self):
    #     json_data = request.get_json(force=True)
    #     sim_input = json_data["sim_input"]
    #     comid_inputs = json_data["comid_inputs"]
    #     network = json_data["network"]
    #     simulation_dependencies = json_data["simulation_dependencies"]
    #     catchment_dependencies = json_data["catchment_dependencies"]
    #
    #     sim_id = str(uuid.uuid4())
    #     # output = self.execute_workflow.apply_async(args=(sim_id, sim_input, comid_inputs, network,
    #     #                                                  simulation_dependencies, catchment_dependencies),
    #     #                                            task_id=sim_id, queue='qed')
    #     return Response(json.dumps({'job_id': sim_id}))

    # @celery.task(name="HMS Workflow Manager", bind=True)
    # def execute_workflow(self, task_id, sim_input, comid_inputs, network, simulation_dependencies, catchment_dependencies):
    #     debug = False
    #     local = False
    #     logging.debug("Starting HMS WorkflowManager. ID: {}".format(task_id))
    #     workflow = WorkflowManager(task_id=task_id, sim_input=sim_input,
    #                                order=network["order"], sources=network["sources"],
    #                                local=local, debug=debug)
    #     print(f"SIM DEP TYPE: {type(simulation_dependencies)}, SIM DEP: {simulation_dependencies}")
    #     workflow.define_presim_dependencies(simulation_dependencies)
    #     workflow.construct(catchment_inputs=comid_inputs, catchment_dependencies=catchment_dependencies)
    #     workflow.compute()
    #     logging.debug("HMS WorkflowManager simulation completed. ID: {}".format(task_id))

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
                    output = self.execute_sim_workflow.apply_async(args=(sim_id,), task_id=sim_id, queue='qed')
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
                        MongoWorkflow.kill_simulation(sim_id=sim_id)
                return MongoWorkflow.get_status(task_id=sim_id)
            else:
                return Response(json.dumps({"error": "No simulation taskid provided. Requires argument 'sim_id'"}))

        @celery.task(name="HMS Sim Workflow Manager", bind=True)
        def execute_sim_workflow(self, task_id):
            logging.info("Starting HMS WorkflowManager. ID: {}".format(task_id))
            valid, workflow = WorkflowManager.load(sim_taskid=task_id)
            if valid == 0:
                MongoWorkflow.update_simulation_entry(simulation_id=task_id, status="FAILED", message=workflow)
            else:
                simulation_entry = MongoWorkflow.get_entry(task_id=task_id)
                simulation_dependencies = simulation_entry["dependencies"]
                workflow.define_presim_dependencies(simulation_dependencies)
                workflow.construct_from_db(catchment_ids=simulation_entry["catchments"])
                workflow.compute()
                logging.info("HMS WorkflowManager simulation created and executed. ID: {}".format(task_id))

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
            wk_entry = MongoWorkflow.get_entry(task_id=args.task_id)
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
                            cat_entry = MongoWorkflow.get_entry(task_id=catchment_id)
                            wk_zip.writestr(f"{comid}_input.json", data=cat_entry["input"])
                            if cat_entry["output"]:
                                wk_zip.writestr(f"{comid}_output.json", data=cat_entry["output"])
                            for dep, dep_id in cat_entry["dependencies"].items():
                                dep_name = f"{comid}-{dep}"
                                dep_entry = MongoWorkflow.get_entry(task_id=dep_id)
                                wk_zip.writestr(f"{dep_name}_input.json", data=json.dumps(dep_entry["input"]))
                                if dep_entry["output"]:
                                    wk_zip.writestr(f"{dep_name}_output.json", data=json.dumps(dep_entry["output"]))
                        wk_data = MongoWorkflow.get_entry(task_id=args.task_id)
                        for dep_id in wk_data["dependencies"]:
                            dep_entry = MongoWorkflow.get_entry(task_id=dep_id)
                            dep_name = f"workflow-{dep_entry['name']}"
                            wk_zip.writestr(f"{dep_name}_input.json", data=json.dumps(dep_entry["input"]))
                            if dep_entry["output"]:
                                wk_zip.writestr(f"{dep_name}_output.json", data=json.dumps(dep_entry["output"]))
                elif wk_entry["type"] == "catchment":
                    cat_entry = wk_entry
                    sim_entry = MongoWorkflow.get_entry(task_id=cat_entry["sim_id"])
                    comid = None
                    for c, catchment_id in sim_entry["catchments"].items():
                        if catchment_id == args.task_id:
                            comid = c
                            break
                    file_name = f"{comid}_{args.task_id}.zip"
                    with zipfile.ZipFile(file_out, "w", compression=zipfile.ZIP_DEFLATED) as wk_zip:
                        wk_zip.writestr(f"{comid}_input.json", data=cat_entry["input"])
                        if cat_entry["output"]:
                            wk_zip.writestr(f"{comid}_output.json", data=cat_entry["output"])
                        for dep, dep_id in cat_entry["dependencies"].items():
                            dep_name = f"{comid}-{dep}"
                            dep_entry = MongoWorkflow.get_entry(task_id=dep_id)
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
