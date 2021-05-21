import os
import uuid
import json
import copy
import logging
import pymongo
import datetime
import requests
import time
import dask
from dask.distributed import Client, LocalCluster


class MongoWorkflow:

    @staticmethod
    def connect_to_mongodb():
        in_docker = (os.getenv("IN_DOCKER", "False") == "True")
        database = 'hms_workflows'
        if not in_docker:
            # Dev env mongoDB
            logging.info("Connecting to mongoDB at: mongodb://localhost:27017/0")
            mongo = pymongo.MongoClient(host='mongodb://localhost:27017/0')
        else:
            # Production env mongoDB
            logging.info("Connecting to mongoDB at: mongodb://mongodb:27017/0")
            mongo = pymongo.MongoClient(host='mongodb://mongodb:27017/0')
        mongo_db = mongo[database]
        mongo[database].Collection.create_index([("date", pymongo.DESCENDING)], expireAfterSeconds=604800)
        return mongo_db

    @staticmethod
    def create_simulation_entry(simulation_id: str, simulation_input: dict = None, status: str = None,
                                catchments=None, order: list = None, sources: list = None, dependencies: dict = None):
        timestamp = datetime.datetime.now().isoformat(' ')
        mongo_db = MongoWorkflow.connect_to_mongodb()
        posts = mongo_db["data"]
        # Check if existing simulation exists with this id and delete existing (may need to change behavior)
        query = {'_id': simulation_id}
        data = posts.find_one(query)
        if data is not None:
            posts.delete_one(query)

        data = {
            "_id": simulation_id,
            "type": "workflow",
            "input": json.dumps(simulation_input),
            "status": status,
            "update_time": timestamp,
            "message": None,
            "catchments": catchments,
            "network_order": order,
            "catchment_sources": sources,
            "dependencies": dependencies,
        }
        posts.insert_one(data)

    @staticmethod
    def create_catchment_entry(simulation_id: str, catchment_id: str, catchment_input: dict = None, status: str = None,
                               upstream=None, dependencies: dict = None):
        timestamp = datetime.datetime.now().isoformat(' ')
        mongo_db = MongoWorkflow.connect_to_mongodb()
        posts = mongo_db["data"]
        data = {
            "_id": catchment_id,
            "type": "catchment",
            "sim_id": simulation_id,
            "input": json.dumps(catchment_input),
            "status": status,
            "update_time": timestamp,
            "message": None,
            "output": None,
            "upstream": upstream,
            "dependencies": dependencies,
            "runtime": None
        }
        posts.insert_one(data)

    @staticmethod
    def update_simulation_entry(simulation_id: str, status: str = None, message: str = None, timestamp: str = None):
        if not timestamp:
            timestamp = datetime.datetime.now().isoformat(' ')
        mongo_db = MongoWorkflow.connect_to_mongodb()
        posts = mongo_db["data"]
        query = {'_id': simulation_id}
        data = posts.find_one(query)
        data["update_time"] = timestamp
        if status:
            data["status"] = status
        if message:
            data["message"] = message
        posts.replace_one(query, data)

    @staticmethod
    def update_catchment_entry(catchment_id: str, status: str = None, message: str = None, output: dict = None,
                               timestamp: str = None, runtime: str = None):
        if not timestamp:
            timestamp = datetime.datetime.now().isoformat(' ')
        mongo_db = MongoWorkflow.connect_to_mongodb()
        posts = mongo_db["data"]
        query = {'_id': catchment_id}
        data = posts.find_one(query)
        data["update_time"] = timestamp
        if status:
            data["status"] = status
        if message:
            data["message"] = message
        if output:
            data["output"] = json.dumps(output)
        if runtime:
            data["runtime"] = runtime
        posts.replace_one(query, data)
        MongoWorkflow.update_simulation_entry(simulation_id=data["sim_id"], timestamp=timestamp)

    @staticmethod
    def prepare_inputs(simulation_id: str, catchment_inputs: dict, upstream: dict = None):
        mongo_db = MongoWorkflow.connect_to_mongodb()
        posts = mongo_db["data"]
        query = {'_id': simulation_id}
        simulation_entry = posts.find_one(query)
        valid = True
        message = []
        complete_inputs = {**json.loads(simulation_entry["input"]), **catchment_inputs}
        if not upstream:
            return complete_inputs, valid, ", ".join(message)
        for stream, stream_id in upstream.items():
            query = {'_id': stream_id}
            upstream_data = posts.find_one(query)
            if upstream_data["status"] != "COMPLETED":
                valid = False
                message.append(upstream_data["message"])
        return complete_inputs, valid, ", ".join(message)

    @staticmethod
    def completion_check(simulation_id: str):
        mongo_db = MongoWorkflow.connect_to_mongodb()
        posts = mongo_db["data"]
        query = {'_id': simulation_id}
        data = posts.find_one(query)
        status = []
        message = []
        for comid, task_id in data["catchments"].items():
            query = {'_id': task_id}
            catchment_data = posts.find_one(query)
            if catchment_data["status"] == "FAILED":
                message.append(catchment_data["message"])
            status.append(catchment_data["status"])
        if "IN-PROGRESS" in status or "PENDING" in status:
            s = "IN-PROGRESS"
        elif "FAILED" in status and "COMPLETED" in status:
            s = "INCOMPLETE"
        elif "FAILED" not in status:
            s = "COMPLETED"
        else:
            s = "FAILED"
        return s, ", ".join(message)

    @staticmethod
    def get_status(task_id: str):
        mongo_db = MongoWorkflow.connect_to_mongodb()
        posts = mongo_db["data"]
        query = {'_id': task_id}
        data = posts.find_one(query)
        if data is None:
            return {"error": f"No data found for a task with id: {task_id}"}
        if "input" in data:
            del data["input"]
        if data["type"] == "workflow":
            catchments = {}
            for catchment, c_id in data["catchments"].items():
                query = {'_id': c_id}
                c_data = posts.find_one(query)
                catchments[catchment] = {
                    "status": c_data["status"],
                    "task_id": c_id,
                    "message": c_data["message"],
                    "update_time": c_data["update_time"],
                    "dependencies": c_data["dependencies"]
                }
            data["catchments"] = catchments
        else:
            del data["output"]
        return data

    @staticmethod
    def get_data(task_id: str):
        mongo_db = MongoWorkflow.connect_to_mongodb()
        posts = mongo_db["data"]
        query = {'_id': task_id}
        data = posts.find_one(query)
        if data is None:
            return {"error": f"No data found for a task with id: {task_id}"}
        if data["type"] == "workflow":
            data["input"] = json.loads(data["input"])
            return data
        elif data["type"] == "dependency":
            return data
        else:
            data["input"] = json.loads(data["input"])
            data["output"] = json.loads(data["output"])
            return data

    @staticmethod
    def dump_data(task_id: str, data, name: str, data_type: str = "dependency"):
        timestamp = datetime.datetime.now().isoformat(' ')
        mongo_db = MongoWorkflow.connect_to_mongodb()
        posts = mongo_db["data"]
        data = {
            "_id": task_id,
            "type": data_type,
            "name": name,
            "data": data,
            "save_time": timestamp
        }
        posts.insert_one(data)


class WorkflowManager:

    def __init__(self, task_id: str, sim_input: dict, sources: list, order: list, local: bool = False,
                 debug: bool = False):
        self.local = local
        self.debug = debug
        self.scheduler = None
        if self.local:
            try:
                client = Client("tcp://127.0.0.1:8786", timeout=2)
                logging.info("Dask Client connected to existing local cluster at tcp://127.0.0.1:8786")
                self.scheduler = client.scheduler
            except Exception as e:
                self.scheduler = LocalCluster()
                client = Client(self.scheduler)
                logging.debug(f"Dask Client connected to new local cluster at {self.scheduler}")
        else:
            scheduler = os.getenv('DASK_SCHEDULER', "127.0.0.1:8786")
            if debug:
                self.scheduler = "127.0.0.1:8786"
            logging.debug(f"Dask Client connecting to existing cluster at {self.scheduler}")
            client = Client(self.scheduler)
        self.task_id = task_id
        self.sim_input = sim_input
        self.catchments = {}
        self.sources = sources
        self.order = order
        self.pourpoint = None
        self.catchment_ids = {}
        self.source_ids = {}
        self.pre_sim_tasks = {}
        self.pre_sim_ids = {}

    def define_presim_dependencies(self, dependencies: list):
        for dep in dependencies:
            task_id = str(uuid.uuid4())
            presim_task = dask.delayed(WorkflowManager.execute_dependency)(task_id, dep["name"], dep["url"],
                                                                           dep["input"], self.debug,
                                                                           dask_key_name=f"{dep['name']}_{task_id}")
            self.pre_sim_ids[dep["name"]] = task_id
            self.pre_sim_tasks[dep["name"]] = presim_task

    def construct(self, catchment_inputs: dict, catchment_dependencies: dict):
        catchment_tasks = {}
        first_level = True
        for level in self.order:
            for catchment in level:
                cat_d_ids = copy.copy(self.pre_sim_ids)
                if first_level:
                    cat_dependencies = copy.copy(self.pre_sim_tasks)
                else:
                    cat_dependencies = {}
                catchment_id = str(uuid.uuid4())
                catchment = str(catchment)
                self.catchment_ids[catchment] = catchment_id
                upstream_catchments = {}
                upstream_ids = {}
                for c in self.sources[catchment]:
                    if "_" not in str(c):
                        upstream_catchments[str(c)] = catchment_tasks[str(c)]
                        upstream_ids[str(c)] = self.catchment_ids[str(c)]
                    else:
                        pcomid = str(c).split("_")
                        upstream_ids[pcomid[0]] = pcomid[1]
                cat_d_ids_only = {}
                for dep in catchment_dependencies[catchment]:
                    task_id = str(uuid.uuid4())
                    presim_task = dask.delayed(WorkflowManager.execute_dependency)(task_id, dep["name"], dep["url"],
                                                                                   dep["input"], self.debug,
                                                                                   dask_key_name=f"{dep['name']}_{task_id}")
                    cat_dependencies[dep["name"]] = presim_task
                    cat_d_ids[dep["name"]] = task_id
                    cat_d_ids_only[dep["name"]] = task_id
                catchment_input = catchment_inputs[catchment]
                MongoWorkflow.create_catchment_entry(self.task_id, catchment_id, catchment_input, status="PENDING",
                                                     upstream=upstream_ids, dependencies=cat_d_ids_only)

                catchment_task = dask.delayed(self.execute_segment)(self.task_id, catchment_id, catchment_input,
                                                                    upstream_catchments, upstream_ids, cat_dependencies,
                                                                    cat_d_ids, self.debug,
                                                                    dask_key_name=f"{catchment}_{catchment_id}")
                catchment_tasks[catchment] = catchment_task
                self.source_ids[catchment] = upstream_ids
                self.pourpoint = catchment_task
            first_level = False
        MongoWorkflow.create_simulation_entry(simulation_id=self.task_id, simulation_input=self.sim_input,
                                              status="IN-PROGRESS", catchments=self.catchment_ids, order=self.order,
                                              sources=self.sources, dependencies=self.pre_sim_ids)

    def compute(self):
        try:
            self.pourpoint.compute()
        except Exception as e:
            state = "FAILED"
            message = str(e)
            MongoWorkflow.update_simulation_entry(self.task_id, status=state, message=message)

    @staticmethod
    @dask.delayed
    def execute_dependency(task_id: str, name: str, url: str, request_input: dict, debug: bool = False):
        if debug:
            time.sleep(10)
            return request_input
        data = requests.post(url, json=request_input)
        MongoWorkflow.dump_data(task_id=task_id, data=json.loads(data.text), name=name)

    @staticmethod
    @dask.delayed
    def execute_segment(simulation_id: str, catchment_id: str, catchment_input: dict, upstream: dict = None,
                        upstream_ids: dict = None, dependency_task: dict = None, dependency_ids: dict = None,
                        debug: bool = False):
        t0 = time.time()
        MongoWorkflow.update_catchment_entry(catchment_id, status="IN-PROGRESS")
        full_input = None
        try:
            full_input, valid, message = MongoWorkflow.prepare_inputs(simulation_id=simulation_id,
                                                                      catchment_inputs=catchment_input,
                                                                      upstream=upstream_ids)
        except Exception as e:
            valid = False
            message = f"{e}"
        if valid:
            try:
                complete_input = {
                    "input": full_input,
                    "upstream": upstream_ids,
                    "data_sources": {},
                    "dependencies": dependency_ids
                }
                output = WorkflowManager.submit_request(complete_input, debug)
                if "metadata" in output:
                    if "ERROR" in output["metadata"]:
                        message = output["metadata"]["ERROR"]
                        status = "FAILED"
                    else:
                        status = "COMPLETED"
                        message = None
                else:
                    status = "COMPLETED"
                    message = None
            except Exception as e:
                status = "FAILED"
                message = str(e)
            t1 = time.time()
            MongoWorkflow.update_catchment_entry(catchment_id=catchment_id, status=status, message=message,
                                                 runtime=str(round(t1-t0, 4)))
        else:
            MongoWorkflow.update_catchment_entry(catchment_id=catchment_id, status="FAILED", message=message)
        status, sim_message = MongoWorkflow.completion_check(simulation_id=simulation_id)
        MongoWorkflow.update_simulation_entry(simulation_id=simulation_id, status=status, message=message)

    @staticmethod
    def submit_request(request_input: dict, debug: bool = False):
        if debug:
            time.sleep(10)
            return request_input
        workflow_url = "workflow/catchment/"
        request_url = str(os.getenv('HMS_BACKEND_SERVER_INTERNAL', "http://localhost:60550/")) + "/api/" + workflow_url
        data = requests.post(request_url, json=request_input)
        return json.loads(data.text)
