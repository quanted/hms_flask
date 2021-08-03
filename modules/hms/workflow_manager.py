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
import hashlib
from dask.distributed import Client, LocalCluster

timeout = 600
debug_logs = True


class MongoWorkflow:
    @staticmethod
    def connect_to_mongodb():
        in_docker = (os.getenv("IN_DOCKER", "False") == "True")
        database = 'hms_workflows'
        if not in_docker:
            # Dev env mongoDB
            if debug_logs:
                logging.info("Connecting to mongoDB at: mongodb://localhost:27017/0")
            mongo = pymongo.MongoClient(host='mongodb://localhost:27017/0')
        else:
            # Production env mongoDB
            if debug_logs:
                logging.info("Connecting to mongoDB at: mongodb://mongodb:27017/0")
            mongo = pymongo.MongoClient(host='mongodb://mongodb:27017/0')
        mongo[database].Collection.create_index([("timestamp", pymongo.DESCENDING)], expireAfterSeconds=604800)

        return mongo

    @staticmethod
    def get_collection(mongo: pymongo.MongoClient):
        database = 'hms_workflows'
        mongo_db = mongo[database]
        return mongo_db

    @staticmethod
    def create_simulation_entry(simulation_id: str, simulation_input: dict = None, status: str = None,
                                catchments=None, order: list = None, sources: list = None, dependencies: dict = None):
        timestamp = datetime.datetime.now().isoformat(' ')
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        # Check if existing simulation exists with this id and delete existing (may need to change behavior)
        query = {'_id': simulation_id}
        data = posts.find_one(query)
        if data is not None:
            posts.delete_one(query)
        if catchments is None:
            catchments = {}
        time_stamp = str(datetime.datetime.utcnow())
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
            "timestamp": time_stamp
        }
        posts.insert_one(data)
        mongo.close()

    @staticmethod
    def create_catchment_entry(simulation_id: str, catchment_id: str, catchment_input: dict = None, status: str = None,
                               upstream=None, dependencies: dict = None):
        timestamp = datetime.datetime.now().isoformat(' ')
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': catchment_id}
        data = posts.find_one(query)
        if data is not None:
            posts.delete_one(query)
        time_stamp = str(datetime.datetime.utcnow())
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
            "runtime": None,
            "timestamp": time_stamp
        }
        posts.insert_one(data)
        mongo.close()

    @staticmethod
    def update_simulation_entry(simulation_id: str, status: str = None, message: str = None, timestamp: str = None):
        if not timestamp:
            timestamp = datetime.datetime.now().isoformat(' ')
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': simulation_id}
        data = posts.find_one(query)
        data["update_time"] = timestamp
        if status:
            data["status"] = status
        if message:
            data["message"] = message
        posts.replace_one(query, data)
        mongo.close()

    @staticmethod
    def add_catchment(simulation_id: str, catchment_id: str, comid: str):
        if debug_logs:
            logging.info(f"Add Catchment: sim_id: {simulation_id}, cat_id: {catchment_id}, COMID: {comid}")
        timestamp = datetime.datetime.now().isoformat(' ')
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': simulation_id}
        data = posts.find_one(query)
        data["update_time"] = timestamp
        data["catchments"][comid] = catchment_id
        posts.replace_one(query, data)
        mongo.close()

    @staticmethod
    def update_catchment_entry(catchment_id: str, status: str = None, message: str = None, output: dict = None,
                               timestamp: str = None, runtime: str = None, upstream = None, dependencies: dict = None):
        if not timestamp:
            timestamp = datetime.datetime.now().isoformat(' ')
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
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
        if upstream:
            data["upstream"] = upstream
        if dependencies:
            data["dependencies"] = dependencies
        posts.replace_one(query, data)
        MongoWorkflow.update_simulation_entry(simulation_id=data["sim_id"], timestamp=timestamp)
        mongo.close()

    @staticmethod
    def prepare_inputs(catchment_id: str = None, upstream: dict = None):
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        c_query = {'_id': catchment_id}
        catchment_entry = posts.find_one(c_query)
        valid = True
        message = []
        if catchment_entry is None:
            logging.info(f"Catchment entry doesn't exist for id {catchment_id}")
        cat_inputs = catchment_entry["input"]
        if type(cat_inputs) == str:
            cat_inputs = json.loads(catchment_entry["input"])
        if not upstream:
            mongo.close()
            return cat_inputs, valid, ", ".join(message)
        for stream, stream_id in upstream.items():
            query = {'_id': stream_id}
            upstream_data = posts.find_one(query)
            if upstream_data["status"] != "COMPLETED":
                valid = False
                message.append(upstream_data["message"])
        mongo.close()
        return cat_inputs, valid, ", ".join(message)

    @staticmethod
    def completion_check(simulation_id: str):
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
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
        mongo.close()
        if "CANCELLED" in status:
            s = "CANCELLED"
        elif "IN-PROGRESS" in status or "PENDING" in status:
            s = "IN-PROGRESS"
        elif "FAILED" in status and "COMPLETED" in status:
            s = "INCOMPLETE"
        elif "FAILED" not in status:
            s = "COMPLETED"
        else:
            s = "FAILED"
        logging.info(f"Simulation ID: {simulation_id}, status: {s}, message: {message}")
        return s, ", ".join(message)

    @staticmethod
    def get_status(task_id: str):
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': task_id}
        data = posts.find_one(query)
        if data is None:
            mongo.close()
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
        mongo.close()
        return data

    @staticmethod
    def get_data(task_id: str):
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': task_id}
        data = posts.find_one(query)
        mongo.close()
        if data is None:
            return {"error": f"No data found for a task with id: {task_id}"}
        if data["type"] == "workflow":
            data["input"] = json.loads(data["input"])
            return data
        elif data["type"] == "dependency":
            return data
        else:
            data["input"] = json.loads(data["input"])
            if type(data["output"]) == str:
                data["output"] = json.loads(data["output"])
            return data

    @staticmethod
    def dump_data(task_id: str, url: str, name: str, request_input: dict, data=None, data_type: str = "dependency", status: str = "FAILED"):
        timestamp = datetime.datetime.now().isoformat(' ')
        exists = MongoWorkflow.get_entry(task_id=task_id)
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        hash = hashlib.md5((url.lower() + json.dumps(request_input, sort_keys=True)).encode()).hexdigest()
        dep_data = {
            "_id": task_id,
            "type": data_type,
            "url": url,
            "name": name,
            "output": data,
            "input": request_input,
            "hash": hash,
            "status": status,
            "timestamp": timestamp,
        }
        if exists:
            posts.replace_one({"_id": task_id}, dep_data)
        else:
            posts.insert_one(dep_data)
        mongo.close()

    @staticmethod
    def check_hash(url: str, request_input: dict):
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        hash = hashlib.md5((url.lower() + json.dumps(request_input, sort_keys=True)).encode()).hexdigest()
        exists = posts.find_one({"hash": hash})
        mongo.close()
        if exists:
            same_task = True
            logging_check = []
            if "status" not in exists.keys():
                same_task = False
                logging_check.append("status not in entry")
            else:
                if exists["status"] != "COMPLETED":
                    same_task = False
                    logging_check.append("status != completed")
            if "metadata" in exists["output"].keys():       # If there is a reported error in the metadata
                if "error" in exists["output"]["metadata"].keys() or "ERROR" in exists["output"]["metadata"].keys():
                    same_task = False
                    logging_check.append("error in metadata")
            if "data" in exists["output"].keys():           # If there is no data in the response
                if len(exists["output"]["data"]) == 0:
                    same_task = False
                    logging_check.append("no data in output")
            else:
                logging.info(f"Unable to reuse task due to {', '.join(logging_check)}")
                same_task = False
            if same_task:
                return exists["_id"]
        return None

    @staticmethod
    def get_entry(task_id: str):
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': task_id}
        data = posts.find_one(query)
        mongo.close()
        if data is None:
            return None
        else:
            return data

    @staticmethod
    def simulation_run_ready(task_id):
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': task_id}
        data = posts.find_one(query)
        mongo.close()
        if data["type"] == "workflow":
            valid = True
            ready_comids = list(data["catchments"].keys())
            notready_comids = []
            for comid in list(data["catchment_sources"].keys()):
                if comid not in ready_comids:
                    valid = False
                    notready_comids.append(comid)
            if valid:
                return 1, None
            else:
                return 0, f"Simulation contains catchments with no provided inputs. COMIDS: {', '.join(notready_comids)}"
        else:
            return 0, f"task_id is not of type workflow, task_id type: {data['type']}"

    @staticmethod
    def set_sim_status(task_id, status: str = "PENDING", replace_completed: bool = True):
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': task_id}
        data = posts.find_one(query)
        if data is None:
            return
        if data["type"] == "catchment":
            if replace_completed:
                data["status"] = status
            elif data["status"] != "COMPLETED":
                data["status"] = status
            data["message"] = None
        elif data["type"] == "workflow":
            for comid, catchment_id in data["catchments"].items():
                MongoWorkflow.set_sim_status(task_id=catchment_id, status=status)
            data["status"] = status
            data["message"] = None
        posts.replace_one({"_id": task_id}, data)
        mongo.close()

    @staticmethod
    def kill_simulation(sim_id: str):
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': sim_id}
        sim_entry = posts.find_one(query)
        if sim_entry:
            if sim_entry["type"] == "workflow":
                pourpoint = str(list(sim_entry["catchment_sources"].keys())[-1])
                pourpoint_id = sim_entry["catchments"][pourpoint]
                client = Client("dask-scheduler:8786", timeout=2)
                pourpoint_future = dask.distributed.Future(key=pourpoint_id, client=client)
                pourpoint_future.cancel()
            MongoWorkflow.set_sim_status(task_id=sim_id, status="CANCELLED", replace_completed=False)
        mongo.close()

    @staticmethod
    def check_dependencies(dependencies: dict):
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        valid = True
        message = ""
        for comid, id in dependencies.items():
            query = {'_id': id}
            entry = posts.find_one(query)
            logging.info(f"TEST DEP - COMID: {comid}, ID: {id}")
            if "status" in entry:
                if entry["status"] == "FAILED":
                    valid = False
                    message = f"Dependency: {id} failed"
                    break
        return valid, message


class WorkflowManager:

    def __init__(self, task_id: str, sim_input: dict, sources: list, order: list, local: bool = False,
                 debug: bool = False):
        self.local = local
        self.debug = debug
        self.scheduler = None
        if self.local:
            try:
                client = Client("tcp://127.0.0.1:8786", timeout=2)
                if debug_logs:
                    logging.info("Dask Client connected to existing local cluster at tcp://127.0.0.1:8786")
                self.scheduler = client.scheduler
            except Exception as e:
                self.scheduler = LocalCluster()
                client = Client(self.scheduler)
                if debug_logs:
                    logging.info(f"Dask Client connected to new local cluster at {self.scheduler}")
        else:
            self.scheduler = os.getenv('DASK_SCHEDULER', "tcp://127.0.0.1:8786")
            client = Client(self.scheduler)
            if debug_logs:
                logging.info(f"Dask Client connecting to existing cluster at {self.scheduler}")
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

    @staticmethod
    def load(sim_taskid: str):
        simulation = MongoWorkflow.get_entry(task_id=sim_taskid)
        if simulation is None:
            return 0, f"No simulation found with id {sim_taskid}, unable to start simulation."
        wf = WorkflowManager(
            task_id=sim_taskid,
            sim_input=simulation["input"],
            sources=simulation["catchment_sources"],
            order=simulation["network_order"]
        )
        return 1, wf

    def define_presim_dependencies(self, dependencies):
        if isinstance(dependencies, str):
            dependencies = json.loads(dependencies.replace("\'", "\""))
        for dep in dependencies:
            task_id = str(uuid.uuid4())
            if debug_logs:
                logging.info(f"Simulation Dependency: {dep}")
            inputs = dep["input"]
            presim_check = MongoWorkflow.check_hash(dep["url"], inputs)
            if presim_check:
                presim_task = None
                task_id = presim_check["_id"]
            else:
                presim_task = dask.delayed(WorkflowManager.execute_dependency)(task_id, dep["name"], dep["url"],
                                                                               inputs, self.debug,
                                                                               dask_key_name=f"{dep['name']}_{task_id}")
            self.pre_sim_ids[dep["name"]] = task_id
            self.pre_sim_tasks[dep["name"]] = presim_task

    def construct(self, catchment_inputs: dict, catchment_dependencies: dict):
        catchment_tasks = {}
        first_level = True
        for level in self.order:
            for catchment in level:
                catchment_id = str(uuid.uuid4())
                catchment = str(catchment)
                cat_d_ids = copy.copy(self.pre_sim_ids)
                if first_level or len(self.sources[catchment]) == 0:
                    cat_dependencies = copy.copy(self.pre_sim_tasks)
                else:
                    cat_dependencies = {}
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
                    cat_input = dep["input"]
                    presim_check = MongoWorkflow.check_hash(dep["url"], cat_input)
                    if presim_check:
                        cat_task = None
                        task_id = presim_check["_id"]
                        if debug_logs:
                            logging.info(f"Using data from existing dependency task for COMID: {catchment}, NAME: {dep['name']}, ID: {task_id}")
                    else:
                        cat_task = dask.delayed(WorkflowManager.execute_dependency)(task_id, dep["name"], dep["url"],
                                                                                    cat_input, self.debug,
                                                                                    dask_key_name=f"{dep['name']}_{task_id}")
                        if debug_logs:
                            logging.info(f"Created new dependency task for COMID: {catchment}, NAME: {dep['name']}, ID: {task_id}")
                    cat_dependencies[dep["name"]] = cat_task
                    cat_d_ids[dep["name"]] = task_id
                    cat_d_ids_only[dep["name"]] = task_id
                catchment_input = catchment_inputs[catchment]
                MongoWorkflow.create_catchment_entry(self.task_id, catchment_id, catchment_input, status="PENDING",
                                                     upstream=upstream_ids, dependencies=cat_d_ids_only)

                catchment_task = dask.delayed(self.execute_segment)(self.task_id, catchment_id,
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

    def construct_from_db(self, catchment_ids: dict):
        catchment_tasks = {}
        first_level = True
        for level in self.order:
            for catchment in level:
                catchment = str(catchment)
                catchment_id = catchment_ids[catchment]
                catchment_entry = MongoWorkflow.get_entry(task_id=catchment_id)
                catchment_dependencies = catchment_entry["dependencies"]
                cat_d_ids = copy.copy(self.pre_sim_ids)
                if first_level or len(self.sources[catchment]) == 0:
                    cat_dependencies = copy.copy(self.pre_sim_tasks)
                else:
                    cat_dependencies = {}
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
                if type(catchment_dependencies) == dict:
                    list_catchment = []
                    for k, v in catchment_dependencies.items():
                        list_catchment.append({"name": k, "taskID": v})
                    catchment_dependencies = list_catchment
                for dep in catchment_dependencies:
                    new_task = False
                    dep_entry = None
                    if "taskID" in dep.keys():
                        dep_entry = MongoWorkflow.get_entry(task_id=dep["taskID"])
                    if "input" not in dep:         # TaskID has been given to the dep, object in db
                        logging.info(f"DEP: {dep}")
                        dep_input = dep_entry["input"]
                        dep_url = dep_entry["url"]
                        task_id = dep["taskID"]
                        if "status" in dep_entry.keys():
                            if dep_entry["status"] != "COMPLETED":
                                new_task = True
                        else:
                            new_task = True
                    else:
                        new_task = True
                        task_id = str(uuid.uuid4())
                        dep_input = dep["input"]
                        dep_url = dep["url"]
                    presim_check = MongoWorkflow.check_hash(dep_url, dep_input)
                    if debug_logs:
                        logging.info(f"Presim_check: {presim_check}")
                    if presim_check and not new_task:
                        if debug_logs:
                            logging.info(f"Using existing dependency task for COMID: {catchment}, Name: {dep['name']}, new_task: {new_task}")
                            logging.info(f"Dependency taskID: {presim_check}")
                        cat_task = None
                        task_id = presim_check
                    else:
                        if debug_logs:
                            logging.info(f"Creating new dependency task for COMID: {catchment}, Name: {dep['name']}")
                            logging.info(f"Dependency taskID: {task_id}")
                        cat_task = dask.delayed(WorkflowManager.execute_dependency)(task_id, dep["name"], dep_url,
                                                                                    dep_input, self.debug,
                                                                                    dask_key_name=f"{task_id}")
                        MongoWorkflow.dump_data(task_id=task_id, url=dep_url, request_input=dep_input, name=dep["name"],
                                                status="PENDING")
                    cat_dependencies[dep["name"]] = cat_task
                    cat_d_ids[dep["name"]] = task_id
                    cat_d_ids_only[dep["name"]] = task_id
                MongoWorkflow.update_catchment_entry(catchment_id=catchment_id, status="PENDING",
                                                     upstream=upstream_ids, dependencies=cat_d_ids_only)
                self.source_ids[catchment] = upstream_ids
                logging.warning(f"COMID: {catchment}, upstream: {upstream_ids}")
                # catchment_task = dask.delayed(self.execute_segment)(simulation_id=self.task_id,
                #                                                     catchment_id=catchment_id,
                #                                                     upstream=upstream_catchments,
                #                                                     upstream_ids=upstream_ids,
                #                                                     dependency_task=cat_dependencies,
                #                                                     dependency_ids=cat_d_ids,
                #                                                     debug=self.debug,
                #                                                     dask_key_name=f"{catchment_id}")
                catchment_task = dask.delayed(self.execute_segment)(self.task_id, catchment_id,
                                                                    upstream_catchments, upstream_ids, cat_dependencies,
                                                                    cat_d_ids, self.debug,
                                                                    dask_key_name=f"{catchment}_{catchment_id}")
                catchment_tasks[catchment] = catchment_task
                self.pourpoint = catchment_task
            first_level = False
        logging.warning(f"Source IDS: {self.source_ids}")
        MongoWorkflow.update_simulation_entry(simulation_id=self.task_id, status="IN-PROGRESS")

    def compute(self):
        try:
            if debug_logs:
                logging.info(f"Pourpoint TYPE: {type(self.pourpoint)}, VALUE: {self.pourpoint}")
            self.pourpoint.compute()
        except Exception as e:
            logging.warning(f"Error: e001, message: {e}, compute failure for task_id: {self.task_id}")
            state = "FAILED"
            message = str(e)
            MongoWorkflow.update_simulation_entry(self.task_id, status=state, message=f"e001: {message}")

    @staticmethod
    @dask.delayed
    def execute_dependency(task_id: str, name: str, url: str, request_input: dict, debug: bool = False):
        MongoWorkflow.dump_data(task_id=task_id, url=url, request_input=request_input, name=name, status="IN-PROGRESS")
        request_url = str(os.getenv('HMS_BACKEND_SERVER_INTERNAL', "http://localhost:60550/")) + url
        status = "FAILED"
        try:
            if debug:
                time.sleep(10)
                data = request_input
            else:
                logging.info(f"Executing dependency task: {task_id}")
                dep_data = requests.post(request_url, json=request_input)
                data = json.loads(dep_data.text)
                if "metadata" in data.keys():
                    if "error" not in data["metadata"].keys():
                        status = "COMPLETED"
                if "data" in data.keys():
                    if len(data["data"]) > 0:
                        status = "COMPLETED"
        except Exception as e:
            logging.warning(f"Error: e002, message: {e}")
            data = {"error": f"e002: {str(e)}"}
        MongoWorkflow.dump_data(task_id=task_id, url=url, request_input=request_input, data=data, name=name, status=status)
        logging.info(f"Completed dependency task: {task_id}, status: {status}")

    @staticmethod
    @dask.delayed
    def execute_segment(simulation_id: str, catchment_id: str, upstream: dict,
                        upstream_ids: dict, dependency_task: dict, dependency_ids: dict,
                        debug: bool = False):
        t0 = time.time()
        MongoWorkflow.update_catchment_entry(catchment_id, status="IN-PROGRESS")
        try:
            if debug_logs:
                logging.warning(f"Upstream_ids: {upstream_ids}")
                logging.warning(f"Upstream: {upstream}")
                logging.warning(f"Dependency_task: {dependency_task}")
                logging.warning(f"Dependency_ids: {dependency_ids}")
            full_input, valid, message = MongoWorkflow.prepare_inputs(catchment_id=catchment_id, upstream=upstream_ids)
            # TODO: Check that the dependencies all have status="SUCCESS" otherwise valid=False
        except Exception as e:
            logging.warning(f"Error: e003, message: {e}")
            valid = False
            message = f"e003: {e}"
        try:
            entry = MongoWorkflow.get_entry(task_id=catchment_id)
            dependency_ids = entry["dependencies"]
            if dependency_ids:
                valid, message = MongoWorkflow.check_dependencies(dependencies=dependency_ids)
        except Exception as e:
            logging.warning(f"Error: e003b, message: {e}")
            valid = False
            message = f"e003b: {e}"
        if valid:
            output = None
            try:
                logging.warning(f"Executing catchment task: {catchment_id}")
                output = WorkflowManager.submit_request(catchment_id, debug)
                try:
                    output = json.loads(output)
                    status = ""
                except Exception as e:
                    status = "FAILED"
                    message = f"e004: {str(e)}"
                    logging.warning(f"Error: e004, message: {e}")
                if "metadata" in output and status != "FAILED":
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
                message = f"e005: {str(e)}"
                logging.warning(f"Error: e005, message: {message}")
            t1 = time.time()
            MongoWorkflow.update_catchment_entry(catchment_id=catchment_id, status=status, message=message,
                                                 output=output, runtime=str(round(t1-t0, 4)))
        else:
            MongoWorkflow.update_catchment_entry(catchment_id=catchment_id, status="FAILED", message=message)
        status, sim_message = MongoWorkflow.completion_check(simulation_id=simulation_id)
        MongoWorkflow.update_simulation_entry(simulation_id=simulation_id, status=status, message=message)
        logging.warning(f"Completed catchment task: {catchment_id}")

    @staticmethod
    def submit_request(catchment_taskid: str, debug: bool = False):
        if debug:
            time.sleep(10)
            return "{'test':'test'}"
        workflow_url = "api/aquatox/workflow"
        request_url = str(os.getenv('HMS_BACKEND_SERVER_INTERNAL', "http://localhost:60550/")) + workflow_url
        data = requests.get(request_url, params={'task_id': catchment_taskid}, timeout=timeout)
        return data.text

    @staticmethod
    def create_simulation(sim_taskid: str, simulation_dependencies: dict, network: dict):
        MongoWorkflow.create_simulation_entry(simulation_id=sim_taskid, simulation_input=None, status="PENDING",
                                              catchments=None, order=network['order'], sources=network['sources'],
                                              dependencies=simulation_dependencies)

    @staticmethod
    def create_catchment(sim_taskid: str, catchment_input: dict, comid: str, dependencies: dict = None):
        sim_check = MongoWorkflow.get_entry(task_id=sim_taskid)
        if sim_check is None:
            return 0, f"No simulation found with id {sim_taskid}, unable to add catchment to simulation."
        catchment_id = str(uuid.uuid4())
        MongoWorkflow.create_catchment_entry(simulation_id=sim_taskid, catchment_id=catchment_id,
                                             catchment_input=catchment_input, status="PENDING",
                                             dependencies=dependencies)
        MongoWorkflow.add_catchment(simulation_id=sim_taskid, catchment_id=catchment_id, comid=comid)
        return 1, catchment_id
