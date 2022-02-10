"""
HMS Workflow Manger handles the creation, execution, and management of HMS simulations.
"""
import os
import uuid
import json
import copy
import logging
import sys

import gridfs
import pymongo
import datetime
import requests
import time
import dask
import hashlib
from dask.distributed import Client, LocalCluster

timeout = 600  # Timeout for task execution of a single segment 600=10 minutes
debug_logs = True  # Outputs to log processing steps


class MongoWorkflow:
    """
    Class for handling all interactions with the mongodb database for HMS workflow objects.
    """

    @staticmethod
    def connect_to_mongodb():
        """
        Get connection to mongodb instance and the database used for storing all workflow objects.
        Sets object expiration to 7 days = 604800 seconds
        :return: mongo Client
        """
        in_docker = (os.getenv("IN_DOCKER", "False") == "True")
        mongodb_host = os.getenv("MONGODB", "mongodb://localhost:27017/0")
        database = 'hms_workflows'
        if not in_docker:
            # Dev env mongoDB
            mongo = pymongo.MongoClient(host=mongodb_host)
        else:
            # Production env mongoDB
            mongo = pymongo.MongoClient(host=mongodb_host)
        mongo[database].Collection.create_index([("timestamp", pymongo.DESCENDING)], expireAfterSeconds=604800)

        return mongo

    @staticmethod
    def get_collection(mongo: pymongo.MongoClient):
        """
        Get a collection from mongodb client
        :param mongo: A mongo DB client
        :return: The collection 'database' from the mongo DB client
        """
        database = 'hms_workflows'
        mongo_db = mongo[database]
        return mongo_db

    @staticmethod
    def stash_bigdata(mongo_db, data):
        if not data:
            return None
        fs = gridfs.GridFS(mongo_db)
        data = data if type(data) == str else json.dumps(data)
        data_id = fs.put(data.encode("utf-8"))
        if debug_logs:
            data_size = sys.getsizeof(data)
            logging.warning(f"GridFS object: {data_id}, size: {data_size/1048576} Mb")
        return data_id

    @staticmethod
    def get_bigdata(mongo_db, data_id):
        if not data_id:
            return None
        fs = gridfs.GridFS(mongo_db)
        data = fs.get(data_id).read().decode("utf-8")
        return data

    @staticmethod
    def create_simulation_entry(simulation_id: str, simulation_input: dict = None, status: str = None,
                                catchments=None, order: list = None, sources: list = None, dependencies: dict = None):
        """
        Create new simulation object in mongodb collection.
        :param simulation_id: The simulation task_id
        :param simulation_input: The simulation inputs
        :param status: The current simulation status
        :param catchments: The collection of catchments in the simulation
        :param order: The order those catchments can be executed in, traversal by reverse BFS
        :param sources: The sources of each catchment in the simulation
        :param dependencies: Simulation level dependencies which must execute prior to any catchment simulations
        :return: None
        """
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
        input_id = MongoWorkflow.stash_bigdata(mongo_db=mongo_db, data=json.dumps(simulation_input))
        data = {
            "_id": simulation_id,
            "type": "workflow",
            "input": input_id,
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
        """
        Create new catchment object in mongodb collection.
        :param simulation_id: The simulation task_id which this catchment corresponds to
        :param catchment_id: The catchment task_id
        :param catchment_input: The inputs for the catchment simulation
        :param status: The current status of the catchment simulation
        :param upstream: The upstream catchments
        :param dependencies: Catchment dependencies which must execute prior to the catchment simulation
        :return: None
        """
        timestamp = datetime.datetime.now().isoformat(' ')
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': catchment_id}
        data = posts.find_one(query)
        if data is not None:
            posts.delete_one(query)
        time_stamp = str(datetime.datetime.utcnow())
        input_id = MongoWorkflow.stash_bigdata(mongo_db=mongo_db, data=json.dumps(catchment_input))
        data = {
            "_id": catchment_id,
            "type": "catchment",
            "sim_id": simulation_id,
            "input": input_id,
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
    def update_simulation_entry(simulation_id: str, status: str = None, message: str = None, timestamp: str = None,
                                dependencies: dict = None):
        """
        Update an existing simulation object in the mongo collection.
        :param simulation_id: The existing simulation task_id
        :param status: The current status of the simulation
        :param message: A message to pass along errors or other necessary information
        :param timestamp: A timestamp of the update, will use now if not provided
        :param dependencies: Update to the dependencies
        :return: None
        """
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
        if dependencies:
            data["dependencies"] = dependencies
        posts.replace_one(query, data)
        mongo.close()

    @staticmethod
    def add_catchment(simulation_id: str, catchment_id: str, comid: str):
        """
        Connect a catchment object to an existing simulation object.
        :param simulation_id: The simulation task_id
        :param catchment_id: The catchment task_id which is being added to a simulation
        :param comid: The comid of the catchment task_id being added to the simulation
        :return: None
        """
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
                               timestamp: str = None, runtime: str = None, upstream=None, dependencies: dict = None):
        """
        Update an existing catchment mongo object.
        :param catchment_id: The catchment task_id
        :param status: The catchment simulation status
        :param message: A message provided by errors or other function of the catchment simulation process
        :param output: The output of the catchment simulation
        :param timestamp: The timestamp of the database update/creation
        :param runtime: The total runtime of the catchment simulation, starting at task execution ending at completion
        :param upstream: The upstream catchments
        :param dependencies: Catchment dependencies which must execute prior to the catchment simulation
        :return: None
        """
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
            output_id = MongoWorkflow.stash_bigdata(mongo_db=mongo_db, data=json.dumps(output))
            data["output"] = output_id
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
        """
        Assemble task inputs and check the status of dependencies and upstream simulations.
        :param catchment_id: The catchment task_id
        :param upstream: The upstream catchments
        :return: The catchment simulation inputs, valid:bool, message:str
        """
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
                if "Segment" not in upstream_data['message']:
                    message.append(f"Segment: {stream}, message: {upstream_data['message']}")
                else:
                    message.append(f"{upstream_data['message']}")
        mongo.close()
        return cat_inputs, valid, ", ".join(message)

    @staticmethod
    def completion_check(simulation_id: str):
        """
        Check if the simulation has finished and the status of the simulation based upon the catchment tasks and dependencies
        :param simulation_id: The simulation task_id
        :return: The current simulation status, message:str
        """
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
            message.append("Simulation was cancelled")
        elif "IN-PROGRESS" in status or "PENDING" in status:
            s = "IN-PROGRESS"
            message.append("Simulation is in progress")
        elif "FAILED" in status and "COMPLETED" in status:
            s = "INCOMPLETE"
            message.append("Simulation has completed with errors")
        elif "FAILED" not in status:
            s = "COMPLETED"
            message.append("Simulation has completed successfully.")
        else:
            s = "FAILED"
            message.append("Simulation failed to complete")
        logging.info(f"Simulation ID: {simulation_id}, status: {s}, message: {message}")
        return s, ", ".join(message)

    @staticmethod
    def get_status(task_id: str):
        """
        Get the status and details of a mongo workflow object by task_id. Does not include inputs and outputs
        :param task_id: The object task_id, maybe a workflow, catchment or dependency
        :return: The status and details of the object, or an error message if no object exists for the task_id
        """
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
            dependencies = {}
            for d_data in data["dependencies"]:
                dep = d_data[0]
                d_id = d_data[1]
                query = {'_id': d_id}
                d_data = posts.find_one(query)
                dependencies[dep] = {
                    "status": d_data["status"],
                    "task_id": d_id,
                    "timestamp": d_data["timestamp"],
                    "message": d_data["message"]
                }
            data["dependencies"] = dependencies
        else:
            del data["output"]
        mongo.close()
        return data

    @staticmethod
    def get_data(task_id: str):
        """
        Get all the output and input data of a mongo object.
        :param task_id: The object task_id
        :return: The input and output data or an error if the task_id does not exist
        """
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': task_id}
        data = posts.find_one(query)
        mongo.close()
        if data is None:
            return {"error": f"No data found for a task with id: {task_id}"}
        if data["type"] == "workflow":
            input_data = json.loads(MongoWorkflow.get_bigdata(mongo_db=mongo_db, data_id=data["input"]))
            data["input"] = input_data
            return data
        elif data["type"] == "dependency":
            if data["output"]:
                output_data = json.loads(MongoWorkflow.get_bigdata(mongo_db=mongo_db, data_id=data["output"]))
                data["output"] = output_data
            return data
        else:
            input_data = json.loads(MongoWorkflow.get_bigdata(mongo_db=mongo_db, data_id=data["input"]))
            data["input"] = input_data
            if data["output"]:
                output_data = json.loads(MongoWorkflow.get_bigdata(mongo_db=mongo_db, data_id=data["output"]))
                data["output"] = output_data
            return data

    @staticmethod
    def dump_data(task_id: str, url: str, name: str, request_input: dict, data=None, data_type: str = "dependency",
                  status: str = "FAILED", message: str = None):
        """
        Simulation/catchment dependencies object database dump, used to create or update dependency objects. Will
        replace an existing object or create a new.
        :param task_id: The object task_id
        :param url: The url to execute the dependency task
        :param name: A name for the task to identify the task data type
        :param request_input: The inputs to be submitted to the url to execute the task
        :param data: The resulting output returned on task completion
        :param data_type: The type of object, defaults to dependency
        :param status: The status of the task
        :param message: A message of the state of the task
        :return: None
        """
        timestamp = datetime.datetime.now().isoformat(' ')
        exists = MongoWorkflow.get_entry(task_id=task_id)
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        hash = hashlib.md5((url.lower() + json.dumps(request_input, sort_keys=True)).encode()).hexdigest() if not exists else exists["hash"]
        logging.info(f"Dependency {name} has hash {hash}, already present: {True if exists else False}")
        output_id = MongoWorkflow.stash_bigdata(mongo_db=mongo_db, data=data)
        dep_data = {
            "_id": task_id,
            "type": data_type,
            "url": url,
            "name": name,
            "output": output_id,
            "input": request_input,
            "hash": hash,
            "status": status,
            "timestamp": timestamp,
            "message": message
        }
        if exists:
            posts.replace_one({"_id": task_id}, dep_data)
        else:
            posts.insert_one(dep_data)
        mongo.close()

    @staticmethod
    def check_hash(url: str, request_input: dict):
        """
        Check the hash of the combined url and serialized request_input against those object tasks that have already
        been completed to reuse the output if data exists.
        :param url: The url for the dependency task
        :param request_input: The request body for the dependency task
        :return: None if no matching completed hash exists, otherwise will return the task_id of the matching object
        """
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
            if exists["output"]:
                output = MongoWorkflow.get_bigdata(mongo_db=mongo_db, data_id=exists["output"])
                output = json.loads(output) if type(output) == str else output
                if "metadata" in output.keys():  # If there is a reported error in the metadata
                    if "error" in output["metadata"].keys() or "ERROR" in output["metadata"].keys():
                        same_task = False
                        logging_check.append("error in metadata")
                if "data" in output.keys():  # If there is no data in the response
                    if len(output["data"]) == 0:
                        same_task = False
                        logging_check.append("no data in output")
            else:
                same_task = False
                logging_check.append("no output in results")
            if same_task:
                logging.info(f"Found existing task {exists['_id']} for hash: {hash}")
                return exists["_id"]
            else:
                logging.info(f"Unable to reuse task due to {', '.join(logging_check)}")
        else:
            logging.info(f"No existing data found in the database for hash: {hash}")
        return None

    @staticmethod
    def get_entry(task_id: str):
        """
        Get the complete mongo db object
        :param task_id: The id of the mongo db object
        :return: The db object if exists else None
        """
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': task_id}
        data = posts.find_one(query)
        mongo.close()
        if data is None:
            return None
        else:
            if "input" in data.keys():
                if data["input"] and type(data["input"]) == str:
                    input_data = MongoWorkflow.get_bigdata(mongo_db=mongo_db, data_id=data["input"])
                    data["input"] = json.loads(input_data) if type(input_data) == str else input_data
            if "output" in data.keys():
                if data["output"]:
                    output_data = MongoWorkflow.get_bigdata(mongo_db=mongo_db, data_id=data["output"])
                    data["output"] = json.loads(output_data) if type(output_data) == str else output_data
            return data

    @staticmethod
    def simulation_run_ready(task_id):
        """
        Checks that all catchments have been added to the simulation and that the simulation is ready to run.
        :param task_id: The simulation task_id
        :return: 0=not-ready/1=ready, message indicating why the simulation is not ready
        """
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
        """
        Updates the simulation status
        :param task_id: The simulation task_id to be updated
        :param status: The new status of the simulation
        :param replace_completed: Replace status of already completed simulation, default=True
        :return: None
        """
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
        """
        Terminate a currently running simulation.
        :param sim_id: The simulation task_id
        :return: None
        """
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        query = {'_id': sim_id}
        sim_entry = posts.find_one(query)
        if sim_entry:
            if sim_entry["type"] == "workflow":
                pourpoint = str(list(sim_entry["catchment_sources"].keys())[-1])
                pourpoint_id = sim_entry["catchments"][pourpoint]
                scheduler = os.getenv('DASK_SCHEDULER', "tcp://127.0.0.1:8786")
                client = Client(scheduler, timeout=2)
                pourpoint_future = dask.distributed.Future(key=pourpoint_id, client=client)
                pourpoint_future.cancel()
            MongoWorkflow.set_sim_status(task_id=sim_id, status="CANCELLED", replace_completed=False)
        mongo.close()

    @staticmethod
    def check_dependencies(dependencies: dict):
        """
        Check the status of a dictionary of dependencies.
        :param dependencies: dictionary of dependencies to check, where {NAME:TASKID}
        :return: True=not failed/False=Failed, message
        """
        mongo = MongoWorkflow.connect_to_mongodb()
        mongo_db = MongoWorkflow.get_collection(mongo)
        posts = mongo_db["data"]
        valid = True
        message = ""
        for comid, id in dependencies.items():
            query = {'_id': id}
            entry = posts.find_one(query)
            if "status" in entry:
                if entry["status"] == "FAILED":
                    valid = False
                    message = f"Dependency: {id} failed"
                    break
        return valid, message


class WorkflowManager:
    """
    The HMS Workflow Manager class that handles all simulation setup, execution and processing. A simulation created as
    a dask task graph, where each catchment simulation is a node in the graph with execution order determined by the
    stream network and dependencies are nodes connected to those catchment simulations requiring that dependency data.
    """

    def __init__(self, task_id: str, sim_input: dict, sources: list, order: list, local: bool = False,
                 debug: bool = False):
        """
        Workflow Manager initializer
        :param task_id: The simulation task_id, must already exist in the database
        :param sim_input: The simulation inputs
        :param sources: The list of sources for each catchment in the simulation
        :param order: The order of execution of the catchments
        :param local: Run locally or in a docker container
        :param debug: Run in debug mode
        """
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
        """
        Load a Workflow Manager instance from existing objects in the database.
        :param sim_taskid: The simulation task_id
        :return: 0=Failed/1=Loaded, error message/instance of the WorkflowManager
        """
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
        """
        Build the simulation dependency tasks for the graph
        :param dependencies: List of dependencies
        :return: None
        """
        if isinstance(dependencies, str):               # convert dependencies from string to list, if necessary
            dependencies = json.loads(dependencies.replace("\'", "\""))
        if type(dependencies) == dict:                  # converts dict to list, occurs with reruns where a db object exists
            dependencies = [v for k, v in dependencies.items()]
        for dep in dependencies:
            task_id = str(uuid.uuid4())
            if debug_logs:
                logging.info(f"Simulation Dependency: {dep}")
            if type(dep) != dict:
                dep = MongoWorkflow.get_entry(task_id=dep)
            inputs = dep["input"]
            presim_check = MongoWorkflow.check_hash(dep["url"], inputs)     # Checks if this task exists and has been completed already
            if presim_check:
                presim_task = None
                task_id = presim_check
            else:
                presim_task = dask.delayed(WorkflowManager.execute_dependency)(task_id, dep["name"], dep["url"],
                                                                               inputs, self.debug,
                                                                               dask_key_name=f"{task_id}")
                MongoWorkflow.dump_data(task_id=task_id, url=dep["url"], request_input=inputs, name=dep["name"],
                                        status="PENDING", message=f"Created simulation data task for {dep['name']}")
            self.pre_sim_ids[dep["name"]] = task_id
            self.pre_sim_tasks[dep["name"]] = presim_task
        MongoWorkflow.update_simulation_entry(simulation_id=self.task_id, dependencies=self.pre_sim_ids, message="All simulation data request tasks have been created.")

    def construct_from_db(self, catchment_ids: dict):
        """
        Constructs the dask computational graph from simulation dependencies, catchment dependencies, and the catchment
        simulation nodes, order and edges determined by stream network and dependency association.
        :param catchment_ids: A dictionary of catchment ids {COMID:TASK_ID}
        :return: None
        """
        catchment_tasks = {}
        first_level = True      # used to assign simulation dependencies as arguments to the catchment simulation node
        # using the stream network order, reverse BFS starting at the pourpoint going upstream.
        for level in self.order:
            for catchment in level:
                catchment = str(catchment)
                catchment_id = catchment_ids[catchment]
                catchment_entry = MongoWorkflow.get_entry(task_id=catchment_id)
                catchment_dependencies = catchment_entry["dependencies"]
                cat_d_ids = copy.copy(self.pre_sim_ids)
                if first_level or len(self.sources[catchment]) == 0:
                    # if the first level we add the simulation dependency tasks to the catchment dependency task list
                    # so that all catchment simulations wait until the sim dep tasks are completed.
                    cat_dependencies = copy.copy(self.pre_sim_tasks)
                else:
                    cat_dependencies = {}
                self.catchment_ids[catchment] = catchment_id
                upstream_catchments = {}
                upstream_ids = {}
                for c in self.sources[catchment]:
                    if "_" in str(c):
                        pcomid = str(c).split("_")
                        upstream_ids[pcomid[0]] = pcomid[1]
                    elif c not in catchment_ids.keys():
                        # out of network source for boundary segment
                        continue
                    else:
                        upstream_catchments[str(c)] = catchment_tasks[str(c)]
                        upstream_ids[str(c)] = self.catchment_ids[str(c)]
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
                    if "input" not in dep and dep_entry:  # TaskID has been given to the dep, object in db
                        logging.info(f"DEP: {dep}")
                        dep_input = dep_entry["input"]
                        dep_url = dep_entry["url"]
                        task_id = dep["taskID"]
                        # if "status" in dep_entry.keys():
                        #     if dep_entry["status"] != "COMPLETED":
                        #         new_task = True
                        # else:
                        #     new_task = True
                    else:
                        new_task = True
                        task_id = str(uuid.uuid4())
                        dep_input = dep["input"]
                        dep_url = dep["url"]
                    presim_check = MongoWorkflow.check_hash(dep_url, dep_input)
                    if debug_logs:
                        logging.info(f"Presim_check: {presim_check}")
                    if presim_check:
                        if debug_logs:
                            logging.info(
                                f"Using existing dependency task for COMID: {catchment}, Name: {dep['name']}, new_task: {new_task}")
                            logging.info(f"Dependency taskID: {presim_check}")
                        cat_task = None
                        task_id = presim_check
                    else:
                        if debug_logs:
                            logging.info(f"Creating new dependency task for COMID: {catchment}, Name: {dep['name']}")
                            logging.info(f"Dependency taskID: {task_id}")
                        cat_task = dask.delayed(WorkflowManager.execute_dependency)(task_id, dep["name"], dep_url,
                                                                                    dep_input, self.debug, catchment,
                                                                                    dask_key_name=f"{task_id}")
                        MongoWorkflow.dump_data(task_id=task_id, url=dep_url, request_input=dep_input, name=dep["name"],
                                                status="PENDING", message=f"Created catchment data task for {catchment}")
                    cat_dependencies[dep["name"]] = cat_task
                    cat_d_ids[dep["name"]] = task_id
                    cat_d_ids_only[dep["name"]] = task_id
                MongoWorkflow.update_catchment_entry(catchment_id=catchment_id, status="PENDING",
                                                     upstream=upstream_ids, dependencies=cat_d_ids_only,
                                                     message=f"Created catchment simulation task for {catchment}")
                self.source_ids[catchment] = upstream_ids
                logging.warning(f"COMID: {catchment}, upstream: {upstream_ids}")
                catchment_task = dask.delayed(self.execute_segment)(self.task_id, catchment_id,
                                                                    upstream_catchments, upstream_ids, cat_dependencies,
                                                                    cat_d_ids, self.debug, catchment,
                                                                    dask_key_name=f"{catchment}_{catchment_id}")
                catchment_tasks[catchment] = catchment_task
                self.pourpoint = catchment_task
            first_level = False
        logging.warning(f"Source IDS: {self.source_ids}")
        MongoWorkflow.update_simulation_entry(simulation_id=self.task_id, status="IN-PROGRESS",
                                              message="Created all simulation tasks.")

    def compute(self):
        """
        Run compute on the completed computational graph. Running compute on the last node, pourpoint, will trigger all
        the tasks required to complete that node, in the sequence specified by the graph.
        :return: None
        """
        try:
            if debug_logs:
                logging.info(f"Pourpoint TYPE: {type(self.pourpoint)}, VALUE: {self.pourpoint}")
            self.pourpoint.compute()
        except Exception as e:
            logging.warning(f"Error: e001, message: {e}, compute failure for task_id: {self.task_id}")
            state = "FAILED"
            message = str(e)
            MongoWorkflow.update_simulation_entry(self.task_id, status=state, message=f"error001: {message}")

    @staticmethod
    @dask.delayed
    def execute_dependency(task_id: str, name: str, url: str, request_input: dict, debug: bool = False, comid: str = None, retries: int = 5):
        """
        The function that corresponds to a dependency node task.
        :param task_id: The task_id
        :param name: The name of the dependency
        :param url: The url that will be making a request to.
        :param request_input: The body for the request
        :param debug: Run in debug mode (does not make the request and copies the input as the output, for testing)
        :param comid: The catchment which corresponds to this task
        :return: None
        """
        MongoWorkflow.dump_data(task_id=task_id, url=url, request_input=request_input, name=name, status="IN-PROGRESS",
                                message=f"Started data retrieval task for {name} in catchment {comid}")
        task_target = os.getenv('HMS_WORKFLOW_BACKEND',
                                os.getenv('HMS_BACKEND_SERVER_INTERNAL', "http://localhost:60550/"))
        request_url = str(task_target) + url
        logging.warning(f"Submitting dependency task: {task_id} to url: {request_url} for comid: {comid}")
        status = "FAILED"
        try:
            message = ""
            if debug:
                time.sleep(10)
                data = request_input
            else:
                dep_data = requests.post(request_url, json=request_input)
                logging.warning(f"Task ID: {task_id}, response: {dep_data.status_code}")
                data = json.loads(dep_data.text)
                if "metadata" in data.keys():
                    if "error" not in data["metadata"].keys() or "ERROR" not in data["metadata"].keys():
                        status = "COMPLETED"
                        message = f"Completed data retrieval task for {name} in catchment {comid}"
                    else:
                        message = f"Failed data retrieval task for {name} in catchment {comid}, error in processing."
                if "data" in data.keys() and status == "COMPLETED":
                    if len(data["data"].keys()) == 0:
                        status = "FAILED"
                        message = f"Failed data retrieval task for {name} in catchment {comid}, no output data."
                    else:
                        message = f"Completed data retrieval task for {name} in catchment {comid}"
        except Exception as e:
            logging.warning(f"Error: e002, message: {e}; retries: {retries} of 5")
            message = f"error002: {str(e)}; retries: {retries} of 5"
            data = {"error": message}
        MongoWorkflow.dump_data(task_id=task_id, url=url, request_input=request_input, data=data, name=name,
                                status=status, message=message)
        if status == "FAILED" and retries > 0:
            WorkflowManager.execute_dependency(task_id=task_id, name=name, url=url, request_input=request_input,
                                               debug=debug, comid=comid, retries=retries-1)
        logging.warning(f"Completed dependency task: {task_id}, status: {status}")

    @staticmethod
    @dask.delayed
    def execute_segment(simulation_id: str, catchment_id: str, upstream: dict,
                        upstream_ids: dict, dependency_task: dict, dependency_ids: dict,
                        debug: bool = False, comid: str = False):
        """
        The function that corresponds to a catchment simulation node task.
        :param simulation_id: The simulation task id
        :param catchment_id: The catchment simulation task id
        :param upstream: The upstream segment(s) tasks, used to create a downstream edge between the segments and
        current catchment node.
        :param upstream_ids: The upstream segment task ids
        :param dependency_task: The dependency tasks, used to create an edge between the deps and the catchment node.
        :param dependency_ids: The dependency task ids
        :param debug: Run in debug mode, well return static message as the output for testing.
        :param comid: The catchments comids
        :return: None
        """
        t0 = time.time()
        logging.warning(f"Starting catchment simulation task, COMID: {comid}, id: {catchment_id}")
        MongoWorkflow.update_catchment_entry(catchment_id, status="IN-PROGRESS",
                                             message=f"Starting catchment simulation for catchment {comid}")
        try:
            if debug_logs:
                logging.info(f"Upstream_ids: {upstream_ids}")
                logging.info(f"Upstream: {upstream}")
                logging.info(f"Dependency_task: {dependency_task}")
                logging.info(f"Dependency_ids: {dependency_ids}")
            full_input, valid, message = MongoWorkflow.prepare_inputs(catchment_id=catchment_id, upstream=upstream_ids)
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
                output = WorkflowManager.submit_request(catchment_id, debug)
                try:
                    output = json.loads(output)
                    status = ""
                except Exception as e:
                    status = "FAILED"
                    message = f"error004: {str(e)}"
                    logging.warning(f"Error: e004, message: {e}")
                if "metadata" in output and status != "FAILED":
                    if "ERROR" in output["metadata"]:
                        message = output["metadata"]["ERROR"]
                        status = "FAILED"
                    else:
                        status = "COMPLETED"
                        message = f"Completed catchment simulation for catchment {comid}"
                else:
                    status = "COMPLETED"
                    message = f"Completed catchment simulation for catchment {comid}"
            except Exception as e:
                status = "FAILED"
                message = f"error005: {str(e)}"
                logging.warning(f"Error: e005, message: {message}")
            t1 = time.time()
            MongoWorkflow.update_catchment_entry(catchment_id=catchment_id, status=status, message=message,
                                                 output=output, runtime=str(round(t1 - t0, 4)))
        else:
            MongoWorkflow.update_catchment_entry(catchment_id=catchment_id, status="FAILED", message=message)
        status, sim_message = MongoWorkflow.completion_check(simulation_id=simulation_id)
        MongoWorkflow.update_simulation_entry(simulation_id=simulation_id, status=status, message=message)
        logging.warning(f"Completed catchment comid: {comid}, task: {catchment_id}")

    @staticmethod
    def submit_request(catchment_taskid: str, debug: bool = False):
        """
        Submit request to hms backend for AQT simulation
        :param catchment_taskid: The catchment simulation task_id
        :param debug: run in debug mode
        :return: the response
        """
        if debug:
            time.sleep(10)
            return "{'test':'test'}"
        workflow_url = "api/aquatox/workflow"
        request_url = str(os.getenv('HMS_BACKEND_SERVER_INTERNAL', "http://localhost:60550/")) + workflow_url
        data = requests.get(request_url, params={'task_id': catchment_taskid}, timeout=timeout)
        return data.text

    @staticmethod
    def create_simulation(sim_taskid: str, simulation_dependencies: dict, network: dict):
        """
        Create a simulation database object, used in simulation input setup
        :param sim_taskid: Simulation task id
        :param simulation_dependencies: The simulation dependencies
        :param network: The stream network
        :return: None
        """
        MongoWorkflow.create_simulation_entry(simulation_id=sim_taskid, simulation_input=None, status="PENDING",
                                              catchments=None, order=network['order'], sources=network['sources'],
                                              dependencies=simulation_dependencies)

    @staticmethod
    def create_catchment(sim_taskid: str, catchment_input: dict, comid: str, dependencies: dict = None):
        """
        Creates a catchment database object, used in the simulation input setup
        :param sim_taskid: The simulation task_id
        :param catchment_input: The inputs for the catchment simulation
        :param comid: The catchments comid
        :param dependencies: The dependencies for the catchment
        :return: 0=Failed/1=Created, error message/catchment task_id
        """
        sim_check = MongoWorkflow.get_entry(task_id=sim_taskid)
        if sim_check is None:
            return 0, f"No simulation found with id {sim_taskid}, unable to add catchment to simulation."
        catchment_id = str(uuid.uuid4())
        MongoWorkflow.create_catchment_entry(simulation_id=sim_taskid, catchment_id=catchment_id,
                                             catchment_input=catchment_input, status="PENDING",
                                             dependencies=dependencies)
        MongoWorkflow.add_catchment(simulation_id=sim_taskid, catchment_id=catchment_id, comid=comid)
        return 1, catchment_id
