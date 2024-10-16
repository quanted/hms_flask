from flask import Flask, make_response, request
from flask_restful import Api, Resource
import os
import logging

# Import modules
from hms_flask.modules import hms_controller, hms_data
from hms_flask.modules.hms.locate_timezone import get_timezone

from logging.config import dictConfig

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'DEBUG',
        'handlers': ['wsgi']
    }
})


app = Flask(__name__)
app.config.update(
    DEBUG=True
)

api = Api(app)

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
os.environ.update({
    'PROJECT_ROOT': PROJECT_ROOT
})
# logging.basicConfig(level=logging.DEBUG)


class StatusTest(Resource):
    def get(self):
        return {"status": "flask_hms up and running."}


base_url = "https://localhost:7777/hms"
logging.info(" flask_hms started: live endpoints")
logging.info(base_url + "/gis")
api.add_resource(hms_controller.HMSFlaskTest, '/gis/test/')

logging.info(base_url + "/gis/tz")
api.add_resource(hms_controller.HMSGetTZ, '/gis/tz/')

# HMS endpoints
# Data retrieval endpoint
api.add_resource(hms_controller.HMSTaskData, '/data')

logging.info(base_url + "/task/revoke/")
api.add_resource(hms_controller.HMSRevokeTask, '/task/revoke/')

logging.info(base_url + "/gis/ncdc/stations/")
api.add_resource(hms_controller.NCDCStationSearch, '/gis/ncdc/stations/')
logging.info(base_url + "/gis/percentage/")
api.add_resource(hms_controller.NLDASGridCells, '/gis/percentage/')

logging.info(base_url + "/proxy/<model>/")
api.add_resource(hms_controller.ProxyDNC2, '/proxy/<path:model>/')

logging.info(base_url + "/nwm/data/")
api.add_resource(hms_controller.NWMDownload, '/nwm/data/')
logging.info(base_url + "/nwm/forecast/short_term")
api.add_resource(hms_controller.NWMDataShortTerm, "/nwm/forecast/short_term")

logging.info(base_url + "/data/curvenumber/")
api.add_resource(hms_data.HMSCurveNumberData, "/data/curvenumber/")

# logging.info(base_url + "/workflow/")
# api.add_resource(hms_controller.HMSWorkflow, "/workflow/")
logging.info(base_url + "/workflow/status/")
api.add_resource(hms_controller.HMSWorkflow.Status, "/workflow/status/")
logging.info(base_url + "/workflow/data/")
api.add_resource(hms_controller.HMSWorkflow.Data, "/workflow/data/")
logging.info(base_url + "/workflow/compute/")
api.add_resource(hms_controller.HMSWorkflow.Simulation, "/workflow/compute/")
logging.info(base_url + "/workflow/download/")
api.add_resource(hms_controller.HMSWorkflow.Download, "/workflow/download/")


if __name__ == '__main__':
    app.run(port=7777, debug=True)
