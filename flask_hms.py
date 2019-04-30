from flask import Flask
from flask_restful import Api, Resource
import os
import logging

# Import modules
from hms_flask.modules import hms_controller

app = Flask(__name__)
app.config.update(
    DEBUG=True
)
api = Api(app)

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
os.environ.update({
    'PROJECT_ROOT': PROJECT_ROOT
})
logging.basicConfig(level=logging.DEBUG)


class StatusTest(Resource):
    def get(self):
        return {"status": "flask_hms up and running."}


base_url = "https://localhost:7777/hms"
logging.info(" flask_hms started: live endpoints")
logging.info(base_url + "/gis")
api.add_resource(hms_controller.HMSFlaskTest, '/gis/test/')

# HMS endpoints
# Data retrieval endpoint
api.add_resource(hms_controller.HMSTaskData, '/data')
logging.info(base_url + "/gis/ncdc/stations/")
api.add_resource(hms_controller.NCDCStationSearch, '/gis/ncdc/stations/')
logging.info(base_url + "/gis/percentage/")
api.add_resource(hms_controller.NLDASGridCells, '/gis/percentage/')

logging.info(base_url + "/proxy/<model>/")
api.add_resource(hms_controller.ProxyDNC2, '/proxy/<path:model>/')

logging.info(base_url + "/hydrodynamic/constant_volume/")
api.add_resource(hms_controller.Hydrodynamics, '/hydrodynamic/constant_volume/')

logging.info(base_url + "/nwm/data/")
api.add_resource(hms_controller.NWMDownload, '/nwm/data/')

#logging.info(base_url + "/hydrodynamics/constant_volume/")
#api.add_resource(hms_controller.Hydrodynamics.constant_volume, '/hydrodynamics/constant_volume/')

if __name__ == '__main__':
    app.run(port=7777, debug=True)
