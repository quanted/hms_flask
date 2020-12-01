from flask import Response
from flask_restful import Resource, reqparse, request
import sqlite3
import datetime
import json

cn_db = "/src/hms-data/curvenumber.sqlite3"

parser_base = reqparse.RequestParser()


def get_db_connection():
    """
    Connect to sqlite database located at db_path
    :return: sqlite connection
    """
    conn = sqlite3.connect(cn_db)
    conn.isolation_level = None
    return conn


class HMSCurveNumberData(Resource):
    """

    """
    parser = parser_base.copy()
    parser.add_argument('comid', type=str, required=True)
    parser.add_argument('type', choices=('avg', 'both'))

    def get(self):
        args = self.parser.parse_args()
        comid = args.comid
        type = args.type
        try:
            conn = get_db_connection()
        except Exception:
            return Response("Data Retrieval Error", status=400)
        c = conn.cursor()
        query = "SELECT * FROM CurveNumber WHERE ComID=?"
        c.execute(query, (comid,))
        cn_avg = list(c.fetchone())
        if len(cn_avg) > 0:
            cn_avg.pop(0)
        response_data = {
            "CN-AVG": cn_avg
        }
        if type == "both":
            query = "SELECT CN FROM CurveNumberRaw WHERE ComID=? ORDER BY TimeStep ASC"
            c.execute(query, (comid,))
            cn_raw = [float(i[0]) for i in c.fetchall()]
            response_data["CN-RAW"] = cn_raw

        timestamp = datetime.datetime.now()
        metadata = {
            "comid": comid,
            "temporal-resolution": "16 days",
            "time-span": "01-01-2001 to 12-31-2017",
            "request-time": str(timestamp),
            "algorithm-link": "placeholder-url",
            "publication-link": "placeholder-url",
            "metadata-link": "placeholder-url"
        }
        response_data["metadata"] = metadata
        return Response(json.dumps(response_data), status=200)
