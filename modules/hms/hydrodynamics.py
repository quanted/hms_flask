from __future__ import division
import numpy as np
from datetime import date, datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd
from math import pi, sqrt, cos, log10
import os


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

class FlowRouting:
    def __init__(self, startDate=None, endDate=None, timestep=None, boundary_flow=None, segments=None):
        self.startDate = startDate
        self.endDate = endDate
        self.timestep = float(timestep)
        self.boundary_flow = float(boundary_flow)
        self.segments = int(float(segments))

    def constantVolume(self):
        time_s = pd.to_datetime([dt.strftime('%Y-%m-%d %H:%M') for dt in
                                 datetime_range(datetime.strptime(self.startDate, '%Y-%m-%d'),
                                                datetime.strptime(self.endDate, '%Y-%m-%d'),
                                                timedelta(minutes=int(float(self.timestep) * 24 * 60)))])
        Q_out = pd.DataFrame(np.zeros((len(time_s), int(float(self.segments)))))
        Q_out['DateTime'] = time_s
        cols = Q_out.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        Q_out = Q_out[cols]
        for t in range((len(time_s))):
            for i in range(self.segments):
                Q_out.iloc[t, i + 1] = self.boundary_flow  # + tributary flow
        #print(Q_out.to_json(orient="split"))
        return Q_out.to_json(orient="split") #print df.reset_index().to_json(orient='records')

    # def changingVolume(self):
    #     time_s = pd.to_datetime([dt.strftime('%Y-%m-%d %H:%M') for dt in
    #                              datetime_range(datetime.strptime(self.startDate, '%Y-%m-%d'),
    #                                             datetime.strptime(self.endDate, '%Y-%m-%d'),
    #                                             timedelta(minutes=int(float(self.timestep) * 24 * 60)))])
    #     Q_out = pd.DataFrame(np.zeros((len(time_s), int(float(self.segments)))))
    #     Q_out['DateTime'] = time_s
    #     cols = Q_out.columns.tolist()
    #     cols = cols[-1:] + cols[:-1]
    #     Q_out = Q_out[cols]
    #     for t in range((len(time_s))):
    #         for i in range(self.segments):
    #             Q_out.iloc[t, i + 1] = self.boundary_flow  # + tributary flow
    #     #print(Q_out.to_json(orient="split"))
    #     return Q_out.to_json(orient="split")