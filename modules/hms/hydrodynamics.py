from __future__ import division
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from math import pi, sqrt, cos, log10
import os

class ConstantVolume:
    def __init__(self, startDate=None, endDate=None, timestep=None, boundary_flow=None, segments=None):
        self.startDate = startDate
        self.endDate = endDate
        self.timestep = timestep
        self.boundary_flow = boundary_flow
        self.segments = segments

    def constantVolume(self):
        time_s = np.arange(startDate, endDate, timestep)
        Q_out = pd.DataFrame(np.zeros((len(time_s), segments)))
        Q_out['t'] = time_s
        for t in range((len(time_s))):
            for i in range(segments):
                Q_out.iloc[t, i + 2] = boundary_flow  # + tributary flow
        return Q_out.to_json(orient="split")
