from __future__ import division
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from math import pi, sqrt, cos, log10
import os

class ConstantVolume:
    # def timeSeries(startDate=None, endDate=None, timestep=None, boundarycondition=None):
        # generate time series

    def constantVolume(self):
        time_s = np.arange(startDate, endDate, timestep)
        nsegments = segments
        boundaryFlow = boundary_flow
        Q_out = pd.DataFrame(np.zeros((len(time_s), nsegments)))
        Q_out['t'] = time_s
        for t in range((len(time_s))):
            for i in range(nsegments):
                Q_out.iloc[t, i + 2] = boundary_flow  # + tributary flow
