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
    # Need to add area, depth, vel, vol. 
    
    #def perimeter_calc(shape, d, wide, z_slope):
    #   if shape =='tz':#for trapezoid
    #       perim=wide +2 *d*(1 + z_slope**2)**0.5    
    #   elif shape =='r':#for rectangle
    #       perim=2*d + wide
    #   elif  shape =='t':#for triangle
    #       perim=2 *d*(1 + z_slope**2)**0.5
    #   return perim  
    
    # def kinematicwave(self):
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
    #             p_calc=perimeter_calc(model_segs['ChanGeom'][i],HP_depth.iloc[t,i+2], model_segs['bot_width'][i], model_segs['z_slope'][i])
    #             alpha_data.iloc[t,i]=((model_segs['mannings_n'][i]/model_segs['chan_slope'][i]**0.5)*(p_calc**(2/3)))**beta
    #              Q_hat = Q_out.iloc[t, i+2]
    #              Q_out.iloc[t+1, i+2] = Q_out.iloc[t, i+2] + ((Q_out.iloc[t+1, i+1] - Q_out.iloc[t, i+2])*(delta_t))/(model_segs['length'][i]*alpha_data.iloc[t, i]*beta*Q_hat**(beta-1))
    #              while Q_hat - Q_out.iloc[t+1, i+2] > 1e-3:  # implicit soln for Q_out.iloc[t+1, i+2]
    #                    Q_hat = Q_out.iloc[t+1, i+2]
    #                    Q_out.iloc[t+1, i+2] = Q_out.iloc[t, i+2] + ((Q_out.iloc[t+1, i+1] - Q_out.iloc[t, i+2])*(delta_t))/(model_segs['length'][i]*alpha_data.iloc[t, i]*beta*Q_hat**(beta-1))
             #else Q_hat - Q_out.iloc[t+1, i+2] <= 1e-3:
            #Q_out.iloc[t+1, i+2] = Q_out.iloc[t, i+2] + ((Q_out.iloc[t+1, i+1] - Q_out.iloc[t, i+2])*(delta_t))/(model_segs['length'][i]*alpha_data.iloc[t, i]*beta*Q_hat**(beta-1))

    #     #print(Q_out.to_json(orient="split"))
    #     return Q_out.to_json(orient="split")
