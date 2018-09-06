#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''fetch FMI climate data, parse and import into pandas dataframe
__author__ = "Antti Ruonakoski"
__license__ = "GPL"
'''
import numpy as np
import datetime
from fmidatafetch import *
from bokeh.plotting import figure, output_file, show, save
from bokeh.models import ColumnDataSource, MonthsTicker, LinearAxis, DatetimeTicker
from bokeh.palettes import Spectral11, Spectral6, Spectral10,RdBu11
from bokeh.transform import linear_cmap

#env_test = True
env_test = False
if __name__ == "__main__":

	wfs_connection = init_connection()
	for station in stations:
		if not env_test:
			result = get_features(wfs_connection, station)
			temp_measurements_data = parse_features(result, station)
		else:
			temp_measurements_data = test_data
			print (temp_measurements_data)

	df = frame_data(temp_measurements_data)
	print (df.describe(include='all'))

	output_file("x.html")

	source = ColumnDataSource(df)
	mapper = linear_cmap(field_name='value', palette=Spectral11, low=-14 ,high=21)

	p = figure(title='Monthly average temps',plot_width=800, plot_height=1200, y_axis_type='datetime',x_range=(-25,25))

	#p.yaxis.ticker = DatetimeTicker(desired_num_ticks=44)
	p.yaxis.ticker = MonthsTicker(months=np.arange(1,13))

	p.hbar(y='time', right='value', height=datetime.timedelta(days=31), left=-25, color=mapper, source=source)

	save(p,filename='tmp/x.html')