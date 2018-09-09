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
from bokeh.models import ColumnDataSource, MonthsTicker, LinearAxis, DatetimeTicker, Range1d
from bokeh.palettes import Spectral11, Spectral6, Spectral10,RdBu11
from bokeh.transform import linear_cmap

#env_test = True
env_test = False

if __name__ == "__main__":

	df = fetch_dataframe()

	#print (df)
	#print (df.describe(include='all'))
	output_file("x.html")

	source = ColumnDataSource(df)

	first_date = min(source.data['time']) - pd.Timedelta(60, unit='d')
	last_date = max(source.data['time']) + pd.Timedelta(60, unit='d')
	row_count = df.shape[0]-1

	print (first_date,last_date,row_count)

	mapper = linear_cmap(field_name='value', palette=Spectral11, low=-14 ,high=21)

	p = figure(
		title='Monthly average temperature @ ' + df['name'][0],plot_height=480,
		plot_width=row_count*11+100, x_axis_type='datetime',
		y_range=Range1d(-25, 25, bounds="auto"),
		x_range=(first_date,last_date),
		tools='xpan, xwheel_zoom'
		)

	#p.yaxis.ticker = DatetimeTicker(desired_num_ticks=44)
	p.xaxis.ticker = MonthsTicker(months=np.arange(1,13))
	p.xaxis.major_label_orientation = "vertical"

	p.vbar(x='time', top='value', width=datetime.timedelta(days=31), bottom=-25, color=mapper, source=source)

	save(p,filename='tmp/x.html')