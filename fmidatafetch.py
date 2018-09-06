#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''fetch FMI climate data, parse and import into pandas dataframe
__author__ = "Antti Ruonakoski"
__license__ = "GPL"
'''

env_test = True

from owslib.wfs import WebFeatureService
import xml.etree.ElementTree as ET
import pandas as pd
#from lxml import etree #lxml why u no accept iostring?
#from io import StringIO, BytesIO
import copy

apikey_filename = 'apikey'

test_data = {
			'time' :['2017-10-01T00:00:00Z','2017-11-01T00:00:00Z','2017-12-01T00:00:00Z','2018-01-01T00:00:00Z','2018-02-01T00:00:00Z','2018-03-01T00:00:00Z','2018-04-01T00:00:00Z'],
			'value': ['2.1','-3.0','-7.5','-9.6','-13.5','-8.3','1.6']
			}

stations = ['100949']
#stations = ['101920', '101933', '101065','100949']
storedqueryid = 'fmi::observations::weather::monthly::timevaluepair'
storedqueryparams = {'fmisid' : '100949', 'starttime' : '2010-01-01T00:00:00Z'}

def get_apikey(filename):
    try:
        with open(apikey_filename, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        print("'%s' file not found" % filename)

apikey = get_apikey(apikey_filename)

def init_connection():
	try:
		c = WebFeatureService(url='http://data.fmi.fi/fmi-apikey/' + apikey + '/wfs', version='2.0.0')
		print ('connection ok')
		return c
	except Exception as e:
		print ('error in WFS connection', e)

def get_features(connection, station):
	try:
		r = connection.getfeature(storedQueryID = storedqueryid, storedQueryParams = storedqueryparams)
		#(storedQueryID='fmi::observations::weather::monthly::simple', storedQueryParams={'fmisid':'101928'}
		#r = connection.getfeature(storedQueryID='fmi::observations::weather::monthly::simple', storedQueryParams={'fmisid':'101928'})
		print ('get features ok')
		return r

		#needs to handle
		#<ExceptionText>Too long time interval requested!</ExceptionText>
		#<ExceptionText>No more than 87600.000000 hours allowed.</ExceptionText>

	except Exception as e:
		print ('error getting WFS features', wfs_connection, storedqueryid, storedqueryparams, e)

def parse_features(result, station):

	temp_measurements = {
						'time': [],
						'value': []
						}

	try:
		duplicate_result = copy.copy(result)
		namespaces = dict([node for _, node in ET.iterparse(duplicate_result, events=['start-ns'])])
		print ('all namespaces in document\n', namespaces)

		doc = ET.parse(result).getroot()

		location_name=""

		entry = doc.findall(".//wml2:MeasurementTimeseries[@gml:id='obs-obs-1-1-tmon']", namespaces)[0]

		for measurement in entry.iterfind(".//wml2:MeasurementTVP", namespaces):
			temp_measurements['time'].append(measurement.find("wml2:time", namespaces).text)
			temp_measurements['value'].append(measurement.find("wml2:value", namespaces).text)
#			print (time, value)
		return temp_measurements

	except Exception as e:
		print ('GML parse error', e)

def frame_data(data):
	try:
		df = pd.DataFrame.from_records(data)
		df['value'] = df['value'].apply(pd.to_numeric)
		df['time'] = df['time'].apply(pd.to_datetime)
		return df

		# handle Nan values
	except Exception as e:
		print ('Error pandas data frame', e)


if __name__ == "__main__":

	wfs_connection = init_connection()
	for station in stations:
		if not env_test:
			result = get_features(wfs_connection, station)
			temp_measurements_data = parse_features(result, station)
		else:
			temp_measurements_data = test_data
			print (temp_measurements_data)

		df = pd.DataFrame.from_records(temp_measurements_data)
		df['value'] = df['value'].apply(pd.to_numeric)
		df['time'] = df['time'].apply(pd.to_datetime)
		#df = pd.DataFrame.from_dict(temp_measurements_data, dtype = 'float64')
		#data1 = pd.Series(parse_features(result, station), dtype = 'float64')
		#data2 = pd.to_numeric(pd.Series(parse_features(result, station)), errors = 'coerce')
		#print (df.describe(include='all'))




