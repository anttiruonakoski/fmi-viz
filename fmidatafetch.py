#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''fetch FMI climate data, parse and import into pandas dataframe
__author__ = "Antti Ruonakoski"
__license__ = "GPL"
'''

env_test = False

from owslib.wfs import WebFeatureService
import xml.etree.ElementTree as ET
import pandas as pd
#from lxml import etree #lxml why u no accept iostring?
#from io import StringIO, BytesIO
#import copy
#from pprint import pprint
import argparse
from datetime import date
from collections import namedtuple

apikey_filename = 'apikey'

test_data = {
			'time' :['2017-10-01T00:00:00Z','2017-11-01T00:00:00Z','2017-12-01T00:00:00Z','2018-01-01T00:00:00Z','2018-02-01T00:00:00Z','2018-03-01T00:00:00Z','2018-04-01T00:00:00Z'],
			'value': ['2.1','-3.0','-7.5','-9.6','-13.5','-8.3','1.6']
			}

namespaces = {
'gco': 'http://www.isotc211.org/2005/gco',
'gmd': 'http://www.isotc211.org/2005/gmd',
'gml': 'http://www.opengis.net/gml/3.2',
'gmlcov': 'http://www.opengis.net/gmlcov/1.0',
'om': 'http://www.opengis.net/om/2.0',
'ompr': 'http://inspire.ec.europa.eu/schemas/ompr/3.0',
'omso': 'http://inspire.ec.europa.eu/schemas/omso/3.0',
'sam': 'http://www.opengis.net/sampling/2.0',
'sams': 'http://www.opengis.net/samplingSpatial/2.0',
'swe': 'http://www.opengis.net/swe/2.0',
'target': 'http://xml.fmi.fi/namespace/om/atmosphericfeatures/1.0',
'wfs': 'http://www.opengis.net/wfs/2.0',
'wml2': 'http://www.opengis.net/waterml/2.0',
'xlink': 'http://www.w3.org/1999/xlink',
'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

stations = ['100949']
#stations = ['101920', '101933', '101065','100949']
storedquery = namedtuple ('storedquery', 'queryname maxinterval')
storedquery_monthly = storedquery('fmi::observations::weather::monthly::timevaluepair', 87600)
storedquery_daily = storedquery('fmi::observations::weather::daily::timevaluepair', 100000) # correct this once known
storedquery_used = storedquery_monthly

# some sane defaults
storedqueryparams = {'starttime' : '2015-01-01T00:00:00Z', 'fmisid': '101933'}
storedqueryparams['endtime'] = str(date.today())

query_specs = {}

def get_apikey(filename):
    try:
        with open(apikey_filename, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        print("'%s' file not found" % filename)

def init_connection(apikey):
	try:
		c = WebFeatureService(url='http://data.fmi.fi/fmi-apikey/' + apikey + '/wfs', version='2.0.0')
		print ('connection ok')
		return c
	except Exception as e:
		print ('error in WFS connection', e)

def calculate_parts(maxinterval='storedquery_used.maxinterval', starttime='starttime', endtime='endtime', *args):
	if maxinterval < int(hours(starttime - endtime)):
		#chunck
		return list(chuncks)
	else:
		return list(chunck)

def get_features(connection, storedqueryparams):
	try:
		#calculate_parts(storedquery_used, storedqueryparams)
		r = connection.getfeature(storedQueryID = storedquery_used.queryname, storedQueryParams = storedqueryparams)
		#alt format
		#r = connection.getfeature(storedQueryID='fmi::observations::weather::monthly::simple', storedQueryParams={'fmisid':'101928'})
		print ('get features ok')
		return r

		#needs to handle
		#<ExceptionText>Too long time interval requested!</ExceptionText>
		#<ExceptionText>No more than 87600.000000 hours allowed.</ExceptionText>

	except Exception as e:
		print ('error getting WFS features', wfs_connection, storedquery_used.queryname, storedqueryparams, e)

def pull_namespaces(result):
	duplicate_result = copy.copy(result)
	ns = dict([node for _, node in ET.iterparse(duplicate_result, events=['start-ns'])])
	return ns

def parse_features(result):

	temp_measurements = {
						'time': [],
						'value': []
						}
	try:
		# handled by dict
		# namespaces = pull_namespaces(result)
		doc = ET.parse(result).getroot()

		print (doc)

		station_name = doc.findall(".//gml:name[@codeSpace='http://xml.fmi.fi/namespace/locationcode/name']", namespaces)[0].text
		print (station_name)
		#station_name=""

		# if queryname == 'fmi::observations::weather::daily::timevaluepair' id on joku tday
		entry = doc.findall(".//wml2:MeasurementTimeseries[@gml:id='obs-obs-1-1-tmon']", namespaces)[0]

		if entry:
			for measurement in entry.iterfind(".//wml2:MeasurementTVP", namespaces):
				temp_measurements['time'].append(measurement.find("wml2:time", namespaces).text)
				temp_measurements['value'].append(measurement.find("wml2:value", namespaces).text)
				#print (time, value)
			return (temp_measurements, station_name)
		else:
			# if queryname == 'fmi::observations::weather::daily::timevaluepair' parse myös maxvalue ja minvalue
			return

	except Exception as e:
		print ('GML parse error', e)

def frame_data(data):
	try:
		df = pd.DataFrame.from_records(data)
		df['value'] = df['value'].apply(pd.to_numeric, errors='coerce')
		df['time'] = df['time'].apply(pd.to_datetime)
		return df

		# handle Nan values
	except Exception as e:
		print ('Error pandas data frame', e)

def fetch_dataframe(station='station', starttime='starttime', endtime='endtime', **kwargs):

	print (kwargs)
	apikey = get_apikey(apikey_filename)
	wfs_connection = init_connection(apikey)

	if starttime:
		storedqueryparams['starttime'] = starttime + 'T00:00:00Z'

	if endtime:
		storedqueryparams['endtime'] = endtime + 'T00:00:00Z'

	try:
		for fmisid in [station]:
			if env_test == False:
				storedqueryparams['fmisid'] = str(fmisid)
				print (storedqueryparams)
				result = get_features(wfs_connection, storedqueryparams)
				# print (result)
				parsed_data = parse_features(result)
				temp_measurements_data = parsed_data[0]
				station_name = parsed_data[1]
			else:
				temp_measurements_data = test_data
				#print (temp_measurements_data)

		if len(temp_measurements_data) > 0:
			print (temp_measurements_data)
			dfd = frame_data(temp_measurements_data)
			dfd['name'] = station_name
			print (dfd.describe(include='all'))
			return dfd
		else:
			print ('no data')
			return

	except Exception as e:
		print ('Error', e)

if __name__ == "__main__":

	parser = argparse.ArgumentParser(
    description="havaintojen aikasarjat Ilmatieteen laitoksen WFS-rajapinnasta", prog='fmidatafetch'
    )

	parser.add_argument(
	type=int,
	help="säähavaintoaseman koodi",
	dest='station')

	parser.add_argument('-m','--mm',
	help="lataa kuukausihavainnot (oletus)",
	dest='monthly', action='store_true', default=True)

	parser.add_argument('-d','--dd',
	help="lataa paivahavainnot",
	dest='daily', action='store_true')

	parser.add_argument('-b','--begin',
	help="havaintosarjan ensimmäinen päivä yyyy-mm-dd muodossa",
	dest='starttime', required=False)

	parser.add_argument('-e','--end',
	help="havaintosarjan viimeinen päivä yyyy-mm-dd muodossa (oletus tänään)",
	dest='endtime', required=False)

	args = vars(parser.parse_args())
	print (args)

	df = fetch_dataframe(**args)

	try:
		if df is not None:
			df.to_pickle("./tmp/dummy.pkl")
	except Exception as e:
		print ('Error', e)






