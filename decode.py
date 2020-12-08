import os
#from fp2 import decode_CR6,fp2
from packets import *
import sys
import mysql.connector
import pdb
import datetime
import struct
import numpy as np
import json
#from waveglider_lib import *
from wg_utility import *
BASE='/Users/bgreenwood/SMODE/waveglider'
WEBDIR='/Library/Webserver/Documents/SMODE/waveglider'
THREDDS='/Volumes/Macintosh HD 3/thredds/public/SMODE/insitu/waveglider'


#smode_gliders = ['PLANCK (SV3-256)','KELVIN (AIRSEA2)','SV3-1043 WH','STOKES (SV3-1041)']
#smode_gliders = ['PLANCK','KELVIN','SV3-1043','STOKES']
smode_gliders = ['SV3-1043']

# Load waveglider data. If file does not already exist, create empty dict
data_file=BASE+'/sioairsea.json'
if os.path.exists(data_file):
	gliders = json.load(open(data_file))
else:
	print("  +Creating new file: %s" % data_file)
	gliders={}
	for g in smode_gliders:
		gliders[g] = {'time':[],'lat':[],'lon':[],'atmp':[],'bpr':[],'wspd':[],'gust':[],'wdir':[],'depth':[],'stmp':[],'cndc':[],'latency':[]}

for g in gliders:
  if 'latency' not in gliders[g]:
    gliders[g]['latency']=[]
    sz = len(gliders[g]['time'])
    for i in range(0,sz):
      gliders[g]['latency'].append(float('nan'))


# Read latest data from wgms line by line. If glider belongs to list above, add to dict. Otherwise, ignore
raw = open( BASE + '/sioairsea.csv','r')
lines = raw.readlines()
for line in lines[1:]:
	field = line.split(',')
	time = float(datetime.datetime.strptime(field[0],'%m/%d/%Y %H:%M').strftime("%s"))

	vehicle = field[1].split(' ')[0]
	if not vehicle in smode_gliders:
		print('Unknown waveglider: %s; ignore.' %vehicle)
		continue

	payload = field[3]
	data = bytearray.fromhex(field[6])
	#print('%s %-20s: ' % (time.strftime('%Y/%m/%d %H:%M:%S'),payload)),
	if payload == 'Weather En Pressure':
		gliders[vehicle]['time'].append(time)
		gliders[vehicle]['atmp'].append(struct.unpack('<H',data[4:6])[0]/ 10.0)
		gliders[vehicle]['bpr'].append(struct.unpack('<H',data[6:8])[0]/10.0)
		gliders[vehicle]['lat'].append(struct.unpack('<i',data[8:12])[0]/60e4)
		gliders[vehicle]['lon'].append(struct.unpack('<i',data[12:16])[0]/60e4)
		gliders[vehicle]['wspd'].append(struct.unpack('<H',data[16:18])[0]/10.0)
		gliders[vehicle]['gust'].append(struct.unpack('<H',data[18:20])[0]/10.0)
		gliders[vehicle]['wdir'].append(struct.unpack('<H',data[22:24])[0]/10.0)
		gliders[vehicle]['depth'].append(np.nan)
		gliders[vehicle]['stmp'].append(np.nan)
		gliders[vehicle]['cndc'].append(np.nan)
		gliders[vehicle]['latency'].append((datetime.datetime.utcnow()-datetime.datetime.fromtimestamp(time)).total_seconds())
		#print('bpr:%.1f atmp:%.1f wspd:%0.1f gust:%0.1f wdir:%0.1f lat:%.4f lon:%.4f' %(bpr,atmp,wspd,gust,wdir,lat,lon))
		print('%s: lat:%.4f lon:%.4f' % (vehicle,gliders[vehicle]['lat'][-1],gliders[vehicle]['lon'][-1]))
	elif payload == 'SCRIPPS':
		t = telemetry(vehicle,data)
		t.write()
		t.write_json()
	elif payload == 'AISyGLocation':
		if len(data)>58:
			mmsi1 = struct.unpack('<I',data[7:11])[0]
			lat1 = struct.unpack('<i',data[11:15])[0]/60e4
			lon1 = struct.unpack('<i',data[15:19])[0]/60e4
			mmsi2 = struct.unpack('<I',data[33:37])[0]
			lat2 = struct.unpack('<i',data[37:41])[0]/60e4
			lon2 = struct.unpack('<i',data[41:45])[0]/60e4
			print('mmsi1:%u lat1:%.4f lon1:%.4f mmsi2:%u lat2:%.4f lon2:%.4f' % (mmsi1,lat1,lon1,mmsi2,lat2,lon2))
		else:
			mmsi1 = struct.unpack('<I',data[7:11])[0]
			lat1 = struct.unpack('<i',data[11:15])[0]/60e4
			lon1 = struct.unpack('<i',data[15:19])[0]/60e4
			print('mmsi1:%u lat1:%.4f lon1:%.4f' % (mmsi1,lat1,lon1))
	#else:
	#	print('Unknown payload: %s' % payload)

for vehicle in smode_gliders:
	print('  %s:' % vehicle)
	#plot_waveglider_pos(vehicle,gliders[vehicle],WEBDIR)
	#plot_waveglider_vars(vehicle,gliders[vehicle],WEBDIR)
	write_waveglider_netcdf(vehicle,gliders[vehicle],THREDDS)
	write_waveglider_mat(vehicle,gliders[vehicle],WEBDIR)
	update_waveglider_mysql(vehicle,gliders[vehicle])

write_waveglider_kml(gliders,WEBDIR)

# remove duplicates, sort, save data
for g in gliders:
	ordered,unique = np.unique(gliders[g]['time'],return_index=True,) # identify unique entries using time as key
	for key in gliders[g]:
		gliders[g][key] = [ gliders[g][key][i] for i in unique ] # removes duplicates and sorts
json.dump(gliders,open(data_file,'w'))

print ('%s Completed.' % datetime.datetime.utcnow().strftime("%y/%m/%d:%H:%M:%S"))

'''
	elif payload == '202':
		gliders[vehicle]['time'].append(time)
		gliders[vehicle]['lat'].append(struct.unpack('<i',data[0:4])[0]/60e4)
		gliders[vehicle]['lon'].append(struct.unpack('<i',data[4:8])[0]/60e4)
		gliders[vehicle]['atmp'].append(np.nan)
		gliders[vehicle]['bpr'].append(np.nan)
		gliders[vehicle]['wspd'].append(np.nan)
		gliders[vehicle]['gust'].append(np.nan)
		gliders[vehicle]['wdir'].append(np.nan)
		gliders[vehicle]['depth'].append(np.nan)
		gliders[vehicle]['stmp'].append(np.nan)
		gliders[vehicle]['cndc'].append(np.nan)
		gliders[vehicle]['latency'].append((datetime.datetime.utcnow()-datetime.datetime.fromtimestamp(time)).total_seconds())

		ctd_time = datetime.datetime.fromtimestamp( struct.unpack('<I',data[8:12])[0] )
		# 2020/01/28 PTS not parsed correctly
		#pres = struct.unpack('<I',data[12:16])[0]/100-10	# dbar
		#stmp = struct.unpack('<I',data[16:20])[0]/1e4-5	# degC
		#cndc = struct.unpack('<I',data[20:24])[0]/1e5-.05	# S/m
		print('%s: lat:%.4f lon:%.4f' % (vehicle,gliders[vehicle]['lat'][-1],gliders[vehicle]['lon'][-1]))
'''
