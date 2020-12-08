import copy
import json
import numpy as np
import re
import os
import struct
import datetime
from wg_utility import *
import sys
from pytz import timezone

BASE = '/users/bgreenwood/SMODE/waveglider'
WEBPATH = '/Library/Webserver/Documents/SMODE/waveglider'

# Covert Campbell 2-byte floating point number to python float
# FP2 provides approx. 3 significant digits of accuracy. It is used for most measurements 
def fp2(dec):
  # swap bytes to match endian
  b1 = dec & 0x00FF
  b2 = dec & 0xFF00
  dec = (b1 << 8) + (b2 >> 8)

  base = dec & 8191
  if ( dec & 32768):
    base = -base
  if ( dec & 8192 ):
    base *= 0.1
  if ( dec & 16384 ):
    base *= 0.01
  if ( base == -8190 ):
    base = np.nan
  return base

# Telemetry base class used to decode, read, and write data from CR6 WGMS telemetry
class telemetry:
	# Constructs takes 2 parameters: vehicle name i.e. 'SV3-1043' name 'data'
	# If data is single integer, constructor loads data from .csv file corresponding to data = table number
	# If data is array of raw binary data, constructor processes raw data 
	def __init__(self,vehicle,data):
		self.vehicle = vehicle
		self.packets = set()
		if isinstance(data,int):
			self.type = data
			self.json_file = WEBPATH + '/' + vehicle + '_%02X.json' % self.type
			self.file = WEBPATH + '/' + vehicle + '_%02X.log' % self.type
			self.load()
		else:
			self.type = data[12]
			self.json_file = WEBPATH + '/' + vehicle + '_%02X.json' % self.type
			self.file = WEBPATH + '/' + vehicle + '_%02X.log' % self.type
			self.load()
			self.process(data)

	# Load records from file matching packet type
	def load(self):
		if self.type == 0x21:
			p = packet_21()
		elif self.type == 0x22:
			p = packet_22()
		elif self.type == 0x23:
			p = packet_23()
		elif self.type == 0x24:
			p = packet_24()
		elif self.type == 0x11:
			p = packet_11()
		else:
			print('Unknown packet type: %d' % self.type)
			return
		#print('Found packet %02X' % self.type)
		if os.path.exists(self.file):
			print('load file: %s' % self.file)
			fin = open(self.file,'r')
			fin.readline() # skip creation timestamp line
			fin.readline() # skip header description line
			for line in fin.readlines():
				p.read(line)
				self.packets.add(copy.copy(p))
	# Decode packet
	def process(self,data):
		if self.type == 0x21:
			p = packet_21()
		elif self.type == 0x22:
			p = packet_22()
		elif self.type == 0x23:
			p = packet_23()
		elif self.type == 0x24:
			p = packet_24()
			indx = 13
			while (indx + 14 <= len(data)):
				p.decode(data[indx:indx+14])
				self.packets.add(copy.copy(p))
				indx = indx + 14
			return
		elif self.type == 0x11:
			p = packet_11()
		else:
			print('unknown packet type: %d' % self.type)
			return
		p.decode(data)
		self.packets.add(copy.copy(p))
	# overload [] operator to return all records of type key
	def __getitem__(self,key):
		return map(lambda p : getattr(p,key), self.packets)
	def keys(self):
		return next(iter(self.packets)).vars
	def __str__(self):
		s = list(self.packets)[0].header()
		for p in sorted(self.packets): # print packets in sorted order -- overload __cmp__
			s += str(p)
			s += "\n"
		return s
	def write(self):
		if len(self.packets):
			fout = open(self.file,'w')
			fout.write('# created: %s\n' % datetime.datetime.utcnow())
			fout.write(str(self))
			fout.close()
	def write_json(self):
		fout = open(self.json_file,'w')
		data = []
		for packet in self.packets:
			rec = {}
			for var in packet.vars:
				rec[var] = getattr(packet,var)
			data.append(copy.copy(rec))
		json.dump(data,fout)

# Base class
class packet:
	def read(self,line):
		data = re.split(r'\s+',line)
		for d,v,format in zip(data,self.vars,self.format):
			if 'f' in format:
				setattr(self,v,float(d))
			else:
				setattr(self,v,d)
		self.t = datetime.datetime.strptime(self.time,'%Y/%m/%dT%H:%M:%SZ')
	def __hash__(self):
		val = int(self.t.strftime("%s"))
		return hash(val)
	def __eq__(self,other):
		return isinstance(other,packet) and self.t == other.t
	def __cmp__(self,other):
		return cmp(self.t,other.t)
	def __str__(self):
		str = ""
		for f,v in zip(self.format,self.vars):
			str += f % getattr(self,v) + ' '
		return str
	def header(self):
		str = ""
		for f,v in zip(self.format,self.vars):
			width = re.search('[0-9]+',f).group() # extracts first integer from string (should be width)
			str += ('%%%ss ' % width) % v
		return str+'\n'

class packet_21(packet):
	def __init__(self):
		self.vars = ['time','rec','lat','lon','hdg_min','hdg_avg','hdg_max','heave_std','heave','wspd','wdir','atmp','rh','bpr','ctd_temp','ctd_cond','ctd_pres','swr','lwr','indx_1hz','indx_10hz','indx_ctd','indx_RDI','latency']
		self.format = ['%20s','%8.0f','%+9.4f','%+9.4f','%7.1f','%7.1f','%7.1f','%9.3f','%9.3f','%5.1f','%4.0f','%5.1f','%3.0f','%6.1f','%8.1f','%8.4f','%8.1f','%6.1f','%5.1f','%8.0f','%9.0f','%8.0f','%8.0f','%7.0f']
	def decode(self,data):
		d = struct.unpack('<IIIffHHHHHHHHHHffHffHHHH',data[13:(13+66)])
		self.t = datetime.datetime(1990,1,1) + datetime.timedelta(seconds=d[0]) # datestamp is seconds since 1990 
		self.latency = (datetime.datetime.utcnow()-self.t).total_seconds()
		self.time = self.t.strftime('%Y/%m/%dT%H:%M:%SZ')
		self.rec = d[2]
		# Sitex GPS
		self.lat = d[3]
		self.lon = d[4]
		if self.lat < -90 or self.lat > 90:
			self.lat = np.nan
		if self.lon < -180 or self.lon > 180:
			self.lon = np.nan
		self.hdg_min = fp2(d[5])
		self.hdg_avg = fp2(d[6])
		self.hdg_max = fp2(d[7])
		self.heave_std = fp2(d[8])
		self.heave = fp2(d[9])
		
		# WXT
		self.wspd = fp2(d[10])
		self.wdir = fp2(d[11])
		self.atmp = fp2(d[12])
		self.rh = fp2(d[13])
		self.bpr = fp2(d[14])
		# GPCTD
		self.ctd_temp = d[15]
		if self.ctd_temp < -5 or self.ctd_temp > 40:
			self.ctd_temp = np.nan
		self.ctd_cond = d[16]
		if self.ctd_cond < 2 or self.ctd_cond > 7:
			self.ctd_cond = np.nan
		self.ctd_pres = fp2(d[17])
		if self.ctd_pres < 0 or self.ctd_pres > 10:
			self.ctd_pres = np.nan
		# Kipp & Zonen Radiometers
		self.swr = d[18]
		if self.swr < -5 or self.swr > 2000:
			self.swr = np.nan
		self.lwr = d[19]
		if self.lwr < 200 or self.lwr > 500:
			self.lwr = np.nan
		self.indx_1hz = fp2(d[20])
		self.indx_10hz = fp2(d[21])
		self.indx_ctd = fp2(d[22])
		self.indx_RDI = fp2(d[23])
		self.last = '#'

class packet_22(packet):
	def __init__(self):
		self.bins = 34
		self.vars = ['time','rec','roll','pitch','hdg','adcp_temp']
		self.format= ['%20s','%8.0f','%6.1f','%6.1f','%3.0f','%9.1f']
		for bin in range(0,self.bins):
			self.vars.append('cur_e%02d' % bin)
			self.format.append('%8.2f')
		for bin in range(0,self.bins):
			self.vars.append('cur_n%02d' % bin)
			self.format.append('%8.2f')
		self.vars.append('latency')
		self.format.append('%7.0f')
	def decode(self,data):
		d = struct.unpack('<IIIHHHH',data[13:33])
		self.t = (datetime.datetime(1990,1,1) + datetime.timedelta(seconds=d[0])) # datestamp is seconds since 1990
		self.latency = (datetime.datetime.utcnow()-self.t).total_seconds()
		self.time = self.t.strftime('%Y/%m/%dT%H:%M:%SZ')
		self.rec  = d[2]
		self.roll = fp2(d[3])
		self.pitch = fp2(d[4])
		self.hdg = fp2(d[5])
		self.adcp_temp = fp2(d[6])
		for bin in range(0,self.bins):
			vel = fp2(struct.unpack('<H',data[34+(bin*2):36+(bin*2)])[0])
			if vel < -5000 or vel > 5000:
				vel = np.nan
			#print('%d %d %.2f' % (34+(bin*2),35+(bin*2),vel))
			setattr(self,'cur_e%02d'%bin,vel)
		for bin in range(0,self.bins):
			vel = fp2(struct.unpack('<H',data[104+(bin*2):106+(bin*2)])[0])
			if vel < -5000 or vel > 5000:
				vel = np.nan
			#print('%d %d %.2f' % (104+(bin*2),105+(bin*2),vel))
			setattr(self,'cur_n%02d'%bin,vel)

class packet_23(packet):
	def __init__(self):
		self.vars = ['time','rec']
		self.format= ['%20s','%8.0f']
		for bin in range(0,50):
			self.vars.append('Szz%02d' % bin)
			self.format.append('%8.2f')
		for bin in range(0,50):
			self.vars.append('theta%02d' % bin)
			self.format.append('%8.2f')
		self.vars.append('latency')
		self.format.append('%7.0f')
	def decode(self,data):
		d = struct.unpack('<III',data[13:(13+12)])
		self.t = (datetime.datetime(1990,1,1) + datetime.timedelta(seconds=d[0])) # datestamp is seconds since 1990
		self.latency = (datetime.datetime.utcnow()-self.t).total_seconds()
		self.time = self.t.strftime('%Y/%m/%dT%H:%M:%SZ')
		self.rec  = d[2]
		for bin in range(0,50):
			vel = fp2(struct.unpack('<H',data[22+(bin*2):24+(bin*2)])[0])
			#print('%d %d %.2f' % (22+(bin*2),24+(bin*2),vel))
			setattr(self,'Szz%02d'%bin,vel)
		for bin in range(0,50):
			vel = fp2(struct.unpack('<H',data[122+(bin*2):124+(bin*2)])[0])
			#print('%d %d %.2f' % (122+(bin*2),124+(bin*2),vel))
			setattr(self,'theta%02d'%bin,vel)

class packet_24(packet):
	def __init__(self):
		self.vars = ['time','pres','temp','cond','latency']
		self.format = ['%20s','%7.2f','%5.2f','%6.4f','%7.0f']
	def decode(self,data):
		#print('packet 24: length %d' % len(data))
		d = struct.unpack('<IIHHH',data[0:14])
		self.t = (datetime.datetime(1990,1,1) + datetime.timedelta(seconds=d[0])) # datestamp is seconds since 1990 
		self.latency = (datetime.datetime.utcnow()-self.t).total_seconds()
		self.time = self.t.strftime('%Y/%m/%dT%H:%M:%SZ')
		# Note TableFile 5 format: TOB1 w/TimeStamp w/o Record
		self.pres = fp2(d[2])
		if self.pres < -10 or self.pres > 10:
			self.pres = np.nan
		self.temp = fp2(d[3])
		if self.temp < -5 or self.temp > 40:
			self.temp = np.nan
		self.cond = fp2(d[4])
		if self.cond < 2 or self.cond > 7:
			self.cond = np.nan

class packet_11(packet):
	def __init__(self):
		self.vars = ['time','rec','batt','CR6_temp','proc_max','proc_avg','vnav_hdg','vnav_pitch','vnav_roll','vnav_vn','vnav_ve','vnav_vd','gill_AWS','gill_AWD','gill_TWS','gill_TWD','gill_temp','roll_min','roll_avg','roll_max','pitch_min','pitch_avg','pitch_max','head_min','head_avg','head_max','N_dbar','N_degC','indx_10hz','indx_gill','indx_adcp','indx_20hz','latency']
		self.format = ['%20s','%8.0f','%5.2f','%8.2f','%8.2f','%8.2f','%8.2f','%10.2f','%9.2f','%7.2f','%7.2f','%7.2f','%8.2f','%8.2f','%8.2f','%8.2f','%9.2f','%8.3f','%8.3f','%8.3f','%9.3f','%9.3f','%9.3f','%8.3f','%8.3f','%8.3f','%6.2f','%6.2f','%9.0f','%9.0f','%9.0f','%9.0f','%9.0f','%7.0f']
	def decode(self,data):
		print('packet 11: length %d' % len(data))
		d = struct.unpack('<IIIHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH',data[13:(13+72)])
		self.t = (datetime.datetime(1990,1,1) + datetime.timedelta(seconds=d[0])) # datestamp is seconds since 1990 
		self.latency = (datetime.datetime.utcnow()-self.t).total_seconds()
		self.time = self.t.strftime('%Y/%m/%dT%H:%M:%SZ')
		self.rec = d[2]
		self.batt = fp2(d[3])
		self.CR6_temp = fp2(d[4])
		self.proc_max = fp2(d[5])
		self.proc_avg = fp2(d[6])
		self.vnav_hdg = fp2(d[7])
		self.vnav_pitch = fp2(d[8])
		self.vnav_roll = fp2(d[9])
		self.vnav_vn = fp2(d[10])
		self.vnav_ve = fp2(d[11])
		self.vnav_vd = fp2(d[12])
		self.gill_AWD = fp2(d[14])
		self.gill_AWS = fp2(d[13])
		self.gill_TWS = fp2(d[15])
		self.gill_TWD = fp2(d[16])
		self.gill_temp = fp2(d[17])
		self.roll_min = fp2(d[18])
		self.roll_avg = fp2(d[19])
		self.roll_max = fp2(d[20])
		self.pitch_min = fp2(d[21])
		self.pitch_avg = fp2(d[22])
		self.pitch_max = fp2(d[23])
		self.head_min = fp2(d[24])
		self.head_avg = fp2(d[25])
		self.head_max = fp2(d[26])
		self.N_dbar = fp2(d[27])
		self.N_degC = fp2(d[28])
		self.indx_10hz = fp2(d[29])
		self.indx_gill = fp2(d[30])
		self.indx_adcp = fp2(d[31])
		self.indx_20hz = fp2(d[32])
