#!/usr/bin/env python

#package loki

import sys
import os
import datetime
import ftplib
import time
import zlib

import loki.db


class Source():
	
	# ##################################################
	# constructor
	
	def __init__(self, lokidb, sourceID):
		assert(isinstance(lokidb, loki.db.Database))
		assert(sourceID > 0)
		self._loki = lokidb
		self._sourceID = sourceID
		self._verbose = False
	#__init__()
	
	
	# ##################################################
	# instance management
	
	def setVerbose(self, verbose=True):
		self._verbose = verbose
	#setVerbose()
	
	
	# ##################################################
	# source interface
	
	def download(self):
		raise Exception("invalid LOKI Source plugin: download() not implemented")
	#download()
	
	
	def update(self):
		raise Exception("invalid LOKI Source plugin: update() not implemented")
	#update()
	
	
	# ##################################################
	# source utility methods
	
	def zfile(self, fileName, splitChar="\n", chunkSize=1*1024*1024):
		dc = zlib.decompressobj(zlib.MAX_WBITS | 32) # autodetect gzip or zlib header
		with open(fileName,'rb') as filePtr:
			text = ""
			loop = True
			while loop:
				data = filePtr.read(chunkSize)
				if data:
					text += dc.decompress(data)
					data = None
				else:
					text += dc.flush()
					loop = False
				if text:
					lines = text.split(splitChar)
					i,x = 0,len(lines)-1
					text = lines[x]
					while i < x:
						yield lines[i]
						i += 1
					lines = None
			#while data remains
			if text:
				yield text
		#with fileName
	#zfile()
	
	
	# unlike split(), delim must be specified and only the first character of will be considered
	def split_escape(self, string, delim, escape=None, limit=0, reverse=False):
		tokens = []
		current = ""
		escaping = False
		
		# parse string
		for char in string:
			if escaping:
				current += char
				escaping = False
			elif (escape) and (char == escape):
				escaping = True
			elif char == delim[0]:
				tokens.append(current)
				current = ""
			else:
				current += char
		if current != "":
			tokens.append(current)
		
		# re-merge the splits that exceed the limit
		if (limit > 0) and (len(tokens) > (limit + 1)):
			if reverse:
				tokens[0:-limit] = [ delim[0].join(tokens[0:-limit]) ]
			else:
				tokens[limit:] = [ delim[0].join(tokens[limit:]) ]
		
		return tokens
	#split_escape()
	
	
	def rsplit_escape(self, string, delim, escape=None, limit=0):
		return self.split_escape(string, delim, escape, limit, True)
	#rsplit_escape()
	
	
	# remFiles={'filename.ext':'/path/on/remote/host/',...}
	def downloadFilesFromFTP(self, remHost, remFiles):
		# check local file sizes and times, and identify all needed remote paths
		remPaths = set()
		remSize = {}
		remTime = {}
		locSize = {}
		locTime = {}
		for locFile in remFiles:
			remPaths.add(remFiles[locFile])
			remSize[locFile] = None
			remTime[locFile] = None
			locSize[locFile] = None
			locTime[locFile] = None
			if os.path.exists(locFile):
				stat = os.stat(locFile)
				locSize[locFile] = long(stat.st_size)
				locTime[locFile] = datetime.datetime.fromtimestamp(stat.st_mtime)
		
		# define FTP directory list parser
		now = datetime.datetime.now()
		def ftpDirCB(line):
			words = line.split()
			if len(words) >= 9 and words[8] in remFiles:
				remSize[words[8]] = long(words[4])
				timestamp = ' '.join(words[5:8])
				try:
					time = datetime.datetime.strptime(timestamp,'%b %d %Y')
				except ValueError:
					try:
						time = datetime.datetime.strptime(timestamp,'%b %d %H:%M')
						time = time.replace(year=now.year)
						while time > now:
							time = time.replace(year=time.year - 1)
					except ValueError:
						time = now
				remTime[words[8]] = time
		
		# connect to source server
		if self._verbose:
			sys.stderr.write("connecting to FTP server %s ..." % remHost)
			sys.stderr.flush()
		ftp = ftplib.FTP(remHost)
		ftp.login() # anonymous
		if self._verbose:
			sys.stderr.write(" OK\n")
		
		# check remote file sizes and times
		if self._verbose:
			sys.stderr.write("checking current file versions ...\n")
			sys.stderr.flush()
		for remPath in remPaths:
			ftp.dir(remPath, ftpDirCB)
		
		# download files as needed
		for locFile in remFiles:
			if remSize[locFile] == locSize[locFile] and remTime[locFile] <= locTime[locFile]:
				if self._verbose:
					sys.stderr.write("  %s: up to date\n" % locFile)
			else:
				if self._verbose:
					sys.stderr.write("  %s: downloading ..." % locFile)
					sys.stderr.flush()
				#TODO: download to temp file, then rename?
				with open(locFile, 'wb') as filePtr:
					ftp.cwd(remFiles[locFile])
					ftp.retrbinary('RETR '+locFile, filePtr.write)
				if self._verbose:
					sys.stderr.write(" OK\n")
			modTime = time.mktime(remTime[locFile].timetuple())
			os.utime(locFile, (modTime,modTime))
		
		# disconnect from source server
		try:
			ftp.quit()
		except Exception:
			ftp.close()
		
		if self._verbose:
			sys.stderr.write("OK\n")
	#downloadFilesFromFTP()
	
#Source
