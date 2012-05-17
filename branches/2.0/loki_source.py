#!/usr/bin/env python

import apsw
import datetime
import ftplib
import httplib
import os
import sys
import time
import zlib
import itertools

from contextlib import contextmanager

import loki_db


class Source():
	
	
	# ##################################################
	# constructor
	
	
	def __init__(self, lokidb):
		assert(isinstance(lokidb, loki_db.Database))
		assert(self.__class__.__name__.startswith('Source_'))
		self._loki = lokidb
		self._db = lokidb._db
		self._dbc = lokidb._db.cursor()
		self._sourceID = self.addSource(self.getSourceName())
		assert(self._sourceID > 0)
	#__init__()
	
	
	# ##################################################
	# source interface
	
	
	def getDependencies(self):
		return tuple()
	#getDependencies()
	
	
	def download(self):
		raise Exception("invalid LOKI Source plugin: download() not implemented")
	#download()
	
	
	def update(self):
		raise Exception("invalid LOKI Source plugin: update() not implemented")
	#update()
	
	
	# ##################################################
	# context managers
	
	
	def __enter__(self):
		return self._loki.__enter__()
	#__enter__()
	
	
	def __exit__(self, excType, excVal, traceback):
		return self._loki.__exit__(excType, excVal, traceback)
	#__exit__()
	
	
	@contextmanager
	def bulkUpdateContext(self, tableList):
		with self._loki:
			if len(tableList) > 0:
				self._loki.dropDatabaseIndexes(None, 'db', tableList)
			yield
			if len(tableList) > 0:
				self._loki.createDatabaseIndexes(None, 'db', tableList)
			if "region_bound" in tableList:
				self._loki.dropDatabaseIndexes(None, 'db', 'region_zone')
				self.updateRegionZones()
				self._loki.createDatabaseIndexes(None, 'db', 'region_zone')
		#with db transaction
	#bulkUpdateContext()
	
	
	# ##################################################
	# instance management
	
	
	def getSourceName(self):
		return self.__class__.__name__[7:]
	#getSourceName()
	
	
	def getSourceID(self):
		return self._sourceID
	#getSourceID()
	
	
	def log(self, message=""):
		return self._loki.log(message)
	#log()
	
	
	def logPush(self, message=None):
		return self._loki.logPush(message)
	#logPush()
	
	
	def logPop(self, message=None):
		return self._loki.logPop(message)
	#logPop()
	
	
	
	# ##################################################
	# metadata management
	
	
	def addNamespace(self, name):
		try:
			self._dbc.execute("INSERT OR ABORT INTO `db`.`namespace` (`namespace`) VALUES (LOWER(?))", (name,))
			return self._db.last_insert_rowid()
		except apsw.ConstraintError:
			return self._loki.getNamespaceID(name)
	#addNamespace()
	
	
	def addPopulation(self, name, comment=None, desc=None):
		try:
			self._dbc.execute("INSERT OR ABORT INTO `db`.`population` (`population`,`ldcomment`,`description`) VALUES (LOWER(?),?,?)", (name,comment,desc))
			return self._db.last_insert_rowid()
		except apsw.ConstraintError:
			return self._loki.getPopulationID(name)
	#addPopulation()
	
	
	def addRelationship(self, name):
		try:
			self._dbc.execute("INSERT OR ABORT INTO `db`.`relationship` (`relationship`) VALUES (LOWER(?))", (name,))
			return self._db.last_insert_rowid()
		except apsw.ConstraintError:
			return self._loki.getRelationshipID(name)
	#addRelationship()
	
	
	def addSource(self, name):
		try:
			self._dbc.execute("INSERT OR ABORT INTO `db`.`source` (`source`) VALUES (LOWER(?))", (name,))
			return self._db.last_insert_rowid()
		except apsw.ConstraintError:
			return self._loki.getSourceID(name)
	#addSource()
	
	
	def addType(self, name):
		try:
			self._dbc.execute("INSERT OR ABORT INTO `db`.`type` (`type`) VALUES (LOWER(?))", (name,))
			return self._db.last_insert_rowid()
		except apsw.ConstraintError:
			return self._loki.getTypeID(name)
	#addType()
	
	
	# ##################################################
	# data management
	
	
	def deleteSourceData(self):
		self._dbc.execute("DELETE FROM `db`.`group` WHERE `source_id` = ?", (self._sourceID,))
		self._dbc.execute("DELETE FROM `db`.`group_name` WHERE `source_id` = ?", (self._sourceID,))
		self._dbc.execute("DELETE FROM `db`.`group_group` WHERE `source_id` = ?", (self._sourceID,))
		self._dbc.execute("DELETE FROM `db`.`group_region` WHERE `source_id` = ?", (self._sourceID,))
		self._dbc.execute("DELETE FROM `db`.`region` WHERE `source_id` = ?", (self._sourceID,))
		self._dbc.execute("DELETE FROM `db`.`region_name` WHERE `source_id` = ?", (self._sourceID,))
		self._dbc.execute("DELETE FROM `db`.`region_bound` WHERE `source_id` = ?", (self._sourceID,))
		self._dbc.execute("DELETE FROM `db`.`snp` WHERE `source_id` = ?", (self._sourceID,))
		self._dbc.execute("DELETE FROM `db`.`snp_merge` WHERE `source_id` = ?", (self._sourceID,))
	#deleteSourceData()
	
	
	def addGroups(self, grpList):
		# grpList=[ (type_id,label,description), ... ]
		retList = []
		for row in self._dbc.executemany(
				"INSERT INTO `db`.`group` (`type_id`,`label`,`description`,`source_id`) VALUES (?,?,?,?); SELECT last_insert_rowid()",
				((grp[0],grp[1],grp[2],self._sourceID) for grp in grpList)
		):
			retList.append(row[0])
		return retList
	#addGroups()
	
	
	def addTypedGroups(self, typeID, grpList):
		# grpList=[ (label,description), ... ]
		retList = []
		for row in self._dbc.executemany(
				"INSERT INTO `db`.`group` (`type_id`,`label`,`description`,`source_id`) VALUES (?,?,?,?); SELECT last_insert_rowid()",
				((typeID,grp[0],grp[1],self._sourceID) for grp in grpList)
		):
			retList.append(row[0])
		return retList
	#addTypedGroups()
	
	
	def addGroupNames(self, nameList):
		# nameList=[ (group_id,namespace_id,name), ... ]
		self._dbc.executemany(
				"INSERT OR IGNORE INTO `db`.`group_name` (`group_id`,`namespace_id`,`name`,`source_id`) VALUES (?,?,?,?)",
				((name[0],name[1],name[2],self._sourceID) for name in nameList)
		)
	#addGroupNames()
	
	
	def addNamespacedGroupNames(self, namespaceID, nameList):
		# nameList=[ (group_id,name), ... ]
		self._dbc.executemany(
				"INSERT OR IGNORE INTO `db`.`group_name` (`group_id`,`namespace_id`,`name`,`source_id`) VALUES (?,?,?,?)",
				((name[0],namespaceID,name[1],self._sourceID) for name in nameList)
		)
	#addNamespacedGroupNames()
	
	
	def addGroupGroups(self, linkList):
		# linkList=[ (group_id,related_group_id,relationship_id), ... ]
		self._dbc.executemany(
				"INSERT OR IGNORE INTO `db`.`group_group` (`group_id`,`related_group_id`,`relationship_id`,`direction`,`source_id`) VALUES (?,?,?,?,?)",
				((link[0],link[1],link[2],1,self._sourceID) for link in linkList)
		)
		self._dbc.executemany(
				"INSERT OR IGNORE INTO `db`.`group_group` (`group_id`,`related_group_id`,`relationship_id`,`direction`,`source_id`) VALUES (?,?,?,?,?)",
				((link[1],link[0],link[2],-1,self._sourceID) for link in linkList)
		)
	#addGroupGroups()
	
	
	def addGroupRegions(self, linkList):
		# linkList=[ (group_id,region_id), ... ]
		self._dbc.executemany(
				"INSERT OR IGNORE INTO `db`.`group_region` (`group_id`,`region_id`,`source_id`) VALUES (?,?,?)",
				((link[0],link[1],self._sourceID) for link in linkList)
		)
	#addGroupRegions()
	
	
	def addRegions(self, regList):
		# regList=[ (type_id,label,description), ... ]
		retList = []
		for row in self._dbc.executemany(
				"INSERT INTO `db`.`region` (`type_id`,`label`,`description`,`source_id`) VALUES (?,?,?,?); SELECT last_insert_rowid()",
				((reg[0],reg[1],reg[2],self._sourceID) for reg in regList)
		):
			retList.append(row[0])
		return retList
	#addRegions()
	
	
	def addTypedRegions(self, typeID, regList):
		# regList=[ (label,description), ... ]
		retList = []
		for row in self._dbc.executemany(
				"INSERT INTO `db`.`region` (`type_id`,`label`,`description`,`source_id`) VALUES (?,?,?,?); SELECT last_insert_rowid()",
				((typeID,reg[0],reg[1],self._sourceID) for reg in regList)
		):
			retList.append(row[0])
		return retList
	#addTypedRegions()
	
	
	def addRegionNames(self, nameList):
		# nameList=[ (region_id,namespace_id,name), ... ]
		self._dbc.executemany(
				"INSERT OR IGNORE INTO `db`.`region_name` (`region_id`,`namespace_id`,`name`,`source_id`) VALUES (?,?,?,?)",
				((name[0],name[1],name[2],self._sourceID) for name in nameList)
		)
	#addRegionNames()
	
	
	def addNamespacedRegionNames(self, namespaceID, nameList):
		# nameList=[ (region_id,name), ... ]
		self._dbc.executemany(
				"INSERT OR IGNORE INTO `db`.`region_name` (`region_id`,`namespace_id`,`name`,`source_id`) VALUES (?,?,?,?)",
				((name[0],namespaceID,name[1],self._sourceID) for name in nameList)
		)
	#addNamespacedRegionNames()
	
	
	def addRegionBounds(self, bndList):
		# bndList=[ (region_id,population_id,chr,posMin,posMax), ... ]
		self._dbc.executemany(
				"INSERT OR IGNORE INTO `db`.`region_bound` (`region_id`,`population_id`,`chr`,`posMin`,`posMax`,`source_id`) VALUES (?,?,?,?,?,?)",
				((bnd[0],bnd[1],bnd[2],min(bnd[3],bnd[4]),max(bnd[3],bnd[4]),self._sourceID) for bnd in bndList)
		)
	#addRegionBounds()
	
	
	def addPopulationRegionBounds(self, populationID, bndList):
		# bndList=[ (region_id,chr,posMin,posMax), ... ]
		self._dbc.executemany(
				"INSERT OR IGNORE INTO `db`.`region_bound` (`region_id`,`population_id`,`chr`,`posMin`,`posMax`,`source_id`) VALUES (?,?,?,?,?,?)",
				((bnd[0],populationID,bnd[1],min(bnd[2],bnd[3]),max(bnd[2],bnd[3]),self._sourceID) for bnd in bndList)
		)
	#addPopulationRegionBounds()
	
	
	def updateRegionZones(self):
		for row in self._dbc.execute("SELECT MAX(`posMax`) FROM `db`.`region_bound`"):
			maxZone = int(row[0]) / 100000
		self._dbc.execute("CREATE TEMP TABLE `temp`.`zones` (`zone` INTEGER PRIMARY KEY NOT NULL)")
		self._dbc.executemany("INSERT INTO `temp`.`zones` (`zone`) VALUES (?)", ((zone,) for zone in xrange(maxZone+1)))
		self._dbc.execute("DELETE FROM `db`.`region_zone`")
		self._dbc.execute("""
INSERT OR IGNORE INTO `db`.`region_zone` (`region_id`,`population_id`,`chr`,`zone`)
SELECT rb.`region_id`, rb.`population_id`, rb.`chr`, tz.`zone`
FROM `db`.`region_bound` AS rb
JOIN `temp`.`zones` AS tz
  ON tz.`zone` >= rb.`posMin` / ?
  AND tz.`zone` <= rb.`posMax` / ?
""", (100000,100000))
		self._dbc.execute("DROP TABLE `temp`.`zones`")
	#updateRegionZones()
	
	
	def addSNPs(self, snpList):
		# snpList=[ (rs,chr,pos), ... ]
		self._dbc.executemany(
				"INSERT INTO `db`.`snp` (`rs`,`chr`,`pos`,`source_id`) VALUES (?,?,?,?)",
				((snp[0],snp[1],snp[2],self._sourceID) for snp in snpList)
		)
	#addChromosomeSNPs()
	
	
	def addChromosomeSNPs(self, chromosome, snpList):
		# snpList=[ (rs,pos), ... ]
		self._dbc.executemany(
				"INSERT INTO `db`.`snp` (`rs`,`chr`,`pos`,`source_id`) VALUES (?,?,?,?)",
				((snp[0],chromosome,snp[1],self._sourceID) for snp in snpList)
		)
	#addChromosomeSNPs()
	
	
	def addSNPMerges(self, mergeList):
		# mergeList=[ (rsOld,rsNew,rsCur), ... ]
		self._dbc.executemany(
				"INSERT INTO `db`.`snp_merge` (`rsOld`,`rsNew`,`rsCur`,`source_id`) VALUES (?,?,?,?)",
				((merge[0],merge[1],merge[2],self._sourceID) for merge in mergeList)
		)
	#addSNPMerges()
	
	def addBuildTrans(self, build_pairs):
		"""
		Adds the build->assembly pairs into the build_assembly table
		"""
		self._dbc.executemany(
			"INSERT OR IGNORE INTO 'db'.'build_assembly' ('build','assembly') VALUES (?,?)",
			(tuple(ba_pair) for ba_pair in build_pairs)
		)
	#addBuildTrans()
	
	def addChains(self, assembly, chain_list):
		"""
		Adds all of the chains described in chain_list and returns the
		ids of the added chains.  The chain_list must be an iterable
		container of objects that can be inserted into the chain table
		"""
		
		retList = []
		for row in self._dbc.executemany(
			"INSERT INTO 'db'.'chain' ('old_assembly','score','old_chr','old_start','old_end','new_chr','new_start','new_end','is_fwd') VALUES (?,?,?,?,?,?,?,?,?); SELECT last_insert_rowid()",
			(tuple(c for c in itertools.chain((assembly,),chain)) for chain in chain_list)
		):
			retList.append(row[0])
		
		return retList;
	#addChains()
	
	def addChainData(self, chain_data_list):
		"""
		Adds all of the chain data into the chain data table
		"""
		self._dbc.executemany(
			"INSERT INTO 'db'.'chain_data' ('chain_id','old_start','old_end','new_start') VALUES (?,?,?,?)",
			(tuple(cd) for cd in chain_data_list)
		)
	#addChainData()
			
	
	
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
	
	
	def findConnectedComponents(self, neighbors):
		f = set()
		c = list()
		for v in neighbors:
			if v not in f:
				f.add(v)
				c.append(self._findConnectedComponents_recurse(neighbors, v, f, {v}))
		return c
	#findConnectedComponents()
	
	
	def _findConnectedComponents_recurse(self, n, v, f, c):
		for u in n[v]:
			if u not in f:
				f.add(u)
				c.add(u)
				self._findConnectedComponents_recurse(n, v, f, c)
		return c
	#_findConnectedComponents_recurse()
	
	
	def findEdgeDisjointCliques(self, neighbors):
		# neighbors = {'a':{'b','c'}, 'b':{'a'}, 'c':{'a'}, ...}
		# 'a' not in neighbors['a']
		# 'b' in neighbors['a'] => 'a' in neighbors['b']
		
		# clone neighbors so we can modify the local copy
		n = { v:set(neighbors[v]) for v in neighbors }
		c = list()
		
		while True:
			# prune isolated vertices and extract hanging pairs
			for v in n.keys():
				try:
					if len(n[v]) == 0:
						del n[v]
					elif len(n[v]) == 1:
						u, = n[v]
						n[v].add(v)
						c.append(n[v])
						del n[v]
						n[u].remove(v)
						if len(n[u]) == 0:
							del n[u]
				except KeyError:
					pass
			#foreach vertex
			
			# if nothing remains, we're done
			if len(n) == 0:
				return c
			
			# find maximal cliques on the remaining graph
			cliques = self.findMaximalCliques(n)
			
			# add disjoint cliques to the solution and remove the covered edges from the graph
			cliques.sort(key=len, reverse=True)
			for clique in cliques:
				ok = True
				for v in clique:
					if len(n[v] & clique) != len(clique) - 1:
						ok = False
						break
				if ok:
					c.append(clique)
					for v in clique:
						n[v] -= clique
			#foreach clique
		#loop
	#findEdgeDisjointCliques()
	
	
	def findMaximalCliques(self, neighbors):
		# neighbors = {'a':{'b','c'}, 'b':{'a'}, 'c':{'a'}, ...}
		# 'a' not in neighbors['a']
		# 'b' in neighbors['a'] => 'a' in neighbors['b']
		#
		# this implementation of the Bron-Kerbosch algorithm incorporates the
		# top-level degeneracy ordering described in:
		#   Listing All Maximal Cliques in Sparse Graphs in Near-optimal Time
		#   David Eppstein, Maarten Loeffler, Darren Strash
		
		# build vertex-degree and degree-vertices maps
		vd = dict()
		dv = list()
		for v in neighbors:
			d = len(neighbors[v])
			vd[v] = d
			while len(dv) <= d:
				dv.append(set())
			dv[d].add(v)
		#foreach vertex
		
		# compute degeneracy ordering
		o = list()
		while len(dv) > 0:
			for dvSet in dv:
				try:
					v = dvSet.pop()
				except KeyError:
					continue
				o.append(v)
				vd[v] = None
				for u in neighbors[v]:
					if vd[u]:
						dv[vd[u]].remove(u)
						vd[u] -= 1
						dv[vd[u]].add(u)
				while len(dv) > 0 and len(dv[-1]) == 0:
					dv.pop()
				break
			#for dvSet in dv (until dvSet is non-empty)
		#while dv remains
		vd = dv = None
		
		# run first recursion layer in degeneracy order
		p = set(o)
		x = set()
		c = list()
		for v in o:
			self._findMaximalCliques_recurse({v}, p & neighbors[v], x & neighbors[v], neighbors, c)
			p.remove(v)
			x.add(v)
		return c
	#findMaximalCliques()
	
	
	def _findMaximalCliques_recurse(self, r, p, x, n, c):
		if len(p) == 0:
			if len(x) == 0:
				return c.append(r)
		else:
			# cursory tests yield best performance by choosing the pivot
			# arbitrarily from x first if x is not empty, else p; also tried
			# picking from p always, picking the pivot with highest degree,
			# and picking the pivot earliest in degeneracy order
			u = iter(x).next() if (len(x) > 0) else iter(p).next()
			for v in (p - n[u]):
				self._findMaximalCliques_recurse(r | {v}, p & n[v], x & n[v], n, c)
				p.remove(v)
				x.add(v)
	#_findMaximalCliques_recurse()
	
	
	# remFiles={'filename.ext':'/path/on/remote/host/to/filename.ext',...}
	def downloadFilesFromFTP(self, remHost, remFiles):
		# check local file sizes and times, and identify all needed remote paths
		remDirs = set()
		remSize = {}
		remTime = {}
		locSize = {}
		locTime = {}
		for locPath in remFiles:
			remDirs.add(remFiles[locPath][0:remFiles[locPath].rfind('/')])
			remSize[locPath] = None
			remTime[locPath] = None
			locSize[locPath] = None
			locTime[locPath] = None
			if os.path.exists(locPath):
				stat = os.stat(locPath)
				locSize[locPath] = long(stat.st_size)
				locTime[locPath] = datetime.datetime.fromtimestamp(stat.st_mtime)
		
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
						time = datetime.datetime.strptime("%s %d" % (timestamp,now.year),'%b %d %H:%M %Y')
					except ValueError:
						try:
							time = datetime.datetime.strptime("%s %d" % (timestamp,now.year-1),'%b %d %H:%M %Y')
						except ValueError:
							time = now
					if (
							(time.year == now.year and time.month > now.month) or
							(time.year == now.year and time.month == now.month and time.day > now.day)
					):
						time = time.replace(year=now.year-1)
				remTime[words[8]] = time
		
		# connect to source server
		self.log("connecting to FTP server %s ..." % remHost)
		ftp = ftplib.FTP(remHost)
		ftp.login() # anonymous
		self.log(" OK\n")
		
		# check remote file sizes and times
		self.log("identifying changed files ...")
		for remDir in remDirs:
			ftp.dir(remDir, ftpDirCB)
		self.log(" OK\n")
		
		# download files as needed
		self.logPush("downloading changed files ...\n")
		for locPath in sorted(remFiles.keys()):
			if remSize[locPath] == locSize[locPath] and remTime[locPath] <= locTime[locPath]:
				self.log("%s: up to date\n" % locPath)
			else:
				self.log("%s: downloading ..." % locPath)
				#TODO: download to temp file, then rename?
				with open(locPath, 'wb') as locFile:
					ftp.cwd(remFiles[locPath][0:remFiles[locPath].rfind('/')])
					ftp.retrbinary('RETR '+locPath, locFile.write)
				self.log(" OK\n")
			modTime = time.mktime(remTime[locPath].timetuple())
			os.utime(locPath, (modTime,modTime))
		
		# disconnect from source server
		try:
			ftp.quit()
		except Exception:
			ftp.close()
		self.logPop("... OK\n")
	#downloadFilesFromFTP()
	
	
	# remFiles={'filename.ext':'/path/on/remote/host/to/filename.ext',...}
	def downloadFilesFromHTTP(self, remHost, remFiles):
		# check local file sizes and times
		remSize = {}
		remTime = {}
		locSize = {}
		locTime = {}
		for locPath in remFiles:
			remSize[locPath] = None
			remTime[locPath] = None
			locSize[locPath] = None
			locTime[locPath] = None
			if os.path.exists(locPath):
				stat = os.stat(locPath)
				locSize[locPath] = long(stat.st_size)
				locTime[locPath] = datetime.datetime.fromtimestamp(stat.st_mtime)
		
		# check remote file sizes and times
		self.log("identifying changed files ...")
		for locPath in remFiles:
			try:
				http = httplib.HTTPConnection(remHost)
				http.request('HEAD', remFiles[locPath])
				response = http.getresponse()
			except IOError as e:
				self.log(" ERROR: %s" % e)
				return False
			
			content_length = response.getheader('content-length')
			if content_length:
				remSize[locPath] = long(content_length)
			
			last_modified = response.getheader('last-modified')
			if last_modified:
				try:
					remTime[locPath] = datetime.datetime.strptime(last_modified,'%a, %d %b %Y %H:%M:%S %Z')
				except ValueError:
					remTime[locPath] = datetime.datetime.now()
			
			http.close()
		self.log(" OK\n")
		
		# download files as needed
		self.logPush("downloading changed files ...\n")
		for locPath in sorted(remFiles.keys()):
			if remSize[locPath] and remSize[locPath] == locSize[locPath] and remTime[locPath] and remTime[locPath] <= locTime[locPath]:
				self.log("%s: up to date\n" % locPath)
			else:
				self.log("%s: downloading ..." % locPath)
				#TODO: download to temp file, then rename?
				with open(locPath, 'wb') as locFile:
					http = httplib.HTTPConnection(remHost)
					http.request('GET', remFiles[locPath])
					response = http.getresponse()
					while True:
						data = response.read()
						if not data:
							break
						locFile.write(data)
					http.close()
				self.log(" OK\n")
			if remTime[locPath]:
				modTime = time.mktime(remTime[locPath].timetuple())
				os.utime(locPath, (modTime,modTime))
		self.logPop("... OK\n")
	#downloadFilesFromHTTP()
	
	
#Source
