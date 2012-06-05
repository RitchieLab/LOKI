#!/usr/bin/env python

import apsw
import datetime
import ftplib
import httplib
import os
import time
import zlib
import itertools

import loki_db


class Source(object):
	
	
	# ##################################################
	# constructor
	
	
	def __init__(self, lokidb):
		assert(isinstance(lokidb, loki_db.Database))
		assert(self.__class__.__name__.startswith('Source_'))
		self._loki = lokidb
		self._db = lokidb._db
		self._sourceID = self.addSource(self.getSourceName())
		assert(self._sourceID > 0)
	#__init__()
	
	
	# ##################################################
	# source interface
	
	
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
	
	
	# ##################################################
	# logging
	
	
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
	# instance management
	
	
	def getSourceName(self):
		return self.__class__.__name__[7:]
	#getSourceName()
	
	
	def getSourceID(self):
		return self._sourceID
	#getSourceID()
	
	
	# ##################################################
	# database update
	
	
	def prepareTableForUpdate(self, table):
		return self._loki.prepareTableForUpdate(table)
	#prepareTableUpdate()
	
	
	def prepareTableForQuery(self, table):
		return self._loki.prepareTableForQuery(table)
	#prepareTableQuery()
	
	
	# ##################################################
	# metadata management
	
	
	def addNamespace(self, name, polyregion=0):
		result = self._loki.getNamespaceID(name)
		if not result:
			self._db.cursor().execute("INSERT INTO `db`.`namespace` (`namespace`,`polyregion`) VALUES (LOWER(?),?)", (name,polyregion))
			result = self._db.last_insert_rowid()
		return result
	#addNamespace()
	
	
	def addNamespaces(self, namespaces):
		# namespaces=[ (namespace,polyregion), ... ]
		result = self._loki.getNamespaceIDs(n[0] for n in namespaces)
		for n in namespaces:
			if not result[n[0]]:
				self._db.cursor().execute("INSERT INTO `db`.`namespace` (`namespace`,`polyregion`) VALUES (LOWER(?),?)", n)
				result[n[0]] = self._db.last_insert_rowid()
		return result
	#addNamespaces()
	
	
	def addPopulation(self, name, ldcomment=None, desc=None):
		result = self._loki.getPopulationID(name)
		if not result:
			self._db.cursor().execute("INSERT INTO `db`.`population` (`population`,`ldcomment`,`description`) VALUES (LOWER(?),?,?)", (name,ldcomment,desc))
			result = self._db.last_insert_rowid()
		return result
	#addPopulation()
	
	
	def addPopulations(self, populations):
		# populations=[ (population,ldcomment,description), ... ]
		result = self._loki.getPopulationIDs(p[0] for p in populations)
		for p in populations:
			if not result[p[0]]:
				self._db.cursor().execute("INSERT INTO `db`.`population` (`population`,`ldcomment`,`description`) VALUES (LOWER(?),?,?)", p)
				result[p[0]] = self._db.last_insert_rowid()
		return result
	#addPopulations()
	
	
	def addRelationship(self, name):
		result = self._loki.getRelationshipID(name)
		if not result:
			self._db.cursor().execute("INSERT INTO `db`.`relationship` (`relationship`) VALUES (LOWER(?))", (name,))
			result = self._db.last_insert_rowid()
		return result
	#addRelationship()
	
	
	def addRelationships(self, relationships):
		# relationships=[ (relationship,), ... ]
		result = self._loki.getRelationshipIDs(r[0] for r in relationships)
		for r in relationships:
			if not result[r[0]]:
				self._db.cursor().execute("INSERT INTO `db`.`relationship` (`relationship`) VALUES (LOWER(?))", r)
				result[r[0]] = self._db.last_insert_rowid()
		return result
	#addRelationships()
	
	
	def addRole(self, name, description=None, coding=None, exon=None):
		result = self._loki.getRoleID(name)
		if not result:
			self._db.cursor().execute("INSERT INTO `db`.`role` (`role`,`description`,`coding`,`exon`) VALUES (LOWER(?),?,?,?)", (name,description,coding,exon))
			result = self._db.last_insert_rowid()
		return result
	#addRole()
	
	
	def addRoles(self, roles):
		# roles=[ (role,description,coding,exon), ... ]
		result = self._loki.getRoleIDs(r[0] for r in roles)
		for r in roles:
			if not result[r[0]]:
				self._db.cursor().execute("INSERT INTO `db`.`role` (`role`,`description`,`coding`,`exon`) VALUES (LOWER(?),?,?,?)", r)
				result[r[0]] = self._db.last_insert_rowid()
		return result
	#addRoles()
	
	
	def addSource(self, name):
		result = self._loki.getSourceID(name)
		if not result:
			self._db.cursor().execute("INSERT INTO `db`.`source` (`source`) VALUES (LOWER(?))", (name,))
			result = self._db.last_insert_rowid()
		return result
	#addSource()
	
	
	def addSources(self, sources):
		# sources=[ (source,), ... ]
		result = self._loki.getSourceIDs(s[0] for s in sources)
		for s in sources:
			if not result[s[0]]:
				self._db.cursor().execute("INSERT INTO `db`.`source` (`source`) VALUES (LOWER(?))", s)
				result[s[0]] = self._db.last_insert_rowid()
		return result
	#addSources()
	
	
	def addType(self, name):
		result = self._loki.getTypeID(name)
		if not result:
			self._db.cursor().execute("INSERT INTO `db`.`type` (`type`) VALUES (LOWER(?))", (name,))
			result = self._db.last_insert_rowid()
		return result
	#addType()
	
	
	def addTypes(self, types):
		# types=[ (type,), ... ]
		result = self._loki.getTypeIDs(t[0] for t in types)
		for t in types:
			if not result[t[0]]:
				self._db.cursor().execute("INSERT INTO `db`.`type` (`type`) VALUES (LOWER(?))", t)
				result[t[0]] = self._db.last_insert_rowid()
		return result
	#addTypes()
	
	
	# ##################################################
	# data management
	
	
	def deleteAll(self):
		dbc = self._db.cursor()
		tables = [
			'group', 'group_name', 'group_group', 'group_region_name',
			'region', 'region_name', 'region_name_name', 'region_bound',
			'snp', 'snp_merge', 'snp_role_entrez'
		]
		args = (self.getSourceID(),)
		for table in tables:
			exists = max( row[0] for row in dbc.execute("SELECT COUNT() FROM (SELECT 1 FROM `db`.`%s` WHERE `source_id` = ? LIMIT 1)" % table, args) )
			if exists:
				self.prepareTableForUpdate(table)
				dbc.execute("DELETE FROM `db`.`%s` WHERE `source_id` = ?" % table, args)
	#deleteAll()
	
	
	def addGroups(self, groups):
		# groups=[ (type_id,label,description), ... ]
		self.prepareTableForUpdate('group')
		extra = (self.getSourceID(),)
		return [
			row[0] for row in self._db.cursor().executemany(
				"INSERT INTO `db`.`group` (`type_id`,`label`,`description`,`source_id`) VALUES (?,?,?,?); SELECT last_insert_rowid()",
				(g+extra for g in groups)
			)
		]
	#addGroups()
	
	
	def addTypedGroups(self, typeID, groups):
		# groups=[ (label,description), ... ]
		self.prepareTableForUpdate('group')
		extra = (typeID,self.getSourceID(),)
		return [
			row[0] for row in self._db.cursor().executemany(
				"INSERT INTO `db`.`group` (`label`,`description`,`type_id`,`source_id`) VALUES (?,?,?,?); SELECT last_insert_rowid()",
				(g+extra for g in groups)
			)
		]
	#addTypedGroups()
	
	
	def addGroupNames(self, groupnames):
		# groupnames=[ (group_id,namespace_id,name), ... ]
		self.prepareTableForUpdate('group_name')
		extra = (self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`group_name` (`group_id`,`namespace_id`,`name`,`source_id`) VALUES (?,?,?,?)",
				(gn+extra for gn in groupnames)
		)
	#addGroupNames()
	
	
	def addGroupNamespacedNames(self, namespaceID, groupnames):
		# groupnames=[ (group_id,name), ... ]
		self.prepareTableForUpdate('group_name')
		extra = (namespaceID,self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`group_name` (`group_id`,`name`,`namespace_id`,`source_id`) VALUES (?,?,?,?)",
				(gn+extra for gn in groupnames)
		)
	#addGroupNamespacedNames()
	
	
	def addGroupRelationships(self, grouprels):
		# grouprels=[ (group_id,related_group_id,relationship_id), ... ]
		self.prepareTableForUpdate('group_group')
		extra = (1,self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`group_group` (`group_id`,`related_group_id`,`relationship_id`,`direction`,`source_id`) VALUES (?,?,?,?,?)",
				(gr+extra for gr in grouprels)
		)
		extra = (-1,self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`group_group` (`group_id`,`related_group_id`,`relationship_id`,`direction`,`source_id`) VALUES (?,?,?,?,?)",
				(gr+extra for gr in grouprels)
		)
	#addGroupRelationships()
	
	
	def addGroupRegions(self, groupregions):
		# group_region is now a derived table; nothing should be inserted directly
		raise Exception('addGroupsRegions() is restricted')
		
		# groupregions=[ (group_id,region_id), ... ]
		self.prepareTableForUpdate('group_region')
		extra = (self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`group_region` (`group_id`,`region_id`,`source_id`) VALUES (?,?,?)",
				(gr+extra for gr in groupregions)
		)
	#addGroupRegions()
	
	
	def addGroupRegionNames(self, groupregionnames):
		# groupregionnames=[ (group_id,member,type_id,namespace_id,name), ... ]
		self.prepareTableForUpdate('group_region_name')
		extra = (self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`group_region_name` (`group_id`,`member`,`type_id`,`namespace_id`,`name`,`source_id`) VALUES (?,?,?,?,?,?)",
				(grn+extra for grn in groupregionnames)
		)
	#addGroupRegionNames()
	
	
	def addGroupTypedRegionNamespacedNames(self, typeID, namespaceID, groupregionnames):
		# groupregionnames=[ (group_id,member,name), ... ]
		self.prepareTableForUpdate('group_region_name')
		extra = (typeID,namespaceID,self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`group_region_name` (`group_id`,`member`,`name`,`type_id`,`namespace_id`,`source_id`) VALUES (?,?,?,?,?,?)",
				(grn+extra for grn in groupregionnames)
		)
	#addGroupTypedRegionNamespacedNames()
	
	
	def addRegions(self, regions):
		# regions=[ (type_id,label,description), ... ]
		self.prepareTableForUpdate('region')
		extra = (self.getSourceID(),)
		return [
			row[0] for row in self._db.cursor().executemany(
				"INSERT INTO `db`.`region` (`type_id`,`label`,`description`,`source_id`) VALUES (?,?,?,?); SELECT last_insert_rowid()",
				(r+extra for r in regions)
			)
		]
	#addRegions()
	
	
	def addTypedRegions(self, typeID, regions):
		# regions=[ (label,description), ... ]
		self.prepareTableForUpdate('region')
		extra = (typeID,self.getSourceID(),)
		return [
			row[0] for row in self._db.cursor().executemany(
				"INSERT INTO `db`.`region` (`label`,`description`,`type_id`,`source_id`) VALUES (?,?,?,?); SELECT last_insert_rowid()",
				(r+extra for r in regions)
			)
		]
	#addTypedRegions()
	
	
	def addRegionNames(self, regionnames):
		# regionnames=[ (region_id,namespace_id,name), ... ]
		self.prepareTableForUpdate('region_name')
		extra = (self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`region_name` (`region_id`,`namespace_id`,`name`,`source_id`) VALUES (?,?,?,?)",
				(rn+extra for rn in regionnames)
		)
	#addRegionNames()
	
	
	def addRegionNamespacedNames(self, namespaceID, regionnames):
		# regionnames=[ (region_id,name), ... ]
		self.prepareTableForUpdate('region_name')
		extra = (namespaceID,self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`region_name` (`region_id`,`name`,`namespace_id`,`source_id`) VALUES (?,?,?,?)",
				(rn+extra for rn in regionnames)
		)
	#addRegionNamespacedNames()
	
	
	def addRegionNameNames(self, regionnamenames):
		# regionnamenames=[ (old_type_id,old_namespace_id,old_name,new_namespace_id,new_name), ... ]
		self.prepareTableForUpdate('region_name_name')
		extra = (self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`region_name_name` (`type_id`,`namespace_id`,`name`,`new_namespace_id`,`new_name`,`source_id`) VALUES (?,?,?,?,?,?)",
				(rnn+extra for rnn in regionnamenames)
		)
	#addRegionNameNames()
	
	
	def addRegionTypedNameNamespacedNames(self, typeID, newNamespaceID, regionnamenames):
		# regionnamenames=[ (old_namespace_id,old_name,new_name), ... ]
		self.prepareTableForUpdate('region_name_name')
		extra = (typeID,newNamespaceID,self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`region_name_name` (`namespace_id`,`name`,`new_name`,`type_id`,`new_namespace_id`,`source_id`) VALUES (?,?,?,?,?,?)",
				(rnn+extra for rnn in regionnamenames)
		)
	#addRegionTypedNameNamespacedNames()
	
	
	def addRegionBounds(self, regionbounds):
		# regionbounds=[ (region_id,population_id,chr,posMin,posMax), ... ]
		self.prepareTableForUpdate('region_bound')
		extra = (self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`region_bound` (`region_id`,`population_id`,`chr`,`posMin`,`posMax`,`source_id`) VALUES (?,?,?,?,?,?)",
				(rb+extra for rb in regionbounds)
		)
	#addRegionBounds()
	
	
	def addRegionPopulationBounds(self, populationID, regionbounds):
		# regionbounds=[ (region_id,chr,posMin,posMax), ... ]
		self.prepareTableForUpdate('region_bound')
		extra = (populationID,self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`region_bound` (`region_id`,`chr`,`posMin`,`posMax`,`population_id`,`source_id`) VALUES (?,?,?,?,?,?)",
				(rb+extra for rb in regionbounds)
		)
	#addRegionPopulationBounds()
	
	
	def addSNPs(self, snps):
		# snps=[ (rs,chr,pos), ... ]
		self.prepareTableForUpdate('snp')
		extra = (self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT INTO `db`.`snp` (`rs`,`chr`,`pos`,`source_id`) VALUES (?,?,?,?)",
				(s+extra for s in snps)
		)
	#addChromosomeSNPs()
	
	
	def addChromosomeSNPs(self, chromosome, snps):
		# snps=[ (rs,pos), ... ]
		self.prepareTableForUpdate('snp')
		extra = (chromosome,self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT INTO `db`.`snp` (`rs`,`pos`,`chr`,`source_id`) VALUES (?,?,?,?)",
				(s+extra for s in snps)
		)
	#addChromosomeSNPs()
	
	
	def addSNPMerges(self, snpmerges):
		# snpmerges=[ (rsOld,rsNew,rsCur), ... ]
		self.prepareTableForUpdate('snp_merge')
		extra = (self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT INTO `db`.`snp_merge` (`rsOld`,`rsNew`,`rsCur`,`source_id`) VALUES (?,?,?,?)",
				(sm+extra for sm in snpmerges)
		)
	#addSNPMerges()
	
	
	def addSNPEntrezRoles(self, snproles):
		# snproles=[ (rs,entrez,role_id), ... ]
		self.prepareTableForUpdate('snp_role_entrez')
		extra = (self.getSourceID(),)
		self._db.cursor().executemany(
				"INSERT OR IGNORE INTO `db`.`snp_role_entrez` (`rs`,`region_entrez`,`role_id`,`source_id`) VALUES (?,?,?,?)",
				(sr+extra for sr in snproles)
		)
	#addSNPEntrezRoles()
	
	
	def addBuildTrans(self, build_pairs):
		"""
		Adds the build->assembly pairs into the build_assembly table
		"""
		self._db.cursor().executemany(
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
		for row in self._db.cursor().executemany(
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
		self._db.cursor().executemany(
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
	
	
	def downloadFilesFromFTP(self, remHost, remFiles):
		# remFiles={'filename.ext':'/path/on/remote/host/to/filename.ext',...}
		
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
				#TODO: verify file size and retry a few times if necessary
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
	
	
	def downloadFilesFromHTTP(self, remHost, remFiles):
		# remFiles={'filename.ext':'/path/on/remote/host/to/filename.ext',...}
		
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
