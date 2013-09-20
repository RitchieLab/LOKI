#!/usr/bin/env python

import collections
import hashlib
import itertools
import os
import pkgutil
import sys
import traceback

import loki_db
import loki_source
import loaders


class Updater(object):
	
	
	##################################################
	# class interrogation
	
	
	@classmethod
	def getVersionString(cls):
		return '3.0 (2013-09-20)'
	#getVersionString()
	
	
	##################################################
	# constructor
	
	
	def __init__(self, lokidb, is_test=False):
		assert(isinstance(lokidb, loki_db.Database))
		self._is_test = is_test
		self._loki = lokidb
		self._db = lokidb._db
		self._sourceLoaders = None
		self._sourceClasses = dict()
		self._sourceObjects = dict()
		self._updating = False
		self._tablesUpdated = None
		self._tablesDeindexed = None
	#__init__()
	
	
	##################################################
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
	
	
	##################################################
	# database update
	
	
	def flagTableUpdate(self, table):
		self._tablesUpdated.add(table)
	#flagTableUpdate()
	
	
	def prepareTableForUpdate(self, table):
		if self._updating:
			self.flagTableUpdate(table)
			if table not in self._tablesDeindexed:
				#print "deindexing %s" % table #DEBUG
				self._tablesDeindexed.add(table)
				self._loki.dropDatabaseIndecies(None, 'db', table)
				#TODO if table is a hybrid pre/post-proc, drop all source=0 rows now
	#prepareTableForUpdate()
	
	
	def prepareTableForQuery(self, table):
		if self._updating:
			if table in self._tablesDeindexed:
				#print "reindexing %s" % table DEBUG
				self._tablesDeindexed.remove(table)
				self._loki.createDatabaseIndecies(None, 'db', table)
	#prepareTableForQuery()
	
	
	def findSourceModules(self):
		if self._sourceLoaders == None:
			self._sourceLoaders = {}
			loader_path = loaders.__path__
			if self._is_test:
				loader_path = [os.path.join(l, "test") for l in loaders.__path__]
			for srcImporter,srcModuleName,_ in pkgutil.iter_modules(loader_path):
				if srcModuleName.startswith('loki_source_'):
					self._sourceLoaders[srcModuleName[12:]] = srcImporter.find_module(srcModuleName)
	#findSourceModules()
	
	
	def getSourceModules(self):
		self.findSourceModules()
		return self._sourceLoaders.keys()
	#getSourceModules()
	
	
	def loadSourceModules(self, sources=None):
		self.findSourceModules()
		srcSet = set()
		for srcName in (set(sources) if sources else self._sourceLoaders.keys()):
			if srcName not in self._sourceClasses:
				if srcName not in self._sourceLoaders:
					self.log("WARNING: unknown source '%s'\n" % srcName)
					continue
				#if module not available
				srcModule = self._sourceLoaders[srcName].load_module('loki_source_%s' % srcName)
				srcClass = getattr(srcModule, 'Source_%s' % srcName)
				if not issubclass(srcClass, loki_source.Source):
					self.log("WARNING: invalid module for source '%s'\n" % srcName)
					continue
				self._sourceClasses[srcName] = srcClass
			#if module class not loaded
			srcSet.add(srcName)
		#foreach source
		return srcSet
	#loadSourceModules()
	
	
	def getSourceModuleVersions(self, sources=None):
		srcSet = self.loadSourceModules(sources)
		return { srcName : self._sourceClasses[srcName].getVersionString() for srcName in srcSet }
	#getSourceModuleVersions()
	
	
	def getSourceModuleOptions(self, sources=None):
		srcSet = self.loadSourceModules(sources)
		return { srcName : self._sourceClasses[srcName].getOptions() for srcName in srcSet }
	#getSourceModuleOptions()
	
	
	def attachSourceModules(self, sources=None):
		sources = self.loadSourceModules(sources)
		srcSet = set()
		for srcName in sources:
			if srcName not in self._sourceObjects:
				if srcName not in self._sourceClasses:
					raise Exception("loadSourceModules() reported false positive for '%s'" % srcName)
				self._sourceObjects[srcName] = self._sourceClasses[srcName](self._loki)
			#if module not instantiated
			srcSet.add(srcName)
		#foreach source
		return srcSet
	#attachSourceModules()
	
	
	def updateDatabase(self, sources=None, sourceOptions=None, cacheOnly=False, forceUpdate=False):
		if self._updating:
			raise Exception("_updating set before updateDatabase()")
		self._loki.testDatabaseWriteable()
		if self._loki.getDatabaseSetting('finalized',int):
			raise Exception("cannot update a finalized database")
		
		# check for extraneous options
		self.logPush("preparing for update ...\n")
		srcSet = self.attachSourceModules(sources)
		srcOpts = sourceOptions or {}
		for srcName in srcOpts.keys():
			if srcName not in srcSet:
				self.log("WARNING: not updating from source '%s' for which options were supplied\n" % srcName)
		logIndent = self.logPop("... OK\n")
		
		# update all specified sources
		iwd = os.path.abspath(os.getcwd())
		self._updating = True
		self._tablesUpdated = set()
		self._tablesDeindexed = set()
		srcErrors = set()
		cursor = self._db.cursor()
		cursor.execute("SAVEPOINT 'updateDatabase'")
		try:
			for srcName in sorted(srcSet):
				cursor.execute("SAVEPOINT 'updateDatabase_%s'" % (srcName,))
				try:
					srcObj = self._sourceObjects[srcName]
					srcID = srcObj.getSourceID()
					
					# validate options, if any
					options = srcOpts.get(srcName, {})
					if options:
						self.logPush("validating %s options ...\n" % srcName)
						msg = srcObj.validateOptions(options)
						if msg != True:
							raise Exception(msg)
						for opt,val in options.iteritems():
							self.log("%s = %s\n" % (opt,val))
						self.logPop("... OK\n")
					
					# switch to a temp subdirectory for this source
					path = os.path.join(iwd, srcName)
					if not os.path.exists(path):
						os.makedirs(path)
					os.chdir(path)
					
					# download files into a local cache
					if not cacheOnly:
						self.logPush("downloading %s data ...\n" % srcName)
						srcObj.download(options)
						self.logPop("... OK\n")
					
					# calculate source file metadata
					# all timestamps are assumed to be in UTC, but if a source
					# provides file timestamps with no TZ (like via FTP) we use them
					# as-is and assume they're supposed to be UTC
					self.log("analyzing %s data files ..." % srcName)
					filehash = dict()
					for filename in os.listdir('.'):
						stat = os.stat(filename)
						md5 = hashlib.md5()
						with open(filename,'rb') as f:
							chunk = f.read(8*1024*1024)
							while chunk:
								md5.update(chunk)
								chunk = f.read(8*1024*1024)
						filehash[filename] = (filename, long(stat.st_size), long(stat.st_mtime), md5.hexdigest())
					self.log(" OK\n")
					
					# compare current loader version, options and file metadata to the last update
					skip = not forceUpdate
					last = '?'
					if skip:
						for row in cursor.execute("SELECT version, DATETIME(updated,'localtime') FROM `db`.`source` WHERE source_id = ?", (srcID,)):
							skip = skip and (row[0] == srcObj.getVersionString())
							last = row[1]
					if skip:
						n = 0
						for row in cursor.execute("SELECT option, value FROM `db`.`source_option` WHERE source_id = ?", (srcID,)):
							n += 1
							skip = skip and (row[0] in options) and (row[1] == options[row[0]])
						skip = skip and (n == len(options))
					if skip:
						n = 0
						for row in cursor.execute("SELECT filename, size, md5 FROM `db`.`source_file` WHERE source_id = ?", (srcID,)):
							n += 1
							skip = skip and (row[0] in filehash) and (row[1] == filehash[row[0]][1]) and (row[2] == filehash[row[0]][3])
						skip = skip and (n == len(filehash))
					
					# skip the update if the current loader and all source file versions match the last update
					if skip:
						self.log("skipping %s update, no data or software changes since %s\n" % (srcName,last))
					else:
						# process new files (or old files with a new loader)
						self.logPush("processing %s data ...\n" % srcName)
						
						cursor.execute("DELETE FROM `db`.`warning` WHERE source_id = ?", (srcID,))
						if srcObj.update(options) == False:
							raise Exception
						cursor.execute("UPDATE `db`.`source` SET updated = DATETIME('now'), version = ? WHERE source_id = ?", (srcObj.getVersionString(), srcID))
						
						cursor.execute("DELETE FROM `db`.`source_option` WHERE source_id = ?", (srcID,))
						sql = "INSERT INTO `db`.`source_option` (source_id, option, value) VALUES (%d,?,?)" % srcID
						cursor.executemany(sql, options.iteritems())
						
						cursor.execute("DELETE FROM `db`.`source_file` WHERE source_id = ?", (srcID,))
						sql = "INSERT INTO `db`.`source_file` (source_id, filename, size, modified, md5) VALUES (%d,?,?,DATETIME(?,'unixepoch'),?)" % srcID
						cursor.executemany(sql, filehash.values())
						
						self.logPop("... OK\n")
					#if skip
				except:
					srcErrors.add(srcName)
					excType,excVal,excTrace = sys.exc_info()
					while self.logPop() > logIndent:
						pass
					self.logPush("ERROR: failed to update %s\n" % (srcName,))
					if excTrace:
						for line in traceback.format_list(traceback.extract_tb(excTrace)[-1:]):
							self.log(line)
					for line in traceback.format_exception_only(excType,excVal):
						self.log(line)
					self.logPop()
					cursor.execute("ROLLBACK TRANSACTION TO SAVEPOINT 'updateDatabase_%s'" % (srcName,))
				finally:
					cursor.execute("RELEASE SAVEPOINT 'updateDatabase_%s'" % (srcName,))
				#try/except/finally
			#foreach source
			
			# cross-map GRCh/UCSChg build versions for all sources
			ucscGRC = collections.defaultdict(int)
			for row in self._db.cursor().execute("SELECT grch,ucschg FROM `db`.`grch_ucschg`"):
				ucscGRC[row[1]] = max(row[0], ucscGRC[row[1]])
				cursor.execute("UPDATE `db`.`source` SET grch = ? WHERE grch IS NULL AND ucschg = ?", (row[0],row[1]))
				cursor.execute("UPDATE `db`.`source` SET ucschg = ? WHERE ucschg IS NULL AND grch = ?", (row[1],row[0]))
			cursor.execute("UPDATE `db`.`source` SET current_ucschg = ucschg WHERE current_ucschg IS NULL")
			
			# check all sources' UCSChg build versions and set the latest as the target
			hgSources = collections.defaultdict(set)
			for row in cursor.execute("SELECT source_id, current_ucschg FROM `db`.`source` WHERE current_ucschg IS NOT NULL"):
				hgSources[row[1]].add(row[0])
			if hgSources:
				targetHG = max(hgSources)
				self.log("database genome build: GRCh%s / UCSChg%s\n" % (ucscGRC.get(targetHG,'?'), targetHG))
				targetUpdated = (self._loki.getDatabaseSetting('ucschg',int) != targetHG)
				self._loki.setDatabaseSetting('ucschg', targetHG)
			
			# liftOver sources with old build versions, if there are any
			if len(hgSources) > 1:
				locusSources = set(row[0] for row in cursor.execute("SELECT DISTINCT source_id FROM `db`.`snp_locus`"))
				regionSources = set(row[0] for row in cursor.execute("SELECT DISTINCT source_id FROM `db`.`biopolymer_region`"))
				chainsUpdated = ('grch_ucschg' in self._tablesUpdated or 'chain' in self._tablesUpdated or 'chain_data' in self._tablesUpdated)
				for oldHG in sorted(hgSources):
					if oldHG == targetHG:
						continue
					if not self._loki.hasLiftOverChains(oldHG, targetHG):
						self.log("ERROR: no chains available to lift hg%d to hg%d\n" % (oldHG, targetHG))
						continue
					
					if targetUpdated or chainsUpdated or 'snp_locus' in self._tablesUpdated:
						sourceIDs = hgSources[oldHG] & locusSources
						if sourceIDs:
							self.liftOverSNPLoci(oldHG, targetHG, sourceIDs)
					if targetUpdated or chainsUpdated or 'biopolymer_region' in self._tablesUpdated:
						sourceIDs = hgSources[oldHG] & regionSources
						if sourceIDs:
							self.liftOverRegions(oldHG, targetHG, sourceIDs)
					
					sql = "UPDATE `db`.`source` SET current_ucschg = %d WHERE source_id = ?" % targetHG
					cursor.executemany(sql, ((sourceID,) for sourceID in hgSources[oldHG]))
				#foreach old build
			#if any old builds
			
			# post-process as needed
			allPost = False
			lastVers,curVers = self._loki.getDatabaseSetting('updaterVersion'),self.getVersionString()
			if lastVers != curVers:
				self._loki.setDatabaseSetting('updaterVersion', curVers)
				allPost = True
				if lastVers:
					self.log("updater version changed from '%s' to '%s', re-running all post-processing\n" % (lastVers,curVers))
			#self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG
			import time
			if allPost or ('snp_merge' in self._tablesUpdated):
				t0 = time.time()
				self.cleanupSNPMerges()
				self.log("(%ds)\n" % (time.time()-t0))
			if allPost or ('snp_merge' in self._tablesUpdated) or ('snp_locus' in self._tablesUpdated):
				t0 = time.time()
				self.updateMergedSNPLoci()
				self.log("(%ds)\n" % (time.time()-t0))
			if allPost or ('snp_locus' in self._tablesUpdated):
				t0 = time.time()
				self.cleanupSNPLoci()
				self.log("(%ds)\n" % (time.time()-t0))
			if allPost or ('snp_merge' in self._tablesUpdated) or ('snp_entrez_role' in self._tablesUpdated):
				t0 = time.time()
				self.updateMergedSNPEntrezRoles()
				self.log("(%ds)\n" % (time.time()-t0))
			if allPost or ('snp_entrez_role' in self._tablesUpdated):
				t0 = time.time()
				self.cleanupSNPEntrezRoles()
				self.log("(%ds)\n" % (time.time()-t0))
			if allPost or ('snp_merge' in self._tablesUpdated) or ('gwas' in self._tablesUpdated):
				t0 = time.time()
				self.updateMergedGWASAnnotations()
				self.log("(%ds)\n" % (time.time()-t0))
			if allPost or ('region' in self._tablesUpdated):
				t0 = time.time()
				self.updateRegionZones()
				self.log("(%ds)\n" % (time.time()-t0))
			if allPost or ('region' in self._tablesUpdated) or ('region_name' in self._tablesUpdated) or ('name_name' in self._tablesUpdated):
				t0 = time.time()
				self.defineOmicUnits()
				self.log("(%ds)\n" % (time.time()-t0))
			if allPost or ('unit_name' in self._tablesUpdated) or ('snp_entrez_role' in self._tablesUpdated):
				t0 = time.time()
				self.resolveSNPUnitRoles()
				self.log("(%ds)\n" % (time.time()-t0))
			if allPost or ('unit_name' in self._tablesUpdated) or ('region_name' in self._tablesUpdated):
				t0 = time.time()
				self.resolveUnitRegions()
				self.log("(%ds)\n" % (time.time()-t0))
			if allPost or ('unit_name' in self._tablesUpdated) or ('group_member_name' in self._tablesUpdated):
				t0 = time.time()
				self.resolveGroupMembers()
				self.log("(%ds)\n" % (time.time()-t0))
			#self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG
			
			# reindex all remaining tables
			self.log("finishing update ...")
			if self._tablesDeindexed:
				self._loki.createDatabaseIndecies(None, 'db', self._tablesDeindexed)
			if self._tablesUpdated:
				self._loki.setDatabaseSetting('optimized',0)
			self.log(" OK\n")
		except:
			excType,excVal,excTrace = sys.exc_info()
			while self.logPop() > logIndent:
				pass
			self.logPush("ERROR: failed to update the database\n")
			if excTrace:
				for line in traceback.format_list(traceback.extract_tb(excTrace)[-1:]):
					self.log(line)
			for line in traceback.format_exception_only(excType,excVal):
				self.log(line)
			self.logPop()
			cursor.execute("ROLLBACK TRANSACTION TO SAVEPOINT 'updateDatabase'")
		finally:
			cursor.execute("RELEASE SAVEPOINT 'updateDatabase'")
			self._updating = False
			self._tablesUpdated = None
			self._tablesDeindexed = None
			os.chdir(iwd)
		#try/except/finally
		
		# report and return
		if srcErrors:
			self.logPush("WARNING: data from these sources was not updated:\n")
			for srcName in sorted(srcErrors):
				self.log("%s\n" % srcName)
			self.logPop()
			return False
		return True
	#updateDatabase()
	
	
	def liftOverSNPLoci(self, oldHG, newHG, sourceIDs):
		self.log("lifting over SNP loci from hg%d to hg%d ..." % (oldHG,newHG))
		self.prepareTableForUpdate('snp_locus')
		cursor = self._db.cursor()
		numLift = numNull = 0
		tally = dict()
		trash = set()
		
		# identify range of IDs
		# (two separate queries is faster because a simple MIN() or MAX() only peeks at the index;
		# SQLite isn't clever enough to do that for both at the same time, it does a table scan instead)
		firstID = min(row[0] for row in cursor.execute("SELECT MIN(_ROWID_) FROM `db`.`snp_locus`"))
		lastID = max(row[0] for row in cursor.execute("SELECT MAX(_ROWID_) FROM `db`.`snp_locus`"))
		
		# define a callback to store loci that can't be lifted over, for later deletion
		def errorCallback(region):
			trash.add( (region[0],) )
		
		# we can't SELECT and UPDATE the same table at the same time,
		# so read in batches of 2.5 million at a time based on ID
		minID = firstID
		maxID = minID + 2500000 - 1
		while minID <= lastID:
			sql = "SELECT _ROWID_, chr, pos FROM `db`.`snp_locus`"
			sql += " WHERE (_ROWID_ BETWEEN ? AND ?) AND source_id IN (%s)" % (','.join(str(i) for i in sourceIDs))
			oldLoci = list(cursor.execute(sql, (minID,maxID)))
			newLoci = self._loki.generateLiftOverLoci(oldHG, newHG, oldLoci, tally, errorCallback)
			sql = "UPDATE OR REPLACE `db`.`snp_locus` SET chr = ?2, pos = ?3 WHERE _ROWID_ = ?1"
			cursor.executemany(sql, newLoci)
			numLift += tally['lift']
			numNull += tally['null']
			if trash:
				cursor.executemany("DELETE FROM `db`.`snp_locus` WHERE _ROWID_ = ?", trash)
				trash.clear()
			minID = maxID + 1
			maxID = minID + 2500000 - 1
		#foreach batch
		
		self.log(" OK: %d loci lifted over, %d dropped\n" % (numLift,numNull))
	#liftOverSNPLoci()	
	
	
	def liftOverRegions(self, oldHG, newHG, sourceIDs):
		self.log("lifting over regions from hg%d to hg%d ..." % (oldHG,newHG))
		self.prepareTableForUpdate('region')
		cursor = self._db.cursor()
		numLift = numNull = 0
		tally = dict()
		trash = set()
		
		# identify range of IDs
		# (two separate queries is faster because a simple MIN() or MAX() only peeks at the index;
		# SQLite isn't clever enough to do that for both at the same time, it does a table scan instead)
		firstID = min(row[0] for row in cursor.execute("SELECT MIN(region_id) FROM `db`.`region`"))
		lastID = max(row[0] for row in cursor.execute("SELECT MAX(region_id) FROM `db`.`region`"))
		
		# define a callback to store regions that can't be lifted over, for later deletion
		def errorCallback(region):
			trash.add( (region[0],) )
		
		# we can't SELECT and UPDATE the same table at the same time,
		# so read in batches of 2.5 million at a time based on ID
		# (this will probably be all of them in one go, but just in case)
		minID = firstID
		maxID = minID + 2500000 - 1
		while minID <= lastID:
			sql = "SELECT region_id, chr, posMin, posMax FROM `db`.`region`"
			sql += " WHERE (region_id BETWEEN ? AND ?) AND source_id IN (%s)" % (','.join(str(i) for i in sourceIDs))
			oldRegions = list(cursor.execute(sql, (minID,maxID)))
			newRegions = self._loki.generateLiftOverRegions(oldHG, newHG, oldRegions, tally, errorCallback)
			sql = "UPDATE OR REPLACE `db`.`region` SET chr = ?2, posMin = ?3, posMax = ?4 WHERE region_id = ?1"
			cursor.executemany(sql, newRegions)
			numLift += tally['lift']
			numNull += tally['null']
			if trash:
				cursor.executemany("DELETE FROM `db`.`region` WHERE region_id = ?", trash)
				trash.clear()
			minID = maxID + 1
			maxID = minID + 2500000 - 1
		#foreach batch
		
		self.log(" OK: %d regions lifted over, %d dropped\n" % (numLift,numNull))
	#liftOverRegions()
	
	
	def cleanupSNPMerges(self):
		self.log("verifying SNP merge records ...")
		self.prepareTableForQuery('snp_merge')
		dbc = self._db.cursor()
		
		# for each set of ROWIDs which constitute a duplicated snp merge, cull all but one
		cull = set()
		sql = "SELECT GROUP_CONCAT(_ROWID_) FROM `db`.`snp_merge` GROUP BY rsMerged HAVING COUNT() > 1"
		#for row in dbc.execute("EXPLAIN QUERY PLAN "+sql): #DEBUG
		#	print row
		for row in dbc.execute(sql):
			cull.update( (long(i),) for i in row[0].split(',')[1:] )
		if cull:
			self.flagTableUpdate('snp_merge')
			dbc.executemany("DELETE FROM `db`.`snp_merge` WHERE _ROWID_ = ?", cull)
		self.log(" OK: %d duplicate merges\n" % (len(cull),))
	#cleanupSNPMerges()
	
	
	def updateMergedSNPLoci(self):
		self.log("checking for merged SNP loci ...")
		self.prepareTableForQuery('snp_locus')
		self.prepareTableForQuery('snp_merge')
		dbc = self._db.cursor()
		sql = """
INSERT INTO `db`.`snp_locus` (rs, chr, pos, validated, source_id)
SELECT sm.rsCurrent, sl.chr, sl.pos, sl.validated, sl.source_id
FROM `db`.`snp_locus` AS sl
JOIN `db`.`snp_merge` AS sm
  ON sm.rsMerged = sl.rs
"""
		#for row in dbc.execute("EXPLAIN QUERY PLAN "+sql): #DEBUG
		#	print row
		dbc.execute(sql)
		numCopied = self._db.changes()
		if numCopied:
			self.flagTableUpdate('snp_locus')
		self.log(" OK: %d loci copied\n" % (numCopied,))
	#updateMergedSNPLoci()
	
	
	def cleanupSNPLoci(self):
		self.log("verifying SNP loci ...")
		self.prepareTableForQuery('snp_locus')
		dbc = self._db.cursor()
		# for each set of ROWIDs which constitute a duplicated snp-locus, cull all but one;
		# but, make sure that if any of the originals were validated, the remaining one is also
		valid = set()
		cull = set()
		sql = "SELECT GROUP_CONCAT(_ROWID_), MAX(validated) FROM `db`.`snp_locus` GROUP BY rs, chr, pos HAVING COUNT() > 1"
		#for row in dbc.execute("EXPLAIN QUERY PLAN "+sql): #DEBUG
		#	print row
		for row in dbc.execute(sql):
			rowids = row[0].split(',')
			if row[1]:
				valid.add( (long(rowids[0]),) )
			cull.update( (long(i),) for i in rowids[1:] )
		if valid:
			dbc.executemany("UPDATE `db`.`snp_locus` SET validated = 1 WHERE _ROWID_ = ?", valid)
		if cull:
			self.flagTableUpdate('snp_locus')
			dbc.executemany("DELETE FROM `db`.`snp_locus` WHERE _ROWID_ = ?", cull)
		self.log(" OK: %d duplicate loci\n" % (len(cull),))
	#cleanupSNPLoci()
	
	
	def updateMergedSNPEntrezRoles(self):
		self.log("checking for merged SNP roles ...")
		self.prepareTableForQuery('snp_entrez_role')
		self.prepareTableForQuery('snp_merge')
		dbc = self._db.cursor()
		sql = """
INSERT OR IGNORE INTO `db`.`snp_entrez_role` (rs, entrez_id, role_id, source_id)
SELECT sm.rsCurrent, ser.entrez_id, ser.role_id, ser.source_id
FROM `db`.`snp_entrez_role` AS ser
JOIN `db`.`snp_merge` AS sm
  ON sm.rsMerged = ser.rs
"""
		#for row in dbc.execute("EXPLAIN QUERY PLAN "+sql): #DEBUG
		#	print row
		dbc.execute(sql)
		numCopied = self._db.changes()
		if numCopied:
			self.flagTableUpdate('snp_entrez_role')
		self.log(" OK: %d roles copied\n" % (numCopied,))
	#updateMergedSNPEntrezRoles()
	
	
	def cleanupSNPEntrezRoles(self):
		self.log("verifying SNP roles ...")
		self.prepareTableForQuery('snp_entrez_role')
		dbc = self._db.cursor()
		cull = set()
		sql = "SELECT GROUP_CONCAT(_ROWID_) FROM `db`.`snp_entrez_role` GROUP BY rs, entrez_id, role_id HAVING COUNT() > 1"
		#for row in dbc.execute("EXPLAIN QUERY PLAN "+sql): #DEBUG
		#	print row
		for row in dbc.execute(sql):
			cull.update( (long(i),) for i in row[0].split(',')[1:] )
		if cull:
			self.flagTableUpdate('snp_entrez_role')
			dbc.executemany("DELETE FROM `db`.`snp_entrez_role` WHERE _ROWID_ = ?", cull)
		self.log(" OK: %d duplicate roles\n" % (len(cull),))
	#cleanupSNPEntrezRoles()
	
	
	def updateMergedGWASAnnotations(self):
		self.log("checking for merged GWAS annotated SNPs ...")
		self.prepareTableForQuery('gwas')
		self.prepareTableForQuery('snp_merge')
		dbc = self._db.cursor()
		sql = """
INSERT INTO `db`.`gwas` (rs, chr, pos, trait, snps, orbeta, allele95ci, riskAfreq, pubmed_id, source_id)
SELECT sm.rsCurrent, w.chr, w.pos, w.trait, w.snps, w.orbeta, w.allele95ci, w.riskAfreq, w.pubmed_id, w.source_id
FROM `db`.`gwas` AS w
JOIN `db`.`snp_merge` AS sm
  ON sm.rsMerged = w.rs
"""
		#for row in dbc.execute("EXPLAIN QUERY PLAN "+sql): #DEBUG
		#	print row
		dbc.execute(sql)
		numCopied = self._db.changes()
		if numCopied:
			self.flagTableUpdate('gwas')
		self.log(" OK: %d annotations copied\n" % (numCopied,))
	#updateMergedGWASAnnotations()
	
	
	def updateRegionZones(self):
		self.log("calculating zone coverage ...")
		size = self._loki.getDatabaseSetting('zone_size',int)
		if not size:
			raise Exception("ERROR: could not determine database setting 'zone_size'")
		dbc = self._db.cursor()
		
		# make sure all regions are correctly oriented
		dbc.execute("UPDATE `db`.`region` SET posMin = posMax, posMax = posMin WHERE posMin > posMax")
		
		# define zone generator
		def _zones(size, regions):
			# regions=[ (region_id,rtype_id,chr,posMin,posMax),... ]
			# yields:[ (region_id,rtype_id,chr,zone),... ]
			for r in regions:
				for z in xrange(int(r[3]/size),int(r[4]/size)+1):
					yield (r[0],r[1],r[2],z)
		#_zones()
		
		# feed all regions through the zone generator
		self.prepareTableForUpdate('region_zone')
		self.prepareTableForQuery('region')
		dbc.execute("DELETE FROM `db`.`region_zone`")
		dbc.executemany(
			"INSERT OR IGNORE INTO `db`.`region_zone` (region_id,rtype_id,chr,zone) VALUES (?,?,?,?)",
			_zones(
				size,
				self._db.cursor().execute("SELECT region_id,rtype_id,chr,posMin,posMax FROM `db`.`region`")
			)
		)
		
		# clean up
		self.prepareTableForQuery('region_zone')
		for row in dbc.execute("SELECT COUNT(), COUNT(DISTINCT region_id) FROM `db`.`region_zone`"):
			numTotal = row[0]
			numRegions = row[1]
		self.log(" OK: %d records (%d regions)\n" % (numTotal,numRegions))
	#updateRegionZones()
	
	
	def defineOmicUnits(self):
		self.logPush("defining omic units ...")
		self.prepareTableForQuery('region')
		self.prepareTableForQuery('region_name')
		self.prepareTableForQuery('name_property')
		self.prepareTableForUpdate('unit')
		self.prepareTableForUpdate('unit_name')
		cursor = self._db.cursor()
		
		# delete old derived records
		self.log("deleting old records ...")
		cursor.execute("DELETE FROM `db`.`unit` WHERE source_id = 0")
		cursor.execute("DELETE FROM `db`.`unit_name` WHERE source_id = 0")
		self.log(" OK\n")
		
		# load namespace definitions
		nsName = dict()
		nsID = dict()
		for row in cursor.execute("SELECT namespace_id,namespace FROM `db`.`namespace`"):
			nsName[row[0]] = row[1]
			nsID[row[1]] = row[0]
		nsCore = set()
		if 'entrez_gid' in nsID:
			nsCore.add(nsID['entrez_gid'])
		if 'ensembl_gid' in nsID:
			nsCore.add(nsID['ensembl_gid'])
		if not nsCore:
			self.logPop("... ERROR: unit definition requires entrez or ensembl identifiers")
			return False
		
		# load the name graph
		self.log("building identifier graph ...")
		nameNamespaceID = list()
		nameName = list()
		nameNum = dict()
		graph = collections.defaultdict(set)
		numEdges = 0
		for row in cursor.execute("SELECT namespace_id1,name1,namespace_id2,name2 FROM `db`.`name_name`"):
			name1 = (row[0],row[1])
			n1 = nameNum.get(name1)
			if n1 == None:
				n1 = nameNum[name1] = len(nameNamespaceID)
				nameNamespaceID.append(row[0])
				nameName.append(row[1])
			name2 = (row[2],row[3])
			n2 = nameNum.get(name2)
			if n2 == None:
				n2 = nameNum[name2] = len(nameNamespaceID)
				nameNamespaceID.append(row[2])
				nameName.append(row[3])
			if n1 != n2:
				numEdges += 1
				graph[n1].add(n2)
				graph[n2].add(n1)
		#for row in cursor
		self.log(" OK: %d identifiers, %d links\n" % (len(graph),numEdges))
		
		# load name properties
		self.log("loading identifier properties ...")
		namePropValues = collections.defaultdict(lambda: collections.defaultdict(set))
		for row in cursor.execute("SELECT namespace_id,name,property,value FROM `db`.`name_property`"):
			name = (row[0],row[1])
			n = nameNum.get(name)
			if n == None:
				n = nameNum[name] = len(nameNamespaceID)
				nameNamespaceID.append(row[0])
				nameName.append(row[1])
				graph[n] = set()
			namePropValues[n][row[2]].add(row[3])
		self.log(" OK: %d identifiers\n" % (len(namePropValues),))
		
		# load regions associated with any of the core namespaces
		self.log("loading core regions ...")
		nameRegions = collections.defaultdict(set)
		for row in cursor.execute("""
SELECT rn.namespace_id, rn.name, r.chr, r.posMin, r.posMax
FROM `db`.`region_name` AS rn
JOIN `db`.`region` AS r USING (region_id)
WHERE rn.namespace_id IN (%s)"""
				% ",".join(str(i) for i in nsCore)
		):
			name = (row[0],row[1])
			n = nameNum.get(name)
			if n == None:
				n = nameNum[name] = len(nameNamespaceID)
				nameNamespaceID.append(row[0])
				nameName.append(row[1])
				graph[n] = set()
			nameRegions[n].add( (row[2],row[3],row[4]) )
		#for row in cursor
		self.log(" OK: %d identifiers\n" % (len(nameRegions),))
		
		# find core sets of names that could become a unit
		self.log("searching for candidate units ...")
		coreNames = list()
		nameFlag = set()
		stack = list()
		for n1 in graph:
			if (nameNamespaceID[n1] in nsCore) and (n1 not in nameFlag):
				names = {n1}
				nameFlag.add(n1)
				stack.append(n1)
				while stack:
					for n2 in graph[stack.pop()]:
						if (nameNamespaceID[n2] in nsCore) and (n2 not in names):
							names.add(n2)
							nameFlag.add(n2)
							stack.append(n2)
				coreNames.append(names)
		#for n1 in graph
		self.log(" OK: %d candidates, %d core identifiers\n" % (len(coreNames),len(nameFlag)))
		nameFlag = None
		
		# split cores according to region gap rules
		self.log("analyzing candidate unit regions ...")
		unitNames = list()
		numNone = numChr = numGap = 0
		while coreNames:
			names = coreNames.pop()
			regions = list()
			for n in names:
				regions.extend( (r+(n,)) for r in nameRegions[n] )
			if not regions:
				numNone += 1
				unitNames.append(names) #TODO: configurable keep-noregion
				continue
			regions.sort()
			names = set()
			uC = uR = None
			for rC,rL,rR,rN in itertools.chain(regions, [(0,0,0,None)]):
				if (uC == None or uC == 23 or uC == 24) and (rC == 23 or rC == 24): #it's ok for a core to have regions on X and Y
					uC,uR = rC,rR
				elif (uC != rC) or (uR + 25000 < rL): #TODO: configurable gap limit
					if names:
						if rN == None:
							pass
						elif uC != rC:
							numChr += 1
						else:
							numGap += 1
						unitNames.append(names)
						names = set()
					uC,uR = rC,rR
				else:
					uR = max(uR,rR)
				names.add(rN)
			#for r in regions
		#while coreNames
		nameUnits = collections.defaultdict(set)
		for u,names in enumerate(unitNames):
			for n in names:
				nameUnits[n].add(u)
		self.log(" OK: %d omic-units (%d no-region, %d chr-splits, %d gap-splits)\n" % (len(unitNames),numNone,numChr,numGap))
		nameRegions = coreNames = None
		
		# assign additional names using a kind of multi-source breadth-first-search #TODO: ambiguity?
		self.log("assigning aliases to units ...")
		nameDist = {n:0 for n in nameUnits}
		queue = collections.deque(nameUnits)
		while queue:
			n1 = queue.pop()
			units = nameUnits[n1]
			dist = nameDist[n1] + 1
			for n2 in graph[n1]:
				if n2 not in nameDist:
					for u in units:
						unitNames[u].add(n2)
					nameUnits[n2] |= units
					nameDist[n2] = dist
					queue.appendleft(n2)
				elif nameDist[n2] == dist:
					for u in units:
						unitNames[u].add(n2)
					nameUnits[n2] |= units
				elif nameDist[n2] > dist:
					raise Exception("BFS failure")
		graph = None
		self.log(" OK: %d identifiers\n" % (len(nameUnits),))
		
		# assign properties for each nameset
		self.log("adding details to units ...")
		unitProps = list()
		zeroset = {(0,0)}
		noneset = {(None,None)}
		for names in unitNames:
			utype_id = list()
			label = list()
			desc = list()
			symbol = list()
			for n in names:
				d = nameDist[n]
				if n in namePropValues:
					utype_id.extend( (d,v) for v in namePropValues[n]['utype_id'] )
					label.extend( (d,v) for v in namePropValues[n]['label'] )
					desc.extend( (d,v) for v in namePropValues[n]['description'] )
				if nameNamespaceID[n] == nsID['symbol']:
					symbol.append( (d,nameName[n]) )
			utype_id = min(utype_id or zeroset)[1]
			label = min(label or symbol or noneset)[1] or min(names)
			desc = min(desc or noneset)[1]
			unitProps.append( (utype_id,label,desc) )
		nameDist = namePropValues = zeroset = noneset = None
		self.log(" OK\n")
		
		# store units
		self.log("storing units ...")
		unitIDs = list()
		for row in cursor.executemany("INSERT INTO `db`.`unit` (utype_id,label,description,source_id) VALUES (?,?,?,0); SELECT last_insert_rowid()", unitProps):
			unitIDs.append(row[0])
		unitProps = None
		self.log(" OK: %d units\n" % (len(unitIDs),))
		
		# store unit names
		self.log("storing unit aliases ...")
		for u,names in enumerate(unitNames):
			cursor.executemany("INSERT OR IGNORE INTO `db`.`unit_name` (unit_id,namespace_id,name,source_id) VALUES (?,?,?,0)", ((unitIDs[u],nameNamespaceID[n],nameName[n]) for n in names))
		#for name,units in nameUnits.iteritems():
		#	cursor.executemany("INSERT OR IGNORE INTO `db`.`unit_name` (unit_id,namespace_id,name,source_id) VALUES (?,?,?,0)", ((unitIDs[u],name[0],name[1]) for u in units))
		nameUnits = unitNames = unitIDs = None
		self.log(" OK\n")
		
		self.logPop("... OK\n")
	#defineOmicUnits()
	
	
	def loadNameUIDs(self):
		cursor = self._db.cursor()
		
		#self.log("loading unit identifiers ...")
		nameUIDs = collections.defaultdict(set)
		for row in cursor.execute("SELECT namespace_id,name,unit_id FROM `db`.`unit_name`"):
			nameUIDs[(row[0],row[1])].add(row[2])
		#self.log(" OK: %d identifiers\n" % len(nameUIDs))
		
		return nameUIDs
	#loadNameUIDs()
	
	
	def resolveSNPUnitRoles(self, nameUIDs=None):
		self.log("resolving SNP roles ...")
		nameUIDs = nameUIDs or self.loadNameUIDs()
		cursor = self._db.cursor()
		
		# translate entrez_ids to unit_ids
		self.prepareTableForUpdate('snp_unit_role')
		cursor.execute("DELETE FROM `db`.`snp_unit_role`")
		namespaceID = self._loki.getNamespaceID('entrez_gid')
		if namespaceID:
			def generate_rows():
				for row in self._db.cursor().execute("SELECT rs, entrez_id, role_id, source_id FROM `db`.`snp_entrez_role`"):
					for u in nameUIDs[(namespaceID,row[1])]:
						yield (row[0],u,row[2],row[3])
			cursor.executemany("INSERT INTO `db`.`snp_unit_role` (rs, unit_id, role_id, source_id) VALUES (?,?,?,?)", generate_rows())
		
		# cull duplicate roles
		self.prepareTableForQuery('snp_unit_role')
		cull = set()
		sql = "SELECT GROUP_CONCAT(_ROWID_) FROM `db`.`snp_unit_role` GROUP BY rs, unit_id, role_id HAVING COUNT() > 1"
		#for row in cursor.execute("EXPLAIN QUERY PLAN "+sql): #DEBUG
		#	print row
		for row in cursor.execute(sql):
			cull.update( (long(i),) for i in row[0].split(',')[1:] )
		if cull:
			self.flagTableUpdate('snp_unit_role')
			cursor.executemany("DELETE FROM `db`.`snp_unit_role` WHERE _ROWID_ = ?", cull)
		
		numTotal = numSNPs = numGenes = 0
		for row in cursor.execute("SELECT COUNT(), COUNT(DISTINCT rs), COUNT(DISTINCT unit_id) FROM `db`.`snp_unit_role`"):
			numTotal = row[0]
			numSNPs = row[1]
			numUnits = row[2]
		self.log(" OK: %d roles (%d SNPs, %d units)\n" % (numTotal,numSNPs,numUnits))
		return nameUIDs
	#resolveSNPUnitRoles()
	
	
	def resolveUnitRegions(self, nameUIDs=None):
		self.log("assigning unit regions ...")
		nameUIDs = nameUIDs or self.loadNameUIDs()
		self.prepareTableForQuery('region_name')
		self.prepareTableForQuery('unit_name')
		self.prepareTableForUpdate('unit_region')
		cursor = self._db.cursor()
		cursor.execute("DELETE FROM `db`.`unit_region` WHERE source_id = 0")
		
		# map regions to units #TODO: ambiguity?
		unitRegions = list()
		regionID = None
		numSingle = numAmbig = numUnrec = 0
		emptyset = set()
		for row in itertools.chain(cursor.execute("SELECT region_id,namespace_id,name FROM `db`.`region_name` ORDER BY region_id"), [(None,None,None)]):
			if regionID != row[0]:
				if regionID:
					unitIDs = set()
					unitIDs.update( *(nameUIDs.get(name,emptyset) for name in names) )
					if len(unitIDs) < 1:
						numUnrec += 1
					elif len(unitIDs) > 1:
						numAmbig += 1
					else:
						numSingle += 1
					unitRegions.extend( (u,regionID) for u in unitIDs )
				regionID = row[0]
				names = set()
			names.add( (row[1],row[2]) )
		cursor.executemany("INSERT OR IGNORE INTO `db`.`unit_region` (unit_id,region_id,urtype_id,source_id) VALUES (?,?,0,0)", unitRegions)
		self.log(" OK: %d regions assigned (%d definite, %d ambiguous, %d unrecognized)\n" % (len(unitRegions),numSingle,numAmbig,numUnrec))
		
		return nameUIDs
	#resolveUnitRegions()
	
	
	def resolveGroupMembers(self, nameUIDs=None):
		self.log("resolving group members ...")
		dbc = self._db.cursor()
		
		# calculate confidence scores for each possible name match
		dbc.execute("""
CREATE TEMP TABLE `temp`.`_group_member_name_score` (
  group_id INTERGER NOT NULL,
  member INTEGER NOT NULL,
  unit_id INTEGER NOT NULL,
  polynames INTEGER NOT NULL,
  implication INTEGER NOT NULL,
  quality INTEGER NOT NULL
)
""")
		self.prepareTableForQuery('group_member_name')
		self.prepareTableForQuery('unit_name')
		self.prepareTableForQuery('namespace')
		dbc.execute("""
INSERT INTO `temp`.`_group_member_name_score` (group_id, member, unit_id, polynames, implication, quality)
/* calculate implication and quality scores for each possible match for each member */
SELECT
  group_id,
  member,
  unit_id,
  polynames,
  COUNT(DISTINCT gmn_rowid) AS implication,
  (CASE WHEN polynames > 0 THEN 1000 * COUNT(DISTINCT gmn_rowid) ELSE SUM(1000 / match_count) END) AS quality
FROM (
  /* count the number of possible matches for each name of each member */
  SELECT
    gmn._ROWID_ AS gmn_rowid,
    gmn.group_id,
    gmn.member,
    gmn.namespace_id,
    gmn.name,
    polynames,
    COUNT(DISTINCT un.unit_id) AS match_count
  FROM (
    /* count the number of matchable polyregion names for each member */
    SELECT
      gmn.group_id,
      gmn.member,
      COUNT(DISTINCT (CASE WHEN n.polygenic > 0 THEN gmn._ROWID_ ELSE NULL END)) AS polynames
    FROM `db`.`group_member_name` AS gmn
    JOIN `db`.`unit_name` AS un USING (name)
    LEFT JOIN `db`.`namespace` AS n
      ON n.namespace_id = gmn.namespace_id
    WHERE gmn.namespace_id IN (0, un.namespace_id)
    GROUP BY gmn.group_id, gmn.member
  )
  JOIN `db`.`group_member_name` AS gmn USING (group_id, member)
  JOIN `db`.`unit_name` AS un USING (name)
  LEFT JOIN `db`.`namespace` AS n
    ON n.namespace_id = gmn.namespace_id
  WHERE gmn.namespace_id IN (0, un.namespace_id)
    AND (n.polygenic > 0 OR polynames = 0)
  GROUP BY gmn.group_id, gmn.member, gmn.namespace_id, gmn.name
) AS gmn
JOIN `db`.`unit_name` AS un USING (name)
WHERE gmn.namespace_id IN (0, un.namespace_id)
GROUP BY group_id, member, unit_id
""")
		dbc.execute("CREATE INDEX `temp`.`_group_member_name_score__group_member_unit` ON `_group_member_name_score` (group_id, member, unit_id)")
		
		# generate group_unit assignments with confidence scores
		self.prepareTableForUpdate('group_unit')
		dbc.execute("DELETE FROM `db`.`group_unit` WHERE source_id = 0")
		dbc.execute("""
/* group-unit assignments with confidence scores */
INSERT INTO `db`.`group_unit` (group_id, unit_id, specificity, implication, quality, source_id)
SELECT
  group_id,
  unit_id,
  MAX(specificity) AS specificity,
  MAX(implication) AS implication,
  MAX(quality) AS quality,
  0 AS source_id
FROM (
  /* identify specific matches with the best score for each member */
  SELECT
    group_id,
    member,
    unit_id,
    (CASE
      WHEN polynames THEN 100 / member_variance
      ELSE 100 / match_basic
    END) AS specificity,
    (CASE
      WHEN polynames THEN 100 * implication / member_implication
      WHEN implication = member_implication THEN 100 / match_implication
      ELSE 0
    END) AS implication,
    (CASE
      WHEN polynames THEN 100 * quality / member_quality
      WHEN quality = member_quality THEN 100 / match_quality
      ELSE 0
    END) AS quality
  FROM (
    /* identify number of matches with the best score for each member */
    SELECT
      group_id,
      member,
      polynames,
      COUNT(DISTINCT implication) AS member_variance,
      member_implication,
      member_quality,
      COUNT() match_basic,
      SUM(CASE WHEN implication >= member_implication THEN 1 ELSE 0 END) AS match_implication,
      SUM(CASE WHEN quality >= member_quality THEN 1 ELSE 0 END) AS match_quality
    FROM (
      /* identify best scores for each member */
      SELECT
        group_id,
        member,
        polynames,
        MAX(implication) AS member_implication,
        MAX(quality) AS member_quality
      FROM `temp`.`_group_member_name_score`
      GROUP BY group_id, member, polynames
    )
    JOIN `temp`.`_group_member_name_score` USING (group_id, member, polynames)
    GROUP BY group_id, member, polynames
  )
  JOIN `temp`.`_group_member_name_score` USING (group_id, member, polynames)
  GROUP BY group_id, member, unit_id
)
GROUP BY group_id, unit_id
""")
		
		# generate group_unit placeholders for unrecognized members
		self.prepareTableForUpdate('group_unit')
		self.prepareTableForQuery('group_member_name')
		self.prepareTableForQuery('unit_name')
		self.prepareTableForQuery('unit')
		dbc.execute("""
INSERT INTO `db`.`group_unit` (group_id, unit_id, specificity, implication, quality, source_id)
SELECT
  group_id,
  0 AS unit_id,
  COUNT() AS specificity,
  0 AS implication,
  0 AS quality,
  0 AS source_id
FROM (
  SELECT gmn.group_id
  FROM `db`.`group_member_name` AS gmn
  LEFT JOIN `db`.`unit_name` AS un
    ON un.name = gmn.name
    AND gmn.namespace_id IN (0, un.namespace_id)
  GROUP BY gmn.group_id, gmn.member
  HAVING MAX(un.unit_id) IS NULL
)
GROUP BY group_id
""")
		
		# clean up
		dbc.execute("DROP TABLE `temp`.`_group_member_name_score`")
		numTotal = numSourced = numMatch = numAmbig = numUnrec = 0
		self.prepareTableForQuery('group_unit')
		for row in dbc.execute("""
SELECT
  COALESCE(SUM(CASE WHEN unit_id > 0 THEN 1 ELSE 0 END),0) AS total,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id > 0 THEN 1 ELSE 0 END),0) AS sourced,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id = 0 AND specificity >= 100 AND implication >= 100 AND quality >= 100 THEN 1 ELSE 0 END),0) AS definite,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id = 0 AND (specificity < 100 OR implication < 100 OR quality < 100) THEN 1 ELSE 0 END),0) AS conditional,
  COALESCE(SUM(CASE WHEN unit_id = 0 AND source_id = 0 THEN specificity ELSE 0 END),0) AS unmatched
FROM `db`.`group_unit`
"""):
			numTotal = row[0]
			numSourced = row[1]
			numMatch = row[2]
			numAmbig = row[3]
			numUnrec = row[4]
		self.log(" OK: %d associations (%d explicit, %d definite, %d conditional, %d unrecognized)\n" % (numTotal,numSourced,numMatch,numAmbig,numUnrec))
	#resolveGroupMembers()
	
	
#Updater
