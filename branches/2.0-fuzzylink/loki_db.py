#!/usr/bin/env python

import apsw
import os
import pkgutil
import sys


class Database(object):
	
	
	# ##################################################
	# public class data
	
	
	ver_maj,ver_min,ver_rev,ver_date = 0,0,5,'2012-04-18'
	chr_list = ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','X','Y','XY','MT')
	chr_num = {}
	chr_name = {}
	for n in range(0,len(chr_list)):
		cname = chr_list[n]
		cnum = n + 1
		chr_num[cnum] = cnum
		chr_num['%s' % cnum] = cnum
		chr_num[cname] = cnum
		chr_name[cnum] = cname
		chr_name['%s' % cnum] = cname
		chr_name[cname] = cname
	
	MATCH_ALL   = 1
	MATCH_FIRST = 2
	MATCH_BEST  = 3
	
	
	# ##################################################
	# private class data
	
	
	_source_loaders = None
	_source_classes = dict()
	_schema = {
		'db': {
			# ########## db.namespace ##########
			'namespace': {
				'table': """
(
  namespace_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  namespace VARCHAR(32) UNIQUE NOT NULL,
  multigene TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {}
			}, #.db.namespace
			
			# ########## db.population ##########
			'population': {
				'table': """
(
  population_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  population VARCHAR(32) UNIQUE NOT NULL,
  ldcomment VARCHAR(128),
  description VARCHAR(128)
)
""",
				'index': {}
			}, #.db.population
			
			# ########## db.relationship ##########
			'relationship': {
				'table': """
(
  relationship_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  relationship VARCHAR(32) UNIQUE NOT NULL
)
""",
				'index': {}
			}, #.db.relationship
			
			# ########## db.role ##########
			'role': {
				'table': """
(
  role_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  role VARCHAR(32) UNIQUE NOT NULL,
  description VARCHAR(128),
  coding TINYINT,
  exon TINYINT
)
""",
				'index': {}
			}, #.db.role
			
			# ########## db.source ##########
			'source': {
				'table': """
(
  source_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  source VARCHAR(32) UNIQUE NOT NULL,
  updated DATETIME
)
""",
				'index': {}
			}, #.db.source
			
			# ########## db.type ##########
			'type': {
				'table': """
(
  type_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  type VARCHAR(32) UNIQUE NOT NULL
)
""",
				'index': {}
			}, #.db.type
			
			
			# ##############################
			
			
			# ########## db.group ##########
			'group': {
				'table': """
(
  group_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  type_id TINYINT NOT NULL,
  label VARCHAR(64) NOT NULL,
  description VARCHAR(256),
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'group__type_label': '(type_id,label)',
				}
			}, #.db.group
			
			# ########## db.group_name ##########
			'group_name': {
				'table': """
(
  group_id INTEGER NOT NULL,
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,namespace_id,name,source_id)
)
""",
				'index': {
					'group_name__name': '(name,namespace_id)',
				}
			}, #.db.group_name
			
			# ########## db.group_group ##########
			'group_group': {
				'table': """
(
  group_id INTEGER NOT NULL,
  related_group_id INTEGER NOT NULL,
  relationship_id SMALLINT NOT NULL,
  direction TINYINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,related_group_id,relationship_id,direction,source_id)
)
""",
				'index': {
					'group_group__related': '(related_group_id,group_id)',
				}
			}, #.db.group_group
			
			# ########## db.group_region_name ##########
			'group_region_name': {
				'table': """
(
  group_id INTEGER NOT NULL,
  member INTEGER NOT NULL,
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,member,namespace_id,name,source_id)
)
""",
				'index': {}
			}, #.db.group_region_name
			
			# ########## db.group_region ##########
			'group_region': {
				'table': """
(
  group_id INTEGER NOT NULL,
  region_id INTEGER NOT NULL,
  specificity TINYINT NOT NULL,
  implication TINYINT NOT NULL,
  quality TINYINT NOT NULL,
  PRIMARY KEY (group_id,region_id)
)
""",
				'index': {
					'group_region__region': '(region_id,group_id)',
				}
			}, #.db.group_region
			
			# ########## db.region ##########
			'region': {
				'table': """
(
  region_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  type_id TINYINT NOT NULL,
  label VARCHAR(64) NOT NULL,
  description VARCHAR(256),
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'region__type_label': '(type_id,label)',
				}
			}, #.db.region
			
			# ########## db.region_name ##########
			'region_name': {
				'table': """
(
  region_id INTEGER NOT NULL,
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (region_id,namespace_id,name,source_id)
)
""",
				'index': {
					'region_name__name': '(name,namespace_id)',
				}
			}, #.db.region_name
			
			# ########## db.region_bound ##########
			'region_bound': {
				'table': """
(
  region_id INTEGER NOT NULL,
  population_id INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  posMin BIGINT NOT NULL,
  posMax BIGINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (region_id,population_id,chr,posMin,posMax,source_id)
)
""",
				'index': {
					'region_bound__posmin': '(population_id,chr,posMin)',
					'region_bound__posmax': '(population_id,chr,posMax)',
				}
			}, #.db.region_bound
			
			# ########## db.region_zone ##########
			'region_zone': {
				'table': """
(
  region_id INTEGER NOT NULL,
  population_id INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER NOT NULL,
  PRIMARY KEY (region_id,population_id,chr,zone)
)
""",
				'index': {
					'region_zone__zone': '(population_id,chr,zone)',
				}
			}, #.db.region_zone
			
			# ########## db.snp ##########
			'snp': {
				'table': """
(
  rs INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  pos BIGINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (rs,chr,pos,source_id)
)
""",
				'index': {
					'snp__chrpos': '(chr,pos)',
				}
			}, #.db.snp
			
			# ########## db.snp_merge ##########
			'snp_merge': {
				'table': """
(
  rsOld INTEGER NOT NULL,
  rsNew INTEGER,
  rsCur INTEGER,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (rsOld,rsNew,rsCur,source_id)
)
""",
				'index': {}
			}, #.db.snp_merge
			
			# ########## db.snp_role_entrez ##########
			'snp_role_entrez': {
				'table': """
(
  rs INTEGER NOT NULL,
  region_entrez INTEGER NOT NULL,
  role_id INTEGER NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (rs,region_entrez,role_id,source_id)
)
""",
				'index': {}
			}, #.db.snp_role_entrez
			
			# ########## db.snp_role ##########
			'snp_role': {
				'table': """
(
  rs INTEGER NOT NULL,
  region_id INTEGER NOT NULL,
  role_id INTEGER NOT NULL,
  PRIMARY KEY (rs,region_id,role_id)
)
""",
				'index': {
					'snp_role__region': '(region_id)',
				}
			}, #.db.snp_role
			
		}, #.db
	} #_schema{}
	
	
	# ##################################################
	# interrogation methods
	
	
	@classmethod
	def getDatabaseDriverName(cls):
		return "SQLite"
	#getDatabaseDriverName()
	
	
	@classmethod
	def getDatabaseDriverVersion(cls):
		return apsw.sqlitelibversion()
	#getDatabaseDriverVersion()
	
	
	@classmethod
	def getDatabaseInterfaceName(cls):
		return "APSW"
	#getDatabaseInterfaceName()
	
	
	@classmethod
	def getDatabaseInterfaceVersion(cls):
		return apsw.apswversion()
	#getDatabaseInterfaceVersion()
	
	
	# ##################################################
	# constructor
	
	
	def __init__(self, dbFile=None):
		# initialize instance properties
		self._verbose = False
		self._logFile = sys.stderr
		self._logIndent = 0
		self._logHanging = False
		self._source_objects = dict()
		self._db = apsw.Connection(':memory:')
		self._db.cursor().execute("PRAGMA synchronous=OFF") #TODO: document why this is a good idea
		self._dbFile = None
		self._dbNew = None
		self.attachDatabaseFile(dbFile)
	#__init__()
	
	
	# ##################################################
	# context manager
	
	
	def __enter__(self):
		return self._db.__enter__()
	#__enter__()
	
	
	def __exit__(self, excType, excVal, traceback):
		return self._db.__exit__(excType, excVal, traceback)
	#__exit__()
	
	
	# ##################################################
	# instance management
	
	
	def getVerbose(self):
		return self._verbose
	#getVerbose()
	
	
	def setVerbose(self, verbose=True):
		self._verbose = verbose
	#setVerbose()
	
	
	def log(self, message=""):
		if self._verbose:
			if (self._logIndent > 0) and (not self._logHanging):
				self._logFile.write(self._logIndent * "  ")
				self._logHanging = True
			self._logFile.write(message)
			if (message == "") or (message[-1] != "\n"):
				self._logHanging = True
				self._logFile.flush()
			else:
				self._logHanging = False
	#log()
	
	
	def logPush(self, message=None):
		if message:
			self.log(message)
		if self._logHanging:
			self.log("\n")
		self._logIndent += 1
	#logPush()
	
	
	def logPop(self, message=None):
		if self._logHanging:
			self.log("\n")
		self._logIndent = max(0, self._logIndent - 1)
		if message:
			self.log(message)
	#logPop()
	
	
	def attachDatabaseFile(self, dbFile):
		dbc = self._db.cursor()
		
		# detach the current db file, if any
		if self._dbFile:
			self.log("unloading knowledge database file '%s' ..." % self._dbFile)
		try:
			dbc.execute("DETACH DATABASE `db`")
		except apsw.SQLError as e:
			# no easy way to check beforehand if a db is attached already,
			# so just ignore that error (but re-raise any other)
			if not str(e).startswith('SQLError: no such database: '):
				raise e
		if self._dbFile:
			self.log(" OK\n")
			self._dbFile = None
		
		# attach the new db file, if any
		if dbFile:
			self.logPush("loading knowledge database file '%s' ..." % dbFile)
			try:
				dbc.execute("ATTACH DATABASE ? AS `db`", (dbFile,))
			except apsw.Error as e:
				raise Error(str(e))
			self._dbFile = dbFile
			
			# establish or audit database schema
			for row in dbc.execute("SELECT COUNT(1) FROM `db`.`sqlite_master`"):
				self._dbNew = (row[0] == 0)
			if self._dbNew:
				with self._db:
					self.createDatabaseObjects(None, 'db')
			else:
				self.auditDatabaseObjects(None, 'db')
			self.logPop("... OK\n")
		else:
			self._dbFile = None
			self._dbNew = None
		#if new dbFile
	#attachDatabaseFile()
	
	
	def detachDatabaseFile(self):
		return self.attachDatabaseFile(None)
	#detachDatabaseFile()
	
	
	# ##################################################
	# database structure management
	
	
	def createDatabaseObjects(self, schema, dbName, tblList=None, tables=True, indexes=True):
		dbc = self._db.cursor()
		schema = schema or self._schema[dbName]
		dbType = "TEMP " if (dbName == "temp") else ""
		if not tblList or tblList == '*':
			tblList = schema.keys()
		elif isinstance(tblList, str):
			tblList = (tblList,)
		for tblName in tblList:
			if tables:
				dbc.execute("CREATE %sTABLE IF NOT EXISTS `%s`.`%s` %s" % (dbType, dbName, tblName, schema[tblName]['table']))
			if indexes:
				for idxName in schema[tblName]['index']:
					dbc.execute("CREATE INDEX IF NOT EXISTS `%s`.`%s` ON `%s` %s" % (dbName, idxName, tblName, schema[tblName]['index'][idxName]))
				dbc.execute("ANALYZE `%s`.`%s`" % (dbName,tblName))
		#foreach tblName in tblList
	#createDatabaseObjects()
	
	
	def createDatabaseTables(self, schema, dbName, tblList, indexes=False):
		return self.createDatabaseObjects(schema, dbName, tblList, True, indexes)
	#createDatabaseTables()
	
	
	def createDatabaseIndexes(self, schema, dbName, tblList, tables=False):
		return self.createDatabaseObjects(schema, dbName, tblList, tables, True)
	#createDatabaseIndexes()
	
	
	def dropDatabaseObjects(self, schema, dbName, tblList=None, tables=True, indexes=True):
		dbc = self._db.cursor()
		schema = schema or self._schema[dbName]
		if not tblList or tblList == '*':
			tblList = schema.keys()
		elif isinstance(tblList, str):
			tblList = (tblList,)
		for tblName in tblList:
			if indexes:
				for idxName in schema[tblName]['index']:
					dbc.execute("DROP INDEX IF EXISTS `%s`.`%s`" % (dbName, idxName))
			if tables:
				dbc.execute("DROP TABLE IF EXISTS `%s`.`%s`" % (dbName, tblName))
		#foreach tblName in tblList
	#dropDatabaseObjects()
	
	
	def dropDatabaseTables(self, schema, dbName, tblList):
		return self.dropDatabaseObjects(schema, dbName, tblList, True, True)
	#dropDatabaseTables()
	
	
	def dropDatabaseIndexes(self, schema, dbName, tblList):
		return self.dropDatabaseObjects(schema, dbName, tblList, False, True)
	#dropDatabaseIndexes()
	
	
	def auditDatabaseObjects(self, schema, dbName, tblList=None, tables=True, indexes=True):
		dbc = self._db.cursor()
		schema = schema or self._schema[dbName]
		master = "`sqlite_temp_master`" if (dbName == "temp") else ("`%s`.`sqlite_master`" % dbName)
		if not tblList or tblList == '*':
			tblList = schema.keys()
		elif isinstance(tblList,str):
			tblList = (tblList,)
		for tblName in tblList:
			try:
				if tables:
					sql = dbc.execute("SELECT sql FROM %s WHERE type=? AND name=?" % master, ('table',tblName)).next()[0]
					if sql != ("CREATE TABLE `%s` %s" % (tblName, schema[tblName]['table'].rstrip())):
						self.log("WARNING: table '%s' schema mismatch" % tblName)
				if indexes:
					for idxName in schema[tblName]['index']:
						try:
							sql = dbc.execute("SELECT sql FROM %s WHERE type=? AND name=?" % master, ('index',idxName)).next()[0]
							if sql != ("CREATE INDEX `%s` ON `%s` %s" % (idxName, tblName, schema[tblName]['index'][idxName].rstrip())):
								self.log("WARNING: index '%s' on table '%s' schema mismatch" % (tblName, idxName))
						except StopIteration:
							self.log("WARNING: index '%s' on table '%s' missing" % (idxName, tblName))
			except StopIteration:
				self.log("WARNING: table '%s' missing" % tblName)
		#foreach tblName in tblList
	#auditDatabaseTables()
	
	
	def defragmentDatabase(self):
		# unfortunately sqlite's VACUUM doesn't work on attached databases,
		# so we have to detach, make a new direct connection, then re-attach
		if self._dbFile:
			dbFile = self._dbFile
			self.detachDatabaseFile()
			db = apsw.Connection(dbFile)
			dbc = db.cursor()
			dbc.execute("VACUUM")
			dbc.close()
			db.close()
			self.attachDatabaseFile(dbFile)
	#defragmentDatabase()
	
	
	# ##################################################
	# database update
	
	
	@classmethod
	def findSourceModules(cls):
		if cls._source_loaders == None:
			cls._source_loaders = {}
			for srcImporter,srcModuleName,_ in pkgutil.iter_modules():
				if srcModuleName.startswith('loki_source_'):
					cls._source_loaders[srcModuleName[12:]] = srcImporter.find_module(srcModuleName)
	#findSourceModules()
	
	
	@classmethod
	def listSourceModules(cls):
		cls.findSourceModules()
		return cls._source_loaders.keys()
	#listSourceModules()
	
	
	def updateDatabase(self, sources, cacheOnly=False):
		# find all available source handler modules
		import loki_source
		self.findSourceModules()
		
		# load and instantiate all requested sources
		srcSet = set()
		for srcName in set(sources):
			if srcName not in self._source_objects:
				if srcName not in self._source_classes:
					if srcName not in self._source_loaders:
						self.log("WARNING: unknown source '%s'\n" % srcName)
						continue
					#if module not available
					srcModule = self._source_loaders[srcName].load_module('loki_source_%s' % srcName)
					srcClass = getattr(srcModule, 'Source_%s' % srcName)
					if not issubclass(srcClass, loki_source.Source):
						self.log("WARNING: invalid module for source '%s'\n" % srcName)
						continue
					self._source_classes[srcName] = srcClass
				#if module not loaded
				self._source_objects[srcName] = self._source_classes[srcName](self)
			#if module not instantiated
			srcSet.add(srcName)
		#foreach source
		
		# update from all requested sources
		iwd = os.getcwd()
		for srcName in srcSet:
			# switch to cache directory
			path = os.path.join('loki_cache', srcName)
			if not os.path.exists(path):
				os.makedirs(path)
			os.chdir(path)
			# download files into a local cache
			if not cacheOnly:
				self.logPush("downloading %s data ...\n" % srcName)
				self._source_objects[srcName].download()
				self.logPop("... OK\n")
			# process new files
			self.logPush("processing %s data ...\n" % srcName)
			self._source_objects[srcName].update()
			os.chdir(iwd)
			self.logPop("... OK\n")
		#foreach source
		
		# don't do this automatically, starts taking a long time on a full db..
		if 0:
			self.log("defragmenting database ...")
			self.defragmentDatabase()
			self.log(" OK\n")
	#updateDatabase()
	
	
	# ##################################################
	# metadata retrieval
	
	
	def getNamespaceID(self, name):
		result = None
		for row in self._db.cursor().execute("SELECT `namespace_id` FROM `db`.`namespace` WHERE `namespace` = LOWER(?)", (name,)):
			result = row[0]
		return result
	#getNamespaceID()
	
	
	def getNamespaceIDs(self, names):
		result = { name:None for name in names }
		for row in self._db.cursor().executemany("SELECT `namespace`,`namespace_id` FROM `db`.`namespace` WHERE `namespace` = LOWER(?)", ((name,) for name in result)):
			result[row[0]] = row[1]
		return result
	#getNamespaceIDs()
	
	
	def getPopulationID(self, name):
		result = NOne
		for row in self._db.cursor().execute("SELECT `population_id` FROM `db`.`population` WHERE `population` = LOWER(?)", (name,)):
			result = row[0]
		return result
	#getPopulationID()
	
	
	def getPopulationIDs(self, names):
		result = { name:None for name in names }
		for row in self._db.cursor().executemany("SELECT `population`,`population_id` FROM `db`.`population` WHERE `population` = LOWER(?)", ((name,) for name in result)):
			result[row[0]] = row[1]
		return result
	#getPopulationIDs()
	
	
	def getRelationshipID(self, name):
		result = None
		for row in self._db.cursor().execute("SELECT `relationship_id` FROM `db`.`relationship` WHERE `relationship` = LOWER(?)", (name,)):
			result = row[0]
		return result
	#getRelationshipID()
	
	
	def getRelationshipIDs(self, names):
		result = { name:None for name in names }
		for row in self._db.cursor().executemany("SELECT `relationship`,`relationship_id` FROM `db`.`relationship` WHERE `relationship` = LOWER(?)", ((name,) for name in result)):
			result[row[0]] = row[1]
		return result
	#getRelationshipIDs()
	
	
	def getRoleID(self, name):
		result = None
		for row in self._db.cursor().execute("SELECT `role_id` FROM `db`.`role` WHERE `role` = LOWER(?)", (name,)):
			result = row[0]
		return result
	#getRoleID()
	
	
	def getRoleIDs(self, names):
		result = { name:None for name in names }
		for row in self._db.cursor().executemany("SELECT `role`,`role_id` FROM `db`.`role` WHERE `role` = LOWER(?)", ((name,) for name in result)):
			result[row[0]] = row[1]
		return result
	#getRoleIDs()
	
	
	def getSourceID(self, name):
		result = None
		for row in self._db.cursor().execute("SELECT `source_id` FROM `db`.`source` WHERE `source` = LOWER(?)", (name,)):
			result = row[0]
		return result
	#getSourceID()
	
	
	def getSourceIDs(self, names):
		result = { name:None for name in names }
		for row in self._db.cursor().executemany("SELECT `source`,`source_id` FROM `db`.`source` WHERE `source` = LOWER(?)", ((name,) for name in result)):
			result[row[0]] = row[1]
		return result
	#getSourceIDs()
	
	
	def getTypeID(self, name):
		result = None
		for row in self._db.cursor().execute("SELECT `type_id` FROM `db`.`type` WHERE `type` = LOWER(?)", (name,)):
			result = row[0]
		return result
	#getTypeID()
	
	
	def getTypeIDs(self, names):
		result = { name:None for name in names }
		for row in self._db.cursor().executemany("SELECT `type`,`type_id` FROM `db`.`type` WHERE `type` = LOWER(?)", ((name,) for name in result)):
			result[row[0]] = row[1]
		return result
	#getTypeIDs()
	
	
	# ##################################################
	# data retrieval
	
	def getGroupIDsByName(self, name, namespaceID=None, typeID=None):
		dbc = self._db.cursor()
		if typeID and namespaceID:
			result = dbc.execute("""
SELECT DISTINCT gn.`group_id`
FROM `db`.`group_name` AS gn
JOIN `db`.`group` AS g
  ON g.`group_id` = gn.`group_id` AND g.`type_id` = ?
WHERE gn.`name` = ? AND gn.`namespace_id` = ?
""", (typeID,name,namespaceID))
		elif typeID:
			result = dbc.execute("""
SELECT DISTINCT gn.`group_id`
FROM `db`.`group_name` AS gn
JOIN `db`.`group` AS g
  ON g.`group_id` = gn.`group_id` AND g.`type_id` = ?
WHERE gn.`name` = ?
""", (typeID,name))
		elif namespaceID:
			result = dbc.execute("""
SELECT DISTINCT gn.`group_id`
FROM `db`.`group_name` AS gn
WHERE gn.`name` = ? AND gn.`namespace_id` = ?
""", (name,namespaceID))
		else:
			result = dbc.execute("""
SELECT DISTINCT gn.`group_id`
FROM `db`.`group_name` AS gn
WHERE gn.`name` = ?
""", (name,))
		return [row[0] for row in result]
	#getGroupIDsByName()
	
	
	def getRegionIDsByName(self, name, namespaceID=None, typeID=None, matchMode=None):
		return self.getRegionIDsByNames((name,), (namespaceID,), typeID, matchMode)
	#getRegionIDsByName()
	
	
	def getRegionIDsByNames(self, names, namespaceIDs, typeID=None, matchMode=None):
		dbc = self._db.cursor()
		sql = """
SELECT rn.region_id
FROM db.region_name AS rn"""
		if typeID:
			sql += """
JOIN db.region AS r
  ON r.region_id = rn.region_id AND r.type_id = %d""" % typeID
		sql += """
WHERE rn.name = ? AND (rn.namespace_id = ? OR COALESCE(?,0) = 0)"""
		setArgs = set(args for args in zip(names,namespaceIDs,namespaceIDs) if args[0] != None)
		
		if (matchMode == None) or (matchMode == self.MATCH_ALL):
			return list( set( row[0] for row in dbc.executemany(sql, setArgs) ) )
		elif matchMode == self.MATCH_FIRST:
			regionIDs = None
			for args in setArgs:
				newIDs = set( row[0] for row in dbc.execute(sql, args) )
				if not regionIDs:
					regionIDs = newIDs
				else:
					regionIDs &= newIDs
				if len(regionIDs) == 1:
					return list(regionIDs)
			return list()
		elif matchMode == self.MATCH_BEST:
			regionHits = dict()
			bestHits = 0
			for row in dbc.executemany(sql, setArgs):
				regionID = row[0]
				hits = (regionHits.get(regionID) or 0) + 1
				regionHits[regionID] = hits
				bestHits = max(bestHits, hits)
			return [ regionID for regionID in regionHits if regionHits[regionID] == bestHits ]
		else:
			raise ValueError("invalid matchMode '%s'" % matchMode)
	#getRegionIDsByNames()
	
	
	def getRegionNameStats(self, namespaceID=None, typeID=None):
		dbc = self._db.cursor()
		if typeID and namespaceID:
			result = dbc.execute("""
SELECT
  COUNT(1) AS `total`,
  SUM(CASE WHEN names = 1 THEN 1 ELSE 0 END) AS `unique`,
  SUM(CASE WHEN names > 1 AND regions = 1 THEN 1 ELSE 0 END) AS `redundant`,
  SUM(CASE WHEN names > 1 AND regions > 1 THEN 1 ELSE 0 END) AS `ambiguous`
FROM (
  SELECT name, COUNT() AS names, COUNT(DISTINCT rn.`region_id`) AS regions
  FROM `db`.`region_name` AS rn
  JOIN `db`.`region` AS r
    ON r.`region_id` = rn.`region_id` AND r.`type_id` = ?
  WHERE rn.`namespace_id` = ?
  GROUP BY rn.`name`
)
""", (typeID,namespaceID))
		elif typeID:
			result = dbc.execute("""
SELECT
  COUNT(1) AS `total`,
  SUM(CASE WHEN names = 1 THEN 1 ELSE 0 END) AS `unique`,
  SUM(CASE WHEN names > 1 AND regions = 1 THEN 1 ELSE 0 END) AS `redundant`,
  SUM(CASE WHEN names > 1 AND regions > 1 THEN 1 ELSE 0 END) AS `ambiguous`
FROM (
  SELECT name, COUNT() AS names, COUNT(DISTINCT rn.`region_id`) AS regions
  FROM `db`.`region_name` AS rn
  JOIN `db`.`region` AS r
    ON r.`region_id` = rn.`region_id` AND r.`type_id` = ?
  GROUP BY rn.`name`
)
""", (typeID,))
		elif namespaceID:
			result = dbc.execute("""
SELECT
  COUNT(1) AS `total`,
  SUM(CASE WHEN names = 1 THEN 1 ELSE 0 END) AS `unique`,
  SUM(CASE WHEN names > 1 AND regions = 1 THEN 1 ELSE 0 END) AS `redundant`,
  SUM(CASE WHEN names > 1 AND regions > 1 THEN 1 ELSE 0 END) AS `ambiguous`
FROM (
  SELECT name, COUNT() AS names, COUNT(DISTINCT rn.`region_id`) AS regions
  FROM `db`.`region_name` AS rn
  WHERE rn.`namespace_id` = ?
  GROUP BY rn.`name`
)
""", (namespaceID,))
		else:
			result = dbc.execute("""
SELECT
  COUNT(1) AS `total`,
  SUM(CASE WHEN names = 1 THEN 1 ELSE 0 END) AS `unique`,
  SUM(CASE WHEN names > 1 AND regions = 1 THEN 1 ELSE 0 END) AS `redundant`,
  SUM(CASE WHEN names > 1 AND regions > 1 THEN 1 ELSE 0 END) AS `ambiguous`
FROM (
  SELECT name, COUNT() AS names, COUNT(DISTINCT rn.`region_id`) AS regions
  FROM `db`.`region_name` AS rn
  GROUP BY rn.`name`
)
""")
		for row in result:
			ret = { 'total':row[0], 'unique':row[1], 'redundant':row[2], 'ambiguous':row[3] }
		return ret
	#getRegionNameStats()
	
#Database
