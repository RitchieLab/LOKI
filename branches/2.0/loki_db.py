#!/usr/bin/env python

import apsw
import itertools
import sys


class Database(object):
	
	
	##################################################
	# public class data
	
	
	ver_maj,ver_min,ver_rev,ver_dev,ver_date = 2,0,0,'a2','2012-06-27'
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
	
	
	##################################################
	# private class data
	
	
	_schema = {
		'db': {
			##################################################
			# configuration tables
			
			
			'setting': {
				'table': """
(
  setting VARCHAR(32) PRIMARY KEY NOT NULL,
  value VARCHAR(256)
)
""",
				'data': [
					('zone_size','100000'),
					('finalized','0'),
				],
				'index': {}
			}, #.db.setting
			
			
			##################################################
			# metadata tables
			
			
			'ldprofile': {
				'table': """
(
  ldprofile_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  ldprofile VARCHAR(32) UNIQUE NOT NULL,
  comment VARCHAR(128),
  description VARCHAR(128)
)
""",
				'index': {}
			}, #.db.ldprofile
			
			
			'namespace': {
				'table': """
(
  namespace_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  namespace VARCHAR(32) UNIQUE NOT NULL,
  polygenic TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {}
			}, #.db.namespace
			
			
			'relationship': {
				'table': """
(
  relationship_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  relationship VARCHAR(32) UNIQUE NOT NULL
)
""",
				'index': {}
			}, #.db.relationship
			
			
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
			
			
			'source': {
				'table': """
(
  source_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  source VARCHAR(32) UNIQUE NOT NULL,
  updated DATETIME,
  build INTEGER
)
""",
				'index': {}
			}, #.db.source
			
			
			'type': {
				'table': """
(
  type_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  type VARCHAR(32) UNIQUE NOT NULL
)
""",
				'index': {}
			}, #.db.type
			
			
			##################################################
			# snp tables
			
			
			'snp_merge': {
				'table': """
(
  rsMerged INTEGER PRIMARY KEY NOT NULL,
  rsCurrent INTEGER NOT NULL,
  source_id TINYINT NOT NULL
)
""",
				'index': {}
			}, #.db.snp_merge
			
			
			'snp_locus': {
				'table': """
(
  rs INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  pos BIGINT NOT NULL,
  validated TINYINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (rs,chr,pos)
)
""",
				'index': {
					'snp_locus__chr_pos_rs': '(chr,pos,rs)',
				}
			}, #.db.snp_locus
			
			
			'snp_entrez_role': {
				'table': """
(
  rs INTEGER NOT NULL,
  entrez_id INTEGER NOT NULL,
  role_id INTEGER NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (entrez_id,rs,role_id)
)
""",
				'index': {}
			}, #.db.snp_entrez_role
			
			
			'snp_biopolymer_role': {
				'table': """
(
  rs INTEGER NOT NULL,
  biopolymer_id INTEGER NOT NULL,
  role_id INTEGER NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (rs,biopolymer_id,role_id)
)
""",
				'index': {
					'snp_biopolymer_role__biopolymer_rs_role': '(biopolymer_id,rs,role_id)',
				}
			}, #.db.snp_biopolymer_role
			
			
			##################################################
			# biopolymer tables
			
			
			'biopolymer': {
				'table': """
(
  biopolymer_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  type_id TINYINT NOT NULL,
  label VARCHAR(64) NOT NULL,
  description VARCHAR(256),
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'biopolymer__type_label': '(type_id,label)',
				}
			}, #.db.biopolymer
			
			
			'biopolymer_name': {
				'table': """
(
  biopolymer_id INTEGER NOT NULL,
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (biopolymer_id,namespace_id,name)
)
""",
				'index': {
					'biopolymer_name__name_namespace_biopolymer': '(name,namespace_id,biopolymer_id)',
				}
			}, #.db.biopolymer_name
			
			
			'biopolymer_name_name': {
				# PRIMARY KEY column order satisfies the need to GROUP BY new_namespace_id, new_name
				'table': """
(
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  type_id TINYINT NOT NULL,
  new_namespace_id INTEGER NOT NULL,
  new_name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (new_namespace_id,new_name,type_id,namespace_id,name)
)
""",
				'index': {}
			}, #.db.biopolymer_name_name
			
			
			'biopolymer_region': {
				'table': """
(
  biopolymer_id INTEGER NOT NULL,
  ldprofile_id INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  posMin BIGINT NOT NULL,
  posMax BIGINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (biopolymer_id,ldprofile_id,chr,posMin,posMax)
)
""",
				'index': {
					'biopolymer_region__ldprofile_chr_min': '(ldprofile_id,chr,posMin)',
					'biopolymer_region__ldprofile_chr_max': '(ldprofile_id,chr,posMax)',
				}
			}, #.db.biopolymer_region
			
			
			'biopolymer_zone': {
				'table': """
(
  biopolymer_id INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER NOT NULL,
  PRIMARY KEY (biopolymer_id,chr,zone)
)
""",
				'index': {
					'biopolymer_zone__zone': '(chr,zone,biopolymer_id)',
				}
			}, #.db.biopolymer_zone
			
			
			##################################################
			# group tables
			
			
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
			
			
			'group_name': {
				'table': """
(
  group_id INTEGER NOT NULL,
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,namespace_id,name)
)
""",
				'index': {
					'group_name__name_namespace_group': '(name,namespace_id,group_id)',
					'group_name__source_name': '(source_id,name)',
				}
			}, #.db.group_name
			
			
			'group_group': {
				'table': """
(
  group_id INTEGER NOT NULL,
  related_group_id INTEGER NOT NULL,
  relationship_id SMALLINT NOT NULL,
  direction TINYINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,related_group_id,relationship_id,direction)
)
""",
				'index': {
					'group_group__related': '(related_group_id,group_id)',
				}
			}, #.db.group_group
			
			
			'group_biopolymer': {
				'table': """
(
  group_id INTEGER NOT NULL,
  biopolymer_id INTEGER NOT NULL,
  specificity TINYINT NOT NULL,
  implication TINYINT NOT NULL,
  quality TINYINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,biopolymer_id,source_id)
)
""",
				'index': {
					'group_biopolymer__biopolymer': '(biopolymer_id,group_id)',
				}
			}, #.db.group_biopolymer
			
			
			'group_member_name': {
				'table': """
(
  group_id INTEGER NOT NULL,
  member INTEGER NOT NULL,
  type_id TINYINT NOT NULL,
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,member,type_id,namespace_id,name)
)
""",
				'index': {}
			}, #.db.group_member_name
			
			
			##################################################
			# liftover tables
			
			
			'build_assembly': {
				'table': """
(
  build VARCHAR(8) PRIMARY KEY NOT NULL,
  assembly INTEGER NOT NULL
)
""",
				'index': {}
			}, #.db.build_assembly
			
			
			'chain': {
				'table': """
(
  chain_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  old_assembly INTEGER NOT NULL,
  score BIGINT NOT NULL,
  old_chr TINYINT NOT NULL,
  old_start INTEGER NOT NULL,
  old_end INTEGER NOT NULL,
  new_chr TINYINT NOT NULL,
  new_start INTEGER NOT NULL,
  new_end INTEGER NOT NULL,
  is_fwd TINYINT NOT NULL
)
""",
				'index': {
					'chain__assy_chr': '(old_assembly,old_chr)',
				}
			}, #.db.chain
			
			
			'chain_data': {
				'table': """
(
  chain_id INTEGER NOT NULL,
  old_start INTEGER NOT NULL,
  old_end INTEGER NOT NULL,
  new_start INTEGER NOT NULL,
  PRIMARY KEY (chain_id,old_start)
)
""",
				'index': {
					'chain_data__end': '(chain_id,old_end)',
				}
			}, #.db.chain_data
			
		}, #.db
		
	} #_schema{}
	
	
	##################################################
	# class interrogation
	
	
	@classmethod
	def getVersionString(cls):
		return "%d.%d.%d%s%s (%s)" % (cls.ver_maj, cls.ver_min, cls.ver_rev, ("-" if cls.ver_dev else ""), (cls.ver_dev or ""), cls.ver_date)
	#getVersionString()
	
	
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
	
	
	##################################################
	# constructor
	
	
	def __init__(self, dbFile=None, cacheLimit=1024*1024*1024): # default 1GB cache
		# initialize instance properties
		self._verbose = False
		self._logger = None
		self._logFile = sys.stderr
		self._logIndent = 0
		self._logHanging = False
		self._db = apsw.Connection('')
		self._dbFile = None
		self._dbNew = None
		self._updater = None
		
		dbc = self._db.cursor()
		# linux VFS doesn't usually report actual disk cluster size, so sqlite
		# ends up using 1KB pages; 4KB is probably better
		dbc.execute("PRAGMA page_size = %d" % (4*1024))
		# negative cache_size means the limit is in kilobytes
		dbc.execute("PRAGMA cache_size = -%d" % (cacheLimit / 1024))
		# for typical read-only usage, synchronization behavior is moot anyway,
		# and while updating we're not that worried about a power failure
		# corrupting the database file since the user could just start the
		# update over from the beginning; so, we'll take the performance gain
		dbc.execute("PRAGMA synchronous = OFF")
		
		self.attachDatabaseFile(dbFile)
	#__init__()
	
	
	##################################################
	# context manager
	
	
	def __enter__(self):
		return self._db.__enter__()
	#__enter__()
	
	
	def __exit__(self, excType, excVal, traceback):
		return self._db.__exit__(excType, excVal, traceback)
	#__exit__()
	
	
	##################################################
	# logging
	
	
	def getVerbose(self):
		return self._verbose
	#getVerbose()
	
	
	def setVerbose(self, verbose=True):
		self._verbose = verbose
	#setVerbose()
	
	
	def setLogger(self, logger=None):
		self._logger = logger
	#setLogger()
	
	
	def log(self, message=""):
		if self._logger:
			self._logger.log(message)
		elif self._verbose:
			if (self._logIndent > 0) and (not self._logHanging):
				self._logFile.write(self._logIndent * "  ")
				self._logHanging = True
			self._logFile.write(message)
			if (message == "") or (message[-1] != "\n"):
				self._logHanging = True
				self._logFile.flush()
			else:
				self._logHanging = False
		#if _logger
	#log()
	
	
	def logPush(self, message=None):
		if self._logger:
			self._logger.logPush(message)
		else:
			if message:
				self.log(message)
			if self._logHanging:
				self.log("\n")
			self._logIndent += 1
		#if _logger
	#logPush()
	
	
	def logPop(self, message=None):
		if self._logger:
			self._logger.logPop(message)
		else:
			if self._logHanging:
				self.log("\n")
			self._logIndent = max(0, self._logIndent - 1)
			if message:
				self.log(message)
		#if _logger
	#logPop()
	
	
	##################################################
	# database management
	
	
	def attachDatabaseFile(self, dbFile, cacheLimit=1024*1024*1024, readOnly=False, quiet=False):
		dbc = self._db.cursor()
		
		# detach the current db file, if any
		if self._dbFile and not quiet:
			self.log("unloading knowledge database file '%s' ..." % self._dbFile)
		try:
			dbc.execute("DETACH DATABASE `db`")
		except apsw.SQLError as e:
			if not str(e).startswith('SQLError: no such database: '):
				raise e
		if self._dbFile and not quiet:
			self.log(" OK\n")
		
		# reset db info
		self._dbFile = None
		self._dbNew = None
		
		# attach the new db file, if any
		if dbFile:
			if not quiet:
				self.logPush("loading knowledge database file '%s' ..." % dbFile)
			dbc.execute("ATTACH DATABASE ? AS `db`", (dbFile,))
			self._dbFile = dbFile
			self._dbNew = (0 == max(row[0] for row in dbc.execute("SELECT COUNT(1) FROM `db`.`sqlite_master`")))
			
			# configure the newly attached database just like in __init__()
			dbc.execute("PRAGMA db.page_size = 4096")
			dbc.execute("PRAGMA db.cache_size = -%d" % (cacheLimit / 1024))
			dbc.execute("PRAGMA db.synchronous = OFF")
			
			# establish or audit database schema
			with self._db:
				if self._dbNew:
					self.createDatabaseObjects(None, 'db')
					ok = True
				else:
					ok = self.auditDatabaseObjects(None, 'db')
			if ok:
				if not quiet:
					self.logPop("... OK\n")
			else:
				self._dbFile = None
				self._dbNew = None
				dbc.execute("DETACH DATABASE `db`")
				if not quiet:
					self.logPop("... ERROR\n")
		#if new dbFile
	#attachDatabaseFile()
	
	
	def detachDatabaseFile(self, quiet=False):
		return self.attachDatabaseFile(None, quiet=quiet)
	#detachDatabaseFile()
	
	
	def testDatabaseUpdate(self):
		if self._dbFile == None:
			raise Exception("ERROR: no knowledge database file is loaded")
		if int(self.getDatabaseSetting('finalized') or 0):
			raise Exception("ERROR: knowledge database has been finalized")
		try:
			if self._db.readonly('db'):
				raise Exception("ERROR: knowledge database file may not be modified")
		except AttributeError: # apsw.Connection.readonly() added in 3.7.11
			try:
				self._db.cursor().execute("UPDATE `db`.`setting` SET value = value")
			except apsw.ReadOnlyError:
				raise Exception("ERROR: knowledge database file may not be modified")
		return True
	#testDatabaseUpdate()
	
	
	def createDatabaseObjects(self, schema, dbName, tblList=None, doTables=True, idxList=None, doIndecies=True):
		dbc = self._db.cursor()
		schema = schema or self._schema[dbName]
		dbType = "TEMP " if (dbName == "temp") else ""
		if tblList and isinstance(tblList, str):
			tblList = (tblList,)
		if idxList and isinstance(idxList, str):
			idxList = (idxList,)
		for tblName in (tblList or schema.keys()):
			if doTables:
				dbc.execute("CREATE %sTABLE IF NOT EXISTS `%s`.`%s` %s" % (dbType, dbName, tblName, schema[tblName]['table']))
				if 'data' in schema[tblName] and schema[tblName]['data']:
					sql = "INSERT OR IGNORE INTO `%s`.`%s` VALUES (%s)" % (dbName, tblName, ("?,"*len(schema[tblName]['data'][0]))[:-1])
					dbc.executemany(sql, schema[tblName]['data'])
			if doIndecies:
				for idxName in (idxList or schema[tblName]['index'].keys()):
					if idxName not in schema[tblName]['index']:
						raise Exception("ERROR: no definition for index '%s' on table '%s'" % (idxName,tblName))
					dbc.execute("CREATE INDEX IF NOT EXISTS `%s`.`%s` ON `%s` %s" % (dbName, idxName, tblName, schema[tblName]['index'][idxName]))
				#foreach idxName in idxList
				dbc.execute("ANALYZE `%s`.`%s`" % (dbName,tblName))
		#foreach tblName in tblList
		if doIndecies:
			dbc.execute("ANALYZE `%s`.`sqlite_master`" % (dbName,))
	#createDatabaseObjects()
	
	
	def createDatabaseTables(self, schema, dbName, tblList, doIndecies=False):
		return self.createDatabaseObjects(schema, dbName, tblList, True, None, doIndecies)
	#createDatabaseTables()
	
	
	def createDatabaseIndecies(self, schema, dbName, tblList, doTables=False, idxList=None):
		return self.createDatabaseObjects(schema, dbName, tblList, doTables, idxList, True)
	#createDatabaseIndecies()
	
	
	def dropDatabaseObjects(self, schema, dbName, tblList=None, doTables=True, idxList=None, doIndecies=True):
		dbc = self._db.cursor()
		schema = schema or self._schema[dbName]
		if tblList and isinstance(tblList, str):
			tblList = (tblList,)
		if idxList and isinstance(idxList, str):
			idxList = (idxList,)
		for tblName in (tblList or schema.keys()):
			if doTables:
				if dbName == 'db':
					self.testDatabaseUpdate()
				dbc.execute("DROP TABLE IF EXISTS `%s`.`%s`" % (dbName, tblName))
			elif doIndecies:
				for idxName in (idxList or schema[tblName]['index'].keys()):
					dbc.execute("DROP INDEX IF EXISTS `%s`.`%s`" % (dbName, idxName))
				#foreach idxName in idxList
		#foreach tblName in tblList
	#dropDatabaseObjects()
	
	
	def dropDatabaseTables(self, schema, dbName, tblList):
		return self.dropDatabaseObjects(schema, dbName, tblList, True, None, True)
	#dropDatabaseTables()
	
	
	def dropDatabaseIndecies(self, schema, dbName, tblList, idxList=None):
		return self.dropDatabaseObjects(schema, dbName, tblList, False, idxList, True)
	#dropDatabaseIndecies()
	
	
	def auditDatabaseObjects(self, schema, dbName, tblList=None, doTables=True, idxList=None, doIndecies=True, doRepair=True):
		# fetch current schema
		dbc = self._db.cursor()
		current = dict()
		dbMaster = "`sqlite_temp_master`" if (dbName == "temp") else ("`%s`.`sqlite_master`" % dbName)
		for row in dbc.execute("SELECT tbl_name,type,name,sql FROM %s WHERE type IN ('table','index')" % dbMaster):
			tblName,objType,idxName,objDef = row
			if tblName not in current:
				current[tblName] = {'table':None, 'index':{}}
			if objType == 'table':
				current[tblName]['table'] = objDef
			elif objType == 'index':
				current[tblName]['index'][idxName] = objDef
		tblEmpty = dict()
		for tblName in current:
			tblEmpty[tblName] = True
			for row in dbc.execute("SELECT 1 FROM `%s`.`%s` LIMIT 1" % (dbName,tblName)):
				tblEmpty[tblName] = False
		# audit requested objects
		schema = schema or self._schema[dbName]
		if tblList and isinstance(tblList, str):
			tblList = (tblList,)
		if idxList and isinstance(idxList, str):
			idxList = (idxList,)
		ok = True
		for tblName in (tblList or schema.keys()):
			if doTables:
				if tblName in current:
					if current[tblName]['table'].rstrip() == ("CREATE TABLE `%s` %s" % (tblName, schema[tblName]['table'].rstrip())):
						if 'data' in schema[tblName] and schema[tblName]['data']:
							sql = "INSERT OR IGNORE INTO `%s`.`%s` VALUES (%s)" % (dbName, tblName, ("?,"*len(schema[tblName]['data'][0]))[:-1])
							dbc.executemany(sql, schema[tblName]['data'])
					elif doRepair and tblEmpty[tblName]:
						self.log("WARNING: table '%s' schema mismatch -- repairing ..." % tblName)
						self.dropDatabaseTables(schema, dbName, tblName)
						self.createDatabaseTables(schema, dbName, tblName, doIndecies)
						self.log(" OK\n")
					elif doRepair:
						self.log("ERROR: table '%s' schema mismatch -- cannot repair\n" % tblName)
						ok = False
					else:
						self.log("ERROR: table '%s' schema mismatch\n" % tblName)
						ok = False
					#if definition match
				elif doRepair:
					self.log("WARNING: table '%s' is missing -- repairing ..." % tblName)
					self.createDatabaseTables(schema, dbName, tblName, doIndecies)
					self.log(" OK\n")
				else:
					self.log("ERROR: table '%s' is missing\n" % tblName)
					ok = False
				#if tblName in current
			#if doTables
			if doIndecies:
				for idxName in (idxList or schema[tblName]['index'].keys()):
					if (tblName not in current) and not (doTables and doRepair):
						self.log("ERROR: table '%s' is missing for index '%s'\n" % (tblName, idxName))
						ok = False
					elif tblName in current and idxName in current[tblName]['index']:
						if current[tblName]['index'][idxName].rstrip() == ("CREATE INDEX `%s` ON `%s` %s" % (idxName, tblName, schema[tblName]['index'][idxName].rstrip())):
							pass
						elif doRepair:
							self.log("WARNING: index '%s' on table '%s' schema mismatch -- repairing ..." % (idxName, tblName))
							self.dropDatabaseIndecies(schema, dbName, tblName, idxName)
							self.createDatabaseIndecies(schema, dbName, tblName, False, idxName)
							self.log(" OK\n")
						else:
							self.log("ERROR: index '%s' on table '%s' schema mismatch\n" % (idxName, tblName))
							ok = False
						#if definition match
					elif doRepair:
						self.log("WARNING: index '%s' on table '%s' is missing -- repairing ..." % (idxName, tblName))
						self.createDatabaseIndecies(schema, dbName, tblName, False, idxName)
						self.log(" OK\n")
					else:
						self.log("ERROR: index '%s' on table '%s' is missing\n" % (idxName, tblName))
						ok = False
					#if tblName,idxName in current
				#foreach idxName in idxList
			#if doIndecies
		#foreach tblName in tblList
		return ok
	#auditDatabaseObjects()
	
		
	def defragmentDatabase(self):
		# unfortunately sqlite's VACUUM doesn't work on attached databases,
		# so we have to detach, make a new direct connection, then re-attach
		if self._dbFile:
			dbFile = self._dbFile
			self.detachDatabaseFile(quiet=True)
			db = apsw.Connection(dbFile)
			db.cursor().execute("VACUUM")
			db.close()
			self.attachDatabaseFile(dbFile, quiet=True)
	#defragmentDatabase()
	
	
	def finalizeDatabase(self):
		self.log("discarding intermediate data ...")
		self.testDatabaseUpdate()
		self.dropDatabaseTables(None, 'db', ('snp_entrez_role','biopolymer_name_name','group_member_name'))
		self.createDatabaseTables(None, 'db', ('snp_entrez_role','biopolymer_name_name','group_member_name'), True)
		self.log(" OK\n")
		self.log("updating optimizer statistics ...")
		self._db.cursor().execute("ANALYZE `db`")
		self.log(" OK\n")
		self.log("compacting knowledge database file (this may take several hours!) ...")
		self.defragmentDatabase()
		self.log(" OK\n")
		self.setDatabaseSetting('finalized', 1)
	#finalizeDatabase()
	
	
	def getDatabaseSetting(self, setting):
		value = None
		if self._dbFile:
			for row in self._db.cursor().execute("SELECT value FROM `db`.`setting` WHERE setting = ?", (setting,)):
				value = row[0]
		return value
	#getDatabaseSetting()
	
	
	def setDatabaseSetting(self, setting, value):
		self.testDatabaseUpdate()
		self._db.cursor().execute("INSERT OR REPLACE INTO `db`.`setting` (setting, value) VALUES (?, ?)", (setting,value))
	#setDatabaseSetting()
	
	
	def listSourceModules(self):
		if not self._updater:
			import loki_updater
			self._updater = loki_updater.Updater(self)
		return self._updater.listSourceModules()
	#listSourceModules()
	
	
	def updateDatabase(self, sources, cacheOnly=False):
		self.testDatabaseUpdate()
		if not self._updater:
			import loki_updater
			self._updater = loki_updater.Updater(self)
		return self._updater.updateDatabase(sources, cacheOnly)
	#updateDatabase()
	
	
	def prepareTableForUpdate(self, table):
		if self._updater:
			return self._updater.prepareTableForUpdate(table)
		return None
	#prepareTableForUpdate()
	
	
	def prepareTableForQuery(self, table):
		if self._updater:
			return self._updater.prepareTableForQuery(table)
		return None
	#prepareTableForQuery()
	
	
	##################################################
	# metadata retrieval
	
	
	def getLDProfileID(self, ldprofile):
		return self.getLDProfileIDs([ldprofile])[ldprofile]
	#getLDProfileID()
	
	
	def getLDProfileIDs(self, ldprofiles):
		if not self._dbFile:
			return { l:None for l in ldprofiles }
		sql = "SELECT i.ldprofile, l.ldprofile_id FROM (SELECT ? AS ldprofile) AS i LEFT JOIN `db`.`ldprofile` AS l ON l.ldprofile = LOWER(i.ldprofile)"
		return { row[0]:row[1] for row in self._db.cursor().executemany(sql, itertools.izip(ldprofiles)) }
	#getLDProfileIDs()
	
	
	def getNamespaceID(self, namespace):
		return self.getNamespaceIDs([namespace])[namespace]
	#getNamespaceID()
	
	
	def getNamespaceIDs(self, namespaces):
		if not self._dbFile:
			return { n:None for n in namespaces }
		sql = "SELECT i.namespace, n.namespace_id FROM (SELECT ? AS namespace) AS i LEFT JOIN `db`.`namespace` AS n ON n.namespace = LOWER(i.namespace)"
		return { row[0]:row[1] for row in self._db.cursor().executemany(sql, itertools.izip(namespaces)) }
	#getNamespaceIDs()
	
	
	def getRelationshipID(self, relationship):
		return self.getRelationshipIDs([relationship])[relationship]
	#getRelationshipID()
	
	
	def getRelationshipIDs(self, relationships):
		if not self._dbFile:
			return { r:None for r in relationships }
		sql = "SELECT i.relationship, r.relationship_id FROM (SELECT ? AS relationship) AS i LEFT JOIN `db`.`relationship` AS r ON r.relationship = LOWER(i.relationship)"
		return { row[0]:row[1] for row in self._db.cursor().executemany(sql, itertools.izip(relationships)) }
	#getRelationshipIDs()
	
	
	def getRoleID(self, role):
		return self.getRoleIDs([role])[role]
	#getRoleID()
	
	
	def getRoleIDs(self, roles):
		if not self._dbFile:
			return { r:None for r in roles }
		sql = "SELECT i.role, role_id FROM (SELECT ? AS role) AS i LEFT JOIN `db`.`role` AS r ON r.role = LOWER(i.role)"
		return { row[0]:row[1] for row in self._db.cursor().executemany(sql, itertools.izip(roles)) }
	#getRoleIDs()
	
	
	def getSourceID(self, source):
		return self.getSourceIDs([source])[source]
	#getSourceID()
	
	
	def getSourceIDs(self, sources):
		if not self._dbFile:
			return { s:None for s in sources }
		sql = "SELECT i.source, s.source_id FROM (SELECT ? AS source) AS i LEFT JOIN `db`.`source` AS s ON s.source = LOWER(i.source)"
		return { row[0]:row[1] for row in self._db.cursor().executemany(sql, itertools.izip(sources)) }
	#getSourceIDs()
	
	
	def getTypeID(self, type):
		return self.getTypeIDs([type])[type]
	#getTypeID()
	
	
	def getTypeIDs(self, types):
		if not self._dbFile:
			return { t:None for t in types }
		sql = "SELECT i.type, t.type_id FROM (SELECT ? AS type) AS i LEFT JOIN `db`.`type` AS t ON t.type = LOWER(i.type)"
		return { row[0]:row[1] for row in self._db.cursor().executemany(sql, itertools.izip(types)) }
	#getTypeIDs()
	
	
	##################################################
	# snp data retrieval
	
	
	def generateCurrentRSesByRS(self, rses, tally=None):
		# rses=[ rs, ... ]
		# tally=dict()
		# yield:[ (rsInput,rsCurrent), ... ]
		sql = """
SELECT i.rsMerged, COALESCE(sm.rsCurrent, i.rsMerged) AS rsCurrent
FROM (SELECT ? AS rsMerged) AS i
LEFT JOIN `db`.`snp_merge` AS sm USING (rsMerged)
"""
		numMerge = numMatch = 0
		for row in self._db.cursor().executemany(sql, itertools.izip(rses)):
			if row[1] != row[0]:
				numMerge += 1
			else:
				numMatch += 1
			yield row
		if tally != None:
			tally['merge'] = numMerge
			tally['match'] = numMatch
	#generateCurrentRSesByRS()
	
	
	def generateSNPLociByRS(self, rses, minMatch=1, maxMatch=1, validated=None, tally=None):
		# rses=[ rs, ... ]
		# tally=dict()
		# yield:[ (rs,chr,pos), ... ]
		sql = """
SELECT i.rs, sl.chr, sl.pos
FROM (SELECT ? AS rs) AS i
LEFT JOIN `db`.`snp_locus` AS sl
  ON sl.rs = i.rs
"""
		if validated != None:
			sql += "  AND sl.validated = %d" % (1 if validated else 0)
		key = matches = None
		numNull = numAmbig = numMatch = 0
		for row in itertools.chain(self._db.cursor().executemany(sql, itertools.izip(rses)), [(None,None,None)]):
			if key != row[0]:
				if key:
					if tally != None:
						if not matches:
							numNull += 1
						elif len(matches) > 1:
							numAmbig += 1
						else:
							numMatch += 1
					if (minMatch or 0) < 1 and not matches:
						yield (key,None,None)
					elif (minMatch or 0) <= len(matches) <= (maxMatch or len(matches)):
						for match in matches:
							yield match
				key = row[0]
				matches = set()
			if row[1] and row[2]:
				matches.add(row)
		#foreach row
		if tally != None:
			tally['null'] = numNull
			tally['ambig'] = numAmbig
			tally['match'] = numMatch
	#generateSNPLociByRS()
	
	
	##################################################
	# biopolymer data retrieval
	
	
	def generateBiopolymersByID(self, ids):
		# ids=[ id, ... ]
		# yield:[ (id,type_id,label,description), ... ]
		sql = "SELECT biopolymer_id, type_id, label, description FROM `db`.`biopolymer` WHERE biopolymer_id = ?"
		return self._db.cursor().executemany(sql, itertools.izip(ids))
	#generateBiopolymersByID()
	
	
	def generateBiopolymerIDsByName(self, names, minMatch=1, maxMatch=1, tally=None, namespaceID=None, typeID=None):
		# names=[ name, ... ]
		# tally=dict()
		# namespaceID=0 means to search names using any namespace
		# namespaceID=None means to search primary labels directly
		# yields (name,id)
		
		sql = """
SELECT i.name, b.biopolymer_id
FROM (SELECT ? AS name) AS i
"""
		
		if namespaceID != None:
			sql += """
LEFT JOIN `db`.`biopolymer_name` AS bn
  ON bn.name = i.name
"""
			if namespaceID:
				sql += """
  AND bn.namespace_id = %d
""" % namespaceID
		
		sql += """
LEFT JOIN `db`.`biopolymer` AS b
"""
		
		if namespaceID != None:
			sql += """
  ON b.biopolymer_id = bn.biopolymer_id
"""
		else:
			sql += """
  ON b.label = i.name
"""
		
		if typeID:
			sql += """
  AND b.type_id = %d
""" % typeID
		
		key = matches = None
		numNull = numAmbig = numMatch = 0
		for row in itertools.chain(self._db.cursor().executemany(sql, itertools.izip(names)), [(None,None)]):
			if key != row[0]:
				if key:
					if tally != None:
						if not matches:
							numNull += 1
						elif len(matches) > 1:
							numAmbig += 1
						else:
							numMatch += 1
					if minMatch < 1 and not matches:
						yield (key,None)
					elif (minMatch or 0) <= len(matches) <= (maxMatch or len(matches)):
						for match in matches:
							yield match
				key = row[0]
				matches = set()
			if row[1]:
				matches.add(row)
		#foreach row
		if tally != None:
			tally['null'] = numNull
			tally['ambig'] = numAmbig
			tally['match'] = numMatch
	#generateBiopolymerIDsByName()
	
	
	def getBiopolymerNameStats(self, namespaceID=None, typeID=None):
		sql = """
SELECT
  COUNT(1) AS `total`,
  SUM(CASE WHEN names = 1 THEN 1 ELSE 0 END) AS `unique`,
  SUM(CASE WHEN names > 1 AND matches = 1 THEN 1 ELSE 0 END) AS `redundant`,
  SUM(CASE WHEN names > 1 AND matches > 1 THEN 1 ELSE 0 END) AS `ambiguous`
FROM (
  SELECT name, COUNT() AS names, COUNT(DISTINCT bn.biopolymer_id) AS matches
  FROM `db`.`biopolymer_name` AS bn
"""
		
		if typeID:
			sql += """
  JOIN `db`.`biopolymer` AS b
    ON b.biopolymer_id = bn.biopolymer_id AND b.type_id = %d
""" % typeID
		
		if namespaceID:
			sql += """
  WHERE bn.namespace_id = %d
""" % namespaceID
		
		sql += """
  GROUP BY bn.name
"""
		for row in self._db.cursor().execute(sql):
			ret = { 'total':row[0], 'unique':row[1], 'redundant':row[2], 'ambiguous':row[3] }
		return ret
	#getBiopolymerNameStats()
	
	
	##################################################
	# group data retrieval
	
	
	def generateGroupsByID(self, ids):
		# ids=[ id, ... ]
		# yield:[ (id,type_id,label,description), ... ]
		sql = "SELECT group_id, type_id, label, description FROM `db`.`group` WHERE group_id = ?"
		return self._db.cursor().executemany(sql, itertools.izip(ids))
	#generateGroupsByID()
	
	
	def generateGroupIDsByName(self, names, minMatch=1, maxMatch=1, tally=None, namespaceID=None, typeID=None):
		# names=[ name, ... ]
		# tally=dict()
		# namespaceID=0 means to search names using any namespace
		# namespaceID=None means to search primary labels directly
		# yields (name,id)
		
		sql = """
SELECT i.name, g.group_id
FROM (SELECT ? AS name) AS i
"""
		
		if namespaceID != None:
			sql += """
LEFT JOIN `db`.`group_name` AS gn
  ON gn.name = i.name
"""
			if namespaceID:
				sql += """
  AND gn.namespace_id = %d
""" % namespaceID
		
		sql += """
LEFT JOIN `db`.`group` AS g
"""
		
		if namespaceID != None:
			sql += """
  ON g.group_id = gn.group_id
"""
		else:
			sql += """
  ON g.label = i.name
"""
		
		if typeID:
			sql += """
  AND g.type_id = %d
""" % typeID
		
		key = matches = None
		numNull = numAmbig = numMatch = 0
		for row in itertools.chain(self._db.cursor().executemany(sql, itertools.izip(names)), [(None,None)]):
			if key != row[0]:
				if key:
					if tally != None:
						if not matches:
							numNull += 1
						elif len(matches) > 1:
							numAmbig += 1
						else:
							numMatch += 1
					if minMatch < 1 and not matches:
						yield (key,None)
					elif (minMatch or 0) <= len(matches) <= (maxMatch or len(matches)):
						for match in matches:
							yield match
				key = row[0]
				matches = set()
			if row[1]:
				matches.add(row)
		#foreach row
		if tally != None:
			tally['null'] = numNull
			tally['ambig'] = numAmbig
			tally['match'] = numMatch
	#generateGroupIDsByName()
	
	
#Database
