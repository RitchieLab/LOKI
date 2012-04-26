#!/usr/bin/env python

#package loki

import sys
import os
import apsw
from contextlib import contextmanager
import pkgutil


class Database(object):
	
	# ##################################################
	# public class data
	
	ver_maj,ver_min,ver_rev,ver_date = 0,0,2,'2012-01-23'
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
	
	
	# ##################################################
	# private class data
	
	_source_plugins = {}
	_schema = {
		'db': {
			# ########## db.namespace ##########
			'namespace': {
				'table': """
(
  namespace_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  namespace VARCHAR(32) UNIQUE NOT NULL
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
			
			
			# ########## db.entity ##########
			'entity': {
				'table': """
(
  entity_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  type_id TINYINT NOT NULL,
  label VARCHAR(64) NOT NULL,
  description VARCHAR(256),
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'entity__type_label': '(type_id,label)',
				}
			}, #.db.entity
			
			# ########## db.entity_link ##########
			'entity_link': {
				'table': """
(
  entity_id INTEGER NOT NULL,
  related_entity_id INTEGER NOT NULL,
  relationship_id SMALLINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (entity_id,related_entity_id,relationship_id,source_id)
)
""",
				'index': {
					#'entity__entity': '(entity_id,related_entity_id)',
					'entity__related': '(related_entity_id,entity_id)',
				}
			}, #.db.entity_link
			
			# ########## db.entity_name ##########
			'entity_name': {
				'table': """
(
  entity_id INTEGER NOT NULL,
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (entity_id,namespace_id,name,source_id)
)
""",
				'index': {
					'entity_name__name': '(name,namespace_id)',
				}
			}, #.db.entity_name
			
			# ########## db.entity_region ##########
			'entity_region': {
				'table': """
(
  entity_id INTEGER NOT NULL,
  population_id INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  posMin BIGINT NOT NULL,
  posMax BIGINT NOT NULL,
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'entity_region__entity': '(entity_id,population_id)',
					'entity_region__posmin': '(population_id,chr,posMin)',
					'entity_region__posmax': '(population_id,chr,posMax)',
				}
			}, #.db.entity_region
			
			# ########## db.entity_zone ##########
			'entity_zone': {
				'table': """
(
  entity_id INTEGER NOT NULL,
  population_id INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER NOT NULL,
  PRIMARY KEY (entity_id,population_id,chr,zone)
)
""",
				'index': {
					'entity_zone__zone': '(population_id,chr,zone)',
				}
			}, #.db.entity_zone
		}, #.db
	} #_schema{}
	
	
	# ##################################################
	# class methods
	
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
	
	def __init__(self, db=None):
		# initialize instance properties
		self._verbose = False
		self._sources = {}
		
		# initialize instance database
		if isinstance(db, apsw.Connection):
			self._db = db
			self._dbc = self._db.cursor()
		else:
			self._db = apsw.Connection(':memory:')
			self._dbc = self._db.cursor()
			self._dbc.execute("PRAGMA synchronous=OFF")
			if db:
				self.attachDatabaseFile(db)
	#__init__()
	
	
	# ##################################################
	# context managers
	
	def __enter__(self):
		return self._db.__enter__()
	#__enter__()
	
	
	def __exit__(self, excType, excVal, traceback):
		return self._db.__exit__(excType, excVal, traceback)
	#__exit__()
	
	
	@contextmanager
	def bulkUpdateContext(self, entity=False, entity_link=False, entity_name=False, entity_region=False):
		tableList = []
		if entity:
			tableList.append('entity')
		if entity_link:
			tableList.append('entity_link')
		if entity_name:
			tableList.append('entity_name')
		if entity_region:
			tableList.append('entity_region')
			tableList.append('entity_zone')
		with self._db:
			if len(tableList) > 0:
				self.dropDatabaseIndexes(None, 'db', tableList)
			yield
			if entity_region:
				self.updateEntityZones()
			if len(tableList) > 0:
				self.createDatabaseIndexes(None, 'db', tableList)
		#with db transaction
	#bulkUpdateContext()
	
	
	# ##################################################
	# instance management
	
	def setVerbose(self, verbose=True):
		self._verbose = verbose
	#setVerbose()
	
	
	def attachDatabaseFile(self, dbFile):
		# detach the current db file, if any
		try:
			self._dbc.execute("DETACH DATABASE db")
		except apsw.SQLError:
			# maybe none is attached yet; no easy way to check, so just ignore this error
			pass
		
		# attach the new db file, if any
		ret = True
		if dbFile:
			try:
				self._dbc.execute("ATTACH DATABASE ? AS db", (dbFile,))
			except apsw.Error as e:
				raise Error(str(e))
			
			# audit database schema
			ret = self.auditDatabaseObjects(None, 'db')
		#if new dbFile
		
		return ret
	#attachDatabaseFile()
	
	
	def detachDatabaseFile(self):
		return self.attachDatabaseFile(None)
	#detachDatabaseFile()
	
	
	# ##################################################
	# database structure management
	
	def createDatabaseObjects(self, schema, dbName, tblList=None, tables=True, indexes=True):
		schema = schema or self._schema[dbName]
		dbType = "TEMP " if (dbName == "temp") else ""
		if not tblList or tblList == '*':
			tblList = schema.keys()
		elif isinstance(tblList, str):
			tblList = (tblList,)
		with self._db:
			for tblName in tblList:
				if tables:
					self._dbc.execute("CREATE %sTABLE IF NOT EXISTS %s.%s %s" % (dbType, dbName, tblName, schema[tblName]['table']))
				if indexes:
					for idxName in schema[tblName]['index']:
						self._dbc.execute("CREATE INDEX IF NOT EXISTS %s.%s ON %s %s" % (dbName, idxName, tblName, schema[tblName]['index'][idxName]))
			#foreach tblName in tblList
		#with db transaction
		return True
	#createDatabaseObjects()
	
	
	def createDatabaseTables(self, schema, dbName, tblList, indexes=False):
		return self.createDatabaseObjects(schema, dbName, tblList, True, indexes)
	#createDatabaseTables()
	
	
	def createDatabaseIndexes(self, schema, dbName, tblList, tables=False):
		return self.createDatabaseObjects(schema, dbName, tblList, tables, True)
	#createDatabaseIndexes()
	
	
	def dropDatabaseObjects(self, schema, dbName, tblList=None, tables=True, indexes=True):
		schema = schema or self._schema[dbName]
		if not tblList or tblList == '*':
			tblList = schema.keys()
		elif isinstance(tblList, str):
			tblList = (tblList,)
		with self._db:
			for tblName in tblList:
				if indexes:
					for idxName in schema[tblName]['index']:
						self._dbc.execute("DROP INDEX IF EXISTS %s.%s" % (dbName, idxName))
				if tables:
					self._dbc.execute("DROP TABLE IF EXISTS %s.%s" % (dbName, tblName))
			#foreach tblName in tblList
		#with db transaction
		return True
	#dropDatabaseObjects()
	
	
	def dropDatabaseTables(self, schema, dbName, tblList):
		return self.dropDatabaseObjects(schema, dbName, tblList, True, True)
	#dropDatabaseTables()
	
	
	def dropDatabaseIndexes(self, schema, dbName, tblList):
		return self.dropDatabaseObjects(schema, dbName, tblList, False, True)
	#dropDatabaseIndexes()
	
	
	def auditDatabaseObjects(self, schema, dbName, tblList=None, tables=True, indexes=True):
		schema = schema or self._schema[dbName]
		master = "sqlite_temp_master" if (dbName == "temp") else ("%s.sqlite_master" % dbName)
		if not tblList or tblList == '*':
			tblList = schema.keys()
		elif isinstance(tblList,str):
			tblList = (tblList,)
		warnings = []
		for tblName in tblList:
			try:
				if tables:
					sql = self._dbc.execute("SELECT sql FROM %s WHERE type=? AND name=?" % master, ('table',tblName)).next()[0]
					if sql != ("CREATE TABLE %s %s" % (tblName, schema[tblName]['table'].rstrip())):
						warnings.append("table '%s' schema mismatch" % tblName)
				if indexes:
					for idxName in schema[tblName]['index']:
						try:
							sql = self._dbc.execute("SELECT sql FROM %s WHERE type=? AND name=?" % master, ('index',idxName)).next()[0]
							if sql != ("CREATE INDEX %s ON %s %s" % (idxName, tblName, schema[tblName]['index'][idxName].rstrip())):
								warnings.append("index '%s' on table '%s' schema mismatch" % (tblName, idxName))
						except StopIteration:
							warnings.append("index '%s' on table '%s' missing" % (idxName, tblName))
			except StopIteration:
				warnings.append("table '%s' missing" % tblName)
		#foreach tblName in tblList
		return warnings if len(warnings) > 0 else True
	#auditDatabaseTables()
	
	
	# ##################################################
	# metadata management
	
	def getNamespaceID(self, name):
		result = self._dbc.execute("SELECT namespace_id FROM db.namespace WHERE namespace = ?", (name,))
		ret = None
		for row in result:
			ret = row[0]
		return ret
	#getNamespaceID()
	
	
	def addNamespace(self, name):
		try:
			self._dbc.execute("INSERT OR ABORT INTO db.namespace (namespace) VALUES (?)", (name,))
		except apsw.ConstraintError:
			return self.getNamespaceID(name)
		return self._db.last_insert_rowid()
	#addNamespace()
	
	
	def getPopulationID(self, name):
		result = self._dbc.execute("SELECT population_id FROM db.population WHERE population = ?", (name,))
		ret = None
		for row in result:
			ret = row[0]
		return ret
	#getPopulationID()
	
	
	def addPopulation(self, name, comment=None, desc=None):
		try:
			self._dbc.execute("INSERT OR ABORT INTO db.population (population,ldcomment,description) VALUES (?,?,?)", (name,comment,desc))
		except apsw.ConstraintError:
			return self.getPopulationID(name)
		return self._db.last_insert_rowid()
	#addPopulation()
	
	
	def getRelationshipID(self, name):
		result = self._dbc.execute("SELECT relationship_id FROM db.relationship WHERE relationship = ?", (name,))
		ret = None
		for row in result:
			ret = row[0]
		return ret
	#getRelationshipID()
	
	
	def addRelationship(self, name):
		try:
			self._dbc.execute("INSERT OR ABORT INTO db.relationship (relationship) VALUES (?)", (name,))
		except apsw.ConstraintError:
			return self.getRelationshipID(name)
		return self._db.last_insert_rowid()
	#addRelationship()
	
	
	def getSourceID(self, name):
		result = self._dbc.execute("SELECT source_id FROM db.source WHERE source = ?", (name,))
		ret = None
		for row in result:
			ret = row[0]
		return ret
	#getSourceID()
	
	
	def addSource(self, name):
		try:
			self._dbc.execute("INSERT OR ABORT INTO db.source (source) VALUES (?)", (name,))
		except apsw.ConstraintError:
			return self.getSourceID(name)
		return self._db.last_insert_rowid()
	#addSource()
	
	
	def getTypeID(self, name):
		result = self._dbc.execute("SELECT type_id FROM db.type WHERE type = ?", (name,))
		ret = None
		for row in result:
			ret = row[0]
		return ret
	#getTypeID()
	
	
	def addType(self, name):
		try:
			self._dbc.execute("INSERT OR ABORT INTO db.type (type) VALUES (?)", (name,))
		except apsw.ConstraintError:
			return self.getTypeID(name)
		return self._db.last_insert_rowid()
	#addType()
	
	
	# ##################################################
	# data management
	
	def deleteSourceData(self, sourceID):
		with self._db:
			self._dbc.execute("DELETE FROM db.entity WHERE source_id = ?", (sourceID,))
			self._dbc.execute("DELETE FROM db.entity_link WHERE source_id = ?", (sourceID,))
			self._dbc.execute("DELETE FROM db.entity_name WHERE source_id = ?", (sourceID,))
			self._dbc.execute("DELETE FROM db.entity_region WHERE source_id = ?", (sourceID,))
		#with db transaction
	#deleteSourceData()
	
	
	def addEntity(self, typeID, label, desc, sourceID):
		self._dbc.execute("INSERT INTO db.entity (type_id,label,description,source_id) VALUES (?,?,?,?)", (typeID,label,desc,sourceID))
		return self._db.last_insert_rowid()
	#addEntity()
	
	
	def addEntities(self, entList, returnIDs=False):
		# entList=[ (type_id,label,description,source_id), ... ]
		if returnIDs:
			# return=[ entity_id, ... ]
			retList = []
			with self._db:
				for ent in entList:
					self._dbc.execute("INSERT INTO db.entity (type_id,label,description,source_id) VALUES (?,?,?,?)", ent)
					retList.append(self._db.last_insert_rowid())
			#with db transaction
			return retList
		else:
			with self._db:
				self._dbc.executemany("INSERT INTO db.entity (type_id,label,description,source_id) VALUES (?,?,?,?)", entList)
	#addEntities()
	
	
	def addEntityLinks(self, linkList):
		# linkList=[ (entity_id,related_entity_id,relationship_id,source_id), ... ]
		with self._db:
			self._dbc.executemany("INSERT INTO db.entity_link (entity_id,related_entity_id,relationship_id,source_id) VALUES (?,?,?,?)", linkList)
		#with db transaction
	#addEntityLinks()
	
	
	def addEntityName(self, entityID, namespaceID, name, sourceID):
		self._dbc.execute("INSERT INTO db.entity_name (entity_id,namespace_id,name,source_id) VALUES (?,?,?,?)", (entityID,namespaceID,name,sourceID))
	#addEntityName()
	
	
	def addEntityNames(self, nameList):
		# nameList=[ (entity_id,namespace_id,name,source_id), ... ]
		with self._db:
			self._dbc.executemany("INSERT INTO db.entity_name (entity_id,namespace_id,name,source_id) VALUES (?,?,?,?)", nameList)
		#with db transaction
	#addEntityNames()
	
	
	def addEntityRegion(self, entityID, populationID, chromosome, posMin, posMax, sourceID):
		self._dbc.execute("INSERT INTO db.entity_region (entity_id,population_id,chr,posMin,posMax,source_id) VALUES (?,?,?,?,?,?)", (entityID,populationID,chromosome,posMin,posMax,sourceID))
	#addEntityRegion()
	
	
	def addEntityRegions(self, regionList, updateZones=True):
		# regionList=[ (entity_id,population_id,chr,posMin,posMax,source_id), ... ]
		with self._db:
			self._dbc.executemany("INSERT INTO db.entity_region (entity_id,population_id,chr,posMin,posMax,source_id) VALUES (?,?,?,?,?,?)", regionList)
			if updateZones:
				self.updateEntityZones()
		#with db transaction
	#addEntityRegions()
	
	
	def updateEntityZones(self):
		with self._db:
			for row in self._dbc.execute("SELECT MAX(posMax) FROM db.entity_region"):
				maxZone = int(row[0]) / 100000
			self._dbc.execute("CREATE TEMP TABLE temp.zones (zone INTEGER PRIMARY KEY NOT NULL)")
			self._dbc.executemany("INSERT INTO temp.zones (zone) VALUES (?)", [(zone,) for zone in range(maxZone+1)])
			self._dbc.execute("DELETE FROM db.entity_zone")
			self._dbc.execute("""
INSERT INTO db.entity_zone (entity_id,population_id,chr,zone)
SELECT DISTINCT er.entity_id, er.population_id, er.chr, tz.zone
FROM db.entity_region AS er
JOIN temp.zones AS tz
  ON tz.zone >= er.posMin / 100000
  AND tz.zone <= er.posMax / 100000
""")
			self._dbc.execute("DROP TABLE temp.zones")
		#with db transaction
	#updateEntityZones()
	
	
	def updateDatabase(self, sourceList):
		# create any missing tables or indexes
		if self._verbose:
			sys.stderr.write("verifying database file ...")
			sys.stderr.flush()
		self.createDatabaseObjects(None, 'db')
		if self._verbose:
			sys.stderr.write(" OK\n")
		
		# load source plugin classes, if we haven't yet
		import loki.source
		if len(self._source_plugins) < 1:
			for importer,srcModuleName,_ in pkgutil.iter_modules(['loki']):
				if srcModuleName.startswith('source_'):
					srcModule = importer.find_module(srcModuleName).load_module('loki.%s' % srcModuleName)
					srcName = srcModuleName[7:]
					srcClass = getattr(srcModule, 'Source_%s' % srcName)
					if issubclass(srcClass, loki.source.Source):
						self._source_plugins[srcName] = srcClass
		
		# update from all requested sources
		warnings = []
		iwd = os.getcwd()
		for source in sourceList:
			for srcName in (self._source_plugins if source == "all" else (source,)):
				# make sure a matching Source_* module was found
				if srcName not in self._source_plugins:
					if self._verbose:
						sys.stderr.write("WARNING: unknown source '%s'\n" % srcName)
					else:
						warnings.append("unknown source '%s'" % srcName)
				else:
					# instantiate the source class against this database instance
					srcID = self.addSource(srcName)
					if srcName in self._sources:
						srcObj = self._sources[srcName]
					else:
						srcObj = self._source_plugins[srcName](self, srcID)
						srcObj.setVerbose(self._verbose)
						self._sources[srcName] = srcObj
					# download files into a local cache
					if self._verbose:
						sys.stderr.write("downloading %s data ...\n" % srcName)
					path = os.path.join('loki.cache', srcName)
					if not os.path.exists(path):
						os.makedirs(path)
					os.chdir(path)
					srcObj.download()
					# process new files
					if self._verbose:
						sys.stderr.write("updating %s ...\n" % srcName)
					srcObj.update()
					if self._verbose:
						sys.stderr.write("OK\n")
		#foreach source
		os.chdir(iwd)
		
		if self._verbose:
			sys.stderr.write("defragmenting database ...")
			sys.stderr.flush()
		self._dbc.execute("VACUUM")
		if self._verbose:
			sys.stderr.write(" OK\n")
		
		return warnings if len(warnings) > 0 else True
	#updateDatabase()
	
	
	# ##################################################
	# data retrieval
	
	def getEntityIDsByName(self, name, namespace=None, etype=None):
		if etype and namespace:
			result = self._dbc.execute("""
SELECT en.entity_id
FROM type AS t
JOIN namespace AS ns
  ON ns.namespace = ?
JOIN entity_name AS en
  ON en.name = ? AND en.namespace_id = ns.namespace_id
JOIN entity AS e
  ON e.entity_id = en.entity_id AND e.type_id = t.type_id
WHERE t.type = ?
""", (namespace,name,etype))
		elif etype:
			result = self._dbc.execute("""
SELECT en.entity_id
FROM type AS t
JOIN entity_name AS en
  ON en.name = ?
JOIN entity AS e
  ON e.entity_id = en.entity_id AND e.type_id = t.type_id
WHERE t.type = ?
""", (name,etype))
		elif namespace:
			result = self._dbc.execute("""
SELECT en.entity_id
FROM namespace AS ns
JOIN entity_name AS en
  ON en.name = ? AND en.namespace_id = ns.namespace_id
WHERE ns.namespace = ?
""", (name,namespace))
		else:
			result = self._dbc.execute("""
SELECT en.entity_id
FROM entity_name AS en
WHERE en.name = ?
""", (name,))
		return [row[0] for row in result]
	#getEntityIDsByName()
	
#Database


class Entity(object):
	
	__slots__ = ('entityID','typeID','label','desc','sourceID','_links','_names','_regions')
	
	def __init__(self, typeID=None, label=None, desc=None, sourceID=None, entityID=None):
		self.entityID = entityID
		self.typeID = typeID
		self.label = label
		self.desc = desc
		self.sourceID = sourceID
		self._links = {}
		self._names = {}
		self._regions = {}
	#__init__()
	
	
	def addLink(self, relatedEntityID, relationshipID, sourceID):
		if relatedEntityID not in self._links:
			self._links[relatedEntityID] = {}
		if relationshipID not in self._links[relatedEntityID]:
			self._links[relatedEntityID][relationshipID] = set()
		self._links[relatedEntityID][relationshipID].add(sourceID)
	#addLink()
	
	
	def getLinkList(self):
		linkList = []
		for relatedID in self._links:
			for relationshipID in self._links[relatedID]:
				for sourceID in self._links[relatedID][relationshipID]:
					linkList.append( (self.entityID, relatedID, relationshipID, sourceID) )
		return linkList
	#getLinkList()
	
		
	def addName(self, namespaceID, name, sourceID):
		if namespaceID not in self._names:
			self._names[namespaceID] = {}
		if name not in self._names[namespaceID]:
			self._names[namespaceID][name] = set()
		self._names[namespaceID][name].add(sourceID)
	#addName()
	
	
	def getNameList(self):
		nameList = []
		for namespaceID in self._names:
			for name in self._names[namespaceID]:
				for sourceID in self._names[namespaceID][name]:
					nameList.append( (self.entityID, namespaceID, name, sourceID) )
		return nameList
	#getNameList()
	
	
	def addRegion(self, populationID, chromosome, posMin, posMax, sourceID):
		if populationID not in self._regions:
			self._regions[populationID] = {}
		self._regions[populationID][sourceID] = (chromosome,posMin,posMax)
	#addRegion()
	
	
	def getRegionList(self):
		regionList = []
		for populationID in self._regions:
			for sourceID in self._regions[populationID]:
				region = self._regions[populationID][sourceID]
				regionList.append( (self.entityID, populationID, region[0], region[1], region[2], sourceID) )
		return regionList
	#getRegionList()
	
#Entity
