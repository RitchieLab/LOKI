#!/usr/bin/env python

import os
import pkgutil

import loki_db
import loki_source
import loaders


class Updater(object):
	
	
	# ##################################################
	# constructor
	
	
	def __init__(self, lokidb):
		assert(isinstance(lokidb, loki_db.Database))
		self._loki = lokidb
		self._db = lokidb._db
		self._sourceLoaders = None
		self._sourceClasses = dict()
		self._sourceObjects = dict()
		self._updating = False
		self._tablesUpdated = None
		self._tablesDeindexed = None
	#__init__()
	
	
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
	# database update
	
	
	def prepareTableForUpdate(self, table):
		if self._updating:
			self._tablesUpdated.add(table)
			if table not in self._tablesDeindexed:
				self._tablesDeindexed.add(table)
				self._loki.dropDatabaseIndexes(None, 'db', table)
	#prepareTableForUpdate()
	
	
	def prepareTableForQuery(self, table):
		if self._updating:
			if table in self._tablesDeindexed:
				self._tablesDeindexed.remove(table)
				self._loki.createDatabaseIndexes(None, 'db', table)
	#prepareTableForQuery()
	
	
	def findSourceModules(self):
		if self._sourceLoaders == None:
			self._sourceLoaders = {}
			for srcImporter,srcModuleName,_ in pkgutil.iter_modules(loaders.__path__):
				if srcModuleName.startswith('loki_source_'):
					self._sourceLoaders[srcModuleName[12:]] = srcImporter.find_module(srcModuleName)
	#findSourceModules()
	
	
	def listSourceModules(self):
		self.findSourceModules()
		return self._sourceLoaders.keys()
	#listSourceModules()
	
	
	def updateDatabase(self, sources, cacheOnly=False):
		# load and instantiate all requested sources
		self.findSourceModules()
		sources = sources or self.listSourceModules()
		srcSet = set()
		for srcName in set(sources):
			if srcName not in self._sourceObjects:
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
				self._sourceObjects[srcName] = self._sourceClasses[srcName](self._loki)
			#if module not instantiated
			srcSet.add(srcName)
		#foreach source
		
		# update from all requested sources
		if self._updating:
			raise Exception('_updating set before updateDatabase()')
		self._updating = True
		self._tablesUpdated = set()
		self._tablesDeindexed = set()
		with self._db:
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
					self._sourceObjects[srcName].download()
					self.logPop("... OK\n")
				# process new files
				self.logPush("processing %s data ...\n" % srcName)
				self._sourceObjects[srcName].update()
				os.chdir(iwd)
				self.logPop("... OK\n")
			#foreach source
			
			# regenerate derived tables as needed
			if 'region_name' in self._tablesUpdated or 'region_name_name' in self._tablesUpdated:
				self.resolveRegionNames()
			if 'region_name' in self._tablesUpdated or 'snp_role_entrez' in self._tablesUpdated:
				self.resolveSNPRoles()
			if 'region_name' in self._tablesUpdated or 'group_region_name' in self._tablesUpdated:
				self.resolveGroupRegions()
			if 'region_bound' in self._tablesUpdated:
				self.updateRegionZones()
			
			# reindex all remaining tables
			if self._tablesDeindexed:
				self.log("finalizing update ...")
				self._loki.createDatabaseIndexes(None, 'db', self._tablesDeindexed)
				self.log(" OK\n")
		#with db transaction
		self._updating = False
		self._tablesUpdated = None
		self._tablesDeindexed = None
		
		# don't defragment automatically, starts taking a long time on a full db..
		if 0:
			self.log("defragmenting database ...")
			self._loki.defragmentDatabase()
			self.log(" OK\n")
	#updateDatabase()
	
	
	def resolveRegionNames(self): #TODO: iterative?
		self.log("resolving region names ...")
		dbc = self._db.cursor()
		
		# calculate confidence scores for each possible name match
		dbc.execute("""
CREATE TEMP TABLE `temp`.`_region_name_name_score` (
  new_namespace_id INTERGER NOT NULL,
  new_name VARCHAR(256) NOT NULL,
  region_id INTEGER NOT NULL,
  polyregion TINYINT NOT NULL,
  implication INTEGER NOT NULL,
  PRIMARY KEY (new_namespace_id, new_name, region_id)
)
""")
		self.prepareTableForQuery('region_name_name')
		self.prepareTableForQuery('region_name')
		dbc.execute("""
INSERT INTO `temp`.`_region_name_name_score`
/* calculate implication score for each possible match for each name */
SELECT
  rnn.new_namespace_id,
  rnn.new_name,
  rn.region_id,
  COALESCE(n.polyregion,0) AS polyregion,
  COUNT(1) AS implication
FROM `db`.`region_name_name` AS rnn
JOIN `db`.`region_name` AS rn USING (name)
JOIN `db`.`region` AS r USING (region_id)
LEFT JOIN `db`.`namespace` AS n
  ON n.namespace_id = rnn.new_namespace_id
WHERE rnn.namespace_id IN (0,rn.namespace_id)
  AND rnn.type_id IN (0,r.type_id)
GROUP BY rnn.new_namespace_id, rnn.new_name, rn.region_id
""")
		
		# extrapolate new region_name records
		self.prepareTableForUpdate('region_name')
		dbc.execute("DELETE FROM `db`.`region_name` WHERE derived > 0")
		dbc.execute("""
INSERT OR IGNORE INTO `db`.`region_name`
/* identify specific match with the best score for each name */
SELECT
  region_id,
  new_namespace_id,
  new_name,
  1 AS derived,
  0 AS source_id
FROM (
  /* identify names with only one best-score match */
  SELECT
    new_namespace_id,
    new_name,
    name_implication,
    SUM(CASE WHEN implication >= name_implication THEN 1 ELSE 0 END) AS match_implication
  FROM (
    /* identify best score for each name */
    SELECT
      new_namespace_id,
      new_name,
      MAX(implication) AS name_implication
    FROM `temp`.`_region_name_name_score`
    GROUP BY new_namespace_id, new_name
  )
  JOIN `temp`.`_region_name_name_score` USING (new_namespace_id, new_name)
  GROUP BY new_namespace_id, new_name
  HAVING polyregion > 0 OR match_implication = 1
)
JOIN `temp`.`_region_name_name_score` USING (new_namespace_id, new_name)
WHERE polyregion > 0 OR implication >= name_implication
""")
		
		# clean up
		dbc.execute("DROP TABLE `temp`.`_region_name_name_score`")
		numTotal = numUnrec = numMatch = 0
		self.prepareTableForQuery('region_name_name')
		self.prepareTableForQuery('region_name')
		for row in dbc.execute("""
SELECT COUNT(), SUM(CASE WHEN regions < 1 THEN 1 ELSE 0 END)
FROM (
  SELECT COUNT(DISTINCT r.region_id) AS regions
  FROM `db`.`region_name_name` AS rnn
  LEFT JOIN `db`.`region_name` AS rn
    ON rn.name = rnn.name
    AND rnn.namespace_id IN (0,rn.namespace_id)
  LEFT JOIN `db`.`region` AS r
    ON r.region_id = rn.region_id
    AND rnn.type_id IN (0,r.type_id)
  GROUP BY new_namespace_id, new_name
)
"""):
			numTotal = row[0]
			numUnrec = row[1]
		for row in dbc.execute("""
SELECT COUNT()
FROM (
  SELECT 1
  FROM `db`.`region_name`
  WHERE derived > 0
  GROUP BY namespace_id, name
)
"""):
			numMatch = row[0]
		numAmbig = numTotal - numUnrec - numMatch
		self.log(" OK: %d identifiers (%d ambiguous, %d unrecognized)\n" % (numMatch,numAmbig,numUnrec))
	#resolveRegionNames()
	
	
	def resolveSNPRoles(self):
		self.log("resolving SNP roles ...")
		dbc = self._db.cursor()
		
		namespaceID = self._loki.getNamespaceID('entrez_gid')
		if namespaceID:
			self.prepareTableForUpdate('snp_role')
			dbc.execute("DELETE FROM `db`.`snp_role`")
			dbc.execute("""
INSERT OR IGNORE INTO `db`.`snp_role` (rs, region_id, role_id)
SELECT sre.rs, rn.region_id, sre.role_id
FROM `db`.`snp_role_entrez` AS sre
JOIN `db`.`region_name` AS rn
  ON rn.namespace_id = ? AND rn.name = sre.region_entrez
""", (namespaceID,))
		#if entrez_gid
		
		numTotal = numSNPs = numGenes = 0
		self.prepareTableForQuery('snp_role')
		for row in dbc.execute("SELECT COUNT(), COUNT(DISTINCT rs), COUNT(DISTINCT region_id) FROM `db`.`snp_role`"):
			numTotal = row[0]
			numSNPs = row[1]
			numGenes = row[2]
		self.log(" OK: %d roles (%d SNPs, %d genes)\n" % (numTotal,numSNPs,numGenes))
	#resolveSNPRoles()
	
	
	def resolveGroupRegions(self):
		self.log("resolving group regions ...")
		dbc = self._db.cursor()
		
		# calculate confidence scores for each possible name match
		dbc.execute("""
CREATE TEMP TABLE `temp`.`_group_region_name_score` (
  group_id INTERGER NOT NULL,
  member INTEGER NOT NULL,
  polynames INTEGER NOT NULL,
  region_id INTEGER NOT NULL,
  implication INTEGER NOT NULL,
  quality INTEGER NOT NULL,
  PRIMARY KEY (group_id, member, region_id)
)
""")
		self.prepareTableForQuery('group_region_name')
		self.prepareTableForQuery('region_name')
		dbc.execute("""
INSERT INTO `temp`.`_group_region_name_score`
/* calculate implication and quality scores for each possible match for each member */
SELECT
  group_id,
  member,
  polynames,
  region_id,
  COUNT(DISTINCT grn_rowid) AS implication,
  (CASE WHEN polynames > 0 THEN 1000 * COUNT(DISTINCT grn_rowid) ELSE SUM(1000 / region_count) END) AS quality
FROM (
  /* count the number of possible matches for each name of each member */
  SELECT
    grn._ROWID_ AS grn_rowid,
    grn.group_id,
    grn.member,
    grn.namespace_id,
    grn.name,
    grn.type_id,
    polynames,
    COUNT(DISTINCT rn.region_id) AS region_count
  FROM (
    /* count the number of matchable polyregion names for each member */
    SELECT
      grn.group_id,
      grn.member,
      COUNT(DISTINCT (CASE WHEN n.polyregion > 0 THEN grn._ROWID_ ELSE NULL END)) AS polynames
    FROM `db`.`group_region_name` AS grn
    JOIN `db`.`region_name` AS rn USING (name)
    JOIN `db`.`region` AS r USING (region_id)
    LEFT JOIN `db`.`namespace` AS n
      ON n.namespace_id = grn.namespace_id
    WHERE grn.namespace_id IN (0,rn.namespace_id)
      AND grn.type_id IN (0,r.type_id)
    GROUP BY grn.group_id, grn.member
  )
  JOIN `db`.`group_region_name` AS grn USING (group_id, member)
  JOIN `db`.`region_name` AS rn USING (name)
  JOIN `db`.`region` AS r USING (region_id)
  LEFT JOIN `db`.`namespace` AS n
    ON n.namespace_id = grn.namespace_id
  WHERE grn.namespace_id IN (0,rn.namespace_id)
    AND grn.type_id IN (0,r.type_id)
    AND (n.polyregion > 0 OR polynames = 0)
  GROUP BY grn.group_id, grn.member, grn.namespace_id, grn.name
) AS grn
JOIN `db`.`region_name` AS rn USING (name)
JOIN `db`.`region` AS r USING (region_id)
WHERE grn.namespace_id IN (0,rn.namespace_id)
  AND grn.type_id IN (0,r.type_id)
GROUP BY group_id, member, region_id
""")
		
		# generate group_region assignments with confidence scores
		self.prepareTableForUpdate('group_region')
		dbc.execute("DELETE FROM `db`.`group_region`")
		dbc.execute("""
/* group-region assignments with confidence scores */
INSERT INTO `db`.`group_region`
SELECT
  group_id,
  region_id,
  MAX(specificity) AS specificity,
  MAX(implication) AS implication,
  MAX(quality) AS quality
FROM (
  /* identify specific matches with the best score for each member */
  SELECT
    group_id,
    member,
    region_id,
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
      FROM `temp`.`_group_region_name_score`
      GROUP BY group_id, member, polynames
    )
    JOIN `temp`.`_group_region_name_score` USING (group_id, member, polynames)
    GROUP BY group_id, member, polynames
  )
  JOIN `temp`.`_group_region_name_score` USING (group_id, member, polynames)
  GROUP BY group_id, member, region_id
)
GROUP BY group_id, region_id
""")
		
		# generate group_region placeholders for unrecognized members
		self.prepareTableForUpdate('group_region')
		self.prepareTableForQuery('group_region_name')
		self.prepareTableForQuery('region_name')
		dbc.execute("""
INSERT INTO `db`.`group_region`
SELECT
  group_id,
  0 AS region_id,
  COUNT() AS specificity,
  0 AS implication,
  0 AS quality
FROM (
  SELECT grn.group_id
  FROM `db`.`group_region_name` AS grn
  LEFT JOIN `db`.`region_name` AS rn
    ON rn.name = grn.name
    AND grn.namespace_id IN (0,rn.namespace_id)
  LEFT JOIN `db`.`region` AS r
    ON r.region_id = rn.region_id
    AND grn.type_id IN (0,r.type_id)
  GROUP BY grn.group_id, grn.member
  HAVING MAX(r.region_id) IS NULL
)
GROUP BY group_id
""")
		
		# clean up
		dbc.execute("DROP TABLE `temp`.`_group_region_name_score`")
		numTotal = numMatch = numAmbig = numUnrec = 0
		self.prepareTableForQuery('group_region')
		for row in dbc.execute("""
SELECT
  COALESCE(SUM(CASE WHEN region_id > 0 THEN 1 ELSE 0 END),0) AS total,
  COALESCE(SUM(CASE WHEN region_id > 0 AND specificity = 100 AND implication = 100 AND quality = 100 THEN 1 ELSE 0 END),0) AS definite,
  COALESCE(SUM(CASE WHEN region_id > 0 AND (specificity < 100 OR implication < 100 OR quality < 100) THEN 1 ELSE 0 END),0) AS conditional,
  COALESCE(SUM(CASE WHEN region_id = 0 THEN specificity ELSE 0 END),0) AS unmatched
FROM `db`.`group_region`
"""):
			numTotal = row[0]
			numMatch = row[1]
			numAmbig = row[2]
			numUnrec = row[3]
		self.log(" OK: %d associations (%d definite, %d conditional, %d unrecognized)\n" % (numTotal,numMatch,numAmbig,numUnrec))
	#resolveGroupRegions()
	
	
	def updateRegionZones(self):
		self.log("calculating zone coverage ...")
		zoneSize = self._loki.getDatabaseSetting('region_zone_size')
		if not zoneSize:
			raise Exception("ERROR: could not determine database setting 'region_zone_size'")
		zoneSize = int(zoneSize)
		dbc = self._db.cursor()
		
		# make sure all regions are correctly oriented
		dbc.execute("UPDATE `db`.`region_bound` SET posMin = posMax, posMax = posMin WHERE posMin > posMax")
		
		# define zone generator
		def _zones(zoneSize, bounds):
			# bounds=[ (region_id,population_id,chr,posMin,posMax),... ]
			# yields:[ (region_id,population_id,chr,zone),... ]
			for b in bounds:
				for z in xrange(int(b[3])/zoneSize,(int(b[4])/zoneSize)+1):
					yield (b[0],b[1],b[2],z)
		#_zones()
		
		# feed all bounds through the zone generator
		self.prepareTableForUpdate('region_zone')
		self.prepareTableForQuery('region_bound')
		dbc.execute("DELETE FROM `db`.`region_zone`")
		dbc.executemany(
			"INSERT OR IGNORE INTO `db`.`region_zone` (region_id,population_id,chr,zone) VALUES (?,?,?,?)",
			_zones(
				zoneSize,
				self._db.cursor().execute("SELECT region_id,population_id,chr,posMin,posMax FROM `db`.`region_bound`")
			)
		)
		
		# clean up
		self.prepareTableForQuery('region_zone')
		for row in dbc.execute("SELECT COUNT(), COUNT(DISTINCT region_id) FROM `db`.`region_zone`"):
			numTotal = row[0]
			numGenes = row[1]
		self.log(" OK: %d records (%d regions)\n" % (numTotal,numGenes))
	#updateRegionZones()
	
	
#Updater
