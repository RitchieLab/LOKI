#!/usr/bin/env python

import collections
import itertools
import sys
import traceback
from loki import loki_source


class Source_loki(loki_source.Source):
	
	
	##################################################
	# source interface
	
	
	@classmethod
	def getVersionString(cls):
		return '3.0 (2013-12-02)'
	#getVersionString()
	
	
	@classmethod
	def getOptions(cls):
		return {
			'unit-core-ns'          : 'ns1[,ns2[...]]  --  comma-separated list of namespaces to define unit cores (default: entrez_gid,ensembl_gid)',
			'require-unit-region'   : '[yes|no]  --  require all units to have at least one region (default: no)',
			'max-unit-gap'          : 'number  --  maximum basepair gap between regions to allow in one unit, or -1 for no limit (default: 25000)',
			'max-unit-alias-dist'   : 'number  --  maximum identifier graph distance from an alias to a unit core, or -1 for no limit (default: -1)',
			'max-shared-alias-dist' : 'number  --  maximum identifier graph distance to allow an alias to be shared, or -1 for no limit (default: -1)',
		}
	#getOptions()
	
	
	def validateOptions(self, options):
		options.setdefault('unit-core-ns', 'ensembl_gid,entrez_gid')
		options.setdefault('require-unit-region', 'no')
		options.setdefault('max-unit-gap', '25000')
		options.setdefault('max-unit-alias-dist', '-1')
		options.setdefault('max-shared-alias-dist', '-1')
		for o,v in options.iteritems():
			v = v.strip().lower()
			if o == 'unit-core-ns':
				v = set(ns for ns in v.split(',') if ns)
				if not v:
					return "%s must include at least one namespace" % (o,)
				v = ','.join(sorted(v))
			elif o == 'require-unit-region':
				if (v == '1') or 'true'.startswith(v) or 'yes'.startswith(v):
					v = 'yes'
				elif (v == '0') or 'false'.startswith(v) or 'no'.startswith(v):
					v = 'no'
				else:
					return "%s must be 'yes' or 'no'" % (o,)
			elif o in ('max-unit-gap','max-unit-alias-dist','max-shared-alias-dist'):
				try:
					v = int(v)
				except ValueError:
					return "invalid integer for %s: %s" % (o,v)
				if v < 0:
					v = -1
				v = str(v)
			else:
				return "unknown option '%s'" % o
			options[o] = v
		return True
	#validateOptions()
	
	
	def download(self, options):
		pass
	#download()
	
	
	def update(self, options, prevOptions, tablesUpdated, forceUpdate):
		cursor = self._db.cursor()
		logIndent = self.logIndent()
		
		# cross-map GRCh/UCSChg build versions for all sources, and set the new target
		ucscGRC = dict()
		for row in self._db.cursor().execute("SELECT grch,ucschg FROM `db`.`grch_ucschg`"):
			ucscGRC[row[1]] = max(row[0], ucscGRC.get(row[1]))
			cursor.execute("UPDATE `db`.`source` SET grch = ? WHERE grch IS NULL AND ucschg = ?", (row[0],row[1]))
			cursor.execute("UPDATE `db`.`source` SET ucschg = ? WHERE ucschg IS NULL AND grch = ?", (row[1],row[0]))
		cursor.execute("UPDATE `db`.`source` SET current_ucschg = ucschg WHERE current_ucschg IS NULL")
		targetHG = max(row[0] for row in cursor.execute("SELECT current_ucschg FROM `db`.`source` WHERE current_ucschg IS NOT NULL"))
		if (targetHG != self._loki.getDatabaseSetting('ucschg',int)):
			self.log("database genome build: GRCh%s / UCSChg%s\n" % (ucscGRC.get(targetHG,'?'), targetHG))
			self._loki.setDatabaseSetting('ucschg', targetHG)
		oldHGs = sum(1 for row in cursor.execute("SELECT 1 FROM `db`.`source` WHERE current_ucschg IS NOT NULL AND current_ucschg != %d" % (targetHG,)))
		
		# decide which post-processing steps are required
		ppCallOrder = [
			'normalizeGenomeBuilds',
			'cleanupSNPMerges',
			'updateMergedSNPLoci',
			'cleanupSNPLoci',
			'updateMergedSNPEntrezRoles',
			'cleanupSNPEntrezRoles',
			'updateMergedGWASAnnotations',
			'updateRegionZones',
			'defineOmicUnits',
			'resolveSNPUnitRoles',
			'resolveUnitRegions',
			'resolveGroupMembers',
		]
		curPP = set(ppCallOrder)
		lastPP = set(pp for pp in (self._loki.getDatabaseSetting('postProcess') or '').split(',') if pp)
		curVers = self.getVersionString()
		lastUpdate = None
		for row in cursor.execute("SELECT updated, version FROM `db`.`source` WHERE source_id = ?", (self.getSourceID(),)):
			lastUpdate = row
		if forceUpdate:
			self.log("force-update enabled; running all post-processing\n")
		elif 'all' in lastPP:
			self.log("error during prior post-process phase; re-running all post-processing\n")
		elif lastUpdate and lastUpdate[0] and (lastUpdate[1] != curVers):
			self.log("updater version changed from '%s' to '%s'; re-running all post-processing\n" % (lastUpdate[1] or "(unknown)",curVers))
		elif (lastPP - curPP):
			self.log("invalid database post-process flags; re-running all post-processing\n")
		else:
			curPP &= lastPP
		#if vers/pp mismatch
		
		# call all necessary post-processing methods
		import time
		self._options = options
		self._nameUIDs = None
		self._loki.setDatabaseSetting('postProcess', ','.join(curPP))
		donePP = set()
		ok = True
		for pp in ppCallOrder:
			# re-scan step triggers before each step, to catch steps
			# which update tables that trigger later steps
			if oldHGs:
				curPP.add('normalizeGenomeBuilds') # snp_locus, region
			if ('snp_merge' in tablesUpdated):
				curPP.add('cleanupSNPMerges') # snp_merge
			if ('snp_merge' in tablesUpdated) or ('snp_locus' in tablesUpdated):
				curPP.add('updateMergedSNPLoci') # snp_locus
			if ('snp_locus' in tablesUpdated):
				curPP.add('cleanupSNPLoci') # snp_locus
			if ('snp_merge' in tablesUpdated) or ('snp_entrez_role' in tablesUpdated):
				curPP.add('updateMergedSNPEntrezRoles') # snp_entrez_role
			if ('snp_entrez_role' in tablesUpdated):
				curPP.add('cleanupSNPEntrezRoles') # snp_entrez_role
			if ('snp_merge' in tablesUpdated) or ('gwas' in tablesUpdated):
				curPP.add('updateMergedGWASAnnotations') # gwas
			#TODO: cleanupGWASAnnotations?
			if ('region' in tablesUpdated):
				curPP.add('updateRegionZones') # region_zone
			if ('region' in tablesUpdated) or ('region_name' in tablesUpdated) or ('name_name' in tablesUpdated) or (lastUpdate and lastUpdate[0] and (options != prevOptions)):
				curPP.add('defineOmicUnits') # unit, unit_name
			if ('unit_name' in tablesUpdated) or ('snp_entrez_role' in tablesUpdated):
				curPP.add('resolveSNPUnitRoles') # snp_unit_role
			if ('unit_name' in tablesUpdated) or ('region_name' in tablesUpdated):
				curPP.add('resolveUnitRegions') # unit_region
			if ('unit_name' in tablesUpdated) or ('group_member_name' in tablesUpdated):
				curPP.add('resolveGroupMembers') # group_unit
			
			# run the step only if needed
			if pp in curPP:
				t0 = time.time()
				savepoint = 'updateDatabase_postprocess_%s' % (pp,)
				cursor.execute("SAVEPOINT '%s'" % (savepoint,))
				try:
					getattr(self,pp)()
					donePP.add(pp)
				except:
					cursor.execute("ROLLBACK TRANSACTION TO SAVEPOINT '%s'" % (savepoint,))
					excType,excVal,excTrace = sys.exc_info()
					while self.logIndent() > logIndent:
						self.logPop()
					self.logPush("ERROR: failed post-processing step %s\n" % (pp,))
					if excTrace:
						for line in traceback.format_list(traceback.extract_tb(excTrace)[-1:]):
							self.log(line)
					for line in traceback.format_exception_only(excType,excVal):
						self.log(line)
					self.logPop()
					return False
				finally:
					cursor.execute("RELEASE SAVEPOINT '%s'" % (savepoint,))
					self._loki.setDatabaseSetting('postProcess', ','.join(curPP - donePP))
				self.log("(%ds)\n" % (time.time()-t0))
			#if pp in curPP
		#foreach ppCallOrder
	#update()
	
	
	##################################################
	# postprocessing
	
	
	def doPostProcessStep(self, step):
		import time
		t0 = time.time()
		
		cursor = self._db.cursor()
		savepoint = 'updateDatabase_postprocess_%s' % (step,)
		cursor.execute("SAVEPOINT '%s'" % (savepoint,))
		try:
			getattr(self,step)()
		except Exception as e:
			cursor.execute("ROLLBACK TRANSACTION TO SAVEPOINT '%s'" % (savepoint,))
			raise e
		finally:
			cursor.execute("RELEASE SAVEPOINT '%s'" % (savepoint,))
		
		self.log("(%ds)\n" % (time.time()-t0))
	#doPostProcessStep()
	
	
	def normalizeGenomeBuilds(self):
		cursor = self._db.cursor()
		locusSources = set(row[0] for row in cursor.execute("SELECT DISTINCT source_id FROM `db`.`snp_locus`"))
		regionSources = set(row[0] for row in cursor.execute("SELECT DISTINCT source_id FROM `db`.`region`"))
		sourceIDs = ','.join(str(int(i)) for i in (locusSources | regionSources))
		newHG = self._loki.getDatabaseSetting('ucschg',int)
		hgSources = collections.defaultdict(set)
		sql = "SELECT current_ucschg, source_id FROM `db`.`source` WHERE current_ucschg IS NOT NULL AND current_ucschg != %d AND source_id IN (%s)" % (newHG,sourceIDs)
		for row in cursor.execute(sql):
			hgSources[row[0]].add(row[1])
		
		for oldHG in sorted(hgSources):
			if not self._loki.hasLiftOverChains(oldHG, newHG):
				raise Exception("ERROR: no chains available to lift hg%d to hg%d\n" % (oldHG,newHG))
			sourceIDs = ','.join(str(i) for i in hgSources[oldHG])
			
			if (hgSources[oldHG] & locusSources):
				self.log("lifting over SNP loci from hg%d to hg%d ..." % (oldHG,newHG))
				self.prepareTableForUpdate('snp_locus')
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
				
				# SQLite can't SELECT and UPDATE the same table at the same time,
				# so read in batches of 2.5 million at a time based on ID
				minID = firstID
				maxID = minID + 2500000 - 1
				while minID <= lastID:
					sql = "SELECT _ROWID_, chr, pos FROM `db`.`snp_locus` WHERE (_ROWID_ BETWEEN ? AND ?) AND source_id IN (%s)" % (sourceIDs,)
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
				tally = trash = oldLoci = newLoci = None
			#if loci
			
			if (hgSources[oldHG] & regionSources):
				self.log("lifting over regions from hg%d to hg%d ..." % (oldHG,newHG))
				self.prepareTableForUpdate('region')
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
				
				# SQLite can't SELECT and UPDATE the same table at the same time,
				# so read in batches of 2.5 million at a time based on ID
				minID = firstID
				maxID = minID + 2500000 - 1
				while minID <= lastID:
					sql = "SELECT region_id, chr, posMin, posMax FROM `db`.`region` WHERE (region_id BETWEEN ? AND ?) AND source_id IN (%s)" % (sourceIDs,)
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
				tally = trash = oldRegions = newRegions = None
			#if regions
			
			sql = "UPDATE `db`.`source` SET current_ucschg = %d WHERE current_ucschg = %d AND source_id IN (%s)" % (newHG,oldHG,sourceIDs)
			cursor.execute(sql)
		#foreach old build
	#normalizeGenomeBuilds()
	
	
	def cleanupSNPMerges(self):
		self.log("verifying SNP merge records ...")
		self.prepareTableForQuery('snp_merge')
		dbc = self._db.cursor()
		
		# for each set of ROWIDs which constitute a duplicated snp merge, cull all but one
		cull = set()
		if 0: #sql method is sometimes very slow, python method seems more consistent
			sql = "SELECT GROUP_CONCAT(_ROWID_) FROM `db`.`snp_merge` GROUP BY rsMerged HAVING COUNT() > 1"
			for row in dbc.execute(sql):
				rowids = row[0].split(',')
				cull.update( (long(rowids[i]),) for i in xrange(1,len(rowids)) )
		else:
			lastRS = None
			sql = "SELECT _ROWID_, rsMerged FROM `db`.`snp_merge` ORDER BY rsMerged DESC, rsCurrent DESC"
			for row in dbc.execute(sql):
				if lastRS == row[1]:
					cull.add( (row[0],) )
				else:
					lastRS = row[1]
		#if sql/python method
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
		if 0: #sql method is sometimes very slow, python method seems more consistent
			sql = "SELECT GROUP_CONCAT(_ROWID_), MAX(validated) FROM `db`.`snp_locus` GROUP BY rs, chr, pos HAVING COUNT() > 1"
			for row in dbc.execute(sql):
				rowids = row[0].split(',')
				if row[1]:
					valid.add( (long(rowids[0]),) )
				cull.update( (long(rowids[i]),) for i in xrange(1,len(rowids)) )
		else:
			lastID = None
			lastPos = None
			sql = "SELECT _ROWID_, rs, chr, pos, validated FROM `db`.`snp_locus` ORDER BY rs, chr, pos"
			for row in dbc.execute(sql):
				pos = (row[1],row[2],row[3])
				if lastPos == pos:
					cull.add( (row[0],) )
					if row[4]:
						valid.add( (lastID,) )
				else:
					lastID = row[0]
					lastPos = pos
		#if sql/python method
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
		if 0: #sql method is sometimes very slow, python method seems more consistent
			sql = "SELECT GROUP_CONCAT(_ROWID_) FROM `db`.`snp_entrez_role` GROUP BY rs, entrez_id, role_id HAVING COUNT() > 1"
			for row in dbc.execute(sql):
				rowids = row[0].split(',')
				cull.update( (long(rowids[i]),) for i in xrange(1,len(rowids)) )
		else:
			lastRole = None
			sql = "SELECT _ROWID_, rs, entrez_id, role_id FROM `db`.`snp_entrez_role` ORDER BY rs, entrez_id, role_id"
			for row in dbc.execute(sql):
				role = (row[1],row[2],row[3])
				if lastRole == role:
					cull.add( (row[0],) )
				else:
					lastRole = role
		#if sql/python method
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
		self.logPush("defining omic units ...\n")
		cursor = self._db.cursor()
		
		# load namespace definitions
		nsName = dict()
		nsID = dict()
		for row in cursor.execute("SELECT namespace_id,namespace FROM `db`.`namespace`"):
			nsName[row[0]] = row[1]
			nsID[row[1]] = row[0]
		nsCore = set()
		for ns in self._options['unit-core-ns'].split(','):
			ns = ns.strip().lower()
			if ns:
				if ns in nsID:
					nsCore.add(nsID[ns])
				#else:
				#	raise Exception("unit core namespace '%s' is not defined by the data" % ns)
		if not nsCore:
			self.logPop("... WARNING: no unit core namespaces are defined\n")
			return
		
		# delete old derived records
		self.log("deleting old records ...")
		self.prepareTableForQuery('region')
		self.prepareTableForQuery('region_name')
		self.prepareTableForQuery('name_property')
		self.prepareTableForUpdate('unit')
		self.prepareTableForUpdate('unit_name')
		cursor.execute("DELETE FROM `db`.`unit` WHERE source_id = ?", (self.getSourceID(),))
		cursor.execute("DELETE FROM `db`.`unit_name` WHERE source_id = ?", (self.getSourceID(),))
		self.log(" OK\n")
		
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
		nameNum = None
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
		maxGap = int(self._options['max-unit-gap'])
		requireUnitRegion = (self._options['require-unit-region'] == 'yes')
		unitNames = list()
		numNone = numChr = numGap = 0
		while coreNames:
			names = coreNames.pop()
			regions = list()
			for n in names:
				regions.extend( (r+(n,)) for r in nameRegions[n] )
			if not regions:
				numNone += 1
				if not requireUnitRegion:
					unitNames.append(names)
				continue
			regions.sort()
			names = set()
			uC = uR = None
			for rC,rL,rR,rN in itertools.chain(regions, [(0,0,0,None)]):
				if (uC == None or uC == 23 or uC == 24) and (rC == 23 or rC == 24): #it's ok for a core to have regions on X and Y
					uC,uR = rC,rR
				elif (uC != rC) or ((maxGap >= 0) and (uR + maxGap < rL)):
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
		
		# assign additional names using a kind of multi-source breadth-first-search
		nameDist = {n:0 for n in nameUnits}
		maxDist = int(self._options['max-unit-alias-dist'])
		if maxDist != 0:
			self.log("assigning aliases to units ...")
			maxSharedDist = int(self._options['max-shared-alias-dist'])
			queue = collections.deque(nameUnits)
			while queue:
				n1 = queue.pop()
				dist = nameDist[n1] + 1
				if dist > 0: # skip shared aliases if not allowSharedAliases
					units = nameUnits[n1]
					for n2 in graph[n1]:
						if n2 not in nameDist:
							for u in units:
								unitNames[u].add(n2)
							nameUnits[n2] |= units
							nameDist[n2] = dist
							if (maxDist < 0) or (dist < maxDist):
								queue.appendleft(n2)
						elif nameDist[n2] == dist:
							# already found during this distance-phase
							if nameUnits[n2] == units:
								# found by the same unit(s) we're searching from now
								pass
							elif (maxSharedDist < 0) or (dist <= maxSharedDist):
								# allowed to be shared
								for u in units:
									unitNames[u].add(n2)
								nameUnits[n2] |= units
							else:
								if (nameUnits[n2] - units) and not (units - nameUnits[n2]): #TODO
									print "%s:%s in %s -> %s:%s in %s" % (nsName[nameNamespaceID[n1]],nameName[n1],str(units),nsName[nameNamespaceID[n2]],nameName[n2],str(nameUnits[n2]))
								# cannot be shared; delete the alias
								for u in nameUnits[n2]:
									unitNames[u].remove(n2)
								del nameUnits[n2]
								nameDist[n2] = -1
						elif nameDist[n2] > dist:
							raise Exception("BFS logic error")
					#for n2 in graph[n1]
				#if dist > 0
			#while queue
			self.log(" OK: %d identifiers, %d assignments\n" % (len(nameUnits),sum(len(units) for units in nameUnits.itervalues())))
		#if maxDist
		graph = None
		
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
				if d >= 0:
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
		for row in cursor.executemany("INSERT INTO `db`.`unit` (utype_id,label,description,source_id) VALUES (?,?,?,%d); SELECT last_insert_rowid()" % (self.getSourceID(),), unitProps):
			unitIDs.append(row[0])
		unitProps = None
		self.log(" OK: %d units\n" % (len(unitIDs),))
		
		# store unit names
		self.log("storing unit aliases ...")
		for u,names in enumerate(unitNames):
			cursor.executemany("INSERT OR IGNORE INTO `db`.`unit_name` (unit_id,namespace_id,name,source_id) VALUES (?,?,?,%d)" % (self.getSourceID(),), ((unitIDs[u],nameNamespaceID[n],nameName[n]) for n in names))
		nameNamespaceID = nameName = nameUnits = unitNames = unitIDs = None
		self.log(" OK\n")
		
		self.logPop("... OK\n")
	#defineOmicUnits()
	
	
	def getNameUIDs(self):
		if not self._nameUIDs:
			cursor = self._db.cursor()
			self._nameUIDs = collections.defaultdict(set)
			for row in cursor.execute("SELECT namespace_id,name,unit_id FROM `db`.`unit_name`"):
				self._nameUIDs[(row[0],row[1])].add(row[2])
		return self._nameUIDs
	#getNameUIDs()
	
	
	def resolveSNPUnitRoles(self):
		self.log("resolving SNP roles ...")
		nameUIDs = self.getNameUIDs()
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
	#resolveSNPUnitRoles()
	
	
	def resolveUnitRegions(self):
		self.log("assigning unit regions ...")
		nameUIDs = self.getNameUIDs()
		self.prepareTableForQuery('region_name')
		self.prepareTableForQuery('unit_name')
		self.prepareTableForUpdate('unit_region')
		cursor = self._db.cursor()
		cursor.execute("DELETE FROM `db`.`unit_region` WHERE source_id = ?", (self.getSourceID(),))
		
		# map regions to units #TODO: ambiguity?
		unitRegions = list()
		regionID = None
		numSingle = numAmbig = numUnrec = 0
		names = emptyset = set()
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
		cursor.executemany("INSERT OR IGNORE INTO `db`.`unit_region` (unit_id,region_id,urtype_id,source_id) VALUES (?,?,0,%d)" % (self.getSourceID(),), unitRegions)
		self.log(" OK: %d assignments (%d definite, %d ambiguous, %d unrecognized)\n" % (len(unitRegions),numSingle,numAmbig,numUnrec))
	#resolveUnitRegions()
	
	
	def resolveGroupMembers(self): #TODO: in python with nameUIDs instead of sql?
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
		dbc.execute("DELETE FROM `db`.`group_unit` WHERE source_id = ?", (self.getSourceID(),))
		dbc.execute("""
/* group-unit assignments with confidence scores */
INSERT INTO `db`.`group_unit` (group_id, unit_id, specificity, implication, quality, source_id)
SELECT
  group_id,
  unit_id,
  MAX(specificity) AS specificity,
  MAX(implication) AS implication,
  MAX(quality) AS quality,
  ? AS source_id
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
""", (self.getSourceID(),))
		
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
  ? AS source_id
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
""", (self.getSourceID(),))
		
		# clean up
		dbc.execute("DROP TABLE `temp`.`_group_member_name_score`")
		numTotal = numSourced = numMatch = numAmbig = numUnrec = 0
		self.prepareTableForQuery('group_unit')
		for row in dbc.execute("""
SELECT
  COALESCE(SUM(CASE WHEN unit_id > 0 THEN 1 ELSE 0 END),0) AS total,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id != ?1 THEN 1 ELSE 0 END),0) AS sourced,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id = ?1 AND specificity >= 100 AND implication >= 100 AND quality >= 100 THEN 1 ELSE 0 END),0) AS definite,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id = ?1 AND (specificity < 100 OR implication < 100 OR quality < 100) THEN 1 ELSE 0 END),0) AS conditional,
  COALESCE(SUM(CASE WHEN unit_id = 0 AND source_id = ?1 THEN specificity ELSE 0 END),0) AS unmatched
FROM `db`.`group_unit`
""", (self.getSourceID(),)):
			numTotal = row[0]
			numSourced = row[1]
			numMatch = row[2]
			numAmbig = row[3]
			numUnrec = row[4]
		self.log(" OK: %d associations (%d explicit, %d definite, %d conditional, %d unrecognized)\n" % (numTotal,numSourced,numMatch,numAmbig,numUnrec))
	#resolveGroupMembers()
	
#Source_loki
