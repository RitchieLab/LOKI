#!/usr/bin/env python

import collections
import itertools
import re
import sys
import time #DEBUG
import traceback
from loki import loki_source


class Source_loki(loki_source.Source):
	
	
	##################################################
	# source interface
	
	
	@classmethod
	def getVersionString(cls):
		return '3.0 (2017-01-13)'
	#getVersionString()
	
	
	@classmethod
	def getOptions(cls):
		return {
			'unit-core-ns'          : 'ns1[,ns2[...]]  --  comma-separated list of namespaces to define unit cores (default: entrez_gid,ensembl_gid)',
			'require-dual-xrefs'    : '[yes|no]  --  require unit core identifier references to be bi-directional (default: no)',
			'require-core-region'   : '[yes|no]  --  require unit core identifiers to have at least one region (default: no)',
			'require-unit-region'   : '[yes|no]  --  require all units to have at least one region (default: no)',
			'max-unit-gap'          : 'number  --  maximum basepair gap between regions to allow in one unit, or -1 for no limit (default: 25000)',
			'max-unit-alias-dist'   : 'number  --  maximum identifier graph distance from an alias to a unit core, or -1 for no limit (default: -1)',
			'max-shared-alias-dist' : 'number  --  maximum identifier graph distance to allow an alias to be shared, or -1 for no limit (default: -1)',
		}
	#getOptions()
	
	
	def validateOptions(self, options):
		options.setdefault('unit-core-ns', 'ensembl_gid,entrez_gid')
		options.setdefault('require-dual-xrefs', 'no')
		options.setdefault('require-core-region', 'no')
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
			elif o in ('require-dual-xrefs','require-core-region','require-unit-region'):
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
		# download the latest source files
		# http://genome.ucsc.edu/FAQ/FAQreleases.html
		# http://genome.ucsc.edu/goldenPath/releaseLog.html
		# TODO: find a better machine-readable source for this data
		self.downloadFilesFromHTTP('genome.ucsc.edu', {
			'FAQreleases.html': '/FAQ/FAQreleases.html',
		})
	#download()
	
	
	def update(self, options, prevOptions, tablesUpdated, forceUpdate):
		cursor = self._db.cursor()
		logIndent = self.logIndent()
		
		# process the latest GRCh/UCSChg conversions
		with open('FAQreleases.html','rU') as datafile:
			self.log("updating GRCh:UCSChg genome build identities ...")
			page = datafile.read()
			rowHuman = False
			for tablerow in re.finditer(r'<tr>.*?</tr>', page, re.IGNORECASE | re.DOTALL):
				cols = tuple(match.group()[4:-5].strip().lower() for match in re.finditer(r'<td>.*?</td>', tablerow.group(), re.IGNORECASE | re.DOTALL))
				if cols and ((cols[0] == 'human') or (rowHuman and (cols[0] in ('','&nbsp;')))):
					rowHuman = True
					grch = ucschg = None
					try:
						if cols[1].startswith('hg'):
							ucschg = int(cols[1][2:])
						if cols[3].startswith('genome reference consortium grch'):
							grch = int(cols[3][32:])
						if cols[3].startswith('ncbi build '):
							grch = int(cols[3][11:])
					except:
						pass
					if grch and ucschg:
						cursor.execute("INSERT OR REPLACE INTO `db`.`grch_ucschg` (grch,ucschg) VALUES (?,?)", (grch,ucschg))
				else:
					rowHuman = False
			#foreach tablerow
			self.log(" OK\n")
		#with datafile
		
		# cross-map GRCh/UCSChg build versions for all sources
		ucscGRC = collections.defaultdict(int)
		for row in self._db.cursor().execute("SELECT grch,ucschg FROM `db`.`grch_ucschg`"):
			ucscGRC[row[1]] = max(row[0], ucscGRC[row[1]])
			cursor.execute("UPDATE `db`.`source` SET grch = ? WHERE grch IS NULL AND ucschg = ?", (row[0],row[1]))
			cursor.execute("UPDATE `db`.`source` SET ucschg = ? WHERE ucschg IS NULL AND grch = ?", (row[1],row[0]))
		cursor.execute("UPDATE `db`.`source` SET current_ucschg = ucschg WHERE current_ucschg IS NULL")
		
		# check for any source with an unrecognized GRCh build
		mismatch = False
		for row in cursor.execute("SELECT source, grch, ucschg FROM `db`.`source` WHERE (grch IS NULL) != (ucschg IS NULL)"):
			self.log("WARNING: unrecognized genome build for '%s' (NCBI GRCh%s, UCSC hg%s)\n" % (row[0],(row[1] or "?"),(row[2] or "?")))
			mismatch = True
		if mismatch:
			self.log("WARNING: database may contain incomparable genome positions!\n")
		
		# check all sources' UCSChg build versions and set the latest as the target
		hgSources = collections.defaultdict(set)
		for row in cursor.execute("SELECT source_id, current_ucschg FROM `db`.`source` WHERE current_ucschg IS NOT NULL"):
			hgSources[row[1]].add(row[0])
		if hgSources:
			targetHG = max(hgSources)
			self.log("database genome build: GRCh%s / UCSChg%s\n" % (ucscGRC.get(targetHG,'?'), targetHG))
			targetUpdated = (self._loki.getDatabaseSetting('ucschg',int) != targetHG)
			self._loki.setDatabaseSetting('ucschg', targetHG)
		
		# decide which post-processing steps are required
		ppCallOrder = [
			'normalizeGenomeBuilds',
			'cleanupSNPMerges',
			'updateMergedSNPLoci',
			'cleanupSNPLoci',
			'updateMergedSNPEntrezRoles',
			'cleanupSNPEntrezRoles',
			'updateMergedGWASAnnotations',
		#	'cleanupGWASAnnotations',
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
		self._loki.setDatabaseSetting('postProcess', ','.join(curPP))
		#if vers/pp mismatch
		
		# call all necessary post-processing methods
		self._options = options
		self._nameUnits = None
		donePP = set()
		for pp in ppCallOrder:
			# re-scan step triggers before each step, to catch steps
			# which update tables that trigger later steps
			if hgSources and (min(hgSources) != targetHG):
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
				t0 = time.time() #DEBUG
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
				self.log("(%ds)\n" % (time.time()-t0)) #DEBUG
			#if pp in curPP
		#foreach ppCallOrder
		
		# clean up
		self._loki.setDatabaseSetting('postProcess', ','.join(curPP - donePP))
		self._options = None
		self._nameUnits = None
	#update()
	
	
	##################################################
	# postprocessing
	
	
	def doPostProcessStep(self, step):
		t0 = time.time() #DEBUG
		
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
		
		self.log("(%ds)\n" % (time.time()-t0)) #DEBUG
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
		cull = list()
		if 0: #TODO
			sql = "SELECT GROUP_CONCAT(_ROWID_) FROM `db`.`snp_merge` GROUP BY rsMerged HAVING COUNT() > 1"
			for row in dbc.execute(sql):
				rowids = row[0].split(',')
				cull.extend( (long(rowids[i]),) for i in xrange(1,len(rowids)) )
		else:
			lastRS = None
			sql = "SELECT _ROWID_, rsMerged FROM `db`.`snp_merge` ORDER BY rsMerged, rsCurrent"
			for row in dbc.execute(sql):
				if lastRS == row[1]:
					cull.append( (row[0],) )
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
		
		if 0: #TODO
			sql = """
INSERT INTO `db`.`snp_locus` (rs, chr, pos, validated, source_id)
SELECT sm.rsCurrent, sl.chr, sl.pos, sl.validated, sl.source_id
FROM `db`.`snp_locus` AS sl
JOIN `db`.`snp_merge` AS sm
  ON sm.rsMerged = sl.rs
"""
			dbc.execute(sql)
			numCopied = self._db.changes()
			if numCopied:
				self.flagTableUpdate('snp_locus')
			self.log(" OK: %d loci copied\n" % (numCopied,))
		else:
			sql = """
SELECT sm.rsCurrent, sl.chr, sl.pos, sl.validated, sl.source_id
FROM `db`.`snp_locus` AS sl
JOIN `db`.`snp_merge` AS sm
  ON sm.rsMerged = sl.rs
"""
			insert = list(dbc.execute(sql))
			if insert:
				self.flagTableUpdate('snp_locus')
				dbc.executemany("INSERT INTO `db`.`snp_locus` (rs, chr, pos, validated, source_id)", insert)
			self.log(" OK: %d loci copied\n" % (len(insert),))
		#if sql/python method
	#updateMergedSNPLoci()
	
	
	def cleanupSNPLoci(self):
		self.log("verifying SNP loci ...")
		self.prepareTableForQuery('snp_locus')
		dbc = self._db.cursor()
		
		# for each set of ROWIDs which constitute a duplicated snp-locus, cull all but one;
		# but, make sure that if any of the originals were validated, the remaining one is also
		valid = list()
		cull = list()
		if 0: #TODO
			sql = "SELECT GROUP_CONCAT(_ROWID_), MAX(validated) FROM `db`.`snp_locus` GROUP BY rs, chr, pos HAVING COUNT() > 1"
			for row in dbc.execute(sql):
				rowids = row[0].split(',')
				if row[1]:
					valid.append( (long(rowids[0]),) )
				cull.extend( (long(rowids[i]),) for i in xrange(1,len(rowids)) )
		else:
			lastID = None
			lastPos = None
			lastValid = False
			sql = "SELECT _ROWID_, rs, chr, pos, validated FROM `db`.`snp_locus` ORDER BY rs, chr, pos"
			for row in dbc.execute(sql):
				pos = (row[1],row[2],row[3])
				if lastPos == pos:
					if row[4] and not lastValid:
						valid.append( (lastID,) )
					cull.append( (row[0],) )
				else:
					lastID = row[0]
					lastPos = pos
					lastValid = row[4]
		#if sql/python method
		if cull:
			self.flagTableUpdate('snp_locus')
			dbc.executemany("DELETE FROM `db`.`snp_locus` WHERE _ROWID_ = ?", cull)
		if valid:
			self.flagTableUpdate('snp_locus')
			dbc.executemany("UPDATE `db`.`snp_locus` SET validated = 1 WHERE _ROWID_ = ?", valid)
		
		self.log(" OK: %d duplicate loci\n" % (len(cull),))
	#cleanupSNPLoci()
	
	
	def updateMergedSNPEntrezRoles(self):
		self.log("checking for merged SNP roles ...")
		self.prepareTableForQuery('snp_entrez_role')
		self.prepareTableForQuery('snp_merge')
		dbc = self._db.cursor()
		
		if 0: #TODO
			sql = """
INSERT OR IGNORE INTO `db`.`snp_entrez_role` (rs, entrez_id, role_id, source_id)
SELECT sm.rsCurrent, ser.entrez_id, ser.role_id, ser.source_id
FROM `db`.`snp_entrez_role` AS ser
JOIN `db`.`snp_merge` AS sm
  ON sm.rsMerged = ser.rs
"""
			dbc.execute(sql)
			numCopied = self._db.changes()
			if numCopied:
				self.flagTableUpdate('snp_entrez_role')
			self.log(" OK: %d roles copied\n" % (numCopied,))
		else:
			sql = """
SELECT sm.rsCurrent, ser.entrez_id, ser.role_id, ser.source_id
FROM `db`.`snp_entrez_role` AS ser
JOIN `db`.`snp_merge` AS sm
  ON sm.rsMerged = ser.rs
"""
			insert = list(dbc.execute(sql))
			if insert:
				self.flagTableUpdate('snp_entrez_role')
				dbc.executemany("INSERT INTO `db`.`snp_entrez_role` (rs, entrez_id, role_id, source_id)", insert)
			self.log(" OK: %d roles copied\n" % (len(insert),))
		#if sql/python method
	#updateMergedSNPEntrezRoles()
	
	
	def cleanupSNPEntrezRoles(self):
		self.log("verifying SNP roles ...")
		self.prepareTableForQuery('snp_entrez_role')
		dbc = self._db.cursor()
		
		cull = list()
		if 0: #TODO
			sql = "SELECT GROUP_CONCAT(_ROWID_) FROM `db`.`snp_entrez_role` GROUP BY rs, entrez_id, role_id HAVING COUNT() > 1"
			for row in dbc.execute(sql):
				rowids = row[0].split(',')
				cull.extend( (long(rowids[i]),) for i in xrange(1,len(rowids)) )
		else:
			lastRole = None
			sql = "SELECT _ROWID_, rs, entrez_id, role_id FROM `db`.`snp_entrez_role` ORDER BY rs, entrez_id, role_id"
			for row in dbc.execute(sql):
				role = (row[1],row[2],row[3])
				if lastRole == role:
					cull.append( (row[0],) )
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
		
		if 0: #TODO
			sql = """
INSERT INTO `db`.`gwas` (rs, chr, pos, trait, snps, orbeta, allele95ci, riskAfreq, pubmed_id, source_id)
SELECT sm.rsCurrent, w.chr, w.pos, w.trait, w.snps, w.orbeta, w.allele95ci, w.riskAfreq, w.pubmed_id, w.source_id
FROM `db`.`gwas` AS w
JOIN `db`.`snp_merge` AS sm
  ON sm.rsMerged = w.rs
"""
			dbc.execute(sql)
			numCopied = self._db.changes()
			if numCopied:
				self.flagTableUpdate('gwas')
			self.log(" OK: %d annotations copied\n" % (numCopied,))
		else:
			sql = """
SELECT sm.rsCurrent, w.chr, w.pos, w.trait, w.snps, w.orbeta, w.allele95ci, w.riskAfreq, w.pubmed_id, w.source_id
FROM `db`.`gwas` AS w
JOIN `db`.`snp_merge` AS sm
  ON sm.rsMerged = w.rs
"""
			insert = list(dbc.execute(sql))
			if insert:
				self.flagTableUpdate('gwas')
				sql = """
INSERT INTO `db`.`gwas`
(rs, chr, pos, trait, snps, orbeta, allele95ci, riskAfreq, pubmed_id, source_id)
VALUES (?,?,?,?,?,?,?,?,?,?)
"""
				dbc.executemany(sql, insert)
			self.log(" OK: %d annotations copied\n" % (len(insert),))
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
			for region_id,rtype_id,chm,posMin,posMax in regions:
				for z in xrange(int(posMin/size),int(posMax/size)+1):
					yield (region_id,rtype_id,chm,z)
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
		emptylist = list()
		emptydict = dict()
		
		# load namespace definitions
		nsLabel = dict()
		nsID = dict()
		nsPoly = set()
		for row in cursor.execute("SELECT namespace_id,namespace,polygenic FROM `db`.`namespace`"):
			nsLabel[row[0]] = row[1]
			nsID[row[1]] = row[0]
			if row[2]:
				nsPoly.add(row[0])
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
		nameNum = dict()
		nameNamespaceID = list()
		nameName = list()
		graph = list()
		numEdges = 0
		for row in cursor.execute("SELECT namespace_id1,name1,namespace_id2,name2 FROM `db`.`name_name` WHERE LENGTH(TRIM(name1)) AND LENGTH(TRIM(name2))"):
			name1 = (row[0],row[1])
			n1 = nameNum.get(name1)
			if n1 == None:
				n1 = nameNum[name1] = len(nameNamespaceID)
				nameNamespaceID.append(row[0])
				nameName.append(row[1])
				graph.append(dict())
			name2 = (row[2],row[3])
			n2 = nameNum.get(name2)
			if n2 == None:
				n2 = nameNum[name2] = len(nameNamespaceID)
				nameNamespaceID.append(row[2])
				nameName.append(row[3])
				graph.append(dict())
			if n1 != n2:
				numEdges += 1
				graph[n1][n2] = True
				graph[n2][n1] = graph[n2].get(n1,False)
		#for row in cursor
		self.log(" OK: %d identifiers, %d links\n" % (len(nameName),numEdges))
		
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
				graph.append(emptydict)
			nameRegions[n].add( (row[2],row[3],row[4]) )
		#for row in cursor
		self.log(" OK: %d identifiers\n" % (len(nameRegions),))
		
		# load name properties
		self.log("loading identifier properties ...")
		namePropValues = collections.defaultdict(lambda: collections.defaultdict(list))
		for row in cursor.execute("SELECT namespace_id,name,property,value FROM `db`.`name_property`"):
			name = (row[0],row[1])
			n = nameNum.get(name)
			if n == None:
				n = nameNum[name] = len(nameNamespaceID)
				nameNamespaceID.append(row[0])
				nameName.append(row[1])
				graph.append(emptydict)
			namePropValues[n][row[2]].append(row[3])
		self.log(" OK: %d identifiers\n" % (len(namePropValues),))
		nameNum = None
		
		# find core sets of names that could become a unit
		self.log("searching for candidate units ...")
		requireCoreRegion = (self._options['require-core-region'] == 'yes')
		coreNames = list()
		nameFlag = set()
		stack = list()
		for n0 in xrange(len(nameName)):
			if (nameNamespaceID[n0] in nsCore) and (n0 not in nameFlag) and ((not requireCoreRegion) or (n0 in nameRegions)):
				names = {n0}
				stack.append(n0)
				while stack:
					n1 = stack.pop()
					for n2,fwd in graph[n1].iteritems():
						if (nameNamespaceID[n2] in nsCore) and (n2 not in names) and ((self._options['require-dual-xrefs'] != 'yes') or (fwd and graph[n2].get(n1)) or (nameNamespaceID[n1] == nameNamespaceID[n2])) and ((not requireCoreRegion) or (n2 in nameRegions)):
							names.add(n2)
							stack.append(n2)
				nameFlag |= names
				coreNames.append(tuple(names))
		#for n0 in graph
		self.log(" OK: %d candidates, %d core identifiers\n" % (len(coreNames),len(nameFlag)))
		nameFlag = None
		
		# split cores according to region gap rules
		self.log("analyzing candidate unit regions ...")
		maxGap = int(self._options['max-unit-gap'])
		requireUnitRegion = (self._options['require-unit-region'] == 'yes')
		nameUnits = [None]*len(nameName)
		nextUnit = 0
		numNone = numChr = numGap = 0
		while coreNames:
			names = coreNames.pop()
			regions = list()
			for n in names:
				regions.extend( (r+(n,)) for r in nameRegions[n] )
			if not regions:
				numNone += 1
				if not requireUnitRegion:
					for n in names:
						if not nameUnits[n]:
							nameUnits[n] = list()
						nameUnits[n].append(nextUnit)
					nextUnit += 1
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
						for n in names:
							if not nameUnits[n]:
								nameUnits[n] = list()
							nameUnits[n].append(nextUnit)
						nextUnit += 1
						names = set()
					uC,uR = rC,rR
				else:
					uR = max(uR,rR)
				names.add(rN)
			#for r in regions
		#while coreNames
		self.log(" OK: %d omic-units (%d no-region, %d chr-splits, %d gap-splits)\n" % (nextUnit,numNone,numChr,numGap))
		nameRegions = coreNames = None
		
		# assign additional names using breadth-first-search with multiple starting seeds
		nameDist = [ (0 if nameUnits[n] else -1) for n in xrange(len(nameName)) ]
		maxDist = int(self._options['max-unit-alias-dist'])
		if maxDist != 0:
			self.log("assigning normal aliases to units ...")
			queue = collections.deque( n for n in xrange(len(nameName)) if nameUnits[n] )
			while queue:
				n1 = queue.pop()
				u1 = nameUnits[n1] or emptylist
				d2 = nameDist[n1] + 1
				for n2 in graph[n1].iterkeys():
					if (0 <= nameDist[n2] < d2) or (nameNamespaceID[n2] in nsPoly):
						pass
					elif nameDist[n2] > d2:
						raise Exception("BFS logic error")
					else:
						if (nameDist[n2] < 0) and ((maxDist < 0) or (d2 < maxDist)):
							queue.appendleft(n2)
						nameDist[n2] = d2
						if not nameUnits[n2]:
							nameUnits[n2] = list()
						nameUnits[n2].extend(u1)
				#foreach n2 in graph[n1]
			#while queue
			for n1 in xrange(len(nameName)):
				if nameUnits[n1]:
					nameUnits[n1] = tuple(set(nameUnits[n1]))
			self.log(" OK: %d identifiers, %d assignments\n" % (sum(1 for units in nameUnits if units),sum(len(units) for units in nameUnits if units)))
			
			# remove shared aliases beyond the distance limit, if any
			maxSharedDist = int(self._options['max-shared-alias-dist'])
			if maxSharedDist >= 0:
				self.log("deleting shared aliases ...")
				delNames = delAssignments = 0
				for n in xrange(len(nameDist)):
					d = nameDist[n]
					if (d >= 0) and (d > maxSharedDist) and (len(nameUnits[n] or emptylist) > 1):
						delNames += 1
						delAssignments += len(nameUnits[n])
						nameUnits[n] = None
						nameDist[n] = -1
				self.log(" OK: deleted %d identifiers, %d assignments\n" % (delNames,delAssignments))
			#if maxSharedDist
			
			self.log("assigning polygenic aliases to units ...")
			numIdent = numAssign = 0
			for n1 in xrange(len(nameDist)):
				if (nameDist[n1] < 0) and (nameNamespaceID[n1] in nsPoly):
					nameDist[n1] = 0
					queue = [n1]
					units = set()
					dist = 0
					i = 0
					while i < len(queue):
						for n2 in graph[queue[i]].iterkeys():
							if (nameNamespaceID[n2] in nsPoly):
								if (nameDist[n2] < 0):
									nameDist[n2] = 0
									queue.append(n2)
							else:
								units.update(nameUnits[n2] or emptylist)
								dist = max(dist,nameDist[n2])
						i += 1
					#while queue
					units = tuple(units)
					numIdent += len(queue)
					numAssign += len(queue) * len(units)
					for n2 in queue:
						nameDist[n2] = dist
						assert(nameUnits[n2] == None)
						nameUnits[n2] = units
				#if unassigned and polygenic
			#foreach name
			self.log(" OK: %d identifiers, %d assignments\n" % (numIdent,numAssign))
		#if maxDist
		graph = None
		
		# convert nameUnits to unitNames
		unitNames = list(list() for u in xrange(nextUnit))
		for n,units in enumerate(nameUnits):
			for u in (units or emptylist):
				unitNames[u].append(n)
		for u in xrange(nextUnit):
			unitNames[u] = tuple(unitNames[u])
		
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
		self.log(" OK\n")
		
		# convert unitNames back to nameUnits for re-use in later steps
		nameUnits = collections.defaultdict(set)
		for u,names in enumerate(unitNames):
			for n in (names or emptylist):
				nameUnits[ (nameNamespaceID[n],nameName[n]) ].add(unitIDs[u])
		self._nameUnits = nameUnits
		nameNamespaceID = nameName = unitNames = unitIDs = None
		
		self.logPop("... OK\n")
	#defineOmicUnits()
	
	
	def getNameUnits(self):
		nameUnits = self._nameUnits
		if not nameUnits:
			nameUnits = collections.defaultdict(set)
			for namespaceID,name,unitID in self._db.cursor().execute("SELECT namespace_id,name,unit_id FROM `db`.`unit_name`"):
				nameUnits[ (namespaceID,name) ].add(unitID)
			self._nameUnits = nameUnits
		return nameUnits
	#getNameUnits()
	
	
	def resolveSNPUnitRoles(self):
		self.log("resolving SNP roles ...")
		nameUnits = self.getNameUnits()
		cursor = self._db.cursor()
		
		# translate entrez_ids to unit_ids
		self.prepareTableForUpdate('snp_unit_role')
		cursor.execute("DELETE FROM `db`.`snp_unit_role`")
		namespaceID = self._loki.getNamespaceID('entrez_gid')
		if namespaceID:
			self.prepareTableForQuery('snp_entrez_role')
			self.prepareTableForQuery('snp_merge')
			def generate_rows():
				sql = """
SELECT COALESCE(sm.rsCurrent, ser.rs), ''||ser.entrez_id, ser.role_id, ser.source_id
FROM `db`.`snp_entrez_role` AS ser
LEFT JOIN `db`.`snp_merge` AS sm
  ON sm.rsMerged = ser.rs
"""
				for rsID,entrezID,roleID,sourceID in self._db.cursor().execute(sql):
					for unitID in nameUnits[(namespaceID,entrezID)]:
						yield (rsID,unitID,roleID,sourceID)
			cursor.executemany("INSERT INTO `db`.`snp_unit_role` (rs, unit_id, role_id, source_id) VALUES (?,?,?,?)", generate_rows())
		
		# cull duplicate roles
		self.prepareTableForQuery('snp_unit_role')
		cull = list()
		if 0: #TODO
			sql = "SELECT GROUP_CONCAT(_ROWID_) FROM `db`.`snp_unit_role` GROUP BY rs, unit_id, role_id HAVING COUNT() > 1"
			for row in cursor.execute(sql):
				rowids = row[0].split(',')
				cull.extend( (long(rowids[i]),) for i in xrange(1,len(rowids)) )
		else:
			lastRole = None
			sql = "SELECT _ROWID_, rs, unit_id, role_id FROM `db`.`snp_unit_role` ORDER BY rs, unit_id, role_id"
			for row in cursor.execute(sql):
				role = (row[1],row[2],row[3])
				if lastRole == role:
					cull.append( (row[0],) )
				else:
					lastRole = role
		#if sql/python method
		if cull:
			self.flagTableUpdate('snp_unit_role')
			cursor.executemany("DELETE FROM `db`.`snp_unit_role` WHERE _ROWID_ = ?", cull)
		
		numTotal = numSNPs = numUnits = 0
		for row in cursor.execute("SELECT COUNT(), COUNT(DISTINCT rs), COUNT(DISTINCT unit_id) FROM `db`.`snp_unit_role`"):
			numTotal,numSNPs,numUnits = row
		self.log(" OK: %d roles (%d SNPs, %d units)\n" % (numTotal,numSNPs,numUnits))
	#resolveSNPUnitRoles()
	
	
	def resolveUnitRegions(self):
		self.log("assigning unit regions ...")
		nameUnits = self.getNameUnits()
		self.prepareTableForQuery('region_name')
		self.prepareTableForQuery('unit_name')
		self.prepareTableForUpdate('unit_region')
		cursor = self._db.cursor()
		cursor.execute("DELETE FROM `db`.`unit_region` WHERE source_id = ?", (self.getSourceID(),))
		
		# map regions to units #TODO: ambiguity?
		unitRegions = list()
		curR = None
		numSingle = numAmbig = numUnrec = 0
		units = set()
		emptyset = set()
		for regionID,namespaceID,name in itertools.chain(cursor.execute("SELECT region_id,namespace_id,name FROM `db`.`region_name` ORDER BY region_id"), [(None,None,None)]):
			if curR != regionID:
				if curR != None:
					if len(units) < 1:
						numUnrec += 1
					elif len(units) > 1:
						numAmbig += 1
					else:
						numSingle += 1
					unitRegions.extend( (u,curR) for u in units )
				#if curR
				curR = regionID
				units = set()
			#if new region
			units.update(nameUnits.get( (namespaceID,name), emptyset ))
		cursor.executemany("INSERT OR IGNORE INTO `db`.`unit_region` (unit_id,region_id,urtype_id,source_id) VALUES (?,?,0,%d)" % (self.getSourceID(),), unitRegions)
		self.log(" OK: %d assignments (%d definite, %d ambiguous, %d unrecognized)\n" % (len(unitRegions),numSingle,numAmbig,numUnrec))
	#resolveUnitRegions()
	
	
	def resolveGroupMembers_sqlite(self):
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
  gmnQuery.group_id,
  gmnQuery.member,
  un.unit_id,
  gmnQuery.polynames,
  COUNT(DISTINCT gmnQuery.gmn_rowid) AS implication,
  (CASE WHEN gmnQuery.polynames > 0 THEN COUNT(DISTINCT gmnQuery.gmn_rowid) ELSE SUM(1000000 / gmnQuery.match_count) END) AS quality
FROM (
  /* count the number of possible matches for each name of each member */
  SELECT
    gmn._ROWID_ AS gmn_rowid,
    gmn.group_id,
    gmn.member,
    gmn.namespace_id,
    gmn.name,
    gmQuery.polynames,
    COUNT(DISTINCT un.unit_id) AS match_count
  FROM (
    /* count the number of matchable polygenic names for each member */
    SELECT
      gmn.group_id,
      gmn.member,
      COUNT(DISTINCT (CASE WHEN n.polygenic > 0 THEN gmn._ROWID_ ELSE NULL END)) AS polynames
    FROM `db`.`group_member_name` AS gmn
    JOIN `db`.`unit_name` AS un
      ON un.name = gmn.name
      AND un.namespace_id = gmn.namespace_id
    JOIN `db`.`namespace` AS n
      ON n.namespace_id = un.namespace_id
      AND n.namespace_id = gmn.namespace_id
    GROUP BY gmn.group_id, gmn.member
  ) AS gmQuery
  JOIN `db`.`group_member_name` AS gmn
    ON gmn.group_id = gmQuery.group_id
    AND gmn.member = gmQuery.member
  JOIN `db`.`unit_name` AS un
    ON un.name = gmn.name
    AND (un.namespace_id = gmn.namespace_id OR gmn.namespace_id = 0)
  JOIN `db`.`namespace` AS n
    ON n.namespace_id = un.namespace_id
    AND (n.namespace_id = gmn.namespace_id OR gmn.namespace_id = 0)
    AND (n.polygenic > 0) = (gmQuery.polynames > 0)
  GROUP BY gmn.group_id, gmn.member, gmn.namespace_id, gmn.name
) AS gmnQuery
JOIN `db`.`unit_name` AS un
  ON un.name = gmnQuery.name
  AND (un.namespace_id = gmnQuery.namespace_id OR gmnQuery.namespace_id = 0)
GROUP BY gmnQuery.group_id, gmnQuery.member, un.unit_id
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
    gmns.group_id,
    gmns.member,
    gmns.unit_id,
    (CASE
      WHEN gmCountQ.polynames > 0 THEN MIN(
        100 * gmCountQ.best_implication / gmCountQ.polynames,
        100 * gmCountQ.count_implication / gmCountQ.count_basic
      )
      ELSE 100 / gmCountQ.count_basic
    END) AS specificity,
    (CASE
      WHEN gmns.implication = gmCountQ.best_implication THEN 100 / gmCountQ.count_implication
      ELSE 0
    END) AS implication,
    (CASE
      WHEN gmns.quality = gmCountQ.best_quality THEN 100 / gmCountQ.count_quality
      ELSE 0
    END) AS quality
  FROM (
    /* identify number of matches with the best score for each member */
    SELECT
      gmns.group_id,
      gmns.member,
      gmBestQ.polynames,
      gmBestQ.best_implication,
      gmBestQ.best_quality,
      COUNT(DISTINCT gmns.unit_id) AS count_basic,
      SUM(gmns.implication = gmBestQ.best_implication) AS count_implication,
      SUM(gmns.quality = gmBestQ.best_quality) AS count_quality
    FROM (
      /* identify best scores for each member */
      SELECT
        group_id,
        member,
        MAX(polynames) AS polynames,
        MAX(1,MAX(implication)) AS best_implication,
        MAX(1,MAX(quality)) AS best_quality
      FROM `temp`.`_group_member_name_score`
      GROUP BY group_id, member
    ) AS gmBestQ
    JOIN `temp`.`_group_member_name_score` AS gmns
      ON gmns.group_id = gmBestQ.group_id
      AND gmns.member = gmBestQ.member
    GROUP BY gmns.group_id, gmns.member
  ) AS gmCountQ
  JOIN `temp`.`_group_member_name_score` AS gmns
    ON gmns.group_id = gmCountQ.group_id
    AND gmns.member = gmCountQ.member
  GROUP BY gmns.group_id, gmns.member, gmns.unit_id
) AS gmuQ
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
  HAVING COUNT(un.unit_id) = 0
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
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id = ?1 AND specificity >= 100 THEN 1 ELSE 0 END),0) AS definite,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id = ?1 AND specificity < 100 AND (implication >= 100 OR quality >= 100) THEN 1 ELSE 0 END),0) AS probable,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id = ?1 AND specificity < 100 AND implication < 100 AND quality < 100 AND (implication > 0 OR quality > 0) THEN 1 ELSE 0 END),0) AS possible,
  COALESCE(SUM(CASE WHEN unit_id = 0 AND source_id = ?1 THEN specificity ELSE 0 END),0) AS unmatched
FROM `db`.`group_unit`
""", (self.getSourceID(),)):
			numTotal,numSourced,numDef,numProb,numPoss,numUnrec = row
		self.log(" OK: %d associations (%d explicit, %d definite, %d probable, %d possible, %d unrecognized)\n" % (numTotal,numSourced,numDef,numProb,numPoss,numUnrec))
	#resolveGroupMembers_sqlite()
	
	
	def resolveGroupMembers_python(self):
		self.log("resolving group members ...")
		nameUnits = self.getNameUnits()
		self.prepareTableForQuery('group_member_name')
		self.prepareTableForUpdate('group_unit')
		cursor = self._db.cursor()
		cursor.execute("DELETE FROM `db`.`group_unit` WHERE source_id = ?", (self.getSourceID(),))
		
		# load polygenic namespaces
		nsPoly = set()
		for namespaceID in cursor.execute("SELECT namespace_id FROM `db`.`namespace` WHERE polygenic > 0"):
			nsPoly.add(namespaceID)
		
		# map group members to units
		emptyset = set()
		curG = curM = gUnitRows = mUnitScores = mPoly = None
		for groupID,member,namespaceID,name in itertools.chain(self._db.cursor().execute("""
SELECT group_id, member, namespace_id, name
FROM `db`.`group_member_name`
ORDER BY group_id, member
"""), [(None,None,None,None)]):
			if curM != member:
				if mUnitScores:
					bestI = numI = bestQ = numQ = 0
					for i,q in mUnitScores.itervalues():
						if bestI < i:
							bestI = i
							numI = 1
						elif bestI == i:
							numI += 1
						if bestQ < q:
							bestQ = q
							numQ = 1
						elif bestQ == q:
							numQ += 1
					#foreach unit
					if mPoly:
						specificity = min(100 * bestI / mPoly, 100 * numI / len(mUnitScores))
					else:
						specificity = 100 / len(mUnitScores)
					for u,scores in mUnitScores.iteritems():
						i,q = scores
						implication = (100 / numI) if (i == bestI) else 0
						quality = implication if mPoly else ( (100 / numQ) if (q == bestQ) else 0 )
						row = gUnitRows.get(u)
						if row:
							row[2] = max(row[2], specificity)
							row[3] = max(row[3], implication)
							row[4] = max(row[4], quality)
						else:
							gUnitRows[u] = [curG,u,specificity,implication,quality]
					#foreach unit
				elif curM != None:
					row = gUnitRows.get(0)
					if row:
						row[2] += 1
					else:
						gUnitRows[0] = [curG,0,1,0,0]
				#if any units
				mUnitScores = dict()
				mPoly = 0
				curM = member
			#if new member
			
			if curG != groupID:
				if gUnitRows:
					cursor.executemany("""
INSERT INTO `db`.`group_unit`
(group_id, unit_id, specificity, implication, quality, source_id)
VALUES
(?, ?, ?, ?, ?, %d)
""" % (self.getSourceID(),), gUnitRows.itervalues())
				#if curG
				gUnitRows = dict()
				curG = groupID
			#if new group
			
			if namespaceID in nsPoly:
				if not mPoly:
					mUnitScores = dict()
				mPoly += 1
			elif mPoly:
				continue
			
			units = nameUnits.get( (namespaceID,name), emptyset )
			for u in units:
				scores = mUnitScores.get(u)
				if scores:
					scores[0] += 1
					scores[1] += (1.0 / len(units))
				else:
					mUnitScores[u] = [1, 1.0 / len(units)]
			#foreach unit of this name
		#foreach group_member_name
		
		numTotal = numSourced = numMatch = numAmbig = numUnrec = 0
		for row in cursor.execute("""
SELECT
  COALESCE(SUM(CASE WHEN unit_id > 0 THEN 1 ELSE 0 END),0) AS total,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id != ?1 THEN 1 ELSE 0 END),0) AS sourced,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id = ?1 AND specificity >= 100 THEN 1 ELSE 0 END),0) AS definite,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id = ?1 AND specificity < 100 AND (implication >= 100 OR quality >= 100) THEN 1 ELSE 0 END),0) AS probable,
  COALESCE(SUM(CASE WHEN unit_id > 0 AND source_id = ?1 AND specificity < 100 AND implication < 100 AND quality < 100 AND (implication > 0 OR quality > 0) THEN 1 ELSE 0 END),0) AS possible,
  COALESCE(SUM(CASE WHEN unit_id = 0 AND source_id = ?1 THEN specificity ELSE 0 END),0) AS unmatched
FROM `db`.`group_unit`
""", (self.getSourceID(),)):
			numTotal,numSourced,numDef,numProb,numPoss,numUnrec = row
		self.log(" OK: %d associations (%d explicit, %d definite, %d probable, %d possible, %d unrecognized)\n" % (numTotal,numSourced,numDef,numProb,numPoss,numUnrec))
	#resolveGroupMembers_python()
	
	
	def resolveGroupMembers(self):
	#	return self.resolveGroupMembers_sqlite()
		return self.resolveGroupMembers_python()
	#resolveGroupMembers()
	
#Source_loki
