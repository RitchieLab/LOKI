#!/usr/bin/env python

import sys
import os

import loki.source


class Source_genes(loki.source.Source):
	
	# class data
	remHost = 'ftp.ncbi.nih.gov'
	remFiles = {
		'Homo_sapiens.gene_info.gz': '/gene/DATA/GENE_INFO/Mammalia/',
		'gene2refseq.gz':            '/gene/DATA/'
	}
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromFTP(self.remHost, self.remFiles)
	#download()
	
	
	def update(self):
		# initialize storage
		regionChr = {}
		regionDesc = {}
		regionMin = {}
		regionMax = {}
		regionAlias = {
			"entrez_id": {},
			"entrez_name": {},
			"entrez_alias": {}
		}
		
		# process gene_info (no header!)
		if self._verbose:
			sys.stderr.write("processing Homo_sapiens.gene_info.gz ...")
			sys.stderr.flush()
		numNameAmbig = 0
		numAliasAmbig = 0
		numXref = 0
		numXrefSource = 0
		numXrefAmbig = 0
		infoFile = self.zfile('Homo_sapiens.gene_info.gz') #TODO:context manager,iterator
		for line in infoFile:
			# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
			if line.startswith("9606\t"):
				# parse gene data
				words = line.split("\t")
				if words[6] in loki.db.Database.chr_num:
					entrezID = int(words[1])
					regionChr[entrezID] = loki.db.Database.chr_num[words[6]]
					regionDesc[entrezID] = words[8]
					regionAlias["entrez_id"][entrezID] = {entrezID}
					
					# store primary name as an "entrez_name"
					name = words[2]
					if name not in regionAlias["entrez_name"]:
						regionAlias["entrez_name"][name] = {entrezID}
					elif entrezID not in regionAlias["entrez_name"][name]:
						numNameAmbig += 1
						regionAlias["entrez_name"][name].add(entrezID)
					
					# store aliases as "entrez_alias"es
					if words[4] != "-":
						for alias in words[4].split("|"):
							if alias not in regionAlias["entrez_alias"]:
								regionAlias["entrez_alias"][alias] = {entrezID}
							elif entrezID not in regionAlias["entrez_alias"][alias]:
								numAliasAmbig += 1
								regionAlias["entrez_alias"][alias].add(entrezID)
					#if any aliases
					
					# store xrefs according to source
					if words[5] != "-":
						for xref in words[5].split("|"):
							numXref += 1
							source,xref = xref.split(":",1)
							if source not in regionAlias:
								numXrefSource += 1
								regionAlias[source] = {}
							if xref not in regionAlias[source]:
								regionAlias[source][xref] = {entrezID}
							elif entrezID not in regionAlias[source][xref]:
								numXrefAmbig += 1
								regionAlias[source][xref].add(entrezID)
					#if any xrefs
				#if chr is valid
			#if taxonomy is 9606 (human)
		#foreach line in infoFile
		
		# print results
		if self._verbose:
			sys.stderr.write(""" OK:
  %d gene chromosomes and descriptions
  %d names (%d ambiguous)
  %d aliases (%d ambiguous)
  %d xrefs from %d sources (%d ambiguous)
""" % (
				len(regionChr),
				len(regionAlias["entrez_name"]), numNameAmbig,
				len(regionAlias["entrez_alias"]), numAliasAmbig,
				numXref, numXrefSource, numXrefAmbig
			))
		
		# process gene2refseq
		if self._verbose:
			sys.stderr.write("processing gene2refseq ...")
			sys.stderr.flush()
		refFile = self.zfile('gene2refseq.gz') #TODO:context manager,iterator
		header = refFile.next().rstrip()
		if header != "#Format: tax_id GeneID status RNA_nucleotide_accession.version RNA_nucleotide_gi protein_accession.version protein_gi genomic_nucleotide_accession.version genomic_nucleotide_gi start_position_on_the_genomic_accession end_position_on_the_genomic_accession orientation assembly (tab is used as a separator, pound sign - start of a comment)":
			if self._verbose:
				sys.stderr.write(" error!\n")
				sys.stderr.write("unrecognized file header: %s\n" % header1)
			return False
		# process lines
		allPosMax = 0
		chrPosMax = {}
		for chm in loki.db.Database.chr_list:
			chrPosMax[loki.db.Database.chr_num[chm]] = 0
		for line in refFile:
			# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
			if line.startswith("9606\t"):
				words = line.split("\t")
				if words[7].startswith("NC_"): # genomic nucleotide accession.version
					# parse gene data
					entrezID = int(words[1])
					if entrezID in regionChr:
						chm = regionChr[entrezID]
						pos1 = long(words[9])
						pos2 = long(words[10])
						regionMin[entrezID] = min(pos1, pos2)
						regionMax[entrezID] = max(pos1, pos2)
						allPosMax = max(allPosMax, pos1, pos2)
						chrPosMax[chm] = max(chrPosMax[chm], pos1, pos2)
				#if geneAcc is NC_
			#if taxonomy is 9606 (human)
		#foreach line in refFile
		
		# print results
		if self._verbose:
			sys.stderr.write(" OK: %d gene positions\n" % len(regionMin))
		
		# begin transaction to update database
		with self._loki.db:
			# drop indexes to speed inserts
			if self._verbose:
				sys.stderr.write("removing indexes from affected database tables ...")
				sys.stderr.flush()
			self._loki.dropDatabaseIndexes(None, 'db', ('region','region_alias','region_bound','region_zone'))
			if self._verbose:
				sys.stderr.write(" OK\n")
			
			# create any missing alias types, and then fetch their ids
			for aliastype in sorted(regionAlias.keys()):
				try:
					self._loki.dbc.execute("INSERT OR ABORT INTO db.aliastype (aliastype) VALUES (?)", (aliastype,))
				except apsw.ConstraintError:
					pass
			aliasTypeID = {}
			for row in self._loki.dbc.execute("SELECT aliastype_id,aliastype FROM db.aliastype"):
				aliasTypeID[row[1]] = row[0]
			
			# these files should be the authorities for genes,
			# so clear out all old records before writing new ones
			if self._verbose:
				sys.stderr.write("deleting all gene region records from the database ...")
				sys.stderr.flush()
			self._loki.dbc.execute("DELETE FROM db.region_alias WHERE region_id IN (SELECT region_id FROM db.region WHERE regiontype_id = 1)")
			self._loki.dbc.executemany("DELETE FROM db.region_alias WHERE aliastype_id = ?", [(aliasTypeID[aliastype],) for aliastype in aliasTypeID])
			self._loki.dbc.execute("DELETE FROM db.region_bound WHERE region_id IN (SELECT region_id FROM db.region WHERE regiontype_id = 1)")
			self._loki.dbc.execute("DELETE FROM db.region_zone WHERE region_id IN (SELECT region_id FROM db.region WHERE regiontype_id = 1)")
			self._loki.dbc.execute("DELETE FROM db.region WHERE regiontype_id = 1")
			if self._verbose:
				sys.stderr.write(" OK\n")
			
			# store gene regions
			if self._verbose:
				sys.stderr.write("writing gene regions to the database ...")
				sys.stderr.flush()
			geneRegionID = {}
			regionBoundList = []
			for entrezID in regionChr:
				if entrezID in regionMin:
					self._loki.dbc.execute("INSERT INTO db.region (region_id,regiontype_id,chr,description) VALUES (NULL,1,?,?)", (regionChr[entrezID],regionDesc[entrezID]))
					geneRegionID[entrezID] = self._loki.db.last_insert_rowid()
					regionBoundList.append( (geneRegionID[entrezID], 0, regionChr[entrezID], regionMin[entrezID], regionMax[entrezID]) )
			if self._verbose:
				sys.stderr.write(" OK\n")
			
			# store gene region boundaries
			if self._verbose:
				sys.stderr.write("writing gene region boundaries to the database ...")
				sys.stderr.flush()
			self._loki.dbc.executemany("INSERT INTO db.region_bound (region_id,population_id,chr,posMin,posMax) VALUES (?,?,?,?,?)", regionBoundList)
			if self._verbose:
				sys.stderr.write(" OK\n")
			
			# store gene region zones of relevance
			if self._verbose:
				sys.stderr.write("writing gene region zones to the database ...")
				sys.stderr.flush()
			self._loki.dbc.execute("CREATE TEMP TABLE temp.zone (zone INTEGER PRIMARY KEY)")
			self._loki.dbc.executemany("INSERT INTO temp.zone (zone) VALUES (?)", zip(xrange(0, (allPosMax // 100000) + 1))) # +1 for inclusive xrange
			# link regions to covered zones
			self._loki.dbc.execute("""
INSERT OR IGNORE INTO db.region_zone (region_id,population_id,chr,zone)
SELECT
  b.region_id,
  b.population_id,
  b.chr,
  z.zone
FROM db.region_bound AS b
JOIN temp.zone AS z
  ON z.zone >= b.posMin / %d
  AND z.zone <= b.posMax / %d
""" % (100000, 100000))
			self._loki.dbc.execute("DROP TABLE temp.zone")
			if self._verbose:
				sys.stderr.write(" OK\n")
			
			# store gene region aliases
			if self._verbose:
				sys.stderr.write("writing gene region aliases to the database ...")
				sys.stderr.flush()
			regionAliasList = []
			for aliastype in regionAlias:
				for alias in regionAlias[aliastype]:
					for entrezID in regionAlias[aliastype][alias]:
						if entrezID in geneRegionID:
							regionAliasList.append( (geneRegionID[entrezID], aliasTypeID[aliastype],alias) )
			self._loki.dbc.executemany("INSERT INTO db.region_alias (region_id,aliastype_id,alias) VALUES (?,?,?)", regionAliasList)
			if self._verbose:
				sys.stderr.write(" OK\n")
			
			# restore indexes
			if self._verbose:
				sys.stderr.write("restoring indexes on affected database tables ...")
				sys.stderr.flush()
			self._loki.createDatabaseIndexes(None, 'db', ('region','region_alias','region_bound','region_zone'))
			if self._verbose:
				sys.stderr.write(" OK\n")
		#with db transaction
	#update()
	
#Source_genes
