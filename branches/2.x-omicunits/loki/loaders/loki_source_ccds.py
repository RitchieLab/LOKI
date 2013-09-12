#!/usr/bin/env python

import collections
import itertools
from loki import loki_source


class Source_ccds(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '2.1 (2013-03-21)'
	#getVersionString()
	
	
	def download(self, options):
		# download the latest source files
		self.downloadFilesFromFTP('ftp.ncbi.nlm.nih.gov', {
			'BuildInfo.current.txt': '/pub/CCDS/current_human/BuildInfo.current.txt',
			'CCDS.current.txt':      '/pub/CCDS/current_human/CCDS.current.txt',
		})
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		nsID = self.addNamespaces([
			('symbol',       0),
			('ccds_gid',     0),
			('entrez_gid',   0),
		])
		utypeID = self.addUTypes([
			('gene',),
		])
		rtypeID = self.addRTypes([
			('composite',),
			('exon',),
		])
		
		nsNames = collections.defaultdict(set)
		numNames = 0
		nsPropVals = collections.defaultdict(set)
		
		# process metadata
		self.log("processing build metadata ...")
		with open('BuildInfo.current.txt','rU') as datafile:
			header = datafile.next().rstrip()
			if not header.startswith("#tax_id	ncbi_release_number	ensembl_release_number	assembly_name"):
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
				return False
			line = datafile.next()
			words = [ w.strip() for w in line.split("\t") ]
			if words[0] != "9606":
				self.log(" ERROR: invalid tax_id '%s'\n" % (words[0],))
				return False
			if not words[3].startswith('GRCh'):
				self.log(" ERROR: invalid assembly_name '%s'\n" % (words[3],))
				return False
			grch = int(words[3][4:].split('.',1)[0])
			self.setSourceBuilds(grch, None)
		#with datafile
		self.log(" OK: GRCh assembly version %d\n" % (grch,))
		
		# process genes/regions
		self.log("processing genes and genomic regions ...")
		rtypeRegions = collections.defaultdict(set)
		regionGenes = set()
		with open('CCDS.current.txt','rU') as datafile:
			header = datafile.next().rstrip()
			if not header.startswith("#chromosome	nc_accession	gene	gene_id	ccds_id	ccds_status	cds_strand	cds_from	cds_to	cds_locations"):
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
				return False
			empty = list()
			for line in datafile:
				words = [ (w.strip() if w != "-" else None) for w in line.split("\t") ]
				chm = words[0]
				ccdsGID = words[4].split('.',1)[0] if words[4] else None
				status = words[5]
				if (chm in self._loki.chr_num) and ccdsGID and not status.startswith("Withdrawn"):
					chm = self._loki.chr_num[chm]
					symbol = words[2]
					entrezGID = words[3]
					posMin = words[7]
					posMax = words[8]
					exons = [ exonRange.split('-') for exonRange in (words[9].strip('[]').split(',') if words[9] else empty) ]
					
					regionGenes.add(ccdsGID)
					
					# store properties and name references
					nsPropVals[(nsID['ccds_gid'],'utype_id')].add( (ccdsGID,utypeID['gene']) )
					if symbol:
						nsPropVals[(nsID['ccds_gid'],'label')].add( (ccdsGID,symbol) )
						nsNames[(nsID['ccds_gid'],nsID['symbol'])].add( (ccdsGID,symbol) )
					if entrezGID:
						nsNames[(nsID['ccds_gid'],nsID['entrez_gid'])].add( (ccdsGID,entrezGID) )
					
					# store regions
					# (positions are 0-based per ftp://ftp.ncbi.nlm.nih.gov/pub/CCDS/README)
					if posMin and posMax:
						rtypeRegions['composite'].add( (ccdsGID,chm,long(posMin)+1,long(posMax)+1) )
					for posMin,posMax in exons:
						rtypeRegions['exon'].add( (ccdsGID,chm,long(posMin)+1,long(posMax)+1) )
				#if chm/status ok
			#foreach line
		#with datafile
		numRegions = sum(len(regions) for regions in rtypeRegions.itervalues())
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d regions (%d genes), %d identifiers\n" % (numRegions,len(regionGenes),numNames-numNames0))
		regionGenes = None
		
		# store regions
		self.log("storing regions ...")
		regionNames = list()
		for rtype,regions in rtypeRegions.iteritems():
			listR = list(regions)
			listID = self.addTypedRegions(rtypeID[rtype], (r[1:] for r in listR))
			regionNames.extend(itertools.izip(listID, (r[0] for r in listR)))
		self.log(" OK: %d regions\n" % (len(regionNames),))
		rtypeRegions = listR = listID = None
		
		# store region names
		self.log("storing region identifiers ...")
		self.addRegionNamespacedNames(nsID['ccds_gid'], regionNames)
		self.log(" OK: %d identifiers\n" % (len(regionNames),))
		regionNames = None
		
		# store identifiers
		self.log("storing identifier references ...")
		numNames = 0
		for ns,names in nsNames.iteritems():
			numNames += len(names)
			self.addUnitNamespacedNameNames(ns[0], ns[1], names)
		self.log(" OK: %d references\n" % (numNames,))
		
		# store properties
		self.log("storing identifier properties ...")
		numProps = 0
		for nsProp,vals in nsPropVals.iteritems():
			numProps += len(vals)
			self.addUnitNamespacedNameProperties(nsProp[0], nsProp[1], vals)
		self.log(" OK: %d properties\n" % (numProps,))
	#update()
	
#Source_ccds
