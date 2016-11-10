#!/usr/bin/env python

import collections
import itertools
import os
import re
from loki import loki_source


class Source_gencode(loki_source.Source):
	
	
	##################################################
	# source interface
	
	
	@classmethod
	def getVersionString(cls):
		return '3.0 (2016-11-04)'
	#getVersionString()
	
	
	def download(self, options):
		# define a callback to identify the latest *-mint-human.txt file
		def remFilesCallback(ftp):
			remFiles = {}
			
			rePath = re.compile('^release_([0-9]+)(.*)$', re.IGNORECASE)
			basepath = '/pub/gencode/Gencode_human'
			ftp.cwd(basepath)
			vers,suffix,subpath = max(
				(int(match.group(1)),match.group(2),match.group(0)) for match in (
					rePath.match(pathname) for pathname in ftp.nlst()
				) if match
			)
			
			remFiles['gencode.annotation.gtf.gz'] = '%s/%s/gencode.v%d.annotation.gtf.gz' % (basepath,subpath,vers)
			remFiles['gencode.metadata.EntrezGene.gz'] = '%s/%s/gencode.v%d.metadata.EntrezGene.gz' % (basepath,subpath,vers)
			remFiles['gencode.metadata.HGNC.gz'] = '%s/%s/gencode.v%d.metadata.HGNC.gz' % (basepath,subpath,vers)
			
			return remFiles
		#remFilesCallback
		
		# download the latest FTP source files
		self.downloadFilesFromFTP('ftp.sanger.ac.uk', remFilesCallback)
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		nsID = self.addNamespaces([
			('symbol',       0),
			('ensembl_gid',  0),
			('havana_gid',   0),
			('ccds_gid',     0),
			('entrez_gid',   0),
		])
		utypeID = self.addUTypes([
			('gene',),
		])
		
		rtypeRegions = collections.defaultdict(set)
		nsNames = collections.defaultdict(set)
		numNames = 0
		nsPropVals = collections.defaultdict(set)
		ensgLabel = dict()
		
		# process regions
		self.log("processing genomic regions ...")
		datafile = self.zfile('gencode.annotation.gtf.gz') #TODO:context manager,iterator
		header = datafile.next()
		reHeader = re.compile('^##description: .* \(GRCh([0-9]+)\)')
		match = reHeader.match(header)
		if not match:
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
			return False
		self.setSourceBuilds(int(match.group(1)), None)
		reField = re.compile(' *([^ ]+) +"?([^"]*)"? *;?')
		numInc = 0
		for line in datafile:
			if line.startswith('##'):
				continue
			words = [ w.strip() for w in line.split("\t") ]
			if len(words) < 9:
				numInc += 1
				continue
			
			chm = self._loki.chr_num.get(words[0][3:]) if words[0].startswith('chr') else None
			rtype = words[2]
			posMin = long(words[3])
			posMax = long(words[4])
			symbol = ensg = enst = havg = havt = ccdsg = None
			for key,val in re.findall(reField, words[8]):
				if key == 'gene_name':
					symbol = val
				elif key == 'gene_id':
					ensg = val.split('.')[0]
				elif key == 'transcript_id':
					enst = val.split('.')[0]
				elif key == 'havana_gene':
					havg = val.split('.')[0]
				elif key == 'havana_transcript':
					havt = val.split('.')[0]
				elif key == 'ccdsid':
					ccdsg = val.split('.')[0]
			
			if chm and ensg:
				# store properties and name references
				nsPropVals[(nsID['ensembl_gid'],'utype_id')].add( (ensg,utypeID['gene']) )
				if symbol:
					ensgLabel[ensg] = symbol
					nsPropVals[(nsID['ensembl_gid'],'label')].add( (ensg,symbol) )
					nsNames[(nsID['ensembl_gid'],nsID['symbol'])].add( (ensg,symbol) )
				if enst:
					nsNames[(nsID['ensembl_gid'],nsID['ensembl_gid'])].add( (ensg,enst) )
				if havg:
					nsNames[(nsID['ensembl_gid'],nsID['havana_gid'])].add( (ensg,havg) )
				if havt:
					nsNames[(nsID['ensembl_gid'],nsID['havana_gid'])].add( (ensg,havt) )
				if ccdsg:
					nsNames[(nsID['ensembl_gid'],nsID['ccds_gid'])].add( (ensg,ccdsg) )
				
				# store region
				# (GTF positions are 1-based, inclusive per http://mblab.wustl.edu/GTF2.html)
				rtypeRegions[rtype].add( (ensg,chm,long(posMin),long(posMax)) )
			#if complete
		#with datafile
		numRegions = sum(len(regions) for regions in rtypeRegions.itervalues())
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d regions (%d genes), %d identifiers\n" % (numRegions,len(ensgLabel),numNames-numNames0))
		self.logPush()
		if numInc:
			self.log("WARNING: %d incomplete records\n" % (numInc,))
		self.logPop()
		
		# process gene identifiers
		self.log("processing Entrez gene identifiers ...")
		datafile = self.zfile('gencode.metadata.EntrezGene.gz') #TODO:context manager,iterator
		numInc = 0
		for line in datafile:
			words = [ w.strip() for w in line.split("\t") ]
			if len(words) < 2:
				numInc += 1
				continue
			ensg = words[0].split('.')[0]
			entrezGID = words[1]
			
			# store name references
			if ensg and entrezGID:
				nsNames[(nsID['ensembl_gid'],nsID['entrez_gid'])].add( (ensg,entrezGID) )
		#foreach line
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d identifiers\n" % (numNames-numNames0,))
		self.logPush()
		if numInc:
			self.log("WARNING: %d incomplete records\n" % (numInc,))
		self.logPop()
		
		# process gene identifiers
		self.log("processing HGNC gene identifiers ...")
		datafile = self.zfile('gencode.metadata.HGNC.gz') #TODO:context manager,iterator
		numInc = 0
		for line in datafile:
			words = [ w.strip() for w in line.split("\t") ]
			if len(words) < 2:
				numInc += 1
				continue
			ensg = words[0].split('.')[0]
			symbol = words[1]
			
			# store name references
			if ensg and symbol:
				nsNames[(nsID['ensembl_gid'],nsID['symbol'])].add( (ensg,symbol) )
		#foreach line
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d identifiers\n" % (numNames-numNames0,))
		self.logPush()
		if numInc:
			self.log("WARNING: %d incomplete records\n" % (numInc,))
		self.logPop()
		
		# get or create rtype metadata records
		rtypeID = self.addRTypes((rtype,) for rtype in rtypeRegions)
		
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
		self.addRegionNamespacedNames(nsID['ensembl_gid'], regionNames)
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
	
#Source_gencode
