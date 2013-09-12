#!/usr/bin/env python

import collections
import itertools
from loki import loki_source


class Source_hgnc(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '2.1 (2013-03-22)'
	#getVersionString()
	
	
	def download(self, options):
		# download the latest source files
		self.downloadFilesFromHTTP('www.genenames.org', {
			'hgnc_downloads__all_approved.txt': '/cgi-bin/hgnc_downloads?preset=all&status=Approved&status_opt=2&where=&order_by=&format=text&limit=&submit=submit',
		}, alwaysDownload=True)
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		nsID = self.addNamespaces([
			('symbol',       0),
			('hgnc_gid',     0),
			('entrez_gid',   0),
			('ensembl_gid',  0),
			('mirbase_gid',  0),
			('refseq_gid',   0),
			('ccds_gid',     0),
			('vega_gid',     0),
			('omim_gid',     0),
			('uniprot_gid',  0),
		])
		utypeID = self.addUTypes([
			('gene',),
		])
		
		nsNames = collections.defaultdict(set)
		numNames = 0
		nsPropVals = collections.defaultdict(set)
		
		# process genes
		self.log("processing genes ...")
		humanHGNC = set()
		with open('hgnc_downloads__all_approved.txt','rU') as datafile:
			header = datafile.next().rstrip()
			if not header.startswith("HGNC ID	Approved Symbol	Approved Name	Status	Locus Type	Locus Group	Previous Symbols	Previous Names	Synonyms	Name Synonyms	Chromosome	Date Approved	Date Modified	Date Symbol Changed	Date Name Changed	Accession Numbers	Enzyme IDs	Entrez Gene ID	Ensembl Gene ID	Mouse Genome Database ID	Specialist Database Links	Specialist Database IDs	Pubmed IDs	RefSeq IDs	Gene Family Tag	Gene family description	Record Type	Primary IDs	Secondary IDs	CCDS IDs	VEGA IDs	Locus Specific Databases	Entrez Gene ID	OMIM ID	RefSeq	UniProt ID	Ensembl ID"):
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
				return False
			for line in datafile:
				words = [ (w.strip() if w != "-" else None) for w in line.split("\t") ]
				hgncGID = words[0]
				status = words[3]
				if hgncGID and (status == 'Approved'):
					symbol = words[1]
					desc = words[2]
					locustype = words[4]
					locusgroup = words[5]
					aliases = [w.strip() for w in words[6].split(',') if w]
					entrezGID1 = words[17]
					ensemblGID1 = words[18]
					specGIDs = [w.strip() for w in words[21].split(',')]
					mirbaseGID = specGIDs[0]
					refseqGID1 = words[23]
					ccdsGID = words[29].split('.',1)[0]
					vegaGID = words[30]
					entrezGID2 = words[32]
					omimGID = words[33]
					refseqGID2 = words[34]
					uniprotGID = words[35]
					ensemblGID2 = words[36]
					
					# store properties and name references
					humanHGNC.add(hgncGID)
					nsPropVals[(nsID['hgnc_gid'],'utype_id')].add( (hgncGID,utypeID['gene']) )
					nsPropVals[(nsID['hgnc_gid'],'label')].add( (hgncGID,symbol) )
					nsPropVals[(nsID['hgnc_gid'],'description')].add( (hgncGID,desc) )
					nsNames[(nsID['hgnc_gid'],nsID['symbol'])].add( (hgncGID,symbol) )
					for alias in aliases:
						nsNames[(nsID['hgnc_gid'],nsID['symbol'])].add( (hgncGID,alias) )
					nsNames[(nsID['hgnc_gid'],nsID['entrez_gid'])].add( (hgncGID,entrezGID1) )
					nsNames[(nsID['hgnc_gid'],nsID['ensembl_gid'])].add( (hgncGID,ensemblGID1) )
					nsNames[(nsID['hgnc_gid'],nsID['mirbase_gid'])].add( (hgncGID,mirbaseGID) )
					nsNames[(nsID['hgnc_gid'],nsID['refseq_gid'])].add( (hgncGID,refseqGID1) )
					nsNames[(nsID['hgnc_gid'],nsID['ccds_gid'])].add( (hgncGID,ccdsGID) )
					nsNames[(nsID['hgnc_gid'],nsID['vega_gid'])].add( (hgncGID,vegaGID) )
					nsNames[(nsID['hgnc_gid'],nsID['entrez_gid'])].add( (hgncGID,entrezGID2) )
					nsNames[(nsID['hgnc_gid'],nsID['omim_gid'])].add( (hgncGID,omimGID) )
					nsNames[(nsID['hgnc_gid'],nsID['refseq_gid'])].add( (hgncGID,refseqGID2) )
					nsNames[(nsID['hgnc_gid'],nsID['uniprot_gid'])].add( (hgncGID,uniprotGID) )
					nsNames[(nsID['hgnc_gid'],nsID['ensembl_gid'])].add( (hgncGID,ensemblGID2) )
				#if id/status ok
			#foreach line
		#with datafile
		for ns,names in nsNames.iteritems():
			nsNames[ns] = set(filter(lambda n:(n[0] and n[1]), names))
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d genes, %d identifiers\n" % (len(humanHGNC),numNames-numNames0))
		
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
	
#Source_hgnc
