#!/usr/bin/env python

import collections
import re
from loki import loki_source


class Source_hgnc(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '3.0 (2016-11-08)'
	#getVersionString()
	
	
	def download(self, options):
		# download the latest source files
	#	self.downloadFilesFromHTTP('www.genenames.org', {
	#		'hgnc_downloads__all_approved.txt': '/cgi-bin/download?preset=all&status=Approved&status_opt=2&where=&order_by=&format=text&limit=&submit=submit',
	#	}, alwaysDownload=True)
		self.downloadFilesFromFTP('ftp.ebi.ac.uk', {
			'hgnc_complete_set.txt': '/pub/databases/genenames/new/tsv/hgnc_complete_set.txt'
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
		reList = re.compile('[ \t]*([^ \t"|][^"|]*?)[ \t]*(?:"|\\||[\r\n]*$)')
		
		nsNames = collections.defaultdict(set)
		numNames = 0
		nsPropVals = collections.defaultdict(set)
		
		# process genes
		self.log("processing genes ...")
		humanHGNC = set()
	#	with open('hgnc_downloads__all_approved.txt','rU') as datafile:
		with open('hgnc_complete_set.txt','rU') as datafile:
			header = datafile.next().rstrip()
			if not (
				header.startswith("HGNC ID	Approved Symbol	Approved Name	Status	Locus Type	Locus Group	Previous Symbols	Previous Names	Synonyms	Name Synonyms	Chromosome	Date Approved	Date Modified	Date Symbol Changed	Date Name Changed	Accession Numbers	Enzyme IDs	Entrez Gene ID	Ensembl Gene ID	Mouse Genome Database ID	Specialist Database Links	Specialist Database IDs	Pubmed IDs	RefSeq IDs	Gene Family Tag	Gene family description	Record Type	Primary IDs	Secondary IDs	CCDS IDs	VEGA IDs	Locus Specific Databases	Entrez Gene ID	OMIM ID	RefSeq	UniProt ID	Ensembl ID")
				or header.startswith("HGNC ID	Approved Symbol	Approved Name	Status	Locus Type	Locus Group	Previous Symbols	Previous Names	Synonyms	Name Synonyms	Chromosome	Date Approved	Date Modified	Date Symbol Changed	Date Name Changed	Accession Numbers	Enzyme IDs	Entrez Gene ID	Ensembl Gene ID	Mouse Genome Database ID	Specialist Database Links	Specialist Database IDs	Pubmed IDs	RefSeq IDs	Gene Family Tag	Gene family description	Record Type	Primary IDs	Secondary IDs	CCDS IDs	VEGA IDs	Locus Specific Databases	Entrez Gene ID(supplied by NCBI)	OMIM ID(supplied by NCBI)	RefSeq(supplied by NCBI)	UniProt ID(supplied by UniProt)	Ensembl ID(supplied by Ensembl)")
				or header.startswith("hgnc_id	symbol	name	locus_group	locus_type	status	location	location_sortable	alias_symbol	alias_name	prev_symbol	prev_name	gene_family	gene_family_id	date_approved_reserved	date_symbol_changed	date_name_changed	date_modified	entrez_id	ensembl_gene_id	vega_id	ucsc_id	ena	refseq_accession	ccds_id	uniprot_ids	pubmed_id	mgd_id	rgd_id	lsdb	cosmic	omim_id	mirbase	homeodb	snornabase	bioparadigms_slc	orphanet	pseudogene.org	horde_id	merops	imgt	iuphar	kznf_gene_catalog	mamit-trnadb	cd	lncrnadb	enzyme_id	intermediate_filament_db")
			):
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
				return False
			newformat = header.startswith("hgnc_id	symbol	")
			for line in datafile:
				words = [ (w.strip() if w != "-" else None) for w in line.split("\t") ]
				hgncGID = words[0]
				status = words[5 if newformat else 3]
				if hgncGID.startswith('HGNC:') and (status == 'Approved'):
					symbol = words[1]
					desc = words[2]
					aliases = list()
					if newformat:
						aliases.extend(reList.findall(words[8]))
						aliases.extend(reList.findall(words[10]))
					else:
						aliases.extend( w.strip() for w in words[6].split(',') if w )
					entrezGIDs = reList.findall(words[18 if newformat else 17])
					ensemblGIDs = reList.findall(words[19 if newformat else 18])
					if newformat:
						mirbaseGIDs = reList.findall(words[32])
					else:
						specGIDs = [w.strip() for w in words[21].split(',')]
						mirbaseGIDs = [ specGIDs[0] ]
					refseqGIDs = reList.findall(words[23])
					ccdsGIDs = [ name.split('.',1)[0] for name in reList.findall(words[24 if newformat else 29]) ]
					vegaGIDs = reList.findall(words[20 if newformat else 30])
					omimGIDs = reList.findall(words[31 if newformat else 33])
					uniprotGIDs = reList.findall(words[25 if newformat else 35])
					if not newformat:
						entrezGIDs.extend(reList.findall(words[32]))
						refseqGIDs.extend(reList.findall(words[34]))
						ensemblGIDs.extend(reList.findall(words[36]))
					
					# store properties and name references
					humanHGNC.add(hgncGID)
					nsPropVals[(nsID['hgnc_gid'],'utype_id')].add( (hgncGID,utypeID['gene']) )
					nsPropVals[(nsID['hgnc_gid'],'label')].add( (hgncGID,symbol) )
					nsPropVals[(nsID['hgnc_gid'],'description')].add( (hgncGID,desc) )
					for name in aliases:
						nsNames[(nsID['hgnc_gid'],nsID['symbol'])].add( (hgncGID,name) )
					for name in entrezGIDs:
						nsNames[(nsID['hgnc_gid'],nsID['entrez_gid'])].add( (hgncGID,name) )
					for name in ensemblGIDs:
						nsNames[(nsID['hgnc_gid'],nsID['ensembl_gid'])].add( (hgncGID,name) )
					for name in mirbaseGIDs:
						nsNames[(nsID['hgnc_gid'],nsID['mirbase_gid'])].add( (hgncGID,name) )
					for name in refseqGIDs:
						nsNames[(nsID['hgnc_gid'],nsID['refseq_gid'])].add( (hgncGID,name) )
					for name in ccdsGIDs:
						nsNames[(nsID['hgnc_gid'],nsID['ccds_gid'])].add( (hgncGID,name) )
					for name in vegaGIDs:
						nsNames[(nsID['hgnc_gid'],nsID['vega_gid'])].add( (hgncGID,name) )
					for name in omimGIDs:
						nsNames[(nsID['hgnc_gid'],nsID['omim_gid'])].add( (hgncGID,name) )
					for name in uniprotGIDs:
						nsNames[(nsID['hgnc_gid'],nsID['uniprot_gid'])].add( (hgncGID,name) )
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
			numNames += sum(1 for n in names if n[1])
			self.addUnitNamespacedNameNames(ns[0], ns[1], (n for n in names if n[1]))
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
