#!/usr/bin/env python

import collections
import itertools
import urllib
from loki import loki_source


class Source_ensembl(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '2.1 (2013-03-21)'
	#getVersionString()
	
	
	def download(self, options):
		# download the latest source files
		self.downloadFilesFromFTP('ftp.ensembl.org', { #TODO callback to find latest file
			'Homo_sapiens.GRCh37.70.gtf.gz': '/pub/current_gtf/homo_sapiens/Homo_sapiens.GRCh37.70.gtf.gz'
		})
		# www.ensembl.org will redirect to whichever mirror they think is closest,
		# but our download functions don't yet understand redirects, so test it
		# first and then send the requests directly to the right mirror
		ensemblHost = self.getHTTPHeaders('www.ensembl.org','/biomart/martservice').get('location','http://www.ensembl.org').split('://',1)[1].split('/',1)[0]
		xml1 = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Query>
<Query virtualSchemaName = "default" formatter = "TSV" header = "1" uniqueRows = "0" count = "" datasetConfigVersion = "0.6" >
	<Dataset name = "hsapiens_gene_ensembl" interface = "default" >
		<Attribute name = "ensembl_gene_id" />
		<Attribute name = "external_gene_id" />
		<Attribute name = "description" />
		<Attribute name = "hgnc_symbol" />
		<Attribute name = "wikigene_name" />
		<Attribute name = "uniprot_genename" />
	</Dataset>
</Query>
"""
		xml2 = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Query>
<Query virtualSchemaName = "default" formatter = "TSV" header = "1" uniqueRows = "0" count = "" datasetConfigVersion = "0.6" >
	<Dataset name = "hsapiens_gene_ensembl" interface = "default" >
		<Attribute name = "ensembl_gene_id" />
		<Attribute name = "ccds" />
		<Attribute name = "entrezgene" />
		<Attribute name = "hgnc_id" />
	</Dataset>
</Query>
"""
		xml3 = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Query>
<Query virtualSchemaName = "default" formatter = "TSV" header = "1" uniqueRows = "0" count = "" datasetConfigVersion = "0.6" >
	<Dataset name = "hsapiens_gene_ensembl" interface = "default" >
		<Attribute name = "ensembl_peptide_id" />
		<Attribute name = "refseq_peptide" />
		<Attribute name = "uniprot_swissprot" />
		<Attribute name = "uniprot_swissprot_accession" />
	</Dataset>
</Query>
"""
		self.downloadFilesFromHTTP(ensemblHost, {
			'biomart_martservice_ensg_desc.txt': '/biomart/martservice?query='+urllib.quote_plus(xml1),
			'biomart_martservice_ensg_refs.txt': '/biomart/martservice?query='+urllib.quote_plus(xml2),
			'biomart_martservice_ensp_refs.txt': '/biomart/martservice?query='+urllib.quote_plus(xml3),
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
			('ensembl_gid',  0),
			('ensembl_pid',  1),
			('ccds_gid',     0),
			('entrez_gid',   0),
			('hgnc_gid',     0),
			('refseq_pid',   1),
			('uniprot_pid',  1),
		])
		utypeID = self.addUTypes([
			('gene',),
		])
		
		nsNames = collections.defaultdict(set)
		numNames = 0
		nsPropVals = collections.defaultdict(set)
		ensgLabel = dict()
		
		# process genes
		self.log("processing genes ...")
		with open('biomart_martservice_ensg_desc.txt','rU') as datafile:
			header = datafile.next().rstrip()
			if not header.startswith("Ensembl Gene ID	Associated Gene Name	Description	HGNC symbol	WikiGene Name	UniProt Gene Name"):
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
				return False
			for line in datafile:
				words = [ w.strip() for w in line.split("\t") ]
				ensg = words[0]
				symbol = words[1]
				desc = words[2].split('[',1)[0].strip()
				hgncSymbol = words[3]
				wikigeneSymbol = words[4]
				uniprotSymbol = words[5]
				label = symbol or hgncSymbol or wikigeneSymbol or uniprotSymbol
				
				if not ensg:
					continue
				
				# store properties and name references
				nsPropVals[(nsID['ensembl_gid'],'utype_id')].add( (ensg,utypeID['gene']) )
				if label:
					ensgLabel[ensg] = label
					nsPropVals[(nsID['ensembl_gid'],'label')].add( (ensg,label) )
				if symbol:
					nsNames[(nsID['ensembl_gid'],nsID['symbol'])].add( (ensg,symbol) )
				if desc:
					nsPropVals[(nsID['ensembl_gid'],'description')].add( (ensg,desc) )
				if hgncSymbol:
					nsNames[(nsID['ensembl_gid'],nsID['symbol'])].add( (ensg,hgncSymbol) )
				if wikigeneSymbol:
					nsNames[(nsID['ensembl_gid'],nsID['symbol'])].add( (ensg,wikigeneSymbol) )
				if uniprotSymbol:
					nsNames[(nsID['ensembl_gid'],nsID['symbol'])].add( (ensg,uniprotSymbol) )
			#foreach line
		#with datafile
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d genes, %d identifiers\n" % (len(ensgLabel),numNames-numNames0))
		
		# process gene regions (no header!)
		self.log("processing genomic regions ...")
		rtypeRegions = collections.defaultdict(set)
		regionGenes = set()
		datafile = self.zfile('Homo_sapiens.GRCh37.70.gtf.gz') #TODO:context manager,iterator
		for line in datafile:
			words = [ w.strip() for w in line.split("\t") ]
			chm = words[0]
			if chm in self._loki.chr_num:
				chm = self._loki.chr_num[chm]
				rtype = words[1]
				posMin = words[3]
				posMax = words[4]
				tags = dict( (tag[0].strip(),tag[1].strip(' "')) for tag in (pair.strip().split(" ",1) for pair in words[8].split(";") if pair.strip()) )
				ensg = tags.get('gene_id')
				
				if not ensg:
					continue
				regionGenes.add(ensg)
				
				# store properties and name references
				nsPropVals[(nsID['ensembl_gid'],'utype_id')].add( (ensg,utypeID['gene']) )
				if 'gene_name' in tags:
					nsNames[(nsID['ensembl_gid'],nsID['symbol'])].add( (ensg,tags['gene_name']) )
					if ensg not in ensgLabel:
						ensgLabel[ensg] = tags['gene_name']
						nsPropVals[(nsID['ensembl_gid'],'label')].add( (ensg,tags['gene_name']) )
				#if 'gene_biotype' in tags:
				#	nsNames[(nsID['ensembl_gid'],nsID['utype'])].add( (ensg,tags['gene_biotype']) )
				if 'transcript_id' in tags:
					nsNames[(nsID['ensembl_gid'],nsID['ensembl_gid'])].add( (ensg,tags['transcript_id']) )
				if 'exon_id' in tags:
					nsNames[(nsID['ensembl_gid'],nsID['ensembl_gid'])].add( (ensg,tags['exon_id']) )
				if 'protein_id' in tags:
					nsNames[(nsID['ensembl_gid'],nsID['ensembl_pid'])].add( (ensg,tags['protein_id']) )
				
				# store region
				# (GTF positions are 1-based, inclusive per http://mblab.wustl.edu/GTF2.html)
				rtypeRegions[rtype].add( (ensg,chm,long(posMin),long(posMax)) )
			#if taxonomy is 9606 (human)
		#foreach line
		numRegions = sum(len(regions) for regions in rtypeRegions.itervalues())
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d regions (%d genes), %d identifiers\n" % (numRegions,len(regionGenes),numNames-numNames0))
		regionGenes = None
		
		# get or create rtype metadata records
		rtypeID = self.addRTypes((rtype,) for rtype in rtypeRegions)
		
		# store regions
		self.log("storing regions ...")
		regionNames = list()
		for rtype,regions in rtypeRegions.iteritems():
			listR = list(regions)
			listID = self.addTypedRegions(rtypeID[rtype], (r[1:] for r in listR))
			regionNames.extend(itertools.izip(listID, (r[0] for r in listR)))
		self.setSourceBuilds(37, None) #TODO
		self.log(" OK: %d regions\n" % (len(regionNames),))
		rtypeRegions = listR = listID = None
		
		# store region names
		self.log("storing region identifiers ...")
		self.addRegionNamespacedNames(nsID['ensembl_gid'], regionNames)
		self.log(" OK: %d identifiers\n" % (len(regionNames),))
		regionNames = None
		
		# process gene identifiers
		self.log("processing gene identifiers ...")
		with open('biomart_martservice_ensg_refs.txt','rU') as datafile:
			header = datafile.next().rstrip()
			if not header.startswith("Ensembl Gene ID	CCDS ID	EntrezGene ID	HGNC ID(s)"):
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
				return False
			for line in datafile:
				words = [ w.strip() for w in line.split("\t") ]
				ensg = words[0]
				ccdsGID = words[1]
				entrezGID = words[2]
				hgncGID = words[3]
				
				if not ensg:
					continue
				
				# store name references
				if ccdsGID:
					nsNames[(nsID['ensembl_gid'],nsID['ccds_gid'])].add( (ensg,ccdsGID) )
				if entrezGID:
					nsNames[(nsID['ensembl_gid'],nsID['entrez_gid'])].add( (ensg,entrezGID) )
				if hgncGID:
					nsNames[(nsID['ensembl_gid'],nsID['hgnc_gid'])].add( (ensg,hgncGID) )
			#foreach line
		#with datafile
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d identifiers\n" % (numNames-numNames0,))
		
		# process protein identifiers
		self.log("processing protein identifiers ...")
		with open('biomart_martservice_ensp_refs.txt','rU') as datafile:
			header = datafile.next().rstrip()
			if not header.startswith("Ensembl Protein ID	RefSeq Protein ID [e.g. NP_001005353]	UniProt/SwissProt ID	UniProt/SwissProt Accession"):
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
				return False
			for line in datafile:
				words = [ w.strip() for w in line.split("\t") ]
				ensp = words[0]
				refseqPID = words[1]
				uniprotPID = words[2]
				uniprotAcc = words[3]
				
				if not ensp:
					continue
				
				# store name references
				if refseqPID:
					nsNames[(nsID['ensembl_pid'],nsID['refseq_pid'])].add( (ensp,refseqPID) )
				if uniprotPID:
					nsNames[(nsID['ensembl_pid'],nsID['uniprot_pid'])].add( (ensp,uniprotPID) )
				if uniprotAcc:
					nsNames[(nsID['ensembl_pid'],nsID['uniprot_pid'])].add( (ensp,uniprotAcc) )
			#foreach line
		#with datafile
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d identifiers\n" % (numNames-numNames0,))
		
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
	
#Source_ensembl
