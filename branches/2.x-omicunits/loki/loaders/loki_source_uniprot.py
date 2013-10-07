#!/usr/bin/env python

import collections
from loki import loki_source


class Source_uniprot(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '3.0 (2013-09-16)'
	#getVersionString()
	
	
	def download(self, options):
		# download the latest source files
		self.downloadFilesFromFTP('ftp.uniprot.org', {
			'HUMAN_9606_idmapping_selected.tab.gz': '/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/HUMAN_9606_idmapping_selected.tab.gz',
		})
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		nsID = self.addNamespaces([
			('entrez_gid',  0),
			('refseq_gid',  0),
			('refseq_pid',  1),
			('ensembl_gid', 0),
			('ensembl_pid', 1),
			('mim_gid',     0),
			('unigene_gid', 0),
			('uniprot_gid', 0),
			('uniprot_pid', 1),
		])
		
		# process uniprot gene names from uniprot (no header!)
		nsNames = collections.defaultdict(set)
		self.log("processing uniprot gene names ...")
		upFile = self.zfile('HUMAN_9606_idmapping_selected.tab.gz') #TODO:context manager,iterator
		""" /* ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/README */
1. UniProtKB-AC
2. UniProtKB-ID
3. GeneID (EntrezGene)
4. RefSeq
5. GI
6. PDB
7. GO
8. IPI
9. UniRef100
10. UniRef90
11. UniRef50
12. UniParc
13. PIR
14. NCBI-taxon
15. MIM
16. UniGene
17. PubMed
18. EMBL
19. EMBL-CDS
20. Ensembl
21. Ensembl_TRS
22. Ensembl_PRO
23. Additional PubMed
"""
		for line in upFile:
			words = line.split("\t")
			uniprotAcc = words[0]
			uniprotID = words[1]
			for word2 in words[2].split(';'):
				entrezID = int(word2.strip()) if word2 else None
				if entrezID:
					nsNames[(nsID['uniprot_pid'],nsID['entrez_gid'])].add( (uniprotAcc,entrezID) )
					nsNames[(nsID['uniprot_gid'],nsID['entrez_gid'])].add( (uniprotID,entrezID) )
			#foreach entrezID mapping
			for word3 in words[3].split(';'):
				refseqID = word3.strip().split('.',1)[0] if word3 else None
				if refseqID:
					nsNames[(nsID['uniprot_pid'],nsID['refseq_pid'])].add( (uniprotAcc,refseqID) )
					nsNames[(nsID['uniprot_pid'],nsID['refseq_gid'])].add( (uniprotAcc,refseqID) )
					nsNames[(nsID['uniprot_gid'],nsID['refseq_pid'])].add( (uniprotID,refseqID) )
					nsNames[(nsID['uniprot_gid'],nsID['refseq_gid'])].add( (uniprotID,refseqID) )
			#foreach refseq mapping
			for word14 in words[14].split(';'):
				mimID = word14.strip() if word14 else None
				if mimID:
					nsNames[(nsID['uniprot_pid'],nsID['mim_gid'])].add( (uniprotAcc,mimID) )
					nsNames[(nsID['uniprot_gid'],nsID['mim_gid'])].add( (uniprotID,mimID) )
			#foreach mim mapping
			for word15 in words[15].split(';'):
				unigeneID = word15.strip() if word15 else None
				if unigeneID:
					nsNames[(nsID['uniprot_pid'],nsID['unigene_gid'])].add( (uniprotAcc,unigeneID) )
					nsNames[(nsID['uniprot_gid'],nsID['unigene_gid'])].add( (uniprotID,unigeneID) )
			#foreach unigene mapping
			for word19 in words[19].split(';'):
				ensemblGID = word19.strip() if word19 else None
				if ensemblGID:
					nsNames[(nsID['uniprot_pid'],nsID['ensembl_gid'])].add( (uniprotAcc,ensemblGID) )
					nsNames[(nsID['uniprot_gid'],nsID['ensembl_gid'])].add( (uniprotID,ensemblGID) )
			#foreach ensG mapping
		#TODO: decide if ENST is useful, and if so, pull them from ensembl directly so they link to ENSGs
		#	for word20 in words[20].split(';'):
		#		ensemblTID = word20.strip() if word20 else None
		#		if ensemblTID:
		#			nsNames[(nsID['uniprot_pid'],nsID['ensembl_gid'])].add( (uniprotAcc,ensemblTID) )
		#			nsNames[(nsID['uniprot_gid'],nsID['ensembl_gid'])].add( (uniprotID,ensemblTID) )
		#	#foreach ensT mapping
			for word21 in words[21].split(';'):
				ensemblPID = word21.strip() if word21 else None
				if ensemblPID:
					nsNames[(nsID['uniprot_pid'],nsID['ensembl_pid'])].add( (uniprotAcc,ensemblPID) )
					nsNames[(nsID['uniprot_gid'],nsID['ensembl_pid'])].add( (uniprotID,ensemblPID) )
			#foreach ensP mapping
		#foreach line in upFile
		
		# print stats
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d identifiers\n" % (numNames,))
		
		# store identifiers
		self.log("storing identifier references ...")
		numNames = 0
		for ns,names in nsNames.iteritems():
			numNames += len(names)
			self.addUnitNamespacedNameNames(ns[0], ns[1], names)
		self.log(" OK: %d references\n" % (numNames,))
	#update()
	
#Source_uniprot
