#!/usr/bin/env python

import os
import re
from loki import loki_source


class Source_gwas(loki_source.Source):
	
	
	##################################################
	# source interface
	
	
	@classmethod
	def getVersionString(cls):
		return '2.3 (2016-04-06)'
	#getVersionString()
	
	
	def download(self, options):
		# download the latest source files
	#	self.downloadFilesFromHTTP('www.genome.gov', {
	#		'gwascatalog.txt': '/admin/gwascatalog.txt',
	#	})
		self.downloadFilesFromHTTP('www.ebi.ac.uk', {
			'gwas_catalog_v1.0-associations.tsv' : '/gwas/api/search/downloads/full'
		}, alwaysDownload=True)
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# process gwas cataog
		# the catalog uses dbSNP positions from b132, which should already be 1-based
		self.log("processing GWAS catalog annotations ...")
		reRS = re.compile('rs[0-9]+', re.I)
		listNone = [None]
		numInc = 0
		setGwas = set()
		if os.path.exists('gwas_catalog_v1.0-associations.tsv'):
			with open('gwas_catalog_v1.0-associations.tsv','rU') as gwasFile:
				header = gwasFile.next().rstrip()
				if header.startswith("DATE ADDED TO CATALOG\tPUBMEDID\tFIRST AUTHOR\tDATE\tJOURNAL\tLINK\tSTUDY\tDISEASE/TRAIT\tINITIAL SAMPLE DESCRIPTION\tREPLICATION SAMPLE DESCRIPTION\tREGION\tCHR_ID\tCHR_POS\tREPORTED GENE(S)\tENTREZ_MAPPED_GENE\tENSEMBL_MAPPED_GENE\tENTREZ_UPSTREAM_GENE_ID\tENTREZ_DOWNSTREAM_GENE_ID\tENSEMBL_UPSTREAM_GENE_ID\tENSEMBL_DOWNSTREAM_GENE_ID\tSNP_GENE_IDS_ENTREZ\tSNP_GENE_IDS_ENSEMBL\tENTREZ_UPSTREAM_GENE_DISTANCE\tENTREZ_DOWNSTREAM_GENE_DISTANCE\tENSEMBL_UPSTREAM_GENE_DISTANCE\tENSEMBL_DOWNSTREAM_GENE_DISTANCE\tSTRONGEST SNP-RISK ALLELE\tSNPS\tMERGED\tSNP_ID_CURRENT\tCONTEXT\tINTERGENIC_ENTREZ\tINTERGENIC_ENSEMBL\tRISK ALLELE FREQUENCY\tP-VALUE\tPVALUE_MLOG\tP-VALUE (TEXT)\tOR or BETA\t95% CI (TEXT)\t"): # PLATFORM [SNPS PASSING QC]\tCNV"):
					pass
				else:
					self.log(" ERROR\n")
					raise Exception("unrecognized file header")
				for line in gwasFile:
					line = line.rstrip("\r\n")
					words = list(w.strip() for w in line.decode('latin-1').split("\t"))
					if len(words) <= 38:
						# blank line at the end is normal
						if (len(words) > 1) or words[0]:
							numInc += 1
						continue
					pubmedID = int(words[1]) if words[1] else None
					trait = words[7]
					chm = self._loki.chr_num[words[11]] if (words[11] in self._loki.chr_num) else None
					pos = long(words[12]) if words[12] else None
					snps = words[26] + ' ' + words[27]
					rses = set(int(rs[2:]) for rs in reRS.findall(snps)) or listNone
					riskAfreq = words[33]
					orBeta = words[37]
					allele95ci = words[38]
					for rs in rses:
						setGwas.add( (rs,chm,pos,trait,snps,orBeta,allele95ci,riskAfreq,pubmedID) )
				#foreach line
			#with gwasFile
		else:
			with open('gwascatalog.txt','rU') as gwasFile:
				header = gwasFile.next().rstrip()
				if header.startswith("Date Added to Catalog\tPUBMEDID\tFirst Author\tDate\tJournal\tLink\tStudy\tDisease/Trait\tInitial Sample Size\tReplication Sample Size\tRegion\tChr_id\tChr_pos\tReported Gene(s)\tMapped_gene\tUpstream_gene_id\tDownstream_gene_id\tSnp_gene_ids\tUpstream_gene_distance\tDownstream_gene_distance\tStrongest SNP-Risk Allele\tSNPs\tMerged\tSnp_id_current\tContext\tIntergenic\tRisk Allele Frequency\tp-Value\tPvalue_mlog\tp-Value (text)\tOR or beta\t95% CI (text)\t"): # "Platform [SNPs passing QC]\tCNV"
					pass
				elif header.startswith("Date Added to Catalog\tPUBMEDID\tFirst Author\tDate\tJournal\tLink\tStudy\tDisease/Trait\tInitial Sample Description\tReplication Sample Description\tRegion\tChr_id\tChr_pos\tReported Gene(s)\tMapped_gene\tUpstream_gene_id\tDownstream_gene_id\tSnp_gene_ids\tUpstream_gene_distance\tDownstream_gene_distance\tStrongest SNP-Risk Allele\tSNPs\tMerged\tSnp_id_current\tContext\tIntergenic\tRisk Allele Frequency\tp-Value\tPvalue_mlog\tp-Value (text)\tOR or beta\t95% CI (text)\t"): # "Platform [SNPs passing QC]\tCNV"
					pass
				else:
					self.log(" ERROR\n")
					raise Exception("unrecognized file header")
				for line in gwasFile:
					line = line.rstrip("\r\n")
					words = list(w.strip() for w in line.decode('latin-1').split("\t"))
					if len(words) <= 31:
						# blank line at the end is normal
						if (len(words) > 1) or words[0]:
							numInc += 1
						continue
					chm = self._loki.chr_num[words[11]] if (words[11] in self._loki.chr_num) else None
					pos = long(words[12]) if words[12] else None
					trait = words[7]
					snps = words[21] if words[20].endswith('aplotype') else words[20]
					rses = list(int(rs[2:]) for rs in reRS.findall(snps)) or listNone
					orBeta = words[30]
					allele95ci = words[31]
					riskAfreq = words[26]
					pubmedID = int(words[1]) if words[1] else None
					for rs in rses:
						setGwas.add( (rs,chm,pos,trait,snps,orBeta,allele95ci,riskAfreq,pubmedID) )
				#foreach line
			#with gwasFile
		#if path
		self.log(" OK: %d catalog entries (%d incomplete)\n" % (len(setGwas),numInc))
		if setGwas:
			self.log("writing GWAS catalog annotations to the database ...")
			self.addGWASAnnotations(setGwas)
			self.log(" OK\n")
	#update()
	
#Source_gwas
