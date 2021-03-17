#!/usr/bin/env python

import collections
import itertools
import re
from loki import loki_source


class Source_entrez(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '3.0 (2017-01-13)'
	#getVersionString()
	
	
	@classmethod
	def getOptions(cls):
		return {
			'favor-primary'    : "[yes|no]  --  reduce symbol ambiguity by favoring primary symbols (default: yes)",
			'favor-historical' : "[yes|no]  --  reduce symbol ambiguity by favoring historical symbols (default: yes)",
		}
	#getOptions()
	
	
	def validateOptions(self, options):
		options.setdefault('favor-primary', 'yes')
		options.setdefault('favor-historical', 'yes')
		for o,v in options.iteritems():
			v = v.strip().lower()
			if o in ('favor-primary','favor-historical'):
				if 'yes'.startswith(v):
					v = 'yes'
				elif 'no'.startswith(v):
					v = 'no'
				else:
					return "%s must be 'yes' or 'no'" % o
			else:
				return "unknown option '%s'" % o
			options[o] = v
		return True
	#validateOptions()
	
	
	def download(self, options):
		# download the latest source files
		self.downloadFilesFromFTP('ftp.ncbi.nih.gov', {
			'Homo_sapiens.gene_info.gz':       '/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz',
			'gene2refseq.gz':                  '/gene/DATA/gene2refseq.gz',
			'gene_history.gz':                 '/gene/DATA/gene_history.gz',
			'gene2ensembl.gz':                 '/gene/DATA/gene2ensembl.gz',
			'gene2unigene':                    '/gene/DATA/gene2unigene',
			'gene_refseq_uniprotkb_collab.gz': '/gene/DATA/gene_refseq_uniprotkb_collab.gz',
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
			('locustag_gid', 0),
			('entrez_gid',   0),
			('refseq_gid',   0),
			('refseq_pid',   1),
			('ensembl_gid',  0),
			('ensembl_pid',  1),
			('hgnc_gid',     0),
			('hprd_gid',     0),
			('mim_gid',      0),
			('mirbase_gid',  0),
			('rgd_gid',      0),
			('vega_gid',     0),
			('unigene_gid',  0),
			('uniprot_gid',  0),
			('uniprot_pid',  1),
		])
		utypeID = self.addUTypes([
			('gene',),
		])
		rtypeID = self.addRTypes([
			('composite',), #TODO
		])
		
		nsNames = collections.defaultdict(set)
		numNames = 0
		nsPropVals = collections.defaultdict(set)
		humanEntrez = set()
		humanRefseqP = set()
		labelEntrez = collections.defaultdict(set)
		historyEntrez = collections.defaultdict(set)
		
		# process genes
		self.log("processing genes ...")
		datafile = self.zfile('Homo_sapiens.gene_info.gz') #TODO:context manager,iterator
		header = datafile.next().rstrip()
		if not (
				header.startswith("#Format: tax_id GeneID Symbol LocusTag Synonyms dbXrefs chromosome map_location description type_of_gene Symbol_from_nomenclature_authority Full_name_from_nomenclature_authority Nomenclature_status Other_designations")
				or header.startswith("#tax_id	GeneID	Symbol	LocusTag	Synonyms	dbXrefs	chromosome	map_location	description	type_of_gene	Symbol_from_nomenclature_authority	Full_name_from_nomenclature_authority	Nomenclature_status	Other_designations") # Modification_date
		):
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
			return False
		xrefNS = {
			'Ensembl_G': 'ensembl_gid',
			'Ensembl_T': 'ensembl_gid',
			'Ensembl_P': 'ensembl_pid',
			'HGNC':      'hgnc_gid',
			'HPRD':      'hprd_gid',
			'MIM':       'mim_gid',
			'miRBase':   'mirbase_gid',
			'RGD':       'rgd_gid',
			'Vega':      'vega_gid',
		}
		for line in datafile:
			# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
			if line.startswith("9606\t"):
				words = [ (w.strip() if w != "-" else None) for w in line.split("\t") ]
				entrezGID = words[1]
				label = words[10] or words[2]
				locustag = words[3]
				aliases = [ words[2],words[10] ]
				aliases.extend(words[4].split("|") if words[4] else list())
				xrefs = words[5].split("|") if words[5] else list()
				desc = words[11] or words[8] or (words[13].split("|",1)[0] if words[13] else None)
				genetype = words[9]
				
				if not entrezGID:
					continue
				
				humanEntrez.add(entrezGID)
				labelEntrez[label].add(entrezGID)
				
				# store properties and name references
				nsPropVals[(nsID['entrez_gid'],'utype_id')].add( (entrezGID,utypeID['gene']) )
				if label:
					nsPropVals[(nsID['entrez_gid'],'label')].add( (entrezGID,label) )
				if locustag:
					nsNames[(nsID['entrez_gid'],nsID['locustag_gid'])].add( (entrezGID,locustag) )
				for alias in aliases:
					if alias:
						nsNames[(nsID['entrez_gid'],nsID['symbol'])].add( (entrezGID,alias) )
				for xref in xrefs:
					xrefDB,xrefID = xref.split(":",1)
					# turn ENSG/ENSP/ENST into Ensembl_X
					if xrefDB == "Ensembl" and xrefID.startswith("ENS") and len(xrefID) > 3:
						xrefDB = "Ensembl_%c" % xrefID[3]
					if (xrefDB in xrefNS) and xrefID:
						nsNames[(nsID['entrez_gid'],nsID[xrefNS[xrefDB]])].add( (entrezGID,xrefID) )
				if desc:
					nsPropVals[(nsID['entrez_gid'],'description')].add( (entrezGID,desc) )
			#if taxonomy is 9606 (human)
		#foreach line
		
		# delete any symbol alias which is also the primary label of exactly one other gene
		if options['favor-primary'] == 'yes':
			dupe = set()
			for ref in nsNames[(nsID['entrez_gid'],nsID['symbol'])]:
				entrezGID = ref[0]
				symbol = ref[1]
				if (len(labelEntrez[symbol]) == 1) and (entrezGID not in labelEntrez[symbol]):
					dupe.add(ref)
			nsNames[(nsID['entrez_gid'],nsID['symbol'])] -= dupe
			dupe = None
		#if favor-primary
		
		# print gene stats
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d genes, %d identifiers\n" % (len(humanEntrez),numNames-numNames0))
		
		# process regions
		self.log("processing genomic regions ...")
		datafile = self.zfile('gene2refseq.gz') #TODO:context manager,iterator
		header = datafile.next().rstrip()
		if not (
				header.startswith("#Format: tax_id GeneID status RNA_nucleotide_accession.version RNA_nucleotide_gi protein_accession.version protein_gi genomic_nucleotide_accession.version genomic_nucleotide_gi start_position_on_the_genomic_accession end_position_on_the_genomic_accession orientation assembly") # "(tab is used as a separator, pound sign - start of a comment)"
				or header.startswith("#tax_id	GeneID	status	RNA_nucleotide_accession.version	RNA_nucleotide_gi	protein_accession.version	protein_gi	genomic_nucleotide_accession.version	genomic_nucleotide_gi	start_position_on_the_genomic_accession	end_position_on_the_genomic_accession	orientation	assembly") # "	mature_peptide_accession.version	mature_peptide_gi	Symbol"
		):
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
			return False
		reBuild = re.compile('GRCh([0-9]+)')
		grcBuild = None
		buildEntrez = collections.defaultdict(set)
		buildRegions = collections.defaultdict(set)
		errInvalid = list()
		errNC = list()
		errBuild = list()
		errChr = list()
		errBound = list()
		for line in datafile:
			# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
			if line.startswith("9606\t"):
				words = [ (w.strip() if w != "-" else None) for w in line.split("\t") ]
				entrezGID = words[1]
				if not entrezGID:
					continue
				status = words[2]
				rnaAcc = words[3].rsplit('.',1)[0] if words[3] else None
				proAcc = words[5].rsplit('.',1)[0] if words[5] else None
				genAcc = words[7].rsplit('.',1)[0] if words[7] else None
				posMin = words[9]
				posMax = words[10]
				build = reBuild.search(words[12]) if words[12] else None
				if genAcc in ('NC_001807','NC_012920'):
					chm = self._loki.chr_num['MT']
				elif genAcc and genAcc.startswith('NC_'):
					chm = int(genAcc[3:].lstrip('0'))
				else:
					chm = None
				
				# store name references
				humanEntrez.add(entrezGID)
				if rnaAcc:
					nsNames[(nsID['entrez_gid'],nsID['refseq_gid'])].add( (entrezGID,rnaAcc) )
				if proAcc:
					humanRefseqP.add(proAcc)
					nsNames[(nsID['entrez_gid'],nsID['refseq_pid'])].add( (entrezGID,proAcc) )
				
				# only store validated region boundaries on GRCh builds of whole chromosomes
				# (refseq accession types: http://www.ncbi.nlm.nih.gov/projects/RefSeq/key.html)
				# skip unvalidated IDs
				if status not in ('PREDICTED','REVIEWED','VALIDATED'): #TODO optional
					errInvalid.append(entrezGID)
				elif not (genAcc and genAcc.startswith("NC_")):
					errNC.append(entrezGID)
				elif not build:
					errBuild.append(entrezGID)
				elif not chm:
					errChr.append(entrezGID)
				elif not (posMin and posMax):
					errBound.append(entrezGID)
				else:
					# store the region by build version number, so we can pick the majority build later
					buildEntrez[build.group(1)].add(entrezGID)
					# Entrez sequences use 0-based closed intervals, according to:
					#   ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/README
					#   http://www.ncbi.nlm.nih.gov/books/NBK3840/#genefaq.Representation_of_nucleotide_pos
					# and comparison of web-reported boundary coordinates to gene length (len = end - start + 1).
					# Since LOKI uses 1-based closed intervals, we add 1 to all coordinates.
					buildRegions[build.group(1)].add( (entrezGID,chm,long(posMin)+1,long(posMax)+1) )
				#if errors
			#if taxonomy is 9606 (human)
		#foreach line
		
		# identify majority build version
		grcBuild = max(buildRegions, key=lambda build: len(buildRegions[build]))
		errVers = list()
		for build,regions in buildRegions.iteritems():
			if build != grcBuild:
				errVers.extend(r[0] for r in regions)
		self.setSourceBuilds(grcBuild, None)
		
		# ignore errors for genes which had some regions that made the cut (or got closer at least)
		setCull = set(buildEntrez[grcBuild])
		errBound = [x for x in errBound if (x not in setCull)]
		setCull.update(errBound)
		errChr = [x for x in errChr if (x not in setCull)]
		setCull.update(errChr)
		errVers = [x for x in errVers if (x not in setCull)]
		setCull.update(errVers)
		errBuild = [x for x in errBuild if (x not in setCull)]
		setCull.update(errBuild)
		errNC = [x for x in errNC if (x not in setCull)]
		setCull.update(errNC)
		errInvalid = [x for x in errInvalid if (x not in setCull)]
		#setCull.update(errInvalid)
		setCull = None
		
		# print region stats
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d regions (%d genes), %d identifiers\n" % (len(buildRegions[grcBuild]),len(buildEntrez[grcBuild]),numNames-numNames0))
		self.logPush()
		if errInvalid:
			self.log("WARNING: %d regions (%d genes) unvalidated\n" % (len(errInvalid),len(set(errInvalid))))
		if errNC:
			self.log("WARNING: %d regions (%d genes) not mapped to whole chromosome\n" % (len(errNC),len(set(errNC))))
		if errBuild:
			self.log("WARNING: %d regions (%d genes) not mapped to any GRCh build\n" % (len(errBuild),len(set(errBuild))))
		if errVers:
			self.log("WARNING: %d regions (%d genes) mapped to GRCh build version other than %s\n" % (len(errVers),len(set(errVers)),grcBuild))
		if errChr:
			self.log("WARNING: %d regions (%d genes) on unknown chromosome\n" % (len(errChr),len(set(errChr))))
		if errBound:
			self.log("WARNING: %d regions (%d genes) with unknown boundaries\n" % (len(errBound),len(set(errBound))))
		self.logPop()
		errInvalid = errNC = errBuild = errVers = errChr = errBound = buildEntrez = None
		
		# store regions
		self.log("storing regions ...")
		listR = list(buildRegions[grcBuild])
		listID = self.addTypedRegions(rtypeID['composite'], (r[1:] for r in listR))
		regionNames = itertools.izip(listID, (r[0] for r in listR))
		self.log(" OK: %d regions\n" % (len(listR),))
		
		# store region names
		self.log("storing region identifiers ...")
		self.addRegionNamespacedNames(nsID['entrez_gid'], regionNames)
		self.log(" OK: %d identifiers\n" % (len(listR),))
		buildRegions = listR = listID = regionNames = None
		
		# process historical gene identifiers
		self.log("processing historical Entrez identifiers ...")
		datafile = self.zfile('gene_history.gz') #TODO:context manager,iterator
		header = datafile.next().rstrip()
		if not (
				header.startswith("#Format: tax_id GeneID Discontinued_GeneID Discontinued_Symbol") # "Discontinue_Date (tab is used as a separator, pound sign - start of a comment)"
				or header.startswith("#tax_id	GeneID	Discontinued_GeneID	Discontinued_Symbol") #	"Discontinue_Date"
		):
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
			return False
		for line in datafile:
			# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
			if line.startswith("9606\t"):
				words = [ (w.strip() if w != "-" else None) for w in line.split("\t") ]
				entrezGID = words[1]
				oldEntrezGID = words[2]
				oldSymbol = words[3]
				
				if not entrezGID:
					continue
				
				humanEntrez.add(entrezGID)
				if oldEntrezGID:
					humanEntrez.add(oldEntrezGID)
					nsNames[(nsID['entrez_gid'],nsID['entrez_gid'])].add( (entrezGID,oldEntrezGID) )
				if oldSymbol and (oldSymbol not in labelEntrez):
					historyEntrez[oldSymbol].add(entrezGID)
					nsNames[(nsID['entrez_gid'],nsID['symbol'])].add( (entrezGID,oldSymbol) )
			#if taxonomy is 9606 (human)
		#foreach line
		
		# delete any symbol alias which is also the historical symbol of exactly one other gene
		if options['favor-historical'] == 'yes':
			dupe = set()
			for ref in nsNames[(nsID['entrez_gid'],nsID['symbol'])]:
				entrezGID = ref[0]
				symbol = ref[1]
				if (len(historyEntrez[symbol]) == 1) and (entrezGID not in historyEntrez[symbol]):
					dupe.add(ref)
			nsNames[(nsID['entrez_gid'],nsID['symbol'])] -= dupe
			dupe = None
		#if favor-historical
		
		# print historical identifier stats
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d identifiers\n" % (numNames-numNames0,))
		
		# process ensembl identifiers
		self.log("processing Ensembl identifiers ...")
		datafile = self.zfile('gene2ensembl.gz') #TODO:context manager,iterator
		header = datafile.next().rstrip()
		if not (
				header.startswith("#Format: tax_id GeneID Ensembl_gene_identifier RNA_nucleotide_accession.version Ensembl_rna_identifier protein_accession.version Ensembl_protein_identifier") # "(tab is used as a separator, pound sign - start of a comment)"
				or header.startswith("#tax_id	GeneID	Ensembl_gene_identifier	RNA_nucleotide_accession.version	Ensembl_rna_identifier	protein_accession.version	Ensembl_protein_identifier")
		):
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
			return False
		for line in datafile:
			# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
			if line.startswith("9606\t"):
				words = [ (w.strip() if w != "-" else None) for w in line.split("\t") ]
				entrezGID = words[1]
				ensemblG = words[2]
				ensemblT = words[4]
				ensemblP = words[6]
				
				if not entrezGID:
					continue
				
				humanEntrez.add(entrezGID)
				if ensemblG:
					nsNames[(nsID['entrez_gid'],nsID['ensembl_gid'])].add( (entrezGID,ensemblG) )
				if ensemblT:
					nsNames[(nsID['entrez_gid'],nsID['ensembl_gid'])].add( (entrezGID,ensemblT) )
				if ensemblP:
					nsNames[(nsID['entrez_gid'],nsID['ensembl_pid'])].add( (entrezGID,ensemblP) )
			#if taxonomy is 9606 (human)
		#foreach line
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d identifiers\n" % (numNames-numNames0,))
		
		# process unigene identifiers
		self.log("processing Unigene identifiers ...")
		with open('gene2unigene','rU') as datafile:
			header = datafile.next().rstrip()
			if not (
					header.startswith("#Format: GeneID UniGene_cluster") # "(tab is used as a separator, pound sign - start of a comment)"
					or header.startswith("#GeneID	UniGene_cluster")
			):
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
				return False
			for line in datafile:
				words = [ (w.strip() if w != "-" else None) for w in line.split("\t") ]
				entrezGID = words[0]
				unigeneGID = words[1]
				
				# there will be lots of extraneous mappings for genes of other species
				if (entrezGID in humanEntrez) and unigeneGID:
					nsNames[(nsID['entrez_gid'],nsID['unigene_gid'])].add( (entrezGID,unigeneGID) )
			#foreach line
		#with datafile
		numNames0 = numNames
		numNames = sum(len(names) for names in nsNames.itervalues())
		self.log(" OK: %d identifiers\n" % (numNames-numNames0,))
		
		# process uniprot identifiers
		self.log("processing Uniprot identifiers ...")
		datafile = self.zfile('gene_refseq_uniprotkb_collab.gz') #TODO:context manager,iterator
		header = datafile.next().rstrip()
		if not (
				header.startswith("#Format: NCBI_protein_accession UniProtKB_protein_accession") # "(tab is used as a separator, pound sign - start of a comment)"
				or header.startswith("#NCBI_protein_accession	UniProtKB_protein_accession")
		):
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
			return False
		for line in datafile:
			words = [ (w.strip() if w != "-" else None) for w in line.split("\t") ]
			proAcc = words[0].rsplit('.',1)[0] if words[0] else None
			uniprotPID = words[1]
			
			# there will be lots of extraneous mappings for proteins of other species
			if (proAcc in humanRefseqP) and uniprotPID:
				nsNames[(nsID['refseq_pid'],nsID['uniprot_pid'])].add( (proAcc,uniprotPID) )
		#foreach line
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
	
#Source_entrez
