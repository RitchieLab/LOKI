#!/usr/bin/env python

import collections
import re
from loki import loki_source


class Source_entrez(loki_source.Source):
	
	
	##################################################
	# private instance methods
	
	
	def _identifyLatestAssembly(self, filenames):
		reFile = re.compile('^GCF_[^_]+_[^0-9]+([0-9]+)(?:\\.p([0-9])+)?.*$', re.IGNORECASE)
		bestvers = bestpatch = 0
		bestasm = None
		for filename in filenames:
			match = reFile.match(filename)
			if match:
				filevers = int(match.group(1))
				filepatch = int(match.group(2) or 0)
				if (filevers > bestvers) or ((filevers == bestvers and filepatch > bestpatch)):
					bestvers = filevers
					bestpatch = filepatch
					bestasm = match.group(0)
		#foreach file in path
		return bestasm
	#_identifyLatestAssembly()
	
	
	##################################################
	# source interface
	
	
	@classmethod
	def getVersionString(cls):
		return '2.3 (2016-04-25)'
	#getVersionString()
	
	
	@classmethod
	def getSpecies(cls):
		return [3702,559292,6239,7227,7955,9606,10090,10116,208964] # ,4932,
	#getSpecies()
	
	
	@classmethod
	def getOptions(cls):
		return {
			'locus-tags'    : "[yes|no]  --  include a gene's 'Locus Tag' as an alias (default: yes)",
			'favor-primary' : "[yes|no]  --  reduce symbol ambiguity by favoring primary symbols (default: yes)",
			'favor-hist'    : "[yes|no]  --  reduce symbol ambiguity by favoring primary symbols (default: yes)",
		}
	#getOptions()
	
	
	def validateOptions(self, options):
		for o,v in options.iteritems():
			v = v.strip().lower()
			if o in ('locus-tags','favor-primary','favor-hist'):
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
		# define a callback to identify the latest sequence version
		def remFilesCallback(ftp):
			if self._tax_id == 3702:
				species = 'Arabidopsis_thaliana'
				genepath = 'Plants/'+species
				seqpath = 'plant/'+species
			elif self._tax_id == 559292 or self._tax_id == 4932:
				species = 'Saccharomyces_cerevisiae'
				genepath = 'Fungi/'+species
				seqpath = 'fungi/'+species
			elif self._tax_id == 6239:
				species = 'Caenorhabditis_elegans'
				genepath = 'Invertebrates/'+species
				seqpath = 'invertebrate/'+species
			elif self._tax_id == 7227:
				species = 'Drosophila_melanogaster'
				genepath = 'Invertebrates/'+species
				seqpath = 'invertebrate/'+species
			elif self._tax_id == 7955:
				species = 'Danio_rerio'
				genepath = 'Non-mammalian_vertebrates/'+species
				seqpath = 'vertebrate_other/'+species
			elif self._tax_id == 10090:
				species = 'Mus_musculus'
				genepath = 'Mammalia/'+species
				seqpath = 'vertebrate_mammalian/'+species
			elif self._tax_id == 10116:
				species = 'Rattus_norvegicus'
				genepath = 'Mammalia/'+species
				seqpath = 'vertebrate_mammalian/'+species
			elif self._tax_id == 208964:
				species = 'Pseudomonas_aeruginosa'
				genepath = 'Archaea_Bacteria/'+species+'_PAO1'
				seqpath = 'bacteria/'+species
			else: # 9606
				species = 'Homo_sapiens'
				genepath = 'Mammalia/'+species
				seqpath = 'vertebrate_mammalian/'+species
			#if _tax_id
			remFiles = {
				(species+'.gene_info.gz'):         ('/gene/DATA/GENE_INFO/'+genepath+'.gene_info.gz'),
				'gene2refseq.gz':                  '/gene/DATA/gene2refseq.gz',
				'gene_history.gz':                 '/gene/DATA/gene_history.gz',
				'gene2ensembl.gz':                 '/gene/DATA/gene2ensembl.gz',
				'gene2unigene':                    '/gene/DATA/gene2unigene',
				'gene_refseq_uniprotkb_collab.gz': '/gene/DATA/gene_refseq_uniprotkb_collab.gz',
			}
			path = '/genomes/refseq/'+seqpath+('/representative' if (self._tax_id == 10116) else '/reference')
			ftp.cwd(path)
			bestasm = self._identifyLatestAssembly(ftp.nlst())
			if bestasm:
				remFiles[species+'.assembly_report.txt'] = '%s/%s/%s_assembly_report.txt' % (path,bestasm,bestasm)
			
			return remFiles
		#remFilesCallback
		
		# download the latest source files
		self.downloadFilesFromFTP('ftp.ncbi.nih.gov', remFilesCallback)
		if self._tax_id == 3702:
			species = 'ARATH_3702'
		elif self._tax_id == 559292 or self._tax_id == 4932:
			species = 'YEAST_559292'
		elif self._tax_id == 6239:
			species = 'CAEEL_6239'
		elif self._tax_id == 7227:
			species = 'DROME_7227'
		elif self._tax_id == 7955:
			species = 'DANRE_7955'
		elif self._tax_id == 10090:
			species = 'MOUSE_10090'
		elif self._tax_id == 10116:
			species = 'RAT_10116'
		elif self._tax_id == 208964:
			return #TODO
		else: # 9606
			species = 'HUMAN_9606'
		#if _tax_id
		remFiles = {
			(species+'_idmapping_selected.tab.gz') : ('/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/'+species+'_idmapping_selected.tab.gz'),
		}
		self.downloadFilesFromFTP('ftp.uniprot.org', remFiles)
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		ldprofileID = self.addLDProfiles([
			('', 'no LD adjustment', None, None),
		])
		namespaceID = self.addNamespaces([
			('symbol',      0),
			('entrez_gid',  0),
			('refseq_gid',  0),
			('refseq_pid',  1),
			('ensembl_gid', 0),
			('ensembl_pid', 1),
			('hgnc_id',     0),
			('mim_id',      0),
			('hprd_id',     0),
			('vega_id',     0),
			('rgd_id',      0),
			('mirbase_id',  0),
			('unigene_gid', 0),
			('uniprot_gid', 0),
			('uniprot_pid', 1),
			('mgi_id',      0),
			('tair_id',     0),
			('sgd_id',      0),
			('flybase_id',  0),
			('zfin_id',     0),
			('pseudocap_id',0),
		])
		typeID = self.addTypes([
			('gene',),
		])
		if self._tax_id == 3702:
			species = 'Arabidopsis_thaliana'
		elif self._tax_id == 559292 or self._tax_id == 4932:
			species = 'Saccharomyces_cerevisiae'
		elif self._tax_id == 6239:
			species = 'Caenorhabditis_elegans'
		elif self._tax_id == 7227:
			species = 'Drosophila_melanogaster'
		elif self._tax_id == 7955:
			species = 'Danio_rerio'
		elif self._tax_id == 10090:
			species = 'Mus_musculus'
		elif self._tax_id == 10116:
			species = 'Rattus_norvegicus'
		elif self._tax_id == 208964:
			species = 'Pseudomonas_aeruginosa'
		else: # 9606
			species = 'Homo_sapiens'
		#if _tax_id
		nsNames = { ns:set() for ns in namespaceID }
		nsNameNames = { ns:set() for ns in namespaceID }
		numNames = numNameNames = numNameRefs = 0
		
		# process assembly chromosome report
		self.log("processing assembled chromosomes ...")
		romanNum = {
			'I'  :1,  'II'  :2,  'III'  :3,  'IV'  :4,  'V'  :5,  'VI'  :6,  'VII'  :7,  'VIII'  :8,  'IX'  :9,  'X'  :10,
			'XI' :11, 'XII' :12, 'XIII' :13, 'XIV' :14, 'XV' :15, 'XVI' :16, 'XVII' :17, 'XVIII' :18, 'XIX' :19, 'XX' :20,
			'XXI':21, 'XXII':22, 'XXIII':23, 'XXIV':24, 'XXV':25, 'XXVI':26, 'XXVII':27, 'XXVIII':28, 'XXIX':29, 'XXX':30,
		}
		accChm = dict()
		asmFile = open(species+'.assembly_report.txt','rU')
		for line in asmFile:
			if line.startswith("#"):
				header = line
			elif not header.startswith("# Sequence-Name	Sequence-Role	Assigned-Molecule	Assigned-Molecule-Location/Type	GenBank-Accn	Relationship	RefSeq-Accn"):
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
			else:
				words = line.split("\t")
				if words[1] == "assembled-molecule":
					chm = words[2].strip()
					if self._tax_id in (4932,559292,6239) and chm in romanNum:
						chm = romanNum[chm]
					accChm[words[6].split('.')[0].strip()] = chm
			#if header
		#foreach line
		if len(accChm) == 1:
			for chm in accChm:
				accChm[chm] = 1
		self.log(" OK: %d chromosomes\n" % (len(accChm),))
	#	print accChm
		
		# process genes (no header!)
		self.log("processing genes ...")
		entrezGene = dict()
		entrezChm = dict()
		primaryEntrez = dict()
		xrefNS = {
			'Ensembl_G': 'ensembl_gid',
			'Ensembl_T': 'ensembl_gid',
			'Ensembl_P': 'ensembl_pid',
			'HGNC':      'hgnc_id',
			'MIM':       'mim_id',
			'HPRD':      'hprd_id',
			'Vega':      'vega_id',
			'RGD':       'rgd_id',
			'miRBase':   'mirbase_id',
			'MGI':       'mgi_id',
			'TAIR':      'tair_id',
			'SGD':       'sgd_id',
			'FLYBASE':   'flybase_id',
			'ZFIN':      'zfin_id',
			'PseudoCap': 'pseudocap_id',
		}
		reENSP = re.compile('^ENS[A-Z]*P[0-9]+', re.IGNORECASE)
		geneFile = self.zfile(species+'.gene_info.gz') #TODO:context manager,iterator
		for line in geneFile:
			# quickly filter out all other taxonomies before taking the time to split()
			if line.startswith(str(self._tax_id) + "\t"):
				words = line.rstrip().split("\t")
				entrezID = int(words[1])
				symbol = words[2]
				aliases = words[4].split("|") if words[4] != "-" else list()
				if options.get('locus-tags','yes') == 'yes' and words[3] != "-":
					aliases.append(words[3])
				xrefs = words[5].split("|") if words[5] != "-" else list()
				chms = words[6]
				desc = words[8]
				
				entrezGene[entrezID] = (symbol,desc)
				if self._tax_id in (4932,559292,6239):
					entrezChm[entrezID] = set( romanNum.get(chm,self._loki.chr_num.get(chm)) for chm in chms.split('|') if chm != '-' )
				else:
					entrezChm[entrezID] = set( self._loki.chr_num.get(chm) for chm in chms.split('|') if chm != '-' )
				if symbol not in primaryEntrez:
					primaryEntrez[symbol] = entrezID
				elif primaryEntrez[symbol] != entrezID:
					primaryEntrez[symbol] = False
				
				# entrezID as a name for itself looks funny here, but later on
				# we'll be translating the target entrezID to biopolymer_id and
				# adding more historical entrezID aliases
				nsNames['entrez_gid'].add( (entrezID,entrezID) )
				nsNames['symbol'].add( (entrezID,symbol) )
				for alias in aliases:
					nsNames['symbol'].add( (entrezID,alias.replace('\\','_')) )
				for xref in xrefs:
					xrefDB,xrefID = xref.split(":",1)
					# turn ENSG/ENSP/ENST into Ensembl_X
					if xrefDB == "Ensembl":
						if reENSP.match(xrefID):
							xrefDB = 'Ensembl_P'
						else:
							xrefDB = 'Ensembl_G'
					if xrefDB in xrefNS:
						nsNames[xrefNS[xrefDB]].add( (entrezID,xrefID) )
			#if taxonomy
		#foreach line in geneFile
		
		# delete any symbol alias which is also the primary name of exactly one other gene
		if options.get('favor-primary','yes') == 'yes':
			dupe = set()
			for alias in nsNames['symbol']:
				entrezID = alias[0]
				symbol = alias[1]
				if (symbol in primaryEntrez) and (primaryEntrez[symbol] != False) and (primaryEntrez[symbol] != entrezID):
					dupe.add(alias)
			nsNames['symbol'] -= dupe
			dupe = None
		#if favor-primary
		
		# print stats
		numGenes = len(entrezGene)
		numNames0 = numNames
		numNames = sum(len(nsNames[ns]) for ns in nsNames)
		self.log(" OK: %d genes, %d identifiers\n" % (numGenes,numNames-numNames0))
		
		# store genes
		self.log("writing genes to the database ...")
		listEntrez = entrezGene.keys()
		listBID = self.addTypedBiopolymers(typeID['gene'], (entrezGene[entrezID] for entrezID in listEntrez))
		entrezBID = dict(zip(listEntrez,listBID))
		numGenes = len(entrezBID)
		self.log(" OK: %d genes\n" % (numGenes))
		entrezGene = None
		
		# translate target entrezID to biopolymer_id in nsNames
		for ns in nsNames:
			names = set( (entrezBID[name[0]],name[1]) for name in nsNames[ns] if name[0] in entrezBID )
			nsNames[ns] = names
		numNames = sum(len(nsNames[ns]) for ns in nsNames)
		
		# process gene regions
		# Entrez sequences use 0-based closed intervals, according to:
		#   http://www.ncbi.nlm.nih.gov/books/NBK3840/#genefaq.Representation_of_nucleotide_pos
		# and comparison of web-reported boundary coordinates to gene length (len = end - start + 1).
		# Since LOKI uses 1-based closed intervals, we add 1 to all coordinates.
		self.log("processing gene regions ...")
		reBuild = re.compile('Reference [^0-9]+([0-9]+)')
		buildGenes = collections.defaultdict(set)
		buildRegions = collections.defaultdict(set)
		setOrphan = set()
		setBadNC = set()
		setBadBuild = set()
		setBadChr = set()
		refseqBIDs = collections.defaultdict(set)
		regionFile = self.zfile('gene2refseq.gz') #TODO:context manager,iterator
		header = regionFile.next().rstrip()
		if not header.startswith("#Format: tax_id GeneID status RNA_nucleotide_accession.version RNA_nucleotide_gi protein_accession.version protein_gi genomic_nucleotide_accession.version genomic_nucleotide_gi start_position_on_the_genomic_accession end_position_on_the_genomic_accession orientation assembly"): # "(tab is used as a separator, pound sign - start of a comment)"
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
		else:
			for line in regionFile:
				# quickly filter out all other taxonomies before taking the time to split()
				if not line.startswith(str(self._tax_id) + "\t"):
					continue
				
				# grab relevant columns
				words = line.split("\t")
				entrezID = int(words[1])
				rnaAcc = words[3].rsplit('.',1)[0] if words[3] != "-" else None
				proAcc = words[5].rsplit('.',1)[0] if words[5] != "-" else None
				genAcc = words[7].rsplit('.',1)[0] if words[7] != "-" else None
				posMin = (long(words[9])+1) if words[9] != "-" else None
				posMax = (long(words[10])+1) if words[10] != "-" else None
				build = reBuild.search(words[12].rstrip() if (len(words) > 12 and words[12] != "-") else '')
				build = build.group(1) if build else 0
				
				# skip unrecognized IDs
				if entrezID not in entrezBID:
					setOrphan.add(entrezID)
					continue
				
				# store rna and protein sequence RefSeq IDs
				# (don't store genAcc, there's only one per chromosome)
				if rnaAcc:
					nsNames['refseq_gid'].add( (entrezBID[entrezID],rnaAcc) )
				if proAcc:
					nsNames['refseq_pid'].add( (entrezBID[entrezID],proAcc) )
					refseqBIDs[proAcc].add(entrezBID[entrezID])
				
				# skip non-whole-chromosome regions
				# (refseq accession types: http://www.ncbi.nlm.nih.gov/RefSeq/key.html)
				if not (genAcc and genAcc.startswith('NC_')):
					setBadNC.add(entrezID)
					continue
				elif self._tax_id in (7955,9606,10090,10116) and not build:
					setBadBuild.add(entrezID)
					continue
				
				# skip chromosome mismatches
				chm = self._loki.chr_num.get(accChm.get(genAcc))
				if not chm:
				#	print "genAcc",genAcc,"->",accChm.get(genAcc),"->",chm
					setBadChr.add(entrezID)
					continue
				elif (entrezID in entrezChm) and entrezChm[entrezID] and (chm not in entrezChm[entrezID]):
					# TODO: make sure we want to ignore any gene region with an ambiguous chromosome
					#       (i.e. gene_info says one thing, gene2refseq says another)
				#	print "#",entrezID,"genAcc",genAcc,"->",accChm.get(genAcc),"->",chm,"!=",entrezChm.get(entrezID)
					setBadChr.add(entrezID)
					continue
				
				# store the region by build version number, so we can pick the majority build later
				buildGenes[build].add(entrezID)
				buildRegions[build].add( (entrezBID[entrezID],chm,posMin,posMax) )
			#foreach line in regionFile
			
			# identify majority build version
			grcBuild = max(buildRegions, key=lambda build: len(buildRegions[build])) if buildRegions else 0
			setBadVers = set()
			for build,genes in buildGenes.iteritems():
				if build != grcBuild:
					setBadVers.update(genes)
			
			# print stats
			setBadVers.difference_update(buildGenes[grcBuild])
			setBadChr.difference_update(buildGenes[grcBuild], setBadVers)
			setBadBuild.difference_update(buildGenes[grcBuild], setBadVers, setBadChr)
			setBadNC.difference_update(buildGenes[grcBuild], setBadVers, setBadChr, setBadNC)
			numRegions = len(buildRegions[grcBuild])
			numGenes = len(buildGenes[grcBuild])
			numNames0 = numNames
			numNames = sum(len(nsNames[ns]) for ns in nsNames)
			self.log(" OK: %d regions (%d genes), %d identifiers\n" % (numRegions,numGenes,numNames-numNames0))
			self.logPush()
			if setOrphan:
				self.log("WARNING: %d regions for undefnied EntrezIDs\n" % (len(setOrphan)))
			if setBadNC:
				self.log("WARNING: %d genes not mapped to whole chromosome\n" % (len(setBadNC)))
			if setBadBuild:
				self.log("WARNING: %d genes not mapped to reference build\n" % (len(setBadBuild)))
			if setBadVers:
				self.log("WARNING: %d genes mapped to reference build version other than %s\n" % (len(setBadVers),grcBuild))
			if setBadChr:
				self.log("WARNING: %d genes on mismatching chromosome\n" % (len(setBadChr)))
			self.logPop()
			entrezChm = setOrphan = setBadNC = setBadBuild = setBadChr = setBadVers = buildGenes = None
			
			# store gene regions
			self.log("writing gene regions to the database ...")
			numRegions = len(buildRegions[grcBuild])
			self.addBiopolymerLDProfileRegions(ldprofileID[''], buildRegions[grcBuild])
			self.log(" OK: %d regions\n" % (numRegions))
			buildRegions = None
		#if gene regions header ok
		
		# process historical gene names
		self.log("processing historical gene names ...")
		entrezUpdate = {}
		historyEntrez = {}
		histFile = self.zfile('gene_history.gz') #TODO:context manager,iterator
		header = histFile.next().rstrip()
		if not header.startswith("#Format: tax_id GeneID Discontinued_GeneID Discontinued_Symbol"): # "Discontinue_Date (tab is used as a separator, pound sign - start of a comment)"
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
		else:
			for line in histFile:
				# quickly filter out all other taxonomies before taking the time to split()
				if not line.startswith(str(self._tax_id) + "\t"):
					continue
				
				words = line.split("\t")
				entrezID = int(words[1]) if words[1] != "-" else None
				oldEntrez = int(words[2]) if words[2] != "-" else None
				oldName = words[3] if words[3] != "-" else None
				
				if entrezID and entrezID in entrezBID:
					if oldEntrez and oldEntrez != entrezID:
						entrezUpdate[oldEntrez] = entrezID
						nsNames['entrez_gid'].add( (entrezBID[entrezID],oldEntrez) )
					if oldName and (oldName not in primaryEntrez or primaryEntrez[oldName] == False):
						if oldName not in historyEntrez:
							historyEntrez[oldName] = entrezID
						elif historyEntrez[oldName] != entrezID:
							historyEntrez[oldName] = False
						nsNames['symbol'].add( (entrezBID[entrezID],oldName) )
			#foreach line in histFile
			
			# delete any symbol alias which is also the historical name of exactly one other gene
			if options.get('favor-hist','yes') == 'yes':
				dupe = set()
				for alias in nsNames['symbol']:
					entrezID = alias[0]
					symbol = alias[1]
					if (symbol in historyEntrez) and (historyEntrez[symbol] != False) and (historyEntrez[symbol] != entrezID):
						dupe.add(alias)
				nsNames['symbol'] -= dupe
				dupe = None
			#if favor-hist
			
			# print stats
			numNames0 = numNames
			numNames = sum(len(nsNames[ns]) for ns in nsNames)
			self.log(" OK: %d identifiers\n" % (numNames-numNames0))
		#if historical name header ok
		
		# process ensembl gene names
		self.log("processing ensembl gene names ...")
		ensFile = self.zfile('gene2ensembl.gz') #TODO:context manager,iterator
		header = ensFile.next().rstrip()
		if not header.startswith("#Format: tax_id GeneID Ensembl_gene_identifier RNA_nucleotide_accession.version Ensembl_rna_identifier protein_accession.version Ensembl_protein_identifier"): # "(tab is used as a separator, pound sign - start of a comment)"
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
		else:
			for line in ensFile:
				# quickly filter out all other taxonomies before taking the time to split()
				if not line.startswith(str(self._tax_id) + "\t"):
					continue
				
				words = line.split("\t")
				entrezID = int(words[1])
				ensemblG = words[2] if words[2] != "-" else None
				ensemblT = words[4] if words[4] != "-" else None
				ensemblP = words[6] if words[6] != "-" else None
				
				if ensemblG or ensemblT or ensemblP:
					while entrezID and (entrezID in entrezUpdate):
						entrezID = entrezUpdate[entrezID]
					
					if entrezID and (entrezID in entrezBID):
						if ensemblG:
							nsNames['ensembl_gid'].add( (entrezBID[entrezID],ensemblG) )
						if ensemblT:
							nsNames['ensembl_gid'].add( (entrezBID[entrezID],ensemblT) )
						if ensemblP:
							nsNames['ensembl_pid'].add( (entrezBID[entrezID],ensemblP) )
			#foreach line in ensFile
			
			# print stats
			numNames0 = numNames
			numNames = sum(len(nsNames[ns]) for ns in nsNames)
			self.log(" OK: %d identifiers\n" % (numNames-numNames0))
		#if ensembl name header ok
		
		# process unigene gene names
		self.log("processing unigene gene names ...")
		with open('gene2unigene','rU') as ugFile:
			header = ugFile.next().rstrip()
			if not header.startswith("#Format: GeneID UniGene_cluster"): # "(tab is used as a separator, pound sign - start of a comment)"
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
			else:
				for line in ugFile:
					words = line.rstrip().split("\t")
					entrezID = int(words[0]) if words[0] != "-" else None
					unigeneID = words[1] if words[1] != "-" else None
					
					while entrezID and (entrezID in entrezUpdate):
						entrezID = entrezUpdate[entrezID]
					
					# there will be lots of extraneous mappings for genes of other species
					if entrezID and (entrezID in entrezBID) and unigeneID:
						nsNames['unigene_gid'].add( (entrezBID[entrezID],unigeneID) )
				#foreach line in ugFile
				
				# print stats
				numNames0 = numNames
				numNames = sum(len(nsNames[ns]) for ns in nsNames)
				self.log(" OK: %d identifiers\n" % (numNames-numNames0))
			#if unigene name header ok
		#with ugFile
		
		if True:
			# process uniprot gene names from entrez
			self.log("processing uniprot gene names ...")
			upFile = self.zfile('gene_refseq_uniprotkb_collab.gz') #TODO:context manager,iterator
			header = upFile.next().rstrip()
			if not header.startswith("#Format: NCBI_protein_accession UniProtKB_protein_accession"): # "(tab is used as a separator, pound sign - start of a comment)"
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
			else:
				for line in upFile:
					words = line.split("\t")
					proteinAcc = words[0].rsplit('.',1)[0] if words[0] != "-" else None
					uniprotAcc = words[1] if words[1] != "-" else None
					
					# there will be tons of identifiers missing from refseqBIDs because they're non-human
					if proteinAcc and (proteinAcc in refseqBIDs) and uniprotAcc:
						for biopolymerID in refseqBIDs[proteinAcc]:
							nsNames['uniprot_pid'].add( (biopolymerID,uniprotAcc) )
				#foreach line in upFile
					
				# print stats
				numNames0 = numNames
				numNames = sum(len(nsNames[ns]) for ns in nsNames)
				self.log(" OK: %d identifiers\n" % (numNames-numNames0))
			#if header ok
		else:
			# process uniprot gene names from uniprot (no header!)
			if self._tax_id == 3702:
				species = 'ARATH_3702'
			elif self._tax_id == 559292 or self._tax_id == 4932:
				species = 'YEAST_559292'
			elif self._tax_id == 6239:
				species = 'CAEEL_6239'
			elif self._tax_id == 7227:
				species = 'DROME_7227'
			elif self._tax_id == 7955:
				species = 'DANRE_7955'
			elif self._tax_id == 10090:
				species = 'MOUSE_10090'
			elif self._tax_id == 10116:
				species = 'RAT_10116'
			elif self._tax_id == 208964:
				species = '' #TODO
			else: # 9606
				species = 'HUMAN_9606'
			#if _tax_id
			upFile = self.zfile(species+'_idmapping_selected.tab.gz') #TODO:context manager,iterator
			self.log("processing uniprot gene names ...")
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
				found = False
				for word2 in words[2].split(';'):
					entrezID = int(word2.strip()) if word2 else None
					if entrezID and (entrezID in entrezBID):
						nsNameNames['uniprot_pid'].add( (namespaceID['entrez_gid'],entrezID,uniprotAcc) )
						nsNameNames['uniprot_gid'].add( (namespaceID['entrez_gid'],entrezID,uniprotID) )
						found = True
				#foreach entrezID mapping
				if not found:
					for word3 in words[3].split(';'):
						refseqID = word3.strip().split('.',1)[0] if word3 else None
						if refseqID:
							nsNameNames['uniprot_pid'].add( (namespaceID['refseq_pid'],refseqID,uniprotAcc) )
							nsNameNames['uniprot_pid'].add( (namespaceID['refseq_gid'],refseqID,uniprotAcc) )
							nsNameNames['uniprot_gid'].add( (namespaceID['refseq_pid'],refseqID,uniprotID) )
							nsNameNames['uniprot_gid'].add( (namespaceID['refseq_gid'],refseqID,uniprotID) )
					#foreach refseq mapping
					for word14 in words[14].split(';'):
						mimID = word14.strip() if word14 else None
						if mimID:
							nsNameNames['uniprot_pid'].add( (namespaceID['mim_id'],mimID,uniprotAcc) )
							nsNameNames['uniprot_gid'].add( (namespaceID['mim_id'],mimID,uniprotID) )
					#foreach mim mapping
					for word15 in words[15].split(';'):
						unigeneID = word15.strip() if word15 else None
						if unigeneID:
							nsNameNames['uniprot_pid'].add( (namespaceID['unigene_gid'],unigeneID,uniprotAcc) )
							nsNameNames['uniprot_gid'].add( (namespaceID['unigene_gid'],unigeneID,uniprotID) )
					#foreach mim mapping
					for word19 in words[19].split(';'):
						ensemblGID = word19.strip() if word19 else None
						if ensemblGID:
							nsNameNames['uniprot_pid'].add( (namespaceID['ensembl_gid'],ensemblGID,uniprotAcc) )
							nsNameNames['uniprot_gid'].add( (namespaceID['ensembl_gid'],ensemblGID,uniprotID) )
					#foreach ensG mapping
					for word20 in words[20].split(';'):
						ensemblTID = word20.strip() if word20 else None
						if ensemblTID:
							nsNameNames['uniprot_pid'].add( (namespaceID['ensembl_gid'],ensemblTID,uniprotAcc) )
							nsNameNames['uniprot_gid'].add( (namespaceID['ensembl_gid'],ensemblTID,uniprotID) )
					#foreach ensT mapping
					for word21 in words[21].split(';'):
						ensemblPID = word21.strip() if word21 else None
						if ensemblPID:
							nsNameNames['uniprot_pid'].add( (namespaceID['ensembl_pid'],ensemblPID,uniprotAcc) )
							nsNameNames['uniprot_gid'].add( (namespaceID['ensembl_pid'],ensemblPID,uniprotID) )
					#foreach ensP mapping
				#if no entrezID match
			#foreach line in upFile
			
			# print stats
			numNames0 = numNames
			numNames = sum(len(nsNames[ns]) for ns in nsNames)
			numNameNames0 = numNameNames
			numNameNames = sum(len(set(n[2] for n in nsNameNames[ns])) for ns in nsNameNames)
			numNameRefs0 = numNameRefs
			numNameRefs = sum(len(nsNameNames[ns]) for ns in nsNameNames)
			self.log(" OK: %d identifiers (%d references)\n" % (numNames-numNames0+numNameNames-numNameNames0,numNameRefs-numNameRefs0))
		#switch uniprot source
		
		# store gene names
		self.log("writing gene identifiers to the database ...")
		numNames = 0
		for ns in nsNames:
			if nsNames[ns]:
				numNames += len(nsNames[ns])
				self.addBiopolymerNamespacedNames(namespaceID[ns], nsNames[ns])
		self.log(" OK: %d identifiers\n" % (numNames,))
		nsNames = None
		
		# store gene names
		if sum(len(nn) for nn in nsNameNames.itervalues()) > 0:
			self.log("writing gene identifier references to the database ...")
			numNameNames = 0
			for ns in nsNameNames:
				if nsNameNames[ns]:
					numNameNames += len(nsNameNames[ns])
					self.addBiopolymerTypedNameNamespacedNames(typeID['gene'], namespaceID[ns], nsNameNames[ns])
			self.log(" OK: %d references\n" % (numNameNames,))
			nsNameNames = None
		#if nsNameNames
		
		# store source metadata
		self.setSourceBuilds(grcBuild, None)
	#update()
	
#Source_entrez
