#!/usr/bin/env python

import loki_source


class Source_entrez(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromFTP('ftp.ncbi.nih.gov', {
			'Homo_sapiens.gene_info.gz':       '/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz',
			'gene2refseq.gz':                  '/gene/DATA/gene2refseq.gz',
			'gene_history.gz':                 '/gene/DATA/gene_history.gz',
			'gene2ensembl.gz':                 '/gene/DATA/gene2ensembl.gz',
			'gene2unigene':                    '/gene/DATA/gene2unigene',
			'gene_refseq_uniprotkb_collab.gz': '/gene/DATA/gene_refseq_uniprotkb_collab.gz',
		})
		self.downloadFilesFromFTP('ftp.uniprot.org', {
			'HUMAN_9606_idmapping_selected.tab.gz': '/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/HUMAN_9606_idmapping_selected.tab.gz',
		})
	#download()
	
	
	def update(self):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
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
		])
		populationID = self.addPopulations([
			('n/a', 'no LD adjustment', None),
		])
		typeID = self.addTypes([
			('gene',),
		])
		
		nsNames = { ns:set() for ns in namespaceID }
		nsNameNames = { ns:set() for ns in namespaceID }
		numNames = numNameNames = numNameRefs = 0
		empty = tuple()
		
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
		}
		geneFile = self.zfile('Homo_sapiens.gene_info.gz') #TODO:context manager,iterator
		for line in geneFile:
			# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
			if line.startswith("9606\t"):
				words = line.rstrip().split("\t")
				entrezID = int(words[1])
				symbol = words[2]
				aliases = words[4].split("|") if words[4] != "-" else empty
				xrefs = words[5].split("|") if words[5] != "-" else empty
				chm = words[6]
				desc = words[8]
				
				entrezGene[entrezID] = (symbol,desc)
				entrezChm[entrezID] = chm
				if symbol not in primaryEntrez:
					primaryEntrez[symbol] = entrezID
				elif primaryEntrez[symbol] != entrezID:
					primaryEntrez[symbol] = False
				
				# entrezID as a name for itself looks funny here, but later on
				# we'll be translating the target entrezID to region_id and
				# adding more historical entrezID aliases
				nsNames['entrez_gid'].add( (entrezID,entrezID) )
				nsNames['symbol'].add( (entrezID,symbol) )
				for alias in aliases:
					nsNames['symbol'].add( (entrezID,alias) )
				for xref in xrefs:
					xrefDB,xrefID = xref.split(":",1)
					# turn ENSG/ENSP/ENST into Ensembl_X
					if xrefDB == "Ensembl" and xrefID.startswith("ENS") and len(xrefID) > 3:
						xrefDB = "Ensembl_%c" % xrefID[3]
					if xrefDB in xrefNS:
						nsNames[xrefNS[xrefDB]].add( (entrezID,xrefID) )
			#if taxonomy is 9606 (human)
		#foreach line in geneFile
		
		# delete any symbol alias which is also the primary name of exactly one other gene
		dupe = set()
		for alias in nsNames['symbol']:
			entrezID = alias[0]
			symbol = alias[1]
			if (symbol in primaryEntrez) and (primaryEntrez[symbol] != False) and (primaryEntrez[symbol] != entrezID):
				dupe.add(alias)
		nsNames['symbol'] -= dupe
		dupe = None
		
		# print stats
		numGenes = len(entrezGene)
		numNames0 = numNames
		numNames = sum(len(nsNames[ns]) for ns in nsNames)
		self.log(" OK: %d genes, %d identifiers\n" % (numGenes,numNames-numNames0))
		
		# store genes
		self.log("writing genes to the database ...")
		listEntrez = entrezGene.keys()
		listRID = self.addTypedRegions(typeID['gene'], (entrezGene[entrezID] for entrezID in listEntrez))
		entrezRID = dict(zip(listEntrez,listRID))
		numGenes = len(entrezRID)
		self.log(" OK: %d genes\n" % (numGenes))
		entrezGene = None
		
		# translate target entrezID to region_id in nsNames
		for ns in nsNames:
			names = set( (entrezRID[name[0]],name[1]) for name in nsNames[ns] if name[0] in entrezRID )
			nsNames[ns] = names
		numNames = sum(len(nsNames[ns]) for ns in nsNames)
		
		# process gene boundaries
		self.log("processing gene boundaries ...")
		setBounds = set()
		setOrphan = set()
		setNoNC = set()
		setMismatch = set()
		refseqRIDs = dict()
		boundFile = self.zfile('gene2refseq.gz') #TODO:context manager,iterator
		header = boundFile.next().rstrip()
		if header != "#Format: tax_id GeneID status RNA_nucleotide_accession.version RNA_nucleotide_gi protein_accession.version protein_gi genomic_nucleotide_accession.version genomic_nucleotide_gi start_position_on_the_genomic_accession end_position_on_the_genomic_accession orientation assembly (tab is used as a separator, pound sign - start of a comment)":
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
		else:
			for line in boundFile:
				# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
				if line.startswith("9606\t"):
					words = line.split("\t")
					entrezID = int(words[1])
					rnaAcc = words[3].rsplit('.',1)[0] if words[3] != "-" else None
					proAcc = words[5].rsplit('.',1)[0] if words[5] != "-" else None
					genAcc = words[7].rsplit('.',1)[0] if words[7] != "-" else None
					posMin = long(words[9]) if words[9] != "-" else None
					posMax = long(words[10]) if words[10] != "-" else None
					
					if entrezID in entrezRID:
						if posMin and posMax:
							# refseq accession types: http://www.ncbi.nlm.nih.gov/RefSeq/key.html
							# NC_ is the "complete genomic" accession, with positions relative to the whole chromosome;
							# NT_ for example has positions relative to the read fragment, which is no use to us
							if (not genAcc) or (not genAcc.startswith('NC_')):
								setNoNC.add(entrezID)
							else:
								# TODO: is there some better way to load these mappings?
								# in theory they ought not to change, but hardcoding them is unfortunate
								if genAcc == 'NC_000023':
									chm = 'X'
								elif genAcc == 'NC_000024':
									chm = 'Y'
								elif genAcc == 'NC_012920':
									chm = 'MT'
								else:
									chm = genAcc[3:].lstrip('0')
								# TODO: we're ignoring any region with an ambiguous chromosome
								# (gene_info says one thing, gene2refseq says another); is that right?
								#6066 17 -> 7
								#6090 7 -> 12
								#693127 7 -> 14
								#693128 6 -> 20
								#100293744 X -> Y
								#100302287 17 -> 8
								#100302657 3 -> 15
								#100313773 16 -> 18
								#100313884 8 -> 2
								#100418703 Y -> X
								#100507426 Y -> X
								if chm not in self._loki.chr_num:
									setMismatch.add(entrezID)
								elif (entrezID in entrezChm) and (chm not in entrezChm[entrezID].split('|')):
									setMismatch.add(entrezID)
								else:
									setBounds.add( (entrezRID[entrezID],self._loki.chr_num[chm],posMin,posMax) )
							#if genAcc is NC_
						# if posMin and posMax
						
						if rnaAcc:
							nsNames['refseq_gid'].add( (entrezRID[entrezID],rnaAcc) )
						if proAcc:
							nsNames['refseq_pid'].add( (entrezRID[entrezID],proAcc) )
							if proAcc not in refseqRIDs:
								refseqRIDs[proAcc] = set()
							refseqRIDs[proAcc].add(entrezRID[entrezID])
						# don't store genAcc as an alias, there's only one per chromosome
					else:
						setOrphan.add(entrezID)
					#if entrezID in entrezRID
				#if taxonomy is 9606 (human)
			#foreach line in boundFile
			
			# print stats
			setGenes = set(bound[0] for bound in setBounds)
			setNoNC -= setGenes
			setMismatch -= setGenes
			numBounds = len(setBounds)
			numGenes = len(setGenes)
			numNames0 = numNames
			numNames = sum(len(nsNames[ns]) for ns in nsNames)
			self.log(" OK: %d boundaries (%d genes), %d identifiers\n" % (numBounds,numGenes,numNames-numNames0))
			self.logPush()
			if setOrphan:
				self.log("WARNING: %d genes not defined\n" % (len(setOrphan)))
			if setNoNC:
				self.log("WARNING: %d genes not mapped to whole chromosome\n" % (len(setNoNC)))
			if setMismatch:
				self.log("WARNING: %d genes on mismatching chromosome\n" % (len(setMismatch)))
			self.logPop()
			entrezChm = setOrphan = setNoNC = setMismatch = setGenes = None
			
			# store gene boundaries
			self.log("writing gene boundaries to the database ...")
			numBounds = len(setBounds)
			self.addRegionPopulationBounds(populationID['n/a'], setBounds)
			self.log(" OK: %d boundaries\n" % (numBounds))
			setBounds = None
		#if gene boundary header ok
		
		# process historical gene names
		self.log("processing historical gene names ...")
		entrezUpdate = {}
		historyEntrez = {}
		histFile = self.zfile('gene_history.gz') #TODO:context manager,iterator
		header = histFile.next().rstrip()
		if header != "#Format: tax_id GeneID Discontinued_GeneID Discontinued_Symbol Discontinue_Date (tab is used as a separator, pound sign - start of a comment)":
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
		else:
			for line in histFile:
				# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
				if line.startswith("9606\t"):
					words = line.split("\t")
					entrezID = int(words[1]) if words[1] != "-" else None
					oldEntrez = int(words[2]) if words[2] != "-" else None
					oldName = words[3] if words[3] != "-" else None
					
					if entrezID and entrezID in entrezRID:
						if oldEntrez and oldEntrez != entrezID:
							entrezUpdate[oldEntrez] = entrezID
							nsNames['entrez_gid'].add( (entrezRID[entrezID],oldEntrez) )
						if oldName and (oldName not in primaryEntrez or primaryEntrez[oldName] == False):
							if oldName not in historyEntrez:
								historyEntrez[oldName] = entrezID
							elif historyEntrez[oldName] != entrezID:
								historyEntrez[oldName] = False
							nsNames['symbol'].add( (entrezRID[entrezID],oldName) )
				#if taxonomy is 9606 (human)
			#foreach line in histFile
			
			# delete any symbol alias which is also the historical name of exactly one other gene
			dupe = set()
			for alias in nsNames['symbol']:
				entrezID = alias[0]
				symbol = alias[1]
				if (symbol in historyEntrez) and (historyEntrez[symbol] != False) and (historyEntrez[symbol] != entrezID):
					dupe.add(alias)
			nsNames['symbol'] -= dupe
			dupe = None
			
			# print stats
			numNames0 = numNames
			numNames = sum(len(nsNames[ns]) for ns in nsNames)
			self.log(" OK: %d identifiers\n" % (numNames-numNames0))
		#if historical name header ok
		
		# process ensembl gene names
		self.log("processing ensembl gene names ...")
		ensFile = self.zfile('gene2ensembl.gz') #TODO:context manager,iterator
		header = ensFile.next().rstrip()
		if header != "#Format: tax_id GeneID Ensembl_gene_identifier RNA_nucleotide_accession.version Ensembl_rna_identifier protein_accession.version Ensembl_protein_identifier (tab is used as a separator, pound sign - start of a comment)":
			self.log(" ERROR: unrecognized file header\n")
			self.log("%s\n" % header)
		else:
			for line in ensFile:
				# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
				if line.startswith("9606\t"):
					words = line.split("\t")
					entrezID = int(words[1])
					ensemblG = words[2] if words[2] != "-" else None
					ensemblT = words[4] if words[4] != "-" else None
					ensemblP = words[6] if words[6] != "-" else None
					
					if ensemblG or ensemblT or ensemblP:
						while entrezID and (entrezID in entrezUpdate):
							entrezID = entrezUpdate[entrezID]
						
						if entrezID and (entrezID in entrezRID):
							if ensemblG:
								nsNames['ensembl_gid'].add( (entrezRID[entrezID],ensemblG) )
							if ensemblT:
								nsNames['ensembl_gid'].add( (entrezRID[entrezID],ensemblT) )
							if ensemblP:
								nsNames['ensembl_pid'].add( (entrezRID[entrezID],ensemblP) )
				#if taxonomy is 9606 (human)
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
			if header != "#Format: GeneID UniGene_cluster (tab is used as a separator, pound sign - start of a comment)":
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
					if entrezID and (entrezID in entrezRID) and unigeneID:
						nsNames['unigene_gid'].add( (entrezRID[entrezID],unigeneID) )
				#foreach line in ugFile
				
				# print stats
				numNames0 = numNames
				numNames = sum(len(nsNames[ns]) for ns in nsNames)
				self.log(" OK: %d identifiers\n" % (numNames-numNames0))
			#if unigene name header ok
		#with ugFile
		
		if 0:
			# process uniprot gene names from entrez
			self.log("processing uniprot gene names ...")
			upFile = self.zfile('gene_refseq_uniprotkb_collab.gz') #TODO:context manager,iterator
			header = upFile.next().rstrip()
			if header != "#Format: NCBI_protein_accession UniProtKB_protein_accession (tab is used as a separator, pound sign - start of a comment)":
				self.log(" ERROR: unrecognized file header\n")
				self.log("%s\n" % header)
			else:
				for line in upFile:
					words = line.split("\t")
					proteinAcc = words[0].rsplit('.',1)[0] if words[0] != "-" else None
					uniprotAcc = words[1] if words[1] != "-" else None
					
					# there will be tons of identifiers missing from refseqRIDs because they're non-human
					if proteinAcc and (proteinAcc in refseqRIDs) and uniprotAcc:
						for regionID in refseqRIDs[proteinAcc]:
							nsNames['uniprot_pid'].add( (regionID,uniprotAcc) )
				#foreach line in upFile
					
				# print stats
				numNames0 = numNames
				numNames = sum(len(nsNames[ns]) for ns in nsNames)
				self.log(" OK: %d identifiers\n" % (numNames-numNames0))
			#if header ok
		
		# process uniprot gene names from uniprot (no header!)
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
			found = False
			for word2 in words[2].split(';'):
				entrezID = int(word2.strip()) if word2 else None
				if entrezID and (entrezID in entrezRID):
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
		
		# store gene names
		self.log("writing gene identifiers to the database ...")
		numNames = 0
		for ns in nsNames:
			if nsNames[ns]:
				numNames += len(nsNames[ns])
				self.addRegionNamespacedNames(namespaceID[ns], nsNames[ns])
		self.log(" OK: %d identifiers\n" % (numNames,))
		nsNames = None
		
		# store gene names
		self.log("writing gene identifier references to the database ...")
		numNameNames = 0
		for ns in nsNameNames:
			if nsNameNames[ns]:
				numNameNames += len(nsNameNames[ns])
				self.addRegionTypedNameNamespacedNames(typeID['gene'], namespaceID[ns], nsNameNames[ns])
		self.log(" OK: %d references\n" % (numNameNames,))
		nsNameNames = None
	#update()
	
#Source_entrez