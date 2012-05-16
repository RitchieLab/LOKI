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
	#download()
	
	
	def update(self):
		# begin transaction to update database
		self.log("initializing update process ...")
		with self.bulkUpdateContext(set(['region','region_name','region_bound'])):
			self.log(" OK\n")
			
			# clear out all old gene data
			self.log("deleting old records from the database ...")
			self.deleteSourceData()
			self.log(" OK\n")
			
			# get or create the required metadata records
			namespaceID = {
				'gene':    self.addNamespace('gene'),
				'entrez':  self.addNamespace('entrez'),
				'refseq':  self.addNamespace('refseq'),
				'ensembl': self.addNamespace('ensembl'),
				'hgnc':    self.addNamespace('hgnc'),
				'mim':     self.addNamespace('mim'),
				'hprd':    self.addNamespace('hprd'),
				'vega':    self.addNamespace('vega'),
				'rgd':     self.addNamespace('rgd'),
				'mirbase': self.addNamespace('mirbase'),
				'uniprot': self.addNamespace('uniprot'),
				'unigene': self.addNamespace('unigene'),
			}
			populationID = {
				'N/A': self.addPopulation('N/A','no LD adjustment'),
			}
			typeID = {
				'gene': self.addType('gene'),
			}
			
			# process genes (no header!)
			self.log("processing genes ...")
			geneFile = self.zfile('Homo_sapiens.gene_info.gz') #TODO:context manager,iterator
			entrezName = dict()
			entrezDesc = dict()
			entrezChm = dict()
			primaryEntrez = dict()
			nsAliases = { ns:set() for ns in namespaceID }
			xrefNS = {
				'Ensembl': 'ensembl',
				'HGNC':    'hgnc',
				'MIM':     'mim',
				'HPRD':    'hprd',
				'Vega':    'vega',
				'RGD':     'rgd',
				'miRBase': 'mirbase',
			}
			empty = tuple()
			for line in geneFile:
				# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
				if line.startswith("9606\t"):
					words = line.rstrip().split("\t")
					entrezID = int(words[1])
					name = words[2]
					aliases = words[4].split("|") if words[4] != "-" else empty
					xrefs = words[5].split("|") if words[5] != "-" else empty
					chm = words[6]
					desc = words[8]
					
					entrezName[entrezID] = name
					entrezDesc[entrezID] = desc
					entrezChm[entrezID] = chm
					#symbol DUX4 -> 22947 , 100288687
					#symbol HBD -> 3045 , 100187828
					#symbol KIR3DL3 -> 115653 , 100133046
					#symbol MEMO1 -> 7795 , 51072
					#symbol MMD2 -> 221938 , 100505381
					#symbol OA3 -> 4936 , 474287
					#symbol PRG4 -> 10216 , 23572
					#symbol RNR1 -> 4549 , 6052
					#symbol RNR2 -> 4550 , 6053
					#symbol TEC -> 7006 , 100124696
					#symbol TTL -> 150465 , 646982
					if name not in primaryEntrez:
						primaryEntrez[name] = entrezID
					elif primaryEntrez[name] != entrezID:
						primaryEntrez[name] = False
					# entrezID->entrezID does look like a tautology, but we'll be
					# adding more historical entrezIDs before storing them all at once
					nsAliases['entrez'].add( (entrezID,entrezID) )
					nsAliases['gene'].add( (entrezID,name) )
					for alias in aliases:
						nsAliases['gene'].add( (entrezID,alias) )
					for xref in xrefs:
						xrefDB,xrefID = xref.split(":",1)
						if xrefDB in xrefNS:
							nsAliases[xrefNS[xrefDB]].add( (entrezID,xrefID) )
				#if taxonomy is 9606 (human)
			#foreach line in geneFile
			# if an alias is a primary name of a gene, remove it as an alias for any other genes
			for name in primaryEntrez.keys():
				if primaryEntrez[name] == False:
					del primaryEntrez[name]
			primaryDupe = set()
			for alias in nsAliases['gene']:
				regionID = alias[0]
				symbol = alias[1]
				if (symbol in primaryEntrez) and (primaryEntrez[symbol] != regionID):
					primaryDupe.add(alias)
			nsAliases['gene'] -= primaryDupe
			numGenes = len(entrezName)
			numIDs = sum(len(nsAliases[ns]) for ns in nsAliases)
			self.log(" OK: %d genes, %d identifiers\n" % (numGenes,numIDs))
			primaryDupe = None
			
			# process gene boundaries
			self.log("processing gene boundaries ...")
			boundFile = self.zfile('gene2refseq.gz') #TODO:context manager,iterator
			header = boundFile.next().rstrip()
			if header != "#Format: tax_id GeneID status RNA_nucleotide_accession.version RNA_nucleotide_gi protein_accession.version protein_gi genomic_nucleotide_accession.version genomic_nucleotide_gi start_position_on_the_genomic_accession end_position_on_the_genomic_accession orientation assembly (tab is used as a separator, pound sign - start of a comment)":
				self.log(" ERROR\n")
				self.log("unrecognized file header: %s\n" % header)
				return False
			setBounds = set()
			setMismatch = set()
			setNoNC = set()
			proteinEntrez = dict()
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
					
					if entrezID not in entrezName:
						entrezName[entrezID] = "Entrez#%d" % entrezID
						entrezDesc[entrezID] = None
					
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
								setBounds.add( (entrezID,self._loki.chr_num[chm],posMin,posMax) )
						#if genAcc is NC_
					# if posMin and posMax
					
					if rnaAcc:
						nsAliases['refseq'].add( (entrezID,rnaAcc) )
					if proAcc:
						nsAliases['refseq'].add( (entrezID,proAcc) )
						if proAcc not in proteinEntrez:
							proteinEntrez[proAcc] = entrezID
						elif proteinEntrez[proAcc] != entrezID:
							proteinEntrez[proAcc] = False
					# don't store genAcc as an alias, there's only one per chromosome
				#if taxonomy is 9606 (human)
			#foreach line in boundFile
			setGenes = set(bound[0] for bound in setBounds)
			setNoNC -= setGenes
			setMismatch -= setGenes
			numBounds = len(setBounds)
			numGenes = len(setGenes)
			numIDs0 = numIDs
			numIDs = sum(len(nsAliases[ns]) for ns in nsAliases)
			self.log(" OK: %d boundaries (%d genes), %d identifiers\n" % (numBounds,numGenes,numIDs-numIDs0))
			self.logPush()
			if setNoNC:
				self.log("WARNING: %d genes not mapped to whole chromosome\n" % (len(setNoNC)))
			if setMismatch:
				self.log("WARNING: %d genes on mismatching chromosome\n" % (len(setMismatch)))
			self.logPop()
			entrezChm = setNoNC = setMismatch = None
			
			# process historical gene names
			self.log("processing historical gene names ...")
			histFile = self.zfile('gene_history.gz') #TODO:context manager,iterator
			header = histFile.next().rstrip()
			if header != "#Format: tax_id GeneID Discontinued_GeneID Discontinued_Symbol Discontinue_Date (tab is used as a separator, pound sign - start of a comment)":
				self.log(" ERROR\n")
				self.log("unrecognized file header: %s\n" % header)
				return False
			entrezUpdate = {}
			historyEntrez = {}
			for line in histFile:
				# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
				if line.startswith("9606\t"):
					words = line.split("\t")
					entrezID = int(words[1]) if words[1] != "-" else None
					oldEntrez = int(words[2]) if words[2] != "-" else None
					oldName = words[3] if words[3] != "-" else None
					
					if entrezID and entrezID in entrezName:
						if oldEntrez and oldEntrez != entrezID:
							entrezUpdate[oldEntrez] = entrezID
							nsAliases['entrez'].add( (entrezID,oldEntrez) )
						if oldName and oldName not in primaryEntrez:
							if oldName not in historyEntrez:
								historyEntrez[oldName] = entrezID
							elif historyEntrez[oldName] != entrezID:
								historyEntrez[oldName] = False
							nsAliases['gene'].add( (entrezID,oldName) )
				#if taxonomy is 9606 (human)
			#foreach line in histFile
			# if an alias is a historical name of a gene, remove it as an alias for any other genes
			for name in historyEntrez.keys():
				if historyEntrez[name] == False:
					del historyEntrez[name]
			historyDupe = set()
			for alias in nsAliases['gene']:
				regionID = alias[0]
				symbol = alias[1]
				if (symbol in historyEntrez) and (historyEntrez[symbol] != regionID):
					historyDupe.add(alias)
			nsAliases['gene'] -= historyDupe
			numIDs0 = numIDs
			numIDs = sum(len(nsAliases[ns]) for ns in nsAliases)
			self.log(" OK: %d identifiers\n" % (numIDs-numIDs0))
			historyDupe = None
			
			# process ensembl gene names
			self.log("processing ensembl gene names ...")
			ensFile = self.zfile('gene2ensembl.gz') #TODO:context manager,iterator
			header = ensFile.next().rstrip()
			if header != "#Format: tax_id GeneID Ensembl_gene_identifier RNA_nucleotide_accession.version Ensembl_rna_identifier protein_accession.version Ensembl_protein_identifier (tab is used as a separator, pound sign - start of a comment)":
				self.log(" ERROR\n")
				self.log("unrecognized file header: %s\n" % header)
				return False
			for line in ensFile:
				# quickly filter out all non-9606 (human) taxonomies before taking the time to split()
				if line.startswith("9606\t"):
					words = line.split("\t")
					entrezID = int(words[1])
					ensemblG = words[2] if words[2] != "-" else None
					ensemblT = words[4] if words[4] != "-" else None
					ensemblP = words[6] if words[6] != "-" else None
					
					if ensemblG or ensemblT or ensemblP:
						while entrezID in entrezUpdate:
							entrezID = entrezUpdate[entrezID]
						
						if entrezID not in entrezName:
							entrezName[entrezID] = "Entrez#%d" % entrezID
							entrezDesc[entrezID] = None
						
						if ensemblG:
							nsAliases['ensembl'].add( (entrezID,ensemblG) )
						if ensemblT:
							nsAliases['ensembl'].add( (entrezID,ensemblT) )
						if ensemblP:
							nsAliases['ensembl'].add( (entrezID,ensemblP) )
				#if taxonomy is 9606 (human)
			#foreach line in ensFile
			numIDs0 = numIDs
			numIDs = sum(len(nsAliases[ns]) for ns in nsAliases)
			self.log(" OK: %d identifiers\n" % (numIDs-numIDs0))
			
			# process unigene gene names
			self.log("processing unigene gene names ...")
			with open('gene2unigene','rU') as ugFile:
				header = ugFile.next().rstrip()
				if header != "#Format: GeneID UniGene_cluster (tab is used as a separator, pound sign - start of a comment)":
					self.log(" ERROR\n")
					self.log("unrecognized file header: %s\n" % header)
					return False
				for line in ugFile:
					words = line.rstrip().split("\t")
					entrezID = int(words[0]) if words[0] != "-" else None
					unigeneID = words[1] if words[1] != "-" else None
					
					while entrezID and (entrezID in entrezUpdate):
						entrezID = entrezUpdate[entrezID]
					
					# there will be lots of extraneous mappings for genes of other species
					if entrezID and (entrezID in entrezName) and unigeneID:
						nsAliases['unigene'].add( (entrezID,unigeneID) )
				#foreach line in ugFile
			#with ugFile
			numIDs0 = numIDs
			numIDs = sum(len(nsAliases[ns]) for ns in nsAliases)
			self.log(" OK: %d identifiers\n" % (numIDs-numIDs0))
			
			# process uniprot gene names
			self.log("processing uniprot gene names ...")
			upFile = self.zfile('gene_refseq_uniprotkb_collab.gz') #TODO:context manager,iterator
			header = upFile.next().rstrip()
			if header != "#Format: NCBI_protein_accession UniProtKB_protein_accession (tab is used as a separator, pound sign - start of a comment)":
				self.log(" ERROR\n")
				self.log("unrecognized file header: %s\n" % header)
				return False
			numAmbig = 0
			for line in upFile:
				words = line.split("\t")
				proteinAcc = words[0].rsplit('.',1)[0] if words[0] != "-" else None
				uniprotAcc = words[1] if words[1] != "-" else None
				
				if proteinAcc and uniprotAcc:
					# there will be lots of extraneous mappings for genes of other species
					if proteinAcc not in proteinEntrez:
						pass
					elif proteinEntrez[proteinAcc] == False:
						numAmbig += 1
					else:
						nsAliases['uniprot'].add( (proteinEntrez[proteinAcc],uniprotAcc) )
			#foreach line in upFile
			numIDs0 = numIDs
			numIDs = sum(len(nsAliases[ns]) for ns in nsAliases)
			self.log(" OK: %d identifiers (%d ambiguous)\n" % (numIDs-numIDs0,numAmbig))
			proteinEntrez = entrezUpdate = None
			
			# store genes
			self.log("writing genes to the database ...")
			listEntrez = entrezName.keys()
			listRID = self.addTypedRegions(typeID['gene'], ((entrezName[entrez],entrezDesc[entrez]) for entrez in listEntrez))
			entrezRID = dict(zip(listEntrez,listRID))
			numGenes = len(listEntrez)
			self.log(" OK: %d genes\n" % (numGenes))
			entrezName = entrezDesc = None
			
			# store gene boundaries
			self.log("writing gene boundaries to the database ...")
			numBounds = len(setBounds)
			self.addPopulationRegionBounds(populationID['N/A'], ((entrezRID[bound[0]],bound[1],bound[2],bound[3]) for bound in setBounds))
			self.log(" OK: %d boundaries\n" % (numBounds))
			setBounds = None
			
			# store gene names
			self.log("writing gene identifiers to the database ...")
			numNames = 0
			for ns in nsAliases:
				if len(nsAliases[ns]) > 0:
					numNames += len(nsAliases[ns])
					self.addNamespacedRegionNames(namespaceID[ns], ((entrezRID[name[0]],name[1]) for name in nsAliases[ns]))
			self.log(" OK: %d identifiers\n" % (numNames))
			nsAliases = None
			
			# commit transaction
			self.log("finalizing update process ...")
		#with bulk update
		self.log(" OK\n")
		
		# print stats
		if self._loki.getVerbose():
			self.log("generating statistics ...")
			stats = self._loki.getRegionNameStats(typeID=typeID['gene'])
			self.log(" OK: %(total)d gene names (%(unique)d unique, %(redundant)d redundant, %(ambiguous)d ambiguous)\n" % stats)
	#update()
	
#Source_entrez
