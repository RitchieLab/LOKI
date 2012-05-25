#!/usr/bin/env python

import zipfile
import loki_source


class Source_pharmgkb(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromHTTP('www.pharmgkb.org', {
			'genes.zip':        '/commonFileDownload.action?filename=genes.zip',
			'pathways-tsv.zip': '/commonFileDownload.action?filename=pathways-tsv.zip',
		})
	#download()
	
	
	def update(self):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('pharmgkb_id',  0),
			('pharmgkb_gid', 0),
			('symbol',       0),
			('entrez_gid',   0),
			('refseq_gid',   0),
			('refseq_pid',   1),
			('ensembl_gid',  0),
			('ensembl_pid',  1),
			('hgnc_id',      0),
			('uniprot_gid',  0),
			('uniprot_pid',  1),
		])
		typeID = self.addTypes([
			('gene',),
			('pathway',),
		])
		
		# process gene names
		self.log("verifying gene name archive file ...")
		setNames = set()
		empty = tuple()
		with zipfile.ZipFile('genes.zip','r') as geneZip:
			err = geneZip.testzip()
			if err:
				self.log(" ERROR\n")
				self.log("CRC failed for %s\n" % err)
				return False
			self.log(" OK\n")
			self.log("processing gene names ...")
			xrefNS = {
				'entrezGene':    ('entrez_gid',),
				'refSeqDna':     ('refseq_gid',),
				'refSeqRna':     ('refseq_gid',),
				'refSeqProtein': ('refseq_pid',),
				'ensembl':       ('ensembl_gid','ensembl_pid'),
				'hgnc':          ('hgnc_id',),
				'uniProtKb':     ('uniprot_gid','uniprot_pid'),
			}
			for info in geneZip.infolist():
				if info.filename == 'genes.tsv':
					geneFile = geneZip.open(info,'r')
					header = geneFile.next().rstrip()
					if header != "PharmGKB Accession Id	Entrez Id	Ensembl Id	Name	Symbol	Alternate Names	Alternate Symbols	Is VIP	Has Variant Annotation	Cross-references":
						self.log(" ERROR\n")
						self.log("unrecognized file header in '%s': %s\n" % (info.filename,header))
						return False
					for line in geneFile:
						words = line.split("\t")
						pgkbID = words[0]
						entrezID = words[1]
						ensemblID = words[2]
						symbol = words[4]
						aliases = words[6].split(',') if words[6] != "" else empty
						xrefs = words[9].strip(', \r\n').split(',') if words[9] != "" else empty
						
						if entrezID:
							setNames.add( (namespaceID['entrez_gid'],entrezID,pgkbID) )
						if ensemblID:
							setNames.add( (namespaceID['ensembl_gid'],ensemblID,pgkbID) )
							setNames.add( (namespaceID['ensembl_pid'],ensemblID,pgkbID) )
						if symbol:
							setNames.add( (namespaceID['symbol'],symbol,pgkbID) )
						for alias in aliases:
							setNames.add( (namespaceID['symbol'],alias.strip('" '),pgkbID) )
						for xref in xrefs:
							try:
								xrefDB,xrefID = xref.split(':',1)
								if xrefDB in xrefNS:
									for ns in xrefNS[xrefDB]:
										setNames.add( (namespaceID[ns],xrefID,pgkbID) )
							except ValueError:
								pass
					#foreach line in geneFile
					geneFile.close()
				#if genes.tsv
			#foreach file in geneZip
		#with geneZip
		numIDs = len(set(n[2] for n in setNames))
		self.log(" OK: %d identifiers (%d references)\n" % (numIDs,len(setNames)))
		
		# store gene names
		self.log("writing gene names to the database ...")
		self.addRegionTypedNameNamespacedNames(typeID['gene'], namespaceID['pharmgkb_gid'], setNames)
		self.log(" OK\n")
		setNames = None
		
		# process pathways
		self.log("verifying pathway archive file ...")
		pathDesc = {}
		nsAssoc = {
			'pharmgkb_gid': set(),
			'symbol':       set(),
		}
		numAssoc = numID = 0
		with zipfile.ZipFile('pathways-tsv.zip','r') as pathZip:
			err = pathZip.testzip()
			if err:
				self.log(" ERROR\n")
				self.log("CRC failed for %s\n" % err)
				return False
			self.log(" OK\n")
			self.log("processing pathways ...")
			for info in pathZip.infolist():
				if info.filename == 'pathways.tsv':
					pathFile = pathZip.open(info,'r')
					curPath = None
					for line in pathFile:
						line = line.rstrip("\r\n")
						if line == "" and lastline == "":
							curPath = None
						elif curPath == None:
							words = line.split(':')
							if len(words) >= 2:
								curPath = words[0].strip()
								pathDesc[curPath] = words[1].strip()
						elif curPath == False:
							pass
						else:
							words = line.split("\t")
							if words[0] == "From":
								curPath = False
							elif words[0] == "Gene":
								pgkbID = words[1]
								symbol = words[2]
								
								numAssoc += 1
								numID += 2
								nsAssoc['pharmgkb_gid'].add( (curPath,numAssoc,pgkbID) )
								nsAssoc['symbol'].add( (curPath,numAssoc,symbol) )
							#if assoc is Gene
						lastline = line
					#foreach line in pathFile
					pathFile.close()
				#if pathways.tsv
			#foreach file in pathZip
		#with pathZip
		self.log(" OK: %d pathways, %d associations (%d identifiers)\n" % (len(pathDesc),numAssoc,numID))
		
		# store pathways
		self.log("writing pathways to the database ...")
		listPath = pathDesc.keys()
		listGID = self.addTypedGroups(typeID['pathway'], ((path,pathDesc[path]) for path in listPath))
		pathGID = dict(zip(listPath,listGID))
		self.log(" OK\n")
		
		# store pathway names
		self.log("writing pathway names to the database ...")
		self.addGroupNamespacedNames(namespaceID['pharmgkb_id'], ((pathGID[path],path) for path in listPath))
		self.log(" OK\n")
		
		# store gene associations
		self.log("writing gene associations to the database ...")
		for ns in nsAssoc:
			self.addGroupTypedRegionNamespacedNames(typeID['gene'], namespaceID[ns], ((pathGID[a[0]],a[1],a[2]) for a in nsAssoc[ns]) )
		self.log(" OK\n")
		
		#TODO: diseases,drugs,relationships?
	#update()
	
#Source_pharmgkb
