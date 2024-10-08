#!/usr/bin/env python

import zipfile
from loki import loki_source


class Source_pharmgkb(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '2.3 (2018-10-30)'
	#getVersionString()
	
	
	def download(self, options):
		self.downloadFilesFromHTTPS('api.pharmgkb.org', {
			'genes.zip':        '/v1/download/file/data/genes.zip',
			'pathways-tsv.zip': '/v1/download/file/data/pathways-tsv.zip',
		})
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('pharmgkb_id',  0),
			('pathway',      0),
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
					header = geneFile.__next__().rstrip()
					if header.decode().startswith("PharmGKB Accession Id	Entrez Id	Ensembl Id	Name	Symbol	Alternate Names	Alternate Symbols	Is VIP	Has Variant Annotation	Cross-references"):
						new2 = 0
					elif header.decode().startswith("PharmGKB Accession Id	NCBI Gene ID	HGNC ID	Ensembl Id	Name	Symbol	Alternate Names	Alternate Symbols	Is VIP	Has Variant Annotation	Cross-references"):
						new2 = 1
					else:
						raise Exception("ERROR: unrecognized file header in '%s': %s" % (info.filename,header))
					for line in geneFile:
						words = line.decode('latin-1').split("\t")
						pgkbID = words[0]
						entrezID = words[1]
						ensemblID = words[2+new2]
						symbol = words[4+new2]
						aliases = words[6+new2].split(',') if words[6+new2] != "" else empty
						xrefs = words[9+new2].strip(', \r\n').split(',') if words[9+new2] != "" else empty
						
						if entrezID:
							setNames.add( (namespaceID['entrez_gid'],entrezID,pgkbID) )
						if ensemblID:
							setNames.add( (namespaceID['ensembl_gid'],ensemblID,pgkbID) )
							setNames.add( (namespaceID['ensembl_pid'],ensemblID,pgkbID) )
						if symbol:
							setNames.add( (namespaceID['symbol'],symbol,pgkbID) )
						for alias in aliases:
							#line.decode('latin-1') should handle this above
							#setNames.add( (namespaceID['symbol'],unicode(alias.strip('" '),errors='ignore'),pgkbID) )
							setNames.add( (namespaceID['symbol'],alias.strip('" '),pgkbID) )
						for xref in xrefs:
							try:
								xrefDB,xrefID = xref.split(':',1)
								if xrefDB in xrefNS:
									for ns in xrefNS[xrefDB]:
										setNames.add( (namespaceID[ns],xrefID,pgkbID) )
										#line.decode('latin-1') should handle this above
										#try:
										#	xrefID.encode('ascii')
										#	setNames.add( (namespaceID[ns],xrefID.decode('utf8').encode('ascii'),pgkbID) )
										#except:
										#	self.log("Cannot encode gene alias")
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
		self.addBiopolymerTypedNameNamespacedNames(typeID['gene'], namespaceID['pharmgkb_gid'], setNames)
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
					# the old format had all pathways in one giant file, delimited by blank lines
					pathFile = pathZip.open(info,'r')
					curPath = None
					for line in pathFile:
						line = line.decode('latin-1').rstrip("\r\n")
						if line == "" and lastline == "":
							curPath = None
						elif curPath == None:
							words = line.split(':',1)
							if len(words) >= 2:
								curPath = words[0].strip()
								desc = words[1].strip().rsplit(' - ',1)
								desc.append('')
								#line.decode('latin-1') should handle this above
								#pathDesc[curPath] = (unicode(desc[0].strip(),errors='ignore'),unicode(desc[1].strip(),errors='ignore'))
								pathDesc[curPath] = (desc[0].strip().replace("`", "'"),desc[1].strip().replace("`", "'"))
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
				elif info.filename.endswith('.tsv'):
					# the new format has separate "PA###-***.tsv" files for each pathway
					pathFile = pathZip.open(info,'r')
					header = next(pathFile)
					if header.decode().startswith("From	To	Reaction Type	Controller	Control Type	Cell Type	PubMed Id	Genes"): #	Drugs	Diseases
						pass
					elif header.decode().startswith("From	To	Reaction Type	Controller	Control Type	Cell Type	PMIDs	Genes"): #	Drugs	Diseases
						pass
					else:
						raise Exception("ERROR: unrecognized file header in '%s': %s" % (info.filename,header))
					parts = info.filename.split('-')
					curPath = parts[0]
					parts = parts[1].split('.')
					pathDesc[curPath] = (parts[0].replace("_"," "),None)
					for line in pathFile:
						for symbol in line.decode('latin-1').split("\t")[7].split(","):
							numAssoc += 1
							numID += 1
							nsAssoc['symbol'].add( (curPath,numAssoc,symbol.strip('"')) )
					#foreach line in pathFile
					pathFile.close()
				#if pathways.tsv
			#foreach file in pathZip
		#with pathZip
		self.log(" OK: %d pathways, %d associations (%d identifiers)\n" % (len(pathDesc),numAssoc,numID))
		
		# store pathways
		self.log("writing pathways to the database ...")
		listPath = pathDesc.keys()
		listGID = self.addTypedGroups(typeID['pathway'], (pathDesc[path] for path in listPath))
		pathGID = dict(zip(listPath,listGID))
		self.log(" OK\n")
		
		# store pathway names
		self.log("writing pathway names to the database ...")
		self.addGroupNamespacedNames(namespaceID['pharmgkb_id'], ((pathGID[path],path) for path in listPath))
		self.addGroupNamespacedNames(namespaceID['pathway'], ((pathGID[path],pathDesc[path][0]) for path in listPath))
		self.log(" OK\n")
		
		# store gene associations
		self.log("writing gene associations to the database ...")
		for ns in nsAssoc:
			self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID[ns], ((pathGID[a[0]],a[1],a[2]) for a in nsAssoc[ns]) )
		self.log(" OK\n")
		
		#TODO: eventually add diseases, drugs, relationships
		
	#update()
	
#Source_pharmgkb
