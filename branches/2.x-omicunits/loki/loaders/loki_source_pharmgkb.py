#!/usr/bin/env python

import zipfile
from loki import loki_source


class Source_pharmgkb(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '3.0 (2013-12-05)'
	#getVersionString()
	
	
	@classmethod
	def getOptions(cls):
		return {
			'names'    : '[yes|no]  --  process and store gene identifiers (default: yes)',
			'pathways' : '[yes|no]  --  process and store gene pathways (default: yes)',
		}
	#getOptions()
	
	
	def validateOptions(self, options):
		options.setdefault('names', 'yes')
		options.setdefault('pathways', 'yes')
		for o,v in options.iteritems():
			v = v.strip().lower()
			if o in ('names','pathways'):
				if (v == '1') or 'true'.startswith(v) or 'yes'.startswith(v):
					v = 'yes'
				elif (v == '0') or 'false'.startswith(v) or 'no'.startswith(v):
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
		remFiles = dict()
		if options['names'] == 'yes':
			remFiles['genes.zip'] = '/commonFileDownload.action?filename=genes.zip'
		if options['pathways'] == 'yes':
			remFiles['pathways-tsv.zip'] = '/commonFileDownload.action?filename=pathways-tsv.zip'
		if remFiles:
			self.downloadFilesFromHTTP('www.pharmgkb.org', remFiles)
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
			('hgnc_gid',     0),
			('uniprot_gid',  0),
			('uniprot_pid',  1),
		])
		gtypeID = self.addGTypes([
			('pathway',),
		])
		
		# process gene names
		if options['names'] == 'yes':
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
					'hgnc':          ('hgnc_gid',),
					'uniProtKb':     ('uniprot_gid','uniprot_pid'),
				}
				for info in geneZip.infolist():
					if info.filename == 'genes.tsv':
						geneFile = geneZip.open(info,'r')
						header = geneFile.next().rstrip()
						if not header.startswith("PharmGKB Accession Id	Entrez Id	Ensembl Id	Name	Symbol	Alternate Names	Alternate Symbols	Is VIP	Has Variant Annotation	Cross-references"):
							self.log(" ERROR\n")
							self.log("unrecognized file header in '%s': %s\n" % (info.filename,header))
							return False
						for line in geneFile:
							words = line.decode('latin-1').split("\t")
							pgkbID = words[0]
							entrezID = words[1]
							ensemblID = words[2]
							symbol = words[4]
							aliases = words[6].split(',') if words[6] != "" else empty
							xrefs = words[9].strip(', \r\n').split(',') if words[9] != "" else empty
							
							if entrezID:
								setNames.add( (namespaceID['pharmgkb_gid'],pgkbID,namespaceID['entrez_gid'],entrezID) )
							if ensemblID:
								setNames.add( (namespaceID['pharmgkb_gid'],pgkbID,namespaceID['ensembl_gid'],ensemblID) )
								setNames.add( (namespaceID['pharmgkb_gid'],pgkbID,namespaceID['ensembl_pid'],ensemblID) )
							if symbol:
								setNames.add( (namespaceID['pharmgkb_gid'],pgkbID,namespaceID['symbol'],symbol) )
							for alias in aliases:
								#line.decode('latin-1') should handle this above
								#setNames.add( (namespaceID['pharmgkb_gid'],pgkbID,namespaceID['symbol'],unicode(alias.strip('" '),errors='ignore')) )
								setNames.add( (namespaceID['pharmgkb_gid'],pgkbID,namespaceID['symbol'],alias.strip('" ')) )
							for xref in xrefs:
								try:
									xrefDB,xrefID = xref.split(':',1)
									if xrefDB in xrefNS:
										for ns in xrefNS[xrefDB]:
											setNames.add( (namespaceID['pharmgkb_gid'],pgkbID,namespaceID[ns],xrefID) )
											#line.decode('latin-1') should handle this above
											#try:
											#	xrefID.encode('ascii')
											#	setNames.add( (namespaceID['pharmgkb_gid'],pgkbID,namespaceID[ns],xrefID.decode('utf8').encode('ascii')) )
											#except:
											#	self.log("Cannot encode gene alias")
								except ValueError:
									pass
						#foreach line in geneFile
						geneFile.close()
					#if genes.tsv
				#foreach file in geneZip
			#with geneZip
			numIDs = len(set(n[1] for n in setNames))
			self.log(" OK: %d identifiers (%d references)\n" % (numIDs,len(setNames)))
			
			# store gene names
			self.log("writing gene names to the database ...")
			self.addUnitNameNames(setNames)
			self.log(" OK\n")
			setNames = None
		#if names
		
		# process pathways
		if options['pathways'] == 'yes':
			self.log("verifying pathway archive file ...")
			pathDesc = {}
			nsAssoc = {
				'pharmgkb_gid': set(),
				'symbol':       set(),
			}
			numAssoc = numID = 0
			numBadHeader = 0
			with zipfile.ZipFile('pathways-tsv.zip','r') as pathZip:
				err = pathZip.testzip()
				if err:
					self.log(" ERROR\n")
					self.log("CRC failed for %s\n" % err)
					return False
				self.log(" OK\n")
				self.log("processing pathways ...")
				for info in pathZip.infolist():
					if info.filename == 'pathways.tsv': # old format before around 2013-05-01
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
					elif info.filename.startswith("PA"): # new format since around 2013-08-21
						pathFile = pathZip.open(info,'r')
						line = pathFile.next()
						if not line.startswith("From\tTo\tReaction Type\tController\tControl Type\tCell Type\tPubMed Id\tGenes"):
							numBadHeader += 1
						else:
							words = info.filename.split('-')
							curPath = words[0]
							desc = words[1].replace("_", " ")
							pathDesc[curPath] = (desc,desc)
							genes = set()
							for line in pathFile:
								line = line.decode('latin-1').rstrip("\r\n")
								words = [ w.strip() for w in line.split("\t") ]
								if words[7]:
									genes.update(w.strip() for w in words[7].split(','))
							#foreach line in pathFile
							for symbol in genes:
								numAssoc += 1
								numID += 1
								nsAssoc['symbol'].add( (curPath,numAssoc,symbol) )
							#foreach genes
						#if header
						pathFile.close()
					#if info.filename
				#foreach file in pathZip
			#with pathZip
			self.log(" OK: %d pathways, %d associations (%d identifiers)\n" % (len(pathDesc),numAssoc,numID))
			
			# store pathways
			self.log("writing pathways to the database ...")
			listPath = pathDesc.keys()
			listGID = self.addTypedGroups(gtypeID['pathway'], (pathDesc[path] for path in listPath))
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
				self.addGroupMemberNamespacedNames(namespaceID[ns], ((pathGID[a[0]],a[1],a[2]) for a in nsAssoc[ns]) )
			self.log(" OK\n")
		#if pathways
		
		#TODO: eventually add diseases, drugs, relationships
		
	#update()
	
#Source_pharmgkb
