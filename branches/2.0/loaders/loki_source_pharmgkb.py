#!/usr/bin/env python

import zipfile
import loki_source


class Source_pharmgkb(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def getDependencies(cls):
		return ('entrez',)
	#getDependencies()
	
	
	def download(self):
		# download the latest source files
		return
		self.downloadFilesFromHTTP('www.pharmgkb.org', {
			'genes.zip':        '/commonFileDownload.action?filename=genes.zip',
			'pathways-tsv.zip': '/commonFileDownload.action?filename=pathways-tsv.zip',
		})
	#download()
	
	
	def update(self):
		# begin transaction to update database
		self.log("initializing update process ...")
		with self.bulkUpdateContext(set(['group','group_name','group_region'])):
			self.log(" OK\n")
			
			# clear out all old data from this source
			self.log("deleting old records from the database ...")
			self.deleteSourceData()
			self.log(" OK\n")
			
			# get or create the required metadata records
			namespaceID = {
				'gene':     self.addNamespace('gene'),
				'entrez':   self.addNamespace('entrez'),
				'refseq':   self.addNamespace('refseq'),
				'ensembl':  self.addNamespace('ensembl'),
				'hgnc':     self.addNamespace('hgnc'),
				'uniprot':  self.addNamespace('uniprot'),
				'pharmgkb': self.addNamespace('pharmgkb'),
			}
			typeID = {
				'gene':     self.addType('gene'),
				'pathway':  self.addType('pathway'),
			}
			
			# process gene names
			self.log("verifying gene name archive file ...")
			setAssoc = set()
			setAmbig = set()
			setUnrec = set()
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
					'entrezGene':    'entrez',
					'refSeqDna':     'refseq',
					'refSeqRna':     'refseq',
					'refSeqProtein': 'refseq',
					'ensembl':       'ensembl',
					'hgnc':          'hgnc',
					'uniProtKb':     'uniprot',
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
							name = words[4]
							aliases = words[6].split(',') if words[6] != "" else empty
							xrefs = words[9].strip(', ').split(',') if words[9] != "" else empty
							
							nameList = []
							nsList = []
							if entrezID:
								nameList.append(entrezID)
								nsList.append(namespaceID['entrez'])
							if ensemblID:
								nameList.append(ensemblID)
								nsList.append(namespaceID['ensembl'])
							if name:
								nameList.append(name)
								nsList.append(namespaceID['gene'])
							for alias in aliases:
								nameList.append(alias.strip('" '))
								nsList.append(namespaceID['gene'])
							for xref in xrefs:
								try:
									xrefDB,xrefID = xref.split(':',1)
									if xrefDB in xrefNS:
										nameList.append(xrefID)
										nsList.append(namespaceID[xrefNS[xrefDB]])
								except ValueError:
									pass
							regionIDs = self._loki.getRegionIDsByNames(nameList, nsList, typeID['gene'], self._loki.MATCH_BEST)
							if len(regionIDs) == 1:
								setAssoc.add( (regionIDs[0],pgkbID) )
							elif len(regionIDs) > 1:
								setAmbig.add( (pgkbID,) + tuple(nameList) )
							else:
								setUnrec.add( (pgkbID,) + tuple(nameList) )
						#foreach line in geneFile
						geneFile.close()
					#if genes.tsv
				#foreach file in geneZip
			#with geneZip
			numAssoc = len(setAssoc)
			numGene = len(set(assoc[0] for assoc in setAssoc))
			numName = len(set(assoc[1] for assoc in setAssoc))
			self.log(" OK: %d associations (%d identifiers for %d genes)\n" % (numAssoc,numGene,numName))
			self.logPush()
			if setAmbig:
				numAssoc = len(setAmbig)
				numName = len(set(assoc[1:] for assoc in setAmbig))
				self.log("WARNING: %d ambiguous associations (%d identifiers)\n" % (numAssoc,numName))
			if setUnrec:
				numAssoc = len(setUnrec)
				numName = len(set(assoc[1:] for assoc in setUnrec))
				self.log("WARNING: %d unrecognized associations (%d identifiers)\n" % (numAssoc,numName))
			self.logPop()
			
			# store gene names
			self.log("writing gene names to the database ...")
			self.addNamespacedRegionNames(namespaceID['pharmgkb'], setAssoc)
			self.log(" OK\n")
			
			# process pathways
			self.log("verifying pathway archive file ...")
			pathDesc = {}
			setAssoc = set()
			setFlagAssoc = set()
			setAmbig = set()
			setUnrec = set()
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
									gene = words[2]
									
									nameList = [pgkbID, gene]
									nsList = [namespaceID['pharmgkb'], namespaceID['gene']]
									regionIDs = self._loki.getRegionIDsByNames(nameList, nsList, typeID['gene'], self._loki.MATCH_BEST)
									if len(regionIDs) == 1:
										setAssoc.add( (curPath,regionIDs[0]) )
									elif len(regionIDs) > 1:
										setAmbig.add( (curPath,pgkbID,gene) )
										for regionID in regionIDs:
											setFlagAssoc.add( (curPath,regionID) )
									else:
										setUnrec.add( (curPath,pgkbID,gene) )
								#if assoc is Gene
							lastline = line
						#foreach line in pathFile
						pathFile.close()
					#if pathways.tsv
				#foreach file in pathZip
			#with pathZip
			numAssoc = len(setAssoc)
			numGene = len(set(assoc[1] for assoc in setAssoc))
			numGroup = len(set(assoc[0] for assoc in setAssoc))
			self.log(" OK: %d associations (%d genes in %d groups)\n" % (numAssoc,numGene,numGroup))
			self.logPush()
			if setAmbig:
				numAssoc = len(setAmbig)
				numName = len(set(assoc[1:] for assoc in setAmbig))
				numGroup = len(set(assoc[0] for assoc in setAmbig))
				self.log("WARNING: %d ambiguous associations (%d identifiers in %d groups)\n" % (numAssoc,numName,numGroup))
			if setUnrec:
				numAssoc = len(setUnrec)
				numName = len(set(assoc[1:] for assoc in setUnrec))
				numGroup = len(set(assoc[0] for assoc in setUnrec))
				self.log("WARNING: %d unrecognized associations (%d identifiers in %d groups)\n" % (numAssoc,numName,numGroup))
			self.logPop()
			
			# store pathways
			self.log("writing pathways to the database ...")
			listPath = pathDesc.keys()
			listGID = self.addTypedGroups(typeID['pathway'], ((path,pathDesc[path]) for path in listPath))
			pathGID = dict(zip(listPath,listGID))
			self.log(" OK\n")
			
			# store pathway names
			self.log("writing pathway names to the database ...")
			self.addNamespacedGroupNames(namespaceID['pharmgkb'], ((pathGID[path],path) for path in listPath))
			self.log(" OK\n")
			
			# store gene associations
			self.log("writing gene associations to the database ...")
			self.addGroupRegions( (pathGID[assoc[0]],assoc[1]) for assoc in setAssoc )
			self.log(" OK\n")
			
			#TODO: diseases,drugs,relationships?
			
			# commit transaction
			self.log("finalizing update process ...")
		#with bulk update
		self.log(" OK\n")
	#update()
	
#Source_pharmgkb
