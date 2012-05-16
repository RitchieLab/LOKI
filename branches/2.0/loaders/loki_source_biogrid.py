#!/usr/bin/env python

import sys #TODO
import zipfile
import loki_source


class Source_biogrid(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def getDependencies(cls):
		return ('entrez',)
	#getDependencies()
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromHTTP('thebiogrid.org', {
			'BIOGRID-ORGANISM-LATEST.tab2.zip': '/downloads/archives/Latest%20Release/BIOGRID-ORGANISM-LATEST.tab2.zip',
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
				'gene':        self.addNamespace('gene'),
				'entrez':      self.addNamespace('entrez'),
				'biogrid':     self.addNamespace('biogrid'),
			}
			typeID = {
				'gene':        self.addType('gene'),
				'interaction': self.addType('interaction'),
			}
			
			# process associations
			self.log("verifying archive file ...")
			pairLabels = dict()
			setAmbig = set()
			setUnrec = set()
			with zipfile.ZipFile('BIOGRID-ORGANISM-LATEST.tab2.zip','r') as assocZip:
				err = assocZip.testzip()
				if err:
					self.log(" ERROR\n")
					self.log("CRC failed for %s\n" % err)
					return False
				self.log(" OK\n")
				self.log("processing gene interactions ...")
				for info in assocZip.infolist():
					if info.filename.find('Homo_sapiens') >= 0:
						assocFile = assocZip.open(info,'r')
						header = assocFile.next().rstrip()
						if header != "#BioGRID Interaction ID	Entrez Gene Interactor A	Entrez Gene Interactor B	BioGRID ID Interactor A	BioGRID ID Interactor B	Systematic Name Interactor A	Systematic Name Interactor B	Official Symbol Interactor A	Official Symbol Interactor B	Synonymns Interactor A	Synonyms Interactor B	Experimental System	Experimental System Type	Author	Pubmed ID	Organism Interactor A	Organism Interactor B	Throughput	Score	Modification	Phenotypes	Qualifications	Tags	Source Database":
							self.log(" ERROR\n")
							self.log("unrecognized file header in '%s': %s\n" % (info.filename,header))
							return False
						for line in assocFile:
							words = line.split("\t")
							bgID = int(words[0])
							entrezID1 = int(words[1])
							entrezID2 = int(words[2])
							syst1 = words[5] if words[5] != "-" else None
							syst2 = words[6] if words[6] != "-" else None
							gene1 = words[7]
							gene2 = words[8]
							#aliases1 = words[9].split('|')
							#aliases2 = words[10].split('|')
							tax1 = words[15]
							tax2 = words[16]
							
							if tax1 == '9606' and tax2 == '9606':
								nameList = [entrezID1, gene1, syst1]
								#nameList.extend(aliases1)
								nsList = [namespaceID['entrez'], namespaceID['gene'], namespaceID['gene']]
								#nsList.extend(namespaceID['gene'] for alias in aliases1)
								regionIDs1 = self._loki.getRegionIDsByNames(nameList, nsList, typeID['gene'], self._loki.MATCH_BEST)
								if len(regionIDs1) > 1:
									setAmbig.add( (bgID,) + tuple(nameList) )
								elif len(regionIDs1) < 1:
									setUnrec.add( (bgID,) + tuple(nameList) )
								
								nameList = [entrezID2, gene2, syst2]
								#nameList.extend(aliases2)
								nsList = [namespaceID['entrez'], namespaceID['gene'], namespaceID['gene']]
								#nsList.extend(namespaceID['gene'] for alias in aliases2)
								regionIDs2 = self._loki.getRegionIDsByNames(nameList, nsList, typeID['gene'], self._loki.MATCH_BEST)
								if len(regionIDs2) > 1:
									setAmbig.add( (bgID,) + tuple(nameList) )
								elif len(regionIDs2) < 1:
									setUnrec.add( (bgID,) + tuple(nameList) )
								
								if len(regionIDs1) == 1 and len(regionIDs2) == 1 and regionIDs1[0] != regionIDs2[0]:
									regionID1 = min(regionIDs1[0],regionIDs2[0])
									regionID2 = max(regionIDs1[0],regionIDs2[0])
									pair = (regionID1,regionID2)
									if pair not in pairLabels:
										pairLabels[pair] = set()
									pairLabels[pair].add(bgID)
						#foreach line in assocFile
						assocFile.close()
					#if Homo_sapiens file
				#foreach file in assocZip
			#with assocZip
			numAssoc = len(pairLabels)
			numGene = len(set(pair[0] for pair in pairLabels) | set(pair[1] for pair in pairLabels))
			numName = sum(len(pairLabels[pair]) for pair in pairLabels)
			self.log(" OK: %d interactions (%d genes), %d pair identifiers\n" % (numAssoc,numGene,numName))
			self.logPush()
			if setAmbig:
				numAssoc = len(setAmbig)
				numName = len(set(assoc[1:] for assoc in setAmbig))
				numGroup = len(set(assoc[0] for assoc in setAmbig))
				self.log("WARNING: %d ambiguous interactors (%d identifiers in %d groups)\n" % (numAssoc,numName,numGroup))
			if setUnrec:
				numAssoc = len(setUnrec)
				numName = len(set(assoc[1:] for assoc in setUnrec))
				numGroup = len(set(assoc[0] for assoc in setUnrec))
				self.log("WARNING: %d unrecognized interactors (%d identifiers in %d groups)\n" % (numAssoc,numName,numGroup))
			self.logPop()
			
			# store interaction groups
			self.log("writing interaction pairs to the database ...")
			listPair = pairLabels.keys()
			listGID = self.addTypedGroups(typeID['interaction'], ((min(pairLabels[pair]),None) for pair in listPair))
			pairGID = dict(zip(listPair,listGID))
			self.log(" OK\n")
			
			# store interaction labels
			listLabels = []
			for pair in listPair:
				listLabels.extend( (pairGID[pair],label) for label in pairLabels[pair] )
			self.log("writing interaction names to the database ...")
			self.addNamespacedGroupNames(namespaceID['biogrid'], listLabels)
			self.log(" OK\n")
			
			# store gene interactions
			self.log("writing gene interactions to the database ...")
			self.addGroupRegions((pairGID[pair],pair[0]) for pair in listPair)
			self.addGroupRegions((pairGID[pair],pair[1]) for pair in listPair)
			self.log(" OK\n")
			
			# identify pseudo-pathways
			self.log("identifying implied networks ...")
			geneAssoc = dict()
			for pair in listPair:
				if pair[0] not in geneAssoc:
					geneAssoc[pair[0]] = set()
				geneAssoc[pair[0]].add(pair[1])
				if pair[1] not in geneAssoc:
					geneAssoc[pair[1]] = set()
				geneAssoc[pair[1]].add(pair[0])
			listPath = self.findMaximalCliques(geneAssoc)
			numAssoc = sum(len(path) for path in listPath)
			numGene = len(geneAssoc)
			numGroup = len(listPath)
			self.log(" OK: %d associations (%d genes in %d groups)\n" % (numAssoc,numGene,numGroup))
			
			# TODO: flagged pathways
			
			# commit transaction
			self.log("finalizing update process ...")
		#with bulk update
		self.log(" OK\n")
	#update()
	
#Source_biogrid
