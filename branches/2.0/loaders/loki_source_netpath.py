#!/usr/bin/env python

import zipfile
import loki_source


class Source_netpath(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def getDependencies(cls):
		return ('entrez',)
	#getDependencies()
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromHTTP('www.netpath.org', {
			'NetPath_GeneReg_TSV1.zip': '/data/batch/NetPath_GeneReg_TSV1.zip', #Last-Modified: Sat, 03 Sep 2011 10:07:03 GMT
			'NetPath_GeneReg_TSV.zip':  '/data/batch/NetPath_GeneReg_TSV.zip', #Last-Modified: Fri, 31 Oct 2008 17:00:16 GMT
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
				'gene':    self.addNamespace('gene'),
				'entrez':  self.addNamespace('entrez'),
				'netpath': self.addNamespace('netpath'),
				'pathway': self.addNamespace('pathway'),
			}
			typeID = {
				'gene':       self.addType('gene'),
				'pathway':    self.addType('pathway'),
			}
			
			# process pathways
			# (this file has associations too, but fewer of them, so we just
			#  use it for its pathway labels, which the other file lacks)
			self.log("verifying pathway archive ...")
			pathName = {}
			with zipfile.ZipFile('NetPath_GeneReg_TSV1.zip','r') as pathZip:
				err = pathZip.testzip()
				if err:
					self.log(" ERROR\n")
					self.log("CRC failed for %s\n" % err)
					return False
				self.log(" OK\n")
				self.log("processing pathways ...")
				for info in pathZip.infolist(): # there should be only one
					pathFile = pathZip.open(info,'rU')
					header = pathFile.next().rstrip()
					if header != "Gene regulation id	Pathway name	Pathway ID	Gene name	Entrez gene ID	Regulation	Experiment	PubMed ID":
						self.log(" ERROR\n")
						self.log("unrecognized file header in '%s': %s\n" % (info.filename,header))
						return False
					for line in pathFile:
						words = line.split("\t")
						name = words[1]
						pathID = words[2]
						
						pathName[pathID] = name
					#foreach line in pathFile
					pathFile.close()
				#foreach file in pathZip
				numPathways = len(pathName)
				self.log(" OK: %d pathways\n" % (numPathways,))
			#with pathZip
			
			# store pathways
			self.log("writing pathways to the database ...")
			listPath = pathName.keys()
			listGID = self.addTypedGroups(typeID['pathway'], ((pathName[pathID],None) for pathID in listPath))
			pathGID = dict(zip(listPath,listGID))
			self.log(" OK\n")
			
			# store pathway names
			self.log("writing pathway names to the database ...")
			self.addNamespacedGroupNames(namespaceID['netpath'], ((pathGID[pathID],pathID) for pathID in listPath))
			self.addNamespacedGroupNames(namespaceID['pathway'], ((pathGID[pathID],pathName[pathID]) for pathID in listPath))
			self.log(" OK\n")
			
			# process associations
			self.log("verifying gene association archive ...")
			setAssoc = set()
			setFlagAssoc = set()
			setOrphan = set()
			setAmbig = set()
			setUnrec = set()
			with zipfile.ZipFile('NetPath_GeneReg_TSV.zip','r') as pathZip:
				err = pathZip.testzip()
				if err:
					self.log(" ERROR\n")
					self.log("CRC failed for %s\n" % err)
					return False
				self.log(" OK\n")
				self.log("processing gene associations ...")
				for info in pathZip.infolist():
					pathFile = pathZip.open(info,'r')
					header = pathFile.next().rstrip()
					if header != "Gene regulation id	Pathway name	Pathway id	Gene Name	Entrez Gene	Regulation	Experiment type	PubMed id":
						self.log(" ERROR\n")
						self.log("unrecognized file header in '%s': %s\n" % (info.filename,header))
						return False
					for line in pathFile:
						words = line.split("\t")
						pathID = words[2]
						gene = words[3]
						entrezID = words[4]
						
						if pathID not in pathGID:
							setOrphan.add( (pathID,entrezID,gene) )
						else:
							regionIDs = self._loki.getRegionIDsByNames(
									[entrezID, gene],
									[namespaceID['entrez'], namespaceID['gene']],
									typeID['gene'],
									self._loki.MATCH_BEST
							)
							if len(regionIDs) == 1:
								setAssoc.add( (pathGID[pathID],regionIDs[0]) )
							elif len(regionIDs) > 1:
								setAmbig.add( (pathGID[pathID],entrezID,gene) )
								for regionID in regionIDs:
									setFlagAssoc.add( (pathGID[pathID],regionID) )
							else:
								setUnrec.add( (pathGID[pathID],entrezID,gene) )
						#if pathway is ok
					#foreach line in pathFile
					pathFile.close()
				#foreach file in pathZip
			#with pathZip
			numAssoc = len(setAssoc)
			numGene = len(set(assoc[1] for assoc in setAssoc))
			numGroup = len(set(assoc[0] for assoc in setAssoc))
			self.log(" OK: %d associations (%d genes in %d groups)\n" % (numAssoc,numGene,numGroup))
			self.logPush()
			if setOrphan:
				numAssoc = len(setOrphan)
				numName = len(set(assoc[1:] for assoc in setOrphan))
				numGroup = len(set(assoc[0] for assoc in setOrphan))
				self.log("WARNING: %d orphaned associations (%d identifiers in %d groups)\n" % (numAssoc,numName,numGroup))
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
			
			# store gene associations
			self.log("writing gene associations to the database ...")
			self.addGroupRegions(setAssoc)
			# TODO: setFlagAssoc
			self.log(" OK\n")
			
			# commit transaction
			self.log("finalizing update process ...")
		#with bulk update
		self.log(" OK\n")
	#update()
	
#Source_netpath
