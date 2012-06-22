#!/usr/bin/env python

import zipfile
import loki_source


class Source_netpath(loki_source.Source):
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromHTTP('www.netpath.org', {
			'NetPath_GeneReg_TSV1.zip': '/data/batch/NetPath_GeneReg_TSV1.zip', #Last-Modified: Sat, 03 Sep 2011 10:07:03 GMT
			'NetPath_GeneReg_TSV.zip':  '/data/batch/NetPath_GeneReg_TSV.zip', #Last-Modified: Fri, 31 Oct 2008 17:00:16 GMT
		})
	#download()
	
	
	def update(self):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('netpath_id', 0),
			('pathway',    0),
			('symbol',     0),
			('entrez_gid', 0),
		])
		typeID = self.addTypes([
			('pathway',),
			('gene',),
		])
		
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
		self.addGroupNamespacedNames(namespaceID['netpath_id'], ((pathGID[pathID],pathID) for pathID in listPath))
		self.addGroupNamespacedNames(namespaceID['pathway'], ((pathGID[pathID],pathName[pathID]) for pathID in listPath))
		self.log(" OK\n")
		
		# process associations
		self.log("verifying gene association archive ...")
		nsAssoc = {
			'symbol':     set(),
			'entrez_gid': set(),
		}
		numAssoc = numID = 0
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
					
					if pathID in pathGID:
						numAssoc += 1
						numID += 2
						nsAssoc['entrez_gid'].add( (pathGID[pathID],numAssoc,entrezID) )
						nsAssoc['symbol'].add( (pathGID[pathID],numAssoc,gene) )
					#if pathway is ok
				#foreach line in pathFile
				pathFile.close()
			#foreach file in pathZip
		#with pathZip
		self.log(" OK: %d associations (%d identifiers)\n" % (numAssoc,numID))
		
		# store gene associations
		self.log("writing gene associations to the database ...")
		for ns in nsAssoc:
			self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID[ns], nsAssoc[ns])
		self.log(" OK\n")
	#update()
	
#Source_netpath
