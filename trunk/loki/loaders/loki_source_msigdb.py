#!/usr/bin/env python

from loki import loki_source

import collections


class Source_msigdb(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '2.0 (2015-04-22)'
	#getVersionString()
	
	
	def download(self, options):
		pass
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('pathway',    0),
			('entrez_gid', 0),
		])
		typeID = self.addTypes([
			('pathway',),
			('gene',),
		])
		
		# process groups
		self.log("processing gene sets ...")
		setGroup = set()
		entrezAssoc = list()
		numAssoc = 0
		with open('c5.all.v5.0.entrez.gmt','rU') as gsFile:
			for line in gsFile:
				words = line.split("\t")
				label = words[0]
				setGroup.add(label)
				for i in xrange(2,len(words)):
					numAssoc += 1
					entrezAssoc.append( (label,numAssoc,words[i]) )
			#foreach line in gsFile
		#with gsFile
		listGroup = list(setGroup)
		setGroup = None
		self.log(" OK: %d gene sets, %d associations\n" % (len(listGroup),len(entrezAssoc)))
		
		# store groups
		self.log("writing gene sets to the database ...")
		listGID = self.addTypedGroups(typeID['pathway'], ((label,None) for label in listGroup))
		groupGID = dict(zip(listGroup,listGID))
		self.log(" OK\n")
		
		# store pathway names
		self.log("writing gene set names to the database ...")
		self.addGroupNamespacedNames(namespaceID['pathway'], ((gid,label) for label,gid in groupGID.iteritems()))
		self.log(" OK\n")
		
		# store gene associations
		self.log("writing gene associations to the database ...")
		self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID['entrez_gid'], ((groupGID[label],n,entrezID) for label,n,entrezID in entrezAssoc))
		self.log(" OK\n")
	#update()
	
#Source_msigdb
