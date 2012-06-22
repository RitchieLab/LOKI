#!/usr/bin/env python

import suds.client
import loki_source


class Source_kegg(loki_source.Source):
	
	
	def download(self):
		pass
	#download()
	
	
	def update(self):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('kegg_id',    0),
			('entrez_gid', 0),
		])
		typeID = self.addTypes([
			('pathway',),
			('gene',),
		])
		
		# connect to SOAP/WSDL service
		self.log("connecting to KEGG data service ...")
		service = suds.client.Client('http://soap.genome.jp/KEGG.wsdl').service
		self.log(" OK\n")
		
		# fetch pathway list
		self.log("fetching pathways ...")
		listPathways = [ (pathway['entry_id'][0],pathway['definition'][0]) for pathway in service.list_pathways('hsa') ]
		self.log(" OK: %d pathways\n" % len(listPathways))
		
		# store pathways
		self.log("writing pathways to the database ...")
		listGIDs = self.addTypedGroups(typeID['pathway'], listPathways)
		self.log(" OK\n")
		
		# store pathway names
		self.log("writing pathway names to the database ...")
		self.addGroupNamespacedNames(namespaceID['kegg_id'], ((listGIDs[n],listPathways[n][0]) for n in xrange(len(listGIDs))))
		self.log(" OK\n")
		
		# fetch genes for each pathway
		self.log("fetching gene associations ...")
		setAssoc = set()
		numAssoc = 0
		for n in xrange(len(listPathways)):
			for hsaGene in service.get_genes_by_pathway(listPathways[n][0]):
				numAssoc += 1
				setAssoc.add( (listGIDs[n],numAssoc,hsaGene[4:]) )
			#foreach association
		#foreach pathway
		self.log(" OK: %d associations\n" % (numAssoc,))
		
		# store gene associations
		self.log("writing gene associations to the database ...")
		self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID['entrez_gid'], setAssoc)
		self.log(" OK\n")
	#update()
	
#Source_kegg
