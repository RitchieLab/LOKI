#!/usr/bin/env python

import suds.client
import loki_source


class Source_kegg(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def download(self):
		pass
	#download()
	
	
	def update(self):
		# begin transaction to update database
		self.log("initializing update process ...")
		with self.bulkUpdateContext(group=True, group_name=True, group_region=True):
			self.log(" OK\n")
			
			# clear out all old data from this source
			self.log("deleting old records from the database ...")
			self.deleteSourceData()
			self.log(" OK\n")
			
			# get or create the required metadata records
			namespaceID = {
				'kegg':    self.addNamespace('kegg'),
				'entrez':  self.addNamespace('entrez'),
			}
			typeID = {
				'pathway': self.addType('pathway'),
				'gene':    self.addType('gene'),
			}
			
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
			self.addNamespacedGroupNames(namespaceID['kegg'], ((listGIDs[n],listPathways[n][0]) for n in xrange(len(listGIDs))))
			self.log(" OK\n")
			
			# fetch genes for each pathway
			self.log("fetching gene associations ...")
			setLiteral = set()
			numAssoc = 0
			for n in xrange(len(listPathways)):
				size = 0
				for hsaGene in service.get_genes_by_pathway(listPathways[n][0]):
					size += 1
					setLiteral.add( (listGIDs[n],size,namespaceID['entrez'],hsaGene[4:]) )
				#foreach association
				numAssoc += size
			#foreach pathway
			numLiteral = len(setLiteral)
			self.log(" OK: %d associations (%d identifiers)\n" % (numAssoc,numLiteral))
			
			# store gene associations
			self.log("writing gene associations to the database ...")
			self.addGroupLiterals(setLiteral)
			self.log(" OK\n")
			
			# commit transaction
			self.log("finalizing update process ...")
		#with bulk update
		self.log(" OK\n")
	#update()
	
#Source_kegg
