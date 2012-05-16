#!/usr/bin/env python

import suds.client
import loki_source


class Source_kegg(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def getDependencies(cls):
		return ('entrez',)
	#getDependencies()
	
	
	def download(self):
		pass
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
				'entrez':  self.addNamespace('entrez'),
				'kegg':    self.addNamespace('kegg'),
			}
			typeID = {
				'gene':    self.addType('gene'),
				'pathway': self.addType('pathway'),
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
			setAssoc = set()
			setFlagAssoc = set()
			setAmbig = set()
			setUnrec = set()
			for n in xrange(len(listPathways)):
				for hsaGene in service.get_genes_by_pathway(listPathways[n][0]):
					regionIDs = self._loki.getRegionIDsByName(hsaGene[4:], namespaceID['entrez'], typeID['gene'], self._loki.MATCH_BEST)
					if len(regionIDs) == 1:
						setAssoc.add( (listGIDs[n],regionIDs[0]) )
					elif len(regionIDs) > 1:
						setAmbig.add( (listGIDs[n],hsaGene[4:]) )
						for regionID in regionIDs:
							setFlagAssoc.add( (listGIDs[n],regionID) )
					else:
						setUnrec.add( (listGIDs[n],hsaGene[4:]) )
				#foreach association
			#foreach pathway
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
	
#Source_kegg
