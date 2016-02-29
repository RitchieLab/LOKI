#!/usr/bin/env python

from loki import loki_source


class Source_kegg(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '2.1 (2016-02-29)'
	#getVersionString()
	
	
	@classmethod
	def getSpecies(cls):
		return [9606,10090]
	#getSpecies()
	
	
	@classmethod
	def getOptions(cls):
		return {
			'api': '[rest|soap|cache]  --  use the new REST API, the old SOAP API, or a local file cache (default: rest)'
		}
	#getOptions()
	
	
	def validateOptions(self, options):
		for o,v in options.iteritems():
			if o == 'api':
				v = v.strip().lower()
				if 'rest'.startswith(v):
					v = 'rest'
				elif 'soap'.startswith(v):
					v = 'soap'
				elif 'cache'.startswith(v):
					v = 'cache'
				else:
					return "api must be 'rest', 'soap' or 'cache'"
				options[o] = v
			else:
				return "unexpected option '%s'" % o
		return True
	#validateOptions()
	
	
	def download(self, options):
		if self._tax_id == 10090:
			species = 'mmu'
		else: # 9606
			species = 'hsa'
		#if _tax_id
		
		if (options.get('api') == 'cache'):
			# do nothing, update() will just expect the files to already be there
			pass
		elif (options.get('api') == 'soap'):
			# connect to SOAP/WSDL service
			import suds.client
			self.log("connecting to KEGG data service ...")
			service = suds.client.Client('http://soap.genome.jp/KEGG.wsdl').service
			self.log(" OK\n")
			
			# fetch pathway list
			self.log("fetching pathways ...")
			pathIDs = set()
			with open('list-pathway-'+species,'wb') as pathFile:
				for pathway in service.list_pathways(species):
					pathID = pathway['entry_id'][0]
					name = pathway['definition'][0]
					pathFile.write("%s\t%s\n" % (pathID,name))
					pathIDs.add(pathID)
				#foreach pathway
			#with pathway cache file
			self.log(" OK: %d pathways\n" % (len(pathIDs),))
			
			# fetch genes for each pathway
			self.log("fetching gene associations ...")
			numAssoc = 0
			with open('link-'+species+'-pathway','wb') as assocFile:
				for pathID in pathIDs:
					for gene in service.get_genes_by_pathway(pathID):
						assocFile.write("%s\t%s\n" % (pathID,gene))
						numAssoc += 1
					#foreach association
				#foreach pathway
			#with assoc cache file
			self.log(" OK: %d associations\n" % (numAssoc,))
		else: # api==rest
			self.downloadFilesFromHTTP('rest.kegg.jp', {
				('list-pathway-'+species):     ('/list/pathway/'+species),
#				('link-'+species+'-pathway'):  ('/link/'+species+'/pathway'),
				('link-pathway-'+species):     ('/link/pathway/'+species),
			})
		#if api==rest/soap/cache
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('kegg_id',    0),
			('pathway',    0),
			('entrez_gid', 0),
		])
		typeID = self.addTypes([
			('pathway',),
			('gene',),
		])
		
		# since download() stores SOAP result data in files that look like REST data,
		# we don't even have to check here -- it's the same local files either way
		if self._tax_id == 10090:
			species = 'mmu'
		else: # 9606
			species = 'hsa'
		#if _tax_id
		
		# process pathways
		self.log("processing pathways ...")
		pathName = {}
		with open('list-pathway-'+species,'rU') as pathFile:
			for line in pathFile:
				words = line.split("\t")
				pathID = words[0]
				name = words[1].rstrip()
				if self._tax_id == 10090 and name.endswith(' - Mus musculus (mouse)'):
					name = name[:-23]
				elif name.endswith(" - Homo sapiens (human)"):
					name = name[:-23]
				
				pathName[pathID] = name
			#foreach line in pathFile
		#with pathFile
		self.log(" OK: %d pathways\n" % (len(pathName),))
		
		# store pathways
		self.log("writing pathways to the database ...")
		listPath = pathName.keys()
		listGID = self.addTypedGroups(typeID['pathway'], ((pathName[pathID],None) for pathID in listPath))
		pathGID = dict(zip(listPath,listGID))
		self.log(" OK\n")
		
		# store pathway names
		self.log("writing pathway names to the database ...")
		self.addGroupNamespacedNames(namespaceID['kegg_id'], ((pathGID[pathID],pathID) for pathID in listPath))
		self.addGroupNamespacedNames(namespaceID['pathway'], ((pathGID[pathID],pathName[pathID]) for pathID in listPath))
		self.log(" OK\n")
		
		# process associations
		self.log("processing gene associations ...")
		entrezAssoc = set()
		numAssoc = 0
		if 0:
			with open('link-'+species+'-pathway','rU') as assocFile:
				for line in assocFile:
					words = line.split("\t")
					pathID = words[0]
					gene = words[1].rstrip()
					
					if (pathID in pathGID) and (gene.startswith(species+":")):
						numAssoc += 1
						entrezAssoc.add( (pathGID[pathID],numAssoc,gene[4:]) )
					#if pathway and gene are ok
				#foreach line in assocFile
		else:
			with open('link-pathway-'+species,'rU') as assocFile:
				for line in assocFile:
					words = line.split("\t")
					gene = words[0]
					pathID = words[1].strip()
					
					if (pathID in pathGID) and (gene.startswith(species+":")):
						numAssoc += 1
						entrezAssoc.add( (pathGID[pathID],numAssoc,gene[4:]) )
					#if pathway and gene are ok
				#foreach line in assocFile
		#with assocFile
		self.log(" OK: %d associations\n" % (numAssoc,))
		
		# store gene associations
		self.log("writing gene associations to the database ...")
		self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID['entrez_gid'], entrezAssoc)
		self.log(" OK\n")
	#update()
	
#Source_kegg
