#!/usr/bin/env python

import re
from loki import loki_source


class Source_go(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '2.1 (2016-04-21)'
	#getVersionString()
	
	
	@classmethod
	def getSpecies(cls):
		return [3702,559292,6239,7227,7955,9606,10090,10116,208964] # ,4932,
	#getSpecies()
	
	
	def download(self, options):
		# download the latest source files
		if self._tax_id == 3702:
			species = 'tair'
		elif self._tax_id == 559292 or self._tax_id == 4932:
			species = 'sgd'
		elif self._tax_id == 6239:
			species = 'wb'
		elif self._tax_id == 7227:
			species = 'fb'
		elif self._tax_id == 7955:
			species = 'zfin'
		elif self._tax_id == 10090:
			species = 'mgi'
		elif self._tax_id == 10116:
			species = 'rgd'
		elif self._tax_id == 208964:
			species = 'pseudocap'
		else: # 9606
			species = 'goa_human'
		#if _tax_id
		remFiles = {
			'gene_ontology.1_2.obo'             : '/go/ontology/obo_format_1_2/gene_ontology.1_2.obo',
			('gene_association.'+species+'.gz') : ('/go/gene-associations/gene_association.'+species+'.gz'),
		}
		self.downloadFilesFromFTP('ftp.geneontology.org', remFiles)
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('go_id',       0),
			('ontology',    0),
			('symbol',      0),
			('uniprot_pid', 1),
			('mgi_id',      0),
			('tair_id',     0),
			('sgd_id',      0),
			('flybase_id',  0),
			('zfin_id',     0),
		])
		relationshipID = self.addRelationships([
			('is_a',),
		])
		typeID = self.addTypes([
			('ontology',),
			('gene',),
		])
		
		# process ontology terms
		self.log("processing ontology terms ...")
		# file format specification: http://www.geneontology.org/GO.format.obo-1_2.shtml
		# correctly handling all the possible escape sequences and special cases
		# in the OBO spec would be somewhat involved, but the previous version
		# of biofilter used a much simpler approach which seemed to work okay in
		# practice, so we'll stick with that for now
		reTrailingEscape = re.compile('(?:^|[^\\\\])(?:\\\\\\\\)*\\\\$')
		empty = tuple()
		goName = {}
		goDef = {}
		goLinks = {}
		#goNS = {}
		#oboProps = {}
		curStanza = curID = curAnon = curObs = curName = curNS = curDef = curLinks = None
		with open('gene_ontology.1_2.obo','rU') as oboFile:
			while True:
				try:
					line = oboFile.next().rstrip()
					parts = line.split('!',1)[0].split(':',1)
					tag = parts[0].strip()
					val = parts[1].strip() if (len(parts) > 1) else None
				except StopIteration:
					line = False
				
				if line == False or tag.startswith('['):
					if (curStanza == 'Term') and curID and (not curAnon) and (not curObs):
						goName[curID] = curName
						goDef[curID] = curDef
						goLinks[curID] = curLinks or empty
				#		goNS[curID] = curNS or (oboProps['default-namespace'][-1] if ('default-namespace' in oboProps) else None)
					if line == False:
						break
					curStanza = tag[1:tag.index(']')]
					curID = curAnon = curObs = curName = curNS = curDef = curLinks = None
				#elif not curStanza:
				#	# before the first stanza, tag-value pairs are global file properties
				#	if tag not in oboProps:
				#		oboProps[tag] = []
				#	oboProps[tag].append(val)
				elif tag == 'id':
					curID = val
				elif tag == 'alt_id':
					pass
				elif tag == 'def':
					curDef = val
					if val.startswith('"'):
						curDef = ''
						words = val.split('"')
						for w in xrange(1,len(words)):
							curDef += words[w]
							if not reTrailingEscape.search(words[w]):
								break
				elif tag == 'is_anonymous':
					curAnon = (val.lower().split()[0] == 'true')
				elif tag == 'is_obsolete':
					curObs = (val.lower().split()[0] == 'true')
				elif tag == 'replaced_by':
					pass
				#elif tag == 'namespace':
				#	curNS = val
				elif tag == 'name':
					curName = val
				elif tag == 'synonym':
					pass
				elif tag == 'xref':
					pass
				elif tag == 'is_a':
					curLinks = curLinks or set()
					curLinks.add( (val.split()[0], relationshipID['is_a'], -1) )
				elif tag == 'relationship':
					curLinks = curLinks or set()
					words = val.split()
					if words[0] not in relationshipID:
						relationshipID[words[0]] = self.addRelationship(words[0])
					if words[0] == 'part_of':
						contains = -1
					elif words[0] in ('regulates','positively_regulates','negatively_regulates'):
						contains = 0
					else:
						contains = None
					curLinks.add( (words[1], relationshipID[words[0]], contains) )
			#foreach line
		#with oboFile
		numTerms = len(goName)
		numLinks = sum(len(goLinks[goID]) for goID in goLinks)
		self.log(" OK: %d terms, %d links\n" % (numTerms,numLinks))
		
		# store ontology terms
		self.log("writing ontology terms to the database ...")
		listGoID = goName.keys()
		listGID = self.addTypedGroups(typeID['ontology'], ((goName[goID],goDef[goID]) for goID in listGoID))
		goGID = dict(zip(listGoID,listGID))
		self.log(" OK\n")
		
		# store ontology term names
		self.log("writing ontology term names to the database ...")
		self.addGroupNamespacedNames(namespaceID['go_id'], ((goGID[goID],goID) for goID in listGoID))
		self.addGroupNamespacedNames(namespaceID['ontology'], ((goGID[goID],goName[goID]) for goID in listGoID))
		self.log(" OK\n")
		
		# store ontology term links
		self.log("writing ontology term relationships to the database ...")
		listLinks = []
		for goID in goLinks:
			for link in (goLinks[goID] or empty):
				if link[0] in goGID:
					listLinks.append( (goGID[goID],goGID[link[0]],link[1],link[2]) )
		self.addGroupRelationships(listLinks)
		self.log(" OK\n")
		
		# process gene associations
		self.log("processing gene associations ...")
		if self._tax_id == 3702:
			species = 'tair'
		elif self._tax_id == 559292 or self._tax_id == 4932:
			species = 'sgd'
		elif self._tax_id == 6239:
			species = 'wb'
		elif self._tax_id == 7227:
			species = 'fb'
		elif self._tax_id == 7955:
			species = 'zfin'
		elif self._tax_id == 10090:
			species = 'mgi'
		elif self._tax_id == 10116:
			species = 'rgd'
		elif self._tax_id == 208964:
			species = 'pseudocap'
		else: # 9606
			species = 'goa_human'
		#if _tax_id
		assocFile = self.zfile('gene_association.'+species+'.gz') #TODO:context manager,iterator
		xrefNS = {
			'UniProtKB': 'uniprot_pid',
			'MGI':       'mgi_id',
			'TAIR':      'tair_id',
			'SGD':       'sgd_id',
			'FB':        'flybase_id',
			'ZFIN':      'zfin_id',
		}
		nsAssoc = {
			'symbol':      set(),
		}
		for xref,ns in xrefNS.iteritems():
			nsAssoc[ns] = set()
		numAssoc = numID = 0
		for line in assocFile:
			words = line.split('\t')
			if len(words) < 13:
				continue
			xrefDB = words[0]
			xrefID = words[1]
			gene = words[2]
			#assocType = words[3]
			goID = words[4]
			#reference = words[5]
			evidence = words[6]
			#withID = words[7]
			#goType = words[8]
			#desc = words[9]
			aliases = words[10].split('|')
			#xrefType = words[11]
			taxon = int(words[12].split(':')[-1])
			#updated = words[13]
			#assigner = words[14]
			#extensions = words[15].split('|')
			#xrefIDsplice = words[16]
			
			# TODO: find out for sure why the old Biofilter loader ignores IEA
			if (goID in goGID) and (evidence != 'IEA') and (taxon == self._tax_id):
				numAssoc += 1
				ns = xrefNS.get(xrefDB)
				if (gene != "-"):
					numID += 1
					nsAssoc['symbol'].add( (goGID[goID],numAssoc,gene) )
				if (ns != None):
					numID += 1
					nsAssoc[ns].add( (goGID[goID],numAssoc,xrefID) )
				for alias in aliases:
					numID += 1
					# aliases might be either symbols or xref identifiers, so try them both ways
					nsAssoc['symbol'].add( (goGID[goID],numAssoc,alias) )
					if (ns != None):
						nsAssoc[ns].add( (goGID[goID],numAssoc,alias) )
			#if association is ok
		#foreach association
		self.log(" OK: %d associations (%d identifiers)\n" % (numAssoc,numID))
		
		# store gene associations
		self.log("writing gene associations to the database ...")
		for ns in nsAssoc:
			self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID[ns], nsAssoc[ns])
		self.log(" OK\n")
	#update()
	
#Source_go
