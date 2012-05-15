#!/usr/bin/env python

import re
import loki_source


class Source_go(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def getDependencies(cls):
		return ('entrez',)
	#getDependencies()
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromFTP('ftp.geneontology.org', {
			'gene_association.goa_human.gz': '/go/gene-associations/gene_association.goa_human.gz',
			'gene_ontology.1_2.obo':         '/go/ontology/obo_format_1_2/gene_ontology.1_2.obo',
		})
	#download()
	
	
	def update(self):
		# begin transaction to update database
		self.log("initializing update process ...")
		with self.bulkUpdateContext(group=True, group_name=True, group_group=True, group_region=True):
			self.log(" OK\n")
			
			# clear out all old data from this source
			self.log("deleting old records from the database ...")
			self.deleteSourceData()
			self.log(" OK\n")
			
			# get or create the required metadata records
			namespaceID = {
				'gene':     self.addNamespace('gene'),
				'uniprot':  self.addNamespace('uniprot'),
				'go':       self.addNamespace('go'),
				'ontology': self.addNamespace('ontology'),
			}
			relationshipID = {
				'is_a':     self.addRelationship('is_a'),
			}
			typeID = {
				'gene':     self.addType('gene'),
				'ontology': self.addType('ontology'),
			}
			
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
						pass #TODO
					elif tag == 'def':
						if val.startswith('"'):
							words = val.split('"')
							w = 1
							while w < len(words):
								if not reTrailingEscape.search(words[w]):
									break
								words[0] += words[w]
								w += 1
						curDef = words[0]
					elif tag == 'is_anonymous':
						curAnon = (val.lower().split()[0] == 'true')
					elif tag == 'is_obsolete':
						curObs = (val.lower().split()[0] == 'true')
					elif tag == 'replaced_by':
						pass #TODO
					#elif tag == 'namespace':
					#	curNS = val
					elif tag == 'name':
						curName = val
					elif tag == 'synonym':
						pass #TODO
					elif tag == 'xref':
						pass #TODO
					elif tag == 'is_a':
						curLinks = curLinks or set()
						curLinks.add( (val.split()[0], relationshipID['is_a']) )
					elif tag == 'relationship':
						curLinks = curLinks or set()
						words = val.split()
						if words[0] not in relationshipID:
							relationshipID[words[0]] = self.addRelationship(words[0])
						curLinks.add( (words[1], relationshipID[words[0]]) )
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
			self.addNamespacedGroupNames(namespaceID['go'], ((goGID[goID],goID) for goID in listGoID))
			self.addNamespacedGroupNames(namespaceID['ontology'], ((goGID[goID],goName[goID]) for goID in listGoID))
			self.log(" OK\n")
			
			# store ontology term links
			self.log("writing ontology term links to the database ...")
			listLinks = []
			for goID in goLinks:
				for link in (goLinks[goID] or empty):
					if link[0] in goGID:
						listLinks.append( (goGID[goID],goGID[link[0]],link[1]) )
			self.addGroupGroups(listLinks)
			self.log(" OK\n")
			
			# process gene associations
			self.log("processing gene associations ...")
			assocFile = self.zfile('gene_association.goa_human.gz') #TODO:context manager,iterator
			setAssoc = set()
			setFlagAssoc = set()
			setOrphan = set()
			setAmbig = set()
			setUnrec = set()
			for line in assocFile:
				words = line.split('\t')
				if len(words) < 13:
					continue
				sourceDB = words[0]
				sourceID = words[1]
				gene = words[2]
				#assocType = words[3]
				goID = words[4]
				#reference = words[5]
				evidence = words[6]
				#withID = words[7]
				#goType = words[8]
				#desc = words[9]
				aliases = words[10].split('|')
				#sourceType = words[11]
				taxon = words[12]
				#updated = words[13]
				#assigner = words[14]
				#extensions = words[15].split('|')
				#sourceIDsplice = words[16]
				
				# TODO: why ignore IEA?
				if sourceDB == 'UniProtKB' and evidence != 'IEA' and taxon == 'taxon:9606':
					if goID not in goGID:
						setOrphan.add( (goID,sourceID,gene) )
					else:
						# aliases might be either symbols or uniprot identifiers, so try them both ways
						# actually, including aliases increases ambiguity more than it reduces unknowns; not worth it
						nameList = [sourceID, gene]
						#nameList.extend(aliases)
						#nameList.extend(aliases)
						nsList = [namespaceID['uniprot'], namespaceID['gene']]
						#nsList.extend(namespaceID['gene'] for alias in aliases)
						#nsList.extend(namespaceID['uniprot'] for alias in aliases)
						regionIDs = self._loki.getRegionIDsByNames(nameList, nsList, typeID['gene'], self._loki.MATCH_BEST)
						if len(regionIDs) == 1:
							setAssoc.add( (goGID[goID],regionIDs[0]) )
						elif len(regionIDs) > 1:
							setAmbig.add( (goGID[goID],sourceID,gene) )
							for regionID in regionIDs:
								setFlagAssoc.add( (goGID[goID],regionID) )
						else:
							setUnrec.add( (goGID[goID],sourceID,gene) )
				#if association is ok
			#foreach association
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
	
#Source_go
