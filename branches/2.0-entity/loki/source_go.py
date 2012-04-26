#!/usr/bin/env python

import sys
import os
import re

import loki.source


class Source_go(loki.source.Source):
	
	# class data
	remHost = 'ftp.geneontology.org'
	remFiles = {
		'gene_association.goa_human.gz': '/go/gene-associations/',
		'gene_ontology.1_2.obo':         '/go/ontology/obo_format_1_2/'
	}
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromFTP(self.remHost, self.remFiles)
	#download()
	
	
	def update(self):
		# process ontology terms
		# file format specification: http://www.geneontology.org/GO.format.obo-1_2.shtml
		# correctly handling all the possible escape sequences and special cases
		# in the OBO spec would be somewhat involved, but the previous version
		# of biofilter used a much simpler approach which seemed to work okay in
		# practice, so we'll stick with that for now
		reEscaped = re.compile('(?:^|[^\\\\])(?:\\\\\\\\)*\\\\$')
		termName = {}
		termNS = {}
		termDef = {}
		termLinks = {}
		oboProps = {}
		curStanza = curID = curAnon = curObs = curName = curNS = curDef = curLinks = None
		with open('gene_ontology.1_2.obo','rU') as oboFile:
			for line in oboFile:
				parts = line.split('!',1)[0].split(':',1)
				tag = parts[0].strip()
				val = parts[1].strip() if (len(parts) > 1) else None
				
				if tag.startswith('['):
					if (curStanza == 'Term') and curID and (not curAnon) and (not curObs):
						termName[curID] = curName
						termNS[curID] = curNS or (oboProps['default-namespace'][-1] if ('default-namespace' in oboProps) else None)
						termDef[curID] = curDef
						termLinks[curID] = curLinks
					curStanza = tag[1:tag.index(']')]
					curID = curAnon = curObs = curName = curNS = curDef = curLinks = None
				elif not curStanza:
					# before the first stanza, tag-value pairs are global file properties
					if tag not in oboProps:
						oboProps[tag] = []
					oboProps[tag].append(val)
				elif tag == 'id':
					curID = val
				elif tag == 'is_anonymous':
					curAnon = (val.lower().split()[0] == 'true')
				elif tag == 'is_obsolete':
					curObs = (val.lower().split()[0] == 'true')
				elif tag == 'name':
					curName = val
				elif tag == 'namespace':
					curNS = val
				elif tag == 'def':
					if val.startswith('"'):
						words = val.split('"')
						w = 1
						while w < len(words):
							if not reEscaped.search(words[w]):
								break
							words[0] += words[w]
							w += 1
					curDef = words[0]
				elif tag == 'is_a':
					if not curLinks:
						curLinks = {}
					curLinks[val.split()[0]] = 'is_a'
				elif tag == 'relationship':
					if not curLinks:
						curLinks = {}
					words = val.split()
					curLinks[words[1]] = words[0]
			#foreach line
		#with oboFile
		
		
		# process associations
		termGenes = {}
		numOrphan = numUnknown = numAmbig = 0
		assocFile = self.zfile('gene_association.goa_human.gz') #TODO:context manager,iterator
		for line in assocFile:
			words = line.split('\t')
			if len(words) < 13:
				continue
			source = words[0]
			sourceID = words[1]
			geneName = words[2]
			#assocType = words[3]
			goID = words[4]
			#reference = words[5]
			evidence = words[6]
			#withID = words[7]
			#goType = words[8]
			proteinName = words[9]
			geneAliases = words[10].split('|')
			#sourceType = words[11]
			taxon = words[12]
			#updated = words[13]
			#assigner = words[14]
			#extensions = words[15].split('|')
			#sourceIDsplice = words[16]
			
			if goID not in termName:
				numOrphan += 1
			elif source.lower().startswith('uniprotkb') and evidence != 'IEA' and taxon.lower() == 'taxon:9606':
				region_ids = self._loki.getRegionsByAlias(sourceID)
				if len(region_ids) != 1:
					found = (len(region_ids) > 0)
					region_ids = self._loki.getRegionsByAlias(geneName)
					if len(region_ids) != 1:
						found = found or (len(region_ids) > 0)
						for geneAlias in geneAliases:
							region_ids = self._loki.getRegionsByAlias(geneAlias)
							if len(region_ids) == 1:
								break
							found = found or (len(region_ids) > 0)
				if len(region_ids) == 1:
					if goID not in termGenes:
						termGenes[goID] = set()
					termGenes[goID].add(region_ids[0])
				elif found:
					numAmbig += 1
				else:
					numUnknown += 1
			#if association is ok
		#foreach association
		
		print "%d orphaned genesets; %d ambiguous gene identifiers; %d unknown genes" % (numOrphan,numAmbig,numUnknown)
		print "%d ontology groups" % len(termName)
		print "%d groups with identifiable genes" % len(termGenes)
	#update()
	
#Source_go
