#!/usr/bin/env python

import zipfile
import loki_source


class Source_reactome(loki_source.Source):
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromHTTP('www.reactome.org', {
			'homo_sapiens.interactions.txt.gz': '/download/current/homo_sapiens.interactions.txt.gz',
			'uniprot_2_pathways.stid.txt':      '/download/current/uniprot_2_pathways.stid.txt',
			'gene_association.reactome':        '/download/current/gene_association.reactome',
			'ReactomePathways.gmt.zip':         '/download/current/ReactomePathways.gmt.zip',
		})
	#download()
	
	
	def update(self):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('symbol',       0),
			('entrez_gid',   0),
			('ensembl_gid',  0),
			('ensembl_pid',  1),
			('uniprot_gid',  0),
			('uniprot_pid',  1),
		])
		typeID = self.addTypes([
			('gene',),
			('pathway',),
		])
		
		# process interaction associations
		self.log("processing gene interactions ...")
		iaReactGenes = dict()
		iaReactLinks = dict()
		iaFile = self.zfile('homo_sapiens.interactions.txt.gz') #TODO:context manager,iterator
		for line in iaFile:
			words = line.split("\t")
			if line.startswith('#') or len(words) < 6:
				continue
			uniprotID1 = words[0][8:]  if words[0].startswith('UniProt:')     else None
			ensemblID1 = words[1][8:]  if words[1].startswith('ENSEMBL:')     else None
			entrezID1  = words[2][12:] if words[2].startswith('Entrez Gene:') else None
			uniprotID2 = words[3][8:]  if words[3].startswith('UniProt:')     else None
			ensemblID2 = words[4][8:]  if words[4].startswith('ENSEMBL:')     else None
			entrezID2  = words[5][12:] if words[5].startswith('Entrez Gene:') else None
			reactIDs = words[7].split('<->')
			if len(reactIDs) == 1 or reactIDs[0] == reactIDs[1]:
				reactID1 = reactIDs[0].split('.',1)[0]
				
				if reactID1 not in iaReactGenes:
					iaReactGenes[reactID1] = set()
				iaReactGenes[reactID1].add(uniprotID1)
				iaReactGenes[reactID1].add(uniprotID2)
			else:
				reactID1 = reactIDs[0].split('.',1)[0]
				reactID2 = reactIDs[1].split('.',1)[0]
				
				if reactID1 not in iaReactLinks:
					iaReactLinks[reactID1] = set()
				iaReactLinks[reactID1].add(reactID2)
				
				if reactID2 not in iaReactLinks:
					iaReactLinks[reactID2] = set()
				iaReactLinks[reactID2].add(reactID1)
		#foreach line in iaFile
		for r in iaReactGenes.keys():
			if len(iaReactGenes[r]) < 2:
				del iaReactGenes[r]
		iaGroups = self.findConnectedComponents(iaReactLinks)
		self.log(" OK: %d reactions, %d merged groups (%d/%f/%d)\n" % (
				len(iaReactGenes.keys()),
				len(iaGroups),
				min(len(g) for g in iaGroups),
				sum(len(g) for g in iaGroups) * 1.0 / len(iaGroups),
				max(len(g) for g in iaGroups)
		))
		
		# process reaction associations
		self.log("processing protein reactions ...")
		raReactGenes = dict()
		raReactLinks = dict()
		with open('gene_association.reactome','rU') as raFile:
			for line in raFile:
				words = line.split("\t")
				if line.startswith('#') or len(words) < 12:
					continue
				sourceDB = words[0]
				sourceID = words[1]
				reactID = words[5][9:] if words[5].startswith("REACTOME:") else None
				taxon = words[12][6:] if words[12].startswith("taxon:") else None
				
				if reactID and taxon == "9606":
					if sourceDB == "UniProtKB":
						if reactID not in raReactGenes:
							raReactGenes[reactID] = set()
						raReactGenes[reactID].add(sourceID)
					elif sourceDB == "Reactome":
						if sourceID not in raReactLinks:
							raReactLinks[sourceID] = set()
						raReactLinks[sourceID].add(reactID)
						
						if reactID not in raReactLinks:
							raReactLinks[reactID] = set()
						raReactLinks[reactID].add(sourceID)
			#foreach line in raFile
		#with raFile
		for r in raReactGenes.keys():
			if len(raReactGenes[r]) < 2:
				del raReactGenes[r]
		raGroups = self.findConnectedComponents(raReactLinks)
		self.log(" OK: %d reactions, %d merged groups (%d/%f/%d)\n" % (
				len(raReactGenes.keys()),
				len(raGroups),
				min(len(g) for g in raGroups),
				sum(len(g) for g in raGroups) * 1.0 / len(raGroups),
				max(len(g) for g in raGroups)
		))
		
		iaReact = set(iaReactGenes.keys())
		raReact = set(raReactGenes.keys())
		self.log("groups: %d / %d / %d\n" % (
				len(iaReact - raReact),
				len(iaReact & raReact),
				len(raReact - iaReact)
		))
		numLeft = numMatch = numRight = 0
		for r in (iaReact & raReact):
			numLeft += len(iaReactGenes[r] - raReactGenes[r])
			numMatch += len(iaReactGenes[r] & raReactGenes[r])
			numRight += len(raReactGenes[r] - iaReactGenes[r])
		self.log("genes: %d / %d (%1.3f) / %d\n" % (numLeft,numMatch,numMatch*100.0/(numLeft+numMatch+numRight),numRight))
		
		#TODO
		
	#update()
	
#Source_reactome
