#!/usr/bin/env python

import loki_source


class Source_pfam(loki_source.Source):
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromFTP('ftp.sanger.ac.uk', {
			'pfamA.txt.gz':    '/pub/databases/Pfam/current_release/database_files/pfamA.txt.gz',
			'seq_info.txt.gz': '/pub/databases/Pfam/current_release/database_files/seq_info.txt.gz',
		})
	#download()
	
	
	def update(self):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('pfam_id',       0),
			('proteinfamily', 0),
			('uniprot_pid',   1),
		])
		relationshipID = self.addRelationships([
			('',),
		])
		typeID = self.addTypes([
			('proteinfamily',),
			('gene',),
		])
		
		# process protein families
		self.log("processing protein families ...")
		pfamFile = self.zfile('pfamA.txt.gz') #TODO:context manager,iterator
		groupPFam = {}
		pfamName = {}
		pfamDesc = {}
		for line in pfamFile:
			words = line.split("\t")
			pfamID = words[1]
			name = words[2]
			desc = words[4]
			group = words[8]
			
			if group not in groupPFam:
				groupPFam[group] = set()
			groupPFam[group].add(pfamID)
			pfamName[pfamID] = name
			pfamDesc[pfamID] = desc
		numGroup = len(groupPFam)
		numPfam = len(pfamName)
		self.log(" OK: %d categories, %d families\n" % (numGroup,numPfam))
		
		# store protein families
		self.log("writing protein families to the database ...")
		listGroup = groupPFam.keys()
		listGID = self.addTypedGroups(typeID['proteinfamily'], ((group,"") for group in listGroup))
		groupGID = dict(zip(listGroup,listGID))
		listPFam = pfamName.keys()
		listGID = self.addTypedGroups(typeID['proteinfamily'], ((pfamName[pfamID],pfamDesc[pfamID]) for pfamID in listPFam))
		pfamGID = dict(zip(listPFam,listGID))
		self.log(" OK\n")
		
		# store protein family names
		self.log("writing protein family names to the database ...")
		self.addGroupNamespacedNames(namespaceID['pfam_id'], ((groupGID[group],group) for group in listGroup))
		self.addGroupNamespacedNames(namespaceID['pfam_id'], ((pfamGID[pfamID],pfamID) for pfamID in listPFam))
		self.addGroupNamespacedNames(namespaceID['proteinfamily'], ((pfamGID[pfamID],pfamName[pfamID]) for pfamID in listPFam))
		self.log(" OK\n")
		
		# store protein family meta-group links
		self.log("writing protein family links to the database ...")
		for group in groupPFam:
			self.addGroupRelationships( (pfamGID[pfamID],groupGID[group],relationshipID['']) for pfamID in groupPFam[group] )
		self.log(" OK\n")
		
		# process associations
		self.log("processing gene associations ...")
		assocFile = self.zfile('seq_info.txt.gz') #TODO:context manager,iterator
		setAssoc = set()
		numAssoc = numID = 0
		for line in assocFile:
			words = line.split("\t")
			if len(words) < 6:
				continue
			pfamID = words[0]
			uniprotAcc = words[5]
			uniprotID = words[6]
			species = words[8]
			
			if pfamID in pfamGID and species == 'Homo sapiens (Human)':
				numAssoc += 1
				numID += 2
				setAssoc.add( (pfamGID[pfamID],numAssoc,uniprotAcc) )
				setAssoc.add( (pfamGID[pfamID],numAssoc,uniprotID) )
			#if association is ok
		#foreach association
		self.log(" OK: %d associations (%d identifiers)\n" % (numAssoc,numID))
		
		# store gene associations
		self.log("writing gene associations to the database ...")
		self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID['uniprot_pid'], setAssoc)
		self.log(" OK\n")
	#update()
	
#Source_pfam
