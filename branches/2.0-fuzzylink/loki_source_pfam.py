#!/usr/bin/env python

import loki_source


class Source_pfam(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromFTP('ftp.sanger.ac.uk', {
			'pfamA.txt.gz':    '/pub/databases/Pfam/current_release/database_files/pfamA.txt.gz',
			'seq_info.txt.gz': '/pub/databases/Pfam/current_release/database_files/seq_info.txt.gz',
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
				'pfam':           self.addNamespace('pfam'),
				'protein_family': self.addNamespace('protein_family'),
				'uniprot':        self.addNamespace('uniprot'),
			}
			relationshipID = {
				'':               self.addRelationship(''),
			}
			typeID = {
				'protein_family': self.addType('protein_family'),
				'gene':           self.addType('gene'),
			}
			
			# process protein families
			self.log("processing protein families ...")
			pfamFile = self.zfile('pfamA.txt.gz') #TODO:context manager,iterator
			pfamName = {}
			pfamDesc = {}
			groupPFam = {}
			for line in pfamFile:
				words = line.split("\t")
				pfamID = words[1]
				name = words[2]
				desc = words[4]
				group = words[8]
				
				pfamName[pfamID] = name
				pfamDesc[pfamID] = desc
				if group not in groupPFam:
					groupPFam[group] = set()
				groupPFam[group].add(pfamID)
			numPfam = len(pfamName)
			numGroup = len(groupPFam)
			self.log(" OK: %d families, %d categories\n" % (numPfam,numGroup))
			
			# store protein families
			self.log("writing protein families to the database ...")
			listPFam = pfamName.keys()
			listGroup = groupPFam.keys()
			listGID = self.addTypedGroups(typeID['protein_family'], ((pfamName[pfamID],pfamDesc[pfamID]) for pfamID in listPFam))
			pfamGID = dict(zip(listPFam,listGID))
			listGID = self.addTypedGroups(typeID['protein_family'], ((group,"") for group in listGroup))
			groupGID = dict(zip(listGroup,listGID))
			self.log(" OK\n")
			
			# store protein family names
			self.log("writing protein family names to the database ...")
			self.addNamespacedGroupNames(namespaceID['pfam'], ((pfamGID[pfamID],pfamID) for pfamID in listPFam))
			self.addNamespacedGroupNames(namespaceID['protein_family'], ((pfamGID[pfamID],pfamName[pfamID]) for pfamID in listPFam))
			self.addNamespacedGroupNames(namespaceID['pfam'], ((groupGID[group],group) for group in listGroup))
			self.log(" OK\n")
			
			# store protein family meta-group links
			self.log("writing protein family links to the database ...")
			for group in groupPFam:
				self.addGroupGroups( (pfamGID[pfamID],groupGID[group],relationshipID['']) for pfamID in groupPFam[group] )
			self.log(" OK\n")
			
			# process associations
			self.log("processing gene associations ...")
			assocFile = self.zfile('seq_info.txt.gz') #TODO:context manager,iterator
			pfamSize = { pfamID:0 for pfamID in pfamGID }
			setLiteral = set()
			for line in assocFile:
				words = line.split("\t")
				if len(words) < 6:
					continue
				pfamID = words[0]
				uniprotAcc = words[5]
				uniprotID = words[6]
				species = words[8]
				
				if pfamID in pfamGID and species == 'Homo sapiens (Human)':
					pfamSize[pfamID] += 1
					setLiteral.add( (pfamGID[pfamID],pfamSize[pfamID],namespaceID['uniprot'],uniprotAcc) )
					setLiteral.add( (pfamGID[pfamID],pfamSize[pfamID],namespaceID['uniprot'],uniprotID) )
				#if association is ok
			#foreach association
			numLiteral = len(setLiteral)
			numAssoc = sum(pfamSize[pfamID] for pfamID in pfamSize)
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
	
#Source_pfam
