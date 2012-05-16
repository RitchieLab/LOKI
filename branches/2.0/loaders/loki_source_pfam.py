#!/usr/bin/env python

import loki_source


class Source_pfam(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def getDependencies(cls):
		return ('entrez',)
	#getDependencies()
	
	
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
		with self.bulkUpdateContext(set(['group','group_name','group_group','group_region'])):
			self.log(" OK\n")
			
			# clear out all old data from this source
			self.log("deleting old records from the database ...")
			self.deleteSourceData()
			self.log(" OK\n")
			
			# get or create the required metadata records
			namespaceID = {
				'uniprot':        self.addNamespace('uniprot'),
				'pfam':           self.addNamespace('pfam'),
				'protein_family': self.addNamespace('protein_family'),
			}
			relationshipID = {
				'':               self.addRelationship(''),
			}
			typeID = {
				'gene':           self.addType('gene'),
				'protein_family': self.addType('protein_family'),
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
			setAssoc = set()
			setFlagAssoc = set()
			setOrphan = set()
			setAmbig = set()
			setUnrec = set()
			for line in assocFile:
				words = line.split("\t")
				if len(words) < 6:
					continue
				pfamID = words[0]
				uniprotAcc = words[5]
				uniprotID = words[6]
				species = words[8]
				
				if species == 'Homo sapiens (Human)':
					if pfamID not in pfamGID:
						setOrphan.add( (pfamID,uniprotAcc,uniprotID) )
					else:
						regionIDs = self._loki.getRegionIDsByNames(
								[uniprotAcc, uniprotID],
								[namespaceID['uniprot'], namespaceID['uniprot']],
								typeID['gene'],
								self._loki.MATCH_BEST
						)
						if len(regionIDs) == 1:
							setAssoc.add( (pfamGID[pfamID],regionIDs[0]) )
						elif len(regionIDs) > 1:
							setAmbig.add( (pfamGID[pfamID],uniprotAcc,uniprotID) )
							for regionID in regionIDs:
								setFlagAssoc.add( (pfamGID[pfamID],regionID) )
						else:
							setUnrec.add( (pfamGID[pfamID],uniprotAcc,uniprotID) )
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
	
#Source_pfam
