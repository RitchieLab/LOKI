#!/usr/bin/env python

import loki_source


class Source_mint(loki_source.Source):
	
	
	# ##################################################
	# source interface
	
	
	def getDependencies(cls):
		return ('entrez',)
	#getDependencies()
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromFTP('mint.bio.uniroma2.it', {
			'2012-02-06-mint-human.txt': '/pub/release/txt/current/2012-02-06-mint-human.txt', #TODO: wildcard filenames
		})
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
				'gene':    self.addNamespace('gene'),
				'entrez':  self.addNamespace('entrez'),
				'refseq':  self.addNamespace('refseq'),
				'uniprot': self.addNamespace('uniprot'),
				'mint':    self.addNamespace('mint'),
			}
			typeID = {
				'gene':        self.addType('gene'),
				'interaction': self.addType('interaction'),
			}
			
			# process interation groups
			self.log("processing interaction groups ...")
			setMint = set()
			setAssoc = set()
			setFlagAssoc = set()
			setAmbig = set()
			setUnrec = set()
			with open('2012-02-06-mint-human.txt','rU') as assocFile:
				header = assocFile.next().rstrip()
				if header != "ID interactors A (baits)	ID interactors B (preys)	Alt. ID interactors A (baits)	Alt. ID interactors B (preys)	Alias(es) interactors A (baits)	Alias(es) interactors B (preys)	Interaction detection method(s)	Publication 1st author(s)	Publication Identifier(s)	Taxid interactors A (baits)	Taxid interactors B (preys)	Interaction type(s)	Source database(s)	Interaction identifier(s)	Confidence value(s)	expansion	biological roles A (baits)	biological role B	experimental roles A (baits)	experimental roles B (preys)	interactor types A (baits)	interactor types B (preys)	xrefs A (baits)	xrefs B (preys)	xrefs Interaction	Annotations A (baits)	Annotations B (preys)	Interaction Annotations	Host organism taxid	parameters Interaction	dataset	Caution Interaction	binding sites A (baits)	binding sites B (preys)	ptms A (baits)	ptms B (preys)	mutations A (baits)	mutations B (preys)	negative	inference	curation depth":
					self.log(" ERROR\n")
					self.log("unrecognized file header: %s\n" % header)
					return False
				xrefNS = {
					'entrezgene/locuslink': 'entrez',
					'refseq':               'refseq',
					'uniprotkb':            'uniprot',
				}
				l = 0
				for line in assocFile:
					l += 1
					words = line.split('\t')
					genes = words[0].split(';')
					genes.extend(words[1].split(';'))
					aliases = words[4].split(';')
					aliases.extend(words[5].split(';'))
					taxes = words[9].split(';')
					taxes.extend(words[10].split(';'))
					labels = words[13].split('|')
					
					# identify interaction group label
					mint = None
					for label in labels:
						if label.startswith('mint:'):
							mint = label
							break
					mint = mint or "MINT-unlabeled-%d" % l
					setMint.add(mint)
					
					# identify interacting genes/proteins
					for n in xrange(0,len(taxes)):
						if taxes[n] == "taxid:9606(Homo sapiens)":
							nameList = [ aliases[n], aliases[n].rsplit('.',1)[0], aliases[n].rsplit('-',1)[0] ]
							nsList = [ namespaceID['gene'], namespaceID['refseq'], namespaceID['uniprot'] ]
							xrefDB,xrefID = genes[n].split(':',1)
							if xrefDB in xrefNS:
								if xrefDB == 'uniprotkb':
									xrefID = xrefID.rsplit('-',1)[0]
								nameList.append(xrefID)
								nsList.append(namespaceID[xrefNS[xrefDB]])
							regionIDs = self._loki.getRegionIDsByNames(nameList, nsList, typeID['gene'], self._loki.MATCH_BEST)
							if len(regionIDs) == 1:
								setAssoc.add( (mint,regionIDs[0]) )
							elif len(regionIDs) > 0:
								setAmbig.add( (mint,)+tuple(nameList) )
								for regionID in regionIDs:
									setFlagAssoc.add( (mint,regionID) )
							else:
								setUnrec.add( (mint,)+tuple(nameList) )
						#if human
					#foreach interacting gene/protein
				#foreach line in assocFile
			#with assocFile
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
			
			# store interaction groups
			self.log("writing interaction groups to the database ...")
			listMint = list(setMint)
			listGID = self.addTypedGroups(typeID['interaction'], ((mint,None) for mint in listMint))
			mintGID = dict(zip(listMint,listGID))
			self.log(" OK\n")
			
			# store interaction group names
			self.log("writing interaction group names to the database ...")
			self.addNamespacedGroupNames(namespaceID['mint'], ((mintGID[mint],mint) for mint in listMint))
			self.log(" OK\n")
			
			# store gene interactions
			self.log("writing gene interactions to the database ...")
			self.addGroupRegions((mintGID[assoc[0]],assoc[1]) for assoc in setAssoc)
			self.log(" OK\n")
			
			# commit transaction
			self.log("finalizing update process ...")
		#with bulk update
		self.log(" OK\n")
	#update()
	
#Source_mint
