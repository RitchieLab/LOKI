#!/usr/bin/env python

import loki_source


class Source_mint(loki_source.Source):
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromFTP('mint.bio.uniroma2.it', {
			'2012-02-06-mint-human.txt': '/pub/release/txt/current/2012-02-06-mint-human.txt', #TODO: wildcard filenames
		})
	#download()
	
	
	def update(self):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('mint_id',     0),
			('symbol',      0),
			('entrez_gid',  0),
			('refseq_gid',  0),
			('refseq_pid',  1),
			('uniprot_pid', 1),
		])
		typeID = self.addTypes([
			('interaction',),
			('gene',),
		])
		
		# process interation groups
		self.log("processing interaction groups ...")
		setMint = set()
		nsAssoc = {
			'symbol':      set(),
			'entrez_gid':  set(),
			'refseq_gid':  set(),
			'refseq_pid':  set(),
			'uniprot_pid': set(),
		}
		numAssoc = numID = 0
		with open('2012-02-06-mint-human.txt','rU') as assocFile:
			header = assocFile.next().rstrip()
			if header != "ID interactors A (baits)	ID interactors B (preys)	Alt. ID interactors A (baits)	Alt. ID interactors B (preys)	Alias(es) interactors A (baits)	Alias(es) interactors B (preys)	Interaction detection method(s)	Publication 1st author(s)	Publication Identifier(s)	Taxid interactors A (baits)	Taxid interactors B (preys)	Interaction type(s)	Source database(s)	Interaction identifier(s)	Confidence value(s)	expansion	biological roles A (baits)	biological role B	experimental roles A (baits)	experimental roles B (preys)	interactor types A (baits)	interactor types B (preys)	xrefs A (baits)	xrefs B (preys)	xrefs Interaction	Annotations A (baits)	Annotations B (preys)	Interaction Annotations	Host organism taxid	parameters Interaction	dataset	Caution Interaction	binding sites A (baits)	binding sites B (preys)	ptms A (baits)	ptms B (preys)	mutations A (baits)	mutations B (preys)	negative	inference	curation depth":
				self.log(" ERROR\n")
				self.log("unrecognized file header: %s\n" % header)
				return False
			xrefNS = {
				'entrezgene/locuslink': ('entrez_gid',),
				'refseq':               ('refseq_gid','refseq_pid'),
				'uniprotkb':            ('uniprot_pid',),
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
						numAssoc += 1
						# the "gene" is a helpful database cross-reference with a label indicating its type
						xrefDB,xrefID = genes[n].split(':',1)
						if xrefDB in xrefNS:
							numID += 1
							if xrefDB == 'refseq':
								xrefID = xrefID.rsplit('.',1)[0]
							elif xrefDB == 'uniprotkb':
								xrefID = xrefID.rsplit('-',1)[0]
							for ns in xrefNS[xrefDB]:
								nsAssoc[ns].add( (mint,numAssoc,xrefID) )
						# but the "alias" could be of any type and isn't identified,
						# so we'll store copies under each possible type
						# and find out later which one matches something
						numID += 1
						nsAssoc['symbol'].add( (mint,numAssoc,aliases[n]) )
						nsAssoc['refseq_gid'].add( (mint,numAssoc,aliases[n].rsplit('.',1)[0]) )
						nsAssoc['refseq_pid'].add( (mint,numAssoc,aliases[n].rsplit('.',1)[0]) )
						nsAssoc['uniprot_pid'].add( (mint,numAssoc,aliases[n].rsplit('-',1)[0]) )
					#if human
				#foreach interacting gene/protein
			#foreach line in assocFile
		#with assocFile
		self.log(" OK: %d associations (%d identifiers)\n" % (numAssoc,numID))
		
		# store interaction groups
		self.log("writing interaction groups to the database ...")
		listMint = list(setMint)
		listGID = self.addTypedGroups(typeID['interaction'], ((mint,None) for mint in listMint))
		mintGID = dict(zip(listMint,listGID))
		self.log(" OK\n")
		
		# store interaction group names
		self.log("writing interaction group names to the database ...")
		self.addGroupNamespacedNames(namespaceID['mint_id'], ((mintGID[mint],mint) for mint in listMint))
		self.log(" OK\n")
		
		# store gene interactions
		self.log("writing gene interactions to the database ...")
		for ns in nsAssoc:
			self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID[ns], ((mintGID[a[0]],a[1],a[2]) for a in nsAssoc[ns]))
		self.log(" OK\n")
	#update()
	
#Source_mint
