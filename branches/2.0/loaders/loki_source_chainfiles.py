import loki_source
import itertools

class Source_chainfiles(loki_source.Source):
	"""
	A loader that loads all of the chainfiles into LOKI
	"""
	
	def download(self):
		"""
		Download all of the chain files
		"""
		self.downloadFilesFromHTTP("hgdownload.cse.ucsc.edu",
			{"hg16.chain.gz" : "/goldenPath/hg16/liftOver/hg16ToHg19.over.chain.gz",
			 "hg17.chain.gz" : "/goldenPath/hg17/liftOver/hg17ToHg19.over.chain.gz",
			 "hg18.chain.gz" : "/goldenPath/hg18/liftOver/hg18ToHg19.over.chain.gz"})
	#download()
		
	def update(self):
		"""
		Parse all of the chain files and insert them into the database
		"""	
		
		
		# First, let's create the "build->assembly" table
		build_translation = [('36.1',18),('36',18),('35',17),('34',16),
			('b36',18),('b35',17),('b34',16)]		
		self.addBuildTrans(build_translation)
		
		# Drop tables and recreate them
		self.log("Dropping old chain data ...")
		self._loki.dropDatabaseTables(None,'db',('chain','chain_data'))
		self._loki.createDatabaseTables(None,'db',('chain','chain_data'))
		self.log("OK\n")
		
		assy_file = {16 : "hg16.chain.gz",
					 17 : "hg17.chain.gz",
					 18 : "hg18.chain.gz"}
		
		with self.bulkUpdateContext(set(['chain','chain_data'])):
			for (assy, fn) in assy_file.iteritems():
				self.log("Parsing Chains for build " + str(assy) + " ...")
				f = self.zfile(fn)
				
				is_hdr = True
				is_valid = True
				chain_hdrs = []
				chain_data = []
				curr_data = []
				for line in f:
					if is_hdr:
						if line:
							try:
								chain_hdrs.append(self._parseChain(line))
							except:
								is_valid = False
							is_hdr = False
					elif line:
						if is_valid:
							curr_data.append(line)
					else:
						if is_valid:
							chain_data.append(self._parseData(chain_hdrs[-1], '\n'.join(curr_data)))
						is_valid = True
						curr_data = []
						is_hdr = True
				
				hdr_ids = self.addChains(assy, chain_hdrs)
				
				# Now, I want to take my list of IDs and my list of list of 
				# tuples and convert them into a list of tuples suitable for
				# entering in the chain_data table
				chain_id_data = zip(hdr_ids, chain_data)
				chain_data_itr = (tuple(itertools.chain((chn[0],),seg)) for chn in chain_id_data for seg in chn[1])
				
				self.addChainData(chain_data_itr)
				
				self.log("OK\n")
			# for (assy, fn) in assy_file.iteritems()
		# with self.BulkUpdateContext
		
	#update()
	
	def _parseChain(self, chain_hdr):
		"""
		Parses the chain header to extract the information required 
		for insertion into the database
		"""
		
		# get the 1st line
		hdr = chain_hdr.strip().split('\n')[0].strip()
		# Parse the first line
		wds = hdr.split()
		
		if wds[0] != "chain":
			raise Exception("Not a valid chain file")
		
		if wds[2][3:] not in self._loki.chr_num:
			raise Exception("Could not find chromosome: " + wds[2][3:] + "->" + wds[7][3:])
			
		is_fwd = (wds[9] == "+")
		if is_fwd:
			new_start = int(wds[10])
			new_end = int(wds[11])
		else:
			# NOTE: If we're going backward, this will mean that 
			# end < start
			new_start = int(wds[8]) - int(wds[10])
			new_end = int(wds[8]) - int(wds[11])
		
		
		# I want a tuple of (score, old_chr, old_start, old_end,
		# new_chr, new_start, new_end, is_forward)
		return (int(wds[1]), 
			self._loki.chr_num[wds[2][3:]], int(wds[5]), int(wds[6]),
			self._loki.chr_num.get(wds[7][3:],-1), new_start, new_end,
			int(is_fwd))
		
	def _parseData(self, chain_tuple, chain_data):
		"""
		Parses the chain data into a more readily usable and iterable 
		form (the data of the chain is everything after the 1st line)
		"""
		_data = [ tuple([int(v) for v in l.split()]) for l in chain_data.split('\n')[:-1] ]
		
		curr_pos = chain_tuple[2]
		new_pos = chain_tuple[5]
			
		_data_txform = []
		for l in _data:
			_data_txform.append((curr_pos, curr_pos + l[0], new_pos))
			curr_pos = curr_pos + l[0] + l[1]
			if chain_tuple[7]:
				new_pos = new_pos + l[0] + l[2]
			else:
				new_pos = new_pos - l[0] - l[2]
			
		_data_txform.append((curr_pos, curr_pos + int(chain_data.split()[-1]), new_pos))
		
		return _data_txform
	
#class Source_chainfiles
	
