import loki_source

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
		Parse all of the chain files
		"""
		
	#update()
	
#class Source_chainfiles
	
