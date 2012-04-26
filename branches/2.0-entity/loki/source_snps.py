#!/usr/bin/env python

import sys
import os

import loki.source


class Source_snps(loki.source.Source):
	
	# ##################################################
	# private class data
	
	_remHost = 'ftp.ncbi.nih.gov'
	_remFiles = {
		'RsMergeArch.bcp.gz': '/snp/organisms/human_9606/database/organism_data/'
	}
	for c in loki.db.Database.chr_list:
		if c != 'XY':
			_remFiles['chr_%s.txt.gz' % c] = '/snp/organisms/human_9606/chr_rpts/'
	
	
	# ##################################################
	# source interface
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromFTP(self._remHost, self._remFiles)
	#download()
	
	
	def update(self):
		# get or create the required metadata records
		namespaceID = self._loki.addNamespace('rs#')
		populationID = self._loki.addPopulation('N/A')
		typeID = self._loki.addType('snp')
		
		# process chromosome report files
		snpList = []
		for c in self._loki.chr_list:
			if c != 'XY':
				if self._verbose:
					sys.stderr.write("processing chr_%s.txt.gz ..." % c)
					sys.stderr.flush()
				chrFile = self.zfile('chr_%s.txt.gz' % c)
				
				# verify file headers
				header1 = chrFile.next().rstrip()
				chrFile.next()
				chrFile.next()
				header2 = chrFile.next().rstrip()
				header3 = chrFile.next().rstrip()
				chrFile.next()
				chrFile.next()
				if header1 != "dbSNP Chromosome Report":
					if self._verbose:
						sys.stderr.write(" error!\n")
						sys.stderr.write("unrecognized file header: %s\n" % header1)
					return False
				elif header2 != "rs#\tmap\tsnp\tchr\tctg\ttotal\tchr\tctg\tctg\tctg\tctg\tchr\tlocal\tavg\ts.e.\tmax\tvali-\tgeno-\tlink\torig\tupd":
					if self._verbose:
						sys.stderr.write(" error!\n")
						sys.stderr.write("unrecognized file subheader: %s\n" % header2)
					return False
				elif header3 != "\twgt\ttype\thits\thits\thits\t\tacc\tver\tID\tpos\tpos\tloci\thet\thet\tprob\tdated\ttypes\touts\tbuild\tbuild":
					if self._verbose:
						sys.stderr.write(" error!\n")
						sys.stderr.write("unrecognized file subheader: %s\n" % header3)
					return False
				
				# process lines
				num = 0
				for line in chrFile:
					words = line.split("\t")
					if words[21].startswith("GRCh"):
						rs = words[0].strip()
						chm = words[6].strip()
						pos = words[11].strip()
						valid = int(words[16])
						if rs != '' and chm != '' and pos != '' and valid > 0:
							rs = long(rs)
							chm = self._loki.chr_num[chm]
							pos = long(pos)
							snpList.append( (rs,chm,pos) )
							num += 1
					#if build is GRCh
				#foreach line in chrFile
				
				# print results
				if self._verbose:
					sys.stderr.write(" OK: %d SNPs\n" % (num))
			#if chr!=XY
		#foreach chr
		
		sys.exit()
		
		# begin transaction to update database
		if self._verbose:
			sys.stderr.write("initializing update process ...")
			sys.stderr.flush()
		with self._loki.bulkUpdateContext(entity=True, entity_name=True, entity_region=True):
			if self._verbose:
				sys.stderr.write(" OK\n")
			
			# clear out all old SNP data
			if self._verbose:
				sys.stderr.write("deleting old SNP records from the database ...")
				sys.stderr.flush()
			self._loki.deleteSourceData(self._sourceID)
			if self._verbose:
				sys.stderr.write(" OK\n")
			
			
			# process merge report (no header!)
			if self._verbose:
				sys.stderr.write("processing RsMergeArch.bcp.gz ...")
				sys.stderr.flush()
			num = missing = 0
			mergeFile = self.zfile('RsMergeArch.bcp.gz') #TODO:context manager,iterator
			for line in mergeFile:
				cols = line.split("\t")
				if len(cols) > 6:
					oldRS = 'rs'+cols[0]
					curRS = 'rs'+cols[6]
					if curRS in rsEntityID:
						self._loki.addEntityName(rsEntityID[curRS], namespaceID, oldRS, self._sourceID)
						num += 1
					else:
						missing += 1
			#foreach line in mergeFile
			if self._verbose:
				sys.stderr.write(" OK: %d merged aliases (%d unrecognized)\n" % (num,missing))
			
			# commit transaction
			if self._verbose:
				sys.stderr.write("finalizing update process ...")
				sys.stderr.flush()
		#with bulk update
		if self._verbose:
			sys.stderr.write(" OK\n")
	#update()
	
#Source_snps
