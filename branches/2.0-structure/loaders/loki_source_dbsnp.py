#!/usr/bin/env python

import loki_source


class Source_dbsnp(loki_source.Source):
	
	
	# ##################################################
	# private class data
	
	
	_chmList = ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','X','Y','MT')
	_remHost = 'ftp.ncbi.nih.gov'
	_remFiles = {
		'RsMergeArch.bcp.gz': '/snp/organisms/human_9606/database/organism_data/RsMergeArch.bcp.gz',
	}
	for chm in _chmList:
		_remFiles['chr_%s.txt.gz' % chm] = '/snp/organisms/human_9606/chr_rpts/chr_%s.txt.gz' % chm
	
	
	# ##################################################
	# source interface
	
	
	def download(self):
		# download the latest source files
		self.downloadFilesFromFTP(self._remHost, self._remFiles)
	#download()
	
	
	def update(self):
		# begin transaction to update database
		self.log("initializing update process ...")
		with self.bulkUpdateContext(snp=True, snp_merge=True):
			self.log(" OK\n")
			
			# clear out all old SNP data
			self.log("deleting old records from the database ...")
			self.deleteSourceData()
			self.log(" OK\n")
			
			# process chromosome report files
			for fileChm in self._chmList:
				self.log("processing chromosome %s SNPs ..." % fileChm)
				chmFile = self.zfile('chr_%s.txt.gz' % fileChm)
				
				# verify file headers
				header1 = chmFile.next().rstrip()
				chmFile.next()
				chmFile.next()
				header2 = chmFile.next().rstrip()
				header3 = chmFile.next().rstrip()
				chmFile.next()
				chmFile.next()
				if header1 != "dbSNP Chromosome Report":
					self.log(" ERROR\n")
					self.log("unrecognized file header: %s\n" % header1)
					return False
				elif header2 != "rs#\tmap\tsnp\tchr\tctg\ttotal\tchr\tctg\tctg\tctg\tctg\tchr\tlocal\tavg\ts.e.\tmax\tvali-\tgeno-\tlink\torig\tupd":
					self.log(" ERROR\n")
					self.log("unrecognized file subheader: %s\n" % header2)
					return False
				elif header3 != "\twgt\ttype\thits\thits\thits\t\tacc\tver\tID\tpos\tpos\tloci\thet\thet\tprob\tdated\ttypes\touts\tbuild\tbuild":
					self.log(" ERROR\n")
					self.log("unrecognized file subheader: %s\n" % header3)
					return False
				
				# process lines
				setPos = set()
				setNoGRCh = set()
				setInvalid = set()
				setMismatch = set()
				for line in chmFile:
					words = line.split("\t")
					rs = words[0].strip()
					chm = words[6].strip()
					pos = words[11].strip()
					valid = int(words[16])
					build = words[21]
					
					if rs != '' and chm != '' and pos != '':
						rs = long(rs)
						pos = long(pos)
						if not build.startswith("GRCh"):
							setNoGRCh.add(rs)
						elif valid <= 0:
							setInvalid.add(rs)
						elif chm != fileChm:
							setMismatch.add(rs)
						else:
							setPos.add( (rs,pos) )
					#if rs/chm/pos provided
				#foreach line in chmFile
				setSNP = set(pos[0] for pos in setPos)
				setNoGRCh.difference_update(setSNP)
				setInvalid.difference_update(setSNP, setNoGRCh)
				setMismatch.difference_update(setSNP, setNoGRCh, setInvalid)
				self.log(" OK: %d SNP positions (%d RS#s)\n" % (len(setPos),len(setSNP)))
				self.logPush()
				if setNoGRCh:
					self.log("WARNING: %d SNPs not mapped to GRCh build\n" % (len(setNoGRCh)))
				if setInvalid:
					self.log("WARNING: %d SNPs not validated\n" % (len(setInvalid)))
				if setMismatch:
					self.log("WARNING: %d SNPs on mismatching chromosome\n" % (len(setMismatch)))
				self.logPop()
				
				# store this set of SNPs and free the memory
				self.log("writing chromosome %s SNPs to the database ..." % fileChm)
				self.addChromosomeSNPs(self._loki.chr_num[fileChm], setPos)
				setPos = setSNP = setNoGRCh = setInvalid = setMismatch = None
				self.log(" OK\n")
			#foreach chromosome
			
			# process merge report (no header!)
			self.log("processing SNP merge records ...")
			mergeFile = self.zfile('RsMergeArch.bcp.gz') #TODO:context manager,iterator
			listMerge = []
			numMerge = 0
			for line in mergeFile:
				words = line.split("\t")
				if len(words) <= 6:
					continue
				rsOld = long(words[0])
				rsNew = long(words[1])
				rsCur = long(words[6])
				
				numMerge += 1
				listMerge.append( (rsOld,rsNew,rsCur) )
				
				# write to the database after each million, to keep memory usage down
				if len(listMerge) >= 1000000:
					self.log(" %d so far\n" % numMerge)
					self.log("writing SNP merge records to the database ...")
					self.addSNPMerges(listMerge)
					listMerge = []
					self.log(" OK\n")
					self.log("processing SNP merge records ...")
			#foreach line in mergeFile
			
			# print final total and write the remaining records
			self.log(" OK: %d merged RS#s\n" % numMerge)
			self.log("writing SNP merge records to the database ...")
			self.addSNPMerges(listMerge)
			listMerge = None
			self.log(" OK\n")
			
			# commit transaction
			self.log("finalizing update process ...")
		#with bulk update
		self.log(" OK\n")
	#update()
	
#Source_dbsnp
