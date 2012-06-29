#!/usr/bin/env python

import loki_source


class Source_dbsnp(loki_source.Source):
	
	
	##################################################
	# private class data
	
	
	_chmList = ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','X','Y','MT')
	_remHost = 'ftp.ncbi.nih.gov'
	_remFiles = {
		'RsMergeArch.bcp.gz':                '/snp/organisms/human_9606/database/organism_data/RsMergeArch.bcp.gz',
		'SnpFunctionCode.bcp.gz':            '/snp/organisms/human_9606/database/shared_data/SnpFunctionCode.bcp.gz',
		'b135_SNPContigLocusId_37_3.bcp.gz': '/snp/organisms/human_9606/database/organism_data/b135_SNPContigLocusId_37_3.bcp.gz',
	}
	for chm in _chmList:
		_remFiles['chr_%s.txt.gz' % chm] = '/snp/organisms/human_9606/chr_rpts/chr_%s.txt.gz' % chm
	
	
	##################################################
	# source interface
	
	
	@classmethod
	def getOptions(cls):
		return {
			'snp-loci': '[all|validated]  --  store all or only validated SNP loci (default: all)'
		}
	#getOptions()
	
	
	def validateOptions(self, options):
		for o,v in options.iteritems():
			if o == 'snp-loci':
				v = v.strip().lower()
				if 'all'.startswith(v):
					v = 'all'
				elif 'validated'.startswith(v):
					v = 'validated'
				else:
					return "snp-loci must be 'all' or 'validated'"
				options[o] = v
			else:
				return "unknown option '%s'" % o
		return True
	#validateOptions()
	
	
	def download(self, options):
		# download the latest source files
		self.downloadFilesFromFTP(self._remHost, self._remFiles)
	#download()
	
	
	def update(self, options):
		# clear out all old data from this source
		self.log("deleting old records from the database ...")
		self.deleteAll()
		self.log(" OK\n")
		
		# process merge report (no header!)
		self.log("processing SNP merge records ...")
		mergeFile = self.zfile('RsMergeArch.bcp.gz') #TODO:context manager,iterator
		numMerge = 0
		listMerge = list()
		for line in mergeFile:
			words = line.split("\t")
			if len(words) <= 6:
				continue
			rsOld = long(words[0])
			#rsNew = long(words[1])
			rsCur = long(words[6])
			
			numMerge += 1
			listMerge.append( (rsOld,rsCur) )
			
			# write to the database after each million, to keep memory usage down
			if len(listMerge) >= 1000000:
				self.log(" %1.f million so far\n" % (numMerge/1000000.0)) #TODO: time estimate
				self.log("writing SNP merge records to the database ...")
				self.addSNPMerges(listMerge)
				listMerge = list()
				self.log(" OK\n")
				self.log("processing SNP merge records ...")
		#foreach line in mergeFile
		self.log(" OK: %d merged RS#s\n" % numMerge)
		
		# write any remaining records
		if listMerge:
			self.log("writing SNP merge records to the database ...")
			self.addSNPMerges(listMerge)
			self.log(" OK\n")
		listMerge = None
		
		# process SNP role function codes
		""" /* from dbSNP_main_table.sql.gz */
CREATE TABLE [SnpFunctionCode]
(
[code] [tinyint] NOT NULL ,
[abbrev] [varchar](20) NOT NULL ,
[descrip] [varchar](255) NOT NULL ,
[create_time] [smalldatetime] NOT NULL ,
[top_level_class] [char](5) NOT NULL ,
[is_coding] [tinyint] NOT NULL ,
[is_exon] [bit] NULL ,
[var_prop_effect_code] [int] NULL ,
[var_prop_gene_loc_code] [int] NULL ,
[SO_id] [varchar](32) NULL
)
"""
		self.log("processing SNP role codes ...")
		roleID = dict()
		codeFile = self.zfile('SnpFunctionCode.bcp.gz')
		for line in codeFile:
			words = line.split('\t')
			code = int(words[0])
			name = words[1]
			desc = words[2]
			coding = int(words[5]) if (len(words) > 5 and words[5] != '') else None
			exon = int(words[6]) if (len(words) > 6 and words[6] != '') else None
			
			roleID[code] = self.addRole(name, desc, coding, exon)
		#foreach line in codeFile
		self.log(" OK: %d codes\n" % len(roleID))
		
		# process SNP roles
		""" /* from human_9606_table.sql.gz */
CREATE TABLE [b135_SNPContigLocusId_37_3]
(
[snp_id] [int] NOT NULL ,
[contig_acc] [varchar](32) NULL ,
[contig_ver] [tinyint] NULL ,
[asn_from] [int] NULL ,
[asn_to] [int] NULL ,
[locus_id] [int] NULL ,
[locus_symbol] [varchar](128) NULL ,
[mrna_acc] [varchar](32) NOT NULL ,
[mrna_ver] [smallint] NOT NULL ,
[protein_acc] [varchar](32) NULL ,
[protein_ver] [smallint] NULL ,
[fxn_class] [int] NOT NULL ,
[reading_frame] [int] NULL ,
[allele] [varchar](256) NULL ,
[residue] [varchar](1024) NULL ,
[aa_position] [int] NULL ,
[build_id] [varchar](4) NOT NULL ,
[ctg_id] [int] NULL ,
[mrna_start] [int] NULL ,
[mrna_stop] [int] NULL ,
[codon] [varchar](1024) NULL ,
[protRes] [char](3) NULL ,
[contig_gi] [int] NULL ,
[mrna_gi] [int] NOT NULL ,
[mrna_orien] [tinyint] NULL ,
[cp_mrna_ver] [smallint] NULL ,
[cp_mrna_gi] [int] NULL ,
[verComp] [varchar](7) NULL
)
"""
		self.log("processing SNP roles ...")
		listRole = list()
		numRole = 0
		setOrphan = set()
		numOrphan = 0
		funcFile = self.zfile('b135_SNPContigLocusId_37_3.bcp.gz')
		for line in funcFile:
			words = line.split("\t")
			rs = long(words[0])
			entrez = int(words[5])
			#genesymbol = words[6]
			code = int(words[11])
			
			if code not in roleID:
				setOrphan.add(code)
				numOrphan += 1
			else:
				listRole.append( (rs,entrez,roleID[code]) )
				numRole += 1
			
			# write to the database after each 2.5 million, to keep memory usage down
			if len(listRole) >= 2500000:
				self.log(" %1.1f million so far\n" % (numRole/1000000.0)) #TODO: time estimate
				self.log("writing SNP roles to the database ...")
				self.addSNPEntrezRoles(listRole)
				listRole = list()
				self.log(" OK\n")
				self.log("processing SNP roles ...")
		#foreach line in funcFile
		self.log(" OK: %d roles\n" % (numRole,))
		self.logPush()
		if setOrphan:
			self.log("WARNING: %d roles (%d codes) unrecognized\n" % (numOrphan,len(setOrphan)))
		setOrphan = None
		self.logPop()
		
		# write any remaining records
		if listRole:
			self.log("writing SNP roles to the database ...")
			self.addSNPEntrezRoles(listRole)
			self.log(" OK\n")
		listRole = None
		
		# process chromosome report files
		grcBuild = grcNum = None
		snpLociValid = (options.get('snp-loci') == 'validated')
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
				raise Exception("ERROR: unrecognized file header '%s'" % header1)
			if header2 != "rs#\tmap\tsnp\tchr\tctg\ttotal\tchr\tctg\tctg\tctg\tctg\tchr\tlocal\tavg\ts.e.\tmax\tvali-\tgeno-\tlink\torig\tupd":
				raise Exception("ERROR: unrecognized file subheader '%s'" % header2)
			if header3 != "\twgt\ttype\thits\thits\thits\t\tacc\tver\tID\tpos\tpos\tloci\thet\thet\tprob\tdated\ttypes\touts\tbuild\tbuild":
				raise Exception("ERROR: unrecognized file subheader '%s'" % header3)
			
			# process lines
			setPos = set()
			setBadBuild = set()
			setBadVers = set()
			setBadValid = set()
			setBadChr = set()
			for line in chmFile:
				words = line.split("\t")
				rs = words[0].strip()
				chm = words[6].strip()
				pos = words[11].strip()
				valid = 1 if (int(words[16]) > 0) else 0
				build = words[21]
				
				if rs != '' and chm != '' and pos != '':
					rs = long(rs)
					pos = long(pos)
					if not build.startswith("GRCh"):
						setBadBuild.add(rs)
					elif grcBuild and grcBuild != build:
						setBadVers.add(rs)
					elif snpLociValid and not valid:
						setBadValid.add(rs)
					elif chm != fileChm:
						setBadChr.add(rs)
					else:
						if not grcBuild:
							grcBuild = build
							try:
								grcNum = int(build[4:].split('.')[0])
							except ValueError:
								raise Exception("ERROR: unrecognized GRCh build format '%s'" % build)
						setPos.add( (rs,pos,valid) )
				#if rs/chm/pos provided
			#foreach line in chmFile
			
			# print results
			setSNP = set(pos[0] for pos in setPos)
			setBadChr.difference_update(setSNP)
			setBadValid.difference_update(setSNP, setBadChr)
			setBadVers.difference_update(setSNP, setBadChr, setBadValid)
			setBadBuild.difference_update(setSNP, setBadChr, setBadValid, setBadVers)
			self.log(" OK: %d SNPs, %d loci\n" % (len(setSNP),len(setPos)))
			self.logPush()
			if setBadBuild:
				self.log("WARNING: %d SNPs not mapped to GRCh build\n" % (len(setBadBuild)))
			if setBadVers:
				self.log("WARNING: %d SNPs mapped to GRCh build version other than %s\n" % (len(setBadVers),grcBuild))
			if setBadValid:
				self.log("WARNING: %d SNPs not validated\n" % (len(setBadValid)))
			if setBadChr:
				self.log("WARNING: %d SNPs on mismatching chromosome\n" % (len(setBadChr)))
			self.logPop()
			
			# store data
			self.log("writing chromosome %s SNPs to the database ..." % fileChm)
			self.addChromosomeSNPLoci(self._loki.chr_num[fileChm], setPos)
			setPos = setSNP = setBadBuild = setBadVers = setBadValid = setBadChr = None
			self.log(" OK\n")
		#foreach chromosome
		
		# store source metadata
		self.setSourceBuild(grcNum)
	#update()
	
#Source_dbsnp
