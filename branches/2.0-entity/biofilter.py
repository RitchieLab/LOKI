#!/usr/bin/env python

import sys
import os
import argparse

import loki


class Biofilter:
	
	# ##################################################
	# public class data
	
	ver_maj,ver_min,ver_rev,ver_date = 0,0,2,'2012-01-23'
	
	
	# ##################################################
	# private class data
	
	_schema = { #TODO
		'main': {
			# ########## main.region ##########
			'region': {
				'table': """
(
  workreg_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(64),
  region_id INTEGER,
  regiontype_id TINYINT,
  population_id INTEGER,
  chr TINYINT,
  posMin BIGINT UNSIGNED,
  posMax BIGINT UNSIGNED
)
""",
				'index': {
					'region__label': '(label)',
					'region__region_id': '(region_id)',
					'region__posmin': '(chr,posMin)',
					'region__posmax': '(chr,posMax)'
				}
			}, #.main.region
			
			# ########## main.region_zone ##########
			'region_zone': {
				'table': """
(
  workreg_id INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER UNSIGNED NOT NULL,
  PRIMARY KEY (workreg_id,zone)
)
""",
				'index': {
					'region_zone__zone': '(chr,zone)'
				}
			}, #.main.region_zone
			
			# ########## main.variant ##########
			'variant': {
				'table': """
(
  workvar_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(64) NOT NULL,
  rs BIGINT UNSIGNED,
  chr TINYINT,
  pos BIGINT UNSIGNED
)
""",
				'index': {
					'variant__label': '(label)',
					'variant__rs': '(rs)',
					'variant__pos': '(chr,pos)'
				}
			}, #.main.variant
		}, #.main
		
		'temp': {
			# ########## temp.region ##########
			'region': {
				'table': """
(
  tempreg_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(64),
  region_id INTEGER,
  regiontype_id TINYINT,
  population_id INTEGER,
  chr TINYINT,
  posMin BIGINT UNSIGNED,
  posMax BIGINT UNSIGNED
)
""",
				'index': {}
			}, #.temp.region
			
			# ########## temp.rs ##########
			'rs': {
				'table': """
(
  rs INTEGER PRIMARY KEY NOT NULL
)
""",
				'index': {}
			}, #.temp.rs
			
			# ########## temp.variant ##########
			'variant': {
				'table': """
(
  tempvar_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(64) NOT NULL,
  rs BIGINT UNSIGNED,
  chr TINYINT,
  pos BIGINT UNSIGNED
)
""",
				'index': {}
			}, #.temp.variant
		}, #.temp
	} #_schema{}
	
	
	# ##################################################
	# constructor
	
	def __init__(self):
		# initialize instance properties
		self._iwd = os.getcwd()
		self._expand = 0
		self._population_id = 0
		
		# initialize instance database
		self._loki = loki.db.Database()
		self._loki.createDatabaseObjects(self._schema['main'], 'main')
		self._loki.setVerbose(True)
	#__init__()
	
	
	# ##################################################
	# instance management
	
	def changeDirectory(self, path):
		try:
			os.chdir(self._iwd if path == "-" else path)
		except OSError as e:
			sys.exit("ERROR: %s" % e)
		sys.stderr.write("OK: %s\n" % os.getcwd())
	#changeDirectory()
	
	
	def readVariantsFromMarkers(self, markers, separator=':'):
		varList = []
		for marker in markers:
			label = rs = chm = pos = None
			cols = marker.split(separator)
			
			# parse line
			if len(cols) == 2:
				chm = cols[0].upper()
				pos = cols[1].upper()
			elif len(cols) == 3:
				chm = cols[0].upper()
				label = cols[1]
				pos = cols[2].upper()
			elif len(cols) >= 4:
				chm = cols[0].upper()
				label = cols[1]
				pos = cols[3].upper()
			else:
				sys.exit("ERROR: malformed marker '%s', expected 'chr:pos' or 'chr:label:pos'" % marker)
			
			# parse, validate and convert chromosome
			if chm[:3] == 'CHR':
				chm = chm[3:]
			if chm not in self._loki.chr_num:
				sys.exit("ERROR: malformed marker '%s', unknown chromosome" % marker)
			chm = self._loki.chr_num[chm]
			
			# parse and convert marker label
			if not label:
				label = 'chr%s:%s' % (self.loki.chr_name[chm], pos)
			elif label[:2].upper() == 'RS' and label[2:].isdigit():
				rs = long(label[2:])
			
			# parse and convert position
			if pos == '-' or pos == 'NA':
				pos = None
			else:
				pos = long(pos)
			
			varList.append( (label,rs,chm,pos) )
		#foreach marker
		return varList
	#readVariantsFromMarkers()
	
	
	def readVariantsFromMapFiles(self, files):
		varList = []
		for filePtr in files:
			sys.stderr.write("processing '%s' ..." % filePtr.name)
			sys.stderr.flush()
			lines = []
			for line in filePtr:
				if len(line) > 0 and line[0] != '#':
					lines.append(line.rstrip())
			varList.extend(self.readVariantsFromMarkers(lines, "\t"))
			sys.stderr.write(" OK: %d variants\n" % len(lines))
		#foreach file
		return varList
	#readVariantsFromMapFiles()
	
	
	def addVariants(self, varList): #TODO
		with self._loki:
			sys.stderr.write("adding variants to working set ...\n")
			
			# load variants into temp table
			self._loki.createDatabaseTables(self._schema['temp'], 'temp', 'variant')
			self._loki.dbc.executemany("INSERT INTO temp.variant (label,rs,chr,pos) VALUES (?,?,?,?)", varList)
			
			# translate merged rs#s
			mergeList = []
			for row in self._loki.dbc.execute("SELECT sm.rs, v.tempvar_id FROM temp.variant AS v JOIN db.snp_merge AS sm ON sm.rsOld = v.rs"):
				mergeList.append( (row[0],row[1]) )
			self._loki.dbc.executemany("UPDATE temp.variant SET rs=? WHERE tempvar_id=?", mergeList)
			numMerge = len(mergeList)
			
			# load variants into working set, using rs# to fill in missing chr/pos
			self._loki.dropDatabaseIndexes(self._schema['main'], 'main', 'variant')
			self._loki.dbc.execute("""
INSERT OR IGNORE INTO main.variant (label,rs,chr,pos)
SELECT
  v.label,
  v.rs,
  COALESCE(v.chr,s.chr),
  COALESCE(v.pos,s.pos)
FROM temp.variant AS v
LEFT JOIN db.snp AS s
  ON s.rs = v.rs
  AND (v.chr IS NULL OR s.chr = v.chr)
  AND (v.pos IS NULL OR s.pos = v.pos)
""")
			numAdd = self._loki.db.changes()
			self._loki.createDatabaseIndexes(self._schema['main'], 'main', 'variant')
			self._loki.dropDatabaseTables(self._schema['temp'], 'temp', 'variant')
			
			# print stats
			sys.stderr.write(" OK: %d added (%d merged rs#s updated)\n" % (numAdd,numMerge))
			sys.stderr.write("verifying ...")
			sys.stderr.flush()
			ttl = self._loki.dbc.execute("SELECT COUNT(1) FROM main.variant").next()[0]
			sys.stderr.write(" OK: %d variants in working set\n" % ttl)
		#with db transaction
		
		return True
	#addVariants()
	
	
	def readSNPsFromRSFiles(self, files):
		snpList = []
		for filePtr in files:
			sys.stderr.write("processing '%s' ..." % filePtr.name)
			fileList = []
			for line in filePtr:
				if len(line) > 0 and line[0] != '#':
					fileList.append( (long(line),) )
			#foreach line in file
			sys.stderr.write(" OK: %d SNPs\n" % len(fileList))
			snpList.extend(fileList)
		#foreach file
		return snpList
	#readSNPsFromRSFiles()
	
	
	def addSNPs(self, snpList): #TODO
		with self._loki.db:
			sys.stderr.write("adding SNPs to working set ...")
			sys.stderr.flush()
			
			# load variants into temp table
			self._loki.createDatabaseTables(self._schema['temp'], 'temp', 'rs')
			self._loki.dbc.executemany("INSERT OR IGNORE INTO temp.rs (rs) VALUES (?)", snpList)
			numDupe = len(snpList) - self._loki.dbc.execute("SELECT COUNT(1) FROM temp.rs").next()[0]
			
			# translate merged rs#s
			mergeList = []
			for row in self._loki.dbc.execute("SELECT sm.rs, r.rs FROM temp.rs AS r JOIN db.snp_merge AS sm ON sm.rsOld = r.rs"):
				mergeList.append( (row[0],row[1]) )
			self._loki.dbc.executemany("UPDATE OR IGNORE temp.rs SET rs=? WHERE rs=?", mergeList)
			self._loki.dbc.executemany("DELETE FROM temp.rs WHERE rs!=? AND rs=?", mergeList)
			for row in self._loki.dbc.execute("SELECT COUNT(1) FROM temp.rs"):
				numLeft = row[0]
			numDrop = len(snpList) - numDupe - numLeft
			numMerge = len(mergeList)
			
			# load variants into working set, using rs# to fill in chr/pos
			self._loki.dropDatabaseIndexes(self._schema['main'], 'main', 'variant')
			self._loki.dbc.execute("""
INSERT OR IGNORE INTO main.variant (label,rs,chr,pos)
SELECT
  'rs'||r.rs,
  r.rs,
  s.chr,
  s.pos
FROM temp.rs AS r
LEFT JOIN db.snp AS s ON s.rs = r.rs
""")
			numAdd = self._loki.db.changes()
			self._loki.createDatabaseIndexes(self._schema['main'], 'main', 'variant')
			self._loki.dropDatabaseTables(self._schema['temp'], 'temp', 'rs')
			
			# print stats
			sys.stderr.write(" OK: %d added (%d updated, %d duplicates)\n" % (numAdd,numMerge-numDrop,numDupe+numDrop))
			sys.stderr.write("verifying ...")
			sys.stderr.flush()
			for row in self._loki.dbc.execute("SELECT COUNT(1) FROM main.variant"):
				ttl = row[0]
			sys.stderr.write(" OK: %d variants in working set\n" % ttl)
		#with db transaction
	#addSNPs()
	
	
	def outputVariants(self, target=sys.stdout): #TODO
		target.write("#chr\tlabel\tpos\n")
		# sqlite3's string concat operator is ||
		for row in self._loki.dbc.execute("""
SELECT
  COALESCE(chr, 'NA') AS chr,
  COALESCE(label, 'rs'||rs, 'chr'||chr||':'||pos, '#'||_rowid_) AS label,
  COALESCE(pos, 'NA') AS pos
FROM main.variant
"""
		):
			target.write("%s\t%s\t%s\n" % row)
	#outputVariants()
	
	
	def outputVariantRegions(self, target=sys.stdout): #TODO
		target.write("#chr\tlabel\tpos"
				+"\tgene_name.match\tgene_start.match\tgene_end.match"
				+"\tgene_name.upstream\tgene_start.upstream\tgene_end.upstream"
				+"\tgene_name.downstream\tgene_start.downstream\tgene_end.downstream"
				+"\n"
		)
		aliastype_id = self._loki.dbc.execute("SELECT aliastype_id FROM aliastype WHERE aliastype = ?", ('entrez_name',)).next()[0]
		# sqlite3's string concat operator is ||
		for row in self._loki.dbc.execute("""
SELECT
  COALESCE(vU.chr, 'NA') AS chr,
  COALESCE(vU.label, 'rs'||vU.rs, 'chr'||vU.chr||':'||vU.pos, '!#'||vU.rowid) AS label,
  COALESCE(vU.pos, 'NA') AS pos,
  vU.aliasM,
  vU.posMinM,
  vU.posMaxM,
  vU.aliasU,
  vU.posMinU,
  vU.posMaxU,
  (CASE WHEN raD.alias IS NULL THEN '' ELSE GROUP_CONCAT(DISTINCT raD.alias) END) AS aliasD,
  (CASE WHEN raD.alias IS NULL THEN '' ELSE MIN(rbD.posMin) END) AS posMinD,
  (CASE WHEN raD.alias IS NULL THEN '' ELSE MAX(rbD.posMax) END) AS posMaxD
FROM (
  SELECT
    vM._rowid_,
    vM.label,
    vM.rs,
    vM.chr,
    vM.pos,
    vM.aliasM,
    vM.posMinM,
    vM.posMaxM,
    (CASE WHEN raU.alias IS NULL THEN '' ELSE GROUP_CONCAT(DISTINCT raU.alias) END) AS aliasU,
    (CASE WHEN raU.alias IS NULL THEN '' ELSE MIN(rbU.posMin) END) AS posMinU,
    (CASE WHEN raU.alias IS NULL THEN '' ELSE MAX(rbU.posMax) END) AS posMaxU,
    vM.rbD_id
  FROM (
    SELECT
      v._rowid_,
      v.label,
      v.rs,
      v.chr,
      v.pos,
      (CASE WHEN raM.alias IS NULL THEN '' ELSE GROUP_CONCAT(DISTINCT raM.alias) END) AS aliasM,
      (CASE WHEN raM.alias IS NULL THEN '' ELSE MIN(rbM.posMin) END) AS posMinM,
      (CASE WHEN raM.alias IS NULL THEN '' ELSE MAX(rbM.posMax) END) AS posMaxM,
      (SELECT rbU.region_id FROM region_bound AS rbU WHERE rbU.population_id = :population_id AND rbU.chr = v.chr AND rbU.posMax < v.pos - :expand ORDER BY rbU.posMax DESC LIMIT 1) AS rbU_id,
      (SELECT rbD.region_id FROM region_bound AS rbD WHERE rbD.population_id = :population_id AND rbD.chr = v.chr AND rbD.posMin > v.pos + :expand ORDER BY rbD.posMin ASC LIMIT 1) AS rbD_id
    FROM main.variant AS v
    LEFT JOIN db.region_zone AS rzM
      ON rzM.population_id = :population_id
      AND rzM.chr = v.chr
      AND rzM.zone >= (v.pos - :expand) / 100000
      AND rzM.zone <= (v.pos + :expand) / 100000
    LEFT JOIN db.region_bound AS rbM
      ON rbM.region_id = rzM.region_id
      AND rbM.population_id = rzM.population_id
      AND rbM.chr = v.chr
      AND rbM.posMin <= v.pos + :expand
      AND rbM.posMax >= v.pos - :expand
    LEFT JOIN db.region AS rM
      ON rM.regiontype_id = 1
      AND rM.region_id = rbM.region_id
    LEFT JOIN db.region_alias AS raM
      ON raM.region_id = rM.region_id
      AND raM.aliastype_id = :aliastype_id
    GROUP BY v._rowid_
  ) AS vM
  LEFT JOIN db.region_bound AS rbU
    ON rbU.region_id = vM.rbU_id
    AND rbU.population_id = :population_id
    AND rbU.chr = vM.chr
    AND rbU.posMax < vM.pos - :expand
  LEFT JOIN db.region AS rU
    ON rU.regiontype_id = 1
    AND rU.region_id = rbU.region_id
  LEFT JOIN db.region_alias AS raU
    ON raU.region_id = rU.region_id
    AND raU.aliastype_id = :aliastype_id
  GROUP BY vM._rowid_
) AS vU
LEFT JOIN db.region_bound AS rbD
  ON rbD.region_id = vU.rbD_id
  AND rbD.population_id = :population_id
  AND rbD.chr = vU.chr
  AND rbD.posMin > vU.pos + :expand
LEFT JOIN db.region AS rD
  ON rD.regiontype_id = 1
  AND rD.region_id = rbD.region_id
LEFT JOIN db.region_alias AS raD
  ON raD.region_id = rD.region_id
  AND raD.aliastype_id = :aliastype_id
GROUP BY vU.rowid
""", { 'expand':self._expand, 'population_id':self._population_id, 'aliastype_id':aliastype_id }
		):
			target.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % row)
	#outputVariantRegions()
	
#Biofilter


class Biofilter_ArgParse(argparse.Action):
	def __call__(self, parser, namespace, values, option_string=None):
		setattr(namespace, 'action', True)


class Biofilter_ArgParse_Database(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> database %s\n" % values)
		namespace.biofilter._loki.attachDatabaseFile(values)


class Biofilter_ArgParse_ChDir(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> chdir %s\n" % values)
		namespace.biofilter.changeDirectory(values)


class Biofilter_ArgParse_Update(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> update %s\n" % values)
		namespace.biofilter._loki.updateDatabase(values)


class Biofilter_ArgParse_Marker(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> marker %s\n" % values)
		varList = namespace.biofilter.readVariantsFromMarkers(values)
		namespace.biofilter.addVariants(varList)


class Biofilter_ArgParse_MapFile(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> mapfile %s\n" % [k.name for k in values])
		varList = namespace.biofilter.readVariantsFromMapFiles(values)
		namespace.biofilter.addVariants(varList)


class Biofilter_ArgParse_SNP(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> snp %s\n" % values)
		namespace.biofilter.addSNPs(values)


class Biofilter_ArgParse_SNPFile(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> snpfile %s\n" % [k.name for k in values])
		snpList = namespace.biofilter.readSNPsFromRSFiles(values)
		namespace.biofilter.addSNPs(snpList)


class Biofilter_ArgParse_Expand(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> expand %s\n" % values)
		if values[-1:].upper() == 'K':
			namespace.biofilter._expand = int(float(values[:-1]) * 1000)
		else:
			namespace.biofilter._expand = int(values)
		sys.stderr.write("OK: region boundary expansion set to %d\n" % namespace.biofilter._expand)


class Biofilter_ArgParse_Output(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> output %s\n" % values)
		if values == 'v':
			namespace.biofilter.outputVariants()
		elif values == 'v:dg':
			namespace.biofilter.outputVariantRegions()


class Biofilter_ArgParse_Version(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write(
"""Biofilter version %d.%d.%d (%s)
     LOKI version %d.%d.%d (%s)
%9s version %s
%9s version %s
""" % (
			Biofilter.ver_maj, Biofilter.ver_min, Biofilter.ver_rev, Biofilter.ver_date,
			loki.db.Database.ver_maj, loki.db.Database.ver_min, loki.db.Database.ver_rev, loki.db.Database.ver_date,
			loki.db.Database.getDatabaseDriverName(), loki.db.Database.getDatabaseDriverVersion(),
			loki.db.Database.getDatabaseInterfaceName(), loki.db.Database.getDatabaseInterfaceVersion()
		))


class Biofilter_ArgParse_NotImplemented(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> %s %s\n" % (self.dest, values))
		sys.stderr.write("NOT YET IMPLEMENTED\n")


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	
	parser.add_argument('-d', '--database',
			type=str, metavar='filename', action=Biofilter_ArgParse_Database,
			help="specify the database file to use"
	)
	
	parser.add_argument('--cd', '--chdir',
			type=str, metavar='pathname', action=Biofilter_ArgParse_ChDir,
			help="change the current working directory, from which relative paths to input and output files are resolved; "
			+"the special path '-' returns to the initial directory when the program was started"
	)
	
	parser.add_argument('-u', '--update',
			type=str, metavar='data', nargs='+', action=Biofilter_ArgParse_Update,
			help="update the database file by downloading and processing new source data of the specified type; "
			+"files will be downloaded into a 'loki.cache' subdirectory of the current working directory and left in place "
			+"afterwards, so that future updates can avoid re-downloading source data which has not changed"
	)
	
	parser.add_argument('-m', '--marker',
			type=str, metavar='marker', nargs='+', action=Biofilter_ArgParse_Marker,
			help="load variants into the working set by marker ('chr:pos' or 'chr:label:pos')"
	)
	parser.add_argument('-M', '--mapfile',
			type=argparse.FileType('r'), metavar='filename', nargs='+', action=Biofilter_ArgParse_MapFile,
			help="load variants into the working set by reading markers from one or more .map files"
	)
	
	parser.add_argument('-s', '--snp',
			type=str, metavar='rs#', nargs='+', action=Biofilter_ArgParse_SNP,
			help="load variants into the working set by rs#"
	)
	parser.add_argument('-S', '--snpfile',
			type=argparse.FileType('r'), metavar='filename', nargs='+', action=Biofilter_ArgParse_SNPFile,
			help="load variants into the working set by reading rs#s from one or more files"
	)
	
	#parser.add_argument('-r', '--region',
			#type=str, metavar='chr:pos-pos', nargs='+', action=Biofilter_ArgParse_NotImplemented,
			#help="load regions into the working set by locus range (chr:pos-pos)"
	#)
	#parser.add_argument('-R', '--regionfile',
			#type=argparse.FileType('r'), metavar='filename', nargs='+', action=Biofilter_ArgParse_NotImplemented,
			#help="load regions into the working set by reading locus ranges from one or more files"
	#)
	
	#parser.add_argument('-g', '--gene',
			#type=str, metavar='alias/tag', nargs='+', action=Biofilter_ArgParse_Gene,
			#help="load regions into the working set by gene alias or special tag: "
			#+"':d' loads all known gene regions from the database; "
			#+"':v' loads gene regions from the database using the working set of variants; "
			#+"':c' clears all regions from the current working set"
	#)
	#parser.add_argument('-G', '--genefile',
			#type=argparse.FileType('r'), metavar='filename', nargs='+', action=Biofilter_ArgParse_NotImplemented,
			#help="load regions into the working set by reading gene aliases from one or more files"
	#)
	
	parser.add_argument('-x', '--expand',
			type=str, metavar='num', action=Biofilter_ArgParse_Expand,
			help="when matching region boundaries to locii, expand the boundaries by this amount; "
			+"the suffix 'k' multiplies the amount by 1000"
	)
	parser.add_argument('-p', '--population',
			type=str, metavar='label', action=Biofilter_ArgParse_NotImplemented,
			help="when matching region boundaries to locii, expand the boundaries according to the linkage disequilibrium calculations stored in the database"
	)
	
	parser.add_argument('-o', '--output',
			type=str, metavar='data', action=Biofilter_ArgParse_Output,
			help="outputs data from the working sets according to the requested type: "
			+"'v' lists all variants; "
			+"'v:dg' annotates variants against known genes"
	)
	
	parser.add_argument('--version', nargs=0, action=Biofilter_ArgParse_Version)
	
	ns = argparse.Namespace()
	ns.biofilter = Biofilter()
	args = parser.parse_args(namespace=ns)
	
	if not hasattr(args, 'action'):
		print "Biofilter version %d.%d.%d (%s)" % (Biofilter.ver_maj, Biofilter.ver_min, Biofilter.ver_rev, Biofilter.ver_date)
		print "     LOKI version %d.%d.%d (%s)" % (loki.db.Database.ver_maj, loki.db.Database.ver_min, loki.db.Database.ver_rev, loki.db.Database.ver_date)
		print
		parser.print_usage()
		print
		print "Use -h for details."
#__main__


"""
h	help
d	database
u	update

s	variants - rs#
m	variants - map
g	regions - genes
r	regions - map
t	groups - pathways
	groups - genesets

p	population
x	expansion
o	output
"""
