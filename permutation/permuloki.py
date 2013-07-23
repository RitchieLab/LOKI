#!/usr/bin/env python

import apsw
import argparse
import collections
import decimal
import gzip
import itertools
import math
import operator
import os
import random
import sys
import time


class loggedrandom():
	@classmethod
	def init(cls, seeds=list(), interval=600):
		cls.interval = max(1,interval)
		cls.init = len(seeds)
		cls.seeds = seeds
		cls.s = 0
		cls.r = 0
	#init()
	
	@staticmethod
	def lrandom():
		if loggedrandom.r < 1:
			while loggedrandom.s >= len(loggedrandom.seeds):
				loggedrandom.seeds.append(long(os.urandom(8).__hash__()))
			random.seed(loggedrandom.seeds[loggedrandom.s])
			loggedrandom.s += 1
			loggedrandom.r = loggedrandom.interval
		loggedrandom.r -= 1
		return random.random()
	#lrandom()
#loggedrandom


def timestr(s):
	str = ''
	s,ds = int(s),(int(s*100)%100)
	m,s = (s/60),(s%60)
	h,m = (m/60),(m%60)
	if h:
		str += '%dh' % h
	if h or m:
		str += '%dm' % m
	if not (h or m or s >= 10):
		str += '%d.%02ds' % (s,ds)
	else:
		str += '%ds' % s
	return str
#timestr()


###############################################################################
# invert[GroupGenes|GeneGroups]


def invertGroupGenes(groupGenes):
	geneGroups = collections.defaultdict(set)
	for groupID,genes in groupGenes.iteritems():
		for geneID in genes:
			geneGroups[geneID].add(groupID)
	return geneGroups
#invertGroupGenes()


def invertGeneGroups(geneGroups):
	groupGenes = collections.defaultdict(set)
	for geneID,groups in geneGroups.iteritems():
		for groupID in groups:
			groupGenes[groupID].add(geneID)
	return groupGenes
#invertGeneGroups()


###############################################################################
# shuffle[Groups|Genes]By[Groups|Genes]


def shuffleGroupsByGroups(randomFunc, groupSize, geneFreq):
	groupGenes = collections.defaultdict(set)
	groupStack = collections.defaultdict(int, groupSize.iteritems())
	geneStack = list(geneFreq.elements())
	random.shuffle(geneStack,randomFunc)
	geneSkip = list()
	id0,size0 = None,None
	while groupStack:
		groupID,size = groupStack.popitem()
		genes = groupGenes[groupID]
		while size > 0:
			try:
				geneID = geneStack.pop()
				if geneID in genes:
					geneSkip.append(geneID)
				else:
					genes.add(geneID)
					size -= 1
			except IndexError:
				# if no progress was made with the last shuffle,
				# then this group must already contain all remaining genes;
				# find other finished groups to donate members instead
				if (id0 == groupID) and (size0 == size):
					groupList = groupGenes.keys()
					random.shuffle(groupList,randomFunc)
					while groupList:
						oldGroupID = groupList.pop()
						oldGenes = groupGenes[oldGroupID]
						for oldGeneID in oldGenes:
							if oldGeneID not in genes:
								oldGenes.remove(oldGeneID)
								groupStack[oldGroupID] += 1
								genes.add(oldGeneID)
								size -= 1
								break
						if size < 1:
							break
				id0,size0 = groupID,size
				geneStack,geneSkip = geneSkip,geneStack
	return groupGenes
#shuffleGroupsByGroups()


def shuffleGroupsByGenes(randomFunc, groupSize, geneFreq):
	groupGenes = collections.defaultdict(set)
	groupStack = list()
	geneStack = collections.defaultdict(int, geneFreq.iteritems())
	id0,freq0 = None,None
	while geneStack:
		geneID,freq = geneStack.popitem()
		while freq > 0:
			try:
				groupID = groupStack.pop()
				genes = groupGenes[groupID]
				if geneID not in genes:
					genes.add(geneID)
					freq -= 1
			except IndexError:
				# if no progress was made with the last shuffle,
				# then this gene must already be in all remaining groups;
				# find other finished groups to swap it into instead
				if (id0 == geneID) and (freq0 == freq):
					groupStack = groupGenes.keys()
					random.shuffle(groupStack,randomFunc)
					while freq > 0:
						genes = groupGenes[groupStack.pop()]
						if genes and (geneID not in genes):
							geneStack[genes.pop()] += 1
							genes.add(geneID)
							freq -= 1
				id0,freq0 = geneID,freq
				groupStack = list(groupID for groupID,size in groupSize.iteritems() if len(groupGenes[groupID]) < size)
				random.shuffle(groupStack,randomFunc)
	return groupGenes
#shuffleGroupsByGenes()


def shuffleGenesByGroups(randomFunc, groupSize, geneFreq):
	geneGroups = collections.defaultdict(set)
	geneStack = list()
	groupStack = collections.defaultdict(int, groupSize.iteritems())
	id0,size0 = None,None
	while groupStack:
		groupID,size = groupStack.popitem()
		while size > 0:
			try:
				geneID = geneStack.pop()
				groups = geneGroups[geneID]
				if groupID not in groups:
					groups.add(groupID)
					size -= 1
			except IndexError:
				# if no progress was made with the last shuffle,
				# then this gene must already be in all remaining groups;
				# find other finished groups to swap it into instead
				if (id0 == groupID) and (size0 == size):
					geneStack = geneGroups.keys()
					random.shuffle(geneStack,randomFunc)
					while size > 0:
						groups = geneGroups[geneStack.pop()]
						if groups and (groupID not in groups):
							groupStack[groups.pop()] += 1
							groups.add(groupID)
							size -= 1
				id0,size0 = groupID,size
				geneStack = list(geneID for geneID,freq in geneFreq.iteritems() if len(geneGroups[geneID]) < freq)
				random.shuffle(geneStack,randomFunc)
	return geneGroups
#shuffleGenesByGroups()


def shuffleGenesByGenes(randomFunc, groupSize, geneFreq):
	geneGroups = collections.defaultdict(set)
	geneStack = collections.defaultdict(int, geneFreq.iteritems())
	groupStack = list(groupSize.elements())
	random.shuffle(groupStack,randomFunc)
	groupSkip = list()
	id0,freq0 = None,None
	while geneStack:
		geneID,freq = geneStack.popitem()
		groups = geneGroups[geneID]
		while freq > 0:
			try:
				groupID = groupStack.pop()
				if groupID in groups:
					groupSkip.append(groupID)
				else:
					groups.add(groupID)
					freq -= 1
			except IndexError:
				# if no progress was made with the last shuffle,
				# then this gene must already be in all remaining groups;
				# find other finished genes to exchange groups with instead
				if (id0 == geneID) and (freq0 == freq):
					geneList = geneGroups.keys()
					random.shuffle(geneList,randomFunc)
					while geneList:
						oldGeneID = geneList.pop()
						oldGroups = geneGroups[oldGeneID]
						for oldGroupID in oldGroups:
							if oldGroupID not in groups:
								oldGroups.remove(oldGroupID)
								geneStack[oldGeneID] += 1
								groups.add(oldGroupID)
								freq -= 1
								break
						if freq < 1:
							break
				id0,freq0 = geneID,freq
				groupStack,groupSkip = groupSkip,groupStack
	return geneGroups
#shuffleGenesByGenes()


###############################################################################
# shuffle[Groups|Genes]_flat


def shuffleGenes_flat(randomFunc, groupSize, geneFreq):
	geneGroups = collections.defaultdict(set)
	geneList = geneFreq.keys()
	for groupID,size in groupSize.iteritems():
		for geneID in random.sample(geneList, size):
			geneGroups[geneID].add(groupID)
	return geneGroups
#shuffleGenes_flat()


###############################################################################
# shuffle[Groups|Genes]_fuzzy


def shuffleGroups_fuzzy(randomFunc, groupSize, geneFreq):
	groupGenes = collections.defaultdict(set)
	assignmentList = list(geneFreq.elements())
	a = len(assignmentList)
	for groupID,size in groupSize.iteritems():
		genes = set()
		while len(genes) < size:
			geneID = assignmentList[int(randomFunc() * a)]
			if geneID not in genes:
				genes.add(geneID)
		groupGenes[groupID] = genes
	return groupGenes
#shuffleGroups_fuzzy()


###############################################################################
# choose_[1|2|3] - a few implementations, #2 seems fastest


def choose_1(n,k):
	# http://stackoverflow.com/questions/3025162/statistics-combinations-in-python
	_float = float
	_xrange = xrange
	return reduce(lambda a,b:a*b, (_float(n-i)/(i+1) for i in _xrange(k)), 1)
#choose_1()


def choose_2(n,k):
	# http://stackoverflow.com/questions/3025162/statistics-combinations-in-python
	_min = min
	_xrange = xrange
	if 0 <= k <= n:
		ntok = 1
		ktok = 1
		for t in _xrange(1, _min(k, n - k) + 1):
			ntok *= n
			ktok *= t
			n -= 1
		return ntok // ktok
	return 0
#choose_2()


def choose_3(n,k):
	# http://stackoverflow.com/questions/3025162/statistics-combinations-in-python
	assert n >= 0
	assert 0 <= k <= n
	_izip = itertools.izip
	_xrange = xrange
	c = 1L
	denom = 1
	for num,denom in _izip(_xrange(n,n-k,-1), _xrange(1,k+1,1)):
		c = (c * num) // denom
	return c
#choose_3()


choose = choose_2


###############################################################################
# calcFreqSizeProb_[choose|product]


def calcFreqSizeProb_choose(numAsgn, sizes, freqs, _):
	print "generating frequency-size probability table ..."
	t0 = time.time()
	_Decimal = decimal.Decimal
	freqSizeProb = dict()
	for freq in freqs:
		AchooseF = _Decimal(choose(numAsgn,freq))
		freqSizeProb[freq] = dict( (size,((AchooseF - _Decimal(choose(numAsgn-size-1,freq))) / AchooseF)) for size in sizes )
	t1 = time.time()
	print "... OK: %d gene frequencies, %d group sizes in %.2f seconds" % (len(freqs),len(sizes),t1-t0)
	return freqSizeProb
#calcFreqSizeProb_choose()


def calcFreqSizeProb_product(numAsgn, sizes, freqs, avgFreq):
	print "generating frequency-size probability table ..."
	t0 = time.time()
	_mul = operator.mul
	sizes1 = set()
	for size in sizes:
		sizes1.add(size)
		sizes1.add(size-1)
	sizes = sorted(sizes1)
	freqSizeProb = dict()
	for freq in freqs:
		sizeProb = dict()
		p,s = 1.0,0
		for size in sizes:
			p = reduce(_mul, (((numAsgn-freq-i*avgFreq)/float(numAsgn-i*avgFreq)) for i in xrange(s,size)), p)
			s = size
			sizeProb[size] = 1 - p
		freqSizeProb[freq] = sizeProb
	t1 = time.time()
	print "... OK: %d gene frequencies, %d group sizes in %.2f seconds" % (len(freqs),len(sizes),t1-t0)
	return freqSizeProb
#calcFreqSizeProb_product()


calcFreqSizeProb = calcFreqSizeProb_product


###############################################################################
# calcFreqFreqMuSigma


def calcFreqFreqMuSigma(sizeTally, freqs, freqSizeProb):
	print "generating frequency-pair distribution values ..."
	t0 = time.time()
	freqFreqMuSigma = dict()
	for fL in freqs:
		lSizeProb = freqSizeProb[fL]
		for fR in freqs:
			if fL > fR:
				continue
			rSizeProb = freqSizeProb[fR]
			mean = variance = 0
			for size,tally in sizeTally.iteritems():
				lrProb = ((lSizeProb[size] * rSizeProb[size-1]) + (lSizeProb[size-1] * rSizeProb[size])) * 0.5
				mean += tally * lrProb
				variance += tally * lrProb * (1 - lrProb)
			stddev = variance ** 0.5
			freqFreqMuSigma[ (fL,fR) ] = (mean,stddev)
			freqFreqMuSigma[ (fR,fL) ] = (mean,stddev)
	t1 = time.time()
	print "... OK: %d gene frequencies, %d group sizes in %.2f seconds" % (len(freqs),len(sizeTally),t1-t0)
	return freqFreqMuSigma
#calcFreqFreqMuSigma()


###############################################################################
# calcScoreTally_[algebraic|random]


def integerizeScoreTally(scoreTally):
	extra = 0.0
	for g in xrange(max(scoreTally),0,-1):
		models = scoreTally[g] + extra
		scoreTally[g] = int(models)
		extra = models % 1
	scoreTally[0] = int(scoreTally[0] + extra)
	scoreTally += collections.Counter()
#integerizeScoreTally()

	
def calcScoreTally_algebraic(sizeTally, freqTally, freqSizeProb):
	print "summarizing scores for randomly distributed models of all gene frequencies ..."
	t0 = time.time()
	scoreTally = collections.Counter()
	for fL,tL in freqTally.iteritems():
		lSizeProb = freqSizeProb[fL]
		for fR,tR in freqTally.iteritems():
			if fL > fR:
				continue
			elif fL == fR:
				nM = (tL**2 - tL) / 2
			else:
				nM = tL * tR
			rSizeProb = freqSizeProb[fR]
			nLR = sum((tally * ((lSizeProb[size] * rSizeProb[size-1]) + (lSizeProb[size-1] * rSizeProb[size])) * 0.5) for size,tally in sizeTally.iteritems())
			wLR = float(nLR % 1)
			g = int(nLR)
			scoreTally[g] += nM * (1 - wLR)
			scoreTally[g+1] += nM * wLR
	#integerizeScoreTally(scoreTally)
	t1 = time.time()
	nM = sum(scoreTally.itervalues())
	print "... OK: %d models (%d with scores) in %.2f seconds" % (nM,nM-scoreTally[0],t1-t0)
	return scoreTally
#calcScoreTally_algebraic()


def calcScoreTally_random(sizeTally, freqTally, freqFreqMuSigma):
	print "summarizing scores for normally distributed models of all gene frequencies ..."
	t0 = time.time()
	scoreTally = collections.Counter()
	for fL,tL in freqTally.iteritems():
		for fR,tR in freqTally.iteritems():
			if fL > fR:
				continue
			elif fL == fR:
				nM = (tL**2 - tL) / 2
			else:
				nM = tL * tR
			mu,sigma = freqFreqMuSigma[(fL,fR)]
			for i in xrange(nM):
				nLR = max(0, random.normalvariate(mu,sigma))
				wLR = float(nLR % 1)
				g = int(nLR)
				scoreTally[g] += (1 - wLR)
				scoreTally[g+1] += wLR
	#integerizeScoreTally(scoreTally)
	t1 = time.time()
	nM = sum(scoreTally.itervalues())
	print "... OK: %d models (%d with scores) in %.2f seconds" % (nM,nM-scoreTally[0],t1-t0)
	return scoreTally
#calcScoreTally_random()


###############################################################################
# __main__


if __name__ == "__main__":
	# define usage
	parser = argparse.ArgumentParser(
		formatter_class=argparse.RawDescriptionHelpFormatter,
		description="permuloki",
		epilog="""
example: %(prog)s -k loki.db -r 12345 -m 12345.models.gz -s 12345.scores
"""
	)
	parser.add_argument('-k', '--knowledge', type=str, metavar='file', required=True,
		help="knowledge database file (required)"
	)
	parser.add_argument('-c', '--sources', type=str, metavar='source', nargs='+',
		help="sources to consider (default: all)"
	)
	parser.add_argument('-a', '--ambiguity', type=str, choices={'strict','resolvable','permissive'}, default='resolvable',
		help="interpret knowledge using ambiguity mode 'strict', 'resolvable' (default) or 'permissive'"
	)
	parser.add_argument('--maximum-model-group-size', '--mmgs', type=int, metavar='size', default=0,
		help="maximum size of a group to use for modeling, or < 1 for unlimited (default: unlimited)"
	)
	parser.add_argument('-d', '--delay', action='store_true', default=False,
		help="delay size-culling of groups until the end (default: cull immediately)"
	)
	parser.add_argument('-t', '--test', action='store_true', default=False,
		help="perform testing and validation of permutation algorithm variants"
	)
	parser.add_argument('-r', '--randomseed', type=int, metavar='seed', default=None,
		help="pseudo-random number generator seed value (default: system clock)"
	)
	parser.add_argument('-R', '--randomfile', type=str, metavar='file', default=None,
		help="file containing random number seed values (default: none)"
	)
	parser.add_argument('-n', '--normal', action='store_true', default=False,
		help="randomize using normal distribution"
	)
	parser.add_argument('-p', '--permute', action='store_true', default=False,
		help="randomly permute prior knowledge before generating models (default: no)"
	)
	parser.add_argument('-f', '--flat', action='store_true', default=False,
		help="permute using average gene frequency (default: preserve original frequencies)"
	)
	parser.add_argument('-z', '--fuzzy', action='store_true', default=False,
		help="permute using probabilistic gene frequencies (default: enforce exact frequencies)"
	)
	parser.add_argument('-s', '--scores', type=str, metavar='file', default=None,
		help="file to write model score summary (default: none)"
	)
	parser.add_argument('-m', '--models', type=str, metavar='file', default=None,
		help="file to write models with scores and p-values (default: none)"
	)
	
	# parse arguments
	args = parser.parse_args()
	
	# open db
	print "opening knowledge database file '%s' ..." % args.knowledge
	db = apsw.Connection(args.knowledge)
	cursor = db.cursor()
	print "... OK"
	
	# initialize PRNG
	randFunc = random.random
	if args.randomfile:
		seeds = list()
		if os.path.exists(args.randomfile):
			print "reading PRNG seeds from '%s' ..." % args.randomfile
			with open(args.randomfile,'rb') as seedFile:
				for line in seedFile:
					seeds.append(long(line.strip()))
			print "... OK: %d seeds" % len(seeds)
		loggedrandom.init(seeds)
		randFunc = loggedrandom.lrandom
	elif args.randomseed != None:
		print "PRNG seed: %s" % args.randomseed
		random.seed(args.randomseed)
	elif args.permute:
		print "PRNG seed: <system defined>"
	
	# load source id/name mappings
	print "loading sources ..."
	sourceName = dict()
	sourceID = dict()
	for row in cursor.execute("SELECT source_id,source FROM `source`"):
		if (not args.sources) or (row[1] in args.sources):
			sourceName[row[0]] = row[1]
			sourceID[row[1]] = row[0]
	print "... OK: %d sources" % len(sourceID)
	
	# load types
	print "loading types ..."
	typeID = dict()
	typeName = dict()
	for row in cursor.execute("SELECT type_id,type FROM `type`"):
		typeID[row[1]] = row[0]
		typeName[row[0]] = row[1]
	print "... OK: %d types" % len(typeID)
	
	# load genes
	print "loading genes ..."
	geneSet = set()
	typeIDgene = typeID['gene']
	for row in cursor.execute("SELECT biopolymer_id,type_id FROM `biopolymer`"):
		if row[1] == typeIDgene:
			geneSet.add(row[0])
	print "... OK: %d genes" % len(geneSet)
	
	# load group/source mappings
	print "loading groups ..."
	groupSource = dict()
	for row in cursor.execute("SELECT group_id,source_id FROM `group`"):
		groupSource[row[0]] = row[1]
	print "... OK: %d groups" % len(groupSource)
	
	# do testing?
	if args.test:
		for sID,sName in sourceName.iteritems():
			sql = """
SELECT group_id, biopolymer_id
FROM `group_biopolymer` AS gb
JOIN `group` AS g USING (group_id)
WHERE g.source_id = %d
""" % (sID,)
			if args.ambiguity == 'strict':
				sql += "  AND gb.specificity >= 100"
			elif args.ambiguity == 'resolvable':
				sql += "  AND (gb.specificity >= 100 OR gb.implication >= 100 OR gb.quality >= 100)"
			else:
				sql += "  AND gb.specificity > 0"
			groupGenes = collections.defaultdict(set)
			geneGroups = collections.defaultdict(set)
			for groupID,geneID in cursor.execute(sql):
				if geneID in geneSet:
					groupGenes[groupID].add(geneID)
					geneGroups[geneID].add(groupID)
			if (not groupGenes) or (not geneGroups):
				print "%s: ---" % sName
				continue
			groupSize = collections.Counter(itertools.chain(*geneGroups.itervalues()))
			geneFreq = collections.Counter(itertools.chain(*groupGenes.itervalues()))
			total = sum(groupSize.itervalues())
			maximum = len(groupGenes) * len(geneGroups)
			print "%s: %d groups (%d members), %d genes (%d assignments), %.2f%% fullness" % (
				sName,len(groupGenes),sum(groupSize.itervalues()),len(geneGroups),sum(geneFreq.itervalues()),(100.0*total/maximum)
			)
			if args.maximum_model_group_size >= 1:
				print "culling groups with over %d genes ..." % args.maximum_model_group_size
				for groupID in groupSize.keys():
					if groupSize[groupID] > args.maximum_model_group_size:
						for geneID in groupGenes[groupID]:
							geneGroups[geneID].remove(groupID)
							if not geneGroups[geneID]:
								del geneGroups[geneID]
						del groupGenes[groupID]
				if (not groupGenes) or (not geneGroups):
					print "%s: ---" % sName
					continue
				groupSize = collections.Counter(itertools.chain(*geneGroups.itervalues()))
				geneFreq = collections.Counter(itertools.chain(*groupGenes.itervalues()))
				total = sum(groupSize.itervalues())
				maximum = len(groupGenes) * len(geneGroups)
				print "%s: %d groups (%d members), %d genes (%d assignments), %.2f%% fullness" % (
					sName,len(groupGenes),sum(groupSize.itervalues()),len(geneGroups),sum(geneFreq.itervalues()),(100.0*total/maximum)
				)
			#if mmgs
			
			t0 = time.time()
			groupGenesX = shuffleGroupsByGroups(randFunc, groupSize, geneFreq)
			t1 = time.time()
			geneGroupsX = collections.defaultdict(set)
			for groupID,genes in groupGenesX.iteritems():
				for geneID in genes:
					geneGroupsX[geneID].add(groupID)
			groupSizeX = collections.Counter(itertools.chain(*geneGroupsX.itervalues()))
			geneFreqX = collections.Counter(itertools.chain(*groupGenesX.itervalues()))
			assert(groupSize==groupSizeX)
			assert(geneFreq==geneFreqX)
			similar = sum(len(genes & groupGenesX[groupID]) for groupID,genes in groupGenes.iteritems())
			print "%s: %d groups (%d members), %d genes (%d assignments), %.2f%% similarity -- %.2f seconds using groups-by-groups" % (
				sName,len(groupGenesX),sum(groupSizeX.itervalues()),len(geneGroupsX),sum(geneFreqX.itervalues()),(100.0*similar/total),t1-t0
			)
			
			t0 = time.time()
			groupGenesX = shuffleGroupsByGenes(randFunc, groupSize, geneFreq)
			t1 = time.time()
			geneGroupsX = collections.defaultdict(set)
			for groupID,genes in groupGenesX.iteritems():
				for geneID in genes:
					geneGroupsX[geneID].add(groupID)
			groupSizeX = collections.Counter(itertools.chain(*geneGroupsX.itervalues()))
			geneFreqX = collections.Counter(itertools.chain(*groupGenesX.itervalues()))
			assert(groupSize==groupSizeX)
			assert(geneFreq==geneFreqX)
			similar = sum(len(genes & groupGenesX[groupID]) for groupID,genes in groupGenes.iteritems())
			print "%s: %d groups (%d members), %d genes (%d assignments), %.2f%% similarity -- %.2f seconds using groups-by-genes" % (
				sName,len(groupGenesX),sum(groupSizeX.itervalues()),len(geneGroupsX),sum(geneFreqX.itervalues()),(100.0*similar/total),t1-t0
			)
			
			t0 = time.time()
			geneGroupsX = shuffleGenesByGroups(randFunc, groupSize, geneFreq)
			t1 = time.time()
			groupGenesX = collections.defaultdict(set)
			for geneID,groups in geneGroupsX.iteritems():
				for groupID in groups:
					groupGenesX[groupID].add(geneID)
			groupSizeX = collections.Counter(itertools.chain(*geneGroupsX.itervalues()))
			geneFreqX = collections.Counter(itertools.chain(*groupGenesX.itervalues()))
			assert(groupSize==groupSizeX)
			assert(geneFreq==geneFreqX)
			similar = sum(len(genes & groupGenesX[groupID]) for groupID,genes in groupGenes.iteritems())
			print "%s: %d groups (%d members), %d genes (%d assignments), %.2f%% similarity -- %.2f seconds using genes-by-groups" % (
				sName,len(groupGenesX),sum(groupSizeX.itervalues()),len(geneGroupsX),sum(geneFreqX.itervalues()),(100.0*similar/total),t1-t0
			)
			
			t0 = time.time()
			geneGroupsX = shuffleGenesByGenes(randFunc, groupSize, geneFreq)
			t1 = time.time()
			groupGenesX = collections.defaultdict(set)
			for geneID,groups in geneGroupsX.iteritems():
				for groupID in groups:
					groupGenesX[groupID].add(geneID)
			groupSizeX = collections.Counter(itertools.chain(*geneGroupsX.itervalues()))
			geneFreqX = collections.Counter(itertools.chain(*groupGenesX.itervalues()))
			assert(groupSize==groupSizeX)
			assert(geneFreq==geneFreqX)
			similar = sum(len(genes & groupGenesX[groupID]) for groupID,genes in groupGenes.iteritems())
			print "%s: %d groups (%d members), %d genes (%d assignments), %.2f%% similarity -- %.2f seconds using genes-by-genes" % (
				sName,len(groupGenesX),sum(groupSizeX.itervalues()),len(geneGroupsX),sum(geneFreqX.itervalues()),(100.0*similar/total),t1-t0
			)
		#foreach source
	#if args.test
	
	if args.scores or args.models:
		groupGenes = collections.defaultdict(set)
		geneFreqOrig = collections.Counter()
		for sID,sName in sourceName.iteritems():
			# load group-gene mappings for this source
			sql = """
SELECT group_id, biopolymer_id
FROM `group_biopolymer` AS gb
JOIN `group` AS g USING (group_id)
WHERE g.source_id = %d
""" % (sID,)
			if args.ambiguity == 'strict':
				sql += "  AND gb.specificity >= 100"
			elif args.ambiguity == 'resolvable':
				sql += "  AND (gb.specificity >= 100 OR gb.implication >= 100 OR gb.quality >= 100)"
			else:
				sql += "  AND gb.specificity > 0"
			groupGenesSrc = collections.defaultdict(set)
			for groupID,geneID in cursor.execute(sql):
				if geneID in geneSet:
					groupGenesSrc[groupID].add(geneID)
			if not groupGenesSrc:
				print "%s: skipped" % (sName,)
				continue
			
			# pre-cull groups?
			if not args.delay:
				for groupID in groupGenesSrc.keys():
					if (len(groupGenesSrc[groupID]) < 2) or (args.maximum_model_group_size and (len(groupGenesSrc[groupID]) > args.maximum_model_group_size)):
						del groupGenesSrc[groupID]
				if not groupGenesSrc:
					print "%s: skipped" % (sName,)
					continue
			
			# update original gene frequencies
			for groupID,genes in groupGenesSrc.iteritems():
				geneFreqOrig.update(genes)
			
			# permute?
			if args.permute:
				t0 = time.time()
				groupSizeSrc = collections.Counter()
				geneFreqSrc = collections.Counter()
				for groupID,genes in groupGenesSrc.iteritems():
					groupSizeSrc[groupID] = len(genes)
					geneFreqSrc.update(genes)
				if args.flat and args.fuzzy:
					groupGenesSrc = invertGeneGroups(shuffleGenes_flat(randFunc, groupSizeSrc, geneFreqSrc))
				elif args.flat:
					avgFreqSrc = int(math.ceil(sum(geneFreqSrc.itervalues()) / float(len(geneFreqSrc))))
					for geneID in geneFreqSrc:
						geneFreqSrc[geneID] = avgFreqSrc
					groupGenesSrc = invertGeneGroups(shuffleGenesByGroups(randFunc, groupSizeSrc, geneFreqSrc))
				elif args.fuzzy:
					groupGenesSrc = shuffleGroups_fuzzy(randFunc, groupSizeSrc, geneFreqSrc)
				else:
					groupGenesSrc = invertGeneGroups(shuffleGenesByGenes(randFunc, groupSizeSrc, geneFreqSrc))
				t1 = time.time()
			#if permute
			
			# post-cull groups?
			if args.delay:
				for groupID in groupGenesSrc.keys():
					if (len(groupGenesSrc[groupID]) < 2) or (args.maximum_model_group_size and (len(groupGenesSrc[groupID]) > args.maximum_model_group_size)):
						del groupGenesSrc[groupID]
				if not groupGenesSrc:
					print "%s: skipped" % (sName,)
					continue
			
			# report
			if args.permute:
				print "%s: %d groups (%d members), %d genes (%d assignments) -- %.2f seconds" % (
					sName,len(groupSizeSrc),sum(groupSizeSrc.itervalues()),len(geneFreqSrc),sum(geneFreqSrc.itervalues()),t1-t0
				)
			else:
				print "%s: %d groups (%d members)" % (
					sName,len(groupGenesSrc),sum(len(genes) for genes in groupGenesSrc.itervalues())
				)
			#if permute
			
			for groupID,genes in groupGenesSrc.iteritems():
				groupGenes[groupID].update(genes)
			groupGenesSrc = groupSizeSrc = geneFreqSrc = None
		#foreach source
		avgFreqOrig = sum(geneFreqOrig.itervalues()) / float(len(geneFreqOrig))
		if args.flat:
			for geneID in geneFreqOrig:
				geneFreqOrig[geneID] = avgFreqOrig
		freqTallyOrig = collections.Counter(geneFreqOrig.itervalues())
		
		# extrapolate from groupGenes to all the other summary structures
		geneGroups = invertGroupGenes(groupGenes)
		groupSize = dict( (groupID,len(genes)) for groupID,genes in groupGenes.iteritems() )
		geneFreq = dict( (geneID,len(groups)) for geneID,groups in geneGroups.iteritems() )
		sizeTally = collections.Counter(groupSize.itervalues())
		freqTally = collections.Counter(geneFreq.itervalues())
		numAsgn = sum(size*tally for size,tally in sizeTally.iteritems())
		avgFreq = numAsgn / float(len(geneGroups))
		numModels = (len(geneGroups)**2 - len(geneGroups)) / 2
		freqSizeProb = freqFreqMuSigma = None
		
		# generate score distribution
		print "%d active genes, %d active groups, %d possible models" % (len(geneGroups),len(groupGenes),numModels)
		scoreTally = collections.defaultdict(collections.Counter)
		if (args.randomseed != None) and not (args.permute):
			freqSizeProb = freqSizeProb or calcFreqSizeProb(numAsgn, sizeTally, freqTally, avgFreq)
			if args.normal:
				freqFreqMuSigma = freqFreqMuSigma or calcFreqFreqMuSigma(sizeTally, freqTally, freqSizeProb)
				scoreTally[0] = calcScoreTally_random(sizeTally, freqTally, freqFreqMuSigma)
			else:
				scoreTally[0] = calcScoreTally_algebraic(sizeTally, freqTally, freqSizeProb)
		else:
			if args.models:
				freqSizeProb = freqSizeProb or calcFreqSizeProb(numAsgn, sizeTally, freqTallyOrig, avgFreqOrig)
				freqFreqMuSigma = freqFreqMuSigma or calcFreqFreqMuSigma(sizeTally, freqTallyOrig, freqSizeProb)
				_erf = math.erf
				print "writing all compressed models to '%s' ..." % args.models
				modelFile = gzip.GzipFile(args.models, mode='wb',  compresslevel=6)
			else:
				print "scoring all models ..."
				modelFile = None
			t0 = time.time()
			m = mP = 0
			genesL = sorted(geneGroups)
			genesR = list(genesL)
			classes = collections.Counter()
		#	print sorted(geneFreqOrig.iterkeys())
		#	sys.exit(1)
			while genesL:
				geneL = genesL.pop()
				freqL = geneFreqOrig[geneL]
				groupsL = geneGroups[geneL]
				genesR.pop()
				for geneR in genesR:
					m += 1
					freqR = geneFreqOrig[geneR]
					groups = groupsL & geneGroups[geneR]
					sources = set(groupSource[g] for g in groups)
					nG = len(groups)
					nS = len(sources)
					classes[ (nS,nG,int(min(freqL,freqR)/100),int(max(freqL,freqR)/100)) ] += 1
					scoreTally[nS][nG] += 1
					if groups and modelFile:
						mu,sigma = freqFreqMuSigma[(freqL,freqR)]
						pval = (_erf( (mu - nG) / (sigma * 1.4142135623730951) ) + 1) * 0.5 # 2**0.5 = 1.4142135623730951
						modelFile.write("%d\t%d\t%d\t%d\t%s\n" % (geneL,geneR,nS,nG,repr(pval)))
				if mP < (10*m/numModels):
					mP = 10*m/numModels
					t1 = time.time()
					print "... %d%% in %s, ~%s to go ..." % (100*m/numModels,timestr(t1-t0),timestr((t1-t0)*numModels/m-(t1-t0)))
			t1 = time.time()
			freqSizeProb = freqFreqMuSigma = None
			if modelFile:
				modelFile.close()
			print "... OK: %d models (%d with scores), %d classes in %s" % (m,m-scoreTally[0][0],len(classes),timestr(t1-t0))
		#if theoretic/empiric
		
		if args.scores:
			print "writing score summary to '%s' ..." % args.scores
			with (sys.stdout if (args.scores == '-') else open(args.scores,'wb')) as scoreFile:
				for s in xrange(max(scoreTally)+1):
					scoreTally[s] += collections.Counter()
					for g in xrange(max(scoreTally[s])+1):
						scoreFile.write("%d\t%d\t%s\n" % (s,g,repr(scoreTally[s][g])))
			print "... OK"
		#if scores
	#if models/scores
	
	if args.randomfile:
		if (loggedrandom.s > loggedrandom.init):
			if loggedrandom.init:
				print "WARNING: %d seed values were provided, but %d were required" % (loggedrandom.init,loggedrandom.s)
	 		print "writing PRNG seeds to '%s' ..." % args.randomfile
			with open(args.randomfile,'wb') as seedFile:
				for s in xrange(loggedrandom.s):
					seedFile.write("%d\n" % loggedrandom.seeds[s])
			print "... OK: %d seeds" % loggedrandom.s
		elif (loggedrandom.s < loggedrandom.init):
			print "WARNING: %d seed values were provided, but only %d were required" % (loggedrandom.init,loggedrandom.s)
	#if randomfile
#if __main__

