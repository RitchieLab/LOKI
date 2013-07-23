#!/usr/bin/env python

import collections
import os
import sys
import time
import zlib


class zopen(object):
	
	def __init__(self, fileName, splitChar="\n", chunkSize=16*1024):
		self._filePtr = open(fileName,'rb')
		self._splitChar = splitChar
		self._chunkSize = chunkSize
		self._dc = zlib.decompressobj(zlib.MAX_WBITS | 32) # autodetect gzip or zlib header
		self._text = ""
		self._lines = list()
	#__init__()
	
	
	def __del__(self):
		if self._filePtr:
			self._filePtr.close()
	#__del__()
	
	
	def __enter__(self):
		return self
	#__enter__()
	
	
	def __exit__(self, excType, excVal, excTrace):
		pass
	#__exit__()
	
	
	def __iter__(self):
		return self
	#__iter__()
	
	
	def __next__(self):
		# if lines are still cached from the last read, pop one
		if len(self._lines) > 0:
			return self._lines.pop()
		# if there's data left in the source file, read and decompress another chunk
		if self._dc:
			data = self._dc.unused_data
			if data:
				self._dc = zlib.decompressobj(zlib.MAX_WBITS | 32) # autodetect gzip or zlib header
			else:
				data = self._filePtr.read(self._chunkSize)
			if data:
				self._text += self._dc.decompress(data)
				data = None
			else:
				self._text += self._dc.flush()
				self._dc = None
		# if there's no text left, we're done
		if not self._text:
			raise StopIteration
		# split the text into lines
		self._lines = self._text.split(self._splitChar)
		self._text = ""
		# if there's more than one line, store the last to combine with the next chunk
		# (but if there's only one line, and more to read, then keep reading until we get a linebreak)
		if len(self._lines) > 1:
			self._text = self._lines.pop()
		elif self._dc:
			self._text = self._lines.pop()
			self._chunkSize *= 2
			return self.__next__()
		# reverse the remaining lines into a stack and pop one to return
		self._lines.reverse()
		return self._lines.pop()
	#__next__()
	
	
	def next(self):
		return self.__next__()
	#next()
	
	
	def seek(self, offset, whence = 0):
		if offset != 0:
			raise Exception("zfile.seek() does not support offsets != 0")
		self._filePtr.seek(0, whence)
		self._dc.flush()
		self._text = ""
		self._lines = list()
	#seek()
	
#zopen


if len(sys.argv) < 4:
	print "usage: %s <directory> <min> <max>" % (sys.argv[0],)
	sys.exit(1)

# data = [hits,src,grp,better,worse,incomparable]
geneGeneData = dict()
genes = set()

scorePath = '%s/a.models.gz' % (sys.argv[1],)
print "scanning %s ..." % (scorePath,)
numModel = numDupe = 0
with zopen(scorePath) as scoreFile:
	for line in scoreFile:
		words = line.split(None,4)
		g1 = int(words[0])
		g2 = int(words[1])
		src = int(words[2])
		grp = int(words[3])
		g1,g2 = min(g1,g2),max(g1,g2)
		
		genes.add(g1)
		genes.add(g2)
		
		if g1 not in geneGeneData:
			geneGeneData[g1] = dict()
		if g2 not in geneGeneData[g1]:
			numModel += 1
			geneGeneData[g1][g2] = [src,grp,0]
		else:
			numDupe += 1
	#foreach line
#with scoreFile
numTotal = ((len(genes)**2) - len(genes)) / 2
print "... OK: %d genes form %d possible models (%d scored, %d duplicate, %d unscored)" % (len(genes),numTotal,numModel,numDupe,numTotal-numModel)
genes = None

t0 = time.time()
pRange = list(xrange(int(sys.argv[2]),int(sys.argv[3])+1))
empty = dict()
for p in pRange:
	scorePath = '%s/p%d.models.gz' % (sys.argv[1],p)
	print "scanning %s ..." % (scorePath,)
	with zopen(scorePath) as scoreFile:
		for line in scoreFile:
			words = line.split(None,4)
			g1 = int(words[0])
			g2 = int(words[1])
			src = int(words[2])
			grp = int(words[3])
			g1,g2 = min(g1,g2),max(g1,g2)
			
			data = (geneGeneData.get(g1,empty)).get(g2,None)
			if data and (src >= data[0]) and (grp >= data[1]):
				data[2] += 1
		#foreach line
	#with scoreFile
#foreach p
t1 = time.time()
print "... OK: %ds" % (t1-t0,)

print "compiling distributions by starting score ..."
with open('scanall.txt','wb') as scanFile:
	scanFile.write("#src\tgrp\tnum\tmin\t5th\tmedian\t95th\tmax\n")
	while geneGeneData:
		g1 = next(geneGeneData.iterkeys())
		g2 = next(geneGeneData[g1].iterkeys())
		src,grp,_ = geneGeneData[g1][g2]
		better = list()
		cull1 = set()
		for g1,g2s in geneGeneData.iteritems():
			cull2 = set()
			for g2,data in g2s.iteritems():
				if (data[0] == src) and (data[1] == grp):
					better.append(data[2])
					cull2.add(g2)
			for g2 in cull2:
				del g2s[g2]
			if not g2s:
				cull1.add(g1)
		for g1 in cull1:
			del geneGeneData[g1]
		better.sort()
		n = len(better) - 1
		scanFile.write("%s\n" % ("\t".join(repr(c) for c in [src,grp,n+1,better[0],better[int(n*0.05)],better[n/2],better[int(n*0.95)],better[n]]),))
print "... OK"

