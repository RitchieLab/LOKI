#!/usr/bin/env python

import collections
import os
import sys
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


if len(sys.argv) <= 5:
	print "usage: %s <modelfile> <idfile> <directory> <min> <max>" % (sys.argv[0],)
	sys.exit(1)

modelScores = dict()
with open(sys.argv[1],'rU') as modelFile:
	for line in modelFile:
		words = line.split()[0:2]
		g1 = min(words)
		g2 = max(words)
		if ((g1,g2) in modelScores) or ((g2,g1) in modelScores):
			print "ERROR: duplicate model:",line
			sys.exit(1)
		modelScores[ (g1,g2) ] = list()
print "%d models in %s" % (len(modelScores),sys.argv[1])

idGene = dict()
with open(sys.argv[2],'rU') as idFile:
	for line in idFile:
		words = line.split()[0:2]
		i = int(words[1])
		if i in idGene:
			print "ERROR: duplicate geneID:",line
			sys.exit(1)
		idGene[i] = words[0]
print "%d gene IDs in %s" % (len(idGene),sys.argv[2])

for p in xrange(int(sys.argv[4]),int(sys.argv[5])+1):
	if p < 0:
		scorePath = '%s/a.models.gz' % (sys.argv[3],)
	else:
		scorePath = '%s/p%d.models.gz' % (sys.argv[3],p)
	print "scanning %s ..." % (scorePath,)
	models = set(modelScores)
	with zopen(scorePath) as scoreFile:
		for line in scoreFile:
			words = line.split()
			g1 = idGene.get(int(words[0]))
			g2 = idGene.get(int(words[1]))
			score = "%d\t%d\t%d\t%s" % (p,int(words[2]),int(words[3]),words[4])
			if g1 and g2:
				m = (g1,g2)
				if m in modelScores:
					models.discard(m)
					modelScores[m].append(score)
				else:
					m = (g2,g1)
					if m in modelScores:
						models.discard(m)
						modelScores[m].append(score)
			#if g1,g2
		#foreach line
		score = "%d\t0\t0\t1.0" % (p,)
		for m in models:
			modelScores[m].append(score)
	#with scoreFile
#foreach p
print "... complete"

for model,scores in modelScores.iteritems():
	scoresPath = 'model.%s.%s.scores' % model
	print "writing %s ..." % (scoresPath,)
	if not os.path.exists(scoresPath):
		scores.insert(0,"#per\tsrc\tgrp\tpval")
	with open(scoresPath,'ab') as scoresFile:
		scoresFile.write("\n".join(scores)+"\n")
print "... complete"
