#!/usr/bin/env python

import math
import sys


def closestPoint(a, b, p, clamped=False):
	ab = (b[0]-a[0], b[1]-a[1])
	ap = (p[0]-a[0], p[1]-a[1])
	abDap = ab[0]*ap[0] + ab[1]*ap[1]
	t = abDap / (ab[0]**2 + ab[1]**2)
	if clamped:
		if t <= 0:
			return a
		if t >= 1:
			return b
	return (a[0]+t*ab[0], a[1]+t*ab[1])
#closestPoint()


print "#gene1\tgene2\tsrc\tgrp\tpval\tnumG\tnumB\tnumW\tnumI\tdiff0\tdiff5\tdiff50\tdiff95\tdiff100"
try:
	slope = float(sys.argv[1])
	a0 = 2
except:
	slope = 1.0
	a0 = 1
for a in xrange(a0,len(sys.argv)):
	with open(sys.argv[a],'rb') as scoreFile:
		src = grp = pvalT = None
		scores = list()
		for line in scoreFile:
			if line.startswith("#"):
				continue
			words = line.split()
			if int(words[0]) < 0:
				src,grp,pvalT = int(words[1]),int(words[2]),float(words[3])
			else:
				scores.append( (int(words[1]),int(words[2])) )
	
	numG = numB = numW = numI = 0
	delta = list()
	for ps,pg in scores:
		if (pg >= grp):
			numG += 1
		if (ps >= src) and (pg >= grp):
			numB += 1
		if (ps <= src) and (pg <= grp):
			numW += 1
		if (ps > src) and (pg < grp):
			numI += 1
		if (ps < src) and (pg > grp):
			numI += 1
		
		cs,cg = closestPoint( (src,grp), (src+1.0,grp+slope), (ps,pg) )
		d = ((src-cs)**2 + (grp-cg)**2)**0.5
		if (src > cs) and (grp > cg):
			d = -d
		delta.append(d)
	#foreach score
	assert(len(delta) == 1000)
	delta.sort()
	
	print "%s\t%d\t%d\t%s\t%d\t%d\t%d\t%d\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f" % (
		"\t".join(sys.argv[a].split('.')[1:3]), src, grp, repr(pvalT),
		numG, numB, numW, numI,
		delta[0], delta[50], delta[500], delta[950], delta[999]
	)
