#!/usr/bin/env python

import sys


def closestPoint(a, p, m=1):
	ab = (1, m)
	ap = (p[0]-a[0], p[1]-a[1])
	abDap = ab[0]*ap[0] + ab[1]*ap[1]
	t = abDap / (ab[0]**2 + ab[1]**2)
	return (a[0]+t*ab[0], a[1]+t*ab[1])
#closestPoint()

# Given a 2x(1000*n) matrix, D, of differences between the actual model score 
# and its permutation, we want to calculate the eigenvector corresponding to
# the largest eigenvalue of D*D' (D*D' is a 2x2 symmetric matrix, so it's 
# a fairly straightforward, analystic calculation that requires O(1) extra space)

# initialize the running sums of the entries of D*D' to 0
DD11 = 0
DD12 = 0
DD22 = 0

for a in xrange(1,len(sys.argv)):
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
	
	for ps,pg in scores:
		# Perhaps we want to limit this to only comparable scores; do that here
		
		DD11 += (src - ps)**2
		DD12 += (src - ps)*(grp - pg)
		DD22 += (grp - pg)**2

# Now that we're out of the loop, we have the matrix D*D', so we want to 
# calculate the eigenvalues.  
D_tr = DD11 + DD22
D_det = DD11*DD22 - DD12*DD12

# The eigenvalues come from the quadratic formula
ev_pos = (D_tr + sqrt(D_tr**2 - 4*D_det))/2
ev_neg = (D_tr - sqrt(D_tr**2 - 4*D_det))/2

# Now, we're going to assume that DD12 is nonzero
max_evec = [max(ev_pos, ev_neg) - D22, D12]

# And we can now define the slope of our projection line as y=mx, where m is the rise/run of the eigenvector
slope = max_evec[1] / max_evec[0]


print "#gene1\tgene2\tsrc\tgrp\tpval\tnumG\tnumB\tnumW\tnumI\tdiff0\tdiff5\tdiff50\tdiff95\tdiff100"
for a in xrange(1,len(sys.argv)):
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
		
		cs,cg = closestPoint( (src,grp), (ps,pg), slope )
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

