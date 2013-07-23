#!/usr/bin/env python

import argparse
import collections


if __name__ == "__main__":
	# define usage
	parser = argparse.ArgumentParser(
		formatter_class=argparse.RawDescriptionHelpFormatter,
		description="permuplot",
		epilog="""
example: %(prog)s -k loki.db -r 12345 -m 12345.models.gz -s 12345.scores
"""
	)
	parser.add_argument('-a', '--actual', type=str, metavar='file',
		help="model score summary for actual knowledge"
	)
	parser.add_argument('-r', '--random', type=str, metavar='file',
		help="model score summary for random knowledge"
	)
	parser.add_argument('-p', '--permuted', type=str, nargs='+', metavar='file',
		help="model score summaries for all permutations"
	)
	parser.add_argument('-s', '--sources', type=str, metavar='file',
		help="filename for sources-score plot"
	)
	parser.add_argument('-g', '--groups', type=str, metavar='file',
		help="filename for groups-score plot"
	)
	
	# parse arguments
	args = parser.parse_args()
	
	# load actual-knowledge scores
	actS = collections.Counter()
	actG = collections.Counter()
	if args.actual:
		print "reading actual-knowledge score summary '%s' ..." % args.actual
		with open(args.actual,'rb') as actFile:
			for line in actFile:
				words = list(int(w) for w in line.split())
				actS[words[0]] += words[2]
				actG[words[1]] += words[2]
		print "... OK"
	
	# load random-knowledge scores
	randS = collections.Counter()
	randG = collections.Counter()
	if args.random:
		print "reading random-knowledge score summary '%s' ..." % args.random
		with open(args.random,'rb') as randFile:
			for line in randFile:
				words = list(int(w) for w in line.split())
				randS[words[0]] += words[2]
				randG[words[1]] += words[2]
		print "... OK"
	
	# load permuted-knowledge scores
	numPerm = 0
	permS = collections.defaultdict(list)
	permG = collections.defaultdict(list)
	if args.permuted:
		print "reading permuted score summaries ..."
		for permPath in args.permuted:
			numPerm += 1
			pS = collections.Counter()
			pG = collections.Counter()
			with open(permPath,'rb') as permFile:
				for line in permFile:
					words = list(int(w) for w in line.split())
					pS[words[0]] += words[2]
					pG[words[1]] += words[2]
			for s,v in pS.iteritems():
				permS[s].append(v)
			for g,v in pG.iteritems():
				permG[g].append(v)
		print "... OK: %d permutations" % numPerm
	
	# print sources-score data
	print "sources\tactual\trandom\tp(min)\tp(25)\tp(50)\tp(75)\tp(max)"
	for s in sorted(set(actS.iterkeys()) | set(randS.iterkeys()) | set(permS.iterkeys())):
		vals = permS.get(s)
		vmin,v25,vmed,v75,vmax = "","","","",""
		if vals:
			vals.extend([0]*(numPerm-len(vals)))
			vals = sorted(vals)
			vmin,v25,vmed,v75,vmax = vals[0],vals[numPerm/4],vals[numPerm/2],vals[numPerm*3/4],vals[-1]
		print "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (s,actS.get(s,""),randS.get(s,""),vmin,v25,vmed,v75,vmax)
	
	# print groups-score data
	print "groups\tactual\trandom\tp(min)\tp(25)\tp(50)\tp(75)\tp(max)"
	for g in sorted(set(actG.iterkeys()) | set(randG.iterkeys()) | set(permG.iterkeys())):
		vals = permG.get(g)
		vmin,v25,vmed,v75,vmax = "","","","",""
		if vals:
			vals.extend([0]*(numPerm-len(vals)))
			vals = sorted(vals)
			vmin,v25,vmed,v75,vmax = vals[0],vals[numPerm/4],vals[numPerm/2],vals[numPerm*3/4],vals[-1]
		print "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (g,actG.get(g,""),randG.get(g,""),vmin,v25,vmed,v75,vmax)
#__main__

