#!/usr/bin/env python

import math
import sys


data = dict()
with open(sys.argv[1],'rb') as summaryFile:
	header = summaryFile.next()
	for line in summaryFile:
		cols = list( int(w) for w in line.split() )
		data[ (cols[0],cols[1]) ] = (cols[3],cols[4],cols[5],cols[6],cols[7])

svgAxes = [
	'<line x1="48" y1="0" x2="48" y2="1002" />',
	'<line x1="48" y1="1001" x2="%d" y2="1001" />' % (50+2*len(data),),
	'<line x1="40" y1="%1.2f" x2="48" y2="%1.2f" />' % (2*(1000-((1000**(1.0/3.0))*100),)),
	'<line x1="40" y1="%1.2f" x2="48" y2="%1.2f" />' % (2*(1000-((500**(1.0/3.0))*100),)),
	'<line x1="40" y1="%1.2f" x2="48" y2="%1.2f" />' % (2*(1000-((100**(1.0/3.0))*100),)),
	'<line x1="40" y1="%1.2f" x2="48" y2="%1.2f" />' % (2*(1000-((50**(1.0/3.0))*100),)),
	'<line x1="40" y1="%1.2f" x2="48" y2="%1.2f" />' % (2*(1000-((10**(1.0/3.0))*100),)),
	'<line x1="40" y1="%1.2f" x2="48" y2="%1.2f" />' % (2*(1000-((1**(1.0/3.0))*100),)),
	'<line x1="40" y1="%1.2f" x2="48" y2="%1.2f" />' % (2*(1000-((0**(1.0/3.0))*100),)),
	'<line x1="48" y1="%1.2f" x2="%d" y2="%1.2f" stroke="#808080" stroke-width="1" stroke-dasharray="5,5" />' % (1000-((50**(1.0/3.0))*100),2+2*len(data),1000-((50**(1.0/3.0))*100)),
]
svgBG = []
svgData1 = []
svgData2 = []
svgText = [
	'<text x="10" y="500" text-anchor="middle" transform="rotate(-90, 10, 500)">permuted scores &gt;= actual</text>',
	'<text x="%d" y="1045" text-anchor="middle">actual scores (sources-groups)</text>' % (50+len(data),),
	'<text x="45" y="%1.2f" text-anchor="end">%d%%</text>' % (1000-((1000**(1.0/3.0))*100)+15,100),
	'<text x="45" y="%1.2f" text-anchor="end">%d%%</text>' % (1000-((500**(1.0/3.0))*100)-2,50),
	'<text x="45" y="%1.2f" text-anchor="end">%d%%</text>' % (1000-((100**(1.0/3.0))*100)-2,10),
	'<text x="45" y="%1.2f" text-anchor="end">%d%%</text>' % (1000-((50**(1.0/3.0))*100)-2,5),
	'<text x="45" y="%1.2f" text-anchor="end">%d%%</text>' % (1000-((10**(1.0/3.0))*100)-2,1),
	'<text x="45" y="%1.2f" text-anchor="end">%1.1f%%</text>' % (1000-((1**(1.0/3.0))*100)-2,0.1),
	'<text x="45" y="%1.2f" text-anchor="end">%d%%</text>' % (1000-((0**(1.0/3.0))*100)-2,0),
]

blocks = list()
blockSrc = blockNum = None
x = 50
for score in sorted(data.keys()):
	if blockSrc != score[0]:
		if blockSrc:
			blocks.append( (len(blocks),blockSrc,blockNum) )
		blockSrc = score[0]
		blockNum = 0
	blockNum += 1
	
	#p0,p5,p50,p95,p100 = data[score]
	l0,l5,l50,l95,l100 = ((p**(1.0/3.0))*100 for p in data[score])
	if (l95 - l5) < 10:
		l95 = min(l95+5, 1000.0)
		l5 = max(l5-5, 0.0)
		l100 = max(l95, l100)
		l0 = min(l0, l5)
	if max(l100-l95, l5-l0) >= 1:
		svgData1.append('<line x1="%d" y1="%1.2f" x2="%d" y2="%1.2f" />' % (x, 1000-l100, x, 1000-l0))
	svgData2.append('<line x1="%d" y1="%1.2f" x2="%d" y2="%1.2f" />' % (x, 1000-l95, x, 1000-l5))
	x += 2

x = 50
for b,s,n in blocks:
	svgAxes.append('<line x1="%d" y1="1001" x2="%d" y2="1010" />' % (x,x))
	svgText.append('<text x="%d" y="1020">%d-x</text>' % (x+5,s))
	if (b % 2):
		svgBG.append('<rect x="%d" y="0" width="%d" height="1000" />' % (x,2*n))
	x += 2*n

print """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
<svg version="1.1" width="%1.2fin" height="%1.2fin" viewBox="0 0 %d %d" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
	<g fill="#e0e0e0">
		%s
	</g>
	<g stroke="#000000" stroke-width="2">
		%s
	</g>
	<g stroke="#8080ff" stroke-width="2">
		%s
	</g>
	<g stroke="#0000ff" stroke-width="2">
		%s
	</g>
	<g fill="#000000" font-family="Terminal" font-size="15px">
		%s
	</g>
</svg>""" % (
	15.0, 15.0*1050/(50+2*len(data)), 50+2*len(data), 1050,
	"\n\t\t".join(svgBG),
	"\n\t\t".join(svgAxes),
	"\n\t\t".join(svgData1),
	"\n\t\t".join(svgData2),
	"\n\t\t".join(svgText),
)

