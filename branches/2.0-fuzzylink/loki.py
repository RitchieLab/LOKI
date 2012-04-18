#!/usr/bin/env python

import argparse
import sys

import loki_db


if __name__ == "__main__":
	version = "LOKI version %d.%d.%d (%s)" % (
			loki_db.Database.ver_maj,
			loki_db.Database.ver_min,
			loki_db.Database.ver_rev,
			loki_db.Database.ver_date
	)
	
	# define arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('--version', action='version',
			version=version+"\n%9s version %s\n%9s version %s" % (
				loki_db.Database.getDatabaseDriverName(), loki_db.Database.getDatabaseDriverVersion(),
				loki_db.Database.getDatabaseInterfaceName(), loki_db.Database.getDatabaseInterfaceVersion()
			)
	)
	parser.add_argument('-k', '--knowledge', type=str, metavar='file', action='store', required=True,
			help="the knowledge database file to use"
	)
	parser.add_argument('-u', '--update', type=str, metavar='source', nargs='*', action='append', default=None,
			help="update the knowledge database file by downloading and processing new data from the specified sources; "
			+"specify '+' (or nothing) to update from all known sources, or '?' to list available sources"
	)
	parser.add_argument('-v', '--verbose', action='store_true',
		help="print warnings and log messages"
	)
	
	# if no arguments, print usage and exit
	if len(sys.argv) < 2:
		print version
		print
		parser.print_usage()
		print
		print "Use -h for details."
		sys.exit(2)
	
	# parse arguments
	args = parser.parse_args()
	
	# instantiate database and load knowledge file
	db = loki_db.Database()
	db.setVerbose(args.verbose)
	db.attachDatabaseFile(args.knowledge)
	
	# update from requested sources, if any
	if args.update != None:
		update = list()
		for updateList in (args.update or tuple()):
			update += updateList
		db.updateDatabase(update)
#__main__
