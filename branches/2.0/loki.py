#!/usr/bin/env python

import argparse
import sys

import loki_db


if __name__ == "__main__":
	version = "LOKI version %s" % (loki_db.Database.getVersionString())
	
	# define arguments
	parser = argparse.ArgumentParser(
		formatter_class=argparse.RawDescriptionHelpFormatter,
		description=version,
	)
	parser.add_argument('--version', action='version',
			version=version+"\n%s version %s\n%s version %s" % (
				loki_db.Database.getDatabaseDriverName(), loki_db.Database.getDatabaseDriverVersion(),
				loki_db.Database.getDatabaseInterfaceName(), loki_db.Database.getDatabaseInterfaceVersion()
			)
	)
	parser.add_argument('-l', '--list-sources', action='store_true',
			help="list all available update sources"
	)
	parser.add_argument('-k', '--knowledge', type=str, metavar='file', action='store', default=None,
			help="the knowledge database file to use"
	)
	parser.add_argument('-u', '--update', type=str, metavar='source', nargs='*', action='append', default=None,
			help="update the knowledge database file by downloading and processing new data from the specified sources; "
			+"if no sources are specified, or if '+' is specified, then use all available sources"
	)
	parser.add_argument('-U', '--update-except', type=str, metavar='source', nargs='*', action='append', default=None,
			help="update the knowledge database file by downloading and processing new data from all known sources EXCEPT those specified"
	)
	parser.add_argument('-f', '--finalize', action='store_true',
			help="finalize the knowledge database file"
	)
	parser.add_argument('-c', '--cache-only', action='store_true',
			help="only use data files available from the local cache, without checking for or downloading any new files"
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
	
	# instantiate database and load knowledge file, if any
	db = loki_db.Database()
	db.setVerbose(args.verbose)
	db.attachDatabaseFile(args.knowledge)
	
	# list source options?
	if args.list_sources:
		print "available source loaders:"
		for src in sorted(db.listSourceModules()):
			print "  %s" % src
	
	# parse requested update sources
	updateSet = None
	if args.update != None:
		updateSet = set()
		for updateList in args.update:
			updateSet |= set(updateList)
	updateExcept = None
	if args.update_except != None:
		updateExcept = set()
		for exceptList in args.update_except:
			updateExcept |= set(exceptList)
	
	# update?
	if (updateSet != None) or (updateExcept != None):
		if updateSet and '+' in updateSet:
			updateSet = set()
		updateSet = (updateSet or set(db.listSourceModules())) - (updateExcept or set())
		db.updateDatabase(updateSet, args.cache_only)
	
	# finalize?
	if args.finalize:
		db.finalizeDatabase()
	
#__main__
