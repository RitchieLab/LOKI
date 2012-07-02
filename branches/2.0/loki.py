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
	parser.add_argument('-l', '--list-source', type=str, metavar='source', nargs='*', action='append', default=None,
			help="list options for the specified source loaders, or if none or '+' are specified, list all available sources"
	)
	parser.add_argument('-k', '--knowledge', type=str, metavar='file', action='store', default=None,
			help="the knowledge database file to use"
	)
	parser.add_argument('-u', '--update', type=str, metavar='source', nargs='*', action='append', default=None,
			help="update the knowledge database file by downloading and processing new data from the specified sources, "
			+"or if none or '+' are specified, from all available sources"
	)
	parser.add_argument('-U', '--update-except', type=str, metavar='source', nargs='*', action='append', default=None,
			help="update the knowledge database file by downloading and processing new data from all available sources EXCEPT those specified"
	)
	parser.add_argument('-o', '--option', type=str, metavar=('source','optionstring'), nargs=2, action='append', default=None,
			help="additional option(s) to pass to the specified source loader module, in the format 'option=value[,option2=value2[,...]]'"
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
	parser.add_argument('-t', '--test-data', action='store_true',
			help="Load testing data only"
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
	db = loki_db.Database(testing=args.test_data, updating=((args.update != None) or (args.update_except != None)))
	db.setVerbose(args.verbose)
	db.attachDatabaseFile(args.knowledge)
		
	# list sources?
	if args.list_source != None:
		srcSet = set()
		for srcList in args.list_source:
			srcSet |= set(srcList)
		if (not srcSet) or ('+' in srcSet):
			print "available source loaders:"
			srcSet = set()
		else:
			print "source loader options:"
		moduleOptions = db.getSourceModuleOptions(srcSet)
		for srcName in sorted(moduleOptions.keys()):
			print "  %s" % srcName
			if moduleOptions[srcName]:
				for srcOption in sorted(moduleOptions[srcName].keys()):
					print "    %s = %s" % (srcOption,moduleOptions[srcName][srcOption])
			elif srcSet:
				print "    <no options>"
	
	# pass options?
	userOptions = {}
	if args.option != None:
		for optList in args.option:
			srcName = optList[0]
			if srcName not in userOptions:
				userOptions[srcName] = {}
			for optString in optList[1].split(','):
				opt,val = optString.split('=',1)
				userOptions[srcName][opt] = val
	userOptions = userOptions or None
	
	# parse requested update sources
	srcSet = None
	if args.update != None:
		srcSet = set()
		for srcList in args.update:
			srcSet |= set(srcList)
	notSet = None
	if args.update_except != None:
		notSet = set()
		for srcList in args.update_except:
			notSet |= set(srcList)
	
	# update?
	if (srcSet != None) or (notSet != None):
		if srcSet and '+' in srcSet:
			srcSet = set()
		srcSet = (srcSet or set(db.getSourceModules())) - (notSet or set())
		db.updateDatabase(srcSet, userOptions, args.cache_only)
	
	# finalize?
	if args.finalize:
		db.finalizeDatabase()
	
#__main__
