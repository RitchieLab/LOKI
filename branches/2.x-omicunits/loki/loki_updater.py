#!/usr/bin/env python

import collections
import hashlib
import itertools
import os
import pkgutil
import sys
import time #DEBUG
import traceback

import loki_db
import loki_source
import loaders


class Updater(object):
	
	
	##################################################
	# constructor
	
	
	def __init__(self, lokidb, is_test=False):
		assert(isinstance(lokidb, loki_db.Database))
		self._is_test = is_test
		self._loki = lokidb
		self._db = lokidb._db
		self._sourceLoaders = None
		self._sourceClasses = dict()
		self._sourceObjects = dict()
		self._updating = False
		self._tablesUpdated = None
		self._tablesDeindexed = None
	#__init__()
	
	
	##################################################
	# logging
	
	
	def log(self, message=""):
		return self._loki.log(message)
	#log()
	
	
	def logPush(self, message=None):
		return self._loki.logPush(message)
	#logPush()
	
	
	def logPop(self, message=None):
		return self._loki.logPop(message)
	#logPop()
	
	
	def logIndent(self):
		return self._loki.logIndent()
	#logIndent()
	
	
	##################################################
	# database update
	
	
	def flagTableUpdate(self, table):
		if self._updating:
			self._tablesUpdated.add(table)
	#flagTableUpdate()
	
	
	def prepareTableForUpdate(self, table):
		if self._updating:
			self.flagTableUpdate(table)
			if table not in self._tablesDeindexed:
			#	print "deindexing %s" % table #DEBUG
				self._tablesDeindexed.add(table)
				self._loki.dropDatabaseIndecies(None, 'db', table)
				#TODO if table is a hybrid pre/post-proc, drop all source=0 rows now
	#prepareTableForUpdate()
	
	
	def prepareTableForQuery(self, table):
		if self._updating:
			if table in self._tablesDeindexed:
			#	print "reindexing %s" % table #DEBUG
				self._tablesDeindexed.remove(table)
				self._loki.createDatabaseIndecies(None, 'db', table)
	#prepareTableForQuery()
	
	
	def findSourceModules(self):
		if self._sourceLoaders == None:
			self._sourceLoaders = {}
			loader_path = loaders.__path__
			if self._is_test:
				loader_path = [os.path.join(l, "test") for l in loaders.__path__]
			for srcImporter,srcModuleName,_ in pkgutil.iter_modules(loader_path):
				if srcModuleName.startswith('loki_source_'):
					self._sourceLoaders[srcModuleName[12:]] = srcImporter.find_module(srcModuleName)
	#findSourceModules()
	
	
	def getSourceModules(self):
		self.findSourceModules()
		return self._sourceLoaders.keys()
	#getSourceModules()
	
	
	def loadSourceModules(self, sources=None):
		self.findSourceModules()
		srcSet = set()
		for srcName in (set(sources) if sources else self._sourceLoaders.keys()):
			if srcName not in self._sourceClasses:
				if srcName not in self._sourceLoaders:
					self.log("WARNING: unknown source '%s'\n" % srcName)
					continue
				#if module not available
				srcModule = self._sourceLoaders[srcName].load_module('loki_source_%s' % srcName)
				srcClass = getattr(srcModule, 'Source_%s' % srcName)
				if not issubclass(srcClass, loki_source.Source):
					self.log("WARNING: invalid module for source '%s'\n" % srcName)
					continue
				self._sourceClasses[srcName] = srcClass
			#if module class not loaded
			srcSet.add(srcName)
		#foreach source
		return srcSet
	#loadSourceModules()
	
	
	def getSourceModuleVersions(self, sources=None):
		srcSet = self.loadSourceModules(sources)
		return { srcName : self._sourceClasses[srcName].getVersionString() for srcName in srcSet }
	#getSourceModuleVersions()
	
	
	def getSourceModuleOptions(self, sources=None):
		srcSet = self.loadSourceModules(sources)
		return { srcName : self._sourceClasses[srcName].getOptions() for srcName in srcSet }
	#getSourceModuleOptions()
	
	
	def attachSourceModules(self, sources=None):
		sources = self.loadSourceModules(sources)
		srcSet = set()
		for srcName in sources:
			if srcName not in self._sourceObjects:
				if srcName not in self._sourceClasses:
					raise Exception("loadSourceModules() reported false positive for '%s'" % srcName)
				self._sourceObjects[srcName] = self._sourceClasses[srcName](self._loki)
			#if module not instantiated
			srcSet.add(srcName)
		#foreach source
		return srcSet
	#attachSourceModules()
	
	
	def updateDatabase(self, sources=None, sourceOptions=None, cacheOnly=False, forceUpdate=False):
		if self._updating:
			raise Exception("_updating set before updateDatabase()")
		self._loki.testDatabaseWriteable()
		if self._loki.getDatabaseSetting('finalized',int):
			raise Exception("cannot update a finalized database")
		
		# check for extraneous options
		self.logPush("preparing for update ...\n")
		srcSet = self.attachSourceModules(itertools.chain(sources, ['loki']))
		srcOpts = sourceOptions or {}
		for srcName in srcOpts.keys():
			if srcName not in srcSet:
				self.log("WARNING: not updating from source '%s' for which options were supplied\n" % srcName)
		logIndent = self.logPop("... OK\n")
		
		# update all specified sources
		iwd = os.path.abspath(os.getcwd())
		self._updating = True
		self._tablesUpdated = set()
		self._tablesDeindexed = set()
		srcOrder = sorted(srcSet - {'loki'}) + ['loki']
		srcErrors = set()
		cursor = self._db.cursor()
		cursor.execute("SAVEPOINT 'updateDatabase'")
		try:
			for srcName in srcOrder:
				t0 = time.time() #DEBUG
				cursor.execute("SAVEPOINT 'updateDatabase_%s'" % (srcName,))
				try:
					srcObj = self._sourceObjects[srcName]
					srcID = srcObj.getSourceID()
					
					# validate options
					prevOptions = dict()
					for row in cursor.execute("SELECT option, value FROM `db`.`source_option` WHERE source_id = ?", (srcID,)):
						prevOptions[str(row[0])] = str(row[1])
					options = srcOpts.get(srcName, prevOptions).copy()
					optionsList = sorted(options)
					if optionsList:
						self.logPush("%s %s options ...\n" % (("validating" if (srcName in srcOpts) else "loading prior"), srcName))
					msg = srcObj.validateOptions(options)
					if msg != True:
						raise Exception(msg)
					if optionsList:
						for opt in optionsList:
							self.log("%s = %s\n" % (opt,options[opt]))
						self.logPop("... OK\n")
					if (set(srcObj.getOptions()) - set(options)):
						raise Exception("options definition/validation mismatch")
					
					# switch to a temp subdirectory for this source
					path = os.path.join(iwd, srcName)
					if not os.path.exists(path):
						os.makedirs(path)
					os.chdir(path)
					
					# skip file fingerprinting for the LOKI postprocessor
					filehash = dict()
					if srcName != 'loki':
						# download files into a local cache
						if not cacheOnly:
							self.logPush("downloading %s data ...\n" % srcName)
							srcObj.download(options)
							self.logPop("... OK\n")
						
						# calculate source file metadata
						# all timestamps are assumed to be in UTC, but if a source
						# provides file timestamps with no TZ (like via FTP) we use them
						# as-is and assume they're supposed to be UTC
						self.log("analyzing %s data files ..." % srcName)
						for filename in os.listdir('.'):
							stat = os.stat(filename)
							md5 = hashlib.md5()
							with open(filename,'rb') as f:
								chunk = f.read(8*1024*1024)
								while chunk:
									md5.update(chunk)
									chunk = f.read(8*1024*1024)
							filehash[filename] = (filename, long(stat.st_size), long(stat.st_mtime), md5.hexdigest())
						self.log(" OK\n")
					#if postprocessor
					
					# compare current options, loader version, and file metadata to the last update to see if anything changed
					changed = forceUpdate or (options != prevOptions)
					prevVersion = '?'
					if not changed:
						for row in cursor.execute("SELECT version, DATETIME(updated,'localtime') FROM `db`.`source` WHERE source_id = ?", (srcID,)):
							changed = changed or (row[0] != srcObj.getVersionString())
							prevVersion = row[1]
					if (not changed) and (srcName != 'loki'):
						n = 0
						for row in cursor.execute("SELECT filename, size, md5 FROM `db`.`source_file` WHERE source_id = ?", (srcID,)):
							n += 1
							changed = changed or (row[0] not in filehash) or (row[1] != filehash[row[0]][1]) or (row[2] != filehash[row[0]][3])
						changed = changed or (n != len(filehash))
					
					# skip the update if nothing changed (but always run the post-processor, it will decide its own necessary steps)
					if changed or (srcName == 'loki'):
						cursor.execute("DELETE FROM `db`.`warning` WHERE source_id = ?", (srcID,))
						# process new files (or old files with a new loader)
						if srcName == 'loki':
							self.logPush("performing LOKI database post-processing ...\n")
							# if the postprocessor has an error, just flag it so we skip optimization;
							# don't revert the savepoint, the postprocessor handles that internally
							if srcObj.update(options, prevOptions, self._tablesUpdated, forceUpdate) == False:
								srcErrors.add(srcName)
						else:
							self.logPush("processing %s data ...\n" % srcName)
							if srcObj.update(options) == False:
								raise Exception
						cursor.execute("UPDATE `db`.`source` SET updated = DATETIME('now'), version = ? WHERE source_id = ?", (srcObj.getVersionString(), srcID))
						
						cursor.execute("DELETE FROM `db`.`source_option` WHERE source_id = ?", (srcID,))
						sql = "INSERT INTO `db`.`source_option` (source_id, option, value) VALUES (%d,?,?)" % srcID
						cursor.executemany(sql, options.iteritems())
						
						cursor.execute("DELETE FROM `db`.`source_file` WHERE source_id = ?", (srcID,))
						sql = "INSERT INTO `db`.`source_file` (source_id, filename, size, modified, md5) VALUES (%d,?,?,DATETIME(?,'unixepoch'),?)" % srcID
						cursor.executemany(sql, filehash.values())
						
						self.logPop("... OK\n")
					else:
						self.log("skipping %s update, no data or software changes since %s\n" % (srcName,prevVersion))
					#if changed
				except:
					cursor.execute("ROLLBACK TRANSACTION TO SAVEPOINT 'updateDatabase_%s'" % (srcName,))
					srcErrors.add(srcName)
					if srcName == 'loki':
						self._loki.setDatabaseSetting('postProcess', 'all')
					excType,excVal,excTrace = sys.exc_info()
					while self.logPop() > logIndent:
						pass
					self.logPush("ERROR: failed to update %s\n" % (srcName,))
					if excTrace:
						for line in traceback.format_list(traceback.extract_tb(excTrace)[-1:]):
							self.log(line)
					for line in traceback.format_exception_only(excType,excVal):
						self.log(line)
					self.logPop()
				finally:
					cursor.execute("RELEASE SAVEPOINT 'updateDatabase_%s'" % (srcName,))
				#try/except/finally
				self.log("(%ds)\n" % (time.time()-t0)) #DEBUG
			#foreach source
			
			# finalize
			self.log("finishing update ...")
			if self._tablesDeindexed:
				self._loki.createDatabaseIndecies(None, 'db', self._tablesDeindexed)
			if self._tablesUpdated:
				self._loki.setDatabaseSetting('optimized', 0)
			self.log(" OK\n")
		except:
			excType,excVal,excTrace = sys.exc_info()
			while self.logPop() > logIndent:
				pass
			self.logPush("ERROR: failed to update the database\n")
			if excTrace:
				for line in traceback.format_list(traceback.extract_tb(excTrace)[-1:]):
					self.log(line)
			for line in traceback.format_exception_only(excType,excVal):
				self.log(line)
			self.logPop()
			cursor.execute("ROLLBACK TRANSACTION TO SAVEPOINT 'updateDatabase'")
		finally:
			cursor.execute("RELEASE SAVEPOINT 'updateDatabase'")
			self._updating = False
			self._tablesUpdated = None
			self._tablesDeindexed = None
			os.chdir(iwd)
		#try/except/finally
		
		# report and return
		if srcErrors:
			self.logPush("WARNING: data from these sources was not updated:\n")
			for srcName in sorted(srcErrors):
				self.log("%s\n" % srcName)
			self.logPop()
			return False
		return True
	#updateDatabase()
	
#Updater
