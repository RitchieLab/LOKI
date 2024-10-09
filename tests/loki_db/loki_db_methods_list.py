class Database(object):
    ##################################################
    # class interrogation

    @classmethod
    def getVersionTuple(cls):
        pass

    @classmethod
    def getVersionString(cls):
        pass

    @classmethod
    def getDatabaseDriverName(cls):
        pass

    @classmethod
    def getDatabaseDriverVersion(cls):
        pass

    @classmethod
    def getDatabaseInterfaceName(cls):
        pass

    @classmethod
    def getDatabaseInterfaceVersion(cls):
        pass

    ##################################################
    # constructor

    def __init__(
        self, dbFile=None, testing=False, updating=False, tempMem=False
    ):  # noqa E501
        # initialize instance properties
        self._is_test = testing
        self._updating = updating
        self._verbose = True
        self._logger = None
        # self._logFile = sys.stderr
        self._logIndent = 0
        self._logHanging = False
        # self._db = apsw.Connection("")
        self._dbFile = None
        self._dbNew = None
        self._updater = None
        self._liftOverCache = dict()  # { (from,to) : [] }

        self.configureDatabase(tempMem=tempMem)
        self.attachDatabaseFile(dbFile)

    ##################################################
    # context manager

    def __enter__(self):
        # return self._db.__enter__()
        ...

    def __exit__(self, excType, excVal, traceback):
        # return self._db.__exit__(excType, excVal, traceback)
        ...

    ##################################################
    # logging

    def _checkTesting(self):
        pass

    def getVerbose(self):
        pass

    def setVerbose(self, verbose=True):
        pass

    def setLogger(self, logger=None):
        pass

    def log(self, message=""):
        pass

    def logPush(self, message=None):
        pass

    def logPop(self, message=None):
        pass

    ##################################################
    # database management

    def getDatabaseMemoryUsage(self, resetPeak=False): ...

    def getDatabaseMemoryLimit(self): ...

    def setDatabaseMemoryLimit(self, limit=0): ...

    def configureDatabase(self, db=None, tempMem=False): ...

    def attachTempDatabase(self, db): ...

    def attachDatabaseFile(self, dbFile, quiet=False): ...

    def detachDatabaseFile(self, quiet=False): ...

    def testDatabaseWriteable(self): ...

    def createDatabaseObjects(
        self, schema, dbName, tblList=None, doTables=True, idxList=None, doIndecies=True
    ):  # noqa E501
        ...

    def createDatabaseTables(self, schema, dbName, tblList, doIndecies=False): ...

    def createDatabaseIndices(
        self, schema, dbName, tblList, doTables=False, idxList=None
    ): ...

    def dropDatabaseObjects(
        self,
        schema,
        dbName,
        tblList=None,
        doTables=True,
        idxList=None,
        doIndecies=True,  # noqa E501
    ): ...

    def dropDatabaseTables(self, schema, dbName, tblList): ...

    def dropDatabaseIndices(self, schema, dbName, tblList, idxList=None): ...

    def updateDatabaseSchema(self): ...

    def auditDatabaseObjects(
        self,
        schema,
        dbName,
        tblList=None,
        doTables=True,
        idxList=None,
        doIndecies=True,
        doRepair=True,
    ): ...

    def finalizeDatabase(self): ...

    def optimizeDatabase(self): ...

    def defragmentDatabase(self): ...

    def getDatabaseSetting(self, setting, type=None): ...

    def setDatabaseSetting(self, setting, value): ...

    def getSourceModules(self): ...

    def getSourceModuleVersions(self, sources=None): ...

    def getSourceModuleOptions(self, sources=None): ...

    def updateDatabase(
        self,
        sources=None,
        sourceOptions=None,
        cacheOnly=False,
        forceUpdate=False,  # noqa E501
    ): ...

    def prepareTableForUpdate(self, table): ...

    def prepareTableForQuery(self, table): ...

    ##################################################
    # metadata retrieval

    def generateGRChByUCSChg(self, ucschg): ...

    def getUCSChgByGRCh(self, grch): ...

    def getLDProfileID(self, ldprofile): ...

    def getLDProfileIDs(self, ldprofiles): ...

    def getLDProfiles(self, ldprofiles=None): ...

    def getNamespaceID(self, namespace): ...

    def getNamespaceIDs(self, namespaces): ...

    def getRelationshipID(self, relationship): ...

    def getRelationshipIDs(self, relationships): ...

    def getRoleID(self, role): ...

    def getRoleIDs(self, roles): ...

    def getSourceID(self, source): ...

    def getSourceIDs(self, sources=None): ...

    def getSourceIDVersion(self, sourceID): ...

    def getSourceIDOptions(self, sourceID): ...

    def getSourceIDFiles(self, sourceID): ...

    def getTypeID(self, type): ...

    def getTypeIDs(self, types): ...

    def getSubtypeID(self, subtype): ...

    def getSubtypeIDs(self, subtypes): ...

    ##################################################
    # snp data retrieval

    def generateCurrentRSesByRSes(self, rses, tally=None): ...

    def generateSNPLociByRSes(
        self,
        rses,
        minMatch=1,
        maxMatch=1,
        validated=None,
        tally=None,
        errorCallback=None,
    ): ...

    ##################################################
    # biopolymer data retrieval

    def generateBiopolymersByIDs(self, ids): ...

    def _lookupBiopolymerIDs(
        self, typeID, identifiers, minMatch, maxMatch, tally, errorCallback
    ): ...

    def generateBiopolymerIDsByIdentifiers(
        self,
        identifiers,
        minMatch=1,
        maxMatch=1,
        tally=None,
        errorCallback=None,  # noqa E501
    ): ...

    def generateTypedBiopolymerIDsByIdentifiers(
        self,
        typeID,
        identifiers,
        minMatch=1,
        maxMatch=1,
        tally=None,
        errorCallback=None,
    ): ...

    def _searchBiopolymerIDs(self, typeID, texts): ...

    def generateBiopolymerIDsBySearch(self, searches): ...

    def generateTypedBiopolymerIDsBySearch(self, typeID, searches): ...

    def generateBiopolymerNameStats(self, namespaceID=None, typeID=None): ...

    ##################################################
    # group data retrieval

    def generateGroupsByIDs(self, ids): ...

    def _lookupGroupIDs(
        self, typeID, identifiers, minMatch, maxMatch, tally, errorCallback
    ): ...

    def generateGroupIDsByIdentifiers(
        self,
        identifiers,
        minMatch=1,
        maxMatch=1,
        tally=None,
        errorCallback=None,  # noqa E501
    ): ...

    def generateTypedGroupIDsByIdentifiers(
        self,
        typeID,
        identifiers,
        minMatch=1,
        maxMatch=1,
        tally=None,
        errorCallback=None,
    ): ...

    def _searchGroupIDs(self, typeID, texts): ...

    def generateGroupIDsBySearch(self, searches): ...

    def generateTypedGroupIDsBySearch(self, typeID, searches): ...

    def generateGroupNameStats(self, namespaceID=None, typeID=None): ...

    ##################################################
    # liftover

    def hasLiftOverChains(self, oldHG, newHG): ...

    def _generateApplicableLiftOverChains(self, oldHG, newHG, chrom, start, end): ...

    def _liftOverRegionUsingChains(
        self, label, start, end, extra, first_seg, end_seg, total_mapped_sz
    ): ...

    def generateLiftOverRegions(
        self, oldHG, newHG, regions, tally=None, errorCallback=None
    ): ...

    def generateLiftOverLoci(
        self, oldHG, newHG, loci, tally=None, errorCallback=None
    ): ...
