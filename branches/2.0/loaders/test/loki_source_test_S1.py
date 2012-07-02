import loki_source

class Source_test_S1(loki_source.Source):
	
	def download(self,options):
		"""
		No downloading
		"""
		pass
		
	def update(self,options):
		"""
		Create source 1's groups
		"""
		path_typeid = self.addType("pathway")
		gene_typeid = self.addType("gene")
		
		relationship_id = self.addRelationship("is_a")
		
		ns_id = self.addNamespace("S1")
		symbol_ns = self.addNamespace("symbol")
		
		self.log("Creating Groups ...")
		gids = self.addTypedGroups(path_typeid, [("P1", "Pathway 1"), ("P2", "Pathway2")])		
		self.addGroupNamespacedNames(ns_id, (gids[r], "P%d" % (r+1)) for r in rage(2))
		self.log("OK\n")
		
		self.log("Creating Group Relationships ...")
		self.addGroupRelationships([(gid[0],gid[1],relationship_id)])		
		self.log("OK\n")
		
		self.log("Creating Group Membership ...")
		self.addGroupMemberTypedNamespacedNames(gene_typeid, symbol_ns, 
			[(gid[0],1,'G1'),(gid[0],2,'G2'),(gid[1],1,'G4')])
		self.log("OK\n")
		
		
		