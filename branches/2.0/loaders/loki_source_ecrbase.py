#!/usr/bin/env python

import loki_source
import itertools

class Source_ecrbase(loki_source.Source):
	"""
	A class to load ECRBase into LOKI
	"""
	
	_remHost = "www.dcode.org"
	_remFiles = {
		"coreEcrs.rheMac.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18rheMac2.txt.gz",
		"ecrs.rheMac.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18rheMac2.txt.gz",
	}
    
	def download(self):
		"""
		Download the ECRBase files
		"""
		self.downloadFilesFromHTTP(self._remHost, self._remFiles)
		
	def update(self):
		"""
		Update based on the downloaded files
		"""	
		fn_by_species = {}
		for fn in self._remFiles:
			(ecr_type, species, other) = fn.split(".",2)
			fn_by_species.setdefault(species,[]).append(fn)
				
		# this will put the "ecr" file last
		for s, fns in fn_by_species.iteritems():
			fns.sort()
		
		# Add a namespace
		ecr_ns = self.addNamespace("ecrbase")
		
		# Add a type of "ecr"
		ecr_typeid = self.addType("ecr")
		
		# Add a type of "ecr_group"
		ecr_group_typeid = self.addType("ecr_group")
		
		for species, fn_list in fn_by_species.iteritems():
			
			# Should only (exactly!) be 2 files:
			base_fn = fn_list[1]
			core_fn = fn_list[0]
			
			desc = "ECRs for " + species
			label = "ecr_" + species
			
			(base_gid, core_gid) = self.addTypedGroups(ecr_group_typeid, [(label, desc),("core_"+label, "Core " + desc)])
			self.addGroupNamespacedNames(ecr_ns, [(base_gid, label), (core_gid, "core_"+label)])
			
			reg_list = [r for r in (self._convertToRegion(l, species) for l in self.zfile(base_fn)) if r is not None]
			reg_ids = self.addTypedRegions(ecr_typeid, ((r[0], '') for r in reg_list))
			reg_dict = dict(zip((r[0] for r in reg_list), reg_ids))
			self.addRegionNamespacedNames(ecr_ns, ((v, k) for (k, v) in reg_dict.iteritems()))
			self.addRegionPopulationBounds(1, (tuple(itertools.chain(*c)) for c in zip(((i,) for i in reg_ids),(r[1] for r in reg_list))))			
			self.addGroupTypedRegionNamespacedNames(ecr_typeid, ecr_ns, ((base_gid, x[0]+1, x[1]) for x in zip(xrange(len(reg_list)), (r[0] for r in reg_list))))
				
			# Now, parse the core ECRs
			reg_list = [r for r in (self._convertToRegion(l, species) for l in self.zfile(base_fn)) if r is not None]
			self.addGroupTypedRegionNamespacedNames(ecr_typeid, ecr_ns, ((core_gid, x[0]+1, x[1]) for x in zip(xrange(len(reg_list)), (r[0] for r in reg_list))))
			
			
	def _convertToRegion(self, line, species):
		"""
		Convert a line in an ECRbase file to a region
		"""
		
		(detail, other) = line.split(None, 1)
		(ch, region) = detail.split(':')
		if ch.lower().startswith('chr'):
			ch = ch[3:]
		
		ch_num = self._loki.chr_num.get(ch, None)
		if ch_num is None:
			return None
		
		(start, end) = region.split('-')	
		name =  species + ":" + detail
		
		return (name, (ch_num, int(start), int(end)))
