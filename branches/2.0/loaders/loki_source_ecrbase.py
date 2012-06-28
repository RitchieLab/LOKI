#!/usr/bin/env python

import loki_source
import itertools

class Source_ecrbase(loki_source.Source):
	"""
	A class to load ECRBase into LOKI
	"""
	
	_remHost = "www.dcode.org"
	_remFiles = {
		"coreEcrs.Macaque.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18rheMac2.txt.gz",
		"ecrs.Macaque.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18rheMac2.txt.gz",
		"ecrs.Chimp.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18panTro2.txt.gz",
		"coreEcrs.Chimp.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18panTro2.txt.gz",
		"ecrs.Mouse.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18mm9.txt.gz",
		"coreEcrs.Mouse.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18mm9.txt.gz",
		"ecrs.Cow.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18bosTau3.txt.gz",
		"coreEcrs.Cow.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18bosTau3.txt.gz",
		"ecrs.Dog.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18canFam2.txt.gz",
		"coreEcrs.Dog.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18canFam2.txt.gz",
		"ecrs.Opossum.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18monDom4.txt.gz",
		"coreEcrs.Opossum.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18monDom4.txt.gz",
		"ecrs.Chicken.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18galGal3.txt.gz",
		"coreEcrs.Chicken.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18galGal3.txt.gz",
		"ecrs.Frog.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18xenTro2.txt.gz",
		"coreEcrs.Frog.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18xenTro2.txt.gz",
		"ecrs.Zebrafish.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18danRer5.txt.gz",
		"coreEcrs.Zebrafish.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18danRer5.txt.gz",
		"ecrs.Fugu.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18fr2.txt.gz",
		"coreEcrs.Fugu.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18fr2.txt.gz",
		"ecrs.Tetradon.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/ecrs.hg18tetNig1.txt.gz",
		"coreEcrs.Tetradon.txt.gz" : "/get_file.cgi?name=ecrbase::ECR/coreEcrs.hg18tetNig1.txt.gz",
	}
	
	def download(self, options):
		"""
		Download the ECRBase files
		"""
		self.downloadFilesFromHTTP(self._remHost, self._remFiles)
		
	def update(self, options):
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
		
		# Make sure the "" ldprofile exists
		ecr_ldprofile_id = self.addLDProfile('', 'no LD adjustment', None)
		
		for species, fn_list in fn_by_species.iteritems():
			
			# Should only (exactly!) be 2 files:
			base_fn = fn_list[1]
			core_fn = fn_list[0]
			
			desc = "ECRs for " + species
			label = "ecr_" + species
			
			(base_gid, core_gid) = self.addTypedGroups(ecr_group_typeid, [(label, desc),("core_"+label, "Core " + desc)])
			
			self.log("processing base ECRs for " + species + " ...")
			self.addGroupNamespacedNames(ecr_ns, [(base_gid, label), (core_gid, "core_"+label)])
			
			reg_list = [r for r in (self._convertToRegion(l, species) for l in self.zfile(base_fn)) if r is not None]
			reg_ids = self.addTypedBiopolymers(ecr_typeid, ((r[0], '') for r in reg_list))
			reg_dict = dict(zip((r[0] for r in reg_list), reg_ids))
			self.addBiopolymerNamespacedNames(ecr_ns, ((v, k) for (k, v) in reg_dict.iteritems()))
			self.addBiopolymerLDProfileRegions(ecr_ldprofile_id, (tuple(itertools.chain(*c)) for c in zip(((i,) for i in reg_ids),(r[1] for r in reg_list))))			
			self.addGroupMemberTypedNamespacedNames(ecr_typeid, ecr_ns, ((base_gid, x[0]+1, x[1]) for x in zip(xrange(len(reg_list)), (r[0] for r in reg_list))))
			self.log(" OK\n")
			self.log("processing core ECRs for " + species + " ...")
			# Now, parse the core ECRs
			reg_list = [r for r in (self._convertToRegion(l, species) for l in self.zfile(base_fn)) if r is not None]
			self.addGroupMemberTypedNamespacedNames(ecr_typeid, ecr_ns, ((core_gid, x[0]+1, x[1]) for x in zip(xrange(len(reg_list)), (r[0] for r in reg_list))))
			self.log(" OK\n")
			
			
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
