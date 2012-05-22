import loki_db
import sys

class liftOver(object):
	"""
	A class to do your heavy lifting for you
	"""
	
	def __init__(self, db_fn):
		self._db = loki_db.Database(db_fn)
		
		self._minFrac = 0.95
	
	def liftRegion(self, assembly, chrom, start, end):
		"""
		Lift a region from a given chromosome on a specified assembly
		to a region on the new assembly
		"""
		
		# We need to actually lift regions to detect dropped sections
		is_region = True
		
		# If the start and end are swapped, reverse them, please
		if start > end:
			(start, end) = (end, start)
		elif start == end:
			is_region = False
			end = start + 1	
		
		ch_list = self._db._dbc.execute(
			"SELECT chain.chain_id, chain_data.old_start, chain_data.old_end, chain_data.new_start, is_fwd, new_chr " +
			"FROM chain INNER JOIN chain_data ON chain.chain_id = chain_data.chain_id " +
			"WHERE old_assembly=? AND old_chr=? AND chain.old_end>? AND chain.old_start<? AND chain_data.old_end>=? AND chain_data.old_start<=? " +
			"ORDER BY score DESC",
			(assembly, chrom, start, end, start, end))
		
		# This will be a tuple of (start, end) of the mapped region
		# If the function returns "None", then it was unable to map
		# the region into the new assembly
		mapped_reg = None
		
		curr_chain = None
		
		total_mapped_sz = 0
		first_seg = None
		end_seg = None
		for seg in ch_list:
					
			if curr_chain is None:
				curr_chain = seg[0]
				first_seg = seg
				end_seg = seg
				total_mapped_sz = seg[2] - seg[1]
			elif seg[0] != curr_chain:
				mapped_reg = self._mapRegion((start, end), first_seg, end_seg, total_mapped_sz)
				if not mapped_reg:
					first_seg = seg
					end_seg = seg
					total_mapped_sz = seg[2] - seg[1]
				else:
					break
			else:
				end_seg = seg
				total_mapped_sz = total_mapped_sz + seg[2] - seg[1]
				
		if not mapped_reg and first_seg is not None:
			mapped_reg = self._mapRegion((start, end), first_seg, end_seg, total_mapped_sz)
		
		if mapped_reg and not is_region:
			mapped_reg = (mapped_reg[0], mapped_reg[0])
			
		return mapped_reg
		
				
				

	def _mapRegion(self, region, first_seg, end_seg, total_mapped_sz):
		"""
		Map a region given the 1st and last segment as well as the total mapped size
		"""
		mapped_reg = None
		
		# The front and end differences are the distances from the
		# beginning of the segment.
		
		# The front difference should be >=0 and <= size of 1st segment
		front_diff = max(0, min(region[0] - first_seg[1], first_seg[2] - first_seg[1]))
		# The end different should be similar, but w/ last
		
		end_diff = max(0, min(region[1] - end_seg[1], end_seg[2] - end_seg[1]))
		
		# Now, if we are moving forward, we add the difference
		# to the new_start, backward, we subtract
		# Also, at this point, if backward, swap start/end
		if first_seg[4]:
			new_start = first_seg[3] + front_diff
			new_end = end_seg[3] + end_diff
		else:
			new_start = end_seg[3] - end_diff
			new_end = first_seg[3] - front_diff
				
		# Here, detect if we have mapped a sufficient fraction 
		# of the region.  liftOver uses a default of 95%
		mapped_size = total_mapped_sz - front_diff - (end_seg[2] - end_seg[1]) + end_diff
		
		if mapped_size / float(region[1] - region[0]) >= self._minFrac:
			mapped_reg = (first_seg[5], new_start, new_end)
			
		return mapped_reg

if __name__ == "__main__":
	lo = liftOver(sys.argv[2])
	f = file(sys.argv[1])
	m = file(sys.argv[3],'w')
	u = file(sys.argv[4],'w')
	
	for l in f:
		wds = l.split()
		chrm = lo._db.chr_num.get(wds[0][3:],-1)
		n = lo.liftRegion(chrm, int(wds[1]), int(wds[2]))
		if n:
			print >> m, "chr" + lo._db.chr_list[n[0]], n[1], n[2]
		else:
			print >> u, l
