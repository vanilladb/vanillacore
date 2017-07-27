package org.vanilladb.core.query.planner.index;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.index.IndexSelectPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexSelector {

	public static IndexSelectPlan selectByBestMatchedIndex(String tblName,
			TablePlan tablePlan, Predicate pred, Transaction tx) {
		
		Set<IndexInfo> candidates = new HashSet<IndexInfo>();
		for (String fieldName : tablePlan.schema().fields()) {
			ConstantRange searchRange = pred.constantRange(fieldName);
			if (searchRange == null)
				continue;
			
			List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblName, fieldName, tx);
			if (iis != null)
				candidates.addAll(iis);
		}
		
		return selectByBestMatchedIndex(candidates, tablePlan, pred, tx);
	}
	
	public static IndexSelectPlan selectByBestMatchedIndex(String tblName,
			TablePlan tablePlan, Predicate pred, Transaction tx, Collection<String> excludedFields) {
		
		Set<IndexInfo> candidates = new HashSet<IndexInfo>();
		for (String fieldName : tablePlan.schema().fields()) {
			if (excludedFields.contains(fieldName))
				continue;
			
			ConstantRange searchRange = pred.constantRange(fieldName);
			if (searchRange == null)
				continue;
			
			for (IndexInfo ii : VanillaDb.catalogMgr().getIndexInfo(tblName, fieldName, tx)) {
				
				boolean ignored = false;
				for (String fldName : ii.fieldNames())
					if (excludedFields.contains(fldName)) {
						ignored = true;
						break;
					}
				
				if (!ignored)
					candidates.add(ii);
			}
		}
		
		return selectByBestMatchedIndex(candidates, tablePlan, pred, tx);
	}
	
	public static IndexSelectPlan selectByBestMatchedIndex(Set<IndexInfo> candidates,
			TablePlan tablePlan, Predicate pred, Transaction tx) {
		// Choose the index with the most matched fields in the predicate
		int matchedCount = 0;
		IndexInfo bestIndex = null;
		Map<String, ConstantRange> searchRanges = null;
		
		for (IndexInfo ii : candidates) {
			if (ii.fieldNames().size() < matchedCount)
				continue;
			
			Map<String, ConstantRange> ranges = new HashMap<String, ConstantRange>();
			for (String fieldName : ii.fieldNames()) {
				ConstantRange searchRange = pred.constantRange(fieldName);
				if (searchRange != null && (
						(ii.indexType() == IndexType.HASH && searchRange.isConstant())
						|| ii.indexType() == IndexType.BTREE))
					ranges.put(fieldName, searchRange);
			}
			
			if (ranges.size() > matchedCount) {
				matchedCount = ranges.size();
				bestIndex = ii;
				searchRanges = ranges;
			}
		}
		
		if (bestIndex != null) {
			return new IndexSelectPlan(tablePlan, bestIndex, searchRanges, tx);
		}
		
		return null;
	}
}
