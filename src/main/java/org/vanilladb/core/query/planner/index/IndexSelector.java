/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
		for (String fieldName : VanillaDb.catalogMgr().getIndexedFields(tblName, tx)) {
			ConstantRange searchRange = pred.constantRange(fieldName);
			if (searchRange == null)
				continue;
			
			List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblName, fieldName, tx);
			candidates.addAll(iis);
		}
		
		return selectByBestMatchedIndex(candidates, tablePlan, pred, tx);
	}
	
	public static IndexSelectPlan selectByBestMatchedIndex(String tblName,
			TablePlan tablePlan, Predicate pred, Transaction tx, Collection<String> excludedFields) {
		
		Set<IndexInfo> candidates = new HashSet<IndexInfo>();
		for (String fieldName : VanillaDb.catalogMgr().getIndexedFields(tblName, tx)) {
			if (excludedFields.contains(fieldName))
				continue;
			
			ConstantRange searchRange = pred.constantRange(fieldName);
			if (searchRange == null)
				continue;
			
			List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblName, fieldName, tx);
			for (IndexInfo ii : iis) {
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
