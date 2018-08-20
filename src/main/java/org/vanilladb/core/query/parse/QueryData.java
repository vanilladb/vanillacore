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
package org.vanilladb.core.query.parse;

import static org.vanilladb.core.sql.RecordComparator.DIR_DESC;

import java.util.List;
import java.util.Set;

import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.aggfn.AggregationFn;
import org.vanilladb.core.sql.predicate.Predicate;

/**
 * Data for the SQL <em>select</em> and <em>explain</em> statements.
 */
public class QueryData {
	private Set<String> projFields;
	private Set<String> tables;
	private Predicate pred;
	private Set<String> groupFields;
	private Set<AggregationFn> aggFn;
	private List<String> sortFields;
	private List<Integer> sortDirs;
	private boolean isExplain;

	/**
	 * Saves the information of a SQL query.
	 * 
	 * @param isExplain
	 *            if the query is an explain statement
	 * @param projFields
	 *            a collection of field names
	 * @param tables
	 *            a collection of table names
	 * @param pred
	 *            the query predicate
	 * @param groupFields
	 *            a collection of grouping field names
	 * @param aggFn
	 *            a collection of aggregation functions
	 * @param sortFields
	 *            a list of field names for sorting
	 * @param sortDirs
	 *            a list of sort directions
	 */
	public QueryData(boolean isExplain, Set<String> projFields, Set<String> tables, Predicate pred,
			Set<String> groupFields, Set<AggregationFn> aggFn, List<String> sortFields, List<Integer> sortDirs) {
		this.isExplain = isExplain;
		this.projFields = projFields;
		this.tables = tables;
		this.pred = pred;
		this.groupFields = groupFields;
		this.aggFn = aggFn;
		this.sortFields = sortFields;
		this.sortDirs = sortDirs;
	}

	/**
	 * Returns the fields mentioned in the select clause.
	 * 
	 * @return a collection of field names
	 */
	public Set<String> projectFields() {
		return projFields;
	}

	/**
	 * Returns the tables mentioned in the from clause.
	 * 
	 * @return a collection of table names
	 */
	public Set<String> tables() {
		return tables;
	}

	/**
	 * Returns the predicate that describes which records should be in the
	 * output table.
	 * 
	 * @return the query predicate
	 */
	public Predicate pred() {
		return pred;
	}

	/**
	 * Returns the fields used to sort the query result.
	 * 
	 * @return a list of field names for sorting
	 */
	public List<String> sortFields() {
		return sortFields;
	}

	/**
	 * Returns a list of sort directions to the sorting fields. The values of
	 * sort directions are defined in {@link RecordComparator}.
	 * 
	 * @return a list of sort directions
	 */
	public List<Integer> sortDirections() {
		return sortDirs;
	}

	/**
	 * Returns the field names mentioned in the group by clause.
	 * 
	 * @return a collection of grouping field names
	 */
	public Set<String> groupFields() {
		return groupFields;
	}

	/**
	 * Returns the aggregation functions mentioned in the clause.
	 * 
	 * @return a collection of aggregation functions
	 */
	public Set<AggregationFn> aggregationFn() {
		return aggFn;
	}

	/**
	 * Returns true if the query is an explain statement.
	 * 
	 * @return true if the query is an explain statement
	 */
	public boolean isExplain() {
		return isExplain;
	}

	public String toString() {
		StringBuilder result = new StringBuilder();
		if (isExplain)
			result.append("explain ");
		result.append("select ");
		for (String fldname : projFields)
			result.append(fldname + ", ");
		// remove the last comma
		result.delete(result.length() - 2, result.length());
		result.append(" from ");
		for (String tblname : tables)
			result.append(tblname + ", ");
		result.delete(result.length() - 2, result.length());

		String predString = pred.toString();
		if (!predString.equals(""))
			result.append(" where " + predString);

		if (groupFields != null) {
			result.append(" group by ");
			for (String gbf : groupFields)
				result.append(gbf + ", ");
			result.delete(result.length() - 2, result.length());
		}

		if (sortFields != null) {
			result.append(" sort by ");
			for (int i = 0; i < sortFields.size(); i++) {
				String sbf = sortFields.get(i);
				int sbd = sortDirs.get(i);
				result.append(sbf + (sbd == DIR_DESC ? " desc" : "") + ", ");
			}
			result.delete(result.length() - 2, result.length());
		}
		return result.toString();
	}
}
