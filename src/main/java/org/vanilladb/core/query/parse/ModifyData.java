/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.Predicate;

/**
 * Data for the SQL <em>update</em> statement.
 */
public class ModifyData {
	private String tblName;
	private Predicate pred;
	private Map<String, Expression> fldVals;

	/**
	 * Saves the table name, the modified field and its new value, and the
	 * predicate.
	 * 
	 * @param tblName
	 *            the name of the affected table
	 * @param fldVals
	 *            the list of names of the target fields
	 * @param pred
	 *            the modification predicate
	 */
	public ModifyData(String tblName, Map<String, Expression> fldVals, Predicate pred) {
		this.tblName = tblName;
		this.fldVals = fldVals;
		this.pred = pred;
	}

	/**
	 * Returns the name of the affected table.
	 * 
	 * @return the name of the affected table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the fields whose values will be modified
	 * 
	 * @return the list of names of the target fields
	 */
	public Collection<String> targetFields() {
		return new ArrayList<String>(fldVals.keySet());
	}

	/**
	 * Returns an expression. Evaluating this expression for a record produces
	 * the value that will be stored in the record's target field.
	 * 
	 * @return the target expression
	 */

	/**
	 * Returns an expression. Evaluating this expression for a record produces
	 * the value that will be stored in the record's target field.
	 * 
	 * @param fldName
	 *            the name of the modified field
	 * @return the target expression
	 */
	public Expression newValue(String fldName) {
		return fldVals.get(fldName);
	}

	/**
	 * Returns the predicate that describes which records should be modified.
	 * 
	 * @return the modification predicate
	 */
	public Predicate pred() {
		return pred;
	}
}
