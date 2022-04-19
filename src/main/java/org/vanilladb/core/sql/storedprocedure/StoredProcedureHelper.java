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
package org.vanilladb.core.sql.storedprocedure;

import org.vanilladb.core.sql.Schema;

/**
 * A helper that helps a stored procedure unpack encoded parameters
 * and prepare for returning results.
 */
public interface StoredProcedureHelper {

	/**
	 * The default {@code StoredProcedureHelper} for being a
	 * place holder.
	 */
	StoredProcedureHelper DEFAULT_HELPER = new StoredProcedureHelper() {
		@Override
		public void prepareParameters(Object... pars) {
			// do nothing
		}
		
		@Override
		public Schema getResultSetSchema() {
			return new Schema();
		}

		@Override
		public SpResultRecord newResultSetRecord() {
			return new SpResultRecord();
		}
		
		@Override
		public boolean isReadOnly() {
			return false;
		}
	};

	/**
	 * Unpacks the encoded parameters for later use in the corresponding
	 * stored procedure.
	 * 
	 * @param pars
	 *            An object array contains all parameter for this stored
	 *            procedure.
	 */
	void prepareParameters(Object... pars);
	
	/**
	 * Gets the schema of result sets.
	 * 
	 * @return the schema of result sets.
	 */
	Schema getResultSetSchema();
	
	/**
	 * Generates a {@code SpResultRecord} that stores the result of 
	 * executing the corresponding procedure. This should be called
	 * only after the stored procedure finishes.
	 * 
	 * @return the record that stores the result of the stored procedure
	 */
	SpResultRecord newResultSetRecord();
	
	/**
	 * Returns whether the corresponding stored procedure is read-only
	 * in terms of the parameters.
	 * 
	 * @return whether the corresponding stored procedure is read-only
	 */
	boolean isReadOnly();
}
