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
package org.vanilladb.core.query.planner;

import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DropTableData;
import org.vanilladb.core.query.parse.DropViewData;
import org.vanilladb.core.query.parse.DropIndexData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The interface implemented by the planners for SQL insert, delete, and modify
 * statements.
 */
public interface UpdatePlanner {

	/**
	 * Executes the specified insert statement, and returns the number of
	 * affected records.
	 * 
	 * @param data
	 *            the parsed representation of the insert statement
	 * @param tx
	 *            the calling transaction
	 * @return the number of affected records
	 */
	int executeInsert(InsertData data, Transaction tx);

	/**
	 * Executes the specified delete statement, and returns the number of
	 * affected records.
	 * 
	 * @param data
	 *            the parsed representation of the delete statement
	 * @param tx
	 *            the calling transaction
	 * @return the number of affected records
	 */
	int executeDelete(DeleteData data, Transaction tx);

	/**
	 * Executes the specified modify statement, and returns the number of
	 * affected records.
	 * 
	 * @param data
	 *            the parsed representation of the modify statement
	 * @param tx
	 *            the calling transaction
	 * @return the number of affected records
	 */
	int executeModify(ModifyData data, Transaction tx);

	/**
	 * Executes the specified create table statement, and returns the number of
	 * affected records.
	 * 
	 * @param data
	 *            the parsed representation of the create table statement
	 * @param tx
	 *            the calling transaction
	 * @return the number of affected records
	 */
	int executeCreateTable(CreateTableData data, Transaction tx);

	/**
	 * Executes the specified create view statement, and returns the number of
	 * affected records.
	 * 
	 * @param data
	 *            the parsed representation of the create view statement
	 * @param tx
	 *            the calling transaction
	 * @return the number of affected records
	 */
	int executeCreateView(CreateViewData data, Transaction tx);

	/**
	 * Executes the specified create index statement, and returns the number of
	 * affected records.
	 * 
	 * @param data
	 *            the parsed representation of the create index statement
	 * @param tx
	 *            the calling transaction
	 * @return the number of affected records
	 */
	int executeCreateIndex(CreateIndexData data, Transaction tx);

	/**
	 * Executes the specified drop table statement, and returns the number of
	 * affected records.
	 * 
	 * @param data
	 *            the parsed representation of the drop table statement
	 * @param tx
	 *            the calling transaction
	 * @return the number of affected records
	 */
	int executeDropTable(DropTableData data, Transaction tx);

	/**
	 * Executes the specified drop view statement, and returns the number of
	 * affected records.
	 * 
	 * @param data
	 *            the parsed representation of the drop view statement
	 * @param tx
	 *            the calling transaction
	 * @return the number of affected records
	 */
	int executeDropView(DropViewData data, Transaction tx);

	/**
	 * Executes the specified drop index statement, and returns the number of
	 * affected records.
	 * 
	 * @param data
	 *            the parsed representation of the drop index statement
	 * @param tx
	 *            the calling transaction
	 * @return the number of affected records
	 */
	int executeDropIndex(DropIndexData data, Transaction tx);
}
