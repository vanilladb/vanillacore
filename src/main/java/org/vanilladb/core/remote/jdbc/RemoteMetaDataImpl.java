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
package org.vanilladb.core.remote.jdbc;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;

/**
 * The RMI server-side implementation of RemoteMetaData.
 */
@SuppressWarnings("serial")
public class RemoteMetaDataImpl extends UnicastRemoteObject implements
		RemoteMetaData {
	private Schema schema;
	private List<String> fields = new ArrayList<String>();

	/**
	 * Creates a metadata object that wraps the specified schema. The method
	 * also creates a list to hold the schema's collection of field names, so
	 * that the fields can be accessed by position.
	 * 
	 * @param schema
	 *            the schema
	 * @throws RemoteException
	 */
	public RemoteMetaDataImpl(Schema schema) throws RemoteException {
		this.schema = schema;
		fields.addAll(schema.fields());
	}

	/**
	 * Returns the size of the field list.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteMetaData#getColumnCount()
	 */
	@Override
	public int getColumnCount() throws RemoteException {
		return fields.size();
	}

	/**
	 * Returns the field name for the specified column number. In JDBC, column
	 * numbers start with 1, so the field is taken from position (column-1) in
	 * the list.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteMetaData#getColumnName(int)
	 */
	@Override
	public String getColumnName(int column) throws RemoteException {
		return fields.get(column - 1);
	}

	/**
	 * Returns the type of the specified column. The method first finds the name
	 * of the field in that column, and then looks up its type in the schema.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteMetaData#getColumnType(int)
	 */
	@Override
	public int getColumnType(int column) throws RemoteException {
		String fldname = getColumnName(column);
		return schema.type(fldname).getSqlType();
	}

	/**
	 * Returns the number of characters required to display the specified
	 * column.
	 * 
	 * @see org.vanilladb.core.remote.jdbc.RemoteMetaData#getColumnDisplaySize(int)
	 */
	@Override
	public int getColumnDisplaySize(int column) throws RemoteException {
		String fldname = getColumnName(column);
		Type fldtype = schema.type(fldname);
		if (fldtype.isFixedSize())
			// 6 and 12 digits for int and double respectively
			return fldtype.maxSize() * 8 / 5;
		return schema.type(fldname).getArgument();
	}
}
