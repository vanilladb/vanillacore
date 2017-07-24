package org.vanilladb.core.storage.index;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;

/**
 * A SearchKeyType represents an array of types for a list of indexed fields.
 */
public class SearchKeyType {

	private Type[] types;
	
	public SearchKeyType(Schema tblSch, List<String> indexedFields) {
		types = new Type[indexedFields.size()];
		Iterator<String> fldNameIter = indexedFields.iterator();
		String fldName;

		for (int i = 0; i < types.length; i++) {
			fldName = fldNameIter.next();
			types[i] = tblSch.type(fldName);
			if (types[i] == null)
				throw new NullPointerException("there is field named '" + fldName + "' in the table");
		}
	}
	
	public SearchKeyType(Type[] types) {
		this.types = Arrays.copyOf(types, types.length);
	}
	
	public int length() {
		return types.length;
	}
	
	public Type get(int index) {
		return types[index];
	}
	
	@Override
	public String toString() {
		return Arrays.toString(types);
	}
	
	public SearchKey getMin() {
		Constant[] vals = new Constant[types.length];
		for (int i = 0; i < vals.length; i++)
			vals[i] = types[i].minValue();
		return new SearchKey(vals);
	}
	
	public SearchKey getMax() {
		Constant[] vals = new Constant[types.length];
		for (int i = 0; i < vals.length; i++)
			vals[i] = types[i].maxValue();
		return new SearchKey(vals);
	}
}
