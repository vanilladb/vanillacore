package org.vanilladb.core.storage.index;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.vanilladb.core.sql.Constant;

/**
 * A SearchKey represents an array of constants for a list of indexed fields.
 */
public class SearchKey implements Comparable<SearchKey> {

	private Constant[] vals;
	private boolean hasHashCode;
	private int hashCode;

	/**
	 * Constructs from the given values for the specified field names.
	 * 
	 * @param indexedFields
	 *            the list of field names for the key
	 * @param fldValMap
	 *            the map stores the actual values
	 * @throws NullPointerException
	 *             if there is a missing value
	 */
	public SearchKey(List<String> indexedFields, Map<String, Constant> fldValMap) {
		vals = new Constant[indexedFields.size()];
		Iterator<String> fldNameIter = indexedFields.iterator();
		String fldName;

		for (int i = 0; i < vals.length; i++) {
			fldName = fldNameIter.next();
			vals[i] = fldValMap.get(fldName);
			if (vals[i] == null)
				throw new NullPointerException("there is no value for '" + fldName + "'");
		}
	}

	public SearchKey(Constant... constants) {
		vals = Arrays.copyOf(constants, constants.length);
	}
	
	public int length() {
		return vals.length;
	}
	
	public Constant get(int index) {
		return vals[index];
	}
	
	@Override
	public String toString() {
		return Arrays.toString(vals);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		
		if (this == obj)
			return true;
		
		if (!obj.getClass().equals(SearchKey.class))
			return false;
		
		SearchKey targetKey = (SearchKey) obj;
		
		if (vals.length != targetKey.vals.length)
			return false;
		
		for (int i = 0; i < vals.length; i++) {
			if (!vals[i].equals(targetKey.vals[i]))
				return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		if (hasHashCode)
			return hashCode;
		
		// Generate the hash code
		hashCode = 37;
		for (Constant val : vals)
			hashCode = 37 * hashCode + val.hashCode();
		hasHashCode = true;
		
		return hashCode;
	}
	
	@Override
	public int compareTo(SearchKey targetKey) {
		// It will not compare the key without the same length
		if (vals.length != targetKey.vals.length)
			throw new IllegalArgumentException("The compared key does not have the same length");
		
		// Compare the value one by one
		int comResult;
		for (int i = 0; i < vals.length; i++) {
			comResult = vals[i].compareTo(targetKey.vals[i]);
			
			if (comResult < 0)
				return -1;
			else if (comResult > 0)
				return 1;
		}
		return 0;
	}
}
