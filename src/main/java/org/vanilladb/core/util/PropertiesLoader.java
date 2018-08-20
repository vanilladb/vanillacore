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
package org.vanilladb.core.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class PropertiesLoader {
	private static Logger logger = Logger.getLogger(PropertiesLoader.class
			.getName());

	protected PropertiesLoader() {
		// read properties file
		boolean config = false;
		String path = getConfigFilePath();
		if (path != null) {
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(path);
				System.getProperties().load(fis);
				config = true;
			} catch (IOException e) {
				// do nothing
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException e) {
					// do nothing
				}
			}
		}
		if (!config && logger.isLoggable(Level.WARNING))
			logger.warning("error reading the config file: "
					+ getConfigFilePath() + ", using defaults");
	}

	public String getPropertyAsString(String propertyName, String defaultValue) {
		String value = getPropertyValue(propertyName);

		// can't find property
		if (value == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("can't find property: " + propertyName
						+ ", using default value: " + defaultValue);
			return defaultValue;
		}

		return value;
	}

	public boolean getPropertyAsBoolean(String propertyName,
			boolean defaultValue) {
		String value = getPropertyValue(propertyName);

		// can't find property
		if (value == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("can't find property: " + propertyName
						+ ", using default value: " + defaultValue);
			return defaultValue;
		}

		// parse to boolean
		boolean boolValue;
		try {
			boolValue = Boolean.parseBoolean(value);
		} catch (NumberFormatException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("parsing property: " + propertyName
						+ " into boolean fails, using default value: "
						+ defaultValue);
			boolValue = defaultValue;
		}

		return boolValue;
	}

	public int getPropertyAsInteger(String propertyName, int defaultValue) {
		String value = getPropertyValue(propertyName);

		// can't find property
		if (value == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("can't find property: " + propertyName
						+ ", using default value: " + defaultValue);
			return defaultValue;
		}

		// parse to int
		int intValue;
		try {
			intValue = Integer.parseInt(value);
		} catch (NumberFormatException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("parsing property: " + propertyName
						+ " into integer fails, using default value: "
						+ defaultValue);
			intValue = defaultValue;
		}

		return intValue;
	}

	public long getPropertyAsLong(String propertyName, long defaultValue) {
		String value = getPropertyValue(propertyName);

		// can't find property
		if (value == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("can't find property: " + propertyName
						+ ", using default value: " + defaultValue);
			return defaultValue;
		}

		// parse to long
		long longValue;
		try {
			longValue = Long.parseLong(value);
		} catch (NumberFormatException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("parsing property: " + propertyName
						+ " into long fails, using default value: "
						+ defaultValue);
			longValue = defaultValue;
		}

		return longValue;
	}

	public double getPropertyAsDouble(String propertyName, double defaultValue) {
		String value = getPropertyValue(propertyName);

		// can't find property
		if (value == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("can't find property: " + propertyName
						+ ", using default value: " + defaultValue);
			return defaultValue;
		}

		// parse to long
		double doubleValue;
		try {
			doubleValue = Double.parseDouble(value);
		} catch (NumberFormatException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("parsing property: " + propertyName
						+ " into double fails, using default value: "
						+ defaultValue);
			doubleValue = defaultValue;
		}

		return doubleValue;
	}

	public String[] getPropertyAsStringArray(String propertyName,
			String[] defaultArray) {
		String value = getPropertyValue(propertyName);

		// can't find property
		if (value == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("can't find property: " + propertyName
						+ ", using default array: "
						+ Arrays.toString(defaultArray));
			return defaultArray;
		}

		// split string by ','
		return value.split(",");
	}

	public int[] getPropertyAsIntegerArray(String propertyName,
			int[] defaultArray) {
		String value = getPropertyValue(propertyName);

		// can't find property
		if (value == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("can't find property: " + propertyName
						+ ", using default array: "
						+ Arrays.toString(defaultArray));
			return defaultArray;
		}

		// split string by ','
		String[] stringArray = value.split(",");

		// parse to double values
		int[] intArray = new int[stringArray.length];
		try {
			for (int i = 0; i < intArray.length; i++)
				intArray[i] = Integer.parseInt(stringArray[i]);
		} catch (NumberFormatException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("parsing property: " + propertyName
						+ " into int array fails, using default array: "
						+ Arrays.toString(defaultArray));
			return defaultArray;
		}

		return intArray;
	}

	public double[] getPropertyAsDoubleArray(String propertyName,
			double[] defaultArray) {
		String value = getPropertyValue(propertyName);

		// can't find property
		if (value == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("can't find property: " + propertyName
						+ ", using default array: "
						+ Arrays.toString(defaultArray));
			return defaultArray;
		}

		// split string by ','
		String[] stringArray = value.split(",");

		// parse to double values
		double[] doubleArray = new double[stringArray.length];
		try {
			for (int i = 0; i < doubleArray.length; i++)
				doubleArray[i] = Double.parseDouble(stringArray[i]);
		} catch (NumberFormatException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("parsing property: " + propertyName
						+ " into double array fails, using default array: "
						+ Arrays.toString(defaultArray));
			return defaultArray;
		}

		return doubleArray;
	}

	public Class<?> getPropertyAsClass(String propertyName,
			Class<?> defaultClass, Class<?> superClassConstraint) {
		String value = getPropertyValue(propertyName);

		// can't find property
		if (value == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("can't find property: " + propertyName
						+ ", using default value: " + defaultClass);
			return defaultClass;
		}

		// parse to class
		Class<?> parsedClass;
		try {
			parsedClass = Class.forName(value);

			// check super class constraint
			if (superClassConstraint != null
					&& !superClassConstraint.isAssignableFrom(parsedClass)) {
				if (logger.isLoggable(Level.WARNING))
					logger.warning("'" + parsedClass
							+ "' class is not the subclass of '"
							+ superClassConstraint + "' for property: "
							+ propertyName + ", using default class: "
							+ defaultClass);
				parsedClass = defaultClass;
			}

		} catch (ClassNotFoundException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("can't find " + value + " for property: "
						+ propertyName + ", using default class: "
						+ defaultClass);
			parsedClass = defaultClass;
		}

		return parsedClass;
	}

	protected abstract String getConfigFilePath();

	private String getPropertyValue(String propertyName) {
		// get a property value as string
		String value = System.getProperty(propertyName);

		if (value == null || value.isEmpty())
			return null;

		return value.trim();
	}
}
