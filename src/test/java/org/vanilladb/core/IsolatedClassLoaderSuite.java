/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * <p>
 * A special suite runner that loads each of the test classes in a separate
 * class loader, along with the class under test.
 * </p>
 * 
 * <p>
 * To use this class, create a test suite class:
 * </p>
 * 
 * <pre>
 * <code> &#064;RunWith(IsolatedClassLoaderSuite.class)
 * &#064;SuiteClasses({ MyTestClass1.class, MyTestClass2.class })
 * &#064;IsolationRoot(MyRootClassLoadedInIsolation.class)
 * public class MyTestSuite {
 *     ...
 * }</code>
 * </pre>
 * 
 * <p>
 * The MyTestClass1 and MyTestClass2 classes are written as normal JUnit test
 * classes. This class allows the test classes to test those methods whose
 * behavior depends on the value of a static field or a <code>System</code>
 * property that's given at class initialization time. Each test class can set a
 * different value of the <code>System</code> property in its @BeforeClass
 * method so the corresponding class under test will be initiated differently
 * when it is loaded by a separate class loader.
 * </p>
 */

public class IsolatedClassLoaderSuite extends Suite {
	/**
	 * An annotation to be used on the test suite class to indicate the root
	 * (i.e., the first class that will be loaded) of classes under test. The
	 * class is used to find the classpath to allow loading all classes under
	 * test in a separate class loader.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	@Inherited
	public @interface IsolationRoot {
		public Class<?> value();
	}

	/**
	 * A special class loader that loads classes from its own class path
	 * (specified via URLs) before delegating to the parent class loader. This
	 * is used to load the test classes in separate class loaders, even though
	 * those classes are also loaded in the parent class loader.
	 */
	static class IsolatedClassLoader extends URLClassLoader {
		public IsolatedClassLoader(URL[] urls, ClassLoader parent) {
			super(urls, parent);
		}

		public Class<?> loadClass(String name, boolean resolve)
				throws ClassNotFoundException {
			Class<?> c = null;
			try {
				c = findLoadedClass(name);
				if (c != null)
					return c;
				c = findClass(name);
				if (resolve)
					resolveClass(c);
			} catch (ClassNotFoundException cex) {
				c = super.loadClass(name, resolve);
			}
			return c;
		}
	}

	/**
	 * Called reflectively on classes annotated with
	 * {@link org.junit.runner.RunWith &#064;RunWith}.
	 */
	public IsolatedClassLoaderSuite(Class<?> cls, RunnerBuilder builder)
			throws InitializationError {
		super(builder, cls, reloadClasses(getIsolatedRoot(cls),
				getSuiteClasses(cls)));
	}

	/**
	 * Sets the thread's context class loader to the class loader for the test
	 * class.
	 */
	@Override
	protected void runChild(Runner runner, RunNotifier notifier) {
		ParentRunner<?> pr = (ParentRunner<?>) runner; // test class runner
		ClassLoader cl = null;
		try {
			cl = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(
					pr.getTestClass().getJavaClass().getClassLoader());
			super.runChild(runner, notifier);
		} finally {
			Thread.currentThread().setContextClassLoader(cl);
		}
	}

	/**
	 * Gets the value of the {@link SuiteClasses} annotation.
	 */
	private static Class<?>[] getSuiteClasses(Class<?> cls)
			throws InitializationError {
		SuiteClasses annotation = cls.getAnnotation(SuiteClasses.class);
		if (annotation == null)
			throw new InitializationError("class '" + cls.getName()
					+ "' must have a SuiteClasses annotation");
		return annotation.value();
	}

	/**
	 * Gets the value of the {@link IsolationRoot} annotation.
	 */
	private static Class<?> getIsolatedRoot(Class<?> cls)
			throws InitializationError {
		IsolationRoot annotation = cls.getAnnotation(IsolationRoot.class);
		if (annotation == null)
			throw new InitializationError("class '" + cls.getName()
					+ "' must have an IsolatedRoot annotation");
		return annotation.value();
	}

	/**
	 * Reloads the classes in a separate class loader.
	 */
	private static Class<?>[] reloadClasses(Class<?> root,
			Class<?>[] suiteClasses) throws InitializationError {
		URL[] urls = new URL[] { classpathOf(root),
				classpathOf(IsolatedClassLoaderSuite.class) };
		Class<?> sc = null;
		try {
			for (int i = 0; i < suiteClasses.length; i++) {
				sc = suiteClasses[i];
				ClassLoader cl = new IsolatedClassLoader(urls,
						IsolatedClassLoaderSuite.class.getClassLoader());
				suiteClasses[i] = cl.loadClass(sc.getName());
			}
			return suiteClasses;
		} catch (ClassNotFoundException cex) {
			throw new InitializationError("could not reload class: " + sc);
		}
	}

	/**
	 * Returns the classpath entry used to load the named resource. Currently,
	 * only two protocols, file: and jar:, are supported.
	 */
	private static URL classpathOf(Class<?> c) {
		String name = "/" + c.getName().replace('.', '/') + ".class";
		try {
			URL url = IsolatedClassLoaderSuite.class.getResource(name);
			if (url.getProtocol().equals("file")) {
				String file = url.getPath();
				if (file.endsWith(name))
					file = file.substring(0, file.length() - name.length() + 1);
				return new URL("file", null, file);
			} else if (url.getProtocol().equals("jar")) {
				String file = url.getPath();
				int i = file.lastIndexOf('!');
				if (i >= 0)
					file = file.substring(0, i);
				return new URL(file);
			} else
				throw new UnsupportedOperationException();
		} catch (MalformedURLException mex) {
			return null;
		}
	}
}
