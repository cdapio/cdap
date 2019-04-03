/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.test;

import co.cask.cdap.common.app.MainClassLoader;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.FilterClassLoader;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JUnit test {@link Runner} for running CDAP unit-test framework tests.
 */
public class TestRunner extends BlockJUnit4ClassRunner {

  private static final Logger LOG = LoggerFactory.getLogger(TestRunner.class);
  static final ClassLoader TEST_CLASSLOADER;

  static {
    // we will use this classloader only for junit itself. However, since junit uses org.hamcrest classes
    // in its API, we also have to load those with this class loader. Hence accept resources and packages
    // from org.junit and org.hamcrest.
    ClassLoader classLoader = MainClassLoader.createFromContext(new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return resource.startsWith("org/junit/")
          || resource.startsWith("org/hamcrest/");
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return packageName.equals("org.junit") || packageName.startsWith("org.junit.")
          || packageName.equals("org.hamcrest") || packageName.startsWith("org.hamcrest.");
      }
    });
    if (classLoader == null) {
      LOG.warn("Unabled to create MainClassLoader for class rewrite in unit-test. Fallback to default ClassLoader.");
      classLoader = Thread.currentThread().getContextClassLoader();
      if (classLoader == null) {
        classLoader = ClassLoader.getSystemClassLoader();
      }
    }
    TEST_CLASSLOADER = classLoader;
  }

  public TestRunner(Class<?> testClass) throws InitializationError {
    super(createClass(testClass));
  }

  @Override
  public void run(RunNotifier notifier) {
    // Set the context classloader to the test class classloader before running test
    ClassLoader cl = ClassLoaders.setContextClassLoader(getTestClass().getJavaClass().getClassLoader());
    try {
      super.run(notifier);
    } finally {
      ClassLoaders.setContextClassLoader(cl);
    }
  }

  /**
   * Creates a new test class using the {@link MainClassLoader} by copying the original test class ClassLoader urls.
   */
  private static Class<?> createClass(Class<?> testClass) throws InitializationError {
    try {
      return TEST_CLASSLOADER.loadClass(testClass.getName());
    } catch (ClassNotFoundException e) {
      throw new InitializationError(e);
    }
  }
}
