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
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.util.List;

/**
 * A test suite similar to Junit {@link Suite} that can be used in the {@link RunWith} annotation to run multiple
 * test classes. It works with the {@link TestRunner} to provide correct classloading for CDAP unit-test.
 */
public class TestSuite extends ParentRunner<Runner> {

  private static Class<?>[] getAnnotatedClasses(Class<?> suiteClass) throws InitializationError {
    Suite.SuiteClasses annotation = suiteClass.getAnnotation(Suite.SuiteClasses.class);
    if (annotation == null) {
      throw new InitializationError(String.format("class '%s' must have a SuiteClasses annotation",
                                                  suiteClass.getName()));
    }
    return annotation.value();
  }

  private final List<Runner> runners;

  @SuppressWarnings("unused")
  public TestSuite(Class<?> suiteClass, RunnerBuilder builder) throws InitializationError {
    this(builder, suiteClass, getAnnotatedClasses(suiteClass));
  }

  private TestSuite(RunnerBuilder builder, Class<?> suiteClass, Class<?>[] runnerClasses) throws InitializationError {
    super(createClass(suiteClass));
    this.runners = builder.runners(createClass(suiteClass), runnerClasses);
  }

  @Override
  protected List<Runner> getChildren() {
    return runners;
  }

  @Override
  protected Description describeChild(Runner child) {
    return child.getDescription();
  }

  @Override
  protected void runChild(Runner runner, final RunNotifier notifier) {
    runner.run(notifier);
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
  private static Class<?> createClass(Class<?> suiteClass) throws InitializationError {
    try {
      return TestRunner.TEST_CLASSLOADER.loadClass(suiteClass.getName());
    } catch (ClassNotFoundException e) {
      throw new InitializationError(e);
    }
  }
}
