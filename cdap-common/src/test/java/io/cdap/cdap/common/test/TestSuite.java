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

package io.cdap.cdap.common.test;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.common.lang.ClassLoaders;
import java.util.List;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

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
    this(suiteClass, builder, TestRunner.TEST_CLASSLOADER);
  }

  @VisibleForTesting
  TestSuite(Class<?> suiteClass, RunnerBuilder builder, ClassLoader classLoader)
      throws InitializationError {
    this(builder,
        loadClass(suiteClass, classLoader),
        resolveRunnerClasses(getAnnotatedClasses(suiteClass), classLoader));
  }

  private TestSuite(RunnerBuilder builder, Class<?> loadedSuiteClass, Class<?>[] runnerClasses)
      throws InitializationError {
    super(loadedSuiteClass);
    // Match run(RunNotifier): set TCCL during runner construction so class initialization and
    // @Parameterized.Parameters on reloaded children resolve against the suite's classloader.
    ClassLoader prevClassLoader =
        ClassLoaders.setContextClassLoader(loadedSuiteClass.getClassLoader());
    try {
      this.runners = builder.runners(loadedSuiteClass, runnerClasses);
    } finally {
      ClassLoaders.setContextClassLoader(prevClassLoader);
    }
  }

  /**
   * Resolves suite child classes in the given {@link ClassLoader}, leaving classes whose
   * {@link RunWith} runner is {@link TestRunner}, {@link TestSuite}, or a subclass thereof
   * (including inherited annotations) on the caller's classloader so those runners reload their
   * targets without instantiating a second {@code MainClassLoader}.
   */
  @VisibleForTesting
  static Class<?>[] resolveRunnerClasses(Class<?>[] runnerClasses, ClassLoader classLoader)
      throws InitializationError {
    Class<?>[] resolved = new Class<?>[runnerClasses.length];
    for (int i = 0; i < runnerClasses.length; i++) {
      RunWith runWith = runnerClasses[i].getAnnotation(RunWith.class);
      boolean managesSuiteClassLoader = runWith != null
          && (TestRunner.class.isAssignableFrom(runWith.value())
              || TestSuite.class.isAssignableFrom(runWith.value()));
      resolved[i] = managesSuiteClassLoader
          ? runnerClasses[i]
          : loadClass(runnerClasses[i], classLoader);
    }
    return resolved;
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
   * Loads the given class using the specified {@link ClassLoader}.
   */
  private static Class<?> loadClass(Class<?> testClass, ClassLoader classLoader)
      throws InitializationError {
    try {
      return classLoader.loadClass(testClass.getName());
    } catch (ClassNotFoundException e) {
      throw new InitializationError(e);
    }
  }
}
