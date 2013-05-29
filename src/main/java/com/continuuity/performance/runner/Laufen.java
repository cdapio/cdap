package com.continuuity.performance.runner;

import com.google.common.base.Throwables;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.runners.model.ReflectiveCallable;
import org.junit.internal.runners.statements.Fail;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.TestClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Similar to PerformanceTestRunner.
 */
public class Laufen {

  private TestClass fTestClass;

  /**
   * Run the tests contained in the classes named in the <code>args</code>.
   * If all tests run successfully, exit with a status of 0. Otherwise exit with a status of 1.
   * Write feedback while tests are running and write
   * stack traces for all failed tests after the tests all complete.
   *
   * @param args names of classes in which to find tests to run
   */
  public static void main(String... args) throws Throwable {
    runMainAndExit(args);
  }

  /**
   * Runs main and exits.
   */
  private static void runMainAndExit(String... args) throws Throwable {
    Result result = new Laufen().runMain(args);
    //System.exit(result.wasSuccessful() ? 0 : 1);
  }

  /**
   *
   * @param args
   * @return
   * @throws Throwable
   */
  private Result runMain(String... args) throws Throwable {
    Class<?> testClass;
      try {
        testClass = Class.forName(args[0]);
        return run(testClass);
      } catch (ClassNotFoundException e) {
        System.out.println("Could not find class: " + args[0]);
        Throwables.propagate(e);
      }
    return null;
  }

  /**
   * Run all the tests in <code>classes</code>.
   *
   * @param testClass the classes containing tests
   * @return a {@link Result} describing the details of the test run and the failed tests.
   */
  private Result run(Class<?> testClass) throws Throwable {
    List<Throwable> errors = new ArrayList<Throwable>();
    fTestClass = new TestClass(testClass);

    for (FrameworkMethod eachBefore : getBeforeClassMethods()) {
      try {
        eachBefore.invokeExplosively(null);
      } catch (Throwable e) {
        errors.add(e);
      }
    }

    Object fTarget = getTarget();
    for (FrameworkMethod eachTest : getTestMethods()) {
      for (FrameworkMethod eachBefore : getBeforeMethods()) {
        eachBefore.invokeExplosively(fTarget);
      }
      eachTest.invokeExplosively(fTarget);
      for (FrameworkMethod eachAfter : getAfterMethods()) {
        try {
          eachAfter.invokeExplosively(fTarget);
        } catch (Throwable e) {
          errors.add(e);
        }
      }
    }

    for (FrameworkMethod eachAfter : getAfterClassMethods()) {
      try {
        eachAfter.invokeExplosively(null);
      } catch (Throwable e) {
        errors.add(e);
      }
    }
    MultipleFailureException.assertEmpty(errors);
    return null;
//    return run(Request.classes(classes));
  }

  /**
   * Returns a {@link org.junit.runners.model.Statement}: run all non-overridden {@code @BeforeClass} methods on this
   * class and superclasses before executing {@code statement}; if any throws an Exception, stop execution and pass
   * the exception on.
   */
  private List<FrameworkMethod> getBeforeClassMethods() {
    return fTestClass.getAnnotatedMethods(BeforeClass.class);
  }

  private List<FrameworkMethod> getAfterClassMethods() {
    return fTestClass.getAnnotatedMethods(AfterClass.class);
  }

  private List<FrameworkMethod>  getBeforeMethods() {
    return fTestClass.getAnnotatedMethods(Before.class);
  }

  private List<FrameworkMethod> getTestMethods() {
    return fTestClass.getAnnotatedMethods(Test.class);
  }

  private List<FrameworkMethod> getAfterMethods() {
    return fTestClass.getAnnotatedMethods(After.class);
  }

  private Object getTarget() {
    Object test;
    try {
      test = new ReflectiveCallable() {
        @Override
        protected Object runReflectiveCall() throws Throwable {
          return fTestClass.getOnlyConstructor().newInstance();
        }
      }.run();
      return test;
    } catch (Throwable e) {
      return new Fail(e);
    }
  }
}
