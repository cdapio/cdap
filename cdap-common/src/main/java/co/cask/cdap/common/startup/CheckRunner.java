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

package co.cask.cdap.common.startup;

import co.cask.cdap.common.internal.guava.ClassPath;
import com.google.common.base.Predicate;
import com.google.inject.Injector;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Runs a collection of {@link Check Checks}.
 */
public class CheckRunner {
  private final Set<Check> checks;

  private CheckRunner(Set<Check> checks) {
    this.checks = checks;
  }

  /**
   * Runs all checks, returning all the check failures.
   *
   * @return list of check failures
   */
  public List<Failure> runChecks() {
    List<Failure> failures = new ArrayList<>();

    for (Check check : checks) {
      try {
        check.run();
      } catch (Exception e) {
        failures.add(new Failure(check.getName(), e));
      }
    }

    return failures;
  }

  /**
   * Create a builder that uses the specified injector to instantiate checks to run.
   *
   * @param injector the injector to use to instantiate the checks
   * @return builder to build a {@link CheckRunner}.
   */
  public static Builder builder(Injector injector) {
    return new Builder(injector);
  }

  /**
   * Builds a {@link CheckRunner}.
   */
  public static class Builder {
    private final Set<Check> checks;
    private final ClassLoader classLoader;
    private final Injector injector;
    private ClassPath classPath;

    public Builder(Injector injector) {
      this.checks = new HashSet<>();
      this.classLoader = this.getClass().getClassLoader();
      this.injector = injector;
    }

    /**
     * Recursively search the specified package for any {@link Check Checks} and add them.
     *
     * @param pkg the package to search for checks
     * @return this builder
     * @throws IOException if there was a problem reading resources from the classpath
     */
    public Builder addChecksInPackage(String pkg) throws IOException {
      ClassPath classPath = getClassPath();
      for (ClassPath.ClassInfo classInfo : classPath.getAllClassesRecursive(pkg)) {
        Class<?> cls = classInfo.load();
        if (!Modifier.isInterface(cls.getModifiers()) &&
          !Modifier.isAbstract(cls.getModifiers()) &&
          Check.class.isAssignableFrom(cls)) {
          checks.add((Check) injector.getInstance(cls));
        }
      }
      return this;
    }

    /**
     * Adds the {@link Check} given its class name.
     *
     * @param className the class name for the {@link Check}.
     * @return the builder
     * @throws ClassNotFoundException if the class could not be found
     * @throws IllegalArgumentException if the specified class is not a {@link Check}.
     */
    public Builder addClass(String className) throws ClassNotFoundException {
      Class<?> cls = classLoader.loadClass(className);
      if (!Check.class.isAssignableFrom(cls)) {
        throw new IllegalArgumentException(className + " does not implement " + Check.class.getName());
      }
      checks.add((Check) injector.getInstance(cls));
      return this;
    }

    /**
     * Build the {@link CheckRunner}.
     *
     * @return the {@link CheckRunner}
     */
    public CheckRunner build() {
      return new CheckRunner(checks);
    }

    private ClassPath getClassPath() throws IOException {
      if (classPath == null) {
        classPath = ClassPath.from(classLoader, new Predicate<URI>() {
          @Override
          public boolean apply(URI input) {
            // protect against cases where / is in the URLClassLoader uris.
            // this can happen when the classpath contains empty entries,
            // and the working directory is the root directory.
            // this is a fairly common scenario in init scripts.
            return !"/".equals(input.getPath());
          }
        });
      }
      return classPath;
    }
  }

  /**
   * Contains the name of a failed check and the exception that caused the failure.
   */
  public static class Failure {
    private final String name;
    private final Exception exception;

    public Failure(String name, Exception exception) {
      this.name = name;
      this.exception = exception;
    }

    public String getName() {
      return name;
    }

    public Exception getException() {
      return exception;
    }
  }
}
