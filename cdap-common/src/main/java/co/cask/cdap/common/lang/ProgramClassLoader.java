/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.lang;

import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import javax.annotation.Nullable;

/**
 * ClassLoader that implements bundle jar feature, in which the application jar contains
 * its dependency jars inside.
 */
public class ProgramClassLoader extends DirectoryClassLoader {

  /**
   * Constructs an instance that load classes from the given directory, without program type. See
   * {@link ProgramResources#getVisibleResources(ProgramType)} for details on system classes that
   * are visible to the returned ClassLoader.
   */
  public static ProgramClassLoader create(File unpackedJarDir, ClassLoader parentClassLoader) throws IOException {
    return create(unpackedJarDir, parentClassLoader, null);
  }

  /**
   * Constructs an instance that load classes from the given directory for the given program type.
   * <p/>
   * The URLs for class loading are:
   * <p/>
   * <pre>
   * [dir]
   * [dir]/*.jar
   * [dir]/lib/*.jar
   * </pre>
   */
  public static ProgramClassLoader create(File unpackedJarDir, ClassLoader parentClassLoader,
                                          @Nullable ProgramType programType) throws IOException {
    Predicate<String> predicate = Predicates.in(ProgramResources.getVisibleResources(programType));
    ClassLoader filteredParent = new FilterClassLoader(predicate, parentClassLoader);
    return new ProgramClassLoader(unpackedJarDir, filteredParent);
  }

  private ProgramClassLoader(File dir, ClassLoader parent) {
    super(dir, parent, "lib");
  }

  /**
   * ClassLoader that filters out certain resources.
   */
  private static final class FilterClassLoader extends ClassLoader {

    private final Predicate<String> resourceAcceptor;
    private final ClassLoader bootstrapClassLoader;

    /**
     * @param resourceAcceptor Filter for accepting resources
     * @param parentClassLoader Parent classloader
     */
    FilterClassLoader(Predicate<String> resourceAcceptor, ClassLoader parentClassLoader) {
      super(parentClassLoader);
      this.resourceAcceptor = resourceAcceptor;
      this.bootstrapClassLoader = new URLClassLoader(new URL[0], null);
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      // Try to load it from bootstrap class loader first
      try {
        return bootstrapClassLoader.loadClass(name);
      } catch (ClassNotFoundException e) {
        if (isValidResource(classNameToResourceName(name))) {
          return super.loadClass(name, resolve);
        }
        throw e;
      }
    }

    @Override
    public URL findResource(String name) {
      if (isValidResource(name)) {
        return super.findResource(name);
      }

      return null;
    }

    @Override
    public Enumeration<URL> findResources(String name) throws IOException {
      if (isValidResource(name)) {
        return super.findResources(name);
      }

      return Iterators.asEnumeration(ImmutableList.<URL>of().iterator());
    }

    private String classNameToResourceName(String className) {
      return className.replace('.', '/') + ".class";
    }

    private boolean isValidResource(String resourceName) {
      return resourceAcceptor.apply(resourceName);
    }
  }
}
