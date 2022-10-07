/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import com.google.common.io.ByteStreams;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * This test runner runs a JUnit test under a delegated classloader which filters out Spark dependencies but maintains
 * this classloader as the parent of all CDAP classes loaded by the test.
 */
public class NoSparkClassLoaderTestRunner extends BlockJUnit4ClassRunner {
  private static final String CDAP_PACKAGE_NAME = "io.cdap.cdap";
  private static final String SPARK_PACKAGE_NAME = "org.apache.spark";

  public NoSparkClassLoaderTestRunner(Class<?> klass) throws ClassNotFoundException, InitializationError {
    super(getTestClassWithDelegatedClassLoader(klass));
  }

  private static Class<?> getTestClassWithDelegatedClassLoader(Class<?> klass) throws ClassNotFoundException {
    return new DelegateNoSparkClassLoader(klass.getClassLoader()).loadClass(klass.getName());
  }

  private static class DelegateNoSparkClassLoader extends ClassLoader {
    final ClassLoader delegate;

    private DelegateNoSparkClassLoader(ClassLoader delegate) {
      this.delegate = delegate;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      if (name.startsWith(CDAP_PACKAGE_NAME)) {
        // Load all CDAP classes with this classloader.
        // We manually feed the original class into the delegated classloader to maintain the parent classloader
        // relationship between the DelegatedNoSparkClassLoader and CDAP classes loaded by the classloader, otherwise
        // the delegated classloader will be used as the parent when a transitive class is instantiated which removes
        // the Spark class filtering logic.
        URL resource = delegate.getResource(name.replace('.', '/') + ".class");
        if (resource == null) {
          throw new ClassNotFoundException("Failed to find resource for class " + name);
        }
        try (InputStream is = resource.openStream()) {
          byte[] classBytes = ByteStreams.toByteArray(is);
          // defineClass must be called by this class to set the parent classloader of the loaded class
          // to this class.
          return defineClass(name, classBytes, 0, classBytes.length);
        } catch (IOException e) {
          throw new ClassNotFoundException("Failed to read class definition for class " + name, e);
        }
      } else if (name.startsWith(SPARK_PACKAGE_NAME)) {
        // Do not load Spark classes at all.
        throw new ClassNotFoundException("Spark classes are not to be loaded by the DelegateNoSparkCLassLoader");
      }
      // For non-CDAP and non-Spark classes, delegate back to original classloader.
      return delegate.loadClass(name);
    }
  }
}
