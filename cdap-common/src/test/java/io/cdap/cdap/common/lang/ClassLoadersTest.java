/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.common.lang;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ClassLoaders}.
 */
public class ClassLoadersTest {

  @Test
  public void testLoadClassWithExplicitAndFallbackClassLoader() throws Exception {
    ClassLoader explicitLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
    Class<?> loadedExplicit = ClassLoaders.loadClass(
        ClassLoadersTest.class.getName(), explicitLoader, this);
    Assert.assertSame(ClassLoadersTest.class, loadedExplicit);

    // Null classLoader falls back via firstNonNull to caller.getClass().getClassLoader().
    Class<?> loadedFallback = ClassLoaders.loadClass(
        ClassLoadersTest.class.getName(), null, this);
    Assert.assertSame(ClassLoadersTest.class, loadedFallback);

    // When both classLoader and caller.getClass().getClassLoader() are null (bootstrap class String),
    // firstNonNull throws NullPointerException.
    try {
      ClassLoaders.loadClass(String.class.getName(), null, "bootstrapCaller");
      Assert.fail("Expected NullPointerException when both classLoaders are null");
    } catch (NullPointerException expected) {
      // expected
    }
  }

  @Test
  public void testGetClassPathUrlAndMalformedUrlPropagation() throws Exception {
    URL fileClassUrl = new URL("file:/tmp/classes/io/cdap/Sample.class");
    Assert.assertEquals(new URL("file:/tmp/classes"),
        ClassLoaders.getClassPathURL("io.cdap.Sample", fileClassUrl));

    URL jarClassUrl = new URL("jar:file:/tmp/lib/my.jar!/io/cdap/Sample.class");
    Assert.assertEquals(new URL("file:/tmp/lib/my.jar"),
        ClassLoaders.getClassPathURL("io.cdap.Sample", jarClassUrl));

    try {
      ClassLoaders.getClassPathURL("io.cdap.Sample",
          new URL("http://example.com/io/cdap/Sample.class"));
      Assert.fail("Expected IllegalStateException for unsupported protocol");
    } catch (IllegalStateException expected) {
      // expected
    }

    // Malformed inner protocol inside a jar: URL triggers MalformedURLException -> RuntimeException.
    URL malformedInnerJarUrl = new URL("jar", "",
        "unknownproto://example.com/my.jar!/io/cdap/Sample.class");
    try {
      ClassLoaders.getClassPathURL("io.cdap.Sample", malformedInnerJarUrl);
      Assert.fail("Expected RuntimeException wrapping MalformedURLException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof MalformedURLException);
    }
  }
}
