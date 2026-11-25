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

package io.cdap.cdap.common.app;

import com.google.common.util.concurrent.MoreExecutors;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link MainClassLoader}.
 */
public class MainClassLoaderTest {

  private static final String MORE_EXECUTORS_CLASS =
      "com.google.common.util.concurrent.MoreExecutors";
  private static final String MORE_EXECUTORS_RESOURCE =
      "com/google/common/util/concurrent/MoreExecutors.class";
  private static final String PRECONDITIONS_CLASS = "com.google.common.base.Preconditions";

  @Test
  public void testCreateFromContextAndNeedIntercept() throws Exception {
    try (MainClassLoader classLoader = MainClassLoader.createFromContext()) {
      Assert.assertNotNull(classLoader);
      Assert.assertSame(String.class, classLoader.loadClass(String.class.getName()));
      Class<?> loaded = classLoader.loadClass(MainClassLoader.class.getName());
      Assert.assertEquals(MainClassLoader.class.getName(), loaded.getName());
    }
  }

  @Test
  public void testCreateFromContextLoadsNativeGuava33Classes() throws Exception {
    try (MainClassLoader classLoader = MainClassLoader.createFromContext()) {
      Assert.assertNotNull(classLoader);

      Class<?> moreExecutors = classLoader.loadClass(MORE_EXECUTORS_CLASS);
      Assert.assertSame(classLoader, moreExecutors.getClassLoader());
      Object direct = moreExecutors.getMethod("directExecutor").invoke(null);
      Assert.assertTrue(direct instanceof Executor);
      AtomicBoolean executed = new AtomicBoolean(false);
      ((Executor) direct).execute(() -> executed.set(true));
      Assert.assertTrue(executed.get());

      Class<?> preconditions = classLoader.loadClass(PRECONDITIONS_CLASS);
      Assert.assertSame(classLoader, preconditions.getClassLoader());
      Object checked = preconditions.getMethod("checkNotNull", Object.class).invoke(null, "ok");
      Assert.assertEquals("ok", checked);

      // Verify MoreExecutors remains native Java 8 bytecode (major == 52) and is not rewritten
      // or downgraded to Java 7 (major == 51).
      Assert.assertEquals(52, readClassMajorVersion(classLoader, MORE_EXECUTORS_RESOURCE));
      try (InputStream in = classLoader.getResourceAsStream(MORE_EXECUTORS_RESOURCE)) {
        Assert.assertNotNull(in);
        Assert.assertNull(classLoader.rewriteClass(MORE_EXECUTORS_CLASS, in));
      }
    }
  }

  @Test
  public void testIsolatedMainClassLoaderWithoutParentGuavaLoadsMoreExecutors() throws Exception {
    URL guavaJarUrl = MoreExecutors.class.getProtectionDomain().getCodeSource().getLocation();
    Assert.assertNotNull(guavaJarUrl);
    ClassLoader isolatedParent = ClassLoader.getSystemClassLoader().getParent();
    try {
      isolatedParent.loadClass(MORE_EXECUTORS_CLASS);
      Assert.fail("Expected isolated parent classloader to not contain Guava");
    } catch (ClassNotFoundException expected) {
      // Expected: platform/bootstrap parent has no Guava classes.
    }

    try (MainClassLoader isolatedLoader =
        new MainClassLoader(new URL[]{guavaJarUrl}, isolatedParent)) {
      Class<?> moreExecutors = isolatedLoader.loadClass(MORE_EXECUTORS_CLASS);
      Assert.assertSame(isolatedLoader, moreExecutors.getClassLoader());
      Object direct = moreExecutors.getMethod("directExecutor").invoke(null);
      Assert.assertTrue(direct instanceof Executor);
      AtomicBoolean executed = new AtomicBoolean(false);
      ((Executor) direct).execute(() -> executed.set(true));
      Assert.assertTrue(executed.get());
      Assert.assertEquals(52, readClassMajorVersion(isolatedLoader, MORE_EXECUTORS_RESOURCE));
    }
  }

  @Test
  public void testNeedInterceptPropagatesIoException() throws Exception {
    IOException simulatedIo = new IOException("corrupt class resource stream");
    URL failingUrl = createFailingUrl(simulatedIo);

    try (InspectableMainClassLoader loader = new InspectableMainClassLoader(failingUrl)) {
      Assert.assertFalse(loader.checkNeedIntercept("java.lang.String"));
      Assert.assertFalse(loader.checkNeedIntercept(MORE_EXECUTORS_CLASS));
      Assert.assertFalse(loader.checkNeedIntercept(PRECONDITIONS_CLASS));
      Assert.assertTrue(loader.checkNeedIntercept("io.cdap.cdap.common.app.MainClassLoader"));

      try {
        loader.checkNeedIntercept("com.example.BrokenCandidate");
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException caught) {
        Assert.assertSame(simulatedIo, caught.getCause());
      }
    }
  }

  private static int readClassMajorVersion(ClassLoader loader, String resourceName)
      throws IOException {
    try (InputStream in = loader.getResourceAsStream(resourceName);
         DataInputStream dataIn = new DataInputStream(in)) {
      Assert.assertNotNull(in);
      Assert.assertEquals(0xCAFEBABE, dataIn.readInt());
      dataIn.readUnsignedShort();
      return dataIn.readUnsignedShort();
    }
  }

  private static URL createFailingUrl(IOException toThrow) throws MalformedURLException {
    return new URL("file", "", -1, "/broken/BrokenCandidate.class", new URLStreamHandler() {
      @Override
      protected URLConnection openConnection(URL u) throws IOException {
        throw toThrow;
      }
    });
  }

  private static final class InspectableMainClassLoader extends MainClassLoader {
    private final URL brokenUrl;

    InspectableMainClassLoader(URL brokenUrl) {
      super(new URL[0], MainClassLoaderTest.class.getClassLoader());
      this.brokenUrl = brokenUrl;
    }

    boolean checkNeedIntercept(String className) {
      return needIntercept(className);
    }

    @Override
    public URL getResource(String name) {
      if ("com/example/BrokenCandidate.class".equals(name)) {
        return brokenUrl;
      }
      return super.getResource(name);
    }
  }
}
