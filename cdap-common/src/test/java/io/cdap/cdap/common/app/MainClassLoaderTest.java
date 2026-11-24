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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link MainClassLoader}.
 */
public class MainClassLoaderTest {

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
  public void testNeedInterceptPropagatesIoException() throws Exception {
    IOException simulatedIo = new IOException("corrupt class resource stream");
    URL failingUrl = createFailingUrl(simulatedIo);

    try (InspectableMainClassLoader loader = new InspectableMainClassLoader(failingUrl)) {
      Assert.assertFalse(loader.checkNeedIntercept("java.lang.String"));
      Assert.assertTrue(loader.checkNeedIntercept("io.cdap.cdap.common.app.MainClassLoader"));

      try {
        loader.checkNeedIntercept("com.example.BrokenCandidate");
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException caught) {
        Assert.assertSame(simulatedIo, caught.getCause());
      }
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
