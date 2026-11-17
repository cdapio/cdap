/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.cli;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link CLIConfig}.
 */
public class CLIConfigTest {

  @Test
  public void testGetVersionLoadsResource() {
    CLIConfig config = new CLIConfig();
    Assert.assertNotNull(config.getVersion());
    Assert.assertFalse(config.getVersion().trim().isEmpty());
  }

  @Test
  public void testTryGetVersionPropagatesIoExceptionFromBrokenResource() throws Exception {
    IOException resourceError = new IOException("broken VERSION resource stream");
    URL brokenVersionUrl = new URL("broken", "localhost", -1, "/VERSION", new URLStreamHandler() {
      @Override
      protected URLConnection openConnection(URL url) {
        return new URLConnection(url) {
          @Override
          public void connect() throws IOException {
            throw resourceError;
          }

          @Override
          public InputStream getInputStream() throws IOException {
            throw resourceError;
          }
        };
      }
    });

    ClassLoader parent = CLIConfig.class.getClassLoader();
    Assert.assertTrue(parent instanceof URLClassLoader);
    URL[] urls = ((URLClassLoader) parent).getURLs();

    try (ChildFirstCliClassLoader isolatedLoader = new ChildFirstCliClassLoader(urls, parent,
        brokenVersionUrl)) {
      Class<?> isolatedCliConfigClass = isolatedLoader.loadClass(CLIConfig.class.getName());
      try {
        isolatedCliConfigClass.getConstructor().newInstance();
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (InvocationTargetException ite) {
        Throwable target = ite.getTargetException();
        Assert.assertTrue(target instanceof RuntimeException);
        Assert.assertSame(resourceError, target.getCause());
      }
    }
  }

  private static final class ChildFirstCliClassLoader extends URLClassLoader {

    private final URL brokenVersionUrl;

    ChildFirstCliClassLoader(URL[] urls, ClassLoader parent, URL brokenVersionUrl) {
      super(urls, parent);
      this.brokenVersionUrl = brokenVersionUrl;
    }

    @Override
    public URL getResource(String name) {
      if ("VERSION".equals(name)) {
        return brokenVersionUrl;
      }
      return super.getResource(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          if (name.startsWith("io.cdap.cdap.cli.")) {
            try (InputStream in = getParent().getResourceAsStream(
                name.replace('.', '/') + ".class")) {
              if (in != null) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buf = new byte[4096];
                int read;
                while ((read = in.read(buf)) != -1) {
                  out.write(buf, 0, read);
                }
                byte[] bytes = out.toByteArray();
                loaded = defineClass(name, bytes, 0, bytes.length);
              }
            } catch (IOException e) {
              throw new ClassNotFoundException(name, e);
            }
          }
          if (loaded == null) {
            loaded = super.loadClass(name, false);
          }
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }
}
