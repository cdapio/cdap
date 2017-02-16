/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.explore.service;

import com.google.common.base.Function;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 *
 */
public class ExploreServiceUtilsTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testHiveVersion() throws Exception {
    // This would throw an exception if it didn't pass
    ExploreServiceUtils.checkHiveSupport(getClass().getClassLoader());
  }

  @Test
  public void testRewriteHiveAuthFactory() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();

    URL hiveAuthURL = classLoader.getResource(HiveAuthFactory.class.getName().replace('.', '/') + ".class");
    String hiveJarFilePath = hiveAuthURL.getPath();
    File hiveJarFile = new File(URI.create(hiveJarFilePath.substring(0, hiveJarFilePath.indexOf("!/"))));

    File targetJar = ExploreServiceUtils.patchHiveClasses(hiveJarFile,
                                                          new File(tmpFolder.newFolder(), "hive.jar"));

    try (
      TestHiveAuthFactoryClassLoader cl = new TestHiveAuthFactoryClassLoader(targetJar, classLoader)
    ) {
      Class<?> hiveAuthFactoryClass = cl.loadClass(HiveAuthFactory.class.getName());
      Method loginFromKeytab = hiveAuthFactoryClass.getMethod("loginFromKeytab", HiveConf.class);

      // If the write is not successful, this will throw exception
      loginFromKeytab.invoke(null, new Object[] { null });
    }

    // Try to rewrite a jar file without the hive auth factory class, it should just return the original jar
    String functionClassPath = classLoader.getResource(Function.class.getName().replace('.', '/') + ".class").getPath();
    File guavaJarFile = new File(URI.create(functionClassPath.substring(0, functionClassPath.indexOf("!/"))));
    targetJar = ExploreServiceUtils.patchHiveClasses(guavaJarFile, new File(tmpFolder.newFolder(), "guava.jar"));

    Assert.assertSame(guavaJarFile, targetJar);
  }

  /**
   * A class loader which tries to load from the given jarFile first rather than the parent.
   */
  private static final class TestHiveAuthFactoryClassLoader extends ClassLoader implements Closeable {

    private final JarFile jarFile;

    public TestHiveAuthFactoryClassLoader(File jarFile, ClassLoader parent) throws IOException {
      super(parent);
      this.jarFile = new JarFile(jarFile);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      return loadClass(name, true);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      // Try to load from itself first
      String fileName = name.replace('.', '/') + ".class";
      JarEntry jarEntry = jarFile.getJarEntry(fileName);
      if (jarEntry == null) {
        return super.loadClass(name, resolve);
      }

      try (InputStream input = jarFile.getInputStream(jarEntry)) {
        byte[] bytes = ByteStreams.toByteArray(input);
        Class<?> cls = defineClass(name, bytes, 0, bytes.length);

        if (resolve) {
          resolveClass(cls);
        }
        return cls;

      } catch (IOException e) {
        throw new ClassNotFoundException("Class " + name + " not found.", e);
      }
    }

    @Override
    public void close() throws IOException {
      jarFile.close();
    }
  }
}
