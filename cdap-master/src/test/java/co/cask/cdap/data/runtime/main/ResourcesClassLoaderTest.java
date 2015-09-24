/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.common.lang.ClassLoaders;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URL;

/**
 *
 */
public class ResourcesClassLoaderTest {

  @SuppressWarnings("AccessStaticViaInstance")
  @Test
  public void testCustomResourceLoading() throws Exception {
    // Using default classloader
    JobConf jobConf = new JobConf();
    // foo-loader is not defined in default classloader
    Assert.assertNull(jobConf.get("foo-loader"));
    // On first load, TestClass.init should be false
    Assert.assertFalse(TestClass.init);
    TestClass.init = true;

    // Using ResourcesClassLoader with URL /test-conf
    URL url = getClass().getResource("/test-conf/mapred-site.xml");
    ClassLoader previousClassLoader = ClassLoaders.setContextClassLoader(
      new ResourcesClassLoader(new URL[]{getParentUrl(url)}, getClass().getClassLoader()));
    jobConf = new JobConf();
    Assert.assertEquals("bar-loader", jobConf.get("foo-loader"));
    // TestClass is already initialzed earlier, hence TestClass.init should be true
    TestClass testClass =
      (TestClass) Thread.currentThread().getContextClassLoader().loadClass(TestClass.class.getName()).newInstance();
    Assert.assertTrue(testClass.init);
    ClassLoaders.setContextClassLoader(previousClassLoader);

    // Using ResourcesClassLoader with URL /test-app-conf
    url = getClass().getResource("/test-app-conf/mapred-site.xml");
    previousClassLoader = ClassLoaders.setContextClassLoader(
      new ResourcesClassLoader(new URL[]{getParentUrl(url)}, getClass().getClassLoader()));
    jobConf = new JobConf();
    Assert.assertEquals("baz-app-loader", jobConf.get("foo-loader"));
    // TestClass is already initialzed earlier, hence TestClass.init should be true
    testClass =
      (TestClass) Thread.currentThread().getContextClassLoader().loadClass(TestClass.class.getName()).newInstance();
    Assert.assertTrue(testClass.init);
    ClassLoaders.setContextClassLoader(previousClassLoader);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testFirstNonNull() throws Exception {
    Integer null1 = null;
    Integer five = 5;

    Assert.assertNull(ResourcesClassLoader.firstNonNull(null1, null1));
    Assert.assertEquals((Integer) 5, ResourcesClassLoader.firstNonNull(null1, five));
    Assert.assertEquals((Integer) 5, ResourcesClassLoader.firstNonNull(five, null1));
    Assert.assertEquals((Integer) 5, ResourcesClassLoader.firstNonNull(five, five));
  }

  private URL getParentUrl(URL url) throws Exception {
    URI uri = url.toURI();
    return uri.getPath().endsWith("/") ? uri.resolve("..").toURL() : uri.resolve(".").toURL();
  }

  public static final class TestClass {
    public static boolean init = false;
  }
}
