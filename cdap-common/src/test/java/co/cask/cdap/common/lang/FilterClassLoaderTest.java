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

package co.cask.cdap.common.lang;

import co.cask.cdap.api.app.Application;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import javax.script.ScriptEngineFactory;
import javax.ws.rs.PUT;

/**
 */
public class FilterClassLoaderTest {

  @Test(expected = ClassNotFoundException.class)
  public void testSystemInternalsHidden() throws ClassNotFoundException {
    FilterClassLoader classLoader = FilterClassLoader.create(this.getClass().getClassLoader());
    classLoader.loadClass(FilterClassLoader.class.getName());
  }

  @Test
  public void testAPIVisible() throws ClassNotFoundException {
    FilterClassLoader classLoader = FilterClassLoader.create(this.getClass().getClassLoader());
    Assert.assertSame(Application.class, classLoader.loadClass(Application.class.getName()));

    // Dependencies of API classes should also be visible
    Assert.assertSame(Logger.class, classLoader.loadClass(Logger.class.getName()));

    // JAX-RS classes should also be visible
    Assert.assertSame(PUT.class, classLoader.loadClass(PUT.class.getName()));
  }

  @Test
  public void testBootstrapResourcesVisible() throws IOException {
    FilterClassLoader classLoader = FilterClassLoader.create(this.getClass().getClassLoader());
    Assert.assertNotNull(classLoader.getResource("java/lang/String.class"));
  }

  @Test
  public void testHadoopResourcesVisible() throws ClassNotFoundException {
    FilterClassLoader classLoader = FilterClassLoader.create(this.getClass().getClassLoader());
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(classLoader);
    try {
      // VersionInfo will based on the context class loader to find the "common-version-info.properties" file.
      // If it is missing/failed to locate that, getVersion() will returns "Unknown".
      Assert.assertNotEquals("Unknown", VersionInfo.getVersion());
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }

    // Load standard Hadoop class. It should pass. The class loader of the loaded class should be the same
    // as the system Configuration class.
    Assert.assertSame(Configuration.class, classLoader.loadClass(Configuration.class.getName()));
  }
}
