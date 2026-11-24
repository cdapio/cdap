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

package io.cdap.cdap.data2.dataset2;

import io.cdap.cdap.data2.datafabric.dataset.service.TestModule1;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DatasetDefinitionRegistries}.
 */
public class DatasetDefinitionRegistriesTest {

  @Test
  public void testRegisterWithNullClassLoaderAndFallbackToSystemClassLoader() throws Exception {
    // 1. Null classLoader uses systemClassLoader via Objects.firstNonNull(classLoader, systemClassLoader)
    InMemoryDatasetDefinitionRegistry registry1 = new InMemoryDatasetDefinitionRegistry();
    DatasetDefinitionRegistries.register(TestModule1.class.getName(), null, registry1);
    Assert.assertTrue(registry1.hasType("datasetType1"));

    // 2. Isolated empty URLClassLoader fails first, then falls back to systemClassLoader
    try (URLClassLoader emptyCl = new URLClassLoader(new URL[0], null)) {
      InMemoryDatasetDefinitionRegistry registry2 = new InMemoryDatasetDefinitionRegistry();
      DatasetDefinitionRegistries.register(TestModule1.class.getName(), emptyCl, registry2);
      Assert.assertTrue(registry2.hasType("datasetType1"));

      // 3. Unknown class fails on both custom and system classLoader with suppressed exception
      try {
        DatasetDefinitionRegistries.register("io.cdap.cdap.MissingModule", emptyCl, registry2);
        Assert.fail("Expected ClassNotFoundException");
      } catch (ClassNotFoundException e) {
        Assert.assertEquals(1, e.getSuppressed().length);
      }
    }
  }
}
