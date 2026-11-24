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

import com.google.inject.Guice;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.data2.datafabric.dataset.service.TestModule1;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.util.Collections;
import java.util.LinkedHashSet;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link StaticDatasetFramework} caching and mutation restrictions.
 */
public class StaticDatasetFrameworkTest {

  @Test
  public void testCachedRegistryAndModulesAndUnsupportedModuleMutations() throws Exception {
    StaticDatasetFramework framework = new StaticDatasetFramework(
        new DefaultDatasetDefinitionRegistryFactory(Guice.createInjector()),
        Collections.singletonMap("testModule1", new TestModule1()));

    LinkedHashSet<String> modules1 = framework.getAvailableModuleClasses(NamespaceId.DEFAULT);
    LinkedHashSet<String> modules2 = framework.getAvailableModuleClasses(NamespaceId.DEFAULT);
    Assert.assertSame(modules1, modules2);
    Assert.assertTrue(modules1.contains(TestModule1.class.getName()));

    DatasetDefinitionRegistry reg1 =
        framework.createRegistry(modules1, getClass().getClassLoader());
    DatasetDefinitionRegistry reg2 =
        framework.createRegistry(modules1, getClass().getClassLoader());
    Assert.assertSame(reg1, reg2);

    DatasetId dsId = NamespaceId.DEFAULT.dataset("staticDs");
    framework.addInstance("datasetType1", dsId, DatasetProperties.EMPTY);
    Assert.assertTrue(framework.hasInstance(dsId));
    Assert.assertNotNull(framework.getAdmin(dsId, null));

    try {
      framework.addModule(NamespaceId.DEFAULT.datasetModule("m"), new TestModule1());
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      // expected
    }

    try {
      framework.deleteModule(NamespaceId.DEFAULT.datasetModule("m"));
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      // expected
    }

    try {
      framework.deleteAllModules(NamespaceId.DEFAULT);
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }
}
