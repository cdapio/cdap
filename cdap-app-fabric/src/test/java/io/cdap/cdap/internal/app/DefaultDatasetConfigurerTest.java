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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.api.DefaultDatasetConfigurer;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DefaultDatasetConfigurerTest {

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateDatasetModules() {
    DefaultDatasetConfigurer configurer = new DefaultDatasetConfigurer();

    configurer.addDatasetModule("TestModule", TestModule1.class);
    // adding the same one multiple times is ok
    configurer.addDatasetModule("TestModule", TestModule1.class);

    // using a different class should fail
    configurer.addDatasetModule("TestModule", TestModule2.class);
  }

  @Test
  public void testDuplicateDatasetInstance() {
    DefaultDatasetConfigurer configurer = new DefaultDatasetConfigurer();

    configurer.createDataset("ds", Table.class.getName());
    // adding the same one multiple times is ok
    configurer.createDataset("ds", Table.class);
    configurer.createDataset("ds", Table.class.getName(), DatasetProperties.EMPTY);
    configurer.createDataset("ds", Table.class, DatasetProperties.EMPTY);

    // adding a different one with the same name should fail
    try {
      configurer.createDataset("ds", "Taybull");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      configurer.createDataset("ds", IndexedTable.class);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      configurer.createDataset("ds", "Table", DatasetProperties.builder().add("k", "v").build());
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      configurer.createDataset("ds", Table.class, DatasetProperties.builder().add("k", "v").build());
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private static class TestModule1 implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      //no-op
    }
  }

  private static class TestModule2 implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      //no-op
    }
  }
}
