/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.metrics.store.timeseries;

import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class EntityTableTest {

  @Test
  public void testGetId() throws Exception {
    InMemoryTableService.create("testGetId");
    MetricsTable table = new InMemoryMetricsTable("testGetId");

    EntityTable entityTable = new EntityTable(table);

    // Make sure it is created sequentially
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // It should get the same value (from cache)
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // Construct another entityTable, it should load from storage.
    entityTable = new EntityTable(table);
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // ID for different type should have ID starts from 1 again.
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("flow", "flow" + i));
    }
  }

  @Test
  public void testRecycleAfterMaxId() throws Exception {
    InMemoryTableService.create("testRecycleId");
    MetricsTable table = new InMemoryMetricsTable("testRecycleId");

    EntityTable entityTable = new EntityTable(table, 101);

    // Generate 500 entries, the (101-200) will replace the (1-100) values and so on as we
    // only have 100 entries as maxId.
    for (long i = 1; i <= 500; i++) {
      entityTable.getId("app", "app" + i);
    }

    // we call getName for the 100 entries, it will be the latest entries 401-500
    for (long i = 1; i <= 100; i++) {
      Assert.assertEquals("app" + String.valueOf(400 + i), entityTable.getName(i, "app"));
    }
  }

  @Test
  public void testGetName() throws Exception {
    InMemoryTableService.create("testGetName");
    MetricsTable table = new InMemoryMetricsTable("testGetName");

    EntityTable entityTable = new EntityTable(table);

    // Create some entities.
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // Reverse lookup
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals("app" + i, entityTable.getName(i, "app"));
    }
  }
}
