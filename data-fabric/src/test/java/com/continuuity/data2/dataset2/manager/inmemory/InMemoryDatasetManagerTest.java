package com.continuuity.data2.dataset2.manager.inmemory;

import com.continuuity.api.common.Bytes;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import org.junit.Assert;
import org.junit.Test;

/**
 * unit-test
 */
public class InMemoryDatasetManagerTest {
  @Test
  public void testSimpleDataset() throws Exception {
    // Configuring Dataset types
    DatasetManager manager = new InMemoryDatasetManager();
    manager.register("inMemory", InMemoryTableModule.class);

    // Performing admin operations to create dataset instance
    manager.addInstance("orderedTable", "my_table", DatasetInstanceProperties.EMPTY);
    DatasetAdmin admin = manager.getAdmin("my_table");
    Assert.assertNotNull(admin);
    admin.create();

    // Accessing dataset instance to perform data operations
    OrderedTable table = manager.getDataset("my_table");
    Assert.assertNotNull(table);
    table.put(Bytes.toBytes("key1"), Bytes.toBytes("column1"), Bytes.toBytes("value1"));
    Assert.assertEquals("value1", Bytes.toString(table.get(Bytes.toBytes("key1"), Bytes.toBytes("column1"))));
  }

  @Test
  public void testCompositeDataset() throws Exception {
    // Configuring Dataset types
    DatasetManager manager = new InMemoryDatasetManager();
    manager.register("inMemory", InMemoryTableModule.class);
    manager.register("keyValue", KeyValueTableDefinition.KeyValueTableModule.class);

    // Performing admin operations to create dataset instance
    manager.addInstance("keyValueTable", "my_table", DatasetInstanceProperties.EMPTY);
    DatasetAdmin admin = manager.getAdmin("my_table");
    Assert.assertNotNull(admin);
    admin.create();

    // Accessing dataset instance to perform data operations
    KeyValueTableDefinition.KeyValueTable table = manager.getDataset("my_table");
    Assert.assertNotNull(table);
    table.put("key1", "value1");
    Assert.assertEquals("value1", table.get("key1"));
  }
}
