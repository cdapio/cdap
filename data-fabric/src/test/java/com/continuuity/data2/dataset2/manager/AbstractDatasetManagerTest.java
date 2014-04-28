package com.continuuity.data2.dataset2.manager;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset2.manager.inmemory.KeyValueTableDefinition;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public abstract class AbstractDatasetManagerTest {
  protected abstract DatasetManager getDatasetManager();

  @Test
  public void testSimpleDataset() throws Exception {
    // Configuring Dataset types
    DatasetManager manager = getDatasetManager();
    String moduleName = "inMemory";

    manager.register(moduleName, InMemoryTableModule.class);

    // Performing admin operations to create dataset instance
    manager.addInstance("orderedTable", "my_table", DatasetInstanceProperties.EMPTY);
    DatasetAdmin admin = manager.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    admin.create();

    // Accessing dataset instance to perform data operations
    OrderedTable table = manager.getDataset("my_table", null);
    Assert.assertNotNull(table);
    table.put(Bytes.toBytes("key1"), Bytes.toBytes("column1"), Bytes.toBytes("value1"));
    Assert.assertEquals("value1", Bytes.toString(table.get(Bytes.toBytes("key1"), Bytes.toBytes("column1"))));

    // cleanup
    manager.deleteInstance("my_table");
    manager.deleteModule("inMemory");
  }

  @Test
  public void testCompositeDataset() throws Exception {
    // Configuring Dataset types
    DatasetManager manager = getDatasetManager();

    manager.register("inMemory", InMemoryTableModule.class);
    manager.register("keyValue", KeyValueTableDefinition.KeyValueTableModule.class);

    // Performing admin operations to create dataset instance
    manager.addInstance("keyValueTable", "my_table", DatasetInstanceProperties.EMPTY);
    DatasetAdmin admin = manager.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    admin.create();

    // Accessing dataset instance to perform data operations
    KeyValueTableDefinition.KeyValueTable table = manager.getDataset("my_table", null);
    Assert.assertNotNull(table);
    table.put("key1", "value1");
    Assert.assertEquals("value1", table.get("key1"));
    // cleanup
    manager.deleteInstance("my_table");
    manager.deleteModule("keyValue");
    manager.deleteModule("inMemory");
  }

}
