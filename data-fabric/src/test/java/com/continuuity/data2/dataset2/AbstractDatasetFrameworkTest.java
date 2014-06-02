package com.continuuity.data2.dataset2;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public abstract class AbstractDatasetFrameworkTest {
  protected abstract DatasetFramework getFramework();

  @Test
  public void testSimpleDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();
    String moduleName = "inMemory";

    framework.register(moduleName, InMemoryTableModule.class);

    // Performing admin operations to create dataset instance
    framework.addInstance("orderedTable", "my_table", DatasetInstanceProperties.EMPTY);
    DatasetAdmin admin = framework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    admin.create();

    // Accessing dataset instance to perform data operations
    OrderedTable table = framework.getDataset("my_table", null);
    Assert.assertNotNull(table);
    table.put(Bytes.toBytes("key1"), Bytes.toBytes("column1"), Bytes.toBytes("value1"));
    Assert.assertEquals("value1", Bytes.toString(table.get(Bytes.toBytes("key1"), Bytes.toBytes("column1"))));

    // cleanup
    framework.deleteInstance("my_table");
    framework.deleteModule("inMemory");
  }

  @Test
  public void testCompositeDataset() throws Exception {
    // Configuring Dataset types
    DatasetFramework framework = getFramework();

    framework.register("inMemory", InMemoryTableModule.class);
    framework.register("keyValue", KeyValueTableDefinition.KeyValueTableModule.class);

    // Performing admin operations to create dataset instance
    framework.addInstance("keyValueTable", "my_table", DatasetInstanceProperties.EMPTY);
    DatasetAdmin admin = framework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    admin.create();

    // Accessing dataset instance to perform data operations
    KeyValueTableDefinition.KeyValueTable table = framework.getDataset("my_table", null);
    Assert.assertNotNull(table);
    table.put("key1", "value1");
    Assert.assertEquals("value1", table.get("key1"));
    // cleanup
    framework.deleteInstance("my_table");
    framework.deleteModule("keyValue");
    framework.deleteModule("inMemory");
  }

}
