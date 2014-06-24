package com.continuuity.data2.dataset2.lib.kv;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 *
 */
public abstract class NoTxKeyValueTableTest {

  private static final byte[] KEY1 = Bytes.toBytes("key1");
  private static final byte[] KEY2 = Bytes.toBytes("key2");

  private static final byte[] VALUE1 = Bytes.toBytes("value1");
  private static final byte[] VALUE2 = Bytes.toBytes("value2");

  protected abstract DatasetDefinition<? extends NoTxKeyValueTable, ? extends DatasetAdmin> getDefinition()
    throws IOException;

  @Test
  public void test() throws IOException {
    DatasetDefinition<? extends NoTxKeyValueTable, ? extends DatasetAdmin> def = getDefinition();
    DatasetSpecification spec = def.configure("table", DatasetProperties.EMPTY);

    ClassLoader cl = NoTxKeyValueTable.class.getClassLoader();

    // create & exists
    DatasetAdmin admin = def.getAdmin(spec, cl);
    Assert.assertFalse(admin.exists());
    admin.create();
    Assert.assertTrue(admin.exists());

    // put/get
    NoTxKeyValueTable table = def.getDataset(spec, cl);
    Assert.assertNull(table.get(KEY1));
    table.put(KEY1, VALUE1);
    Assert.assertArrayEquals(VALUE1, table.get(KEY1));
    Assert.assertNull(table.get(KEY2));

    // override
    table.put(KEY1, VALUE2);
    Assert.assertArrayEquals(VALUE2, table.get(KEY1));
    Assert.assertNull(table.get(KEY2));

    // delete & truncate
    table.put(KEY2, VALUE1);
    Assert.assertArrayEquals(VALUE2, table.get(KEY1));
    Assert.assertArrayEquals(VALUE1, table.get(KEY2));
    table.put(KEY2, null);
    Assert.assertNull(table.get(KEY2));
    Assert.assertArrayEquals(VALUE2, table.get(KEY1));

    admin.truncate();
    Assert.assertNull(table.get(KEY1));
    Assert.assertNull(table.get(KEY2));

    Assert.assertTrue(admin.exists());
    admin.drop();
    Assert.assertFalse(admin.exists());

    // drop should cleanup data
    admin.create();
    Assert.assertTrue(admin.exists());
    Assert.assertNull(table.get(KEY1));
    Assert.assertNull(table.get(KEY2));

    table.put(KEY1, VALUE1);
    Assert.assertArrayEquals(VALUE1, table.get(KEY1));

    admin.drop();
    Assert.assertFalse(admin.exists());
    admin.create();
    Assert.assertTrue(admin.exists());
    Assert.assertNull(table.get(KEY1));
  }
}
