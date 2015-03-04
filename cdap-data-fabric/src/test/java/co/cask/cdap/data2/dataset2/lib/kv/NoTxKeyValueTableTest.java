/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.kv;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.proto.Id;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public abstract class NoTxKeyValueTableTest {

  private static final byte[] KEY1 = Bytes.toBytes("key1");
  private static final byte[] KEY2 = Bytes.toBytes("key2");

  private static final byte[] VALUE1 = Bytes.toBytes("value1");
  private static final byte[] VALUE2 = Bytes.toBytes("value2");

  private static final Map<String, String> NO_ARGS = DatasetDefinition.NO_ARGUMENTS;

  protected static final Id.Namespace NAMESPACE_ID = Id.Namespace.from("myspace");

  protected abstract DatasetDefinition<? extends NoTxKeyValueTable, ? extends DatasetAdmin> getDefinition()
    throws IOException;

  @Test
  public void test() throws IOException {
    DefaultDatasetNamespace dsNamespace = new DefaultDatasetNamespace(CConfiguration.create());
    String name = dsNamespace.namespace(NAMESPACE_ID, "table");
    DatasetDefinition<? extends NoTxKeyValueTable, ? extends DatasetAdmin> def = getDefinition();
    DatasetSpecification spec = def.configure(name, DatasetProperties.EMPTY);

    ClassLoader cl = NoTxKeyValueTable.class.getClassLoader();

    // create & exists
    DatasetAdmin admin = def.getAdmin(spec, cl);
    Assert.assertFalse(admin.exists());
    admin.create();
    Assert.assertTrue(admin.exists());

    // put/get
    NoTxKeyValueTable table = def.getDataset(spec, NO_ARGS, cl);
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
