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
package co.cask.cdap.test;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.Id;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class KeyValueTableTest extends IntegrationTestBase {

  static final byte[] KEY1 = Bytes.toBytes("KEY1");
  static final byte[] KEY2 = Bytes.toBytes("KEY2");
  static final byte[] KEY3 = Bytes.toBytes("KEY3");
  static final byte[] VAL1 = Bytes.toBytes("VAL1");
  static final byte[] VAL2 = Bytes.toBytes("VAL2");
  static final byte[] VAL3 = Bytes.toBytes("VAL3");

  @Test
  public void test() throws Exception {
    Id.DatasetInstance instance = Id.DatasetInstance.from("default", "kv");

    DatasetClient client = getDatasetClient();
    client.create(instance, new DatasetInstanceConfiguration("keyValueTable", null));

    try {
      RKeyValueTable kvTable = new RKeyValueTable(instance, client);

      // write a value and read it back
      kvTable.write(KEY1, VAL1);
      Assert.assertArrayEquals(VAL1, kvTable.read(KEY1));

      // update the value and read it back
      kvTable.write(KEY1, VAL2);
      Assert.assertArrayEquals(VAL2, kvTable.read(KEY1));

      // attempt to swap, expecting old value
      Assert.assertFalse(kvTable.compareAndSwap(KEY1, VAL1, VAL3));
      Assert.assertArrayEquals(VAL2, kvTable.read(KEY1));

      // swap the value and read it back
      Assert.assertTrue(kvTable.compareAndSwap(KEY1, VAL2, VAL3));
      Assert.assertArrayEquals(VAL3, kvTable.read(KEY1));

      // delete the value and verify its gone
      kvTable.delete(KEY1);
      Assert.assertNull(kvTable.read(KEY1));
    } finally {
      client.delete(instance);
    }
  }

}