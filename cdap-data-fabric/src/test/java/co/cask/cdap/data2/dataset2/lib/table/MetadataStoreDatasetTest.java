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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.Id;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

public class MetadataStoreDatasetTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @Test
  public void testList() throws Exception {
    Id.DatasetInstance storeTable = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "store_table");
    dsFrameworkUtil.createInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);

    Table table = dsFrameworkUtil.getInstance(storeTable);
    MetadataStoreDataset metadataStoreDataset = new MetadataStoreDataset(table);
    Assert.assertNotNull(metadataStoreDataset);

    // Write some values
    for (int i = 0; i < 5; ++i) {
      MDSKey mdsKey = new MDSKey.Builder().add(i).build();
      metadataStoreDataset.write(mdsKey, i);
    }

    // Fetch one record at a time
    for (int i = 0; i < 5; ++i) {
      final int fv = i;
      Map<MDSKey, Integer> val = metadataStoreDataset.listKV(new MDSKey.Builder().add(0).build(),
                                                             new MDSKey.Builder().add(5).build(),
                                                             Integer.class, 1,
                                                             new Predicate<Integer>() {
                                                               @Override
                                                               public boolean apply(Integer input) {
                                                                 return input == fv;
                                                               }
                                                             });
      Assert.assertEquals(1, val.size());
      Assert.assertEquals(i, (int) Iterables.get(val.values(), 0));
    }

    // Fetch two records at a time
    for (int i = 0; i < 4; ++i) {
      final int fv = i;
      Map<MDSKey, Integer> val = metadataStoreDataset.listKV(new MDSKey.Builder().add(0).build(),
                                                              new MDSKey.Builder().add(5).build(),
                                                              Integer.class, 2,
                                                              new Predicate<Integer>() {
                                                                @Override
                                                                public boolean apply(Integer input) {
                                                                  return input == fv || input == fv + 1;
                                                                }
                                                              });
      Assert.assertEquals(2, val.size());
      Assert.assertEquals(i, (int) Iterables.get(val.values(), 0));
      Assert.assertEquals(i + 1, (int) Iterables.get(val.values(), 1));
    }
  }
}
