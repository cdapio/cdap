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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.Id;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MetadataStoreDatasetTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @Test
  public void testList() throws Exception {
    Id.DatasetInstance storeTable = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "testList");
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

  @Test
  public void testScan() throws Exception {
    Id.DatasetInstance storeTable = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "testScan");
    dsFrameworkUtil.createInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);

    Table table = dsFrameworkUtil.getInstance(storeTable);
    MetadataStoreDataset metadataStoreDataset = new MetadataStoreDataset(table);
    Assert.assertNotNull(metadataStoreDataset);

    // Write some values
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < 25; ++i) {
      MDSKey mdsKey = new MDSKey.Builder().add(i).build();
      metadataStoreDataset.write(mdsKey, i);
      expected.add(i);
    }

    MDSKey start = new MDSKey.Builder().add(0).build();
    MDSKey end = new MDSKey.Builder().add(25).build();

    List<Integer> actual = new ArrayList<>();
    // Use scan limit of 3
    int scanLimit = 3;
    int runs = 0;
    while (true) {
      ScanFunction function = new ScanFunction(scanLimit);
      metadataStoreDataset.scan(start, end, Integer.class, function);
      if (function.getNumProcessed() == 0) {
        break;
      }

      ++runs;
      Assert.assertTrue(function.getValues().size() <= scanLimit);
      actual.addAll(function.getValues());
      start = new MDSKey(Bytes.stopKeyForPrefix(function.getLastKey().getKey()));
    }

    Assert.assertEquals(9, runs);
    Assert.assertEquals(expected, actual);
  }

  private static class ScanFunction implements Function<MetadataStoreDataset.KeyValue<Integer>, Boolean> {
    private final List<Integer> values = new ArrayList<>();
    private final int limit;
    private int numProcessed = 0;
    private MDSKey lastKey;

    public ScanFunction(int limit) {
      this.limit = limit;
    }

    public List<Integer> getValues() {
      return Collections.unmodifiableList(values);
    }

    public int getNumProcessed() {
      return numProcessed;
    }

    public MDSKey getLastKey() {
      return lastKey;
    }

    @Override
    public Boolean apply(MetadataStoreDataset.KeyValue<Integer> input) {
      if (++numProcessed > limit) {
        return false;
      }

      lastKey = input.getKey();
      values.add(input.getValue());
      return true;
    }
  }
}
