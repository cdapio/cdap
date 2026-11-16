/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table;

import io.cdap.cdap.api.dataset.DataSetException;
import io.cdap.cdap.api.dataset.table.Delete;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Scan;
import io.cdap.cdap.api.dataset.table.Table;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link MetadataStoreDataset} unchecked and checked exception propagation across
 * all seven catch blocks touched in Guava migration.
 */
public class MetadataStoreDatasetExceptionTest {

  @FunctionalInterface
  private interface MdsAction {
    void run(MetadataStoreDataset mds, MDSKey key);
  }

  @Test
  public void testAllSevenMethodsPropagateUncheckedExceptionsWithoutDoubleWrapping() {
    MDSKey key = new MDSKey.Builder().add("part1").add("part2").build();
    Set<MDSKey> keySet = Collections.singleton(key);

    assertUncheckedAndCheckedPropagation(
        (mds, k) -> mds.getFirst(k, String.class));
    assertUncheckedAndCheckedPropagation(
        (mds, k) -> mds.deleteAll(k));
    assertUncheckedAndCheckedPropagation(
        (mds, k) -> mds.delete(k));
    assertUncheckedAndCheckedPropagation(
        (mds, k) -> mds.write(k, "val"));
    assertUncheckedAndCheckedPropagation(
        (mds, k) -> mds.increment(k, 1L));
    assertUncheckedAndCheckedPropagation(
        (mds, k) -> mds.listKV(keySet, String.class, 10));
    assertUncheckedAndCheckedPropagation(
        (mds, k) -> mds.listKV(k, String.class, 10));
  }

  private static void assertUncheckedAndCheckedPropagation(MdsAction action) {
    final DataSetException expectedUnchecked = new DataSetException("table unchecked failure");
    Table uncheckedFailingTable = createThrowingTable(expectedUnchecked);
    MetadataStoreDataset mdsUnchecked = new MetadataStoreDataset(uncheckedFailingTable);
    MDSKey key = new MDSKey.Builder().add("k").build();

    try {
      action.run(mdsUnchecked, key);
      Assert.fail("Expected DataSetException to propagate directly");
    } catch (RuntimeException actual) {
      Assert.assertSame(
          "Unchecked RuntimeException must not be double-wrapped in new RuntimeException",
          expectedUnchecked, actual);
    }

    final IOException expectedChecked = new IOException("table checked failure");
    Table checkedFailingTable = createThrowingTable(expectedChecked);
    MetadataStoreDataset mdsChecked = new MetadataStoreDataset(checkedFailingTable);

    try {
      action.run(mdsChecked, key);
      Assert.fail("Expected checked exception wrapped in RuntimeException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedChecked, actual.getCause());
    }
  }

  private static Table createThrowingTable(Throwable toThrow) {
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.scan(Mockito.any(byte[].class), Mockito.any(byte[].class)))
        .thenAnswer(inv -> {
          throw toThrow;
        });
    Mockito.when(table.scan(Mockito.any(Scan.class)))
        .thenAnswer(inv -> {
          throw toThrow;
        });
    Mockito.doAnswer(inv -> {
      throw toThrow;
    }).when(table).delete(Mockito.any(byte[].class), Mockito.any(byte[].class));
    Mockito.doAnswer(inv -> {
      throw toThrow;
    }).when(table).delete(Mockito.any(Delete.class));
    Mockito.doAnswer(inv -> {
      throw toThrow;
    }).when(table).put(Mockito.any(Put.class));
    Mockito.doAnswer(inv -> {
      throw toThrow;
    }).when(table).increment(Mockito.any(byte[].class), Mockito.any(byte[].class), Mockito.anyLong());
    return table;
  }
}
