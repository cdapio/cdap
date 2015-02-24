/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.inmemory;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.dataset2.lib.table.PutValue;
import co.cask.cdap.data2.dataset2.lib.table.Update;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.NavigableMap;

/**
 *
 */
public class InMemoryTableServiceTest {
  @Test
  public void testInternalsNotLeaking() {
    // Test that there's no way to break the state of InMemoryTableService by changing parameters of update
    // methods (after method invocation) or by changing returned values "in-place"

    InMemoryTableService.create("table");

    // verify writing thru merge is guarded
    NavigableMap<byte[], NavigableMap<byte[], Update>> updates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], Update> rowUpdate = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] rowParam = new byte[] {1};
    byte[] columnParam = new byte[] {2};
    byte[] valParam = new byte[] {3};
    rowUpdate.put(columnParam, new PutValue(valParam));
    updates.put(rowParam, rowUpdate);

    InMemoryTableService.merge("table", updates, 1L);

    verify123();

    updates.remove(rowParam);
    rowUpdate.remove(columnParam);
    rowParam[0]++;
    columnParam[0]++;
    valParam[0]++;

    verify123();

    // verify changing returned data from get doesn't affect the stored data
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowFromGet =
      InMemoryTableService.get("table", new byte[]{1}, 1L);
    Assert.assertEquals(1, rowFromGet.size());
    byte[] columnFromGet = rowFromGet.firstEntry().getKey();
    Assert.assertArrayEquals(new byte[] {2}, columnFromGet);
    byte[] valFromGet = rowFromGet.firstEntry().getValue().get(1L);
    Assert.assertArrayEquals(new byte[] {3}, valFromGet);

    rowFromGet.firstEntry().getValue().remove(1L);
    rowFromGet.remove(columnFromGet);
    columnFromGet[0]++;
    valFromGet[0]++;

    verify123();

    // verify changing returned data doesn't affect the stored data
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fromGetRange = 
      InMemoryTableService.getRowRange("table", null, null, 1L);
    Assert.assertEquals(1, fromGetRange.size());
    byte[] keyFromGetRange = fromGetRange.firstEntry().getKey();
    Assert.assertArrayEquals(new byte[] {1}, keyFromGetRange);
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowFromGetRange = fromGetRange.get(new byte[] {1});
    Assert.assertEquals(1, rowFromGetRange.size());
    byte[] columnFromGetRange = rowFromGetRange.firstEntry().getKey();
    Assert.assertArrayEquals(new byte[] {2}, columnFromGetRange);
    byte[] valFromGetRange = rowFromGetRange.firstEntry().getValue().get(1L);
    Assert.assertArrayEquals(new byte[] {3}, valFromGetRange);

    rowFromGetRange.firstEntry().getValue().remove(1L);
    rowFromGetRange.remove(columnFromGetRange);
    fromGetRange.remove(keyFromGetRange);
    keyFromGetRange[0]++;
    columnFromGetRange[0]++;
    valFromGet[0]++;

    verify123();
  }

  private void verify123() {
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowFromGet =
      InMemoryTableService.get("table", new byte[]{1}, 1L);
    Assert.assertEquals(1, rowFromGet.size());
    Assert.assertArrayEquals(new byte[] {2}, rowFromGet.firstEntry().getKey());
    Assert.assertArrayEquals(new byte[] {3}, rowFromGet.firstEntry().getValue().get(1L));
  }
}
