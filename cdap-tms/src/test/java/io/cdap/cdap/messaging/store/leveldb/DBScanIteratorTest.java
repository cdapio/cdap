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

package io.cdap.cdap.messaging.store.leveldb;

import io.cdap.cdap.api.common.Bytes;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DBScanIterator}.
 */
public class DBScanIteratorTest {

  @Test
  public void testScanRangeAndCleanClose() {
    List<Map.Entry<byte[], byte[]>> entries = Arrays.asList(
        new AbstractMap.SimpleImmutableEntry<>(Bytes.toBytes("k1"), Bytes.toBytes("v1")),
        new AbstractMap.SimpleImmutableEntry<>(Bytes.toBytes("k2"), Bytes.toBytes("v2")),
        new AbstractMap.SimpleImmutableEntry<>(Bytes.toBytes("k3"), Bytes.toBytes("v3")));
    final Iterator<Map.Entry<byte[], byte[]>> backing = entries.iterator();

    DBIterator dbIterator = Mockito.mock(DBIterator.class);
    Mockito.when(dbIterator.hasNext()).thenAnswer(inv -> backing.hasNext());
    Mockito.when(dbIterator.next()).thenAnswer(inv -> backing.next());

    DB db = Mockito.mock(DB.class);
    Mockito.when(db.iterator()).thenReturn(dbIterator);

    DBScanIterator scanIterator = new DBScanIterator(db, Bytes.toBytes("k1"), Bytes.toBytes("k3"));
    List<String> keys = new ArrayList<>();
    while (scanIterator.hasNext()) {
      keys.add(Bytes.toString(scanIterator.next().getKey()));
    }
    Assert.assertEquals(Arrays.asList("k1", "k2"), keys);
    scanIterator.close();
    Assert.assertFalse(scanIterator.hasNext());
  }

  @Test
  public void testClosePropagatesIoExceptionAndMarksIteratorClosed() throws IOException {
    final IOException expectedIo = new IOException("iterator close failure");
    DBIterator dbIterator = Mockito.mock(DBIterator.class);
    Mockito.when(dbIterator.hasNext()).thenReturn(true);
    Mockito.when(dbIterator.next()).thenReturn(
        new AbstractMap.SimpleImmutableEntry<>(Bytes.toBytes("k1"), Bytes.toBytes("v1")));
    Mockito.doThrow(expectedIo).when(dbIterator).close();

    DB db = Mockito.mock(DB.class);
    Mockito.when(db.iterator()).thenReturn(dbIterator);

    DBScanIterator scanIterator = new DBScanIterator(db, null, null);
    try {
      scanIterator.close();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedIo, actual.getCause());
    }
    // Even when close() throws, the finally block must mark the iterator closed / endOfData
    Assert.assertFalse(scanIterator.hasNext());
  }
}
