/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests covering the {@link co.cask.cdap.api.dataset.lib.CounterTimeseriesTable} class.
 */
public class CounterTimeseriesTableTest extends AbstractDatasetTest {
  private static CounterTimeseriesTable table = null;
  private static Id.DatasetInstance counterTable = Id.DatasetInstance.from(NAMESPACE_ID, "counterTable");

  @BeforeClass
  public static void setup() throws Exception {
    createInstance("counterTimeseriesTable", counterTable, DatasetProperties.EMPTY);
    table = getInstance(counterTable);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    deleteInstance(counterTable);
  }

  @Test
  public void testCounter() throws Exception {
    TransactionExecutor tx = newTransactionExecutor(table);
    tx.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        byte[] rowKey1 = Bytes.toBytes("1");
        byte[] rowKey2 = Bytes.toBytes("2");
        byte[] rowKey3 = Bytes.toBytes("3");
        byte[] rowKey4 = Bytes.toBytes("4");

        long timestamp1 = System.currentTimeMillis();
        long timestamp2 = timestamp1 + 500;
        long timestamp3 = timestamp1 + 1000;
        long timestamp4 = timestamp1 + 1001;
        long timestamp5 = timestamp1 + 1500;
        long timestamp6 = timestamp1 + 2000;

        byte[] tag1 = Bytes.toBytes('t');
        byte[] tag2 = Bytes.toBytes('u');

        // Test increment
        assertEquals(2L, table.increment(rowKey1, 2L, timestamp1));
        assertEquals(2L, table.increment(rowKey1, 0L, timestamp1));
        assertEquals(7L, table.increment(rowKey1, 5L, timestamp1));
        assertEquals(-2L, table.increment(rowKey2, -2L, timestamp1));
        assertEquals(0L, table.increment(rowKey3, 0L, timestamp1));
        assertEquals(5L, table.increment(rowKey1, 5L, timestamp2));
        assertEquals(10L, table.increment(rowKey1, 5L, timestamp2));
        assertEquals(7L, table.increment(rowKey1, 7L, timestamp3));
        assertEquals(-2L, table.increment(rowKey2, 0L, timestamp1));

        // Test set
        table.set(rowKey1, 20L, timestamp4);
        Iterator<CounterTimeseriesTable.Counter> result = table.read(rowKey1, timestamp4, timestamp4);
        assertCounterEquals(rowKey1, 20L, timestamp4, result.next());
        assertFalse(result.hasNext());

        // Test read
        result = table.read(rowKey1, timestamp1, timestamp4);
        assertCounterEquals(rowKey1, 7L, timestamp1, result.next());
        assertCounterEquals(rowKey1, 10L, timestamp2, result.next());
        assertCounterEquals(rowKey1, 7L, timestamp3, result.next());
        assertCounterEquals(rowKey1, 20L, timestamp4, result.next());
        assertFalse(result.hasNext());

        result = table.read(rowKey1, timestamp1, timestamp3, 1, 2);
        assertCounterEquals(rowKey1, 10L, timestamp2, result.next());
        assertCounterEquals(rowKey1, 7L, timestamp3, result.next());
        assertFalse(result.hasNext());

        table.set(rowKey4, 3L, timestamp1, tag1);
        table.increment(rowKey4, 5L, timestamp2);
        table.increment(rowKey4, 7L, timestamp3);
        table.increment(rowKey4, 11L, timestamp3, tag1);
        table.increment(rowKey4, 13L, timestamp3);
        table.increment(rowKey4, 17L, timestamp4, tag1);
        table.increment(rowKey4, 19L, timestamp4);
        table.increment(rowKey4, 23L, timestamp5, tag1);
        table.increment(rowKey4, 29L, timestamp5, tag1);
        table.increment(rowKey4, 44L, timestamp5, tag2, tag1);
        table.set(rowKey4, 31L, timestamp5);
        table.set(rowKey4, 37L, timestamp6, tag2);
        table.set(rowKey3, 41L, timestamp5, tag1);
        table.set(rowKey3, 43L, timestamp5);
        table.set(rowKey3, 47L, timestamp6, tag1, tag2);

        result = table.read(rowKey4, timestamp1, timestamp6, tag1);
        assertCounterEquals(rowKey4, 3L, timestamp1, result.next());
        assertCounterEquals(rowKey4, 11L, timestamp3, result.next());
        assertCounterEquals(rowKey4, 17L, timestamp4, result.next());
        assertCounterEquals(rowKey4, 52L, timestamp5, result.next());
        assertCounterEquals(rowKey4, 44L, timestamp5, result.next());
        assertFalse(result.hasNext());

        // Multiple tags
        result = table.read(rowKey3, timestamp1, timestamp6, tag2, tag1);
        assertCounterEquals(rowKey3, 47L, timestamp6, result.next());
        assertFalse(result.hasNext());
        result = table.read(rowKey4, timestamp1, timestamp6, tag1, tag2);
        assertCounterEquals(rowKey4, 44L, timestamp5, result.next());
        assertFalse(result.hasNext());
      }
    });
  }

  public static void assertCounterEquals(byte[] expectedCount, long expectedValue, long expectedTimestamp,
                                         CounterTimeseriesTable.Counter actual) {
    assertEquals(expectedCount.length, actual.getCounter().length);
    for (int i = 0; i < expectedCount.length; i++) {
      assertTrue(expectedCount[i] == actual.getCounter()[i]);
    }
    assertEquals(expectedValue, actual.getValue());
    assertEquals(expectedTimestamp, actual.getTimestamp());
  }
}
