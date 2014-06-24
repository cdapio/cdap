/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.ticker.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Scanner;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.DataSetManager;
import com.continuuity.test.ReactorTestBase;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class MultiIndexedTableTest extends ReactorTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(MultiIndexedTableTest.class);

  private static final byte[] TIMESTAMP_COL = Bytes.toBytes("ts");
  private static final byte[] COLOR_COL = Bytes.toBytes("c");
  private static final byte[] TEMP_COL = Bytes.toBytes("t");

  private static Random random = new Random();

  @Test
  public void testKeyValueIndexing() throws Exception {
    
    // TransactionContext txContext = newTransaction();
    ApplicationManager appManager = deployApplication(AppWithMultiIndexedTable.class);
    DataSetManager<MultiIndexedTable> myTableManager = appManager.getDataSet("indexedTable");
    MultiIndexedTable table = myTableManager.get();
    long now = System.currentTimeMillis();

    Put p = createRecord(now++, "blue", "hot");
    byte[] row1 = p.getRow();
    table.put(p);

    p = createRecord(now++, "blue", "cold");
    byte[] row2 = p.getRow();
    table.put(p);

    p = createRecord(now++, "green", "warm");
    byte[] row3 = p.getRow();
    table.put(p);

    p = createRecord(now++, "green", "hot");
    byte[] row4 = p.getRow();
    table.put(p);

    p = createRecord(now++, "red", "cold");
    byte[] row5 = p.getRow();
    table.put(p);

    p = createRecord(now++, "blue", "cold");
    byte[] row6 = p.getRow();
    table.put(p);

    p = createRecord(now++, "blue", "hot");
    byte[] row7 = p.getRow();
    table.put(p);

    // Must flush for the data to be available to scans
    myTableManager.flush();

    Table index = table.getIndexTable();
    Scanner scan = index.scan(Bytes.EMPTY_BYTE_ARRAY, null);
    Row nextRow = null;
    int cnt = 0;
    while ((nextRow = scan.next()) != null) {
      LOG.info("Row " + cnt + ": " + Bytes.toStringBinary(nextRow.getRow()));
      cnt++;
    }

    // Try some queries
    Map<byte[], byte[]> criteria = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    // blue: 1, 2, 6, 7
    criteria.put(COLOR_COL, Bytes.toBytes("blue"));
    List<Row> results = table.readBy(criteria, 0, Long.MAX_VALUE - 1);
    assertEquals(4, results.size());
    assertContains(results, row1);
    assertContains(results, row2);
    assertContains(results, row6);
    assertContains(results, row7);

    criteria.clear();
    results.clear();

    // green: 3, 4
    criteria.put(COLOR_COL, Bytes.toBytes("green"));
    results = table.readBy(criteria, 0, Long.MAX_VALUE - 1);
    assertEquals(2, results.size());
    assertContains(results, row3);
    assertContains(results, row4);

    criteria.clear();
    results.clear();

    // cold: 2, 5, 6
    criteria.put(TEMP_COL, Bytes.toBytes("cold"));
    results = table.readBy(criteria, 0, Long.MAX_VALUE - 1);
    assertEquals(3, results.size());
    assertContains(results, row2);
    assertContains(results, row5);
    assertContains(results, row6);

    criteria.clear();
    results.clear();

    // hot: 1, 4, 7
    criteria.put(TEMP_COL, Bytes.toBytes("hot"));
    results = table.readBy(criteria, 0, Long.MAX_VALUE - 1);
    assertEquals(3, results.size());
    assertContains(results, row1);
    assertContains(results, row4);
    assertContains(results, row7);

    criteria.clear();
    results.clear();

    // blue + hot: 1, 7
    criteria.put(COLOR_COL, Bytes.toBytes("blue"));
    criteria.put(TEMP_COL, Bytes.toBytes("hot"));
    results = table.readBy(criteria, 0, Long.MAX_VALUE - 1);
    assertEquals(2, results.size());
    assertContains(results, row1);
    assertContains(results, row7);

    criteria.clear();
    results.clear();

    // blue + cold: 2, 6
    criteria.put(COLOR_COL, Bytes.toBytes("blue"));
    criteria.put(TEMP_COL, Bytes.toBytes("cold"));
    results = table.readBy(criteria, 0, Long.MAX_VALUE - 1);
    assertEquals(2, results.size());
    assertContains(results, row2);
    assertContains(results, row6);

    criteria.clear();
    results.clear();

    // red + hot: no matches
    criteria.put(COLOR_COL, Bytes.toBytes("red"));
    criteria.put(TEMP_COL, Bytes.toBytes("hot"));
    results = table.readBy(criteria, 0, Long.MAX_VALUE - 1);
    assertEquals(0, results.size());
  }

  private Put createRecord(long timestamp, String color, String temp) {
    Put p = new Put(Bytes.toBytes(Math.abs(random.nextLong())));
    p.add(TIMESTAMP_COL, timestamp);
    p.add(COLOR_COL, color);
    p.add(TEMP_COL, temp);
    return p;
  }

  private void assertContains(List<Row> results, byte[] expectedKey) {
    for (Row r : results) {
      if (Bytes.equals(expectedKey, r.getRow())) {
        return;
      }
    }
    fail("Expected row " + Bytes.toStringBinary(expectedKey) + " not found");
  }
}
