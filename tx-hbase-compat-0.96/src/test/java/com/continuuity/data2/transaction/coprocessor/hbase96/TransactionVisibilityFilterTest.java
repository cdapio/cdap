/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction.coprocessor.hbase96;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.hbase.AbstractTransactionVisibilityFilterTest;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * HBase 0.96 specific test for filtering logic applied when reading data transactionally.
 */
public class TransactionVisibilityFilterTest extends AbstractTransactionVisibilityFilterTest {
  /**
   * Test filtering of KeyValues for in-progress and invalid transactions.
   * @throws Exception
   */
  @Test
  public void testFiltering() throws Exception {
    /*
     * Start and stop some transactions.  This will give us a transaction state something like the following
     * (numbers only reflect ordering, not actual transaction IDs):
     *   6  - in progress
     *   5  - committed
     *   4  - invalid
     *   3  - in-progress
     *   2  - committed
     *   1  - committed
     *
     *   read ptr = 5
     *   write ptr = 6
     */

    Transaction tx1 = txManager.startShort();
    assertTrue(txManager.canCommit(tx1, EMPTY_CHANGESET));
    assertTrue(txManager.commit(tx1));

    Transaction tx2 = txManager.startShort();
    assertTrue(txManager.canCommit(tx2, EMPTY_CHANGESET));
    assertTrue(txManager.commit(tx2));

    Transaction tx3 = txManager.startShort();
    Transaction tx4 = txManager.startShort();
    txManager.invalidate(tx4.getWritePointer());

    Transaction tx5 = txManager.startShort();
    assertTrue(txManager.canCommit(tx5, EMPTY_CHANGESET));
    assertTrue(txManager.commit(tx5));

    Transaction tx6 = txManager.startShort();

    Map<byte[], Long> ttls = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    Filter filter = createFilter(tx6, ttls);

    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", "val1", tx6.getWritePointer())));
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", "val1", tx5.getWritePointer())));
    assertEquals(Filter.ReturnCode.SKIP,
                 filter.filterKeyValue(newKeyValue("row1", "val1", tx4.getWritePointer())));
    assertEquals(Filter.ReturnCode.SKIP,
                 filter.filterKeyValue(newKeyValue("row1", "val1", tx3.getWritePointer())));
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", "val1", tx2.getWritePointer())));
  }

  /**
   * Test filtering for TTL settings.
   * @throws Exception
   */
  @Test
  public void testTTLFiltering() throws Exception {
    Map<byte[], Long> ttls = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    ttls.put(FAM, 10L);
    ttls.put(FAM2, 30L);
    ttls.put(FAM3, 0L);

    Transaction tx = txManager.startShort();
    long now = tx.getVisibilityUpperBound();
    Filter filter = createFilter(tx, ttls);
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", FAM, "val1", now)));
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", FAM, "val1", now - 1 * TxConstants.MAX_TX_PER_MS)));
    assertEquals(Filter.ReturnCode.NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", FAM, "val1", now - 11 * TxConstants.MAX_TX_PER_MS)));
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", FAM2, "val1", now - 11 * TxConstants.MAX_TX_PER_MS)));
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", FAM2, "val1", now - 21 * TxConstants.MAX_TX_PER_MS)));
    assertEquals(Filter.ReturnCode.NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", FAM2, "val1", now - 31 * TxConstants.MAX_TX_PER_MS)));
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", FAM3, "val1", now - 31 * TxConstants.MAX_TX_PER_MS)));
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row1", FAM3, "val1", now - 1001 * TxConstants.MAX_TX_PER_MS)));
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row2", FAM, "val1", now)));
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL,
                 filter.filterKeyValue(newKeyValue("row2", FAM, "val1", now - 1 * TxConstants.MAX_TX_PER_MS)));
  }

  @Override
  protected Filter createFilter(Transaction tx, Map<byte[], Long> familyTTLs) {
    return new TransactionVisibilityFilter(tx, familyTTLs, false);
  }

  protected KeyValue newKeyValue(String rowkey, String value, long timestamp) {
    return new KeyValue(Bytes.toBytes(rowkey), FAM, COL, timestamp, Bytes.toBytes(value));
  }

  protected KeyValue newKeyValue(String rowkey, byte[] family, String value, long timestamp) {
    return new KeyValue(Bytes.toBytes(rowkey), family, COL, timestamp, Bytes.toBytes(value));
  }
}
