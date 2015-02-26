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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.transaction.queue.ConsumerEntryState;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import com.google.common.primitives.Ints;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 * HBase 0.96 implementation of {@link HBaseQueueConsumer}.
 */
final class HBase96QueueConsumer extends HBaseQueueConsumer {
  private final Filter processedStateFilter;

  HBase96QueueConsumer(CConfiguration cConf, ConsumerConfig consumerConfig, HTable hTable, QueueName queueName,
                       HBaseConsumerState consumerState, HBaseConsumerStateStore stateStore,
                       HBaseQueueStrategy queueStrategy) {
    super(cConf, consumerConfig, hTable, queueName, consumerState, stateStore, queueStrategy);
    this.processedStateFilter = createStateFilter();
  }

  @Override
  protected Scan createScan(byte[] startRow, byte[] stopRow, int numRows) {
    // Scan the table for queue entries.
    Scan scan = new Scan();
    // we should roughly divide by number of buckets, but don't want another RPC for the case we are not exactly right
    int caching = (int) (1.1 * numRows / HBaseQueueAdmin.ROW_KEY_DISTRIBUTION_BUCKETS);
    scan.setCaching(caching);
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.addColumn(QueueEntryRow.COLUMN_FAMILY, QueueEntryRow.DATA_COLUMN);
    scan.addColumn(QueueEntryRow.COLUMN_FAMILY, QueueEntryRow.META_COLUMN);
    scan.addColumn(QueueEntryRow.COLUMN_FAMILY, stateColumnName);
    scan.setFilter(createFilter());
    scan.setMaxVersions(1);
    return scan;
  }

  /**
   * Creates a HBase filter that will filter out rows that that has committed state = PROCESSED.
   */
  private Filter createFilter() {
    return new FilterList(FilterList.Operator.MUST_PASS_ONE, processedStateFilter, new SingleColumnValueFilter(
      QueueEntryRow.COLUMN_FAMILY, stateColumnName, CompareFilter.CompareOp.GREATER,
      new BinaryPrefixComparator(Bytes.toBytes(transaction.getReadPointer()))
    ));
  }

  /**
   * Creates a HBase filter that will filter out rows with state column state = PROCESSED (ignoring transaction).
   */
  private Filter createStateFilter() {
    byte[] processedMask = new byte[Ints.BYTES * 2 + 1];
    processedMask[processedMask.length - 1] = ConsumerEntryState.PROCESSED.getState();
    return new SingleColumnValueFilter(QueueEntryRow.COLUMN_FAMILY, stateColumnName,
                                       CompareFilter.CompareOp.NOT_EQUAL,
                                       new BitComparator(processedMask, BitComparator.BitwiseOp.AND));
  }
}
