package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.queue.ConsumerEntryState;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
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
 * HBase 0.96 implementation of {@link HBaseQueue2Consumer}.
 */
final class HBase96Queue2Consumer extends HBaseQueue2Consumer {
  private final Filter processedStateFilter;

  HBase96Queue2Consumer(ConsumerConfig consumerConfig, HTable hTable, QueueName queueName,
                        HBaseConsumerState consumerState, HBaseConsumerStateStore stateStore) {
    super(consumerConfig, hTable, queueName, consumerState, stateStore);
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
