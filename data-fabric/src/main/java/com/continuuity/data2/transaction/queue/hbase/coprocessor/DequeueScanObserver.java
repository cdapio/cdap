package com.continuuity.data2.transaction.queue.hbase.coprocessor;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.queue.hbase.DequeueScanAttributes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public class DequeueScanObserver extends BaseRegionObserver {
  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    ConsumerConfig consumerConfig = DequeueScanAttributes.getConsumerConfig(scan);
    Transaction tx = DequeueScanAttributes.getTx(scan);
    byte[] queueName = DequeueScanAttributes.getQueueName(scan);

    if (consumerConfig == null || tx == null || queueName == null) {
      return super.preScannerOpen(e, scan, s);
    }

    Filter dequeueFilter = new DequeueFilter(queueName, consumerConfig, tx);

    Filter existing = scan.getFilter();
    if (existing != null) {
      Filter combined = new FilterList(FilterList.Operator.MUST_PASS_ALL, existing, dequeueFilter);
      scan.setFilter(combined);
    } else {
      scan.setFilter(dequeueFilter);
    }

    return super.preScannerOpen(e, scan, s);
  }

  private static class DequeueFilter extends FilterBase {
    private final ConsumerConfig consumerConfig;
    private final Transaction transaction;
    private final int queueNamePrefixLength;
    private final byte[] stateColumnName;

    private boolean stopScan;

    private boolean skipRow;

    private int counter;
    private long writePointer;

    public DequeueFilter(byte[] queueName, ConsumerConfig consumerConfig, Transaction transaction) {
      this.consumerConfig = consumerConfig;
      this.transaction = transaction;
      this.queueNamePrefixLength = QueueEntryRow.getQueueRowPrefix(queueName).length;
      this.stateColumnName = Bytes.add(QueueEntryRow.STATE_COLUMN_PREFIX,
                                       Bytes.toBytes(consumerConfig.getGroupId()));
    }

    @Override
    public boolean filterAllRemaining() {
      return stopScan;
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
      // last 4 bytes in a row key
      counter = Bytes.toInt(buffer, offset + length - 4, Ints.BYTES);
      // row key is queue_name + writePointer + counter
      writePointer = Bytes.toLong(buffer, offset + queueNamePrefixLength, Longs.BYTES);

      // If writes later than the reader pointer, abort the loop, as entries that comes later are all uncommitted.
      // this is probably not needed due to the limit of the scan to the stop row, but to be safe...
      if (writePointer > transaction.getReadPointer()) {
        stopScan = true;
        return true;
      }

      // If the write is in the excluded list, ignore it.
      if (transaction.isExcluded(writePointer)) {
        return true;
      }

      return false;
    }

    @Override
    public void filterRow(List<KeyValue> kvs) {
      byte[] dataBytes = null;
      byte[] metaBytes = null;
      byte[] stateBytes = null;
      // list is very short so it is ok to loop thru to find columns
      for (KeyValue kv : kvs) {
        if (hasQualifier(kv, QueueEntryRow.DATA_COLUMN)) {
          dataBytes = kv.getValue();
        } else if (hasQualifier(kv, QueueEntryRow.META_COLUMN)) {
          metaBytes = kv.getValue();
        } else if (hasQualifier(kv, stateColumnName)) {
          stateBytes = kv.getValue();
        }
      }

      if (dataBytes == null || metaBytes == null) {
        skipRow = true;
        return;
      }

      QueueEntryRow.CanConsume canConsume =
        QueueEntryRow.canConsume(consumerConfig, transaction, writePointer, counter, metaBytes, stateBytes);

      skipRow = QueueEntryRow.CanConsume.YES != canConsume;
    }

    @Override
    public boolean filterRow() {
      return skipRow;
    }

    private static boolean hasQualifier(KeyValue kv, byte[] qual) {
      return Bytes.equals(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
                          qual, 0, qual.length);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      throw new UnsupportedOperationException("Unexpected call of Writable interface method write(DataOutput)");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      throw new UnsupportedOperationException("Unexpected call of Writable interface method readFields(DataInput)");
    }
  }
}
