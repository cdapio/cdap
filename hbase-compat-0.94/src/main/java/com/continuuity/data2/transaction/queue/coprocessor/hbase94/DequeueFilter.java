package com.continuuity.data2.transaction.queue.coprocessor.hbase94;

import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.queue.hbase.DequeueScanAttributes;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueAdmin;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Server-side filter for dequeue operation
 */
// todo: unit-test it (without DequeueScanObserver)
public class DequeueFilter extends FilterBase {
  private ConsumerConfig consumerConfig;
  private Transaction transaction;
  private byte[] stateColumnName;

  private int queueNamePrefixLength;

  private boolean stopScan;
  private boolean skipRow;

  private int counter;
  private long writePointer;

  public DequeueFilter(byte[] queueRowPrefix, ConsumerConfig consumerConfig, Transaction transaction) {
    this.consumerConfig = consumerConfig;
    this.transaction = transaction;
    // +1 for salting
    this.queueNamePrefixLength = queueRowPrefix.length + HBaseQueueAdmin.SALT_BYTES;
    this.stateColumnName = Bytes.add(QueueEntryRow.STATE_COLUMN_PREFIX,
                                     Bytes.toBytes(consumerConfig.getGroupId()));
  }

  @Override
  public void reset() {
    stopScan = false;
    skipRow = false;
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
  public boolean hasFilterRow() {
    return true;
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

    // Only skip the row when canConsumer == NO, so that in case of NO_INCLUDING_ALL_OLDER, the client
    // can still see the row and move the scan start row.
    skipRow = canConsume == QueueEntryRow.CanConsume.NO;
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
    DequeueScanAttributes.write(out, consumerConfig);
    DequeueScanAttributes.write(out, transaction);
    out.writeInt(queueNamePrefixLength);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.consumerConfig = DequeueScanAttributes.readConsumerConfig(in);
    this.transaction = DequeueScanAttributes.readTx(in);
    this.queueNamePrefixLength = in.readInt();
  }
}
