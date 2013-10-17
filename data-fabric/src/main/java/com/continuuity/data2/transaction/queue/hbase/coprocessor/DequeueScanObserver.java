package com.continuuity.data2.transaction.queue.hbase.coprocessor;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
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
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public class DequeueScanObserver extends BaseRegionObserver {
  private static final String ATTR_CONSUMER_CONFIG = "continuuity.queue.dequeue.consumerConfig";
  private static final String ATTR_TX = "continuuity.queue.dequeue.transaction";
  private static final String ATTR_QUEUE_NAME = "continuuity.queue.dequeue.queueName";

  public static void setAttributes(Scan scan,
                                   QueueName queueName, ConsumerConfig consumerConfig, Transaction transaction) {
    try {
      scan.setAttribute(ATTR_CONSUMER_CONFIG, toBytes(consumerConfig));
      scan.setAttribute(ATTR_TX, toBytes(transaction));
    } catch (IOException e) {
      // SHOULD NEVER happen
      throw new RuntimeException(e);
    }
    scan.setAttribute(ATTR_QUEUE_NAME, queueName.toBytes());
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    byte[] consumerConfigAttr = scan.getAttribute(ATTR_CONSUMER_CONFIG);
    byte[] txAttr = scan.getAttribute(ATTR_TX);
    byte[] queueName = scan.getAttribute(ATTR_QUEUE_NAME);
    if (consumerConfigAttr == null || txAttr == null || queueName == null) {
      return super.preScannerOpen(e, scan, s);
    }

    ConsumerConfig consumerConfig = bytesToConsumerConfig(consumerConfigAttr);
    Transaction tx = bytesToTx(txAttr);
    Filter dequeueFilter = new DequeueFilter(queueName, consumerConfig, tx);

    Filter existing = scan.getFilter();
    Filter combined = new FilterList(FilterList.Operator.MUST_PASS_ALL, existing, dequeueFilter);
    scan.setFilter(combined);

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

  // serde for attributes

  private static byte[] toBytes(ConsumerConfig consumerConfig) throws IOException {
    ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
    dataOutput.writeLong(consumerConfig.getGroupId());
    dataOutput.writeInt(consumerConfig.getGroupSize());
    dataOutput.writeInt(consumerConfig.getInstanceId());
    WritableUtils.writeEnum(dataOutput, consumerConfig.getDequeueStrategy());
    WritableUtils.writeString(dataOutput, consumerConfig.getHashKey());
    return dataOutput.toByteArray();
  }

  private static ConsumerConfig bytesToConsumerConfig(byte[] bytes) throws IOException {
    ByteArrayDataInput dataInput = ByteStreams.newDataInput(bytes);
    long groupId = dataInput.readLong();
    int groupSize = dataInput.readInt();
    int instanceId = dataInput.readInt();
    DequeueStrategy strategy = WritableUtils.readEnum(dataInput, DequeueStrategy.class);
    String hashKey = WritableUtils.readString(dataInput);

    return new ConsumerConfig(groupId, instanceId, groupSize, strategy, hashKey);
  }

  private static byte[] toBytes(Transaction tx) throws IOException {
    ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
    dataOutput.writeLong(tx.getReadPointer());
    dataOutput.writeLong(tx.getWritePointer());
    dataOutput.writeLong(tx.getFirstShortInProgress());
    write(dataOutput, tx.getInProgress());
    write(dataOutput, tx.getInvalids());
    return dataOutput.toByteArray();
  }

  private static Transaction bytesToTx(byte[] bytes) throws IOException {
    ByteArrayDataInput dataInput = ByteStreams.newDataInput(bytes);
    long readPointer = dataInput.readLong();
    long writePointer = dataInput.readLong();
    long firstShortInProgress = dataInput.readLong();
    long[] inProgress = readLongArray(dataInput);
    long[] invalids = readLongArray(dataInput);
    return new Transaction(readPointer, writePointer, invalids, inProgress, firstShortInProgress);
  }

  private static void write(DataOutput dataOutput, long[] array) throws IOException {
    dataOutput.writeInt(array.length);
    for (long val : array) {
      dataOutput.writeLong(val);
    }
  }

  private static long[] readLongArray(DataInput dataInput) throws IOException {
    int length = dataInput.readInt();
    long[] array = new long[length];
    for (int i = 0; i < array.length; i++) {
      array[i] = dataInput.readLong();
    }
    return array;
  }
}
