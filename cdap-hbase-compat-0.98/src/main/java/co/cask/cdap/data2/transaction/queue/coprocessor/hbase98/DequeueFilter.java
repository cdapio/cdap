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

package co.cask.cdap.data2.transaction.queue.coprocessor.hbase98;

import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.hbase.DequeueScanAttributes;
import co.cask.tephra.Transaction;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
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

  private boolean stopScan;
  private boolean skipRow;

  private int counter;
  private long writePointer;

  // For Writable
  private DequeueFilter() {
  }

  public DequeueFilter(ConsumerConfig consumerConfig, Transaction transaction) {
    this.consumerConfig = consumerConfig;
    this.transaction = transaction;
    this.stateColumnName = Bytes.add(QueueEntryRow.STATE_COLUMN_PREFIX,
                                     Bytes.toBytes(consumerConfig.getGroupId()));
  }

  @Override
  public void reset() throws IOException {
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
    counter = Bytes.toInt(buffer, offset + length - Ints.BYTES, Ints.BYTES);
    // row key is queue_name + writePointer + counter
    writePointer = Bytes.toLong(buffer, offset + length - Longs.BYTES - Ints.BYTES, Longs.BYTES);

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
  public void filterRowCells(List<Cell> cells) {
    byte[] dataBytes = null;
    byte[] metaBytes = null;
    byte[] stateBytes = null;
    // list is very short so it is ok to loop thru to find columns
    for (Cell cell : cells) {
      if (CellUtil.matchingQualifier(cell, QueueEntryRow.DATA_COLUMN)) {
        dataBytes = CellUtil.cloneValue(cell);
      } else if (CellUtil.matchingQualifier(cell, QueueEntryRow.META_COLUMN)) {
        metaBytes = CellUtil.cloneValue(cell);
      } else if (CellUtil.matchingQualifier(cell, stateColumnName)) {
        stateBytes = CellUtil.cloneValue(cell);
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

  /* Writable implementation for HBase 0.94 */

  public void write(DataOutput out) throws IOException {
    DequeueScanAttributes.write(out, consumerConfig);
    DequeueScanAttributes.write(out, transaction);
  }

  public void readFields(DataInput in) throws IOException {
    this.consumerConfig = DequeueScanAttributes.readConsumerConfig(in);
    this.transaction = DequeueScanAttributes.readTx(in);
  }

  /* Serialization support for HBase 0.98+ */

  public byte[] toByteArray() throws IOException {
    // TODO: in the future actual serialization here should be done using protobufs
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    write(new DataOutputStream(bos));
    return bos.toByteArray();
  }

  public static Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
    DequeueFilter filter = new DequeueFilter();
    try {
      filter.readFields(new DataInputStream(new ByteArrayInputStream(pbBytes)));
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }
    return filter;
  }
}
