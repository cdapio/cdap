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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.DequeueStrategy;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionType;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Deals with dequeue scan attributes.
 */
public class DequeueScanAttributes {
  private static final String ATTR_CONSUMER_CONFIG = "cdap.queue.dequeue.consumerConfig";
  private static final String ATTR_TX = "cdap.queue.dequeue.transaction";

  /**
   * Deprecated. It is maintained for old queue table only.
   */
  @Deprecated
  private static final String ATTR_QUEUE_ROW_PREFIX = "cdap.queue.dequeue.queueRowPrefix";

  public static void set(Scan scan, ConsumerConfig consumerConfig) {
    try {
      scan.setAttribute(ATTR_CONSUMER_CONFIG, toBytes(consumerConfig));
    } catch (IOException e) {
      // SHOULD NEVER happen
      throw new RuntimeException(e);
    }
  }

  public static void set(Scan scan, Transaction transaction) {
    try {
      scan.setAttribute(ATTR_TX, toBytes(transaction));
    } catch (IOException e) {
      // SHOULD NEVER happen
      throw new RuntimeException(e);
    }
  }

  @Nullable
  public static ConsumerConfig getConsumerConfig(Scan scan) {
    byte[] consumerConfigAttr = scan.getAttribute(ATTR_CONSUMER_CONFIG);
    try {
      return consumerConfigAttr == null ? null : bytesToConsumerConfig(consumerConfigAttr);
    } catch (IOException e) {
      // SHOULD NEVER happen
      throw new RuntimeException(e);
    }
  }

  @Nullable
  public static Transaction getTx(Scan scan) {
    byte[] txAttr = scan.getAttribute(ATTR_TX);
    try {
      return txAttr == null ? null : bytesToTx(txAttr);
    } catch (IOException e) {
      // SHOULD NEVER happen
      throw new RuntimeException(e);
    }
  }

  /**
   * TODO: Remove when {@link #ATTR_QUEUE_ROW_PREFIX} is removed.
   */
  @Deprecated
  public static void setQueueRowPrefix(Scan scan, byte[] queueNamePrefix) {
    scan.setAttribute(ATTR_QUEUE_ROW_PREFIX, queueNamePrefix);
  }

  private static byte[] toBytes(ConsumerConfig consumerConfig) throws IOException {
    ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
    write(dataOutput, consumerConfig);
    return dataOutput.toByteArray();
  }

  public static void write(DataOutput dataOutput, ConsumerConfig consumerConfig) throws IOException {
    dataOutput.writeLong(consumerConfig.getGroupId());
    dataOutput.writeInt(consumerConfig.getGroupSize());
    dataOutput.writeInt(consumerConfig.getInstanceId());
    WritableUtils.writeEnum(dataOutput, consumerConfig.getDequeueStrategy());
    WritableUtils.writeString(dataOutput, consumerConfig.getHashKey());
  }

  private static ConsumerConfig bytesToConsumerConfig(byte[] bytes) throws IOException {
    ByteArrayDataInput dataInput = ByteStreams.newDataInput(bytes);
    return readConsumerConfig(dataInput);
  }

  public static ConsumerConfig readConsumerConfig(DataInput dataInput) throws IOException {
    long groupId = dataInput.readLong();
    int groupSize = dataInput.readInt();
    int instanceId = dataInput.readInt();
    DequeueStrategy strategy = WritableUtils.readEnum(dataInput, DequeueStrategy.class);
    String hashKey = WritableUtils.readString(dataInput);

    return new ConsumerConfig(groupId, instanceId, groupSize, strategy, hashKey);
  }

  private static byte[] toBytes(Transaction tx) throws IOException {
    ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
    write(dataOutput, tx);
    return dataOutput.toByteArray();
  }

  public static void write(DataOutput dataOutput, Transaction tx) throws IOException {
    dataOutput.writeLong(tx.getReadPointer());
    dataOutput.writeLong(tx.getWritePointer());
    dataOutput.writeLong(tx.getFirstShortInProgress());
    write(dataOutput, tx.getInProgress());
    write(dataOutput, tx.getInvalids());
  }

  private static Transaction bytesToTx(byte[] bytes) throws IOException {
    ByteArrayDataInput dataInput = ByteStreams.newDataInput(bytes);
    return readTx(dataInput);
  }

  public static Transaction readTx(DataInput dataInput) throws IOException {
    long readPointer = dataInput.readLong();
    long writePointer = dataInput.readLong();
    long firstShortInProgress = dataInput.readLong();
    long[] inProgress = readLongArray(dataInput);
    long[] invalids = readLongArray(dataInput);
    return new Transaction(readPointer, writePointer, invalids, inProgress, firstShortInProgress, 
                           TransactionType.SHORT);
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
