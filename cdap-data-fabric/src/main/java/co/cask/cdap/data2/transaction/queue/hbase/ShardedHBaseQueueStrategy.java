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
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import co.cask.cdap.data2.queue.DequeueStrategy;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueScanner;
import co.cask.cdap.hbase.wd.DistributedScanner;
import com.google.common.base.Function;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Threads;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link HBaseQueueStrategy} with sharded keys.
 *
 * The row key has structure of:
 *
 * <pre>
 * {@code
 *
 * row_key = <shard> <queue_prefix> <write_pointer> <counter>
 * shard = <salt> <consumer_group_id> <consumer_instance_id>
 * salt = First byte of MD5 of <consumer_group_id>, <consumer_instance_id> and <queue_name>
 * consumer_group_id = 8 bytes long value of the target consumer group or 0 if it is FIFO
 * consumer_instance_id = 4 bytes int value of target consumer instance id or -1 if FIFO
 * queue_prefix = <name_hash> <queue_name>
 * name_hash = First byte of MD5 of <queue_name>
 * queue_name = flowlet_name + "/" + output_name
 * write_pointer = 8 bytes long value of the write pointer of the transaction
 * counter = 4 bytes int value of a monotonic increasing number assigned for each entry written in the same transaction
 * }
 * </pre>
 *
 */
final class ShardedHBaseQueueStrategy implements HBaseQueueStrategy, Closeable {

  // Number of bytes as the row key prefix, including salt bytes added by the row key distributor
  static final int PREFIX_BYTES = HBaseQueueAdmin.SALT_BYTES + Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;

  private static final Function<byte[], byte[]> ROW_KEY_CONVERTER = new Function<byte[], byte[]>() {
    @Override
    public byte[] apply(byte[] input) {
      // Instead of using ROW_KEY_DISTRIBUTOR.getOriginalKey (which strip off salt bytes),
      // Do a array copying directly to reduce extra byte[] being created
      return Arrays.copyOfRange(input, PREFIX_BYTES, input.length);
    }
  };

  private final ExecutorService scansExecutor;

  ShardedHBaseQueueStrategy() {
    // Using the "direct handoff" approach, new threads will only be created
    // if it is necessary and will grow unbounded. This could be bad but in DistributedScanner
    // we only create as many Runnables as there are buckets data is distributed to. It means
    // it also scales when buckets amount changes.
    ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 20,
                                                         60, TimeUnit.SECONDS,
                                                         new SynchronousQueue<Runnable>(),
                                                         Threads.newDaemonThreadFactory("queue-consumer-scan"));
    executor.allowCoreThreadTimeOut(true);
    this.scansExecutor = executor;
  }

  @Override
  public QueueScanner createScanner(ConsumerConfig consumerConfig,
                                    HTable hTable, Scan scan, int numRows) throws IOException {
    // Modify the scan with sharded key prefix
    Scan shardedScan = new Scan(scan);
    shardedScan.setStartRow(getShardedKey(consumerConfig, consumerConfig.getInstanceId(), scan.getStartRow()));
    shardedScan.setStopRow(getShardedKey(consumerConfig, consumerConfig.getInstanceId(), scan.getStopRow()));

    ResultScanner scanner = DistributedScanner.create(hTable, shardedScan,
                                                      HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR, scansExecutor);
    return new HBaseQueueScanner(scanner, numRows, ROW_KEY_CONVERTER);
  }

  @Override
  public byte[] getActualRowKey(ConsumerConfig consumerConfig, byte[] originalRowKey) {
    return HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getDistributedKey(getShardedKey(consumerConfig,
                                                                               consumerConfig.getInstanceId(),
                                                                               originalRowKey));
  }

  @Override
  public void getRowKeys(Iterable<ConsumerGroupConfig> consumerGroupConfigs, QueueEntry queueEntry, byte[] rowKeyPrefix,
                         long writePointer, int counter, Collection<byte[]> rowKeys) {

    byte[] rowKey = new byte[rowKeyPrefix.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT];
    Bytes.putBytes(rowKey, 0, rowKeyPrefix, 0, rowKeyPrefix.length);
    Bytes.putLong(rowKey, rowKeyPrefix.length, writePointer);
    Bytes.putInt(rowKey, rowKey.length - Bytes.SIZEOF_INT, counter);

    // Generates all row keys, one per consumer group.
    for (ConsumerGroupConfig config : consumerGroupConfigs) {
      DequeueStrategy dequeueStrategy = config.getDequeueStrategy();

      // Default for FIFO
      int instanceId = -1;

      if (dequeueStrategy != DequeueStrategy.FIFO) {
        if (dequeueStrategy == DequeueStrategy.ROUND_ROBIN) {
          instanceId = QueueEntryRow.getRoundRobinConsumerInstance(writePointer, counter, config.getGroupSize());
        } else if (dequeueStrategy == DequeueStrategy.HASH) {
          instanceId = QueueEntryRow.getHashConsumerInstance(queueEntry.getHashKeys(),
                                                             config.getHashKey(), config.getGroupSize());
        } else {
          throw new IllegalArgumentException("Unsupported consumer strategy: " + dequeueStrategy);
        }
      }
      rowKeys.add(HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getDistributedKey(getShardedKey(config, instanceId, rowKey)));
    }
  }

  @Override
  public void close() throws IOException {
    scansExecutor.shutdownNow();
  }

  private byte[] getShardedKey(ConsumerGroupConfig groupConfig, int instanceId, byte[] originalRowKey) {
    // Need to subtract the SALT_BYTES as the row key distributor will prefix the key with salted bytes
    byte[] result = new byte[PREFIX_BYTES - HBaseQueueAdmin.SALT_BYTES + originalRowKey.length];
    Bytes.putBytes(result, PREFIX_BYTES - HBaseQueueAdmin.SALT_BYTES,
                   originalRowKey, 0, originalRowKey.length);

    // Default for FIFO case.
    int shardId = groupConfig.getDequeueStrategy() == DequeueStrategy.FIFO ? -1 : instanceId;
    Bytes.putLong(result, 0, groupConfig.getGroupId());
    Bytes.putInt(result, Bytes.SIZEOF_LONG, shardId);

    return result;
  }
}
