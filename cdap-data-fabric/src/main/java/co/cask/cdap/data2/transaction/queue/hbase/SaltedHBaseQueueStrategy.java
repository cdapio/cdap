/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.transaction.queue.QueueScanner;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.DistributedScanner;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import com.google.common.base.Function;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A {@link HBaseQueueStrategy} that scans HBase by using the {@link DistributedScanner}.
 * The row key has the following format:
 *
 * <pre>
 * {@code
 *
 * row_key = <salt_prefix> <row_key_base>
 * salt_prefix = 1 byte hash of <row_key_base>
 * row_key_base = <queue_prefix> <write_point> <counter>
 * queue_prefix = <name_hash> <queue_name>
 * name_hash = First byte of MD5 of <queue_name>
 * queue_name = flowlet_name + "/" + output_name
 * write_pointer = 8 bytes long value of the write pointer of the transaction
 * counter = 4 bytes int value of a monotonic increasing number assigned for each entry written in the same transaction
 * }
 * </pre>
 */
public final class SaltedHBaseQueueStrategy implements HBaseQueueStrategy {

  public static final int SALT_BYTES = 1;

  private final HBaseTableUtil tableUtil;
  private final int distributorBuckets;
  private final AbstractRowKeyDistributor rowKeyDistributor;
  private final Function<byte[], byte[]> rowKeyConverter;
  private final ExecutorService scansExecutor;

  /**
   * Constructs a new instance with the given number of buckets for distributed scan.
   */
  SaltedHBaseQueueStrategy(HBaseTableUtil tableUtil, int distributorBuckets) {
    this.tableUtil = tableUtil;
    this.distributorBuckets = distributorBuckets;
    this.rowKeyDistributor = new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(distributorBuckets));
    this.rowKeyConverter = new Function<byte[], byte[]>() {
      @Override
      public byte[] apply(byte[] input) {
        return rowKeyDistributor.getOriginalKey(input);
      }
    };

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
    // we should roughly divide by number of buckets, but don't want another RPC for the case we are not exactly right
    ScanBuilder distributedScan = tableUtil.buildScan(scan);
    int caching = (int) (1.1 * numRows / distributorBuckets);
    distributedScan.setCaching(caching);

    ResultScanner scanner = DistributedScanner.create(hTable, distributedScan.build(),
                                                      rowKeyDistributor, scansExecutor);
    return new HBaseQueueScanner(scanner, numRows, rowKeyConverter);
  }

  @Override
  public byte[] getActualRowKey(ConsumerConfig consumerConfig, byte[] originalRowKey) {
    return rowKeyDistributor.getDistributedKey(originalRowKey);
  }

  @Override
  public void getRowKeys(Iterable<ConsumerGroupConfig> consumerGroupConfigs, QueueEntry queueEntry, byte[] rowKeyPrefix,
                         long writePointer, int counter, Collection<byte[]> rowKeys) {
    byte[] rowKey = new byte[rowKeyPrefix.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT];
    Bytes.putBytes(rowKey, 0, rowKeyPrefix, 0, rowKeyPrefix.length);
    Bytes.putLong(rowKey, rowKeyPrefix.length, writePointer);
    Bytes.putInt(rowKey, rowKey.length - Bytes.SIZEOF_INT, counter);

    rowKeys.add(rowKeyDistributor.getDistributedKey(rowKey));
  }

  @Override
  public void close() throws IOException {
    scansExecutor.shutdownNow();
  }
}
