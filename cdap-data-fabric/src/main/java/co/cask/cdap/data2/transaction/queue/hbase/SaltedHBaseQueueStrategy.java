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
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.transaction.queue.QueueScanner;
import co.cask.cdap.hbase.wd.DistributedScanner;
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
 */
final class SaltedHBaseQueueStrategy implements HBaseQueueStrategy {

  private static final Function<byte[], byte[]> ROW_KEY_CONVERTER = new Function<byte[], byte[]>() {
    @Override
    public byte[] apply(byte[] input) {
      return HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getOriginalKey(input);
    }
  };

  private final ExecutorService scansExecutor;

  SaltedHBaseQueueStrategy() {
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
    ResultScanner scanner = DistributedScanner.create(hTable, scan, HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR, scansExecutor);
    return new HBaseQueueScanner(scanner, numRows, ROW_KEY_CONVERTER);
  }

  @Override
  public byte[] getActualRowKey(ConsumerConfig consumerConfig, byte[] originalRowKey) {
    return HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getDistributedKey(originalRowKey);
  }

  @Override
  public void getRowKeys(QueueEntry queueEntry, byte[] rowKeyPrefix,
                         long writePointer, int counter, Collection<byte[]> rowKeys) {
    byte[] rowKey = new byte[rowKeyPrefix.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT];
    Bytes.putBytes(rowKey, 0, rowKeyPrefix, 0, rowKeyPrefix.length);
    Bytes.putLong(rowKey, rowKeyPrefix.length, writePointer);
    Bytes.putInt(rowKey, rowKey.length - Bytes.SIZEOF_INT, counter);

    rowKeys.add(HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getDistributedKey(rowKey));
  }

  @Override
  public void close() throws IOException {
    scansExecutor.shutdownNow();
  }
}
