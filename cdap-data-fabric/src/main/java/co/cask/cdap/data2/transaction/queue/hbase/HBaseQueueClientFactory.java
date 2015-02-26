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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.data2.util.hbase.TableId;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Factory for creating HBase queue producer and consumer instances.
 */
public final class HBaseQueueClientFactory implements QueueClientFactory {

  // 4M write buffer for HTable
  private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final HBaseQueueAdmin queueAdmin;
  private final HBaseStreamAdmin streamAdmin;
  private final HBaseQueueUtil queueUtil;
  private final HBaseTableUtil hBaseTableUtil;

  @Inject
  public HBaseQueueClientFactory(CConfiguration cConf, Configuration hConf,
                                 QueueAdmin queueAdmin, HBaseStreamAdmin streamAdmin) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.queueAdmin = (HBaseQueueAdmin) queueAdmin;
    this.streamAdmin = streamAdmin;
    this.queueUtil = new HBaseQueueUtilFactory().get();
    this.hBaseTableUtil = new HBaseTableUtilFactory().get();
  }

  // for testing only
  String getTableName(QueueName queueName) {
    return (queueName.isStream() ? streamAdmin : queueAdmin).getActualTableName(queueName);
  }

  // for testing only
  String getConfigTableName(QueueName queueName) {
    return (queueName.isStream() ? streamAdmin : queueAdmin).getConfigTableName(queueName);
  }

  @Override
  public QueueConsumer createConsumer(QueueName queueName,
                                      ConsumerConfig consumerConfig, int numGroups) throws IOException {
    HBaseQueueAdmin admin = ensureTableExists(queueName);
    HTable configTable = createHTable(admin.getConfigTableName(queueName));
    HBaseConsumerStateStore stateStore = new HBaseConsumerStateStore(queueName, consumerConfig, configTable);
    HBaseConsumerState consumerState = stateStore.getState();
    return queueUtil.getQueueConsumer(cConf, consumerConfig, createHTable(admin.getActualTableName(queueName)),
                                      queueName, consumerState, stateStore, getQueueStrategy());
  }

  @Override
  public QueueProducer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public QueueProducer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    HBaseQueueAdmin admin = ensureTableExists(queueName);
    return new HBaseQueueProducer(createHTable(admin.getActualTableName(queueName)), queueName, queueMetrics,
                                  getQueueStrategy());
  }

  protected HBaseQueueStrategy getQueueStrategy() {
    return new SaltedHBaseQueueStrategy();
  }

  /**
   * Helper method to select the queue or stream admin, and to ensure it's table exists.
   * @param queueName name of the queue to be opened.
   * @return the queue admin for that queue.
   * @throws IOException
   */
  private HBaseQueueAdmin ensureTableExists(QueueName queueName) throws IOException {
    HBaseQueueAdmin admin = queueName.isStream() ? streamAdmin : queueAdmin;
    try {
      if (!admin.exists(queueName)) {
        admin.create(queueName);
      }
    } catch (Exception e) {
      throw new IOException("Failed to open table " + admin.getActualTableName(queueName), e);
    }
    return admin;
  }

  private HTable createHTable(String name) throws IOException {
    HTable consumerTable = hBaseTableUtil.getHTable(hConf, TableId.from(name));
    // TODO: make configurable
    consumerTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    consumerTable.setAutoFlush(false);
    return consumerTable;
  }
}
