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
package co.cask.cdap.data2.transaction.queue.leveldb;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import co.cask.cdap.data2.transaction.queue.inmemory.InMemoryQueueClientFactory;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * Factory for LevelDB queue clients.
 */
public final class LevelDBAndInMemoryQueueClientFactory implements QueueClientFactory {

  private final InMemoryQueueClientFactory inMemoryFactory;
  private final LevelDBQueueClientFactory levelDBFactory;

  @Inject
  public LevelDBAndInMemoryQueueClientFactory(InMemoryQueueClientFactory inMemoryFactory,
                                              LevelDBQueueClientFactory levelDBFactory) {
    this.inMemoryFactory = inMemoryFactory;
    this.levelDBFactory = levelDBFactory;
  }

  @Override
  public QueueProducer createProducer(QueueName queueName,
                                      Iterable<? extends ConsumerGroupConfig> consumerGroupConfigs) throws IOException {
    return delegate(queueName).createProducer(queueName, consumerGroupConfigs);
  }

  @Override
  public QueueProducer createProducer(QueueName queueName, Iterable<? extends ConsumerGroupConfig> consumerGroupConfigs,
                                      QueueMetrics queueMetrics) throws IOException {
    return delegate(queueName).createProducer(queueName, consumerGroupConfigs, queueMetrics);
  }

  @Override
  public QueueConsumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig, int numGroups)
    throws IOException {
    return delegate(queueName).createConsumer(queueName, consumerConfig, numGroups);
  }

  private QueueClientFactory delegate(QueueName queueName) {
    return queueName.isStream() ? levelDBFactory : inMemoryFactory;
  }
}

