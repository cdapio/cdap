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

package co.cask.cdap.data2.transaction.queue.inmemory;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import com.google.inject.Inject;

import java.io.IOException;

/**
 *
 */
public class InMemoryQueueClientFactory implements QueueClientFactory {

  private final InMemoryQueueService queueService;

  @Inject
  public InMemoryQueueClientFactory(InMemoryQueueService queueService) {
    this.queueService = queueService;
  }

  @Override
  public QueueProducer createProducer(QueueName queueName,
                                      Iterable<? extends ConsumerGroupConfig> consumerGroupConfigs) throws IOException {
    return createProducer(queueName, consumerGroupConfigs, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public QueueProducer createProducer(QueueName queueName, Iterable<? extends ConsumerGroupConfig> consumerGroupConfigs,
                                      QueueMetrics queueMetrics) throws IOException {
    return new InMemoryQueueProducer(queueName, queueService, queueMetrics);
  }

  @Override
  public QueueConsumer createConsumer(QueueName queueName,
                                      ConsumerConfig consumerConfig, int numGroups) throws IOException {
    return new InMemoryQueueConsumer(queueName, consumerConfig, numGroups, queueService);
  }
}
