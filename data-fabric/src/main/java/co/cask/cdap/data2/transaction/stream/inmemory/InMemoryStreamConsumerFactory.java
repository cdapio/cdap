/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.data2.transaction.stream.inmemory;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.transaction.queue.inmemory.InMemoryQueueService;
import co.cask.cdap.data2.transaction.stream.QueueToStreamConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * In memory implementation of StreamConsumer would be using the in memory queue implementation.
 */
public final class InMemoryStreamConsumerFactory implements StreamConsumerFactory {

  private final QueueClientFactory queueClientFactory;
  private final InMemoryQueueService queueService;

  @Inject
  public InMemoryStreamConsumerFactory(QueueClientFactory queueClientFactory, InMemoryQueueService queueService) {
    this.queueClientFactory = queueClientFactory;
    this.queueService = queueService;
  }

  @Override
  public StreamConsumer create(QueueName streamName, String namespace,
                               ConsumerConfig consumerConfig) throws IOException {

    QueueConsumer consumer = queueClientFactory.createConsumer(streamName, consumerConfig, -1);
    return new QueueToStreamConsumer(streamName, consumerConfig, consumer);
  }

  @Override
  public void dropAll(QueueName streamName, String namespace, Iterable<Long> groupIds) throws IOException {
    // A bit hacky to assume namespace is formed by appId.flowId. See AbstractDataFabricFacade
    // String namespace = String.format("%s.%s", programId.getApplicationId(), programId.getId());

    int idx = namespace.indexOf('.');
    String appId = namespace.substring(0, idx);
    String flowId = namespace.substring(idx + 1);

    queueService.truncateAllWithPrefix(QueueName.prefixForFlow(appId, flowId));
  }
}
