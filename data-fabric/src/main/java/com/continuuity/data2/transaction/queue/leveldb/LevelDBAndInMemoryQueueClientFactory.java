/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.queue.QueueConsumer;
import com.continuuity.data2.queue.QueueProducer;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueClientFactory;
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
  public QueueProducer createProducer(QueueName queueName) throws IOException {
    return delegate(queueName).createProducer(queueName);
  }

  @Override
  public QueueConsumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig, int numGroups)
    throws IOException {
    return delegate(queueName).createConsumer(queueName, consumerConfig, numGroups);
  }

  @Override
  public QueueProducer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    return delegate(queueName).createProducer(queueName, queueMetrics);
  }

  private QueueClientFactory delegate(QueueName queueName) {
    return queueName.isStream() ? levelDBFactory : inMemoryFactory;
  }
}

