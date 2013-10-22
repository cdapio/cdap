/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
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
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return delegate(queueName).createProducer(queueName);
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig, int numGroups)
    throws IOException {
    return delegate(queueName).createConsumer(queueName, consumerConfig, numGroups);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    return delegate(queueName).createProducer(queueName, queueMetrics);
  }

  private QueueClientFactory delegate(QueueName queueName) {
    return queueName.isStream() ? levelDBFactory : inMemoryFactory;
  }
}

