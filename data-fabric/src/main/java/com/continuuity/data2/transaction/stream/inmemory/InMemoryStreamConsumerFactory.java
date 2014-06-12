/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.inmemory;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueService;
import com.continuuity.data2.transaction.stream.QueueToStreamConsumer;
import com.continuuity.data2.transaction.stream.StreamConsumer;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
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

    Queue2Consumer consumer = queueClientFactory.createConsumer(streamName, consumerConfig, -1);
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
