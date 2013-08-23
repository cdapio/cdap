/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;

/**
 * A helper class for managing queue consumer instances.
 */
@NotThreadSafe
final class QueueConsumerSupplier implements Supplier<Queue2Consumer> {

  private static final Logger LOG = LoggerFactory.getLogger(QueueConsumerSupplier.class);

  private final QueueClientFactory clientFactory;
  private final QueueName queueName;
  private final int numGroups;
  private ConsumerConfig consumerConfig;
  private Queue2Consumer consumer;

  QueueConsumerSupplier(QueueClientFactory clientFactory, QueueName queueName,
                        ConsumerConfig consumerConfig, int numGroups) {
    this.clientFactory = clientFactory;
    this.queueName = queueName;
    this.consumerConfig = consumerConfig;
    this.numGroups = numGroups;
    this.consumer = createConsumer(null);
  }

  void updateInstanceCount(int groupSize) {
    consumerConfig = new ConsumerConfig(consumerConfig.getGroupId(),
                                        consumerConfig.getInstanceId(),
                                        groupSize,
                                        consumerConfig.getDequeueStrategy(),
                                        consumerConfig.getHashKey());
    consumer = createConsumer(consumer);
  }

  @Override
  public Queue2Consumer get() {
    return consumer;
  }


  private Queue2Consumer createConsumer(Queue2Consumer oldConsumer) {
    try {
      if (oldConsumer != null && oldConsumer instanceof Closeable) {
        ((Closeable) oldConsumer).close();
      }
    } catch (IOException e) {
      LOG.warn("Fail to close queue consumer.", e);
    }

    try {
      return clientFactory.createConsumer(queueName, consumerConfig, numGroups);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
