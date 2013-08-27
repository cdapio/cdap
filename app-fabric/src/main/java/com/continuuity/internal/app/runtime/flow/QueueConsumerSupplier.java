/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.internal.app.runtime.DataFabricFacade;
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

  private final DataFabricFacade dataFabricFacade;
  private final QueueName queueName;
  private final int numGroups;
  private ConsumerConfig consumerConfig;
  private Queue2Consumer consumer;

  QueueConsumerSupplier(DataFabricFacade dataFabricFacade,
                        QueueName queueName, ConsumerConfig consumerConfig, int numGroups) {
    this.dataFabricFacade = dataFabricFacade;
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
        // Call close in a new transaction.
        // TODO (terence): Actually need to coordinates with other flowlets to drain the queue.
        TransactionAgent txAgent = dataFabricFacade.createTransactionAgent();
        txAgent.start();
        try {
          ((Closeable) oldConsumer).close();
          txAgent.finish();
        } catch (OperationException e) {
          LOG.warn("Fail to commit transaction when closing consumer.");
          txAgent.abort();
        }
      }
    } catch (Exception e) {
      LOG.warn("Fail to close queue consumer.", e);
    }

    try {
      return dataFabricFacade.createConsumer(queueName, consumerConfig, numGroups);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
