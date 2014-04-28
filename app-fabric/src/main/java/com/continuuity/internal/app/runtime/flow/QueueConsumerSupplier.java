/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A helper class for managing queue consumer instances.
 */
@NotThreadSafe
final class QueueConsumerSupplier implements Supplier<Queue2Consumer>, Closeable {

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
    this.numGroups = numGroups;
    this.consumerConfig = consumerConfig;
    open(consumerConfig.getGroupSize());
  }

  /**
   * Updates number of instances for the consumer group that this instance belongs to. It'll close existing
   * consumer and create a new one with the new group size.
   *
   * @param groupSize New group size.
   */
  void open(int groupSize) {
    try {
      close();
      ConsumerConfig config = consumerConfig;
      if (groupSize != config.getGroupSize()) {
        config = new ConsumerConfig(consumerConfig.getGroupId(),
                                    consumerConfig.getInstanceId(),
                                    groupSize,
                                    consumerConfig.getDequeueStrategy(),
                                    consumerConfig.getHashKey());
      }
      consumer = dataFabricFacade.createConsumer(queueName, config, numGroups);
      consumerConfig = consumer.getConfig();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Close the current consumer if there is one.
   */
  @Override
  public void close() throws IOException {
    try {
      if (consumer != null && consumer instanceof Closeable) {
        // Call close in a new transaction.
        // TODO (terence): Actually need to coordinates with other flowlets to drain the queue.
        TransactionContext txContext = dataFabricFacade.createTransactionManager();
        txContext.start();
        try {
          ((Closeable) consumer).close();
          txContext.finish();
        } catch (TransactionFailureException e) {
          LOG.warn("Fail to commit transaction when closing consumer.");
          txContext.abort();
        }
      }
    } catch (Exception e) {
      LOG.warn("Fail to close queue consumer.", e);
    }
    consumer = null;
  }

  @Override
  public Queue2Consumer get() {
    return consumer;
  }
}
