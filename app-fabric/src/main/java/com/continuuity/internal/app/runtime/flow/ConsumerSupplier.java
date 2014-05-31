/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.stream.StreamConsumer;
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
 *
 * @param <T> Type of consumer to supply.
 */
@NotThreadSafe
final class ConsumerSupplier<T> implements Supplier<T>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerSupplier.class);

  private final DataFabricFacade dataFabricFacade;
  private final QueueName queueName;
  private final int numGroups;
  private ConsumerConfig consumerConfig;
  private Object consumer;

  static <T> ConsumerSupplier<T> create(DataFabricFacade dataFabricFacade,
                                        QueueName queueName, ConsumerConfig consumerConfig) {
    return create(dataFabricFacade, queueName, consumerConfig, -1);
  }

  static <T> ConsumerSupplier<T> create(DataFabricFacade dataFabricFacade, QueueName queueName,
                                        ConsumerConfig consumerConfig, int numGroups) {
    return new ConsumerSupplier<T>(dataFabricFacade, queueName, consumerConfig, numGroups);
  }

  private ConsumerSupplier(DataFabricFacade dataFabricFacade, QueueName queueName,
                           ConsumerConfig consumerConfig, int numGroups) {
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
      if (queueName.isQueue()) {
        Queue2Consumer queueConsumer = dataFabricFacade.createConsumer(queueName, config, numGroups);
        consumerConfig = queueConsumer.getConfig();
        consumer = queueConsumer;
      } else {
        StreamConsumer streamConsumer = dataFabricFacade.createStreamConsumer(queueName, config);
        consumerConfig = streamConsumer.getConsumerConfig();
        consumer = streamConsumer;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  QueueName getQueueName() {
    return queueName;
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

  @SuppressWarnings("unchecked")
  @Override
  public T get() {
    return (T) consumer;
  }
}
