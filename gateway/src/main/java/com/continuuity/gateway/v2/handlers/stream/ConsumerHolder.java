package com.continuuity.gateway.v2.handlers.stream;

import com.continuuity.api.data.OperationException;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.v2.txmanager.TxManager;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Handles dequeue of stream making sure that access to consumer is serialized.
 */
final class ConsumerHolder implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumerHolder.class);

  private final Queue2Consumer consumer;
  private final TxManager txManager;

  public ConsumerHolder(ConsumerKey key, TransactionSystemClient txClient,
                        QueueClientFactory queueClientFactory) throws Exception {
    // 0th instance of group 'groupId' of size 1
    this.consumer =
      queueClientFactory.createConsumer(key.getQueueName(),
                                        new ConsumerConfig(key.getGroupId(), 0, 1, DequeueStrategy.FIFO, null), 1);
    this.txManager = new TxManager(txClient, (TransactionAware) consumer);
  }

  public synchronized DequeueResult dequeue() throws Throwable {
    try {
      txManager.start();

      try {
        DequeueResult result = consumer.dequeue();
        txManager.commit();
        return result;
      } catch (Throwable e) {
        LOG.error("Exception while dequeuing stream using consumer {}", consumer, e);
        txManager.abort();
        throw e;
      }
    } catch (OperationException e) {
      LOG.error("Got exception", e);
      throw e;
    }
  }

  @Override
  public synchronized void close() throws IOException {
  if (consumer instanceof Closeable) {
      ((Closeable) consumer).close();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("consumer", consumer)
      .add("txManager", txManager)
      .toString();
  }
}
