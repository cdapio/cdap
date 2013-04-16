package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueName;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConsumer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public final class QueueInputDatum implements InputDatum {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private final QueueConsumer consumer;
  private final DequeueResult dequeueResult;
  private final QueueName queueName;
  private final int numGroups;
  private final AtomicInteger retry;

  public QueueInputDatum(QueueConsumer consumer, QueueName queueName, DequeueResult dequeueResult, int numGroups) {
    this.consumer = consumer;
    this.dequeueResult = dequeueResult;
    this.queueName = queueName;
    this.numGroups = numGroups;
    this.retry = new AtomicInteger(0);
  }

  @Override
  public void submitAck(TransactionAgent txAgent) throws OperationException {
    QueueAck ack;
    if (numGroups > 0) {
      ack = new QueueAck(queueName.toBytes(), dequeueResult.getEntryPointer(), consumer, numGroups);
    } else {
      ack = new QueueAck(queueName.toBytes(), dequeueResult.getEntryPointer(), consumer);
    }
    txAgent.submit(ack);
  }

  @Override
  public boolean needProcess() {
    return !dequeueResult.isEmpty();
  }

  @Override
  public ByteBuffer getData() {
    return needProcess() ? ByteBuffer.wrap(dequeueResult.getEntry().getData()) : EMPTY_BUFFER;
  }

  @Override
  public void incrementRetry() {
    retry.incrementAndGet();
  }

  @Override
  public int getRetry() {
    return retry.get();
  }

  @Override
  public InputContext getInputContext() {
    final String name = queueName.getSimpleName();

    return new InputContext() {
      @Override
      public String getOrigin() {
        return name;
      }

      @Override
      public int getRetryCount() {
        return retry.get();
      }
    };
  }

  @Override
  public String toString() {
    if (!dequeueResult.isEmpty()) {
      return String.format("%s, %s, %d, %d",
                           queueName, dequeueResult.getEntryPointer().getEntryId(), numGroups, retry.get());
    } else {
      return String.format("%s, empty, %d, %d", queueName, numGroups, retry.get());
    }
  }
}
