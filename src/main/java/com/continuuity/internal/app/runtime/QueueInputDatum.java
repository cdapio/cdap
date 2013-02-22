package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.InputContext;
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
  private final AtomicInteger retry;

  public QueueInputDatum(QueueConsumer consumer, QueueName queueName, DequeueResult dequeueResult) {
    this.consumer = consumer;
    this.dequeueResult = dequeueResult;
    this.queueName = queueName;
    this.retry = new AtomicInteger(0);
  }

  @Override
  public void submitAck(TransactionAgent txAgent) throws OperationException {
    txAgent.submit(new QueueAck(queueName.toBytes(), dequeueResult.getEntryPointer(), consumer));
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
      public String getName() {
        return name;
      }

      @Override
      public int getRetryCount() {
        return retry.get();
      }
    };
  }
}
