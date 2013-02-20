package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.QueueName;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConsumer;

import java.nio.ByteBuffer;

/**
 *
 */
public class InputDatum {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private final QueueConsumer consumer;
  private final DequeueResult dequeueResult;
  private final QueueName queueName;
  private int retry;

  public InputDatum(QueueConsumer consumer, QueueName queueName, DequeueResult dequeueResult) {
    this.consumer = consumer;
    this.dequeueResult = dequeueResult;
    this.queueName = queueName;
  }

  public QueueAck asAck() {
    return new QueueAck(
                         dequeueResult.getEntryPointer().getQueueName(),
                         dequeueResult.getEntryPointer(),
                         consumer
    );
  }

  public boolean isEmpty() {
    return dequeueResult.isEmpty();
  }

  public ByteBuffer getData() {
    return isEmpty() ? EMPTY_BUFFER : ByteBuffer.wrap(dequeueResult.getValue());
  }

  public void incrementRetry() {
    retry++;
  }

  public int getRetry() {
    return retry;
  }

  public InputContext getInputContext() {
    final String name = queueName.getSimpleName();
    final int retry = this.retry;

    return new InputContext() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public int getRetryCount() {
        return retry;
      }
    };
  }
}
