package com.continuuity.internal.app.runtime;

import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.google.common.base.Charsets;

import java.net.URI;
import java.nio.ByteBuffer;

/**
 *
 */
public class InputDatum {

  private final QueueConsumer consumer;
  private final DequeueResult dequeueResult;

  public InputDatum(QueueConsumer consumer, DequeueResult dequeueResult) {
    this.consumer = consumer;
    this.dequeueResult = dequeueResult;
  }

  public QueueAck asAck() {
    return new QueueAck(dequeueResult.getEntryPointer().getQueueName(),
                        dequeueResult.getEntryPointer(),
                        consumer);
  }

  public ByteBuffer getData() {
    return ByteBuffer.wrap(dequeueResult.getValue());
  }
}
