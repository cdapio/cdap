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
class InputDatum {

  private final QueueConsumer consumer;
  private final URI queueName;
  private final DequeueResult dequeueResult;

  InputDatum(QueueConsumer consumer, URI queueName, DequeueResult dequeueResult) {
    this.consumer = consumer;
    this.queueName = queueName;
    this.dequeueResult = dequeueResult;
  }

  QueueAck asAck() {
    return new QueueAck(queueName.toASCIIString().getBytes(Charsets.US_ASCII),
                        dequeueResult.getEntryPointer(),
                        consumer);
  }

  ByteBuffer getData() {
    return ByteBuffer.wrap(dequeueResult.getValue());
  }
}
