package com.continuuity.internal.app.runtime;

import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.Map;

/**
*
*/
final class EmittedDatum {

  private final QueueProducer queueProducer;
  private final URI queueName;
  private final byte[] data;
  private final Map<String, String> header;

  EmittedDatum(QueueProducer queueProducer, URI queueName, byte[] data, Map<String, Object> partitions) {
    this.queueProducer = queueProducer;
    this.queueName = queueName;
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, Object> entry : partitions.entrySet()) {
      builder.put(entry.getKey(), String.valueOf(entry.getValue().hashCode()));
    }
    this.data = data;
    this.header = builder.build();
  }

  QueueEnqueue asEnqueue() {
    return new QueueEnqueue(queueProducer,
                            queueName.toASCIIString().getBytes(Charsets.US_ASCII),
                            header, data);
  }
}
