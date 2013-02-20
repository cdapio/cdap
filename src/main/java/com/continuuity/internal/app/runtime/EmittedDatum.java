/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.app.queue.QueueName;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;

/**
 *
 */
public final class EmittedDatum {

  private final QueueProducer queueProducer;
  private final QueueName queueName;
  private final byte[] data;
  private final Map<String, String> header;

  public static Function<EmittedDatum, WriteOperation> datumToWriteOp() {
    return new Function<EmittedDatum, WriteOperation>() {
      @Override
      public WriteOperation apply(EmittedDatum input) {
        return input.asEnqueue();
      }
    };
  }

  public EmittedDatum(QueueProducer queueProducer, QueueName queueName, byte[] data, Map<String, Object> partitions) {
    this.queueProducer = queueProducer;
    this.queueName = queueName;
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for(Map.Entry<String, Object> entry : partitions.entrySet()) {
      builder.put(entry.getKey(), String.valueOf(entry.getValue().hashCode()));
    }
    this.data = data;
    this.header = builder.build();
  }


  public QueueEnqueue asEnqueue() {
//        return new QueueEnqueue(queueProducer,
//                                queueName.toBytes(),
//                                header, data);
    return null;
  }
}
