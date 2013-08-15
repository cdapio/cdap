/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Collection;

/**
*
*/
final class SimpleDequeueResult implements DequeueResult {

  private static final Function<HBaseQueueEntry, byte[]> ENTRY_TO_BYTE_ARRAY = new Function<HBaseQueueEntry, byte[]>() {
    @Override
    public byte[] apply(HBaseQueueEntry input) {
      return input.getData();
    }
  };
  private final int size;
  private final Iterable<byte[]> data;
  private final QueueName queueName;
  private final ConsumerConfig config;

  SimpleDequeueResult(Collection<HBaseQueueEntry> entries, QueueName queueName, ConsumerConfig config) {
    this.queueName = queueName;
    this.config = config;
    this.size = entries.size();

    if (entries.isEmpty()) {
      data = ImmutableList.of();
    } else if (entries.size() == 1) {
      data = ImmutableList.of(ENTRY_TO_BYTE_ARRAY.apply(entries.iterator().next()));
    } else {
      data = Iterables.transform(entries, ENTRY_TO_BYTE_ARRAY);
    }
  }

  @Override
  public QueueName getQueueName() {
    return queueName;
  }

  @Override
  public ConsumerConfig getConsumerConfig() {
    return config;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public Iterable<byte[]> getData() {
    return data;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("size", size)
      .add("queue", queueName)
      .add("config", config)
      .toString();
  }
}
