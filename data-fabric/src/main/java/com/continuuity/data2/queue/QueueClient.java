/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import com.continuuity.data.operation.ttqueue.QueueEntry;

import java.io.IOException;

/**
 *
 */
public interface QueueClient {

  void enqueue(QueueEntry entry) throws IOException;

  void enqueue(Iterable<QueueEntry> entries) throws IOException;

  QueueConsumer createConsumer(ConsumerConfig consumerConfig) throws IOException;
}
