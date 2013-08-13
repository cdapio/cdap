/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import com.continuuity.data.operation.ttqueue.QueueEntry;

import java.io.IOException;

/**
 * TODO: This class should be renamed as QueueProducer when the old queue is gone.
 */
public interface Queue2Producer {

  void enqueue(QueueEntry entry) throws IOException;

  void enqueue(Iterable<QueueEntry> entries) throws IOException;
}
