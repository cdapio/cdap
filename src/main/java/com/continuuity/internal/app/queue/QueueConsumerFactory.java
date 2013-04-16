package com.continuuity.internal.app.queue;

import com.continuuity.data.operation.ttqueue.QueueConsumer;

/**
 *  A factory interface to create QueueConsumer
 */
public interface QueueConsumerFactory {

  /**
   * Creates a QueueConsumer with the given groupSize, and runs a QueueConfigure with the new QueueConsumer.
   * @param groupSize Size of the group of which the created QueueConsumer will be part of
   * @return Created QueueConsumer
   */
  QueueConsumer create(int groupSize);
}
