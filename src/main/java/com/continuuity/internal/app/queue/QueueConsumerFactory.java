package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueAdmin;

import java.util.Map;

/**
 *  A factory interface to create QueueConsumer
 */
public interface QueueConsumerFactory {

  /**
   * Creates a QueueConsumer with the given groupSize, and runs a QueueConfigure with the new QueueConsumer.
   * @param groupSize Size of the group of which the created QueueConsumer will be part of
   * @return Created QueueConsumer
   * @throws OperationException An OperationException can be thrown during the execution of QueueConfigure operation
   */
  QueueConsumer create(int groupSize) throws OperationException;
}
