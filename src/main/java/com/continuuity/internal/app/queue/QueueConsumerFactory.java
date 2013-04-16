package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;

import java.util.Map;

/**
 *
 */
public interface QueueConsumerFactory {

  QueueConsumer create(int groupSize) throws OperationException;
}
