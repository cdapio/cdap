package com.continuuity.internal.app.queue;

import com.continuuity.app.program.Program;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.google.common.base.Supplier;

/**
 *
 */
public interface QueueReaderFactory {

  QueueReader create(Program program, QueueName queueName,
                     Supplier<QueueConsumer> queueConsumerSupplier, int numGroups);
}
