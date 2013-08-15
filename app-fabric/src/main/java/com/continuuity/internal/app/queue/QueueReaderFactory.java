package com.continuuity.internal.app.queue;

import com.continuuity.app.program.Program;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.internal.app.annotations.TxDs2;
import com.google.common.base.Supplier;

/**
 *
 */
public interface QueueReaderFactory {

  QueueReader create(Program program, QueueName queueName,
                     Supplier<QueueConsumer> queueConsumerSupplier, int numGroups);

  @TxDs2
  QueueReader create(Supplier<Queue2Consumer> consumerSupplier, int batchSize);
}
