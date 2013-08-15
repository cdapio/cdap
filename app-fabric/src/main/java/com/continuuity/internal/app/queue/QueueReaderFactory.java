package com.continuuity.internal.app.queue;

import com.continuuity.app.queue.QueueReader;
import com.continuuity.data2.queue.Queue2Consumer;
import com.google.common.base.Supplier;

/**
 *
 */
public interface QueueReaderFactory {

  QueueReader create(Supplier<Queue2Consumer> consumerSupplier, int batchSize);
}
