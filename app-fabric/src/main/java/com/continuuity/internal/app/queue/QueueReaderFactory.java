package com.continuuity.internal.app.queue;

import com.continuuity.app.queue.QueueReader;
import com.continuuity.data2.queue.Queue2Consumer;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

import java.nio.ByteBuffer;

/**
 *
 */
public final class QueueReaderFactory {

  public <T> QueueReader<T> create(Supplier<Queue2Consumer> consumerSupplier,
                                   int batchSize, Function<ByteBuffer, T> decoder) {
    return new SingleQueue2Reader<T>(consumerSupplier, batchSize, decoder);
  }
}
