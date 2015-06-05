/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.internal.app.queue;

import co.cask.cdap.app.queue.InputDatum;
import co.cask.cdap.app.queue.QueueReader;
import co.cask.cdap.data2.queue.QueueConsumer;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * A {@link QueueReader} for reading from {@link QueueConsumer}.
 *
 * @param <T> Type of input dequeued from this reader.
 */

public final class SingleQueue2Reader<T> extends TimeTrackingQueueReader<T> {

  private final Supplier<QueueConsumer> consumerSupplier;
  private final int batchSize;
  private final Function<byte[], T> decoder;

  SingleQueue2Reader(Supplier<QueueConsumer> consumerSupplier, int batchSize, final Function<ByteBuffer, T> decoder) {
    this.consumerSupplier = consumerSupplier;
    this.batchSize = batchSize;
    this.decoder = new Function<byte[], T>() {
      @Override
      public T apply(byte[] input) {
        return decoder.apply(ByteBuffer.wrap(input));
      }
    };
  }

  @Override
  public InputDatum<T> tryDequeue(long timeout, TimeUnit timeoutUnit) throws IOException {
    QueueConsumer consumer = consumerSupplier.get();
    return new BasicInputDatum<>(consumer.getQueueName(), consumer.dequeue(batchSize), decoder);
  }
}
