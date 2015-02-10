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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.app.queue.InputDatum;
import co.cask.cdap.app.queue.QueueReader;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.stream.StreamConsumer;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A {@link QueueReader} implementation that dequeue from stream
 *
 * @param <T> Target type.
 */
public final class StreamQueueReader<T> implements QueueReader<T> {

  private final Supplier<StreamConsumer> consumerSupplier;
  private final int batchSize;
  private final Function<StreamEvent, T> eventTransform;

  StreamQueueReader(Supplier<StreamConsumer> consumerSupplier, int batchSize,
                    Function<StreamEvent, T> eventTransform) {
    this.consumerSupplier = consumerSupplier;
    this.batchSize = batchSize;
    this.eventTransform = eventTransform;
  }

  @Override
  public InputDatum<T> dequeue(long timeout, TimeUnit timeoutUnit) throws IOException, InterruptedException {
    StreamConsumer consumer = consumerSupplier.get();
    return new BasicInputDatum<StreamEvent, T>(QueueName.fromStream(consumer.getStreamId()),
                                               consumer.poll(batchSize, timeout, timeoutUnit), eventTransform);
  }
}
