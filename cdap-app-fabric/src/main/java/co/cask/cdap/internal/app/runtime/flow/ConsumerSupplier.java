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
package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumer;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A helper class for managing queue consumer instances.
 *
 * @param <T> Type of consumer to supply.
 */
@NotThreadSafe
final class ConsumerSupplier<T> implements Supplier<T>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerSupplier.class);

  private final DataFabricFacade dataFabricFacade;
  private final QueueName queueName;
  private final int numGroups;
  private ConsumerConfig consumerConfig;
  private Closeable consumer;

  static <T> ConsumerSupplier<T> create(DataFabricFacade dataFabricFacade,
                                        QueueName queueName, ConsumerConfig consumerConfig) {
    return create(dataFabricFacade, queueName, consumerConfig, -1);
  }

  static <T> ConsumerSupplier<T> create(DataFabricFacade dataFabricFacade, QueueName queueName,
                                        ConsumerConfig consumerConfig, int numGroups) {
    return new ConsumerSupplier<T>(dataFabricFacade, queueName, consumerConfig, numGroups);
  }

  private ConsumerSupplier(DataFabricFacade dataFabricFacade, QueueName queueName,
                           ConsumerConfig consumerConfig, int numGroups) {
    this.dataFabricFacade = dataFabricFacade;
    this.queueName = queueName;
    this.numGroups = numGroups;
    this.consumerConfig = consumerConfig;
    open(consumerConfig.getGroupSize());
  }

  /**
   * Updates number of instances for the consumer group that this instance belongs to. It'll close existing
   * consumer and create a new one with the new group size.
   *
   * @param groupSize New group size.
   */
  void open(int groupSize) {
    try {
      close();
      ConsumerConfig config = consumerConfig;
      if (groupSize != config.getGroupSize()) {
        config = new ConsumerConfig(consumerConfig.getGroupId(),
                                    consumerConfig.getInstanceId(),
                                    groupSize,
                                    consumerConfig.getDequeueStrategy(),
                                    consumerConfig.getHashKey());
      }
      if (queueName.isQueue()) {
        QueueConsumer queueConsumer = dataFabricFacade.createConsumer(queueName, config, numGroups);
        consumerConfig = queueConsumer.getConfig();
        consumer = queueConsumer;
      } else {
        StreamConsumer streamConsumer = dataFabricFacade.createStreamConsumer(queueName.toStreamId(), config);
        consumerConfig = streamConsumer.getConsumerConfig();
        consumer = streamConsumer;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  QueueName getQueueName() {
    return queueName;
  }

  /**
   * Close the current consumer if there is one.
   */
  @Override
  public void close() throws IOException {
    try {
      if (consumer != null) {
        consumer.close();
      }
    } catch (Exception e) {
      LOG.warn("Fail to close queue consumer.", e);
    }
    consumer = null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T get() {
    return (T) consumer;
  }
}
