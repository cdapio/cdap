/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A helper class for managing
 */
@NotThreadSafe
final class ProducerSupplier implements Supplier<QueueProducer>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ProducerSupplier.class);

  private final QueueName queueName;
  private final QueueClientFactory queueClientFactory;
  private final QueueMetrics queueMetrics;
  private QueueProducer producer;

  ProducerSupplier(QueueName queueName, QueueClientFactory queueClientFactory, QueueMetrics queueMetrics) {
    this.queueName = queueName;
    this.queueClientFactory = queueClientFactory;
    this.queueMetrics = queueMetrics;
    open();
  }

  void open() {
    try {
      close();
      producer = queueClientFactory.createProducer(queueName, queueMetrics);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public QueueProducer get() {
    return producer;
  }

  @Override
  public void close() throws IOException {
    try {
      if (producer != null) {
        producer.close();
      }
    } catch (IOException e) {
      LOG.error("Failed to close producer for queue {}", queueName, e);
    }
    producer = null;
  }
}
