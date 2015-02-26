/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.data2.queue;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;

import java.io.IOException;

/**
 * Factory for creating {@link QueueProducer} and {@link QueueConsumer} for different queue.
 */
public interface QueueClientFactory {

  QueueProducer createProducer(QueueName queueName,
                               Iterable<? extends ConsumerGroupConfig> consumerGroupConfigs) throws IOException;

  QueueProducer createProducer(QueueName queueName, Iterable<? extends ConsumerGroupConfig> consumerGroupConfigs,
                               QueueMetrics queueMetrics) throws IOException;

  QueueConsumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig, int numGroups) throws IOException;
}
