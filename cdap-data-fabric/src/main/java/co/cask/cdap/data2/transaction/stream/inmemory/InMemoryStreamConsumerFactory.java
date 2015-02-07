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
package co.cask.cdap.data2.transaction.stream.inmemory;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.transaction.queue.inmemory.InMemoryQueueService;
import co.cask.cdap.data2.transaction.stream.QueueToStreamConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Iterator;

/**
 * In memory implementation of StreamConsumer would be using the in memory queue implementation.
 */
public final class InMemoryStreamConsumerFactory implements StreamConsumerFactory {

  private final QueueClientFactory queueClientFactory;
  private final InMemoryQueueService queueService;

  @Inject
  public InMemoryStreamConsumerFactory(QueueClientFactory queueClientFactory, InMemoryQueueService queueService) {
    this.queueClientFactory = queueClientFactory;
    this.queueService = queueService;
  }

  @Override
  public StreamConsumer create(Id.Stream streamName, String namespace,
                               ConsumerConfig consumerConfig) throws IOException {

    QueueName queueName = QueueName.fromStream(streamName);
    QueueConsumer consumer = queueClientFactory.createConsumer(queueName, consumerConfig, -1);
    return new QueueToStreamConsumer(streamName, consumerConfig, consumer);
  }

  @Override
  public void dropAll(Id.Stream streamName, String namespace, Iterable<Long> groupIds) throws IOException {
    // A bit hacky to assume namespace is formed by namespaceId.appId.flowId. See AbstractDataFabricFacade
    // String namespace = String.format("%s.%s.%s",
    //                                  programId.getNamespaceId(),
    //                                  programId.getApplicationId(),
    //                                  programId.getId());

    String invalidNamespaceError = String.format("Namespace string %s must be of the form <namespace>.<app>.<flow>",
                                                 namespace);
    Iterator<String> namespaceParts = Splitter.on('.').split(namespace).iterator();
    Preconditions.checkArgument(namespaceParts.hasNext(), invalidNamespaceError);
    String namespaceId = namespaceParts.next();
    Preconditions.checkArgument(namespaceParts.hasNext(), invalidNamespaceError);
    String appId = namespaceParts.next();
    Preconditions.checkArgument(namespaceParts.hasNext(), invalidNamespaceError);
    String flowId = namespaceParts.next();

    queueService.truncateAllWithPrefix(QueueName.prefixForFlow(namespaceId, appId, flowId));
  }
}
