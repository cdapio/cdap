/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;

/**
 * Abstract class that fetches notifications from TMS
 */
public abstract class AbstractNotificationSubscriberService extends AbstractMessagingSubscriberService<Notification> {

  private static final Gson GSON = new Gson();

  private final String name;
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;

  @Inject
  protected AbstractNotificationSubscriberService(String name, CConfiguration cConf, String topicName,
                                                  boolean transactionalFetch, int fetchSize, long emptyFetchDelayMillis,
                                                  MessagingService messagingService,
                                                  DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                                  MetricsCollectionService metricsCollectionService) {
    super(NamespaceId.SYSTEM.topic(topicName), transactionalFetch, fetchSize, emptyFetchDelayMillis,
          RetryStrategies.fromConfiguration(cConf, "system.notification."),
          metricsCollectionService.getContext(ImmutableMap.of(
            Constants.Metrics.Tag.COMPONENT, Constants.Service.MASTER_SERVICES,
            Constants.Metrics.Tag.INSTANCE_ID, "0",
            Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
            Constants.Metrics.Tag.TOPIC, topicName,
            Constants.Metrics.Tag.CONSUMER, name
          )));
    this.name = name;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, ImmutableMap.of(), null, null, messagingContext)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
  }

  @Override
  protected String getServiceName() {
    return name;
  }

  @Override
  protected MessagingContext getMessagingContext() {
    return messagingContext;
  }

  @Override
  protected Transactional getTransactional() {
    return transactional;
  }

  @Override
  protected Notification decodeMessage(Message message) throws Exception {
    return GSON.fromJson(message.getPayloadAsString(), Notification.class);
  }
}
