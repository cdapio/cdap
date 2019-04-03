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

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.tephra.TxConstants;

/**
 * Abstract class that fetches notifications from TMS.
 * No transactions should be started in any of the overrided methods since they are already wrapped in a transaction.
 */
public abstract class AbstractNotificationSubscriberService extends AbstractMessagingSubscriberService<Notification> {

  private static final Gson GSON = new Gson();

  private final String name;
  private final MultiThreadMessagingContext messagingContext;
  private final TransactionRunner transactionRunner;

  protected AbstractNotificationSubscriberService(String name, CConfiguration cConf, String topicName,
                                                  int fetchSize, long emptyFetchDelayMillis,
                                                  MessagingService messagingService,
                                                  MetricsCollectionService metricsCollectionService,
                                                  TransactionRunner transactionRunner) {
    super(NamespaceId.SYSTEM.topic(topicName), fetchSize, cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT),
          emptyFetchDelayMillis,
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
    this.transactionRunner = transactionRunner;
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
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @Override
  protected Notification decodeMessage(Message message) throws Exception {
    return GSON.fromJson(message.getPayloadAsString(), Notification.class);
  }
}
