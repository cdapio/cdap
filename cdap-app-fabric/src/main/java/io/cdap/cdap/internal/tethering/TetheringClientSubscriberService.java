/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * Service responsible for consuming messages from TMS from tethered instances to start/stop new program runs.
 */
public class TetheringClientSubscriberService extends AbstractMessagingSubscriberService<TetheringControlMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(TetheringClientSubscriberService.class);
  private static final Gson GSON = new Gson();
  private static final String CONSUMER_NAME = "tethering.client";

  private final MultiThreadMessagingContext messagingContext;
  private final TransactionRunner transactionRunner;

  @Inject
  TetheringClientSubscriberService(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                                   MessagingService messagingService, TransactionRunner transactionRunner) {
    super(
      NamespaceId.SYSTEM.topic(cConf.get(Constants.Tethering.TETHERING_TOPIC)),
      cConf.getInt(Constants.Metadata.MESSAGING_FETCH_SIZE),
      cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT),
      cConf.getLong(Constants.Metadata.MESSAGING_POLL_DELAY_MILLIS),
      RetryStrategies.fromConfiguration(cConf, "system.runtime.monitor."),
      metricsCollectionService.getContext(ImmutableMap.of(
        Constants.Metrics.Tag.COMPONENT, Constants.Service.REMOTE_AGENT_SERVICE,
        Constants.Metrics.Tag.INSTANCE_ID, "0",
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
        Constants.Metrics.Tag.TOPIC, cConf.get(Constants.Tethering.TETHERING_TOPIC),
        Constants.Metrics.Tag.CONSUMER, CONSUMER_NAME
      )));
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactionRunner = transactionRunner;
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
  protected TetheringControlMessage decodeMessage(Message message) {
    return message.decodePayload(r -> GSON.fromJson(r, TetheringControlMessage.class));
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    return appMetadataStore.retrieveSubscriberState(getTopicId().getTopic(), CONSUMER_NAME);
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    appMetadataStore.persistSubscriberState(getTopicId().getTopic(), CONSUMER_NAME, messageId);
  }

  @Override
  protected void processMessages(StructuredTableContext structuredTableContext, Iterator<ImmutablePair<String,
    TetheringControlMessage>> messages) throws Exception {
    // TODO: CDAP-18788 - based on TetheringControlMessage.Type, start (if it's not already running) or stop program
  }
}
