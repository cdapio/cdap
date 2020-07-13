/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
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

import java.util.Collections;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * TMS Log subscriber for preview logs.
 */
public class PreviewTMSLogSubscriber extends AbstractMessagingSubscriberService<Iterator<byte[]>> {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewTMSLogSubscriber.class);
  private static final String CONSUMER_NAME = "preview.log.writer";
  private static final String TOPIC_NAME = "previewlog0";

  private final MultiThreadMessagingContext messagingContext;
  private final TransactionRunner transactionRunner;
  private final RemoteExecutionLogProcessor logProcessor;
  private final int maxRetriesOnError;
  private int errorCount = 0;
  private String erroredMessageId = null;

  @Inject
  PreviewTMSLogSubscriber(CConfiguration cConf,
                          @Named(PreviewConfigModule.GLOBAL_TMS) MessagingService messagingService,
                          MetricsCollectionService metricsCollectionService,
                          TransactionRunner transactionRunner, RemoteExecutionLogProcessor logProcessor) {
    super(
      NamespaceId.SYSTEM.topic(TOPIC_NAME),
      cConf.getInt(Constants.Metadata.MESSAGING_FETCH_SIZE),
      cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT),
      cConf.getLong(Constants.Metadata.MESSAGING_POLL_DELAY_MILLIS),
      RetryStrategies.fromConfiguration(cConf, "system.preview."),
      metricsCollectionService.getContext(
        ImmutableMap.of(Constants.Metrics.Tag.COMPONENT, Constants.Service.PREVIEW_HTTP,
                        Constants.Metrics.Tag.INSTANCE_ID, "0",
                        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
                        Constants.Metrics.Tag.TOPIC, TOPIC_NAME,
                        Constants.Metrics.Tag.CONSUMER, CONSUMER_NAME
      )));

    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactionRunner = transactionRunner;
    this.maxRetriesOnError = cConf.getInt(Constants.Metadata.MESSAGING_RETRIES_ON_CONFLICT);
    this.logProcessor = logProcessor;
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
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
  protected void processMessages(StructuredTableContext structuredTableContext,
                                 Iterator<ImmutablePair<String, Iterator<byte[]>>> messages) throws Exception {
    while (messages.hasNext()) {
      ImmutablePair<String, Iterator<byte[]>> next = messages.next();
      String messageId = next.getFirst();
      Iterator<byte[]> message = next.getSecond();
      try {
        logProcessor.process(message);
      } catch (Exception e) {
        if (messageId.equals(erroredMessageId)) {
          errorCount++;
          if (errorCount >= maxRetriesOnError) {
            LOG.warn("Skipping preview message {} after processing it has caused {} consecutive errors: {}",
                     message, errorCount, e.getMessage());
            continue;
          }
        } else {
          erroredMessageId = messageId;
          errorCount = 1;
        }
        throw e;
      }
    }
  }

  @Override
  protected MessagingContext getMessagingContext() {
    return messagingContext;
  }

  @Override
  protected Iterator<byte[]> decodeMessage(Message message) throws Exception {
    return Collections.singletonList(message.getPayload()).iterator();
  }
}
