/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

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
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuditLoggerSpi;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * This class subscribes to the audit log messaging topic, reads and processes the messages into {@link AuditLogContext}
 * and delegates the batch of AuditLogContexts to External Auth service using {@link AuditLoggerSpi}, which would
 * further publish as configured.
 */
public class AuditLogSubscriberService extends AbstractMessagingSubscriberService<AuditLogContext> {

  private static final Logger LOG = LoggerFactory.getLogger(AuditLogSubscriberService.class);
  private static final Gson GSON = new Gson();

  private final MultiThreadMessagingContext messagingContext;
  private final TransactionRunner transactionRunner;
  private final AccessControllerInstantiator accessControllerInstantiator;

  @Inject
  AuditLogSubscriberService(CConfiguration cConf, MessagingService messagingService,
                            MetricsCollectionService metricsCollectionService,
                            TransactionRunner transactionRunner,
                            AccessControllerInstantiator accessControllerInstantiator) {

    super(
      NamespaceId.SYSTEM.topic(cConf.get(Constants.AuditLogging.AUDIT_LOG_EVENT_TOPIC)),
      cConf.getInt(Constants.AuditLogging.AUDIT_LOG_FETCH_SIZE),
      cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT),
      cConf.getInt(Constants.AuditLogging.AUDIT_LOG_POLL_DELAY_MILLIS),
      RetryStrategies.exponentialDelay(10, 200, TimeUnit.MILLISECONDS),
      metricsCollectionService.getContext(ImmutableMap.of(
        Constants.Metrics.Tag.COMPONENT, Constants.Service.MASTER_SERVICES,
        Constants.Metrics.Tag.INSTANCE_ID, "0",
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
        Constants.Metrics.Tag.TOPIC, cConf.get(Constants.AuditLogging.AUDIT_LOG_EVENT_TOPIC),
        Constants.Metrics.Tag.CONSUMER, Constants.AuditLogging.AUDIT_LOG_CONSUMER_WRITER_SUBSCRIBER
      )));
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactionRunner = transactionRunner;
    this.accessControllerInstantiator = accessControllerInstantiator;
  }

  /**
   * Returns the {@link TransactionRunner} for executing tasks in transaction.
   */
  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  /**
   * Loads last persisted message id. This method will be called from a transaction. The returned
   * message id will be used as the starting message id (exclusive) for the first fetch.
   */
  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    return appMetadataStore.retrieveSubscriberState(getTopicId().getTopic(),
                                                    Constants.AuditLogging.AUDIT_LOG_WRITER_SUBSCRIBER);
  }

  /**
   * Persists the given message id. This method will be called from a transaction, which is the same
   * transaction for the call to {@link #processMessages(StructuredTableContext, Iterator)}.
   */
  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    appMetadataStore.persistSubscriberState(getTopicId().getTopic(),
                                            Constants.AuditLogging.AUDIT_LOG_WRITER_SUBSCRIBER, messageId);
  }

  /**
   * Processes the give list of messages. This method will be called from the same transaction as
   * the {@link #storeMessageId(StructuredTableContext, String)} call. If {@link Exception} is
   * raised from this method, the messages as provided through the {@code messages} parameter will
   * be replayed in the next call.
   */
  @Override
  protected void processMessages(StructuredTableContext structuredTableContext,
                                 Iterator<ImmutablePair<String, AuditLogContext>> messages) throws Exception {

    Queue<AuditLogContext> auditLogContextQueue = new LinkedBlockingDeque<>();

    int count = 0 ;
    while (messages.hasNext()) {
      ImmutablePair<String, AuditLogContext> next = messages.next();
      AuditLogContext auditLogContext = next.getSecond();
      if (auditLogContext.isAuditLoggingRequired()){
        auditLogContextQueue.add(auditLogContext);
      }
      count++;
    }

    if (!auditLogContextQueue.isEmpty()) {
      LOG.debug("Publishing a queue of Audit Log events of size {} events.", auditLogContextQueue.size());
      AuditLoggerSpi.PublishStatus publishStatus =
        this.accessControllerInstantiator.get().publish(auditLogContextQueue);
      // TODO : This logic can change based on how Auth Ext publishes a batch.
      if (publishStatus.equals(AuditLoggerSpi.PublishStatus.UNSUCCESSFUL)) {
        throw new Exception("The publishing of audit log events Failed.");
      }
    }

    LOG.trace("Publishing a queue of Audit Log events of size {} events is successful.", auditLogContextQueue.size());
  }

  /**
   * Returns the {@link MessagingContext} that this service used for interacting with TMS.
   */
  @Override
  protected MessagingContext getMessagingContext() {
    return messagingContext;
  }

  /**
   * Decodes the raw {@link Message} into an object of type {@link AuditLogContext}.
   */
  @Override
  protected AuditLogContext decodeMessage(Message message) throws Exception {
    return message.decodePayload(r -> GSON.fromJson(r, AuditLogContext.class));
  }
}
