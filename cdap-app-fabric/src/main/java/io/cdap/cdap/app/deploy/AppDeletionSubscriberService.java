/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.cdap.app.deploy;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.state.AppStateTable;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import io.cdap.cdap.proto.artifact.AppDeletionMessage;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.tephra.TxConstants;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * A TMS Subscriber Service responsible for consuming app deletion messages from TMS and executes removal from
 * the app spec table. Subscribes to the Constants.AppFabric.APP_DELETION_EVENT_TOPIC topic.
 */
public class AppDeletionSubscriberService extends AbstractMessagingSubscriberService<AppDeletionMessage> {
  private static final String SUBSCRIBER = "appdelete.writer";
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private final CConfiguration cConf;
  private final MultiThreadMessagingContext messagingContext;
  private final TransactionRunner transactionRunner;
  private final int maxRetriesOnConflict;
  private final MetricsCollectionService metricsCollectionService;


  @Inject
  AppDeletionSubscriberService(CConfiguration cConf, MessagingService messagingService,
                            MetricsCollectionService metricsCollectionService,
                            TransactionRunner transactionRunner) {
    super(
      NamespaceId.SYSTEM.topic(cConf.get(Constants.AppFabric.APP_DELETION_EVENT_TOPIC)),
      cConf.getInt(Constants.AppFabric.APP_DELETE_EVENT_FETCH_SIZE),
      cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT),
      cConf.getLong(Constants.AppFabric.APP_DELETE_EVENT_POLL_DELAY_MILLIS),
      RetryStrategies.fromConfiguration(cConf, "system.appdelete."),
      metricsCollectionService.getContext(ImmutableMap.of(
        Constants.Metrics.Tag.COMPONENT, Constants.Service.MASTER_SERVICES,
        Constants.Metrics.Tag.INSTANCE_ID, "0",
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
        Constants.Metrics.Tag.TOPIC, cConf.get(Constants.AppFabric.APP_DELETION_EVENT_TOPIC),
        Constants.Metrics.Tag.CONSUMER, SUBSCRIBER
      )));

    this.cConf = cConf;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactionRunner = transactionRunner;
    this.maxRetriesOnConflict = cConf.getInt(Constants.Metadata.MESSAGING_RETRIES_ON_CONFLICT);
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    return appMetadataStore.retrieveSubscriberState(getTopicId().getTopic(), SUBSCRIBER);
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    appMetadataStore.persistSubscriberState(getTopicId().getTopic(), SUBSCRIBER, messageId);
  }

  @Override
  protected void processMessages(StructuredTableContext structuredTableContext,
                                 Iterator<ImmutablePair<String, AppDeletionMessage>> messages) throws Exception {
    while (messages.hasNext()) {
      ImmutablePair<String, AppDeletionMessage> pair = messages.next();
      AppDeletionMessage message = pair.getSecond();
      ApplicationId appId = message.getPayload(GSON, ApplicationId.class);
      removeApp(structuredTableContext, appId);
    }
  }

  private void removeApp(StructuredTableContext context, ApplicationId id) throws IOException {
    getAppStateTable(context).deleteAll(id.getNamespaceId(), id.getApplication());
    AppMetadataStore metaStore = getAppMetadataStore(context);
    metaStore.deleteApplication(id.getAppReference());
    metaStore.deleteApplicationEditRecord(id.getAppReference());
    metaStore.deleteProgramHistory(id.getAppReference());
  }

  private AppMetadataStore getAppMetadataStore(StructuredTableContext context) {
    return AppMetadataStore.create(context);
  }

  private AppStateTable getAppStateTable(StructuredTableContext context) throws TableNotFoundException {
    return new AppStateTable(context);
  }

  @Override
  protected MessagingContext getMessagingContext() {
    return messagingContext;
  }

  @Override
  protected AppDeletionMessage decodeMessage(Message message) throws Exception {
    return message.decodePayload(r -> GSON.fromJson(r, AppDeletionMessage.class));
  }
}
