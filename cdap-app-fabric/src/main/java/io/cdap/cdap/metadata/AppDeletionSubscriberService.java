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
package io.cdap.cdap.metadata;

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.state.AppStateTable;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.artifact.AppDeletionMessage;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;

import java.io.IOException;
import java.util.Iterator;
import javax.inject.Inject;

/**
 * A TMS Subscriber Service responsible for consuming app deletion messages from TMS and executes removal from
 * the app spec table. Subscribes to the Constants.AppFabric.APP_DELETION_EVENT_TOPIC topic.
 */
public class AppDeletionSubscriberService extends AbstractMetadataStoreMessagingSubscriberService<AppDeletionMessage> {

  @Inject
  AppDeletionSubscriberService(CConfiguration cConf, MessagingService messagingService,
                            MetricsCollectionService metricsCollectionService,
                            TransactionRunner transactionRunner) {
    super(cConf, messagingService, metricsCollectionService, transactionRunner, AppDeletionMessage.class);
  }

  private void removeApp(StructuredTableContext structuredTableContext, ApplicationId id) throws IOException {
    getAppStateTable(structuredTableContext).deleteAll(id.getNamespaceId(), id.getApplication());
    getAppMetadataStore(structuredTableContext).removeApplicationData(id.getAppReference());
  }

  private AppMetadataStore getAppMetadataStore(StructuredTableContext context) {
    return AppMetadataStore.create(context);
  }

  private AppStateTable getAppStateTable(StructuredTableContext context) throws TableNotFoundException {
    return new AppStateTable(context);
  }

  @Override
  protected void processMessages(StructuredTableContext structuredTableContext,
                                 Iterator<ImmutablePair<String, AppDeletionMessage>> messages) throws Exception {
    while (messages.hasNext()) {
      ImmutablePair<String, AppDeletionMessage> pair = messages.next();
      AppDeletionMessage message = pair.getSecond();
      ApplicationId appId = message.getApplicationId();
      removeApp(structuredTableContext, appId);
    }
  }
}
