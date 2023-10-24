/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.app.runtime.service;

import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.service.SystemServiceContext;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.services.DefaultSystemTableConfigurer;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.tephra.TransactionSystemClient;

/**
 * Default implementation of {@link SystemServiceContext} for system app services to use.
 */
public class BasicSystemServiceContext extends BasicServiceContext implements SystemServiceContext {

  private final NamespaceId namespaceId;
  private final TransactionRunner transactionRunner;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  public BasicSystemServiceContext(ServiceSpecification spec, Program program,
      ProgramOptions programOptions,
      int instanceId, AtomicInteger instanceCount,
      CConfiguration cConf,
      MetricsCollectionService metricsCollectionService, DatasetFramework datasetFramework,
      TransactionSystemClient transactionSystemClient,
      @Nullable PluginInstantiator pluginInstantiator, SecureStore secureStore,
      SecureStoreManager secureStoreManager, MessagingService messagingService,
      MetadataReader metadataReader, MetadataPublisher metadataPublisher,
      NamespaceQueryAdmin namespaceQueryAdmin, FieldLineageWriter fieldLineageWriter,
      TransactionRunner transactionRunner, RemoteClientFactory remoteClientFactory,
      ArtifactManager artifactManager, AppStateStoreProvider appStateStoreProvider) {
    super(spec, program, programOptions, instanceId, instanceCount, cConf, metricsCollectionService,
        datasetFramework,
        transactionSystemClient, pluginInstantiator, secureStore, secureStoreManager,
        messagingService, metadataReader, metadataPublisher, namespaceQueryAdmin,
        fieldLineageWriter,
        remoteClientFactory, artifactManager, appStateStoreProvider);
    this.namespaceId = program.getId().getNamespaceId();
    this.transactionRunner = transactionRunner;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    if (!namespaceId.equals(NamespaceId.SYSTEM)) {
      // should not happen in normal circumstances, as this is checked when the application is deployed.
      // could possibly be called if the user is directly casting to a SystemHttpServiceContext in user services.
      throw new IllegalStateException("System table transactions can only be run by "
          + "applications in the system namespace.");
    }
    // table names are prefixed to prevent clashes with CDAP platform tables.
    transactionRunner.run(context -> runnable.run(
        tableId -> context.getTable(
            new StructuredTableId(DefaultSystemTableConfigurer.PREFIX + tableId.getName()))));
  }

  @Override
  public List<NamespaceSummary> listNamespaces() throws Exception {
    List<NamespaceSummary> summaries = new ArrayList<>();
    namespaceQueryAdmin.list().forEach(
        ns -> summaries.add(
            new NamespaceSummary(ns.getName(), ns.getDescription(), ns.getGeneration())));
    return summaries;
  }
}
