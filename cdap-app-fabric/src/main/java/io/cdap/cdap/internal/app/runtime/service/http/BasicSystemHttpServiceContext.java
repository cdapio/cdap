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
 */

package io.cdap.cdap.internal.app.runtime.service.http;

import com.google.gson.Gson;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.RemoteTaskExecutor;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.MacroParser;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.services.DefaultSystemTableConfigurer;
import io.cdap.cdap.internal.app.worker.SystemAppTask;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link SystemHttpServiceContext} for system app services to use.
 */
public class BasicSystemHttpServiceContext extends BasicHttpServiceContext implements SystemHttpServiceContext {

  private static final Gson GSON = new Gson();

  private final NamespaceId namespaceId;
  private final TransactionRunner transactionRunner;
  private final PreferencesFetcher preferencesFetcher;
  private final RemoteTaskExecutor remoteTaskExecutor;
  private final CConfiguration cConf;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final ContextAccessEnforcer contextAccessEnforcer;

  /**
   * Creates a BasicSystemHttpServiceContext.
   */
  public BasicSystemHttpServiceContext(Program program, ProgramOptions programOptions,
                                       CConfiguration cConf, @Nullable HttpServiceHandlerSpecification spec,
                                       int instanceId, AtomicInteger instanceCount,
                                       MetricsCollectionService metricsCollectionService,
                                       DatasetFramework dsFramework, DiscoveryServiceClient discoveryServiceClient,
                                       TransactionSystemClient txClient,
                                       @Nullable PluginInstantiator pluginInstantiator,
                                       SecureStore secureStore, SecureStoreManager secureStoreManager,
                                       MessagingService messagingService, ArtifactManager artifactManager,
                                       MetadataReader metadataReader, MetadataPublisher metadataPublisher,
                                       NamespaceQueryAdmin namespaceQueryAdmin, PluginFinder pluginFinder,
                                       FieldLineageWriter fieldLineageWriter, TransactionRunner transactionRunner,
                                       PreferencesFetcher preferencesFetcher, RemoteClientFactory remoteClientFactory,
                                       ContextAccessEnforcer contextAccessEnforcer) {
    super(program, programOptions, cConf, spec, instanceId, instanceCount, metricsCollectionService, dsFramework,
          discoveryServiceClient, txClient, pluginInstantiator, secureStore, secureStoreManager, messagingService,
          artifactManager, metadataReader, metadataPublisher, namespaceQueryAdmin, pluginFinder, fieldLineageWriter,
          remoteClientFactory);

    this.namespaceId = program.getId().getNamespaceId();
    this.transactionRunner = transactionRunner;
    this.preferencesFetcher = preferencesFetcher;
    this.cConf = cConf;
    this.contextAccessEnforcer = contextAccessEnforcer;
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, metricsCollectionService, remoteClientFactory);
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
      tableId -> context.getTable(new StructuredTableId(DefaultSystemTableConfigurer.PREFIX + tableId.getName()))));
  }


  @Override
  public Map<String, String> evaluateMacros(String namespace, Map<String, String> macros,
                                            MacroEvaluator evaluator,
                                            MacroParserOptions options) throws InvalidMacroException {
    MacroParser macroParser = new MacroParser(evaluator, options);
    Map<String, String> evaluated = new HashMap<>();

    for (Map.Entry<String, String> property : macros.entrySet()) {
      String key = property.getKey();
      String val = property.getValue();
      evaluated.put(key, macroParser.parse(val));
    }

    return evaluated;
  }

  /**
   * Get preferences for the supplied namespace.
   *
   * @param namespace the name of the namespace to fetch preferences for.
   * @param resolved  true if resolved properties are desired.
   * @return Map containing the preferences for this namespace
   * @throws IOException if the preferencesFetcher could not complete the request.
   * @throws IllegalArgumentException if the supplied namespace doesn't exist.
   */
  @Override
  public Map<String, String> getPreferencesForNamespace(String namespace, boolean resolved)
    throws IOException, IllegalArgumentException, UnauthorizedException {
    try {
      return Retries
        .callWithRetries(() -> preferencesFetcher.get(new NamespaceId(namespace), resolved).getProperties(),
                         getRetryStrategy());
    } catch (NotFoundException nfe) {
      throw new IllegalArgumentException(String.format("Namespace '%s' does not exist", namespace), nfe);
    } catch (IOException | IllegalArgumentException | UnauthorizedException e) {
      //keep the behavior same prior to retry logic
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] runTask(RunnableTaskRequest runnableTaskRequest) throws Exception {
    if (!isRemoteTaskEnabled()) {
      throw new RuntimeException("Remote task worker is not enabled. Task cannot be executed.");
    }
    String systemAppClassName = SystemAppTask.class.getName();
    RunnableTaskRequest taskRequest = RunnableTaskRequest.getBuilder(systemAppClassName)
      .withNamespace(getNamespace())
      .withArtifact(getArtifactId().toApiArtifactId())
      .withEmbeddedTaskRequest(runnableTaskRequest)
      .build();
    return remoteTaskExecutor.runTask(taskRequest);
  }

  @Override
  public boolean isRemoteTaskEnabled() {
    return cConf.getBoolean(Constants.TaskWorker.POOL_ENABLE, false);
  }

  @Override
  public List<NamespaceSummary> listNamespaces() throws Exception {
    List<NamespaceSummary> summaries = new ArrayList<>();
    namespaceQueryAdmin.list().forEach(
      ns -> summaries.add(new NamespaceSummary(ns.getName(), ns.getDescription(), ns.getGeneration())));
    return summaries;
  }

  @Override
  public ContextAccessEnforcer getContextAccessEnforcer() {
    return contextAccessEnforcer;
  }
}
