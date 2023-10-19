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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.service.ServiceContext;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AbstractContext;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.messaging.spi.MessagingService;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.tephra.TransactionSystemClient;

/**
 * Basic service context
 */
public class BasicServiceContext extends AbstractContext implements ServiceContext {

  private final ServiceSpecification specification;
  private final ArtifactManager artifactManager;
  private final int instanceId;
  private final AtomicInteger instanceCount;

  public BasicServiceContext(ServiceSpecification spec, Program program,
      ProgramOptions programOptions,
      int instanceId, AtomicInteger instanceCount,
      CConfiguration cConf,
      MetricsCollectionService metricsCollectionService, DatasetFramework datasetFramework,
      TransactionSystemClient transactionSystemClient,
      @Nullable PluginInstantiator pluginInstantiator,
      SecureStore secureStore,
      SecureStoreManager secureStoreManager,
      MessagingService messagingService, MetadataReader metadataReader,
      MetadataPublisher metadataPublisher,
      NamespaceQueryAdmin namespaceQueryAdmin, FieldLineageWriter fieldLineageWriter,
      RemoteClientFactory remoteClientFactory, ArtifactManager artifactManager,
      AppStateStoreProvider appStateStoreProvider) {
    super(program, programOptions, cConf, Collections.emptySet(), datasetFramework,
        transactionSystemClient, false,
        metricsCollectionService, ImmutableMap.of(), secureStore, secureStoreManager,
        messagingService,
        pluginInstantiator, metadataReader, metadataPublisher, namespaceQueryAdmin,
        fieldLineageWriter,
        remoteClientFactory, appStateStoreProvider);
    this.instanceCount = instanceCount;
    this.instanceId = instanceId;
    this.specification = spec;
    this.artifactManager = artifactManager;
  }

  @Override
  public ServiceSpecification getSpecification() {
    return specification;
  }

  @Override
  public List<ArtifactInfo> listArtifacts() throws IOException, AccessException {
    return artifactManager.listArtifacts();
  }

  @Override
  public List<ArtifactInfo> listArtifacts(String namespace) throws IOException, AccessException {
    return artifactManager.listArtifacts(namespace);
  }

  @Override
  public CloseableClassLoader createClassLoader(ArtifactInfo artifactInfo,
      @Nullable ClassLoader parentClassLoader)
      throws IOException, AccessException {
    return artifactManager.createClassLoader(artifactInfo, parentClassLoader);
  }

  @Override
  public CloseableClassLoader createClassLoader(String namespace, ArtifactInfo artifactInfo,
      @Nullable ClassLoader parentClassLoader)
      throws IOException, AccessException {
    return artifactManager.createClassLoader(namespace, artifactInfo, parentClassLoader);
  }

  @Override
  public int getInstanceCount() {
    return instanceCount.get();
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }
}
