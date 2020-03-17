/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.worker;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.worker.WorkerContext;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AbstractContext;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.messaging.MessagingService;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;

import javax.annotation.Nullable;

/**
 * Default implementation of {@link WorkerContext}
 */
final class BasicWorkerContext extends AbstractContext implements WorkerContext {

  private final WorkerSpecification specification;
  private final int instanceId;
  private volatile int instanceCount;

  BasicWorkerContext(WorkerSpecification spec, Program program, ProgramOptions programOptions,
                     CConfiguration cConf, int instanceId, int instanceCount,
                     MetricsCollectionService metricsCollectionService,
                     DatasetFramework datasetFramework,
                     TransactionSystemClient transactionSystemClient,
                     DiscoveryServiceClient discoveryServiceClient,
                     @Nullable PluginInstantiator pluginInstantiator,
                     SecureStore secureStore,
                     SecureStoreManager secureStoreManager,
                     MessagingService messagingService, MetadataReader metadataReader,
                     MetadataPublisher metadataPublisher,
                     NamespaceQueryAdmin namespaceQueryAdmin, FieldLineageWriter fieldLineageWriter) {
    super(program, programOptions, cConf, spec.getDatasets(),
          datasetFramework, transactionSystemClient, discoveryServiceClient, true,
          metricsCollectionService, ImmutableMap.of(Constants.Metrics.Tag.INSTANCE_ID, String.valueOf(instanceId)),
          secureStore, secureStoreManager, messagingService, pluginInstantiator, metadataReader, metadataPublisher,
          namespaceQueryAdmin, fieldLineageWriter);

    this.specification = spec;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
  }

  public WorkerSpecification getSpecification() {
    return specification;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  public void setInstanceCount(int instanceCount) {
    this.instanceCount = instanceCount;
  }

}
