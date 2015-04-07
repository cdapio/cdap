/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy;

import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationTemplateVerificationStage;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeletedProgramHandlerStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeployDatasetModulesStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.EnableConcurrentRunsStage;
import co.cask.cdap.internal.app.deploy.pipeline.LocalArchiveLoaderStage;
import co.cask.cdap.internal.app.deploy.pipeline.ProgramGenerationStage;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.proto.Id;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.apache.twill.discovery.DiscoveryServiceClient;

import javax.annotation.Nullable;

/**
 * This class is concrete implementation of {@link Manager} that deploys an ApplicationTemplate.
 */
public class LocalApplicationTemplateManager implements Manager<DeploymentInfo, ApplicationWithPrograms> {
  private final PipelineFactory pipelineFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final CConfiguration configuration;
  private final Store store;
  private final StreamConsumerFactory streamConsumerFactory;
  private final QueueAdmin queueAdmin;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final AdapterService adapterService;
  private final ProgramTerminator programTerminator;
  private final DatasetFramework datasetFramework;
  private final DatasetFramework inMemoryDatasetFramework;
  private final PreferencesStore preferencesStore;

  @Inject
  public LocalApplicationTemplateManager(CConfiguration configuration, PipelineFactory pipelineFactory,
                                         NamespacedLocationFactory namespacedLocationFactory,
                                         Store store, StreamConsumerFactory streamConsumerFactory,
                                         QueueAdmin queueAdmin, DiscoveryServiceClient discoveryServiceClient,
                                         DatasetFramework datasetFramework,
                                         @Named("datasetMDS") DatasetFramework inMemoryDatasetFramework,
                                         StreamAdmin streamAdmin, ExploreFacade exploreFacade,
                                         AdapterService adapterService,
                                         PreferencesStore preferencesStore,
                                         @Assisted ProgramTerminator programTerminator) {
    this.configuration = configuration;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.pipelineFactory = pipelineFactory;
    this.discoveryServiceClient = discoveryServiceClient;
    this.store = store;
    this.streamConsumerFactory = streamConsumerFactory;
    this.queueAdmin = queueAdmin;
    this.programTerminator = programTerminator;
    this.datasetFramework = datasetFramework;
    this.inMemoryDatasetFramework = inMemoryDatasetFramework;
    this.adapterService = adapterService;
    this.preferencesStore = preferencesStore;
  }

  @Override
  public ListenableFuture<ApplicationWithPrograms> deploy(Id.Namespace namespace, @Nullable String templateId,
                                                          DeploymentInfo input) throws Exception {
    Pipeline<ApplicationWithPrograms> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArchiveLoaderStage(store, configuration, namespace, templateId));
    pipeline.addLast(new ApplicationTemplateVerificationStage(store, datasetFramework, adapterService));
    pipeline.addLast(new DeployDatasetModulesStage(configuration, namespace,
                                                   datasetFramework, inMemoryDatasetFramework));
    pipeline.addLast(new DeletedProgramHandlerStage(store, programTerminator, streamConsumerFactory,
                                                    queueAdmin, discoveryServiceClient));
    pipeline.addLast(new ProgramGenerationStage(configuration, namespacedLocationFactory));
    pipeline.addLast(new ApplicationRegistrationStage(store));
    pipeline.setFinally(new EnableConcurrentRunsStage(preferencesStore));
    return pipeline.execute(input);
  }
}
