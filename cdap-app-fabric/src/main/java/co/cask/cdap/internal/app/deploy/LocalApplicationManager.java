/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationVerificationStage;
import co.cask.cdap.internal.app.deploy.pipeline.CreateDatasetInstancesStage;
import co.cask.cdap.internal.app.deploy.pipeline.CreateSchedulesStage;
import co.cask.cdap.internal.app.deploy.pipeline.CreateStreamsStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeleteScheduleStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeletedProgramHandlerStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeployDatasetModulesStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentCleanupStage;
import co.cask.cdap.internal.app.deploy.pipeline.LocalArtifactLoaderStage;
import co.cask.cdap.internal.app.deploy.pipeline.ProgramGenerationStage;
import co.cask.cdap.internal.app.deploy.pipeline.SystemMetadataWriterStage;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.pipeline.Context;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.pipeline.Stage;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;

/**
 * This class is concrete implementation of {@link Manager} that deploys an Application.
 *
 * @param <I> Input type.
 * @param <O> Output type.
 */
public class LocalApplicationManager<I, O> implements Manager<I, O> {

  /**
   * The key used in the {@link Stage} {@link Context} property for storing the artifact classloader
   * of the artifact used during deployment.
   */
  public static final String ARTIFACT_CLASSLOADER_KEY = "artifact.classLoader";

  private final PipelineFactory pipelineFactory;
  private final CConfiguration configuration;
  private final Store store;
  private final OwnerAdmin ownerAdmin;
  private final StreamConsumerFactory streamConsumerFactory;
  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final Scheduler scheduler;
  private final ProgramTerminator programTerminator;
  private final DatasetFramework datasetFramework;
  private final DatasetFramework inMemoryDatasetFramework;
  private final MetricStore metricStore;
  private final UsageRegistry usageRegistry;
  private final ArtifactRepository artifactRepository;
  private final MetadataStore metadataStore;
  private final PrivilegesManager privilegesManager;
  private final Impersonator impersonator;
  private final AuthenticationContext authenticationContext;

  @Inject
  LocalApplicationManager(CConfiguration configuration, PipelineFactory pipelineFactory,
                          Store store, OwnerAdmin ownerAdmin, StreamConsumerFactory streamConsumerFactory,
                          QueueAdmin queueAdmin, DatasetFramework datasetFramework,
                          @Named("datasetMDS") DatasetFramework inMemoryDatasetFramework,
                          StreamAdmin streamAdmin, Scheduler scheduler,
                          @Assisted ProgramTerminator programTerminator, MetricStore metricStore,
                          UsageRegistry usageRegistry, ArtifactRepository artifactRepository,
                          MetadataStore metadataStore, PrivilegesManager privilegesManager,
                          Impersonator impersonator, AuthenticationContext authenticationContext) {
    this.configuration = configuration;
    this.pipelineFactory = pipelineFactory;
    this.store = store;
    this.ownerAdmin = ownerAdmin;
    this.streamConsumerFactory = streamConsumerFactory;
    this.queueAdmin = queueAdmin;
    this.programTerminator = programTerminator;
    this.datasetFramework = datasetFramework;
    this.inMemoryDatasetFramework = inMemoryDatasetFramework;
    this.streamAdmin = streamAdmin;
    this.scheduler = scheduler;
    this.metricStore = metricStore;
    this.usageRegistry = usageRegistry;
    this.artifactRepository = artifactRepository;
    this.metadataStore = metadataStore;
    this.privilegesManager = privilegesManager;
    this.impersonator = impersonator;
    this.authenticationContext = authenticationContext;
  }

  @Override
  public ListenableFuture<O> deploy(I input) throws Exception {
    Pipeline<O> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArtifactLoaderStage(configuration, store, artifactRepository, impersonator));
    pipeline.addLast(new ApplicationVerificationStage(store, datasetFramework, ownerAdmin));
    pipeline.addLast(new DeployDatasetModulesStage(configuration, datasetFramework, inMemoryDatasetFramework));
    pipeline.addLast(new CreateDatasetInstancesStage(configuration, datasetFramework));
    pipeline.addLast(new CreateStreamsStage(streamAdmin));
    pipeline.addLast(new DeletedProgramHandlerStage(store, programTerminator, streamConsumerFactory, queueAdmin,
                                                    metricStore, metadataStore, privilegesManager, impersonator));
    pipeline.addLast(new ProgramGenerationStage(privilegesManager, authenticationContext));
    pipeline.addLast(new DeleteScheduleStage(scheduler));
    pipeline.addLast(new ApplicationRegistrationStage(store, usageRegistry, ownerAdmin));
    pipeline.addLast(new CreateSchedulesStage(scheduler));
    pipeline.addLast(new SystemMetadataWriterStage(metadataStore));
    pipeline.setFinally(new DeploymentCleanupStage());
    return pipeline.execute(input);
  }
}
