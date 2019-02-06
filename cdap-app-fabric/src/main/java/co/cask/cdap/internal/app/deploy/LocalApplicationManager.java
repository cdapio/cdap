/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

import co.cask.cdap.api.metrics.MetricsSystemClient;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.MetadataPublisher;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationVerificationStage;
import co.cask.cdap.internal.app.deploy.pipeline.CreateDatasetInstancesStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeleteAndCreateSchedulesStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeletedProgramHandlerStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeployDatasetModulesStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentCleanupStage;
import co.cask.cdap.internal.app.deploy.pipeline.LocalArtifactLoaderStage;
import co.cask.cdap.internal.app.deploy.pipeline.ProgramGenerationStage;
import co.cask.cdap.internal.app.deploy.pipeline.SystemMetadataWriterStage;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.pipeline.Context;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.pipeline.Stage;
import co.cask.cdap.scheduler.Scheduler;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
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
  private final ProgramTerminator programTerminator;
  private final DatasetFramework datasetFramework;
  private final DatasetFramework inMemoryDatasetFramework;
  private final MetricsSystemClient metricsSystemClient;
  private final UsageRegistry usageRegistry;
  private final ArtifactRepository artifactRepository;
  private final MetadataPublisher metadataPublisher;
  private final Impersonator impersonator;
  private final AuthenticationContext authenticationContext;
  private final co.cask.cdap.scheduler.Scheduler programScheduler;
  private final AuthorizationEnforcer authorizationEnforcer;

  @Inject
  LocalApplicationManager(CConfiguration configuration, PipelineFactory pipelineFactory,
                          Store store, OwnerAdmin ownerAdmin,
                          DatasetFramework datasetFramework,
                          @Named("datasetMDS") DatasetFramework inMemoryDatasetFramework,
                          @Assisted ProgramTerminator programTerminator, MetricsSystemClient metricsSystemClient,
                          UsageRegistry usageRegistry, ArtifactRepository artifactRepository,
                          MetadataPublisher metadataPublisher,
                          Impersonator impersonator, AuthenticationContext authenticationContext,
                          Scheduler programScheduler,
                          AuthorizationEnforcer authorizationEnforcer) {
    this.configuration = configuration;
    this.pipelineFactory = pipelineFactory;
    this.store = store;
    this.ownerAdmin = ownerAdmin;
    this.programTerminator = programTerminator;
    this.datasetFramework = datasetFramework;
    this.inMemoryDatasetFramework = inMemoryDatasetFramework;
    this.metricsSystemClient = metricsSystemClient;
    this.usageRegistry = usageRegistry;
    this.artifactRepository = artifactRepository;
    this.metadataPublisher = metadataPublisher;
    this.impersonator = impersonator;
    this.authenticationContext = authenticationContext;
    this.programScheduler = programScheduler;
    this.authorizationEnforcer = authorizationEnforcer;
  }

  @Override
  public ListenableFuture<O> deploy(I input) throws Exception {
    Pipeline<O> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArtifactLoaderStage(configuration, store, artifactRepository, impersonator,
                                                  authorizationEnforcer, authenticationContext));
    pipeline.addLast(new ApplicationVerificationStage(store, datasetFramework, ownerAdmin, authenticationContext));
    pipeline.addLast(new DeployDatasetModulesStage(configuration, datasetFramework, inMemoryDatasetFramework,
                                                   ownerAdmin, authenticationContext));
    pipeline.addLast(new CreateDatasetInstancesStage(configuration, datasetFramework, ownerAdmin,
                                                     authenticationContext));
    pipeline.addLast(new DeletedProgramHandlerStage(store, programTerminator,
                                                    metricsSystemClient, metadataPublisher, programScheduler));
    pipeline.addLast(new ProgramGenerationStage());
    pipeline.addLast(new ApplicationRegistrationStage(store, usageRegistry, ownerAdmin));
    pipeline.addLast(new DeleteAndCreateSchedulesStage(programScheduler));
    pipeline.addLast(new SystemMetadataWriterStage(metadataPublisher));
    pipeline.setFinally(new DeploymentCleanupStage());
    return pipeline.execute(input);
  }
}
