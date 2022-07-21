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

package io.cdap.cdap.internal.app.deploy;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationVerificationStage;
import io.cdap.cdap.internal.app.deploy.pipeline.CreateDatasetInstancesStage;
import io.cdap.cdap.internal.app.deploy.pipeline.CreateSystemTablesStage;
import io.cdap.cdap.internal.app.deploy.pipeline.DeleteAndCreateSchedulesStage;
import io.cdap.cdap.internal.app.deploy.pipeline.DeletedProgramHandlerStage;
import io.cdap.cdap.internal.app.deploy.pipeline.DeployDatasetModulesStage;
import io.cdap.cdap.internal.app.deploy.pipeline.DeploymentCleanupStage;
import io.cdap.cdap.internal.app.deploy.pipeline.LocalArtifactLoaderStage;
import io.cdap.cdap.internal.app.deploy.pipeline.MetadataWriterStage;
import io.cdap.cdap.internal.app.deploy.pipeline.ProgramGenerationStage;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.pipeline.Pipeline;
import io.cdap.cdap.pipeline.PipelineFactory;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.spi.data.StructuredTableAdmin;

/**
 * This class is concrete implementation of {@link Manager} that deploys an Application.
 *
 * @param <I> Input type.
 * @param <O> Output type.
 */
public class LocalApplicationManager<I, O> implements Manager<I, O> {

  private final PipelineFactory pipelineFactory;
  private final CConfiguration cConf;
  private final Store store;
  private final OwnerAdmin ownerAdmin;
  private final ProgramTerminator programTerminator;
  private final DatasetFramework datasetFramework;
  private final DatasetFramework inMemoryDatasetFramework;
  private final MetricsSystemClient metricsSystemClient;
  private final UsageRegistry usageRegistry;
  private final ArtifactRepository artifactRepository;
  private final MetadataServiceClient metadataServiceClient;
  private final Impersonator impersonator;
  private final AuthenticationContext authenticationContext;
  private final io.cdap.cdap.scheduler.Scheduler programScheduler;
  private final AccessEnforcer accessEnforcer;
  private final StructuredTableAdmin structuredTableAdmin;
  private final CapabilityReader capabilityReader;
  private final ConfiguratorFactory configuratorFactory;

  @Inject
  LocalApplicationManager(CConfiguration cConf, PipelineFactory pipelineFactory,
                          Store store, OwnerAdmin ownerAdmin,
                          DatasetFramework datasetFramework,
                          @Named("datasetMDS") DatasetFramework inMemoryDatasetFramework,
                          @Assisted ProgramTerminator programTerminator, MetricsSystemClient metricsSystemClient,
                          UsageRegistry usageRegistry, ArtifactRepository artifactRepository,
                          MetadataServiceClient metadataServiceClient,
                          Impersonator impersonator, AuthenticationContext authenticationContext,
                          Scheduler programScheduler,
                          AccessEnforcer accessEnforcer,
                          StructuredTableAdmin structuredTableAdmin,
                          CapabilityReader capabilityReader,
                          ConfiguratorFactory configuratorFactory) {
    this.cConf = cConf;
    this.pipelineFactory = pipelineFactory;
    this.store = store;
    this.ownerAdmin = ownerAdmin;
    this.programTerminator = programTerminator;
    this.datasetFramework = datasetFramework;
    this.inMemoryDatasetFramework = inMemoryDatasetFramework;
    this.metricsSystemClient = metricsSystemClient;
    this.usageRegistry = usageRegistry;
    this.artifactRepository = artifactRepository;
    this.metadataServiceClient = metadataServiceClient;
    this.impersonator = impersonator;
    this.authenticationContext = authenticationContext;
    this.programScheduler = programScheduler;
    this.accessEnforcer = accessEnforcer;
    this.structuredTableAdmin = structuredTableAdmin;
    this.capabilityReader = capabilityReader;
    this.configuratorFactory = configuratorFactory;
  }

  @Override
  public ListenableFuture<O> deploy(I input) throws Exception {
    Pipeline<O> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArtifactLoaderStage(cConf, store, accessEnforcer, authenticationContext,
                                                  capabilityReader, configuratorFactory));
    pipeline.addLast(new ApplicationVerificationStage(store, datasetFramework, ownerAdmin, authenticationContext));
    pipeline.addLast(new CreateSystemTablesStage(structuredTableAdmin));
    pipeline.addLast(new DeployDatasetModulesStage(cConf, datasetFramework, inMemoryDatasetFramework, ownerAdmin,
                                                   authenticationContext, artifactRepository, impersonator));
    pipeline.addLast(new CreateDatasetInstancesStage(cConf, datasetFramework, ownerAdmin,
                                                     authenticationContext));
    pipeline.addLast(new DeletedProgramHandlerStage(store, programTerminator,
                                                    metricsSystemClient, metadataServiceClient, programScheduler));
    pipeline.addLast(new ProgramGenerationStage());
    pipeline.addLast(new ApplicationRegistrationStage(store, usageRegistry, ownerAdmin));
    pipeline.addLast(new DeleteAndCreateSchedulesStage(programScheduler));
    pipeline.addLast(new MetadataWriterStage(metadataServiceClient));
    pipeline.setFinally(new DeploymentCleanupStage());
    return pipeline.execute(input);
  }
}
