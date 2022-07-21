/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.app.preview;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationVerificationStage;
import io.cdap.cdap.internal.app.deploy.pipeline.CreateDatasetInstancesStage;
import io.cdap.cdap.internal.app.deploy.pipeline.DeployDatasetModulesStage;
import io.cdap.cdap.internal.app.deploy.pipeline.DeploymentCleanupStage;
import io.cdap.cdap.internal.app.deploy.pipeline.LocalArtifactLoaderStage;
import io.cdap.cdap.internal.app.deploy.pipeline.ProgramGenerationStage;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.pipeline.Pipeline;
import io.cdap.cdap.pipeline.PipelineFactory;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;

/**
 * This class is concrete implementation of {@link Manager} that deploys a Preview Application.
 *
 * @param <I> Input type.
 * @param <O> Output type.
 */
public class PreviewApplicationManager<I, O> implements Manager<I, O> {

  private final PipelineFactory pipelineFactory;
  private final CConfiguration cConf;
  private final Store store;
  private final OwnerAdmin ownerAdmin;
  private final DatasetFramework datasetFramework;
  private final DatasetFramework inMemoryDatasetFramework;
  private final UsageRegistry usageRegistry;
  private final ArtifactRepository artifactRepository;
  private final Impersonator impersonator;
  private final AuthenticationContext authenticationContext;
  private final AccessEnforcer accessEnforcer;
  private final CapabilityReader capabilityReader;
  private final ConfiguratorFactory configuratorFactory;

  @Inject
  PreviewApplicationManager(CConfiguration cConf, PipelineFactory pipelineFactory,
                            Store store, OwnerAdmin ownerAdmin, DatasetFramework datasetFramework,
                            @Named("datasetMDS") DatasetFramework inMemoryDatasetFramework,
                            UsageRegistry usageRegistry, ArtifactRepository artifactRepository,
                            AuthenticationContext authenticationContext, Impersonator impersonator,
                            AccessEnforcer accessEnforcer,
                            CapabilityReader capabilityReader,
                            ConfiguratorFactory configuratorFactory) {
    this.cConf = cConf;
    this.pipelineFactory = pipelineFactory;
    this.store = store;
    this.datasetFramework = datasetFramework;
    this.inMemoryDatasetFramework = inMemoryDatasetFramework;
    this.usageRegistry = usageRegistry;
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;
    this.authenticationContext = authenticationContext;
    this.ownerAdmin = ownerAdmin;
    this.accessEnforcer = accessEnforcer;
    this.capabilityReader = capabilityReader;
    this.configuratorFactory = configuratorFactory;
  }

  @Override
  public ListenableFuture<O> deploy(I input) throws Exception {
    Pipeline<O> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArtifactLoaderStage(cConf, store, accessEnforcer, authenticationContext,
                                                  capabilityReader, configuratorFactory));
    pipeline.addLast(new ApplicationVerificationStage(store, datasetFramework, ownerAdmin, authenticationContext));
    pipeline.addLast(new DeployDatasetModulesStage(cConf, datasetFramework, inMemoryDatasetFramework, ownerAdmin,
                                                   authenticationContext, artifactRepository, impersonator));
    pipeline.addLast(new CreateDatasetInstancesStage(cConf, datasetFramework, ownerAdmin, authenticationContext));
    pipeline.addLast(new ProgramGenerationStage());
    pipeline.addLast(new ApplicationRegistrationStage(store, usageRegistry, ownerAdmin));
    pipeline.setFinally(new DeploymentCleanupStage());
    return pipeline.execute(input);
  }
}
