/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.preview;

import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationVerificationStage;
import co.cask.cdap.internal.app.deploy.pipeline.CreateDatasetInstancesStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeployDatasetModulesStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentCleanupStage;
import co.cask.cdap.internal.app.deploy.pipeline.LocalArtifactLoaderStage;
import co.cask.cdap.internal.app.deploy.pipeline.ProgramGenerationStage;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.name.Named;

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
  private final DatasetFramework datasetFramework;
  private final DatasetFramework inMemoryDatasetFramework;
  private final UsageRegistry usageRegistry;
  private final ArtifactRepository artifactRepository;
  private final Impersonator impersonator;
  private final PrivilegesManager privilegesManager;
  private final AuthenticationContext authenticationContext;

  @Inject
  PreviewApplicationManager(CConfiguration configuration, PipelineFactory pipelineFactory,
                            Store store, DatasetFramework datasetFramework,
                            @Named("datasetMDS") DatasetFramework inMemoryDatasetFramework,
                            UsageRegistry usageRegistry, ArtifactRepository artifactRepository,
                            PrivilegesManager privilegesManager,
                            AuthenticationContext authenticationContext, Impersonator impersonator) {
    this.cConf = configuration;
    this.pipelineFactory = pipelineFactory;
    this.store = store;
    this.datasetFramework = datasetFramework;
    this.inMemoryDatasetFramework = inMemoryDatasetFramework;
    this.usageRegistry = usageRegistry;
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;
    this.privilegesManager = privilegesManager;
    this.authenticationContext = authenticationContext;
  }

  @Override
  public ListenableFuture<O> deploy(I input) throws Exception {
    Pipeline<O> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArtifactLoaderStage(cConf, store, artifactRepository, impersonator));
    pipeline.addLast(new ApplicationVerificationStage(store, datasetFramework));
    pipeline.addLast(new DeployDatasetModulesStage(cConf, datasetFramework,
                                                   inMemoryDatasetFramework));
    pipeline.addLast(new CreateDatasetInstancesStage(cConf, datasetFramework));
    pipeline.addLast(new ProgramGenerationStage(privilegesManager, authenticationContext));
    pipeline.addLast(new ApplicationRegistrationStage(store, usageRegistry));
    pipeline.setFinally(new DeploymentCleanupStage());
    return pipeline.execute(input);
  }
}
