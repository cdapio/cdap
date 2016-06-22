/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationVerificationStage;
import co.cask.cdap.internal.app.deploy.pipeline.CreateDatasetInstancesStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeployDatasetModulesStage;
import co.cask.cdap.internal.app.deploy.pipeline.LocalArtifactLoaderStage;
import co.cask.cdap.internal.app.deploy.pipeline.ProgramGenerationStage;
import co.cask.cdap.internal.app.deploy.pipeline.SystemMetadataWriterStage;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * This class is concrete implementation of {@link Manager} that deploys an Preview Application.
 *
 * @param <I> Input type.
 * @param <O> Output type.
 */
public class PreviewApplicationManager<I, O> implements Manager<I, O> {
  private final PipelineFactory pipelineFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final CConfiguration configuration;
  private final Store store;
  private final DatasetFramework datasetFramework;
  private final DatasetFramework inMemoryDatasetFramework;
  private final UsageRegistry usageRegistry;
  private final ArtifactRepository artifactRepository;
  private final MetadataStore metadataStore;
  private final AuthorizerInstantiator authorizerInstantiator;

  @Inject
  PreviewApplicationManager(CConfiguration configuration, PipelineFactory pipelineFactory,
                            NamespacedLocationFactory namespacedLocationFactory,
                            Store store, DatasetFramework datasetFramework,
                            @Named("datasetMDS") DatasetFramework inMemoryDatasetFramework,
                            UsageRegistry usageRegistry, ArtifactRepository artifactRepository,
                            MetadataStore metadataStore, AuthorizerInstantiator authorizerInstantiator) {
    this.configuration = configuration;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.pipelineFactory = pipelineFactory;
    this.store = store;
    this.datasetFramework = datasetFramework;
    this.inMemoryDatasetFramework = inMemoryDatasetFramework;
    this.usageRegistry = usageRegistry;
    this.artifactRepository = artifactRepository;
    this.metadataStore = metadataStore;
    this.authorizerInstantiator = authorizerInstantiator;
  }

  @Override
  public ListenableFuture<O> deploy(I input) throws Exception {
    Pipeline<O> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArtifactLoaderStage(configuration, store, artifactRepository));
    pipeline.addLast(new ApplicationVerificationStage(store, datasetFramework));
    pipeline.addLast(new DeployDatasetModulesStage(configuration, datasetFramework,
                                                   inMemoryDatasetFramework));
    pipeline.addLast(new CreateDatasetInstancesStage(configuration, datasetFramework));
    pipeline.addLast(new ProgramGenerationStage(configuration, namespacedLocationFactory,
                                                authorizerInstantiator.get()));
    pipeline.addLast(new ApplicationRegistrationStage(store, usageRegistry));
    // not sure if we need this meta data writer stage.
    pipeline.addLast(new SystemMetadataWriterStage(metadataStore));
    return pipeline.execute(input);
  }
}
