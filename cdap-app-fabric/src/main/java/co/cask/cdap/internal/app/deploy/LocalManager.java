/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import co.cask.cdap.internal.app.deploy.pipeline.CreateDatasetInstancesStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeletedProgramHandlerStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeployCleanupStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeployDatasetModulesStage;
import co.cask.cdap.internal.app.deploy.pipeline.LocalArchiveLoaderStage;
import co.cask.cdap.internal.app.deploy.pipeline.ProgramGenerationStage;
import co.cask.cdap.internal.app.deploy.pipeline.VerificationStage;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.proto.Id;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;

import javax.annotation.Nullable;

/**
 * This class is concrete implementation of {@link Manager}.
 *
 * @param <I> Input type.
 * @param <O> Output type.
 */
public class LocalManager<I, O> implements Manager<I, O> {
  private final PipelineFactory pipelineFactory;
  private final LocationFactory locationFactory;
  private final CConfiguration configuration;
  private final Store store;
  private final StreamConsumerFactory streamConsumerFactory;
  private final QueueAdmin queueAdmin;
  private final DiscoveryServiceClient discoveryServiceClient;

  private final ProgramTerminator programTerminator;

  private final DatasetFramework datasetFramework;
  private final DatasetFramework inMemoryDatasetFramework;


  @Inject
  public LocalManager(CConfiguration configuration, PipelineFactory pipelineFactory,
                      LocationFactory locationFactory, StoreFactory storeFactory,
                      StreamConsumerFactory streamConsumerFactory,
                      QueueAdmin queueAdmin, DiscoveryServiceClient discoveryServiceClient,
                      DatasetFramework datasetFramework, @Named("datasetMDS") DatasetFramework inMemoryDatasetFramework,
                      @Assisted ProgramTerminator programTerminator) {

    this.configuration = configuration;
    this.pipelineFactory = pipelineFactory;
    this.locationFactory = locationFactory;
    this.discoveryServiceClient = discoveryServiceClient;
    this.store = storeFactory.create();
    this.streamConsumerFactory = streamConsumerFactory;
    this.queueAdmin = queueAdmin;
    this.programTerminator = programTerminator;
    this.datasetFramework =
      new NamespacedDatasetFramework(datasetFramework,
                                     new DefaultDatasetNamespace(configuration, Namespace.USER));
    this.inMemoryDatasetFramework = inMemoryDatasetFramework;
  }

  @Override
  public ListenableFuture<O> deploy(Id.Account id, @Nullable String appId, I input) throws Exception {
    Pipeline<O> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArchiveLoaderStage(configuration, id, appId));
    pipeline.addLast(new VerificationStage(datasetFramework));
    pipeline.addLast(new DeployDatasetModulesStage(configuration, datasetFramework, inMemoryDatasetFramework));
    pipeline.addLast(new CreateDatasetInstancesStage(configuration, datasetFramework));
    pipeline.addLast(new DeletedProgramHandlerStage(store, programTerminator, streamConsumerFactory,
                                                    queueAdmin, discoveryServiceClient));
    pipeline.addLast(new ProgramGenerationStage(configuration, locationFactory));
    pipeline.addLast(new ApplicationRegistrationStage(store));
    pipeline.setFinally(new DeployCleanupStage());
    return pipeline.execute(input);
  }
}
