/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import co.cask.cdap.internal.app.deploy.pipeline.CreateDatasetInstancesStage;
import co.cask.cdap.internal.app.deploy.pipeline.CreateSchedulesStage;
import co.cask.cdap.internal.app.deploy.pipeline.CreateStreamsStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeletedProgramHandlerStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeployCleanupStage;
import co.cask.cdap.internal.app.deploy.pipeline.DeployDatasetModulesStage;
import co.cask.cdap.internal.app.deploy.pipeline.LocalArchiveLoaderStage;
import co.cask.cdap.internal.app.deploy.pipeline.ProgramGenerationStage;
import co.cask.cdap.internal.app.deploy.pipeline.VerificationStage;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.proto.Id;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
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
  private final StreamAdmin streamAdmin;
  private final DatasetFramework datasetFramework;
  private final ExploreFacade exploreFacade;
  private final Scheduler scheduler;
  private final boolean exploreEnabled;

  private final AdapterService adapterService;
  private final ProgramTerminator programTerminator;



  @Inject
  public LocalManager(CConfiguration configuration, PipelineFactory pipelineFactory,
                      LocationFactory locationFactory, StoreFactory storeFactory,
                      StreamConsumerFactory streamConsumerFactory,
                      QueueAdmin queueAdmin, DiscoveryServiceClient discoveryServiceClient,
                      DatasetFramework datasetFramework,
                      StreamAdmin streamAdmin, ExploreFacade exploreFacade,
                      Scheduler scheduler, AdapterService adapterService,
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
      new NamespacedDatasetFramework(datasetFramework, new DefaultDatasetNamespace(configuration));
    this.streamAdmin = streamAdmin;
    this.exploreFacade = exploreFacade;
    this.scheduler = scheduler;
    this.exploreEnabled = configuration.getBoolean(Constants.Explore.EXPLORE_ENABLED);
    this.adapterService = adapterService;
  }

  @Override
  public ListenableFuture<O> deploy(Id.Namespace id, @Nullable String appId, I input) throws Exception {
    Pipeline<O> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArchiveLoaderStage(configuration, id, appId));
    pipeline.addLast(new VerificationStage(datasetFramework, adapterService));
    pipeline.addLast(new DeployDatasetModulesStage(datasetFramework));
    pipeline.addLast(new CreateDatasetInstancesStage(datasetFramework));
    pipeline.addLast(new CreateStreamsStage(id, streamAdmin, exploreFacade, exploreEnabled));
    pipeline.addLast(new CreateSchedulesStage(store, scheduler));
    pipeline.addLast(new DeletedProgramHandlerStage(store, programTerminator, streamConsumerFactory,
                                                    queueAdmin, discoveryServiceClient));
    pipeline.addLast(new ProgramGenerationStage(configuration, locationFactory));
    pipeline.addLast(new ApplicationRegistrationStage(store));
    pipeline.setFinally(new DeployCleanupStage());
    return pipeline.execute(input);
  }
}
