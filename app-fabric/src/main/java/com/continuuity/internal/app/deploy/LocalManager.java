/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.Id;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.NamespacedDatasetFramework;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import com.continuuity.internal.app.deploy.pipeline.CreateDatasetInstancesStage;
import com.continuuity.internal.app.deploy.pipeline.DeletedProgramHandlerStage;
import com.continuuity.internal.app.deploy.pipeline.DeployDatasetModulesStage;
import com.continuuity.internal.app.deploy.pipeline.LocalArchiveLoaderStage;
import com.continuuity.internal.app.deploy.pipeline.ProgramGenerationStage;
import com.continuuity.internal.app.deploy.pipeline.VerificationStage;
import com.continuuity.pipeline.Pipeline;
import com.continuuity.pipeline.PipelineFactory;
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

  private final ProgramTerminator programTerminator;

  private final DatasetFramework datasetFramework;


  @Inject
  public LocalManager(CConfiguration configuration, PipelineFactory pipelineFactory,
                      LocationFactory locationFactory, StoreFactory storeFactory,
                      StreamConsumerFactory streamConsumerFactory,
                      QueueAdmin queueAdmin, DiscoveryServiceClient discoveryServiceClient,
                      DatasetFramework datasetFramework,
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
                                     new ReactorDatasetNamespace(configuration, DataSetAccessor.Namespace.USER));
  }

  @Override
  public ListenableFuture<O> deploy(Id.Account id, @Nullable String appId, I input) throws Exception {
    Pipeline<O> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArchiveLoaderStage(id, appId));
    pipeline.addLast(new VerificationStage());
    pipeline.addLast(new DeployDatasetModulesStage(datasetFramework));
    pipeline.addLast(new CreateDatasetInstancesStage(datasetFramework));
    pipeline.addLast(new DeletedProgramHandlerStage(store, programTerminator, streamConsumerFactory,
                                                    queueAdmin, discoveryServiceClient));
    pipeline.addLast(new ProgramGenerationStage(configuration, locationFactory));
    pipeline.addLast(new ApplicationRegistrationStage(store));
    return pipeline.execute(input);
  }
}
