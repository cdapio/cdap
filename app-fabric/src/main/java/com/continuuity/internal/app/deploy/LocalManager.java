/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.Id;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.deploy.pipeline.DeletedProgramHandlerStage;
import com.continuuity.internal.app.deploy.pipeline.LocalArchiveLoaderStage;
import com.continuuity.internal.app.deploy.pipeline.ProgramGenerationStage;
import com.continuuity.internal.app.deploy.pipeline.VerificationStage;
import com.continuuity.pipeline.Pipeline;
import com.continuuity.pipeline.PipelineFactory;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * This class is concrete implementation of {@link Manager}.
 */
public class LocalManager implements Manager<Location, ApplicationWithPrograms> {
  private final PipelineFactory<?> pipelineFactory;
  private final LocationFactory locationFactory;
  private final CConfiguration configuration;
  private final Store store;
  private final ProgramTerminator programTerminator;
  private final QueueAdmin queueAdmin;
  private final DiscoveryServiceClient discoveryServiceClient;

  public LocalManager(CConfiguration configuration, PipelineFactory<?> pipelineFactory,
                      LocationFactory locationFactory, StoreFactory storeFactory,
                      ProgramTerminator programTerminator, QueueAdmin queueAdmin,
                      DiscoveryServiceClient discoveryServiceClient) {
    this.configuration = configuration;
    this.pipelineFactory = pipelineFactory;
    this.locationFactory = locationFactory;
    this.discoveryServiceClient = discoveryServiceClient;
    this.store = storeFactory.create();
    this.programTerminator = programTerminator;
    this.queueAdmin = queueAdmin;
  }

  /**
   * Executes a pipeline for deploying an archive.
   *
   * @param id account id to which the archive is deployed.
   * @param archive to be verified and converted into programs
   * @return A future of Application with Programs.
   */
  @Override
  public ListenableFuture<ApplicationWithPrograms> deploy(Id.Account id, Location archive) {
    Pipeline<ApplicationWithPrograms> pipeline = (Pipeline<ApplicationWithPrograms>) pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArchiveLoaderStage(id));
    pipeline.addLast(new VerificationStage());
    pipeline.addLast(new DeletedProgramHandlerStage(store, programTerminator, queueAdmin, discoveryServiceClient));
    pipeline.addLast(new ProgramGenerationStage(configuration, locationFactory));
    pipeline.addLast(new ApplicationRegistrationStage(store));
    return pipeline.execute(archive);
  }
}
