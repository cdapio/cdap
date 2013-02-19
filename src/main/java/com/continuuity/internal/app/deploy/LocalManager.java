/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.deploy.Manager;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.Store;
import com.continuuity.common.conf.Configuration;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.deploy.pipeline.ApplicationRegistrationStage;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.deploy.pipeline.LocalArchiveLoaderStage;
import com.continuuity.internal.app.deploy.pipeline.ProgramGenerationStage;
import com.continuuity.internal.app.deploy.pipeline.VerificationStage;
import com.continuuity.pipeline.Pipeline;
import com.continuuity.pipeline.PipelineFactory;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

/**
 * This class is concrete implementation of {@link Manager}
 */
public class LocalManager implements Manager<Location, ApplicationWithPrograms> {
  private final PipelineFactory pipelineFactory;
  private final LocationFactory locationFactory;
  private final Configuration configuration;
  private final Store store;

  @Inject
  public LocalManager(Configuration configuration, PipelineFactory pipelineFactory,
                      LocationFactory locationFactory, Store store) {
    this.configuration = configuration;
    this.pipelineFactory = pipelineFactory;
    this.locationFactory = locationFactory;
    this.store = store;
  }

  /**
   * Executes a pipeline for deploying an archive.
   *
   * @param id account id to which the archive is deployed.
   * @param archive to be verified and converted into programs
   * @return A future of Application with Programs.
   */
  @Override
  public ListenableFuture<ApplicationWithPrograms> deploy(Id.Account id, Location archive) throws Exception {
    Pipeline<ApplicationWithPrograms> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new LocalArchiveLoaderStage(id));
    pipeline.addLast(new VerificationStage());
    pipeline.addLast(new ProgramGenerationStage(configuration, locationFactory));
    pipeline.addLast(new ApplicationRegistrationStage(store));
    return pipeline.execute(archive);
  }
}
