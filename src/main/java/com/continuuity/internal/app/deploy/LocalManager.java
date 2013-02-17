/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.deploy.Manager;
import com.continuuity.app.program.Id;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.deploy.pipeline.LocalArchiveLoaderStage;
import com.continuuity.internal.app.deploy.pipeline.VerificationStage;
import com.continuuity.pipeline.Pipeline;
import com.continuuity.pipeline.PipelineFactory;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

/**
 * This class is concrete implementation of
 */
public class LocalManager implements Manager<Location, String> {
  private final PipelineFactory factory;

  @Inject
  public LocalManager(PipelineFactory factory) {
    this.factory = factory;
  }

  @Override
  public ListenableFuture<String> deploy(Id.Account id, Location archive) throws Exception {
    Pipeline<String> pipeline = factory.getPipeline();
    pipeline.addLast(new LocalArchiveLoaderStage(id));
    pipeline.addLast(new VerificationStage());
    return pipeline.execute(archive);
  }
}
