/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.deploy.Manager;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.pipeline.LocalArchiveLoaderStage;
import com.continuuity.internal.pipeline.VerificationStage;
import com.continuuity.pipeline.Pipeline;
import com.continuuity.pipeline.PipelineFactory;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

/**
 *
 */
public class LocalManager implements Manager {
  private final PipelineFactory factory;

  @Inject
  public LocalManager(PipelineFactory factory) {
    this.factory = factory;
  }

  @Override
  public ListenableFuture<?> deploy(Location archive) throws Exception {
    Pipeline pipeline = factory.getPipeline();
    pipeline.addLast(new LocalArchiveLoaderStage());
    pipeline.addLast(new VerificationStage());
    return pipeline.execute(archive);
  }
}
