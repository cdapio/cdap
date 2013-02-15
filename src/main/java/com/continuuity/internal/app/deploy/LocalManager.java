/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.deploy.Manager;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.pipeline.PipelineFactory;
import com.continuuity.pipeline.Pipeline;

/**
 *
 */
public class LocalManager implements Manager {

  @Override
  public Pipeline deploy(Location deployedJar) throws Exception {
    Pipeline pipeline = PipelineFactory.newSynchronousPipeline();
    pipeline.addLast(new LocalArchiveLoaderStage());
    pipeline.addLast(new VerificationStage());
    return pipeline;
  }
}
