/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.pipeline;

import com.continuuity.pipeline.Pipeline;

/**
 * A factory interface for creating pipelines. This class allows to
 * implement different {@link com.continuuity.pipeline.Pipeline} based on external constraints.
 */
public final class PipelineFactory {
  /**
   * @return A {@link com.continuuity.pipeline.Pipeline} created by the factory.
   */
  public static Pipeline newSynchronousPipeline() {
    return new SynchronousPipeline();
  }
}
