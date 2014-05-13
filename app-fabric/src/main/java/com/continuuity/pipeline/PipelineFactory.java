/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.pipeline;

/**
 * A factory interface for creating pipelines. This class allows to
 * implement different {@link com.continuuity.pipeline.Pipeline} based on external constraints.
 */
public interface PipelineFactory {
  /**
   * @return A {@link com.continuuity.pipeline.Pipeline} created by the factory.
   */
  <T> Pipeline<T> getPipeline();
}
