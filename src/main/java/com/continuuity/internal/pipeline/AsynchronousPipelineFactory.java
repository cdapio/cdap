package com.continuuity.internal.pipeline;

import com.continuuity.pipeline.Pipeline;
import com.continuuity.pipeline.PipelineFactory;

/**
 * A factory for providing asynchronous pipeline.
 */
public class AsynchronousPipelineFactory<T> implements PipelineFactory<T> {

  /**
   * @return A asynchronous pipeline.
   */
  @Override
  public Pipeline<T> getPipeline() {
    return new AsynchronousPipeline<T>();
  }
}
