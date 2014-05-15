package com.continuuity.internal.pipeline;

import com.continuuity.pipeline.Pipeline;
import com.continuuity.pipeline.PipelineFactory;

/**
 * A factory for providing asynchronous pipeline.
 */
public class AsynchronousPipelineFactory implements PipelineFactory {

  /**
   * @return A asynchronous pipeline.
   */
  @Override
  public <T> Pipeline<T> getPipeline() {
    return new AsynchronousPipeline<T>();
  }
}
