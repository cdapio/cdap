/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.pipeline;

import com.continuuity.pipeline.Context;
import com.continuuity.pipeline.Pipeline;
import com.continuuity.pipeline.Stage;

/**
 * Concrete implementation of synchronous {@link Pipeline}.
 * <p>
 *   Input from each {@link Stage} is passed to next {@link Stage}.
 *   Before processing to next {@link Stage}, we wait for results
 *   to be available.
 * </p>
 */
final class SynchronousPipeline extends AbstractPipeline {
  /**
   * Executes a pipeline in synchronous mode.
   * <p>
   *   Waits for the results of previous to be available to move to next
   *   stage of processing.
   * </p>
   * @param o argument to run the pipeline.
   */
  @Override
  public void execute(Object o) throws Exception {
    Object input = o;
    for(Stage stage : getStages()) {
      Context ctx = new StageContext(input);
      stage.process(ctx);
      input = ctx.getDownStream();
    }
    // After the processing has completed, set the result to be retrieved.
    setResult(input);
  }

}
