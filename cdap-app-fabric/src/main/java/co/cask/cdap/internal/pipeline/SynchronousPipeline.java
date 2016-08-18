/*
 * Copyright © 2014-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.pipeline;

import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.Stage;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete implementation of synchronous {@link Pipeline}.
 * <p>
 * Input from each {@link Stage} is passed to next {@link Stage}.
 * Before processing to next {@link Stage}, we wait for results
 * to be available.
 * </p>
 *
 * @param <T> Type of object produced by this pipeline.
 */
public final class SynchronousPipeline<T> extends AbstractPipeline<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SynchronousPipeline.class);

  /**
   * Executes a pipeline in synchronous mode.
   * <p>
   * Waits for the results of previous to be available to move to next
   * stage of processing.
   * </p>
   *
   * @param o argument to run the pipeline.
   */
  @SuppressWarnings("unchecked")
  @Override
  public ListenableFuture<T> execute(Object o) {
    StageContext ctx = new StageContext(o);
    try {
      for (Stage stage : getStages()) {
        stage.process(ctx);
        // Output of previous stage is input to next stage
        ctx = StageContext.next(ctx);
      }
      return Futures.immediateFuture((T) ctx.getUpStream());
    } catch (Throwable th) {
      return Futures.immediateFailedFuture(th);
    } finally {
      Stage finalStage = getFinalStage();
      if (finalStage != null) {
        try {
          finalStage.process(ctx);
        } catch (Throwable t) {
          LOG.warn("Exception thrown when executing final stage {}", finalStage, t);
        }
      }
    }
  }
}
