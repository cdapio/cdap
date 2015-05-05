/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.template.etl.api.realtime;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.template.etl.api.PipelineConfigurable;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.StageLifecycle;

/**
 * Realtime Sink.
 *
 * @param <I> Type of object that sink operates on
 */
@Beta
public abstract class RealtimeSink<I> implements PipelineConfigurable, StageLifecycle<RealtimeContext> {

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Initialize the Sink. This method is guaranteed to be invoked before any calls to {@link RealtimeSink#write}
   * are made.
   *
   * @param context {@link RealtimeContext}
   */
  @Override
  public void initialize(RealtimeContext context) throws Exception {
    // no-op
  }

  /**
   * Write the given objects.
   *
   * @param objects {@link Iterable} of I to write
   * @param dataWriter {@link DataWriter} write to CDAP streams and datasets
   * @return the number of items written. Used by metrics to report how many records written by the sink
   * @throws Exception if there was some exception writing the objects
   */
  public abstract int write(Iterable<I> objects, DataWriter dataWriter) throws Exception;

  /**
   * Destroy the Sink.
   */
  @Override
  public void destroy() {
    //no-op
  }
}
