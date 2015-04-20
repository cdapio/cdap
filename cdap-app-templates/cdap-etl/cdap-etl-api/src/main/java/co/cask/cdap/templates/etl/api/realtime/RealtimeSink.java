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

package co.cask.cdap.templates.etl.api.realtime;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;

/**
 * Realtime Sink.
 *
 * @param <I> Object sink operates on
 */
public abstract class RealtimeSink<I> implements ProgramLifecycle<RealtimeContext>, EndPointStage {

  @Override
  public void configure(StageConfigurer configurer) {
    // no-op
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Initialize the Sink.
   *
   * @param context {@link RealtimeContext}
   */
  @Override
  public void initialize(RealtimeContext context) throws Exception {
    // no-op
  }

  /**
   * Write the given object.
   *
   * @param object {@link Iterable} of T
   * @param dataWriter {@link DataWriter} write to CDAP streams and datasets
   * @throws Exception if there was some exception writing the object
   */
  public abstract void write(Iterable<I> object, DataWriter dataWriter) throws Exception;

  @Override
  public void destroy() {
    //no-op
  }
}
