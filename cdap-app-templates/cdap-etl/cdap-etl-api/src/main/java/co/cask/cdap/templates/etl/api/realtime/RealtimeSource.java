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
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;

import javax.annotation.Nullable;

/**
 * Realtime Source.
 *
 * @param <T> Type of object that the source emits
 */
public abstract class RealtimeSource<T> implements ProgramLifecycle<SourceContext> {

  private SourceContext context;

  /**
   * Configure the Source.
   *
   * @param configurer {@link RealtimeConfigurer}
   */
  public void configure(RealtimeConfigurer configurer) {
    // no-op
  }

  /**
   * Configure an ETL pipeline, adding datasets and streams that the source needs.
   *
   * @param stageConfig the configuration for the source
   * @param pipelineConfigurer the configurer used to add required datasets and streams
   */
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Initialize the Source.

   * @param context {@link SourceContext}
   */
  public void initialize(SourceContext context) {
    this.context = context;
  }

  /**
   * Poll for new data.
   *
   * @param writer {@link Emitter}
   * @param currentState {@link SourceState} current state of the source
   * @return {@link SourceState} state of the source after poll, will be persisted when all data from poll are processed
   */
  @Nullable
  public abstract SourceState poll(Emitter<T> writer, SourceState currentState);

  /**
   * Invoked when source is suspended.
   */
  public void onSuspend() {
    // no-op
  }

  /**
   * Resume/reconfigure from the state of suspension.
   */
  public void onResume() {
    // no-op
  }

  @Override
  public void destroy() {
    // no-op
  }

  protected SourceContext getContext() {
    return context;
  }
}
