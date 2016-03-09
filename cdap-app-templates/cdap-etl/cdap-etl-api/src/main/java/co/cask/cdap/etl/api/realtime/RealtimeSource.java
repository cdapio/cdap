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

package co.cask.cdap.etl.api.realtime;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageLifecycle;

import javax.annotation.Nullable;

/**
 * Realtime Source.
 *
 * @param <T> Type of object that the source emits
 */
@Beta
public abstract class RealtimeSource<T> implements PipelineConfigurable, StageLifecycle<RealtimeContext> {
  public static final String PLUGIN_TYPE = "realtimesource";

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Initialize the Source. This method is guaranteed to be invoked before any calls to {@link RealtimeSource#poll}
   * are made.

   * @param context {@link RealtimeContext}
   */
  @Override
  public void initialize(RealtimeContext context) throws Exception {
    // no-op
  }

  /**
   * Poll for new data.
   *
   * @param writer {@link Emitter}
   * @param currentState {@link SourceState} current state of the source
   * @throws Exception if there's an error during this method invocation
   *
   * @return {@link SourceState} state of the source after poll, will be persisted when all data from poll are processed
   */
  @Nullable
  public abstract SourceState poll(Emitter<T> writer, SourceState currentState) throws Exception;

  /**
   * Destroy the Source.
   */
  @Override
  public void destroy() {
    // no-op
  }
}
