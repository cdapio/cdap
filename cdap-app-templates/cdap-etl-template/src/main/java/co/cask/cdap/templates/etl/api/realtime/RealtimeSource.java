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

import co.cask.cdap.templates.etl.api.SourceConfigurer;
import co.cask.cdap.templates.etl.api.SourceContext;
import co.cask.cdap.templates.etl.api.StageLifecycle;

import javax.annotation.Nullable;

/**
 * Realtime Source.
 */
public interface RealtimeSource<O> extends StageLifecycle {

  /**
   * Configure the Source.
   * @param configurer {@link SourceConfigurer}
   */
  void configure(SourceConfigurer configurer);

  /**
   * Initialize the Source.
   * @param context
   */
  void initialize(SourceContext context);

  /**
   * Invoked when source is suspended
   */
  void onSuspend();

  /**
   * Resume/reconfigure from the state of suspension.
   * @param oldInstance old instance count
   * @param newInstance new instance count
   */
  void onResume(int oldInstance, int newInstance);

  /**
   * Poll for new data.
   *
   * @param writer {@link Emitter}
   * @param currentState {@link SourceState} current state of the Source
   * @return {@link SourceState} state of the source after poll, will be persisted when all data from poll are processed
   */
  @Nullable
  SourceState poll(Emitter<O> writer, @Nullable SourceState currentState);
}
