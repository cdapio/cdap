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

package co.cask.cdap.templates.etl.api;

import co.cask.cdap.api.RuntimeContext;

/**
 * Provides lifecycle methods for ETL Pipeline Stages.
 *
 * @param <T> type of the stage runtime context
 */
public interface StageLifecycle<T extends RuntimeContext> {

  /**
   * Initializes a Stage.
   *  <p>
   *    This method will be called only once per {@link StageLifecycle} instance.
   *  </p>
   *
   *  @param context An instance of {@link RuntimeContext}
   *  @throws Exception If there is any error during initialization.
   */
  void initialize(T context) throws Exception;

  /**
   * Destroy the Pipeline Stage.
   */
  void destroy();
}
