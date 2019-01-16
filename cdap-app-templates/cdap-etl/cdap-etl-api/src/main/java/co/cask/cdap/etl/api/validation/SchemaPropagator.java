/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.etl.api.validation;

/**
 * Propagates schema and performs validation.
 *
 * @param <T> type of stage configurer for getting input schema(s) and setting output schema(s)
 */
public interface SchemaPropagator<T> {

  /**
   * Validate that the pipeline stage is properly configured and propagate schema.
   * If the stage is invalid, an {@link InvalidStageException} should be thrown containing details about what
   * is invalid and why.
   *
   * Propagation is performed multiple times before a pipeline is deployed, when it is still being constructed.
   * PluginConfig properties that contain macros will not have actual values at this point. If an InvalidStageException
   * is thrown, all reasons will be displayed to the user.
   *
   * Propagation is also performed once when the pipeline is deployed.
   * PluginConfig properties that contain macros will not have actual values at this point. If an InvalidStageException
   * is thrown, pipeline deployment will fail.
   *
   * Propagation is also performed once at the start of each pipeline run.
   * Macros will be evaluated at this point. If an InvalidStageException is thrown, the pipeline run will fail.
   *
   * @param stageConfigurer configurer used to get the input schema(s) and set the output schema(s)
   * @throws InvalidStageException if validation failed
   */
  default void propagateSchema(T stageConfigurer) {
    // no-op by default
  }
}
