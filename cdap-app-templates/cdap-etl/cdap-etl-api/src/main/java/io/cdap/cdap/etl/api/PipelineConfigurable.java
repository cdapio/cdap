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

package io.cdap.cdap.etl.api;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.etl.api.validation.ValidationException;

/**
 * Allows the stage to configure pipeline.
 */
@Beta
public interface PipelineConfigurable {

  /**
   * Configure an ETL pipeline, registering datasets and plugins that the stage needs.
   * Validation should be performed in this method, throwing a {@link ValidationException} if there are any
   * invalid config properties, or if the input or output schema is not compatible. Output schema should also be set.
   * This method is called many times during the lifecycle of a pipeline so it should not generate any side effects.
   *
   * When the pipeline is being constructed, this is called in order to validate the pipeline and
   * propagate schema. Any datasets registered at this time will be ignored. Config properties that contain macros
   * will not have been evaluated yet.
   *
   * When the pipeline is deployed, this is called in order to validate the pipeline and create any datasets that
   * are registered. Config properties that contain macros will not have been evaluated yet.
   *
   * @param pipelineConfigurer the configurer used to register required datasets and plugins
   * @throws ValidationException if the pipeline stage is invalid
   */
  void configurePipeline(PipelineConfigurer pipelineConfigurer);
}
