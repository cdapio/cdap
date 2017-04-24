/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.plugin.PluginConfigurer;

import java.util.Map;

/**
 * Configures an ETL Pipeline. Allows adding datasets and streams, which will be created when a pipeline is created.
 * Using this as a layer between plugins and CDAP's PluginConfigurer in case pipelines need etl specific methods.
 * Similar to {@link PipelineConfigurer} except it exposes {@link MultiInputStageConfigurer}
 */
@Beta
public interface MultiInputPipelineConfigurer extends PluginConfigurer {

  /**
   * Get multi input stage configurer for the pipeline stage
   * @return multi input stage configurer
   */
  MultiInputStageConfigurer getMultiInputStageConfigurer();

  /**
   * @return the engine for this pipeline
   */
  Engine getEngine();

  /**
   * Set pipeline properties that will be applied to each run of the pipeline.
   * Depending on the engine used, the properties will be added to the SparkConf for each run or the mapreduce
   * Configuration for each run.
   *
   * @param properties the properties to set
   */
  void setPipelineProperties(Map<String, String> properties);
}
