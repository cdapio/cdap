/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.FeatureFlagsProvider;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.plugin.PluginConfigurer;

import java.util.Collections;
import java.util.Map;

/**
 * Configures an ETL Pipeline. Allows adding datasets and streams, which will be created when a pipeline is created.
 * Using this as a layer between plugins and CDAP's PluginConfigurer in case pipelines need etl specific methods.
 */
@Beta
public interface PipelineConfigurer extends PluginConfigurer, DatasetConfigurer, FeatureFlagsProvider {

  /**
   * Get stage configurer for the pipeline stage
   * @return stage configurer
   */
  StageConfigurer getStageConfigurer();

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
