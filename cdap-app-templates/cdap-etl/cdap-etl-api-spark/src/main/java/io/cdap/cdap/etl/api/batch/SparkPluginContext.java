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

package co.cask.cdap.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.etl.api.PipelineConfigurer;
import org.apache.spark.SparkConf;

/**
 * Context passed to spark plugin types during prepare run phase.
 */
@Beta
public interface SparkPluginContext extends BatchContext {

  /**
   * Sets a {@link SparkConf} to be used for the Spark execution.
   *
   * If your configuration will not change between pipeline runs,
   * use {@link PipelineConfigurer#setPipelineProperties}
   * instead. This method should only be used when you need different
   * configuration settings for each run.
   *
   * Due to limitations in Spark Streaming, this method cannot be used
   * in realtime data pipelines. Calling this method will throw an
   * {@link UnsupportedOperationException} in realtime pipelines.
   */
  void setSparkConf(SparkConf sparkConf);
}
