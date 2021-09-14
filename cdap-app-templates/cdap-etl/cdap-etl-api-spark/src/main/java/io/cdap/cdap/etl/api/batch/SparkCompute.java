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

package io.cdap.cdap.etl.api.batch;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageLifecycle;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * Spark Compute stage.
 *
 * @param <IN> Type of input object
 * @param <OUT> Type of output object
 */
@Beta
public abstract class SparkCompute<IN, OUT>
  implements SubmitterLifecycle<SparkPluginContext>, StageLifecycle<SparkExecutionPluginContext>, PipelineConfigurable,
  Serializable {
  public static final String PLUGIN_TYPE = "sparkcompute";

  private static final long serialVersionUID = -8156450728774382658L;


  /**
   * Configure a pipeline.
   *
   * @param pipelineConfigurer the configurer used to add required datasets and streams
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    // no-op
  }

  @Override
  public void onRunFinish(boolean succeeded, SparkPluginContext context) {
    // no-op
  }

  /**
   * Initialize the plugin. Will be called before any calls to {@link #transform(SparkExecutionPluginContext, JavaRDD)}
   * are made.
   *
   * @param context {@link SparkExecutionPluginContext} for this job
   * @throws Exception if there is an error initializing
   */
  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    // no-op
  }

  @Override
  public void destroy() {
    // no-op
  }

  /**
   * Transform the input and return the output to be sent to the next stage in the pipeline.
   *
   * @param context {@link SparkExecutionPluginContext} for this job
   * @param input input data to be transformed
   * @throws Exception if there is an error during this method invocation
   */
  public abstract JavaRDD<OUT> transform(SparkExecutionPluginContext context, JavaRDD<IN> input) throws Exception;

}
