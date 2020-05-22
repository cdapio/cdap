/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.ReducibleAggregator;
import io.cdap.cdap.etl.api.StageLifecycle;
import io.cdap.cdap.etl.api.validation.ValidationException;

/**
 * A {@link ReducibleAggregator} used in batch programs.
 * As it is used in batch programs, a BatchReducibleAggregator must be parameterized
 * with supported group key and value classes. Group keys and values can be a
 * byte[], Boolean, Integer, Long, Float, Double, String, or StructuredRecord.
 * If the group key is not one of those types and is being used in mapreduce,
 * it must implement Hadoop's org.apache.hadoop.io.WritableComparable interface.
 * If the group value is not one of those types and is being used in mapreduce,
 * it must implement Hadoop's org.apache.hadoop.io.Writable interface.
 * If the aggregator is being used in spark, both the group key and value must implement the
 * {@link java.io.Serializable} interface.
 *
 * @param <GROUP_KEY> group key type. Must be a supported type
 * @param <GROUP_VALUE> group value type. Must be a supported type
 * @param <AGG_VALUE> agg value type
 * @param <OUT> output object type
 */
@Beta
public abstract class BatchReducibleAggregator<GROUP_KEY, GROUP_VALUE, AGG_VALUE, OUT>
  extends BatchConfigurable<BatchAggregatorContext>
  implements ReducibleAggregator<GROUP_KEY, GROUP_VALUE, AGG_VALUE, OUT>,
  PipelineConfigurable, StageLifecycle<BatchRuntimeContext> {

  public static final String PLUGIN_TYPE = BatchAggregator.PLUGIN_TYPE;

  /**
   * Configure the pipeline. This is run once when the pipeline is being published.
   * This is where you perform any static logic, like creating required datasets, performing schema validation,
   * setting output schema, and things of that nature.
   *
   * @param pipelineConfigurer the configurer used to add required datasets and streams
   * @throws ValidationException if the given config is invalid
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Prepare a pipeline run. This is run every time before a pipeline runs in order to help set up the run.
   * This is where you would set things like the number of partitions to use when grouping, and setting the
   * group key and value classes if they are not known at compile time.
   *
   * @param context batch execution context
   * @throws Exception
   */
  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    // no-op
  }

  /**
   * Initialize the Batch Reduce Aggregator. Executed inside the Batch Run. This method is guaranteed to be invoked
   * before any calls to {@link #groupBy(Object, Emitter)} and {@link #finalize(Object, Object, Emitter)} are made.
   *
   * @param context {@link BatchRuntimeContext}
   * @throws Exception if there is any error during initialization
   */
  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    // no-op
  }

  /**
   * Destroy the Batch Aggregator. Executed at the end of the Batch Run.
   */
  @Override
  public void destroy() {
    // no-op
  }
}
