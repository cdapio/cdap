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
import co.cask.cdap.etl.api.Aggregator;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageLifecycle;

import java.util.Iterator;

/**
 * An {@link Aggregator} used in batch programs.
 * As it is used in batch programs, a BatchAggregator must be parameterized
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
 * @param <OUT> output object type
 */
@Beta
public abstract class BatchAggregator<GROUP_KEY, GROUP_VALUE, OUT> extends BatchConfigurable<BatchAggregatorContext>
  implements Aggregator<GROUP_KEY, GROUP_VALUE, OUT>, PipelineConfigurable, StageLifecycle<BatchRuntimeContext> {
  public static final String PLUGIN_TYPE = "batchaggregator";

  /**
   * Configure the pipeline. This is run once when the pipeline is being published.
   * This is where you perform any static logic, like creating required datasets, performing schema validation,
   * setting output schema, and things of that nature.
   *
   * @param pipelineConfigurer the configurer used to add required datasets and streams
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
   * Initialize the Batch Aggregator. Executed inside the Batch Run. This method is guaranteed to be invoked
   * before any calls to {@link #groupBy(Object, Emitter)} and {@link #aggregate(Object, Iterator, Emitter)} are made.
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
