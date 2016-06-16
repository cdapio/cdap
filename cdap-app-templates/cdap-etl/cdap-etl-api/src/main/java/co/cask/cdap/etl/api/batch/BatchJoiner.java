
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
import co.cask.cdap.etl.api.Joiner;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageLifecycle;

import java.util.List;

/**
 * A {@link Joiner} used for batch programs.
 * As it is used in batch programs, a BatchJoiner must be parameterized
 * with supported join key and input record classes. Join keys and input records can be a
 * byte[], Boolean, Integer, Long, Float, Double, String, or StructuredRecord.
 * If the join key is not one of those types and is being used in mapreduce,
 * it must implement Hadoop's org.apache.hadoop.io.WritableComparable interface.
 * If the input record is not one of those types and is being used in mapreduce,
 * it must implement Hadoop's org.apache.hadoop.io.Writable interface.
 * If the joiner is being used in spark, both the join key and input record must implement the
 * {@link java.io.Serializable} interface.
 *
 *
 * @param <JOIN_KEY> type of join key. Must be a supported type
 * @param <INPUT_RECORD> type of input record. Must be a supported type
 * @param <OUT> type of output object
 */
@Beta
public abstract class BatchJoiner<JOIN_KEY, INPUT_RECORD, OUT> extends BatchConfigurable<BatchJoinerContext>
  implements Joiner<JOIN_KEY, INPUT_RECORD, OUT>, PipelineConfigurable, StageLifecycle<BatchJoinerRuntimeContext> {
  public static final String PLUGIN_TYPE = "batchjoiner";

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
   * This is where you would set things like the number of partitions to use when joining, and setting the
   * join key class if they are not known at compile time.
   *
   * @param context batch execution context
   * @throws Exception
   */
  @Override
  public void prepareRun(BatchJoinerContext context) throws Exception {
    //no-op
  }

  /**
   * Initialize the Batch Joiner. Executed inside the Batch Run. This method is guaranteed to be invoked
   * before any calls to {@link #joinOn(String, Object)} and {@link #merge(Object, List)} are made.
   *
   * @param context runtime context for joiner which exposes input schemas and output schema for joiner
   * @throws Exception if there is any error during initialization
   */
  @Override
  public void initialize(BatchJoinerRuntimeContext context) throws Exception {
    //no-op
  }

  /**
   * Destroy the Batch Joiner. Executed at the end of the Batch Run.
   */
  @Override
  public void destroy() {
    //no-op
  }
}
