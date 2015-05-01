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

package co.cask.cdap.templates.etl.api.batch;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.templates.etl.api.Destroyable;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Transformation;

/**
 * Batch Sink forms the last stage of a Batch ETL Pipeline. In addition to configuring the Batch job, the sink
 * also transforms a single input object into a key value pair that the Batch job will output. By default, the input
 * object is used as the key and null is used as the value.
 *
 * {@link BatchSink#initialize}, {@link BatchSink#transform} and {@link BatchSink#destroy} methods are called inside
 * the Batch Job while {@link BatchSink#prepareJob} and {@link BatchSink#teardownJob} methods are called on the client
 * side, which launches the BatchJob, before the BatchJob starts and after it completes respectively.
 *
 * @param <IN> the type of input object to the sink
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class BatchSink<IN, KEY_OUT, VAL_OUT>
  implements EndPointStage, Transformation<IN, KeyValue<KEY_OUT, VAL_OUT>>, Destroyable {

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Prepare the Batch Job. Used to configure the Hadoop Job before starting the Batch Job.
   *
   * @param context {@link BatchSinkContext}
   * @throws Exception if there's an error during this method invocation
   */
  public abstract void prepareJob(BatchSinkContext context) throws Exception;

  /**
   * Initialize the sink. This is called once each time the Batch Job runs, before any
   * calls to {@link #transform(Object, Emitter)} are made.
   *
   * @param context {@link BatchSinkContext}
   */
  public void initialize(BatchSinkContext context) throws Exception {
    // no-op
  }

  /**
   * Transform the input received from previous stage to a {@link KeyValue} pair which can be
   * consumed by the output configured in the Job.
   *
   * @param input the input to transform
   * @param emitter {@link Emitter} to emit data to the next stage
   * @throws Exception if there's an error during this method invocation
   */
  @Override
  public void transform(IN input, Emitter<KeyValue<KEY_OUT, VAL_OUT>> emitter) throws Exception {
    emitter.emit(new KeyValue<KEY_OUT, VAL_OUT>((KEY_OUT) input, null));
  }

  /**
   * Destroy the sink. This is called at the end of the Batch Job run.
   */
  @Override
  public void destroy() {
    // no-op
  }

  /**
   * Get the result of the Batch Job. Used to perform any end of the run logic.
   *
   * @param succeeded defines the result of job execution: true if job succeeded, false otherwise
   * @param context job execution context
   * @throws Exception if there's an error during this method invocation
   */
  public void teardownJob(boolean succeeded, BatchSinkContext context) throws Exception {
    // no-op
  }
}
