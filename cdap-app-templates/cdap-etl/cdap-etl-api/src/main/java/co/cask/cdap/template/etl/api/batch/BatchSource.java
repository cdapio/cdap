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

package co.cask.cdap.template.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.EndPointStage;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.Transformation;

/**
 * Batch Source forms the first stage of a Batch ETL Pipeline. Along with configuring the Batch job, it
 * also transforms the key value pairs provided by the Batch job into a single output type to be consumed by
 * subsequent transforms. By default, the value of the key value pair will be output.
 *
 * {@link BatchSource#initialize}, {@link BatchSource#transform} and {@link BatchSource#destroy} methods are called
 * inside the Batch Job while {@link BatchSource#prepareJob} and {@link BatchSource#teardownJob} methods are called
 * on the client side, which launches the BatchJob, before the BatchJob starts and after it completes respectively.
 *
 * @param <KEY_IN> the type of input key from the Batch job
 * @param <VAL_IN> the type of input value from the Batch job
 * @param <OUT> the type of output for the source
 */
@Beta
public abstract class BatchSource<KEY_IN, VAL_IN, OUT>
  implements EndPointStage, Transformation<KeyValue<KEY_IN, VAL_IN>, OUT, BatchSourceContext> {

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Prepare the Batch Job. Used to configure the Hadoop Job before starting the Batch Job.
   *
   * @param context {@link BatchSourceContext}
   * @throws Exception if there's an error during this method invocation
   */
  public abstract void prepareJob(BatchSourceContext context) throws Exception;

  @Override
  public void initialize(BatchSourceContext context) throws Exception {
    // no-op
  }

  /**
   * Transform the {@link KeyValue} pair produced by the input, configured in the Job,
   * to a single object and emit it to the next stage. By default it emits the value.
   *
   * @param input the input to transform
   * @param emitter {@link Emitter} to emit data to the next stage
   * @throws Exception if there's an error during this method invocation
   */
  @Override
  public void transform(KeyValue<KEY_IN, VAL_IN> input, Emitter<OUT> emitter) throws Exception {
    emitter.emit((OUT) input.getValue());
  }

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
  public void teardownJob(boolean succeeded, BatchSourceContext context) throws Exception {
    // no-op
  }
}
