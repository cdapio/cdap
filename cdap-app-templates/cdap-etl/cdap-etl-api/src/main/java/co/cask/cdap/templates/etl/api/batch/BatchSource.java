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
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Transform;

/**
 * Batch Source forms the first stage of a Batch ETL Pipeline. Along with configuring the Batch job, it
 * also transforms the key value pairs provided by the Batch job into a single output type to be consumed by
 * subsequent transforms. By default, the value of the key value pair will be ouput.
 *
 * @param <KEY_IN> the type of input key from the Batch job
 * @param <VAL_IN> the type of input value from the Batch job
 * @param <OUT> the type of output for the source
 */
public abstract class BatchSource<KEY_IN, VAL_IN, OUT>
  implements EndPointStage, Transform<KeyValue<KEY_IN, VAL_IN>, OUT> {

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Prepare the Batch Job. Used to configure the Hadoop Job before starting the Batch Job.
   *
   * @param context {@link BatchSourceContext}
   */
  public abstract void prepareJob(BatchSourceContext context);

  /**
   * Initialize the source. This is called once each time the Hadoop Job runs, before any
   * calls to {@link #transform(KeyValue, Emitter)} are made.
   *
   * @param properties plugin properties
   */
  public void initialize(PluginProperties properties) throws Exception {
    // no-op
  }

  @Override
  public void transform(KeyValue<KEY_IN, VAL_IN> input, Emitter<OUT> emitter) throws Exception {
    emitter.emit((OUT) input.getValue());
  }

  /**
   * Destroy the source. This is called at the end of the Hadoop Job run.
   */
  public void destroy() {
    // no-op
  }
}
