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

import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;

/**
 * Batch Sink forms the last stage of a Batch ETL Pipeline.
 *
 * @param <IN> the type of input object to the sink
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class BatchSink<IN, KEY_OUT, VAL_OUT> {

  private final Type inputType = new TypeToken<IN>(getClass()) { }.getType();

  /**
   * Get the Type of {@link IN}.
   *
   * @return {@link Type} of input object
   */
  public final Type getInputType() {
    return inputType;
  }

  /**
   * Configure the Sink.
   *
   * @param configurer {@link StageConfigurer}
   */
  public void configure(StageConfigurer configurer) {
    // no-op
  }

  /**
   * Configure an ETL pipeline, adding datasets and streams that the source needs.
   *
   * @param stageConfig the configuration for the source
   * @param pipelineConfigurer the configurer used to add required datasets and streams
   */
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Prepare the Batch Job. Used to configure the Hadoop Job before starting the Batch Job.
   *
   * @param context {@link BatchSinkContext}
   */
  public abstract void prepareJob(BatchSinkContext context);

  /**
   * Initialize the sink. This is called once each time the Hadoop Job runs, before any
   * calls to {@link #write} are made.
   *
   * @param stageConfig the configuration for the stage.
   */
  public void initialize(ETLStage stageConfig) {
    // no-op
  }

  /**
   * Write the given input as a key value pair. By default the given input as the key and null as the value.
   *
   * @param input the value to write as a key value pair
   */
  public void write(IN input, SinkWriter<KEY_OUT, VAL_OUT> writer) throws Exception {
    writer.write((KEY_OUT) input, null);
  }
}
