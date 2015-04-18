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

import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;

/**
 * Batch Source forms the first stage of a Batch ETL Pipeline.
 *
 * @param <KEY_IN> the type of input key from the Batch job
 * @param <VAL_IN> the type of input value from the Batch job
 * @param <OUT> the type of output for the source
 */
public abstract class BatchSource<KEY_IN, VAL_IN, OUT> {

  private final Type outputType = new TypeToken<OUT>(getClass()) { }.getType();

  /**
   * Get the Type of {@link OUT}.
   *
   * @return {@link Type}
   */
  public final Type getOutputType() {
    return outputType;
  }

  /**
   * Configure the Batch Source stage.
   *
   * @param configurer {@link StageConfigurer}
   */
  public void configure(StageConfigurer configurer) {
    // no-op
  }

  /**
   * Configure an ETL pipeline, adding datasets and streams that the source needs. This is called once when
   * an ETL pipeline is created.
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
   * @param context {@link BatchSourceContext}
   */
  public abstract void prepareJob(BatchSourceContext context);

  /**
   * Initialize the source. This is called once each time the Hadoop Job runs, before any
   * calls to {@link #emit} are made.
   *
   * @param stageConfig the configuration for the stage.
   */
  public void initialize(ETLStage stageConfig) {
    // no-op
  }

  /**
   * Emit values for the given key and value returned by the Hadoop Job prepared in {@link #prepareJob}.
   * By default it assumes that the key is ignored and the value is emitted.
   *
   * @param key the key from the Hadoop Job
   * @param val the value from the Hadoop Job
   * @param emitter the emitter to use to emit values
   */
  public void emit(KEY_IN key, VAL_IN val, Emitter<OUT> emitter) {
    emitter.emit((OUT) val);
  }
}
