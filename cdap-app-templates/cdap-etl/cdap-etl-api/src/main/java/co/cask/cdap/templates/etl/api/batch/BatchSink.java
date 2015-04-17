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
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;

/**
 * Batch Sink forms the last stage of a Batch ETL Pipeline. In addition to configuring the Batch job, the sink
 * also transforms a single input object into a key value pair that the Batch job will output. By default, the input
 * object is used as the key and null is used as the value.
 *
 * @param <IN> the type of input object to the sink
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class BatchSink<IN, KEY_OUT, VAL_OUT>
  implements EndPointStage, Transform<IN, KeyValue<KEY_OUT, VAL_OUT>> {

  private final Type inputType = new TypeToken<IN>(getClass()) { }.getType();

  /**
   * Get the Type of {@link IN}.
   *
   * @return {@link Type} of input object
   */
  public final Type getInputType() {
    return inputType;
  }

  @Override
  public void configure(StageConfigurer configurer) {
    // no-op
  }

  @Override
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
   * calls to {@link #transform(Object, Emitter)} are made.
   *
   * @param stageConfig the configuration for the stage.
   */
  public void initialize(ETLStage stageConfig) {
    // no-op
  }

  @Override
  public void transform(IN input, Emitter<KeyValue<KEY_OUT, VAL_OUT>> emitter) throws Exception {
    emitter.emit(new KeyValue<KEY_OUT, VAL_OUT>((KEY_OUT) input, null));
  }
}
