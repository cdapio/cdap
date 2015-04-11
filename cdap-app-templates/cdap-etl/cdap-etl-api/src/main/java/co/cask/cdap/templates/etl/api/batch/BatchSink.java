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
 * @param <KEY> Batch Output Key class
 * @param <VALUE> Batch Output Value class
 */
public abstract class BatchSink<KEY, VALUE> {

  private final Type keyType = new TypeToken<KEY>(getClass()) { }.getType();
  private final Type valueType = new TypeToken<VALUE>(getClass()) { }.getType();

  /**
   * Get the Type of {@link KEY}.
   *
   * @return {@link Type}
   */
  public final Type getKeyType() {
    return keyType;
  }

  /**
   * Get the Type of {@link VALUE}.
   *
   * @return {@link Type}
   */
  public final Type getValueType() {
    return valueType;
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
}
