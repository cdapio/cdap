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

package co.cask.cdap.etl.api.streaming;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;

/**
 * Source for Spark Streaming pipelines.
 *
 * @param <T> type of object contained in the stream
 */
@Beta
public abstract class StreamingSource<T> implements PipelineConfigurable, Serializable {

  public static final String PLUGIN_TYPE = "streamingsource";

  private static final long serialVersionUID = -7949508317034247623L;

  /**
   * Get the stream to read from.
   *
   * @param context the streaming context for this stage of the pipeline
   * @return the stream to read from.
   */
  public abstract JavaDStream<T> getStream(StreamingContext context) throws Exception;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    // no-op
  }
}
