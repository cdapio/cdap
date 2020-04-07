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

package io.cdap.cdap.etl.api.streaming;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;
import java.util.List;

/**
 * Source for Spark Streaming pipelines.
 *
 * @param <T> type of object contained in the stream
 */
@Beta
public abstract class StreamingSource<T> implements PipelineConfigurable,
  SubmitterLifecycle<StreamingSourceContext>, Serializable {

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
  public void prepareRun(StreamingSourceContext context) throws Exception {
    // no-op
  }

  @Override
  public void onRunFinish(boolean succeeded, StreamingSourceContext context) {
    // no-op
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Record field-level lineage for streaming source plugins. This method should be called from prepareRun of any source
   * plugin.
   * @param context Streaming from prepareRun
   * @param outputName name of output dataset
   * @param tableSchema schema of fields
   * @param fieldNames list of field names
   * @param operationName name of the operation
   * @param description operation description; complete sentences preferred
   */
  protected void recordLineage(StreamingSourceContext context, String outputName, Schema tableSchema,
      List<String> fieldNames, String operationName, String description) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
    lineageRecorder.createExternalDataset(tableSchema);
    if (!fieldNames.isEmpty()) {
      lineageRecorder.recordRead(operationName, description, fieldNames);
    }
  }

  /**
   * Get number of required executors for the streaming source. This needs to be overriden in case
   * {@link JavaDStream} returned in {@link StreamingSource#getStream(StreamingContext)} is a union of multiple streams
   *
   * @return number of executors required for the streaming source, defaults to 1
   */
  public int getRequiredExecutors() {
    return 1;
  }
}
