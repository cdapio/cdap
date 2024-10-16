/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.io;

import io.cdap.cdap.etl.api.exception.ErrorPhase;
import io.cdap.cdap.etl.batch.DelegatingOutputFormat;
import io.cdap.cdap.etl.common.ErrorDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * An {@link OutputFormat} that enables metrics tracking through {@link TaskAttemptContext}
 * counters to Spark metrics.
 *
 * @param <K> type of key to write
 * @param <V> type of value to write
 */
public class StageTrackingOutputFormat<K, V> extends DelegatingOutputFormat<K, V> {
  public static final String WRAPPED_STAGE_NAME = "io.cdap.pipeline.wrapped.stage.name";

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) {
    OutputFormat<K, V> delegate = getDelegate(context.getConfiguration());
    Configuration conf = context.getConfiguration();
    try {
      // Spark already emitting bytes written metrics for file base output,
      // hence we don't want to double count
      if (delegate instanceof FileOutputFormat) {
        return new StageTrackingRecordWriter<>(delegate.getRecordWriter(context),
          getStageName(conf), ErrorDetails.getErrorDetailsProvider(conf));
      }

      return new StageTrackingRecordWriter<>(
        new TrackingRecordWriter(delegate.getRecordWriter(new TrackingTaskAttemptContext(context))),
        getStageName(conf), ErrorDetails.getErrorDetailsProvider(conf));
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, getStageName(conf),
        ErrorDetails.getErrorDetailsProvider(conf), ErrorPhase.WRITING);
    }
  }

  @Override
  public void checkOutputSpecs(JobContext context) {
    Configuration conf = context.getConfiguration();
    try {
      getDelegate(conf).checkOutputSpecs(context);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, getStageName(conf),
        ErrorDetails.getErrorDetailsProvider(conf), ErrorPhase.VALIDATING_OUTPUT_SPECS);
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    OutputFormat<K, V> delegate = getDelegate(context.getConfiguration());
    Configuration conf = context.getConfiguration();
    try {
      // Spark already emitting bytes written metrics for file base output,
      // hence we don't want to double count
      if (delegate instanceof FileOutputFormat) {
        return new StageTrackingOutputCommitter(delegate.getOutputCommitter(context),
          getStageName(conf), ErrorDetails.getErrorDetailsProvider(conf));
      }

      return new StageTrackingOutputCommitter(new TrackingOutputCommitter(
        delegate.getOutputCommitter(new TrackingTaskAttemptContext(context))),
        getStageName(conf), ErrorDetails.getErrorDetailsProvider(conf));
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, getStageName(conf),
        ErrorDetails.getErrorDetailsProvider(conf), ErrorPhase.COMMITTING);
    }
  }

  private String getStageName(Configuration conf) {
    return conf.get(WRAPPED_STAGE_NAME);
  }
}
