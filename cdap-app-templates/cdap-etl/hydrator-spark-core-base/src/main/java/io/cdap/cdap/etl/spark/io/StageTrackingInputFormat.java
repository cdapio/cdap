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
import io.cdap.cdap.etl.batch.DelegatingInputFormat;
import io.cdap.cdap.etl.common.ErrorDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.List;

/**
 * An {@link InputFormat} that enables metrics tracking through {@link TaskAttemptContext} counters to Spark metrics.
 *
 * @param <K> type of key to read
 * @param <V> type of value to read
 */
public class StageTrackingInputFormat<K, V> extends DelegatingInputFormat<K, V> {

  public static final String DELEGATE_CLASS_NAME = "io.cdap.pipeline.tracking.input.classname";
  public static final String WRAPPED_STAGE_NAME = "io.cdap.pipeline.wrapped.stage.name";

  @Override
  protected String getDelegateClassNameKey() {
    return DELEGATE_CLASS_NAME;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    Configuration conf = context.getConfiguration();
    try {
      return getDelegate(conf).getSplits(context);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, getStageName(conf),
        ErrorDetails.getErrorDetailsProvider(conf), ErrorPhase.SPLITTING);
    }
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
    TaskAttemptContext context) {
    Configuration conf = context.getConfiguration();
    try {
      // Spark already tracking metrics for file based input, hence we don't need to track again.
      if (split instanceof FileSplit || split instanceof CombineFileSplit) {
        return new StageTrackingRecordReader<>(super.createRecordReader(split, context),
          getStageName(conf), ErrorDetails.getErrorDetailsProvider(conf));
      }

      return new StageTrackingRecordReader<>(new TrackingRecordReader<>(
        super.createRecordReader(split, new TrackingTaskAttemptContext(context))),
        getStageName(conf), ErrorDetails.getErrorDetailsProvider(conf));
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, getStageName(conf),
        ErrorDetails.getErrorDetailsProvider(conf), ErrorPhase.READING);
    }
  }

  private String getStageName(Configuration conf) {
    return conf.get(WRAPPED_STAGE_NAME);
  }
}
