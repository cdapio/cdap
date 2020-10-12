/*
 * Copyright © 2020 Cask Data, Inc.
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

import io.cdap.cdap.etl.batch.DelegatingOutputFormat;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * An {@link OutputFormat} that enables metrics tracking through {@link TaskAttemptContext} counters to Spark metrics.
 *
 * @param <K> type of key to write
 * @param <V> type of value to write
 */
public class TrackingOutputFormat<K, V> extends DelegatingOutputFormat<K, V> {

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    OutputFormat<K, V> delegate = getDelegate(context.getConfiguration());

    // Spark already emitting bytes written metrics for file base output, hence we don't want to double count
    if (delegate instanceof FileOutputFormat) {
      return delegate.getRecordWriter(context);
    }

    return new TrackingRecordWriter<>(delegate.getRecordWriter(new TrackingTaskAttemptContext(context)));
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    OutputFormat<K, V> delegate = getDelegate(context.getConfiguration());

    // Spark already emitting bytes written metrics for file base output, hence we don't want to double count
    if (delegate instanceof FileOutputFormat) {
      return delegate.getOutputCommitter(context);
    }

    return new TrackingOutputCommitter(delegate.getOutputCommitter(new TrackingTaskAttemptContext(context)));
  }
}
