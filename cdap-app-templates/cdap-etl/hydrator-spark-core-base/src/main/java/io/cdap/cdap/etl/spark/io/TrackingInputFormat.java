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

import io.cdap.cdap.etl.batch.DelegatingInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * An {@link InputFormat} that enables metrics tracking through {@link TaskAttemptContext} counters to Spark metrics.
 *
 * @param <K> type of key to read
 * @param <V> type of value to read
 */
public class TrackingInputFormat<K, V> extends DelegatingInputFormat<K, V> {

  public static final String DELEGATE_CLASS_NAME = "io.cdap.pipeline.tracking.input.classname";

  @Override
  protected String getDelegateClassNameKey() {
    return DELEGATE_CLASS_NAME;
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
                                               TaskAttemptContext context) throws IOException, InterruptedException {
    // Spark already tracking metrics for file based input, hence we don't need to track again.
    if (split instanceof FileSplit || split instanceof CombineFileSplit) {
      return super.createRecordReader(split, context);
    }

    return new TrackingRecordReader<>(super.createRecordReader(split, new TrackingTaskAttemptContext(context)));
  }
}
