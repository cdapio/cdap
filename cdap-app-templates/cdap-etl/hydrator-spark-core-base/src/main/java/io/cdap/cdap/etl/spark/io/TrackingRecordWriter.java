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

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * A {@link RecordWriter} that delegate all operations to another {@link RecordWriter}, with counter metrics
 * sending to Spark metrics.
 *
 * @param <K> type of key to write
 * @param <V> type of value to write
 */
public class TrackingRecordWriter<K, V> extends RecordWriter<K, V> {

  private final RecordWriter<K, V> delegate;

  public TrackingRecordWriter(RecordWriter<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void write(K key, V value) throws IOException, InterruptedException {
    delegate.write(key, value);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    delegate.close(new TrackingTaskAttemptContext(context));
  }
}
