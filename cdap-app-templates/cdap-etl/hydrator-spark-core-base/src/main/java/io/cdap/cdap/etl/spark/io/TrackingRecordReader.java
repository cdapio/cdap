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

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * A {@link RecordReader} that delegate all operations to another {@link RecordReader}, with counter metrics
 * sending to Spark metrics.
 *
 * @param <K> type of key to read
 * @param <V> type of value to read
 */
public class TrackingRecordReader<K, V> extends RecordReader<K, V> {

  private final RecordReader<K, V> delegate;

  public TrackingRecordReader(RecordReader<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    delegate.initialize(split, new TrackingTaskAttemptContext(context));
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return delegate.nextKeyValue();
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return delegate.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return delegate.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return delegate.getProgress();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
