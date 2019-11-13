/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.preview;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * A record reader that limits the number of records read.
 *
 * @param <K> type of key to read
 * @param <V> type of value to read
 */
public class LimitingRecordReader<K, V> extends RecordReader<K, V> {
  private final RecordReader<K, V> delegate;
  private final int maxToRead;
  private int numRead;

  public LimitingRecordReader(RecordReader<K, V> delegate, int maxToRead) {
    this.delegate = delegate;
    this.maxToRead = maxToRead;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    delegate.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (numRead > maxToRead) {
      return false;
    }
    boolean answer = delegate.nextKeyValue();
    numRead++;
    return answer;
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
    return Math.max(delegate.getProgress(), (float) numRead / maxToRead);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
