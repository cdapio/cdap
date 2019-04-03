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

package co.cask.cdap.internal.app.runtime.batch.dataset.input;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * This is a delegating RecordReader, which delegates the functionality to the
 * underlying record reader in {@link TaggedInputSplit}
 *
 * @param <K> Type of key
 * @param <V> Type of value
 */
public class DelegatingRecordReader<K, V> extends RecordReader<K, V> {
  private final RecordReader<K, V> originalRR;

  /**
   * Constructs the DelegatingRecordReader.
   *
   * @param recordReader the actual RecordReader to delegate operations to.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  public DelegatingRecordReader(RecordReader<K, V> recordReader) throws IOException, InterruptedException {
    this.originalRR = recordReader;
  }

  @Override
  public void close() throws IOException {
    originalRR.close();
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return originalRR.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return originalRR.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return originalRR.getProgress();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    // We need to be sure not to pass the TaggedInputSplit to the underlying RecordReader. Otherwise, it can result
    // in ClassCastExceptions
    InputSplit inputSplit = ((TaggedInputSplit) split).getInputSplit();
    originalRR.initialize(inputSplit, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return originalRR.nextKeyValue();
  }

}
