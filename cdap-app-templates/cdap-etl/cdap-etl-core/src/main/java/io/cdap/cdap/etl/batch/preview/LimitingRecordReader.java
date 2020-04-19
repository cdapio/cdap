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

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

/**
 * A record reader that limits the number of records read.
 *
 * @param <K> type of key to read
 * @param <V> type of value to read
 */
public class LimitingRecordReader<K, V> extends RecordReader<K, V> {

  private final InputFormat<K, V> delegateFormat;
  private TaskAttemptContext context;
  private LimitingInputSplit inputSplit;
  private int perSplitLimit;
  private int numRead;
  private Iterator<InputSplit> splitIterator;
  private RecordReader<K, V> currentReader;

  LimitingRecordReader(InputFormat<K, V> delegateFormat) {
    this.delegateFormat = delegateFormat;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    if (!(split instanceof LimitingInputSplit)) {
      throw new IOException("Expected input split class " + LimitingInputSplit.class.getName()
                              + ", but got " + split.getClass().getName());
    }
    this.context = context;
    this.inputSplit = (LimitingInputSplit) split;
    this.splitIterator = inputSplit.getInputSplits().iterator();

    // Round up the per split limit so that each reader at most is opened once
    int numberOfSplits = inputSplit.getInputSplits().size();
    int perSplitLimit = (inputSplit.getRecordLimit() + numberOfSplits - 1) / numberOfSplits;
    this.perSplitLimit = Math.max(1, perSplitLimit);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (numRead >= inputSplit.getRecordLimit()) {
      return false;
    }

    // Find the next reader when hitting the per split boundary or the current reader is exhausted
    boolean answer = (numRead % perSplitLimit != 0) && currentReader.nextKeyValue();
    while (!answer && splitIterator.hasNext()) {
      if (currentReader != null) {
        currentReader.close();
      }
      InputSplit split = splitIterator.next();
      currentReader = delegateFormat.createRecordReader(split, context);
      currentReader.initialize(split, context);
      answer = currentReader.nextKeyValue();
    }
    if (answer) {
      numRead++;
    }
    return answer;
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return currentReader.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return currentReader.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return Math.max(currentReader.getProgress(), (float) numRead / inputSplit.getRecordLimit());
  }

  @Override
  public void close() throws IOException {
    if (currentReader != null) {
      currentReader.close();
    }
  }
}
