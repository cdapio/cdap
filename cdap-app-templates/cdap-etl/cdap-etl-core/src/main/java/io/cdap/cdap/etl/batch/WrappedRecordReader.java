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

package io.cdap.cdap.etl.batch;

import io.cdap.cdap.api.exception.WrappedException;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class WrappedRecordReader<K, V> extends RecordReader<K, V> {
  private  final RecordReader<K, V> recordReader;
  private final String stageName;

  public WrappedRecordReader(RecordReader<K, V> recordReader, String stageName) {
    this.recordReader = recordReader;
    this.stageName = stageName;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    try {
      recordReader.initialize(inputSplit, taskAttemptContext);
    } catch (Exception e) {
      if (stageName != null) {
        throw new WrappedException(e, stageName);
      }
      throw e;
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    try {
      return recordReader.nextKeyValue();
    } catch (Exception e) {
      if (stageName != null) {
        throw new WrappedException(e, stageName);
      }
      throw e;
    }
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    try {
      return recordReader.getCurrentKey();
    } catch (Exception e) {
      if (stageName != null) {
        throw new WrappedException(e, stageName);
      }
      throw e;
    }
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    try {
      return recordReader.getCurrentValue();
    } catch (Exception e) {
      if (stageName != null) {
        throw new WrappedException(e, stageName);
      }
      throw e;
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    try {
      return recordReader.getProgress();
    } catch (Exception e) {
      if (stageName != null) {
        throw new WrappedException(e, stageName);
      }
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      recordReader.close();
    } catch (Exception e) {
      if (stageName != null) {
        throw new WrappedException(e, stageName);
      }
      throw e;
    }
  }
}
