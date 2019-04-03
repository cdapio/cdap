/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * An InputFormat that returns no data.
 * @param <K> the key class
 * @param <V> the value class
 */
public class EmptyInputFormat<K, V> extends InputFormat<K, V> {

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return Collections.emptyList();
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new RecordReader<K, V>() {
      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) {
        // do nothing
      }

      @Override
      public boolean nextKeyValue() {
        return false;
      }

      @Override
      public K getCurrentKey() {
        return null;
      }

      @Override
      public V getCurrentValue() {
        return null;
      }

      @Override
      public float getProgress() {
        return 1.0F;
      }

      @Override
      public void close() {
        // nothing to do
      }
    };
  }
}
