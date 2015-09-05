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

package co.cask.cdap.internal.app.runtime.batch.dataset;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * OutputFormat that allows instantiation of the RecordWriter, but throws {@link UnsupportedOperationException}
 * upon any attempts to write to it.
 *
 * All other operations are no-ops.
 *
 * @param <K> Type of key
 * @param <V> Type of value
 */
public class UnsupportedOutputFormat<K, V> extends OutputFormat<K, V> {

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) {
    return new RecordWriter<K, V>() {
      public void write(K key, V value) {
        throw new UnsupportedOperationException("Writing to output is not supported.");
      }
      public void close(TaskAttemptContext context) { }
    };
  }

  @Override
  public void checkOutputSpecs(JobContext context) { }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new NullOutputFormat<>().getOutputCommitter(context);
  }
}
