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

package co.cask.cdap.internal.app.runtime.batch.dataset.partitioned;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A RecordWriter that can concurrently to multiple partitions of a PartitionedFileSet.
 */
final class MultiWriter<K, V> extends DynamicPartitionerWriterWrapper<K, V> {

  // a cache storing the record writers for different output files.
  private Map<PartitionKey, RecordWriter<K, V>> recordWriters = new HashMap<>();
  private Map<PartitionKey, TaskAttemptContext> contexts = new HashMap<>();

  MultiWriter(TaskAttemptContext job) {
    super(job);
  }

  public void write(K key, V value) throws IOException, InterruptedException {
    PartitionKey partitionKey = dynamicPartitioner.getPartitionKey(key, value);
    RecordWriter<K, V> rw = this.recordWriters.get(partitionKey);
    if (rw == null) {
      // if we don't have the record writer yet for the final path, create one and add it to the cache
      TaskAttemptContext taskAttemptContext = getKeySpecificContext(partitionKey);
      rw = getBaseRecordWriter(taskAttemptContext);
      this.recordWriters.put(partitionKey, rw);
      this.contexts.put(partitionKey, taskAttemptContext);
    }
    rw.write(key, value);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      Map<PartitionKey, RecordWriter<?, ?>> recordWriters = new HashMap<>();
      recordWriters.putAll(this.recordWriters);
      MultipleOutputs.closeRecordWriters(recordWriters, contexts);
      taskContext.flushOperations();
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      dynamicPartitioner.destroy();
    }
  }
}
