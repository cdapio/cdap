/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A RecordWriter that can only write to a single partition of a PartitionedFileSet at any given time, but over time
 * can write to multiple partitions. Once it starts writing to a new partition, the previous partition is closed and
 * the previous partition can not be written to after that point.
 *
 * See {@link PartitionedFileSetArguments#setDynamicPartitionerConcurrency(Map, boolean)}.
 */
final class SingleWriter<K, V> extends DynamicPartitionerWriterWrapper<K, V> {

  // partition keys for which we have already written to and closed the corresponding RecordWriter
  private final Set<PartitionKey> closedKeys = new HashSet<>();

  private PartitionKey currPartitionKey;
  private RecordWriter<K, V> currRecordWriter;
  private TaskAttemptContext currContext;

  SingleWriter(TaskAttemptContext job) {
    super(job);
  }

  @Override
  public void write(K key, V value) throws IOException, InterruptedException {
    PartitionKey partitionKey = dynamicPartitioner.getPartitionKey(key, value);
    if (!partitionKey.equals(currPartitionKey)) {
      // make sure we haven't written to this partition previously
      if (closedKeys.contains(partitionKey)) {
        throw new IllegalStateException(
          String.format("Encountered a partition key for which the writer has already been closed: '%s'.",
                        partitionKey));
      }

      // currPartitionKey can be null for the first key value pair, in which case there's no writer to close
      if (currPartitionKey != null) {
        // close the existing RecordWriter and create a new one for the new PartitionKEy
        currRecordWriter.close(currContext);
        closedKeys.add(currPartitionKey);
      }

      currPartitionKey = partitionKey;
      currContext = getKeySpecificContext(currPartitionKey);
      currRecordWriter = getBaseRecordWriter(currContext);
    }
    currRecordWriter.write(key, value);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      // the writer can be null if this writer didn't get any records (split with no data, for instance)
      if (currRecordWriter != null) {
        currRecordWriter.close(currContext);
      }
      taskContext.flushOperations();
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      dynamicPartitioner.destroy();
    }
  }
}
