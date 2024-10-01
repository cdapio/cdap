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

package io.cdap.cdap.etl.spark.io;

import io.cdap.cdap.api.exception.WrappedStageException;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A delegating record writer that catches exceptions thrown during execution of a call
 * and wraps them in a {@link WrappedStageException}.
 * This class is primarily used to associate the exception with a specific stage name in a pipeline,
 * helping in better debugging and error tracking.
 *
 * <p>
 * The class delegates the actual calling operation to another {@link TrackingRecordWriter} instance
 * and ensures that any exceptions thrown are caught and rethrown as a {@link WrappedStageException}
 * , which includes the stage name where the error occurred.
 * </p>
 *
 * @param <K> type of key to write
 * @param <V> type of value to write
 */
public class StageTrackingRecordWriter<K, V> extends RecordWriter<K, V> {
  private final RecordWriter<K, V> delegate;
  private final String stageName;

  public StageTrackingRecordWriter(RecordWriter<K, V> delegate, String stageName) {
    this.delegate = delegate;
    this.stageName = stageName;
  }

  @Override
  public void write(K k, V v) {
    try {
      delegate.write(k, v);
    } catch (Exception e) {
      throw new WrappedStageException(e, stageName);
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    try {
      delegate.close(taskAttemptContext);
    } catch (Exception e) {
      throw new WrappedStageException(e, stageName);
    }
  }
}
