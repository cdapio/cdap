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

import io.cdap.cdap.api.exception.ErrorDetailsProvider;
import io.cdap.cdap.api.exception.WrappedStageException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A delegating record writer that catches exceptions thrown during execution of a call
 * and wraps them in a {@link WrappedStageException}.
 * This class is primarily used to associate the exception with a specific stage name in a pipeline,
 * helping in better debugging and error tracking.
 *
 * <p>
 * The class delegates the actual calling operation to another {@link TrackingRecordReader} instance
 * and ensures that any exceptions thrown are caught and rethrown as a {@link WrappedStageException}
 * , which includes the stage name where the error occurred.
 * </p>
 *
 * @param <K> type of key to read
 * @param <V> type of value to read
 */
public class StageTrackingRecordReader<K, V> extends RecordReader<K, V> implements
  ErrorDetailsProvider<Void> {

  private final RecordReader<K, V> delegate;
  private final String stageName;

  public StageTrackingRecordReader(RecordReader<K, V> delegate, String stageName) {
    this.delegate = delegate;
    this.stageName = stageName;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    try {
      delegate.initialize(split, new TrackingTaskAttemptContext(context));
    } catch (Exception e) {
      throw getExceptionDetails(e, null);
    }
  }

  @Override
  public boolean nextKeyValue() {
    try {
      return delegate.nextKeyValue();
    } catch (Exception e) {
      throw getExceptionDetails(e, null);
    }
  }

  @Override
  public K getCurrentKey() {
    try {
      return delegate.getCurrentKey();
    } catch (Exception e) {
      throw getExceptionDetails(e, null);
    }
  }

  @Override
  public V getCurrentValue() {
    try {
      return delegate.getCurrentValue();
    } catch (Exception e) {
      throw getExceptionDetails(e, null);
    }
  }

  @Override
  public float getProgress() {
    try {
      return delegate.getProgress();
    } catch (Exception e) {
      throw getExceptionDetails(e, null);
    }
  }

  @Override
  public void close() {
    try {
      delegate.close();
    } catch (Exception e) {
      throw getExceptionDetails(e, null);
    }
  }

  @Override
  public RuntimeException getExceptionDetails(Throwable e, Void conf) {
    RuntimeException exception = null;
    if (delegate instanceof ErrorDetailsProvider<?>) {
      exception = ((ErrorDetailsProvider<?>) delegate).getExceptionDetails(e, null);
    }
    return new WrappedStageException(exception == null ? e : exception, stageName);
  }
}
