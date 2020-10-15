/*
 * Copyright © 2020 Cask Data, Inc.
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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * A {@link OutputCommitter} that delegate all operations to another {@link OutputCommitter}, with counter metrics
 * sending to Spark metrics.
 */
public class TrackingOutputCommitter extends OutputCommitter {

  private final OutputCommitter delegate;

  public TrackingOutputCommitter(OutputCommitter delegate) {
    this.delegate = delegate;
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    delegate.setupJob(jobContext);
  }

  @Override
  @Deprecated
  public void cleanupJob(JobContext jobContext) throws IOException {
    delegate.cleanupJob(jobContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    delegate.commitJob(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    delegate.abortJob(jobContext, state);
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    delegate.setupTask(new TrackingTaskAttemptContext(taskContext));
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    return delegate.needsTaskCommit(new TrackingTaskAttemptContext(taskContext));
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    delegate.commitTask(new TrackingTaskAttemptContext(taskContext));
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    delegate.abortTask(new TrackingTaskAttemptContext(taskContext));
  }

  @Override
  public boolean isRecoverySupported() {
    return delegate.isRecoverySupported();
  }

  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    delegate.recoverTask(new TrackingTaskAttemptContext(taskContext));
  }
}
