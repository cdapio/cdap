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

import io.cdap.cdap.etl.api.exception.ErrorDetailsProvider;
import io.cdap.cdap.etl.api.exception.ErrorPhase;
import io.cdap.cdap.etl.common.ErrorDetails;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A delegating output format that catches exceptions thrown during execution of a call
 * and wraps them in a {@link WrappedStageException}.
 * This class is primarily used to associate the exception with a specific stage name in a pipeline,
 * helping in better debugging and error tracking.
 *
 * <p>
 * The class delegates the actual calling operation to another
 * {@link TrackingOutputCommitter} instance and ensures that any exceptions thrown are caught and
 * rethrown as a {@link WrappedStageException},which includes the
 * stage name where the error occurred.
 * </p>
 */
public class StageTrackingOutputCommitter extends OutputCommitter {

  private final OutputCommitter delegate;
  private final String stageName;
  private final ErrorDetailsProvider errorDetailsProvider;

  public StageTrackingOutputCommitter(OutputCommitter delegate, String stageName,
    ErrorDetailsProvider errorDetailsProvider) {
    this.delegate = delegate;
    this.stageName = stageName;
    this.errorDetailsProvider = errorDetailsProvider;
  }

  @Override
  public void setupJob(JobContext jobContext) {
    try {
      delegate.setupJob(jobContext);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }

  @Override
  @Deprecated
  public void cleanupJob(JobContext jobContext) {
    try {
      delegate.cleanupJob(jobContext);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }

  @Override
  public void commitJob(JobContext jobContext) {
    try {
      delegate.commitJob(jobContext);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }

  @Override
  public void abortJob(JobContext jobContext, State state) {
    try {
      delegate.abortJob(jobContext, state);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) {
    try {
      delegate.setupTask(taskAttemptContext);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
    try {
      return delegate.needsTaskCommit(taskAttemptContext);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) {
    try {
      delegate.commitTask(taskAttemptContext);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) {
    try {
      delegate.abortTask(taskAttemptContext);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }

  @Override
  @Deprecated
  public boolean isRecoverySupported() {
    return delegate.isRecoverySupported();
  }

  @Override
  public boolean isCommitJobRepeatable(JobContext jobContext) {
    try {
      return delegate.isCommitJobRepeatable(jobContext);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }

  @Override
  public boolean isRecoverySupported(JobContext jobContext) {
    try {
      return delegate.isRecoverySupported(jobContext);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }

  @Override
  public void recoverTask(TaskAttemptContext taskContext) {
    try {
      delegate.recoverTask(taskContext);
    } catch (Exception e) {
      throw ErrorDetails.handleException(e, stageName, errorDetailsProvider,
        ErrorPhase.COMMITTING);
    }
  }
}
