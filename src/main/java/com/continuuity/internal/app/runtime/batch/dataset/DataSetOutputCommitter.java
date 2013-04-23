package com.continuuity.internal.app.runtime.batch.dataset;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

class DataSetOutputCommitter extends OutputCommitter {
  @Override
  public void setupJob(final JobContext jobContext) throws IOException {
    // TODO: start transaction
  }

  @Override
  public void commitJob(final JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    // TODO: commit transaction
  }

  @Override
  public void abortJob(final JobContext jobContext, final JobStatus.State state) throws IOException {
    super.abortJob(jobContext, state);
    // TODO: rollback transaction
  }

  @Override
  public boolean needsTaskCommit(final TaskAttemptContext taskContext) throws IOException {
    // Don't do commit of individual task work. Work is committed on job level
    return false;
  }

  @Override
  public void setupTask(final TaskAttemptContext taskContext) throws IOException {
    // DO NOTHING, see needsTaskCommit() comment
  }

  @Override
  public void commitTask(final TaskAttemptContext taskContext) throws IOException {
    // DO NOTHING, see needsTaskCommit() comment
  }

  @Override
  public void abortTask(final TaskAttemptContext taskContext) throws IOException {
    // DO NOTHING, see needsTaskCommit() comment
  }
}
