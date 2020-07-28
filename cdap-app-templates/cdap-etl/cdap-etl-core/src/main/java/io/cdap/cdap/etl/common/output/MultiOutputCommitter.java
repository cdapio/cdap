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

package io.cdap.cdap.etl.common.output;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

/**
 * Delegates to other record writers.
 */
public class MultiOutputCommitter extends OutputCommitter {
  private final Map<String, OutputCommitter> delegates;

  public MultiOutputCommitter(Map<String, OutputCommitter> delegates) {
    this.delegates = delegates;
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> entry : delegates.entrySet()) {
      entry.getValue().setupJob(MultiOutputFormat.getNamedJobContext(jobContext, entry.getKey()));
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> entry : delegates.entrySet()) {
      entry.getValue().commitJob(MultiOutputFormat.getNamedJobContext(jobContext, entry.getKey()));
    }
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    IOException ex = null;
    for (Map.Entry<String, OutputCommitter> entry : delegates.entrySet()) {
      try {
        entry.getValue().abortJob(MultiOutputFormat.getNamedJobContext(jobContext, entry.getKey()), state);
      } catch (IOException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> entry : delegates.entrySet()) {
      entry.getValue().setupTask(MultiOutputFormat.getNamedTaskContext(taskContext, entry.getKey()));
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> entry : delegates.entrySet()) {
      if (entry.getValue().needsTaskCommit(MultiOutputFormat.getNamedTaskContext(taskContext, entry.getKey()))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> entry : delegates.entrySet()) {
      entry.getValue().commitTask(MultiOutputFormat.getNamedTaskContext(taskContext, entry.getKey()));
    }
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    IOException ex = null;
    for (Map.Entry<String, OutputCommitter> entry : delegates.entrySet()) {
      try {
        entry.getValue().abortTask(MultiOutputFormat.getNamedTaskContext(taskContext, entry.getKey()));
      } catch (IOException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Override
  public boolean isRecoverySupported() {
    for (OutputCommitter delegate : delegates.values()) {
      if (!delegate.isRecoverySupported()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> entry : delegates.entrySet()) {
      entry.getValue().recoverTask(MultiOutputFormat.getNamedTaskContext(taskContext, entry.getKey()));
    }
  }
}
