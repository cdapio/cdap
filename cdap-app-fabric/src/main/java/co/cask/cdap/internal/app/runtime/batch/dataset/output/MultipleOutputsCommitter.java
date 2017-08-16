/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataset.output;

import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * OutputCommitter that delegates to a collection of other OutputFormatCommitters.
 */
public class MultipleOutputsCommitter extends OutputCommitter {

  private Map<String, OutputCommitter> committers;
  protected final TaskAttemptContext taskAttemptContext;

  public MultipleOutputsCommitter(TaskAttemptContext taskAttemptContext) {
    this.taskAttemptContext = taskAttemptContext;
  }

  // TODO explain why it cant happen in ctor (must be after setupJob)
  // TODO: validate that setupJob is called before any setupTask
  private Map<String, OutputCommitter> getCommitters() {
    if (committers == null) {
      try {
        // use a linked hash map: it preserves the order of insertion, so the output committers are called in the
        // same order as outputs were added. This makes multi-output a little more predictable (and testable).
        committers = new LinkedHashMap<>();
        for (String name : MultipleOutputs.getNamedOutputsList(taskAttemptContext)) {
          Class<? extends OutputFormat> namedOutputFormatClass =
            MultipleOutputs.getNamedOutputFormatClass(taskAttemptContext, name);

          TaskAttemptContext namedContext = MultipleOutputs.getNamedTaskContext(taskAttemptContext, name);

          OutputFormat outputFormat =
            ReflectionUtils.newInstance(namedOutputFormatClass, namedContext.getConfiguration());
          committers.put(name, outputFormat.getOutputCommitter(namedContext));
        }
      } catch (IOException | InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
    return committers;
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> committer : getCommitters().entrySet()) {
      JobContext namedJobContext = MultipleOutputs.getNamedJobContext(jobContext, committer.getKey());
      committer.getValue().setupJob(namedJobContext);
    }
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> committer : getCommitters().entrySet()) {
      TaskAttemptContext namedTaskContext = MultipleOutputs.getNamedTaskContext(taskContext, committer.getKey());
      committer.getValue().setupTask(namedTaskContext);
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    // needs task commit if any delegates need task commit
    for (Map.Entry<String, OutputCommitter> committer : getCommitters().entrySet()) {
      TaskAttemptContext namedTaskContext = MultipleOutputs.getNamedTaskContext(taskContext, committer.getKey());
      if (committer.getValue().needsTaskCommit(namedTaskContext)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> committer : getCommitters().entrySet()) {
      TaskAttemptContext namedTaskContext = MultipleOutputs.getNamedTaskContext(taskContext, committer.getKey());
      if (committer.getValue().needsTaskCommit(namedTaskContext)) {
        committer.getValue().commitTask(namedTaskContext);
      }
    }
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> committer : getCommitters().entrySet()) {
      TaskAttemptContext namedTaskContext = MultipleOutputs.getNamedTaskContext(taskContext, committer.getKey());
      committer.getValue().abortTask(namedTaskContext);
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> committer : getCommitters().entrySet()) {
      JobContext namedJobContext = MultipleOutputs.getNamedJobContext(jobContext, committer.getKey());
      committer.getValue().commitJob(namedJobContext);
    }
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    for (Map.Entry<String, OutputCommitter> committer : getCommitters().entrySet()) {
      JobContext namedJobContext = MultipleOutputs.getNamedJobContext(jobContext, committer.getKey());
      committer.getValue().abortJob(namedJobContext, state);
    }
  }

  @Override
  public boolean isRecoverySupported() {
    // recovery is supported if it is supported on all delegates
    for (OutputCommitter committer : getCommitters().values()) {
      if (!committer.isRecoverySupported()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    for (Map.Entry<String, OutputCommitter> committer : getCommitters().entrySet()) {
      TaskAttemptContext namedTaskContext = MultipleOutputs.getNamedTaskContext(taskContext, committer.getKey());
      committer.getValue().recoverTask(namedTaskContext);
    }
  }
}
