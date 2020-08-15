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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

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
    delegateInParallel((name, delegate) -> {
      JobContext namedContext = MultiOutputFormat.getNamedJobContext(jobContext, name);
      delegate.setupJob(namedContext);
    });
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    delegateInParallel((name, delegate) -> {
      JobContext namedContext = MultiOutputFormat.getNamedJobContext(jobContext, name);
      delegate.commitJob(namedContext);
    });
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    delegateInParallel((name, delegate) -> {
      JobContext namedContext = MultiOutputFormat.getNamedJobContext(jobContext, name);
      delegate.abortJob(namedContext, state);
    });
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    delegateInParallel((name, delegate) -> {
      TaskAttemptContext namedContext = MultiOutputFormat.getNamedTaskContext(taskContext, name);
      delegate.setupTask(namedContext);
    });
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
    delegateInParallel((name, delegate) -> {
      TaskAttemptContext namedContext = MultiOutputFormat.getNamedTaskContext(taskContext, name);
      delegate.commitTask(namedContext);
    });
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    delegateInParallel((name, delegate) -> {
      TaskAttemptContext namedContext = MultiOutputFormat.getNamedTaskContext(taskContext, name);
      delegate.abortTask(namedContext);
    });
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
    delegateInParallel((name, delegate) -> {
      TaskAttemptContext namedContext = MultiOutputFormat.getNamedTaskContext(taskContext, name);
      delegate.recoverTask(namedContext);
    });
  }

  private void delegateInParallel(DelegateFunction delegateFunction) throws IOException {
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("multi-output-committer-%d").build();
    int numThreads = Math.min(10, delegates.size());
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads, threadFactory);
    ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<>(executorService);

    for (Map.Entry<String, OutputCommitter> entry : delegates.entrySet()) {
      String delegateName = entry.getKey();
      OutputCommitter delegate = entry.getValue();
      ecs.submit(() -> {
        delegateFunction.call(delegateName, delegate);
        return null;
      });
    }

    IOException exc = null;
    for (int i = 0; i < delegates.size(); i++) {
      try {
        ecs.take().get();
      } catch (InterruptedException e) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
        return;
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (exc != null) {
          exc.addSuppressed(cause);
        } else if (cause instanceof IOException) {
          exc = (IOException) cause;
        } else {
          exc = new IOException(cause);
        }
      }
    }

    executorService.shutdownNow();
    if (exc != null) {
      throw exc;
    }
  }

  /**
   * A function to perform on a delegate OutputCommitter.
   */
  private interface DelegateFunction {

    void call(String delegateName, OutputCommitter delegate) throws IOException;
  }
}
