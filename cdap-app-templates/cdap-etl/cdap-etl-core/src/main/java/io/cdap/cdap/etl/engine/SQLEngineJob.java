/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.engine;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Contains information about a SQL Engine Job.
 * @param <T> The output type for this SQL Engine Job.
 */
public class SQLEngineJob<T> {
  private final SQLEngineJobKey key;
  private final CompletableFuture<T> task;

  public SQLEngineJob(SQLEngineJobKey key, CompletableFuture<T> task) {
    this.key = key;
    this.task = task;
  }



  public boolean isDone() {
    return task.isDone();
  }

  public boolean isCancelled() {
    return task.isCancelled();
  }

  public boolean isCompletedExceptionally() {
    return task.isCompletedExceptionally();
  }

  public T result() throws ExecutionException, InterruptedException {
    return task.get();
  }

  public T waitFor() throws CompletionException, CancellationException {
    return task.join();
  }

  public void waitFor(long timeout) throws InterruptedException {
    task.wait(timeout);
  }

  public T get() throws ExecutionException, InterruptedException {
    return task.get();
  }

  public SQLEngineJobKey getKey() {
    return key;
  }

  public String getDatasetName() {
    return key.getDatasetName();
  }

  public SQLEngineJobType getType() {
    return key.getJobType();
  }

  public CompletableFuture<?> getTask() {
    return task;
  }

  public void cancel() {
    if (!task.isDone()) {
      task.cancel(true);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SQLEngineJob that = (SQLEngineJob) o;
    return this.key.equals(that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }
}
