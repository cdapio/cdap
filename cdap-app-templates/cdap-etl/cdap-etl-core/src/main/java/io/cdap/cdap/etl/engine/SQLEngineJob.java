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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Contains information about a SQL Engine Job.
 * @param <T> The output type for this SQL Engine Job.
 */
public class SQLEngineJob<T> {
  private final String tableName;
  private final SQLEngineJobType type;
  private SQLEngineJobStatus status;
  private final CompletableFuture<T> task;

  public SQLEngineJob(String tableName, SQLEngineJobType type, CompletableFuture<T> task) {
    this.tableName = tableName;
    this.type = type;
    this.status = SQLEngineJobStatus.CREATED;
    this.task = task;
  }

  public SQLEngineJobStatus updateStatus() {
    if (!task.isDone()) {
      status = SQLEngineJobStatus.RUNNING;
    } else if (task.isCompletedExceptionally()) {
      status = SQLEngineJobStatus.FAILED;
    } else if (task.isDone()) {
      status = SQLEngineJobStatus.COMPLETED;
    }

    return status;
  }

  public boolean isDone() {
    return task.isDone();
  }

  public boolean isCancelled() {
    return task.isCancelled();
  }

  public T result() throws ExecutionException, InterruptedException {
    return task.get();
  }

  public void waitFor() throws InterruptedException {
    task.wait();
  }

  public void waitFor(long timeout) throws InterruptedException {
    task.wait(timeout);
  }

  public String getTableName() {
    return tableName;
  }

  public SQLEngineJobType getType() {
    return type;
  }

  public SQLEngineJobStatus getStatus() {
    return status;
  }

  public Future<?> getTask() {
    return task;
  }

  public void cancel() {
    if (!task.isDone() || task.isCompletedExceptionally() || task.isCancelled()) {
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
    return tableName.equals(that.tableName) &&
      type == that.type &&
      status == that.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, type, status);
  }
}
