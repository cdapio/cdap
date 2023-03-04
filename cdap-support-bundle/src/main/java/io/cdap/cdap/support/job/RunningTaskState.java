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

package io.cdap.cdap.support.job;


import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Support Bundle task state to record the task status.
 */
public class RunningTaskState {

  private final Future<SupportBundleTaskStatus> future;
  private final AtomicLong startTime;
  private final SupportBundleTaskStatus taskStatus;

  public RunningTaskState(Future<SupportBundleTaskStatus> future, AtomicLong startTime,
      SupportBundleTaskStatus taskStatus) {
    this.future = future;
    this.startTime = startTime;
    this.taskStatus = taskStatus;
  }

  public Future<SupportBundleTaskStatus> getFuture() {
    return future;
  }

  public AtomicLong getStartTime() {
    return startTime;
  }

  public SupportBundleTaskStatus getTaskStatus() {
    return taskStatus;
  }
}
