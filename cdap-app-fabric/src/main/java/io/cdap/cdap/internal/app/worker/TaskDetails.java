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

package io.cdap.cdap.internal.app.worker;

/**
 * Class for holding details of a task
 */
public class TaskDetails {
  private final boolean success;
  private final String className;
  private final long startTime;

  public TaskDetails(boolean success, String className, long startTime) {
    this.success = success;
    this.className = className;
    this.startTime = startTime;
  }

  public boolean isSuccess() {
    return success;
  }

  public String getClassName() {
    return className;
  }

  public long getStartTime() {
    return startTime;
  }
}
