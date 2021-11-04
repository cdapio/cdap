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

package io.cdap.cdap.support.status;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/**
 * Each individual task status when generating support bundle.
 */
public class SupportBundleTaskStatus {
  // unique task name, also defines directory task stores resulting files in
  private String name;
  // task class name
  private String type;
  // status for task (QUEUED/IN_PROGRESS/FINISHED/FAILED)
  private CollectionState status;
  // if task was retried, number of retries, otherwise 0
  private int retries = 0;
  // array of subtasks (if any)
  @SerializedName("sub-tasks")
  private List<SupportBundleTaskStatus> subTasks = new ArrayList<>();
  // if task was already started, timestamp of the last start
  @SerializedName("start-timestamp")
  private long startTimestamp;
  // if the task was already finished, timestamp of the last finish Cleared when the task started
  // after retry and repopulated on subsequent finish
  @SerializedName("finish-timestamp")
  private long finishTimestamp;

  public SupportBundleTaskStatus(String name, String type, long startTimestamp) {
    this.name = name;
    this.type = type;
    this.startTimestamp = startTimestamp;
  }

  public SupportBundleTaskStatus(SupportBundleTaskStatus outdatedTaskStatus, int retries, CollectionState status) {
    this.name = outdatedTaskStatus.getName();
    this.type = outdatedTaskStatus.getType();
    this.startTimestamp = outdatedTaskStatus.getStartTimestamp();
    this.subTasks = outdatedTaskStatus.getSubTasks();
    this.retries = retries;
    this.status = status;
  }

  public SupportBundleTaskStatus(SupportBundleTaskStatus outdatedTaskStatus, long finishTimestamp,
                                 CollectionState status) {
    this.name = outdatedTaskStatus.getName();
    this.type = outdatedTaskStatus.getType();
    this.startTimestamp = outdatedTaskStatus.getStartTimestamp();
    this.subTasks = outdatedTaskStatus.getSubTasks();
    this.retries = outdatedTaskStatus.getRetries();
    this.finishTimestamp = finishTimestamp;
    this.status = status;
  }

  /**
   * Get support bundle generation task name
   */
  public String getName() {
    return name;
  }

  /**
   * Get support bundle generation task type
   */
  public String getType() {
    return type;
  }

  /**
   * Get support bundle generation task status
   */
  public CollectionState getStatus() {
    return status;
  }

  /**
   * Get support bundle generation task retry times
   */
  public int getRetries() {
    return retries;
  }

  /**
   * Get support bundle generation subtask status
   */
  public List<SupportBundleTaskStatus> getSubTasks() {
    return subTasks;
  }

  /**
   * Get support bundle generation task start time
   */
  public long getStartTimestamp() {
    return startTimestamp;
  }

  /**
   * Get support bundle generation task finish time
   */
  public long getFinishTimestamp() {
    return finishTimestamp;
  }
}
