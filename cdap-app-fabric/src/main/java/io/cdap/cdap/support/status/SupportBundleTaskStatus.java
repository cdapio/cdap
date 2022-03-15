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
 * Each individual task status when generating support bundle
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
  private List<SupportBundleTaskStatus> subTasks = new ArrayList<>();
  // if task was already started, timestamp of the last start
  private Long startTimestamp;
  // if the task was already finished, timestamp of the last finish Cleared when the task started
  // after retry and repopulated on subsequent finish
  private Long finishTimestamp;

  /** Get support bundle generation task name */
  public String getName() {
    return name;
  }

  /** Set support bundle generation task name */
  public void setName(String name) {
    this.name = name;
  }

  /** Get support bundle generation task type */
  public String getType() {
    return type;
  }

  /** Set support bundle generation task type */
  public void setType(String type) {
    this.type = type;
  }

  /** Get support bundle generation task status */
  public CollectionState getStatus() {
    return status;
  }

  /** Set support bundle generation task status */
  public void setStatus(CollectionState status) {
    this.status = status;
  }

  /** Get support bundle generation task retry times */
  public int getRetries() {
    return retries;
  }

  /** Set support bundle generation task retry times */
  public void setRetries(int retries) {
    this.retries = retries;
  }

  /** Get support bundle generation subtask status */
  public List<SupportBundleTaskStatus> getSubTasks() {
    return subTasks;
  }

  /** Set support bundle generation subtask status */
  public void setSubTasks(List<SupportBundleTaskStatus> subTasks) {
    this.subTasks = subTasks;
  }

  /** Get support bundle generation task start time */
  public Long getStartTimestamp() {
    return startTimestamp;
  }

  /** Set support bundle generation task start time */
  public void setStartTimestamp(Long startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  /** Get support bundle generation task finish time */
  public Long getFinishTimestamp() {
    return finishTimestamp;
  }

  /** Set support bundle generation task finish time */
  public void setFinishTimestamp(Long finishTimestamp) {
    this.finishTimestamp = finishTimestamp;
  }
}
